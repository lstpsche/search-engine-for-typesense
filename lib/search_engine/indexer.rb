# frozen_string_literal: true

require 'json'
require 'timeout'
require 'digest'
require 'time'

module SearchEngine
  # Batch importer for streaming JSONL documents into a physical collection.
  #
  # Emits one AS::Notifications event per attempt: "search_engine.indexer.batch_import".
  # Works strictly batch-by-batch to avoid memory growth and retries transient
  # failures with exponential backoff and jitter.
  class Indexer
    # Aggregated summary of an import run.
    Summary = Struct.new(
      :collection,
      :status,
      :batches_total,
      :docs_total,
      :success_total,
      :failed_total,
      :duration_ms_total,
      :batches,
      keyword_init: true
    )

    # Rebuild a single partition end-to-end using the model's partitioning + mapper.
    #
    # The flow is:
    # - Resolve a partition fetch enumerator from the partitioning DSL (or fall back to source adapter)
    # - Optionally run before/after hooks with configured timeouts
    # - Map each batch to documents and stream-import them into the target collection
    #
    # @param klass [Class] a {SearchEngine::Base} subclass
    # @param partition [Object] opaque partition key as defined by the DSL/source
    # @param into [String, nil] target collection; defaults to resolver or the logical collection alias
    # @return [Summary]
    # @raise [SearchEngine::Errors::InvalidParams]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Indexer#partitioned-indexing`
    def self.rebuild_partition!(klass, partition:, into: nil)
      raise Errors::InvalidParams, 'klass must be a Class' unless klass.is_a?(Class)
      unless klass.ancestors.include?(SearchEngine::Base)
        raise Errors::InvalidParams, 'klass must inherit from SearchEngine::Base'
      end

      compiled_partitioner = SearchEngine::Partitioner.for(klass)
      mapper = SearchEngine::Mapper.for(klass)
      unless mapper
        raise Errors::InvalidParams,
              "mapper is not defined for #{klass.name}. Define it via `index do ... map { ... } end`."
      end

      target_into = resolve_into!(klass, partition: partition, into: into)
      rows_enum = rows_enumerator_for(klass, partition: partition, compiled_partitioner: compiled_partitioner)

      before_hook = compiled_partitioner&.instance_variable_get(:@before_hook_proc)
      after_hook  = compiled_partitioner&.instance_variable_get(:@after_hook_proc)

      started_at = monotonic_ms
      pfields = SearchEngine::Observability.partition_fields(partition)
      dispatch_ctx = SearchEngine::Instrumentation.context
      instrument_partition_start(klass, target_into, pfields, dispatch_ctx)

      docs_enum = build_docs_enum(rows_enum, mapper)

      summary = nil
      SearchEngine::Instrumentation.with_context(into: target_into) do
        run_before_hook_if_present(before_hook, partition, klass)

        summary = import!(
          klass,
          into: target_into,
          enum: docs_enum,
          batch_size: nil,
          action: :upsert
        )

        run_after_hook_if_present(after_hook, partition)
      end

      instrument_partition_finish(klass, target_into, pfields, summary, started_at)

      summary
    end

    # Delete stale documents from a physical collection using a developer-provided filter.
    #
    # @param klass [Class] a {SearchEngine::Base} subclass
    # @param partition [Object, nil]
    # @param into [String, nil]
    # @param dry_run [Boolean]
    # @return [Hash]
    # @raise [SearchEngine::Errors::InvalidParams]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Indexer#stale-deletes`
    def self.delete_stale!(klass, partition: nil, into: nil, dry_run: false)
      validate_stale_args!(klass)

      cfg = SearchEngine.config
      sd_cfg = cfg.stale_deletes
      target_into = resolve_into!(klass, partition: partition, into: into)

      skipped = skip_if_disabled(klass, sd_cfg, target_into, partition)
      return skipped if skipped

      compiled = SearchEngine::StaleFilter.for(klass)
      skipped = skip_if_no_filter(compiled, klass, target_into, partition)
      return skipped if skipped

      filter = compiled.call(partition: partition)
      skipped = skip_if_empty_filter(filter, klass, target_into, partition)
      return skipped if skipped

      skipped = skip_if_strict_blocked(filter, sd_cfg, klass, target_into, partition)
      return skipped if skipped

      fhash = Digest::SHA1.hexdigest(filter)
      started = monotonic_ms
      instrument_started(klass: klass, into: target_into, partition: partition, filter_hash: fhash)

      if dry_run
        estimated = estimate_found_if_enabled(cfg, sd_cfg, target_into, filter)
        return dry_run_summary(klass, target_into, partition, filter, fhash, started, estimated)
      end

      deleted_count = perform_delete_and_count(target_into, filter, sd_cfg.timeout_ms)
      duration = monotonic_ms - started
      instrument_finished(
        klass: klass,
        into: target_into,
        partition: partition,
        duration_ms: duration,
        deleted_count: deleted_count
      )
      ok_summary(klass, target_into, partition, filter, fhash, duration, deleted_count)
    rescue Errors::Error => error
      duration = monotonic_ms - (started || monotonic_ms)
      instrument_error(error, klass: klass, into: target_into, partition: partition)
      failed_summary(klass, target_into, partition, filter, fhash, duration, error)
    end

    # Import pre-batched documents using JSONL bulk import.
    #
    # @param klass [Class] a SearchEngine::Base subclass (reserved for future mappers)
    # @param into [String] target physical collection name
    # @param enum [Enumerable] yields batches (Array-like) of Hash documents
    # @param batch_size [Integer, nil] soft guard only; not used unless 413 handling
    # @param action [Symbol] :upsert (default), :create, or :update
    # @return [Summary]
    # @raise [SearchEngine::Errors::InvalidParams]
    # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Indexer`
    # @see `https://typesense.org/docs/latest/api/documents.html#import-documents`
    def self.import!(klass, into:, enum:, batch_size: nil, action: :upsert)
      SearchEngine::Indexer::BulkImport.call(
        klass: klass,
        into: into,
        enum: enum,
        batch_size: batch_size,
        action: action
      )
    end

    class << self
      private

      def validate_stale_args!(klass)
        raise Errors::InvalidParams, 'klass must be a Class' unless klass.is_a?(Class)
        return if klass.ancestors.include?(SearchEngine::Base)

        raise Errors::InvalidParams, 'klass must inherit from SearchEngine::Base'
      end

      def skip_if_disabled(klass, sd_cfg, into, partition)
        return nil if sd_cfg&.enabled

        instrument_stale(:skipped, reason: :disabled, klass: klass, into: into, partition: partition)
        skip_summary(klass, into, partition)
      end

      def skip_if_no_filter(compiled, klass, into, partition)
        return nil if compiled

        instrument_stale(:skipped, reason: :no_filter_defined, klass: klass, into: into, partition: partition)
        skip_summary(klass, into, partition)
      end

      def skip_if_empty_filter(filter, klass, into, partition)
        return nil if filter && !filter.to_s.strip.empty?

        instrument_stale(:skipped, reason: :empty_filter, klass: klass, into: into, partition: partition)
        skip_summary(klass, into, partition)
      end

      def skip_if_strict_blocked(filter, sd_cfg, klass, into, partition)
        return nil unless sd_cfg.strict_mode && suspicious_filter?(filter)

        instrument_stale(:skipped, reason: :strict_blocked, klass: klass, into: into, partition: partition)
        {
          status: :skipped,
          collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
          into: into,
          partition: partition,
          filter_by: filter,
          filter_hash: Digest::SHA1.hexdigest(filter),
          duration_ms: 0.0,
          deleted_count: 0,
          estimated_found: nil
        }
      end

      def estimate_found_if_enabled(cfg, sd_cfg, into, filter)
        return nil unless sd_cfg.estimation_enabled && cfg.default_query_by && !cfg.default_query_by.to_s.strip.empty?

        client = SearchEngine::Client.new
        payload = { q: '*', query_by: cfg.default_query_by, per_page: 0, filter_by: filter }
        params = SearchEngine::CompiledParams.new(payload)
        res = client.search(collection: into, params: params, url_opts: {})
        res&.found
      rescue StandardError
        nil
      end

      def perform_delete_and_count(into, filter, timeout_ms)
        client = SearchEngine::Client.new
        resp = client.delete_documents_by_filter(
          collection: into,
          filter_by: filter,
          timeout_ms: timeout_ms
        )
        (resp && (resp[:num_deleted] || resp[:deleted] || resp[:numDeleted])).to_i
      end

      def dry_run_summary(klass, into, partition, filter, filter_hash, started, estimated)
        duration = monotonic_ms - started
        {
          status: :ok,
          collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
          into: into,
          partition: partition,
          filter_by: filter,
          filter_hash: filter_hash,
          duration_ms: duration.round(1),
          deleted_count: 0,
          estimated_found: estimated,
          will_delete: true
        }
      end

      def ok_summary(klass, into, partition, filter, filter_hash, duration, deleted_count)
        {
          status: :ok,
          collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
          into: into,
          partition: partition,
          filter_by: filter,
          filter_hash: filter_hash,
          duration_ms: duration.round(1),
          deleted_count: deleted_count,
          estimated_found: nil
        }
      end

      def failed_summary(klass, into, partition, filter, filter_hash, duration, error)
        {
          status: :failed,
          collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
          into: into,
          partition: partition,
          filter_by: filter,
          filter_hash: filter_hash,
          duration_ms: duration.round(1),
          deleted_count: 0,
          estimated_found: nil,
          error_class: error.class.name,
          message_truncated: error.message.to_s[0, 200]
        }
      end

      def skip_summary(klass, into, partition)
        {
          status: :skipped,
          collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
          into: into,
          partition: partition,
          filter_by: nil,
          filter_hash: nil,
          duration_ms: 0.0,
          deleted_count: 0,
          estimated_found: nil
        }
      end

      def rows_enumerator_for(klass, partition:, compiled_partitioner:)
        if compiled_partitioner
          compiled_partitioner.partition_fetch_enum(partition)
        else
          dsl = mapper_dsl_for(klass)
          source_def = dsl && dsl[:source]
          unless source_def
            raise Errors::InvalidParams,
                  'No partition_fetch defined and no source adapter provided. Define one in the DSL.'
          end
          adapter = SearchEngine::Sources.build(source_def[:type], **(source_def[:options] || {}), &source_def[:block])
          adapter.each_batch(partition: partition)
        end
      end

      def resolve_into!(klass, partition:, into:)
        return into if into && !into.to_s.strip.empty?

        resolver = SearchEngine.config.partitioning&.default_into_resolver
        if resolver.respond_to?(:arity)
          case resolver.arity
          when 1
            val = resolver.call(klass)
            return val if val && !val.to_s.strip.empty?
          when 2, -1
            val = resolver.call(klass, partition)
            return val if val && !val.to_s.strip.empty?
          end
        elsif resolver
          val = resolver.call(klass)
          return val if val && !val.to_s.strip.empty?
        end

        name = if klass.respond_to?(:collection)
                 klass.collection
               else
                 klass.name.to_s
               end
        name.to_s
      end

      def run_hook_with_timeout(proc_obj, partition, timeout_ms:)
        return proc_obj.call(partition) unless timeout_ms&.to_i&.positive?

        Timeout.timeout(timeout_ms.to_f / 1000.0) do
          proc_obj.call(partition)
        end
      end

      def import_batch_with_handling(client, collection, docs, action, next_index)
        buffer = +''
        docs_count = encode_jsonl!(docs, buffer)
        bytes_sent = buffer.bytesize
        idx = next_index.call

        begin
          attempt_stats = with_retries do |attempt|
            perform_attempt(client, collection, action, buffer, docs_count, bytes_sent, idx, attempt)
          end
          [attempt_stats]
        rescue Errors::Api => error
          if error.status.to_i == 413 && docs.size > 1
            mid = docs.size / 2
            left = docs[0...mid]
            right = docs[mid..]
            import_batch_with_handling(client, collection, left, action, next_index) +
              import_batch_with_handling(client, collection, right, action, next_index)
          else
            [
              {
                index: idx,
                docs_count: docs_count,
                success_count: 0,
                failure_count: docs_count,
                attempts: 1,
                http_status: error.status.to_i,
                duration_ms: 0.0,
                bytes_sent: bytes_sent,
                errors_sample: [safe_error_excerpt(error)]
              }
            ]
          end
        end
      end

      def perform_attempt(client, collection, action, jsonl, docs_count, bytes_sent, idx, attempt)
        start = monotonic_ms
        success_count = 0
        failure_count = 0
        http_status = 200
        error_sample = []

        if defined?(ActiveSupport::Notifications)
          se_payload = {
            collection: SearchEngine::Instrumentation.context[:collection] || collection,
            into: collection,
            batch_index: idx,
            docs_count: docs_count,
            success_count: nil,
            failure_count: nil,
            attempts: attempt,
            http_status: nil,
            bytes_sent: bytes_sent,
            transient_retry: attempt > 1,
            retry_after_s: nil,
            error_sample: nil
          }
          SearchEngine::Instrumentation.instrument('search_engine.indexer.batch_import', se_payload) do |ctx|
            raw = client.import_documents(collection: collection, jsonl: jsonl, action: action)
            success_count, failure_count, error_sample = parse_import_response(raw)
            http_status = 200
            ctx[:success_count] = success_count
            ctx[:failure_count] = failure_count
            ctx[:http_status] = http_status
          end
        else
          raw = client.import_documents(collection: collection, jsonl: jsonl, action: action)
          success_count, failure_count, error_sample = parse_import_response(raw)
        end

        duration = monotonic_ms - start
        {
          index: idx,
          docs_count: docs_count,
          success_count: success_count,
          failure_count: failure_count,
          attempts: attempt,
          http_status: http_status,
          duration_ms: duration.round(1),
          bytes_sent: bytes_sent,
          errors_sample: error_sample
        }
      end

      def with_retries
        cfg = SearchEngine.config.indexer
        attempts = cfg&.retries && cfg.retries[:attempts].to_i.positive? ? cfg.retries[:attempts].to_i : 3
        base = cfg&.retries && cfg.retries[:base].to_f.positive? ? cfg.retries[:base].to_f : 0.5
        max = cfg&.retries && cfg.retries[:max].to_f.positive? ? cfg.retries[:max].to_f : 5.0
        jitter = cfg&.retries && cfg.retries[:jitter_fraction].to_f >= 0 ? cfg.retries[:jitter_fraction].to_f : 0.2

        (1..attempts).each do |i|
          return yield(i)
        rescue Errors::Timeout, Errors::Connection
          raise if i >= attempts

          sleep_with_backoff(i, base: base, max: max, jitter_fraction: jitter)
        rescue Errors::Api => error
          code = error.status.to_i
          raise unless transient_status?(code)
          raise if i >= attempts

          sleep_with_backoff(i, base: base, max: max, jitter_fraction: jitter)
        end
      end

      def sleep_with_backoff(attempt, base:, max:, jitter_fraction:)
        exp = [base * (2 ** (attempt - 1)), max].min
        jitter = exp * jitter_fraction
        delta = rand(-jitter..jitter)
        sleep_time = exp + delta
        sleep(sleep_time) if sleep_time.positive?
      end

      def transient_status?(code)
        return true if code == 429
        return true if code >= 500 && code <= 599

        false
      end

      def to_array(batch)
        return batch if batch.is_a?(Array)

        batch.respond_to?(:to_a) ? batch.to_a : Array(batch)
      end

      def encode_jsonl!(docs, buffer)
        count = 0
        buffer.clear
        docs.each do |raw|
          doc = ensure_hash_document(raw)
          ensure_id!(doc)
          # Force system timestamp field just before serialization; developers cannot override.
          now_i = if defined?(Time) && defined?(Time.zone) && Time.zone
                    Time.zone.now.to_i
                  else
                    Time.now.to_i
                  end
          doc[:doc_updated_at] = now_i if doc.is_a?(Hash)
          buffer << JSON.generate(doc)
          buffer << "\n" if count < (docs.size - 1)
          count += 1
        end
        count
      end

      def ensure_hash_document(obj)
        if obj.is_a?(Hash)
          obj
        else
          raise Errors::InvalidParams,
                'Indexer requires batches of Hash-like documents with at least an :id key. ' \
                'Mapping DSL is not available yet. See docs/indexer.md.'
        end
      end

      def ensure_id!(doc)
        has_id = doc.key?(:id) || doc.key?('id')
        raise Errors::InvalidParams, 'document is missing required id' unless has_id
      end

      def parse_import_response(raw)
        return parse_from_string(raw) if raw.is_a?(String)
        return parse_from_array(raw) if raw.is_a?(Array)

        [0, 0, []]
      end

      def parse_from_string(str)
        success = 0
        failure = 0
        samples = []

        str.each_line do |line|
          line = line.strip
          next if line.empty?

          h = safe_parse_json(line)
          unless h
            failure += 1
            samples << 'invalid-json-line'
            next
          end

          if truthy?(h['success'] || h[:success])
            success += 1
          else
            failure += 1
            msg = h['error'] || h[:error] || h['message'] || h[:message]
            samples << msg.to_s[0, 200] if msg
          end
        end

        [success, failure, samples[0, 5]]
      end

      def parse_from_array(arr)
        success = 0
        failure = 0
        samples = []

        arr.each do |h|
          if h.is_a?(Hash) && truthy?(h['success'] || h[:success])
            success += 1
          else
            failure += 1
            msg = h.is_a?(Hash) ? (h['error'] || h[:error] || h['message'] || h[:message]) : nil
            samples << msg.to_s[0, 200] if msg
          end
        end

        [success, failure, samples[0, 5]]
      end

      def safe_parse_json(line)
        JSON.parse(line)
      rescue StandardError
        nil
      end

      def truthy?(val)
        val == true || val.to_s.downcase == 'true'
      end

      def safe_error_excerpt(error)
        cls = error.class.name
        msg = error.message.to_s
        "#{cls}: #{msg[0, 200]}"
      end

      def monotonic_ms
        SearchEngine::Instrumentation.monotonic_ms
      end

      def mapper_dsl_for(klass)
        return unless klass.instance_variable_defined?(:@__mapper_dsl__)

        klass.instance_variable_get(:@__mapper_dsl__)
      end

      def instrument_started(klass:, into:, partition:, filter_hash:)
        return unless defined?(ActiveSupport::Notifications)

        payload = {
          collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
          into: into,
          partition: partition,
          filter_hash: filter_hash
        }
        ActiveSupport::Notifications.instrument('search_engine.stale_deletes.started', payload) {}
      end

      def instrument_finished(klass:, into:, partition:, duration_ms:, deleted_count:)
        return unless defined?(ActiveSupport::Notifications)

        payload = {
          collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
          into: into,
          partition: partition,
          duration_ms: duration_ms.round(1),
          deleted_count: deleted_count
        }
        ActiveSupport::Notifications.instrument('search_engine.stale_deletes.finished', payload) {}
        pf = SearchEngine::Observability.partition_fields(partition)
        SearchEngine::Instrumentation.instrument('search_engine.indexer.delete_stale', payload.merge(partition_hash: pf[:partition_hash], status: 'ok')) {}
      end

      def instrument_error(error, klass:, into:, partition:)
        return unless defined?(ActiveSupport::Notifications)

        payload = {
          collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
          into: into,
          partition: partition,
          error_class: error.class.name,
          message_truncated: error.message.to_s[0, 200]
        }
        ActiveSupport::Notifications.instrument('search_engine.stale_deletes.error', payload) {}
        pf = SearchEngine::Observability.partition_fields(partition)
        SearchEngine::Instrumentation.instrument('search_engine.indexer.delete_stale', payload.merge(partition_hash: pf[:partition_hash], status: 'failed')) {}
      end

      def instrument_stale(_type, reason:, klass:, into:, partition:)
        return unless defined?(ActiveSupport::Notifications)

        payload = {
          reason: reason,
          collection: klass.respond_to?(:collection) ? klass.collection : klass.name.to_s,
          into: into,
          partition: partition
        }
        ActiveSupport::Notifications.instrument('search_engine.stale_deletes.skipped', payload) {}
        pf = SearchEngine::Observability.partition_fields(partition)
        SearchEngine::Instrumentation.instrument('search_engine.indexer.delete_stale', payload.merge(partition_hash: pf[:partition_hash], status: 'skipped')) {}
      end

      def suspicious_filter?(filter)
        s = filter.to_s
        return true unless s.include?('=')

        # Contains wildcard star without any field comparator context
        return true if s.include?('*') && !s.match?(/[a-zA-Z0-9_]+\s*[:><=!]/)

        false
      end

      def run_before_hook_if_present(before_hook, partition, klass)
        return unless before_hook

        # Guard: skip executing before_partition when the logical collection (alias or
        # same-named physical) is missing. This avoids 404s during the initial schema
        # apply before the alias swap has occurred.
        present = begin
          klass.respond_to?(:current_schema) && klass.current_schema
        rescue StandardError
          false
        end
        return unless present

        # Safety: do not execute before_partition hooks for nil partitions.
        # This prevents developers from accidentally issuing dangerous deletes
        # with empty filter values (e.g., "store_id:=").
        return if partition.nil?

        run_hook_with_timeout(
          before_hook,
          partition,
          timeout_ms: SearchEngine.config.partitioning.before_hook_timeout_ms
        )
      end

      def run_after_hook_if_present(after_hook, partition)
        return unless after_hook

        run_hook_with_timeout(
          after_hook,
          partition,
          timeout_ms: SearchEngine.config.partitioning.after_hook_timeout_ms
        )
      end

      def instrument_partition_start(klass, target_into, pfields, dispatch_ctx)
        SearchEngine::Instrumentation.instrument(
          'search_engine.indexer.partition_start',
          {
            collection: (klass.respond_to?(:collection) ? klass.collection : klass.name.to_s),
            into: target_into,
            partition: pfields[:partition],
            partition_hash: pfields[:partition_hash],
            dispatch_mode: dispatch_ctx[:dispatch_mode],
            job_id: dispatch_ctx[:job_id],
            timestamp: Time.now.utc.iso8601
          }
        ) {}
      end

      def instrument_partition_finish(klass, target_into, pfields, summary, started_at)
        SearchEngine::Instrumentation.instrument(
          'search_engine.indexer.partition_finish',
          {
            collection: (klass.respond_to?(:collection) ? klass.collection : klass.name.to_s),
            into: target_into,
            partition: pfields[:partition],
            partition_hash: pfields[:partition_hash],
            batches_total: summary.batches_total,
            docs_total: summary.docs_total,
            success_total: summary.success_total,
            failed_total: summary.failed_total,
            status: summary.status,
            duration_ms: (monotonic_ms - started_at).round(1)
          }
        ) {}
      end

      def build_docs_enum(rows_enum, mapper)
        Enumerator.new do |y|
          idx = 0
          rows_enum.each do |rows|
            docs, _report = mapper.map_batch!(rows, batch_index: idx)
            y << docs
            idx += 1
          end
        end
      end
    end
  end
end
