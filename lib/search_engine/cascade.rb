# frozen_string_literal: true

module SearchEngine
  # Cascade reindexing for collections that reference other collections via
  # Typesense field-level references.
  #
  # Public API:
  # - {.cascade_reindex!(source:, ids:, context: :update, client: nil)} => Hash summary
  #   - source: Class (SearchEngine::Base subclass) or String collection name
  #   - ids: Array<String, Integer>, the target key values to match in referencers
  #   - context: :update or :full (controls partial vs full behavior)
  module Cascade
    class << self
      # Trigger cascade reindex on collections that reference +source+.
      #
      # @param source [Class, String]
      # @param ids [Array<#to_s>, nil]
      # @param context [Symbol] :update or :full
      # @param client [SearchEngine::Client, nil]
      # @return [Hash]
      # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Metrics/PerceivedComplexity, Metrics/BlockNesting
      def cascade_reindex!(source:, ids:, context: :update, client: nil)
        raise ArgumentError, 'context must be :update or :full' unless %i[update full].include?(context.to_sym)

        src_collection = normalize_collection_name(source)
        source_klass = source.is_a?(Class) ? source : safe_collection_class(src_collection)
        ts_client =
          client ||
          (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) ||
          SearchEngine::Client.new

        graph = build_reverse_graph(client: ts_client)
        referencers = Array(graph[src_collection])

        # Detect immediate cycles (A <-> B) and skip those pairs
        cycle_pairs = detect_immediate_cycles(graph)

        # Per-run cache for alias lookups to avoid repeated network calls
        alias_cache = {}

        ensure_source_reference_fields!(
          source_klass,
          src_collection,
          referencers,
          client: ts_client,
          alias_cache: alias_cache
        )

        outcomes = []
        partial_count = 0
        full_count = 0
        skipped_unregistered = 0
        skipped_cycles = []

        seen_full = {}
        referencers.each do |edge|
          referrer_coll = edge[:referrer]
          local_key = edge[:local_key]

          # Skip cycle pairs deterministically (avoid ping-pong)
          if cycle_pairs.include?([src_collection, referrer_coll])
            skipped_cycles << { pair: [src_collection, referrer_coll] }
            outcomes << { collection: referrer_coll, mode: :skipped_cycle }
            next
          end

          ref_klass = safe_collection_class(referrer_coll)
          unless ref_klass
            skipped_unregistered += 1
            outcomes << { collection: referrer_coll, mode: :skipped_unregistered }
            next
          end

          mode = :full
          if context.to_sym == :update && can_partial_reindex?(ref_klass)
            begin
              SearchEngine::Indexer.rebuild_partition!(ref_klass, partition: { local_key.to_sym => Array(ids) },
into: nil
              )
              mode = :partial
              partial_count += 1
            rescue StandardError => error
              # Fallback to full when partial path fails unexpectedly
              if seen_full[referrer_coll]
                mode = :skipped_duplicate
              else
                executed = __se_full_reindex_for_referrer(ref_klass, client: ts_client, alias_cache: alias_cache)
                seen_full[referrer_coll] = true if executed
                if executed
                  mode = :full
                  full_count += 1
                else
                  mode = :skipped_no_partitions
                end
              end
              # Record diagnostic on the outcome for visibility upstream
              outcomes << { collection: referrer_coll, mode: :partial_failed, error_class: error.class.name,
                            message: error.message.to_s[0, 200] }
            end
          elsif seen_full[referrer_coll]
            mode = :skipped_duplicate
          else
            executed = __se_full_reindex_for_referrer(ref_klass, client: ts_client, alias_cache: alias_cache)
            seen_full[referrer_coll] = true if executed
            if executed
              mode = :full
              full_count += 1
            else
              mode = :skipped_no_partitions
            end
          end

          outcomes << { collection: referrer_coll, mode: mode }
        end

        payload = {
          source_collection: src_collection,
          ids_count: Array(ids).size,
          context: context.to_sym,
          targets_total: referencers.size,
          partial_count: partial_count,
          full_count: full_count,
          skipped_unregistered: skipped_unregistered,
          skipped_cycles: skipped_cycles
        }
        SearchEngine::Instrumentation.instrument('search_engine.cascade.run', payload.merge(outcomes: outcomes)) {}

        payload.merge(outcomes: outcomes)
      end
      # rubocop:enable Metrics/AbcSize, Metrics/MethodLength, Metrics/PerceivedComplexity, Metrics/BlockNesting

      # Build a reverse graph from Typesense live schemas when possible, falling
      # back to compiled local schemas for registered models.
      #
      # @param client [SearchEngine::Client]
      # @return [Hash{String=>Array<Hash>}] mapping target_collection => [{ referrer, local_key, foreign_key }]
      def build_reverse_graph(client:)
        from_ts = build_from_typesense(client)
        return from_ts unless from_ts.empty?

        build_from_registry
      end

      private

      # Perform a full reindex for a referencer collection, honoring partitioning
      # directives when present. Falls back to a single non-partitioned rebuild
      # when no partitions are configured.
      # @param ref_klass [Class]
      # @return [void]
      # rubocop:disable Metrics/PerceivedComplexity
      def __se_full_reindex_for_referrer(ref_klass, client:, alias_cache:)
        logical = ref_klass.respond_to?(:collection) ? ref_klass.collection.to_s : ref_klass.name.to_s
        physical = resolve_physical_collection_name(logical, client: client, cache: alias_cache)

        # For cascade full reindex, force a schema rebuild (blue/green) to
        # refresh reference targets before importing documents.
        forced = reindex_referencer_with_fresh_schema!(
          ref_klass,
          logical,
          physical,
          client: client,
          force_rebuild: true
        )
        return true if forced

        # Fallback: force full destructive reindex when forced rebuild fails.
        dropped = reindex_referencer_with_drop!(ref_klass, logical, physical)
        return true if dropped

        begin
          compiled = SearchEngine::Partitioner.for(ref_klass)
        rescue StandardError
          compiled = nil
        end

        executed = false

        if compiled
          parts = begin
            Array(compiled.partitions)
          rescue StandardError
            []
          end

          parts = parts.reject { |p| p.nil? || p.to_s.strip.empty? }

          if parts.empty?
            coll_display = physical && physical != logical ? "#{logical} (physical: #{physical})" : logical
            puts(%(  Referencer "#{coll_display}" — partitions=0 → skip))
            return false
          end

          coll_display = physical && physical != logical ? "#{logical} (physical: #{physical})" : logical
          puts(%(  Referencer "#{coll_display}" — partitions=#{parts.size} parallel=#{compiled.max_parallel}))
          mp = compiled.max_parallel.to_i
          if mp > 1 && parts.size > 1
            require 'concurrent-ruby'
            pool = Concurrent::FixedThreadPool.new(mp)
            ctx = SearchEngine::Instrumentation.context
            mtx = Mutex.new
            begin
              post_partitions_to_pool!(pool, ctx, parts, ref_klass, mtx)
            ensure
              pool.shutdown
              # Wait up to 1 hour, then force-kill and wait a bit more to ensure cleanup
              pool.wait_for_termination(3600) || pool.kill
              pool.wait_for_termination(60)
            end
            executed = true
          else
            executed = rebuild_partitions_sequential!(ref_klass, parts)
          end

        else
          coll_display = physical && physical != logical ? "#{logical} (physical: #{physical})" : logical
          puts(%(  Referencer "#{coll_display}" — single))
          SearchEngine::Indexer.rebuild_partition!(ref_klass, partition: nil, into: nil)
          executed = true
        end
        executed
      end
      # rubocop:enable Metrics/PerceivedComplexity

      # Resolve logical alias to physical name with optional per-run memoization.
      # @param logical [String]
      # @param client [SearchEngine::Client]
      # @param cache [Hash, nil] per-run cache; when provided, both hits and misses are cached
      # @return [String, nil]
      def resolve_physical_collection_name(logical, client:, cache: nil)
        key = logical.to_s
        return cache[key] if cache&.key?(key)

        value = begin
          physical = client.resolve_alias(key)
          physical && !physical.to_s.strip.empty? ? physical.to_s : nil
        rescue StandardError
          nil
        end
        cache[key] = value if cache
        value
      end

      def normalize_collection_name(source)
        return source.to_s unless source.is_a?(Class)

        if source.respond_to?(:collection)
          source.collection.to_s
        else
          source.name.to_s
        end
      end

      # Check if a collection's live schema has references pointing to physical
      # collection names that no longer exist. This can happen after blue/green
      # deployments when the referenced collection was reindexed but this
      # referencer's schema still points to the old physical name.
      #
      # @param collection_name [String] physical or logical collection name
      # @param client [SearchEngine::Client]
      # @return [Boolean]
      def referencer_has_stale_references?(collection_name, client:)
        schema = begin
          client.retrieve_collection_schema(collection_name)
        rescue StandardError
          nil
        end
        return false unless schema

        fields = Array(schema[:fields] || schema['fields'])
        fields.any? do |field|
          ref = field[:reference] || field['reference']
          next false if ref.nil? || ref.to_s.strip.empty?

          ref_coll = ref.to_s.split('.', 2).first
          next false if ref_coll.empty?

          # Check if it looks like a physical name (has timestamp suffix)
          next false unless ref_coll.match?(/_\d{8}_\d{6}_\d{3}$/)

          logical = ref_coll.sub(/_\d{8}_\d{6}_\d{3}$/, '')
          alias_target = begin
            client.resolve_alias(logical)
          rescue StandardError
            nil
          end

          if alias_target && !alias_target.to_s.strip.empty? && alias_target.to_s != ref_coll
            true
          else
            # Verify the referenced physical collection doesn't exist
            ref_schema = begin
              client.retrieve_collection_schema(ref_coll)
            rescue StandardError
              nil
            end
            ref_schema.nil?
          end
        end
      end

      # Determine whether a referencer schema needs a rebuild due to stale
      # references, missing collection, or detected schema drift.
      #
      # @param ref_klass [Class]
      # @param collection_name [String]
      # @param client [SearchEngine::Client]
      # @return [Boolean]
      def referencer_requires_schema_rebuild?(ref_klass, collection_name, client:)
        return true if referencer_has_stale_references?(collection_name, client: client)

        diff = SearchEngine::Schema.diff(ref_klass, client: client)[:diff] || {}
        stale_refs = Array(diff[:stale_references])
        return true if stale_refs.any?

        opts = (diff[:collection_options] || {}).to_h
        return true if opts[:live] == :missing

        added = Array(diff[:added_fields])
        removed = Array(diff[:removed_fields])
        changed = (diff[:changed_fields] || {}).to_h
        coll_opts = (diff[:collection_options] || {}).to_h

        added.any? || removed.any? || !changed.empty? || !coll_opts.empty?
      rescue StandardError
        false
      end

      # Force a full reindex of the referencer to rebuild its schema with valid
      # alias references. Suppresses cascade to avoid infinite recursion.
      #
      # @param ref_klass [Class]
      # @param logical [String]
      # @param physical [String, nil]
      # @return [Boolean]
      def reindex_referencer_with_fresh_schema!(ref_klass, logical, physical, client:, force_rebuild: false)
        coll_display = physical && physical != logical ? "#{logical} (physical: #{physical})" : logical
        action = force_rebuild ? 'force_rebuild index_collection' : 'index_collection'
        puts(%(  Referencer "#{coll_display}" — schema rebuild required, running #{action}))

        SearchEngine::Instrumentation.with_context(bulk_suppress_cascade: true) do
          ref_klass.index_collection(client: client, pre: :ensure, force_rebuild: force_rebuild)
        end
        true
      rescue StandardError => error
        puts(%(  Referencer "#{logical}" — schema rebuild failed: #{error.message}))
        false
      end

      def reindex_referencer_with_drop!(ref_klass, logical, physical)
        coll_display = physical && physical != logical ? "#{logical} (physical: #{physical})" : logical
        puts(%(  Referencer "#{coll_display}" — force reindex (drop+index)))

        SearchEngine::Instrumentation.with_context(bulk_suppress_cascade: true) do
          ref_klass.reindex_collection!
        end
        true
      rescue StandardError => error
        puts(%(  Referencer "#{logical}" — force reindex failed: #{error.message}))
        false
      end

      def post_partitions_to_pool!(pool, ctx, parts, ref_klass, mtx)
        parts.each do |p|
          pool.post do
            SearchEngine::Instrumentation.with_context(ctx) do
              summary = SearchEngine::Indexer.rebuild_partition!(ref_klass, partition: p, into: nil)
              mtx.synchronize { puts(SearchEngine::Logging::PartitionProgress.line(p, summary)) }
            end
          end
        end
      end

      def rebuild_partitions_sequential!(ref_klass, parts)
        executed = false
        parts.each do |p|
          summary = SearchEngine::Indexer.rebuild_partition!(ref_klass, partition: p, into: nil)
          puts(SearchEngine::Logging::PartitionProgress.line(p, summary))
          executed = true
        end
        executed
      end

      def build_from_typesense(client)
        graph = Hash.new { |h, k| h[k] = [] }
        # Use a slightly longer timeout for metadata requests to avoid noisy timeouts
        meta_timeout = begin
          t = SearchEngine.config.timeout_ms.to_i
          t = 5_000 if t <= 0
          t < 10_000 ? 10_000 : t
        rescue StandardError
          10_000
        end
        collections = Array(client.list_collections(timeout_ms: meta_timeout))
        names = collections.map { |c| (c[:name] || c['name']).to_s }.reject(&:empty?)
        names.each do |name|
          begin
            schema = client.retrieve_collection_schema(name, timeout_ms: meta_timeout)
          rescue StandardError
            schema = nil
          end
          next unless schema

          fields = Array(schema[:fields] || schema['fields'])
          fields.each do |f|
            ref = f[:reference] || f['reference']
            next if ref.nil? || ref.to_s.strip.empty?

            coll, fk = parse_reference(ref)
            next if coll.nil? || coll.empty?

            referrer_name = (schema[:name] || schema['name']).to_s
            referrer_logical = normalize_physical_to_logical(referrer_name)
            graph[coll] << { referrer: referrer_logical, local_key: (f[:name] || f['name']).to_s,
foreign_key: fk }
          end
        end
        graph
      rescue StandardError
        {}
      end

      def build_from_registry
        graph = Hash.new { |h, k| h[k] = [] }
        # Use models_map instead of Registry.mapping to ensure all models are discovered,
        # including those that may not be explicitly registered yet
        mapping = SearchEngine::CollectionResolver.models_map
        mapping.each do |coll_name, klass|
          compiled = SearchEngine::Schema.compile(klass)
          fields = Array(compiled[:fields])
          fields.each do |f|
            ref = f[:reference] || f['reference']
            next if ref.nil? || ref.to_s.strip.empty?

            target_coll, fk = parse_reference(ref)
            next if target_coll.nil? || target_coll.empty?

            graph[target_coll] << {
              referrer: coll_name.to_s,
              local_key: (f[:name] || f['name']).to_s,
              foreign_key: fk
            }
          end
        rescue StandardError
          # ignore individual compile errors for robustness
        end
        graph
      end

      def parse_reference(ref_value)
        s = ref_value.to_s
        parts = s.split('.', 2)
        coll = parts[0].to_s
        fk = parts[1]&.to_s
        [coll, fk]
      end

      # Convert a physical collection name like
      #   logical_YYYYMMDD_HHMMSS_###
      # back to its logical base name. If it doesn't match the pattern, return as-is.
      # @param name [String]
      # @return [String]
      def normalize_physical_to_logical(name)
        s = name.to_s
        m = s.match(/\A(.+)_\d{8}_\d{6}_\d{3}\z/)
        return s unless m

        base = m[1].to_s
        base.empty? ? s : base
      end

      def detect_immediate_cycles(graph)
        pairs = []
        # Avoid mutating the Hash while iterating: do not access graph[other] unless key exists
        graph.each do |target, edges|
          edges.each do |e|
            other = e[:referrer]
            next unless graph.key?(other)

            back_edges = graph[other]
            back = Array(back_edges).any? { |x| x[:referrer] == target }
            pairs << [target, other] if back
          end
        end
        pairs.uniq
      end

      def safe_collection_class(name)
        SearchEngine::CollectionResolver.model_for_logical(name)
      end

      def can_partial_reindex?(klass)
        # Disallow partial when a custom Partitioner is used
        return false if SearchEngine::Partitioner.for(klass)

        # Require ActiveRecord source adapter for partition Hash filtering support
        dsl = begin
          klass.instance_variable_defined?(:@__mapper_dsl__) ? klass.instance_variable_get(:@__mapper_dsl__) : nil
        rescue StandardError
          nil
        end
        return false unless dsl.is_a?(Hash)

        src = dsl[:source]
        src && src[:type].to_s == 'active_record'
      end

      def ensure_source_reference_fields!(source_klass, logical_name, referencers, client:, alias_cache:)
        return unless source_klass

        required_fields = referencers.map { |edge| edge[:foreign_key].to_s }.reject(&:empty?).uniq
        return if required_fields.empty?

        physical =
          resolve_physical_collection_name(logical_name, client: client, cache: alias_cache) || logical_name.to_s
        schema = begin
          client.retrieve_collection_schema(physical)
        rescue StandardError
          nil
        end
        return unless schema

        live_fields =
          Array(schema[:fields] || schema['fields']).map { |f| (f[:name] || f['name']).to_s }
        missing = required_fields - live_fields
        return if missing.empty?

        SearchEngine::Instrumentation.with_context(bulk_suppress_cascade: true) do
          source_klass.index_collection(pre: :ensure)
        end
      end
    end
  end
end
