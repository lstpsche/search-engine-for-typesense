# frozen_string_literal: true

require 'search_engine/cli/support'

module SearchEngine
  # Internal helpers for operator-facing CLI/Rake tasks.
  #
  # This module is intentionally minimal and only used by task definitions
  # located under `lib/tasks/search_engine.rake`. It avoids changing the
  # library require graph by being required from the Rake file.
  module Cli
    class << self
      # Resolve a collection argument into a model class.
      #
      # Attempts to constantize when the input looks like a class name; falls back
      # to the registry via {SearchEngine.collection_for} for logical identifiers.
      #
      # @param arg [#to_s] collection argument (e.g., "SearchEngine::Product" or "products")
      # @return [Class] subclass of {SearchEngine::Base}
      # @raise [ArgumentError] when resolution fails
      def resolve_collection!(arg)
        input = arg.to_s
        raise ArgumentError, 'collection argument required' if input.strip.empty?

        klass = try_constantize(input)
        klass ||= safe_collection_for(input)

        unless klass.ancestors.include?(SearchEngine::Base)
          raise ArgumentError, "#{klass.name} must inherit from SearchEngine::Base"
        end

        klass
      end

      # Parse a partition argument into a typed value.
      #
      # Numeric strings are converted to Integer; blank values return nil.
      # Any other value is returned as-is (String).
      #
      # @param arg [Object]
      # @return [Integer, String, nil]
      def parse_partition(arg)
        return nil if arg.nil?

        str = arg.to_s
        return nil if str.strip.empty? || str.strip.casecmp('nil').zero?

        return Integer(str) if str.match?(/\A-?\d+\z/)

        str
      rescue ArgumentError
        str
      end

      # Run a task with small, structured instrumentation.
      #
      # Emits three events when ActiveSupport::Notifications is available:
      # - `search_engine.cli.started`
      # - `search_engine.cli.finished`
      # - `search_engine.cli.error`
      #
      # @param task [String] task name (e.g., "schema:diff")
      # @param payload [Hash] small JSON-safe payload (e.g., { collection: "products" })
      # @yield executes the task body
      # @return [Object] the block's return value
      def with_task_instrumentation(task, payload = {})
        started = monotonic_ms
        instrument('search_engine.cli.started', payload.merge(task: task))
        result = yield
        duration = (monotonic_ms - started).round(1)
        instrument('search_engine.cli.finished', payload.merge(task: task, duration_ms: duration, status: 'ok'))
        result
      rescue StandardError => error
        duration = (monotonic_ms - started).round(1)
        instrument(
          'search_engine.cli.error',
          payload.merge(task: task, duration_ms: duration, status: 'error', error_class: error.class.name,
                        message_truncated: error.message.to_s[0, 200]
          )
        )
        raise
      end

      # Resolve the effective dispatch mode from ENV override or config.
      # @param env_value [String, Symbol, nil]
      # @return [Symbol] :inline or :active_job (falls back to :inline if AJ unavailable)
      def resolve_dispatch_mode(env_value)
        val = (env_value || ENV['DISPATCH'] || SearchEngine.config.indexer.dispatch || :inline).to_s.downcase
        case val
        when 'active_job', 'activejob', 'aj'
          return :active_job if defined?(::ActiveJob::Base)
        end
        :inline
      end

      # Return true when ENV[name] is a truthy flag (1/true/yes/on).
      # @param name [String]
      # @return [Boolean]
      def boolean_env?(name)
        SearchEngine::Cli::Support.boolean_env?(name)
      end

      # Whether JSON output is requested via FORMAT=json.
      # @return [Boolean]
      def json_output?
        SearchEngine::Cli::Support.json_output?
      end

      # Build an Enumerator that yields a single mapped documents batch for dry-run preview.
      #
      # @param klass [Class]
      # @param partition [Object, nil]
      # @return [Enumerable<Array<Hash>>>]
      # @raise [ArgumentError] when mapper/source is missing
      def docs_enum_for_first_batch(klass, partition)
        mapper = SearchEngine::Mapper.for(klass)
        raise ArgumentError, "mapper is not defined for #{klass.name}" unless mapper

        rows_enum = rows_enumerator_for(klass, partition)
        first_rows = next_from_enum(rows_enum)
        first_rows ||= []

        docs, _report = mapper.map_batch!(first_rows, batch_index: 0)
        [docs]
      end

      # Resolve the physical collection name to import into, mirroring Indexer semantics.
      #
      # @param klass [Class]
      # @param partition [Object, nil]
      # @param into [String, nil]
      # @return [String]
      def resolve_into!(klass, partition: nil, into: nil)
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

        # Fallback to logical name (alias)
        if klass.respond_to?(:collection)
          klass.collection.to_s
        else
          klass.name.to_s
        end
      end

      # Enumerate partitions if DSL is present; otherwise a single nil partition.
      # @param klass [Class]
      # @return [Enumerable]
      def partitions_for(klass)
        compiled = SearchEngine::Partitioner.for(klass)
        return [nil] unless compiled

        compiled.partitions
      end

      private

      def try_constantize(input)
        return nil unless looks_like_constant?(input)

        Object.const_get(input)
      rescue NameError
        nil
      end

      def looks_like_constant?(str)
        str.include?('::') || str[0] =~ /[A-Z]/
      end

      def safe_collection_for(name)
        SearchEngine.collection_for(name)
      rescue ArgumentError
        raise ArgumentError,
              "Unknown collection '#{name}'. Provide a fully qualified class name or a registered collection."
      end

      def rows_enumerator_for(klass, partition)
        compiled_partitioner = SearchEngine::Partitioner.for(klass)
        return compiled_partitioner.partition_fetch_enum(partition) if compiled_partitioner

        dsl = klass.instance_variable_get(:@__mapper_dsl__) if klass.instance_variable_defined?(:@__mapper_dsl__)
        source_def = dsl && dsl[:source]
        unless source_def
          raise ArgumentError, 'No partition_fetch defined and no source adapter provided. Define one in the DSL.'
        end

        adapter = SearchEngine::Sources.build(source_def[:type], **(source_def[:options] || {}), &source_def[:block])
        adapter.each_batch(partition: partition)
      end

      def next_from_enum(enum)
        return enum.first if enum.respond_to?(:first)

        e = enum.to_enum
        e.next
      rescue StopIteration
        nil
      end

      def instrument(event, payload)
        SearchEngine::Instrumentation.instrument(event, payload) {}
      end

      def monotonic_ms
        SearchEngine::Instrumentation.monotonic_ms
      end
    end
  end
end
