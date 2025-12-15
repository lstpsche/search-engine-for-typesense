# frozen_string_literal: true

module SearchEngine
  # Compiles and validates partitioning directives captured by the index DSL.
  #
  # Provides an immutable object with callables for:
  # - partitions -> Enumerable of partition keys
  # - partition_fetch(partition) -> Enumerable of batches (Arrays of records)
  # - before_hook(partition)
  # - after_hook(partition)
  class Partitioner
    # Immutable compiled holder
    class Compiled
      attr_reader :klass, :partitions_proc, :partition_fetch_proc, :before_hook_proc, :after_hook_proc, :max_parallel

      def initialize(klass:, partitions_proc:, partition_fetch_proc:, before_hook_proc:, after_hook_proc:,
                     max_parallel: 1)
        @klass = klass
        @partitions_proc = partitions_proc
        @partition_fetch_proc = partition_fetch_proc
        validate_hook_arity!(before_hook_proc, name: 'before_partition') if before_hook_proc
        validate_hook_arity!(after_hook_proc, name: 'after_partition') if after_hook_proc
        @before_hook_proc = before_hook_proc
        @after_hook_proc = after_hook_proc
        mp = begin
          Integer(max_parallel)
        rescue StandardError
          1
        end
        @max_parallel = mp.positive? ? mp : 1
        freeze
      end

      # Enumerate partition keys. Validates the return value shape.
      # @return [Enumerable] list/Enumerable of opaque partition tokens
      # @raise [SearchEngine::Errors::InvalidParams] when the block does not return an Enumerable
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer#partitioning
      def partitions
        return [] unless @partitions_proc

        res = @partitions_proc.call
        unless res.respond_to?(:each)
          raise SearchEngine::Errors::InvalidParams,
                'partitions block must return an Enumerable of partition keys (Array acceptable). ' \
                'See https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer#partitioning.'
        end
        res
      end

      # Return an Enumerator for batches for the given partition, validating element shape.
      # @param partition [Object]
      # @return [Enumerable<Array>] enumerator yielding Arrays of records
      # @raise [ArgumentError] when partition_fetch is not defined
      # @raise [SearchEngine::Errors::InvalidParams] when the block returns a non-enumerable or yields non-arrays
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer#partitioning
      def partition_fetch_enum(partition)
        raise ArgumentError, 'partition_fetch not defined' unless @partition_fetch_proc

        enum = @partition_fetch_proc.call(partition)
        unless enum.respond_to?(:each)
          raise SearchEngine::Errors::InvalidParams,
                'partition_fetch must return an Enumerable yielding Arrays of records. ' \
                'See https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer#partitioning.'
        end

        Enumerator.new do |y|
          idx = 0
          enum.each do |batch|
            unless batch.is_a?(Array) || batch.respond_to?(:to_a)
              raise SearchEngine::Errors::InvalidParams,
                    "partition_fetch must yield Arrays of records; got #{batch.class} at index #{idx}."
            end
            y << (batch.is_a?(Array) ? batch : batch.to_a)
            idx += 1
          end
        end
      end

      private

      def validate_hook_arity!(proc_obj, name:)
        ar = proc_obj.arity
        return if ar == 1 || ar.negative?

        raise SearchEngine::Errors::InvalidParams, "#{name} block must accept exactly 1 parameter (partition)."
      end
    end

    class << self
      # Resolve a compiled partitioner for a model class, or nil if directives are absent.
      # @param klass [Class]
      # @return [SearchEngine::Partitioner::Compiled, nil]
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer#partitioning
      def for(klass)
        dsl = mapper_dsl_for(klass)
        return nil unless dsl

        any = dsl[:partitions] || dsl[:partition_fetch] || dsl[:before_partition] || dsl[:after_partition]
        return nil unless any

        cache[klass] ||= compile(klass, dsl)
      end

      private

      def cache
        @cache ||= {}
      end

      def compile(klass, dsl)
        Compiled.new(
          klass: klass,
          partitions_proc: dsl[:partitions],
          partition_fetch_proc: dsl[:partition_fetch],
          before_hook_proc: dsl[:before_partition],
          after_hook_proc: dsl[:after_partition],
          max_parallel: dsl[:partition_max_parallel]
        )
      end

      def mapper_dsl_for(klass)
        return unless klass.instance_variable_defined?(:@__mapper_dsl__)

        klass.instance_variable_get(:@__mapper_dsl__)
      end
    end
  end
end
