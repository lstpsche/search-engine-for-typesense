# frozen_string_literal: true

module SearchEngine
  module Sources
    # Adapter that delegates batch enumeration to a provided callable.
    #
    # The callable is expected to implement `call(cursor:, partition:)` and use one mode:
    # - return mode: return an Enumerable of batches
    # - yield mode: yield batches via the provided block argument
    #
    # Returning batch-like data (Array/Enumerator) while also yielding is treated as
    # an ambiguous mixed mode and raises an error.
    # Shapes are application-defined.
    #
    # @example
    #   src = SearchEngine::Sources::LambdaSource.new(->(cursor:, partition:) { [[row1, row2]] })
    #   src.each_batch { |rows| ... }
    #
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer`
    class LambdaSource
      include Base

      # @param callable [#call] object responding to call(cursor:, partition:)
      # @raise [ArgumentError] when callable does not respond to :call
      def initialize(callable)
        raise ArgumentError, 'callable must respond to :call(cursor:, partition:)' unless callable.respond_to?(:call)

        @callable = callable
      end

      # Enumerate batches produced by the callable.
      # @param partition [Object, nil]
      # @param cursor [Object, nil]
      # @yieldparam rows [Array]
      # @return [Enumerator]
      def each_batch(partition: nil, cursor: nil)
        return enum_for(:each_batch, partition: partition, cursor: cursor) unless block_given?

        started = monotonic_ms
        consume_rows = lambda do |rows|
          duration = monotonic_ms - started
          instrument_batch_fetched(source: 'lambda', batch_index: nil, rows_count: Array(rows).size,
                                   duration_ms: duration, partition: partition, cursor: cursor,
                                   adapter_options: { callable: @callable.class.name }
          )
          yield(rows)
          started = monotonic_ms
          nil
        end

        begin
          yielded = false
          returned = @callable.call(cursor: cursor, partition: partition) do |rows|
            yielded = true
            consume_rows.call(rows)
          end

          if yielded
            if mixed_mode_batch_return?(returned)
              raise SearchEngine::Errors::InvalidParams,
                    'lambda source callable must either yield batches or return an Enumerable of batches, not both'
            end
          else
            to_iterate = returned.respond_to?(:each) ? returned : Array(returned)
            to_iterate.each { |rows| consume_rows.call(rows) }
          end
        rescue StandardError => error
          instrument_error(source: 'lambda', error: error, partition: partition, cursor: cursor,
                           adapter_options: { callable: @callable.class.name }
          )
          raise
        end
      end

      private

      def mixed_mode_batch_return?(returned)
        returned.is_a?(Array) || returned.is_a?(Enumerator)
      end
    end
  end
end
