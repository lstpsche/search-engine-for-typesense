# frozen_string_literal: true

module SearchEngine
  module Sources
    # Adapter that delegates batch enumeration to a provided callable.
    #
    # The callable is expected to implement `call(cursor:, partition:)` and return either
    # an Enumerator or yield arrays of rows. Shapes are application-defined.
    #
    # @example
    #   src = SearchEngine::Sources::LambdaSource.new(->(cursor:, partition:) { [[row1, row2]] })
    #   src.each_batch { |rows| ... }
    #
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer`
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
        begin
          enum = @callable.call(cursor: cursor, partition: partition)
          Array(enum).each do |rows|
            duration = monotonic_ms - started
            instrument_batch_fetched(source: 'lambda', batch_index: nil, rows_count: Array(rows).size,
                                     duration_ms: duration, partition: partition, cursor: cursor,
                                     adapter_options: { callable: @callable.class.name }
            )
            yield(rows)
            started = monotonic_ms
          end
        rescue StandardError => error
          instrument_error(source: 'lambda', error: error, partition: partition, cursor: cursor,
                           adapter_options: { callable: @callable.class.name }
          )
          raise
        end
      end
    end
  end
end
