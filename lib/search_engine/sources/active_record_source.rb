# frozen_string_literal: true

module SearchEngine
  module Sources
    # ActiveRecord-backed source adapter that yields arrays of records in batches.
    #
    # Uses ORM batch APIs (`in_batches`/`find_in_batches`) honoring batch_size and an
    # optional scope proc. Ensures stable memory by disabling query cache and using
    # readonly relations. Does not accumulate results across batches.
    #
    # @example
    #   src = SearchEngine::Sources::ActiveRecordSource.new(model: ::Product, scope: -> { where(active: true) }, batch_size: 2000)
    #   src.each_batch(partition: nil, cursor: nil) { |rows| ... }
    #
    # @note Emits "search_engine.source.batch_fetched" and "search_engine.source.error".
    class ActiveRecordSource
      include Base

      # @param model [Class] ActiveRecord model class
      # @param scope [Proc, nil] optional proc evaluated in the context of model.all (returns a Relation)
      # @param batch_size [Integer, nil] override batch size (defaults from config)
      # @param use_transaction [Boolean, nil] wrap in read-only transaction (best-effort)
      # @param readonly [Boolean, nil] mark relations readonly (default true)
      def initialize(model:, scope: nil, batch_size: nil, use_transaction: nil, readonly: nil)
        @model = model
        @scope_proc = scope
        cfg = SearchEngine.config.sources.active_record
        @batch_size = (batch_size || cfg.batch_size).to_i
        @use_transaction = use_transaction.nil? ? truthy?(cfg.use_transaction) : truthy?(use_transaction)
        @readonly = !readonly.nil? ? truthy?(readonly) : truthy?(cfg.readonly)
      end

      # Iterate over batches of records.
      #
      # @param partition [Object, nil] optional opaque partition (e.g., id range)
      # @param cursor [Object, nil] optional opaque cursor (e.g., last id)
      # @yieldparam rows [Array<Object>] array of model instances
      # @return [Enumerator] when no block is given
      def each_batch(partition: nil, cursor: nil, &block)
        return enum_for(:each_batch, partition: partition, cursor: cursor) unless block_given?

        relation = base_relation
        relation = apply_partition(relation, partition) if partition
        relation = apply_cursor(relation, cursor) if cursor

        idx = 0
        started = monotonic_ms
        run_with_connection do
          run_in_readonly_txn_if_needed do
            without_query_cache do
              dispatch_batches(relation, started, idx, partition, cursor, &block)
            end
          end
        end
      rescue StandardError => error
        instrument_error(source: 'active_record', error: error, partition: partition, cursor: cursor,
                         adapter_options: { batch_size: @batch_size }
        )
        raise
      end

      private

      def truthy?(val)
        val == true
      end

      def dispatch_batches(relation, started, idx, partition, cursor)
        if relation.respond_to?(:in_batches)
          relation.in_batches(of: @batch_size) do |batch_scope|
            batch_scope = mark_readonly(batch_scope)
            rows = batch_scope.to_a
            duration = monotonic_ms - started
            instrument_batch_fetched(source: 'active_record', batch_index: idx, rows_count: rows.size,
                                     duration_ms: duration, partition: partition, cursor: cursor,
                                     adapter_options: { batch_size: @batch_size }
            )
            yield(rows)
            idx += 1
            started = monotonic_ms
          end
        elsif relation.respond_to?(:find_in_batches)
          relation.find_in_batches(batch_size: @batch_size) do |rows|
            rows = rows.map { |r| r }
            duration = monotonic_ms - started
            instrument_batch_fetched(source: 'active_record', batch_index: idx, rows_count: rows.size,
                                     duration_ms: duration, partition: partition, cursor: cursor,
                                     adapter_options: { batch_size: @batch_size }
            )
            yield(rows)
            idx += 1
            started = monotonic_ms
          end
        else
          # Last resort: materialize and slice
          records = relation.to_a
          records.each_slice(@batch_size) do |rows|
            duration = monotonic_ms - started
            instrument_batch_fetched(source: 'active_record', batch_index: idx, rows_count: rows.size,
                                     duration_ms: duration, partition: partition, cursor: cursor,
                                     adapter_options: { batch_size: @batch_size }
            )
            yield(rows)
            idx += 1
            started = monotonic_ms
          end
        end
      end

      def base_relation
        rel = @model.all
        rel = rel.instance_exec(&@scope_proc) if @scope_proc.respond_to?(:call)
        mark_readonly(rel)
      end

      def mark_readonly(rel)
        if @readonly && rel.respond_to?(:readonly)
          rel.readonly(true)
        else
          rel
        end
      end

      def apply_partition(rel, partition)
        case partition
        when Range
          pk = rel.klass.primary_key
          rel.where(pk => partition)
        when Hash
          rel.where(partition)
        else
          rel
        end
      end

      def apply_cursor(rel, cursor)
        return rel unless cursor

        pk = rel.klass.primary_key
        if cursor.is_a?(Hash)
          rel.where(cursor)
        else
          rel.where(arel_table(rel)[pk].gt(cursor)).order(pk => :asc)
        end
      end

      def arel_table(rel)
        rel.klass.arel_table
      end

      def run_with_connection(&block)
        if defined?(ActiveRecord::Base)
          ActiveRecord::Base.connection_pool.with_connection(&block)
        else
          yield
        end
      end

      def run_in_readonly_txn_if_needed
        if @use_transaction && defined?(ActiveRecord::Base)
          ActiveRecord::Base.connection.transaction(requires_new: true) do
            if ActiveRecord::Base.connection.respond_to?(:execute)
              begin
                ActiveRecord::Base.connection.execute('SET TRANSACTION READ ONLY')
              rescue StandardError
                # best-effort; ignore if not supported
              end
            end
            yield
          end
        else
          yield
        end
      end

      def without_query_cache(&block)
        if defined?(ActiveRecord::Base) && ActiveRecord::Base.respond_to?(:connection)
          ActiveRecord::Base.connection.uncached(&block)
        else
          yield
        end
      end
    end
  end
end
