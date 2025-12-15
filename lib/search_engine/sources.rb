# frozen_string_literal: true

module SearchEngine
  # Factory and DSL for building data source adapters that yield batches.
  #
  # Usage via symbol and options:
  #   SearchEngine::Sources.build(:active_record, model: ::Product, scope: -> { where(active: true) }, batch_size: 2000)
  #   SearchEngine::Sources.build(:sql, sql: "SELECT * FROM products WHERE active = TRUE", fetch_size: 2000)
  #
  # Usage via block (lambda source):
  #   SearchEngine::Sources.build(:lambda) do |cursor:, partition:|
  #     Enumerator.new { |y| external_api.each_page(cursor) { |rows| y << rows } }
  #   end
  #
  # All adapters implement `each_batch(partition:, cursor:)` and return an Enumerator
  # when no block is provided.
  module Sources
    # Build a source adapter from a symbol and options or from a block.
    #
    # @param type [Symbol] :active_record, :sql, or :lambda
    # @param options [Hash] adapter-specific options
    # @yield for :lambda sources, a block taking (cursor:, partition:) and returning an Enumerator
    # @return [Object] adapter responding to `each_batch(partition:, cursor:)`
    def self.build(type, **options, &block)
      case type.to_sym
      when :active_record
        model = options[:model]
        unless model.is_a?(Class)
          raise SearchEngine::Errors::InvalidParams,
                'active_record source requires :model (ActiveRecord class). See ' \
                'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer.'
        end

        scope = options[:scope]
        batch_size = options[:batch_size]
        readonly = options[:readonly]
        use_txn = options[:use_transaction]
        ActiveRecordSource.new(model: model, scope: scope, batch_size: batch_size, use_transaction: use_txn,
                               readonly: readonly
        )
      when :sql
        sql = options[:sql]
        unless sql.is_a?(String) && !sql.strip.empty?
          raise SearchEngine::Errors::InvalidParams,
                'sql source requires :sql (String). See ' \
                'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer.'
        end

        binds = options[:binds]
        fetch_size = options[:fetch_size]
        row_shape = options[:row_shape]
        stmt_timeout = options[:statement_timeout_ms]
        SqlSource.new(sql: sql, binds: binds, fetch_size: fetch_size, row_shape: row_shape,
                      statement_timeout_ms: stmt_timeout
        )
      when :lambda
        callable = block || options[:callable]
        unless callable
          raise SearchEngine::Errors::InvalidParams,
                'lambda source requires a block or :callable. See ' \
                'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer.'
        end

        LambdaSource.new(callable)
      else
        raise SearchEngine::Errors::InvalidParams,
              "unknown source type: #{type.inspect}. Supported: :active_record, :sql, :lambda"
      end
    end
  end
end
