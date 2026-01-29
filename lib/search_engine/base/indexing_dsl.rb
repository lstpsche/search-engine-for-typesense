# frozen_string_literal: true

require 'active_support/concern'
require 'active_support/core_ext/hash/indifferent_access'

module SearchEngine
  class Base
    # Indexing DSL: define index mapping, identity computation and stale filter.
    module IndexingDsl
      extend ActiveSupport::Concern

      class_methods do
        # Define collection-level indexing configuration and mapping.
        # @yieldparam dsl [SearchEngine::Mapper::Dsl]
        # @return [void]
        def index(&block)
          raise ArgumentError, 'index requires a block' unless block

          dsl = SearchEngine::Mapper::Dsl.new(self)
          # Support both styles:
          # - index { source :active_record, ...; map { ... } }
          # - index { |dsl| dsl.source :active_record, ...; dsl.map { ... } }
          if block.arity == 1
            yield dsl
          else
            dsl.instance_eval(&block)
          end

          definition = dsl.to_definition
          unless definition[:map].respond_to?(:call)
            raise ArgumentError, 'index requires a map { |record| ... } block returning a document'
          end

          # Store definition on the class; Mapper.for will compile and cache
          instance_variable_set(:@__mapper_dsl__, definition)
          instance_variable_set(:@__stale_entries__, Array(definition[:stale]))
          nil
        end

        # Configure how to compute the Typesense document id for this collection.
        # @param strategy [Symbol, String, Proc]
        # @yield [record]
        # @return [Class]
        def identify_by(strategy = nil, &block)
          callable = if block_given?
                       block
                     elsif strategy.is_a?(Proc)
                       if strategy.arity != 1 && strategy.arity != -1
                         raise SearchEngine::Errors::InvalidOption,
                               'identify_by Proc/Lambda must accept exactly 1 argument (record)'
                       end

                       strategy
                     elsif strategy.is_a?(Symbol) || strategy.is_a?(String)
                       meth = strategy.to_s
                       ->(record) { record.public_send(meth) }
                     else
                       raise SearchEngine::Errors::InvalidOption,
                             'identify_by expects a Symbol/String method name or a Proc/Lambda (or block)'
                     end

          # Normalize to a proc that always returns String
          @identify_by_proc = lambda do |record|
            val = callable.call(record)
            val.is_a?(String) ? val : val.to_s
          end

          # Persist minimal metadata about the raw strategy to inform type hints
          if strategy.is_a?(Symbol) || strategy.is_a?(String)
            @__identify_by_kind__ = :symbol
            @__identify_by_symbol__ = strategy.to_sym
          else
            @__identify_by_kind__ = :proc
            @__identify_by_symbol__ = nil
          end
          self
        end
      end

      class_methods do
        # Build mapped data for given input using the model's declared source and mapping.
        #
        # Accepts input corresponding to the configured source:
        # - When source is :active_record, accepts an instance of the configured AR model
        #   or an Array of such instances. Output preserves input shape.
        # - When source is :sql, accepts a SQL String, executes it to fetch rows, and
        #   always returns an Array of results (even when a single row is returned).
        #
        # The mapping is compiled from the model's `index` DSL and validated against
        # the local schema. No Typesense calls are made.
        #
        # @param data [Object] source input (AR instance/Array for :active_record; SQL String for :sql)
        # @param mode [Symbol] output mode; :instance returns hydrated model instances,
        #   :hash returns HashWithIndifferentAccess documents
        # @return [Object] a single instance/hash or an Array, per shape policy
        # @raise [SearchEngine::Errors::InvalidOption] when mode is unknown or source unsupported
        # @raise [SearchEngine::Errors::InvalidParams] when inputs are invalid or mapper/source missing
        #
        # @example ActiveRecord source -> instance
        #   SearchEngine::Product.from(Product.first)
        #   # => #<SearchEngine::Product ...>
        #
        # @example ActiveRecord source -> array of instances
        #   SearchEngine::Product.from([Product.first, Product.second])
        #   # => [#<SearchEngine::Product ...>, #<SearchEngine::Product ...>]
        #
        # @example SQL source -> array of instances
        #   SearchEngine::Product.from("SELECT id, name FROM products", mode: :instance)
        #   # => [#<SearchEngine::Product ...>, ...]
        #
        # @example Hash mode with indifferent access
        #   SearchEngine::Product.from(Product.first, mode: :hash) # => {"id"=>..., :id=>..., ...}
        def from(data, mode: :instance)
          unless %i[instance hash].include?(mode)
            raise SearchEngine::Errors::InvalidOption,
                  "Invalid mode: #{mode.inspect}. Allowed: :instance or :hash"
          end

          mapper = __se_resolve_mapper_for_from!

          # Resolve source definition captured by DSL
          source_def = __se_resolve_source_for_from!

          type = source_def[:type].to_sym
          rows = []
          output_shape = :array

          case type
          when :active_record
            model = source_def[:options] && source_def[:options][:model]
            rows, output_shape = __se_normalize_rows_for_active_record!(model, data)
          when :sql
            rows = __se_fetch_rows_for_sql!(data)
            output_shape = :array
          else
            raise SearchEngine::Errors::InvalidOption,
                  "Unsupported source type: #{type.inspect}. Supported: :active_record, :sql"
          end

          docs, = mapper.map_batch!(rows, batch_index: 0)

          case mode
          when :instance
            instances = docs.map { |doc| from_document(doc) }
            return instances.first if output_shape == :single

            instances
          when :hash
            hashes = docs.map(&:with_indifferent_access)
            return hashes.first if output_shape == :single

            hashes
          end
        end

        # -- helpers (class methods) -------------------------------------------------

        # @return [SearchEngine::Mapper::Compiled]
        def __se_resolve_mapper_for_from!
          mapper = SearchEngine::Mapper.for(self)
          return mapper if mapper

          raise SearchEngine::Errors::InvalidParams,
                "mapper is not defined for #{name}. Define it via `index do ... map { ... } end`."
        end

        # @return [Hash]
        def __se_resolve_source_for_from!
          dsl_def = instance_variable_get(:@__mapper_dsl__) if instance_variable_defined?(:@__mapper_dsl__)
          source_def = dsl_def && dsl_def[:source]
          return source_def if source_def && source_def[:type]

          raise SearchEngine::Errors::InvalidParams,
                "source is not defined for #{name}. Define it via `index { source :active_record, ... }` or `:sql`."
        end

        # @param model [Class]
        # @param data [Object]
        # @return [Array<Array, Symbol>] [rows, shape]
        def __se_normalize_rows_for_active_record!(model, data)
          unless model.is_a?(Class)
            raise SearchEngine::Errors::InvalidParams,
                  'ActiveRecord source requires a :model class in index DSL'
          end

          if data.is_a?(Array)
            unless data.all? { |r| r.is_a?(model) }
              raise SearchEngine::Errors::InvalidParams,
                    "All elements must be instances of #{model.name} for :active_record source"
            end
            [data, :array]
          else
            unless data.is_a?(model)
              raise SearchEngine::Errors::InvalidParams,
                    "Expected instance of #{model.name} for :active_record source"
            end
            [[data], :single]
          end
        end

        # @param sql [String]
        # @return [Array<Hash>]
        def __se_fetch_rows_for_sql!(sql)
          unless sql.is_a?(String) && !sql.strip.empty?
            raise SearchEngine::Errors::InvalidParams,
                  'SQL input must be a non-empty String for :sql source'
          end
          src = SearchEngine::Sources::SqlSource.new(sql: sql)
          rows = []
          src.each_batch(partition: nil, cursor: nil) { |batch| rows.concat(batch) }
          rows
        end

        # Compute the Typesense document id for a given source record using the configured
        # identity strategy (or the default +record.id.to_s+ when unset).
        # @param record [Object]
        # @return [String]
        def compute_document_id(record)
          val =
            if instance_variable_defined?(:@identify_by_proc) && (proc = @identify_by_proc)
              proc.call(record)
            else
              record.respond_to?(:id) ? record.id : nil
            end
          val.is_a?(String) ? val : val.to_s
        end

        # Map a single record using the compiled mapper for this collection.
        # Returns the normalized document as it would be imported during indexation.
        # @param record [Object]
        # @return [Hash]
        # @raise [SearchEngine::Errors::InvalidParams] when mapper is missing or record is nil
        def mapped_data_for(record)
          raise SearchEngine::Errors::InvalidParams, 'record must be provided' if record.nil?

          mapper = SearchEngine::Mapper.for(self)
          unless mapper
            raise SearchEngine::Errors::InvalidParams,
                  "mapper is not defined for #{name}. Define it via `index do ... map { ... } end`."
          end

          docs, = mapper.map_batch!([record], batch_index: 0)
          docs.first
        end

        # Return frozen stale cleanup entries defined on this model.
        # @return [Array<Hash>]
        def stale_entries
          list = instance_variable_defined?(:@__stale_entries__) ? @__stale_entries__ : []
          list.dup.freeze
        end
      end
    end
  end
end
