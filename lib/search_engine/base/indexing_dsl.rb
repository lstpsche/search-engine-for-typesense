# frozen_string_literal: true

require 'active_support/concern'

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

        # Define a stale filter builder for delete-by-filter operations.
        # @yieldparam partition [Object, nil]
        # @yieldreturn [String, nil]
        # @return [void]
        def stale_filter_by(&block)
          raise ArgumentError, 'stale_filter_by requires a block' unless block

          instance_variable_set(:@__stale_filter_proc__, block)
          nil
        end
      end
    end
  end
end
