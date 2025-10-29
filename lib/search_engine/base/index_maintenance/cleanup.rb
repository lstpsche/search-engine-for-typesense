# frozen_string_literal: true

require 'active_support/concern'

module SearchEngine
  class Base
    module IndexMaintenance
      # Cleanup-related helpers for the SearchEngine DSL.
      module Cleanup
        extend ActiveSupport::Concern

        class_methods do
          # Return a chainable Relation of stale documents compiled from `stale` rules.
          #
          # Compiles all `stale` entries declared in the indexing DSL for the
          # given partition and returns a Relation for the merged filter (OR semantics).
          # When no rules are present or the effective filter resolves to blank,
          # returns an empty relation.
          #
          # @param partition [Object, nil] optional partition token passed to the filter block
          # @return [SearchEngine::Relation]
          def stale(partition: nil)
            filters = SearchEngine::StaleRules.compile_filters(self, partition: partition)
            merged = SearchEngine::StaleRules.merge_filters(filters)

            if merged.nil? || merged.to_s.strip.empty?
              # Impossible predicate to ensure an empty Relation when there are no stale rules
              return all.where('id:=null && id:!=null')
            end

            all.where(merged)
          end

          # Delete stale documents from the collection according to DSL rules.
          #
          # Evaluates all stale definitions declared via the indexing DSL and
          # `stale_filter_by`, building a filter that deletes matching documents
          # using {SearchEngine::Deletion.delete_by}. When no stale configuration
          # is present, the method logs a skip message and returns 0.
          #
          # @param into [String, nil] optional physical collection override
          # @param partition [Object, nil] optional partition token forwarded to resolvers
          # @return [Integer] number of deleted documents
          def cleanup(into: nil, partition: nil)
            logical = respond_to?(:collection) ? collection.to_s : name.to_s
            puts
            puts(%(>>>>>> Cleanup Collection "#{logical}"))

            filters = SearchEngine::StaleRules.compile_filters(self, partition: partition)
            filters.compact!
            filters.reject! { |f| f.to_s.strip.empty? }
            if filters.empty?
              puts('Cleanup — skip (no stale configuration)')
              return 0
            end

            merged_filter = SearchEngine::StaleRules.merge_filters(filters)
            puts("Cleanup — filter=#{merged_filter.inspect}")

            deleted = SearchEngine::Deletion.delete_by(
              klass: self,
              filter: merged_filter,
              into: into,
              partition: partition
            )

            puts("Cleanup — deleted=#{deleted}")
            deleted
          rescue StandardError => error
            warn(
              "Cleanup — error=#{error.class}: #{error.message.to_s[0, 200]}"
            )
            0
          ensure
            puts(%(>>>>>> Cleanup Done))
          end

          private

          def build_scope_filters(entries, partition: nil)
            filters = entries
                      .select { |entry| entry[:type] == :scope }
                      .map do |entry|
                        scope = entry[:name]
                        next unless respond_to?(scope)

                        rel = invoke_scope(scope, partition)
                        next unless rel.is_a?(SearchEngine::Relation)

                        rel.filter_params
                      end
            filters.compact
          rescue StandardError
            []
          end

          def build_attribute_filters(entries)
            filters = entries
                      .select { |entry| entry[:type] == :attribute }
                      .map do |entry|
                        attr = entry[:name]
                        val = entry[:value]
                        relation_for({ attr => val })&.filter_params
                      end
            filters.compact
          rescue StandardError
            []
          end

          def build_hash_filters(entries)
            filters = entries
                      .select { |entry| entry[:type] == :hash }
                      .map { |entry| relation_for(entry[:hash])&.filter_params }
            filters.compact
          rescue StandardError
            []
          end

          def build_raw_filters(entries, partition: nil)
            raw = entries.select { |entry| %i[filter relation block].include?(entry[:type]) }

            filters = raw.flat_map do |entry|
              case entry[:type]
              when :filter then entry[:value]
              when :relation then entry[:relation]&.filter_params
              when :block
                evaluate_block_entry(entry[:block], partition: partition)
              end
            end
            Array(filters).compact
          rescue StandardError
            []
          end

          def merge_filters(filters)
            return filters.first if filters.size == 1

            fragments = filters.map do |filter|
              next if filter.to_s.strip.empty?

              "(#{filter})"
            end.compact

            fragments.join(' || ')
          end

          def relation_for(hash)
            SearchEngine::Relation.new(self).where(hash)
          end

          def evaluate_block_entry(block, partition: nil)
            params = block.parameters
            result = if params.any? { |(kind, name)| %i[key keyreq].include?(kind) && name == :partition }
                       instance_exec(partition: partition, &block)
                     elsif block.arity.positive?
                       instance_exec(partition, &block)
                     else
                       instance_exec(&block)
                     end

            case result
            when String then result
            when Hash then relation_for(result)&.filter_params
            when SearchEngine::Relation then result.filter_params
            end
          rescue StandardError
            nil
          end

          def invoke_scope(scope, partition)
            method_obj = method(scope)
            params = method_obj.parameters
            if params.empty?
              public_send(scope)
            elsif params.any? do |(kind, name)|
              %i[key keyreq].include?(kind) && %i[partition _partition].include?(name)
            end
              public_send(scope, partition: partition)
            elsif params.first && %i[req opt].include?(params.first.first)
              public_send(scope, partition)
            else
              public_send(scope)
            end
          rescue ArgumentError
            public_send(scope)
          end
        end
      end
    end
  end
end
