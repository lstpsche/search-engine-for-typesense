# frozen_string_literal: true

module SearchEngine
  class Base
    module IndexMaintenance
      # Lifecycle orchestration for full/partial indexing flows.
      module Lifecycle
        extend ActiveSupport::Concern

        class_methods do
          # Run indexing workflow for this collection.
          # @param partition [Object, Array<Object>, nil]
          # @param client [SearchEngine::Client, nil]
          # @param pre [Symbol, nil] :ensure (ensure presence) or :index (ensure + fix drift)
          # @return [void]
          def indexate(partition: nil, client: nil, pre: nil)
            logical = respond_to?(:collection) ? collection.to_s : name.to_s
            puts
            puts(%(>>>>>> Indexating Collection "#{logical}"))
            client_obj = client || (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new

            if partition.nil?
              __se_indexate_full(client: client_obj, pre: pre)
            else
              __se_indexate_partial(partition: partition, client: client_obj, pre: pre)
            end
            nil
          end

          def reindexate!(pre: nil)
            drop_collection!
            indexate(pre: pre)
          end

          def rebuild_partition!(partition:, into: nil, pre: nil)
            if pre
              client_obj = (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
              __se_preflight_dependencies!(mode: pre, client: client_obj)
            end
            parts = if partition.nil? || (partition.respond_to?(:empty?) && partition.empty?)
                      [nil]
                    else
                      Array(partition)
                    end

            return SearchEngine::Indexer.rebuild_partition!(self, partition: parts.first, into: into) if parts.size == 1

            parts.map { |p| SearchEngine::Indexer.rebuild_partition!(self, partition: p, into: into) }
          end

          def __se_indexate_full(client:, pre: nil)
            logical = respond_to?(:collection) ? collection.to_s : name.to_s
            __se_preflight_dependencies!(mode: pre, client: client) if pre

            diff = SearchEngine::Schema.diff(self, client: client)[:diff] || {}
            missing = __se_schema_missing?(diff)
            puts("Step 1: Presence — processing → #{missing ? 'missing' : 'present'}")

            applied, indexed_inside_apply = __se_full_apply_if_missing(client, missing)
            drift = __se_full_check_drift(diff, missing)
            applied, indexed_inside_apply = __se_full_apply_if_drift(client, drift, applied, indexed_inside_apply)
            __se_full_indexation(applied, indexed_inside_apply)
            __se_full_retention(applied, logical, client)
          end

          def __se_full_apply_if_missing(client, missing)
            applied = false
            indexed_inside_apply = false
            if missing
              puts('Step 2: Create+Apply Schema — processing')
              SearchEngine::Schema.apply!(self, client: client) do |new_physical|
                indexed_inside_apply = __se_index_partitions!(into: new_physical)
              end
              applied = true
              puts('Step 2: Create+Apply Schema — done')
            else
              puts('Step 2: Create+Apply Schema — skip (collection present)')
            end
            [applied, indexed_inside_apply]
          end

          def __se_full_check_drift(diff, missing)
            unless missing
              puts('Step 3: Check Schema Status — processing')
              drift = __se_schema_drift?(diff)
              puts("Step 3: Check Schema Status — #{drift ? 'drift' : 'in_sync'}")
              return drift
            end
            puts('Step 3: Check Schema Status — skip (just created)')
            false
          end

          def __se_full_apply_if_drift(client, drift, applied, indexed_inside_apply)
            if drift
              puts('Step 4: Apply New Schema — processing')
              SearchEngine::Schema.apply!(self, client: client) do |new_physical|
                indexed_inside_apply = __se_index_partitions!(into: new_physical)
              end
              applied = true
              puts('Step 4: Apply New Schema — done')
            else
              puts('Step 4: Apply New Schema — skip')
            end
            [applied, indexed_inside_apply]
          end

          def __se_full_indexation(applied, indexed_inside_apply)
            cascade_ok = false
            if applied && indexed_inside_apply
              puts('Step 5: Indexation — skip (performed during schema apply)')
              begin
                cascade_ok = indexed_inside_apply.to_sym == :ok
              rescue StandardError
                cascade_ok = false
              end
            else
              puts('Step 5: Indexation — processing')
              idx_status = __se_index_partitions!(into: nil)
              puts('Step 5: Indexation — done')
              cascade_ok = (idx_status == :ok)
            end
            __se_cascade_after_indexation!(context: :full) if cascade_ok
          end

          def __se_full_retention(applied, logical, client)
            if applied
              puts('Step 6: Retention Cleanup — skip (handled by schema apply)')
            else
              puts('Step 6: Retention Cleanup — processing')
              dropped = __se_retention_cleanup!(logical: logical, client: client)
              puts("Step 6: Retention Cleanup — dropped=#{dropped.inspect}")
            end
          end

          def __se_indexate_partial(partition:, client:, pre: nil)
            partitions = Array(partition)
            diff_res = SearchEngine::Schema.diff(self, client: client)
            diff = diff_res[:diff] || {}

            missing = __se_schema_missing?(diff)
            puts("Step 1: Presence — processing → #{missing ? 'missing' : 'present'}")
            if missing
              puts('Partial: collection is not present. Quit early.')
              return
            end

            puts('Step 2: Check Schema Status — processing')
            drift = __se_schema_drift?(diff)
            if drift
              puts('Partial: schema is not up-to-date. Quit early (run full indexation).')
              return
            end
            puts('Step 2: Check Schema Status — in_sync')

            __se_preflight_dependencies!(mode: pre, client: client) if pre

            puts('Step 3: Partial Indexation — processing')
            all_ok = true
            partitions.each do |p|
              summary = SearchEngine::Indexer.rebuild_partition!(self, partition: p, into: nil)
              puts(SearchEngine::Logging::PartitionProgress.line(p, summary))
              begin
                all_ok &&= (summary.status == :ok)
              rescue StandardError
                all_ok &&= false
              end
            end
            puts('Step 3: Partial Indexation — done')
            __se_cascade_after_indexation!(context: :full) if all_ok
          end

          # rubocop:disable Metrics/PerceivedComplexity
          def __se_cascade_after_indexation!(context: :full)
            puts
            puts(%(>>>>>> Cascade Referencers))
            results = SearchEngine::Cascade.cascade_reindex!(source: self, ids: nil, context: context)
            outcomes = Array(results[:outcomes])
            if outcomes.empty?
              puts('  none')
            else
              outcomes.each do |o|
                coll = o[:collection] || o['collection']
                mode = (o[:mode] || o['mode']).to_s
                case mode
                when 'partial'
                  puts(%(  Referencer "#{coll}" → partial reindex))
                when 'full'
                  puts(%(  Referencer "#{coll}" → full reindex))
                when 'skipped_unregistered'
                  puts(%(  Referencer "#{coll}" → skipped (unregistered)))
                when 'skipped_cycle'
                  puts(%(  Referencer "#{coll}" → skipped (cycle)))
                else
                  puts(%(  Referencer "#{coll}" → #{mode}))
                end
              end
            end
            puts('>>>>>> Cascade Done')
          rescue StandardError => error
            base = "Cascade — error=#{error.class}: #{error.message.to_s[0, 200]}"
            if error.respond_to?(:status) || error.respond_to?(:body)
              status = begin
                error.respond_to?(:status) ? error.status : nil
              rescue StandardError
                nil
              end
              body_preview = begin
                b = error.respond_to?(:body) ? error.body : nil
                if b.is_a?(String)
                  b[0, 500]
                elsif b.is_a?(Hash)
                  b.inspect[0, 500]
                else
                  b.to_s[0, 500]
                end
              rescue StandardError
                nil
              end
              warn([base, ("status=#{status}" if status), ("body=#{body_preview}" if body_preview)].compact.join(' '))
            else
              warn(base)
            end
          end
          # rubocop:enable Metrics/PerceivedComplexity

          def __se_retention_cleanup!(*_)
            SearchEngine::Schema.prune_history!(self)
          rescue StandardError
            nil
          end
        end
      end
    end
  end
end
