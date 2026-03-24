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
          # @param force_rebuild [Boolean] when true, force schema rebuild (blue/green)
          # @return [Hash, nil] result hash with :status, :docs_total, :success_total, :failed_total, :sample_error
          def index_collection(partition: nil, client: nil, pre: nil, force_rebuild: false)
            logical = respond_to?(:collection) ? collection.to_s : name.to_s
            puts
            puts(SearchEngine::Logging::Color.header(%(>>>>>> Indexing Collection "#{logical}")))
            client_obj = client || SearchEngine.client

            result = if partition.nil?
                       __se_index_full(client: client_obj, pre: pre, force_rebuild: force_rebuild)
                     else
                       __se_index_partial(partition: partition, client: client_obj, pre: pre)
                     end

            result.is_a?(Hash) ? result.merge(collection: logical) : { collection: logical, status: :ok }
          end

          def reindex_collection!(pre: nil)
            drop_collection!
            index_collection(pre: pre)
          end

          def rebuild_partition!(partition:, into: nil, pre: nil)
            if pre
              client_obj = SearchEngine.client
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

          def __se_index_full(client:, pre: nil, force_rebuild: false)
            logical = respond_to?(:collection) ? collection.to_s : name.to_s
            __se_preflight_dependencies!(mode: pre, client: client) if pre

            diff = SearchEngine::Schema.diff(self, client: client)[:diff] || {}
            missing = __se_schema_missing?(diff)
            presence = if missing
                         SearchEngine::Logging::Color.apply('missing', :yellow)
                       else
                         SearchEngine::Logging::Color.apply('present', :green)
                       end
            puts("Step 1: Presence — #{SearchEngine::Logging::Color.bold('processing')} → #{presence}")

            applied, indexed_inside_apply = __se_full_apply_if_missing(client, missing)
            drift = __se_full_check_drift(diff, missing, force_rebuild)
            applied, indexed_inside_apply = __se_full_apply_if_drift(
              client,
              drift,
              applied,
              indexed_inside_apply,
              force_rebuild
            )
            result = __se_full_indexation(applied, indexed_inside_apply)
            __se_full_retention(applied, logical, client)
            result
          end

          def __se_full_apply_if_missing(client, missing)
            applied = false
            indexed_inside_apply = false
            if missing
              puts("Step 2: Create+Apply Schema — #{SearchEngine::Logging::Color.bold('processing')}")
              SearchEngine::Schema.apply!(self, client: client) do |new_physical|
                indexed_inside_apply = __se_index_partitions!(into: new_physical)
              end
              applied = true
              puts("Step 2: Create+Apply Schema — #{SearchEngine::Logging::Color.apply('done', :green)}")
            else
              puts(SearchEngine::Logging::Color.dim('Step 2: Create+Apply Schema — skip (collection present)'))
            end
            [applied, indexed_inside_apply]
          end

          def __se_full_check_drift(diff, missing, force_rebuild)
            unless missing
              puts("Step 3: Check Schema Status — #{SearchEngine::Logging::Color.bold('processing')}")
              drift = __se_schema_drift?(diff)
              if force_rebuild && !drift
                puts("Step 3: Check Schema Status — #{SearchEngine::Logging::Color.apply('force_rebuild', :yellow)}")
                return true
              end
              schema_status = if drift
                                SearchEngine::Logging::Color.apply('drift', :yellow)
                              else
                                SearchEngine::Logging::Color.apply('in_sync', :green)
                              end
              puts("Step 3: Check Schema Status — #{schema_status}")
              return drift
            end
            puts(SearchEngine::Logging::Color.dim('Step 3: Check Schema Status — skip (just created)'))
            false
          end

          def __se_full_apply_if_drift(client, drift, applied, indexed_inside_apply, force_rebuild)
            if drift
              puts("Step 4: Apply New Schema — #{SearchEngine::Logging::Color.bold('processing')}")
              SearchEngine::Schema.apply!(self, client: client, force_rebuild: force_rebuild) do |new_physical|
                indexed_inside_apply = __se_index_partitions!(into: new_physical)
              end
              applied = true
              puts("Step 4: Apply New Schema — #{SearchEngine::Logging::Color.apply('done', :green)}")
            else
              puts(SearchEngine::Logging::Color.dim('Step 4: Apply New Schema — skip'))
            end
            [applied, indexed_inside_apply]
          end

          def __se_full_indexation(applied, indexed_inside_apply)
            result = nil
            if applied && indexed_inside_apply
              puts(SearchEngine::Logging::Color.dim('Step 5: Indexing — skip (performed during schema apply)'))
              result = indexed_inside_apply if indexed_inside_apply.is_a?(Hash)
            else
              puts("Step 5: Indexing — #{SearchEngine::Logging::Color.bold('processing')}")
              result = __se_index_partitions!(into: nil)
              puts("Step 5: Indexing — #{SearchEngine::Logging::Color.apply('done', :green)}")
            end

            cascade_ok = result.is_a?(Hash) ? result[:status] == :ok : false
            __se_cascade_after_indexation!(context: :full) if cascade_ok
            result
          end

          def __se_full_retention(applied, logical, client)
            if applied
              puts(SearchEngine::Logging::Color.dim('Step 6: Retention Cleanup — skip (handled by schema apply)'))
            else
              puts("Step 6: Retention Cleanup — #{SearchEngine::Logging::Color.bold('processing')}")
              dropped = __se_retention_cleanup!(logical: logical, client: client)
              dropped_str = SearchEngine::Logging::Color.apply("dropped=#{dropped.inspect}", :green)
              puts("Step 6: Retention Cleanup — #{dropped_str}")
            end
          end

          def __se_index_partial(partition:, client:, pre: nil)
            partitions = Array(partition)
            diff_res = SearchEngine::Schema.diff(self, client: client)
            diff = diff_res[:diff] || {}

            missing = __se_schema_missing?(diff)
            presence = if missing
                         SearchEngine::Logging::Color.apply('missing', :yellow)
                       else
                         SearchEngine::Logging::Color.apply('present', :green)
                       end
            puts("Step 1: Presence — #{SearchEngine::Logging::Color.bold('processing')} → #{presence}")
            if missing
              msg = SearchEngine::Logging::Color.apply(
                'Step 1: Partial — collection is not present. Quit early.', :yellow
              )
              puts(msg)
              return { status: :failed, docs_total: 0, success_total: 0, failed_total: 0,
                       sample_error: 'Collection not present' }
            end

            puts("Step 2: Check Schema Status — #{SearchEngine::Logging::Color.bold('processing')}")
            drift = __se_schema_drift?(diff)
            if drift
              msg = SearchEngine::Logging::Color.apply(
                'Step 2: Partial — schema is not up-to-date. Exit early (run full indexing).', :yellow
              )
              puts(msg)
              return { status: :failed, docs_total: 0, success_total: 0, failed_total: 0,
                       sample_error: 'Schema drift detected' }
            end
            puts("Step 2: Check Schema Status — #{SearchEngine::Logging::Color.apply('in_sync', :green)}")

            __se_preflight_dependencies!(mode: pre, client: client) if pre

            puts("Step 3: Partial Indexing — #{SearchEngine::Logging::Color.bold('processing')}")
            summaries = []
            partitions.each do |p|
              summary = SearchEngine::Indexer.rebuild_partition!(self, partition: p, into: nil)
              summaries << summary
              puts(SearchEngine::Logging::PartitionProgress.line(p, summary))
            end
            puts("Step 3: Partial Indexing — #{SearchEngine::Logging::Color.apply('done', :green)}")

            result = __se_build_index_result(summaries)
            __se_cascade_after_indexation!(context: :full) if result[:status] == :ok
            result
          end

          # rubocop:disable Metrics/PerceivedComplexity, Metrics/AbcSize
          def __se_cascade_after_indexation!(context: :full)
            if SearchEngine::Instrumentation.context&.[](:bulk_suppress_cascade)
              puts
              puts(SearchEngine::Logging::Color.dim('>>>>>> Cascade Referencers — suppressed (bulk)'))
              return
            end
            puts
            puts(SearchEngine::Logging::Color.header(%(>>>>>> Cascade Referencers)))
            results = SearchEngine::Cascade.cascade_reindex!(source: self, ids: nil, context: context)
            outcomes = Array(results[:outcomes])
            if outcomes.empty?
              puts(SearchEngine::Logging::Color.dim('  none'))
            else
              outcomes.each do |o|
                coll = o[:collection] || o['collection']
                mode = (o[:mode] || o['mode']).to_s
                case mode
                when 'partial'
                  puts(%(  Referencer "#{coll}" → #{SearchEngine::Logging::Color.apply('partial reindex', :green)}))
                when 'full'
                  puts(%(  Referencer "#{coll}" → #{SearchEngine::Logging::Color.apply('full reindex', :green)}))
                when 'skipped_unregistered'
                  puts(SearchEngine::Logging::Color.dim(%(  Referencer "#{coll}" → skipped (unregistered))))
                when 'skipped_cycle'
                  puts(SearchEngine::Logging::Color.dim(%(  Referencer "#{coll}" → skipped (cycle))))
                else
                  puts(%(  Referencer "#{coll}" → #{mode}))
                end
              end
            end
            puts(SearchEngine::Logging::Color.header('>>>>>> Cascade Done'))
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
              err_parts = [base]
              err_parts << "status=#{status}" if status
              err_parts << "body=#{body_preview}" if body_preview
              warn(SearchEngine::Logging::Color.apply(err_parts.compact.join(' '), :red))
            else
              warn(SearchEngine::Logging::Color.apply(base, :red))
            end
          end
          # rubocop:enable Metrics/PerceivedComplexity, Metrics/AbcSize

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
