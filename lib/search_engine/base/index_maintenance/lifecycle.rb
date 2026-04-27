# frozen_string_literal: true

require 'search_engine/logging/output'

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
            SearchEngine::Logging::Output.puts
            SearchEngine::Logging::Output.puts(
              SearchEngine::Logging::Color.header(%(>>>>>> Indexing Collection "#{logical}"))
            )
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
            step = SearchEngine::Logging::StepLine.new('Presence', io: SearchEngine::Logging::Output.io)
            missing ? step.finish_warn('missing') : step.finish('present')

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
            step = SearchEngine::Logging::StepLine.new('Schema', io: SearchEngine::Logging::Output.io)
            if missing
              step.update('creating')
              begin
                SearchEngine::Schema.apply!(self, client: client) do |new_physical|
                  step.yield_line!
                  indexed_inside_apply = __se_index_partitions!(into: new_physical)
                  __se_abort_apply_if_failed!(indexed_inside_apply)
                end
                applied = true
                step.finish('created')
              rescue SearchEngine::Errors::IndexationAborted
                applied = true
                step.finish_warn('created (indexing failed — alias not swapped)')
              end
            else
              step.skip('collection present')
            end
            [applied, indexed_inside_apply]
          ensure
            step&.close
          end

          def __se_full_check_drift(diff, missing, force_rebuild)
            step = SearchEngine::Logging::StepLine.new('Schema Status', io: SearchEngine::Logging::Output.io)
            unless missing
              step.update('checking')
              drift = __se_schema_drift?(diff)
              if force_rebuild && !drift
                step.finish_warn('force_rebuild')
                return true
              end
              drift ? step.finish_warn('drift') : step.finish('in_sync')
              return drift
            end
            step.skip('just created')
            false
          ensure
            step&.close
          end

          def __se_full_apply_if_drift(client, drift, applied, indexed_inside_apply, force_rebuild)
            step = SearchEngine::Logging::StepLine.new('Schema Apply', io: SearchEngine::Logging::Output.io)
            if drift
              step.update('applying')
              begin
                SearchEngine::Schema.apply!(self, client: client, force_rebuild: force_rebuild) do |new_physical|
                  step.yield_line!
                  indexed_inside_apply = __se_index_partitions!(into: new_physical)
                  __se_abort_apply_if_failed!(indexed_inside_apply)
                end
                applied = true
                step.finish('applied')
              rescue SearchEngine::Errors::IndexationAborted
                applied = true
                step.finish_warn('applied (indexing failed — alias not swapped)')
              end
            else
              step.skip
            end
            [applied, indexed_inside_apply]
          ensure
            step&.close
          end

          def __se_full_indexation(applied, indexed_inside_apply)
            result = nil
            step = SearchEngine::Logging::StepLine.new('Indexing', io: SearchEngine::Logging::Output.io)
            if applied && indexed_inside_apply
              result = indexed_inside_apply if indexed_inside_apply.is_a?(Hash)
              if __se_result_status(result) == :ok
                step.skip('performed during schema apply')
              else
                __se_finish_indexation_step(step, result)
              end
            else
              step.update('indexing')
              step.yield_line!
              result = __se_index_partitions!(into: nil)
              __se_finish_indexation_step(step, result)
            end

            __se_cascade_after_indexation!(context: :full) if __se_result_status(result) == :ok
            result
          ensure
            step&.close
          end

          def __se_result_status(result)
            result.is_a?(Hash) ? result[:status] : :ok
          end

          def __se_finish_indexation_step(step, result)
            case __se_result_status(result)
            when :ok      then step.finish('done')
            when :partial then step.finish_warn('partial')
            else               step.finish_warn('failed')
            end
          end

          def __se_full_retention(applied, logical, client)
            step = SearchEngine::Logging::StepLine.new('Retention', io: SearchEngine::Logging::Output.io)
            if applied
              step.skip('handled by schema apply')
            else
              step.update('cleaning')
              dropped = __se_retention_cleanup!(logical: logical, client: client)
              step.finish("dropped=#{dropped.inspect}")
            end
          ensure
            step&.close
          end

          def __se_index_partial(partition:, client:, pre: nil)
            partitions = Array(partition)
            diff_res = SearchEngine::Schema.diff(self, client: client)
            diff = diff_res[:diff] || {}

            missing = __se_schema_missing?(diff)
            step = SearchEngine::Logging::StepLine.new('Presence', io: SearchEngine::Logging::Output.io)
            if missing
              step.finish_warn('missing — collection not present, exit early')
              return { status: :failed, docs_total: 0, success_total: 0, failed_total: 0,
                       sample_error: 'Collection not present' }
            end
            step.finish('present')

            step = SearchEngine::Logging::StepLine.new('Schema Status', io: SearchEngine::Logging::Output.io)
            step.update('checking')
            drift = __se_schema_drift?(diff)
            if drift
              step.finish_warn('drift — exit early (run full indexing)')
              return { status: :failed, docs_total: 0, success_total: 0, failed_total: 0,
                       sample_error: 'Schema drift detected' }
            end
            step.finish('in_sync')

            __se_preflight_dependencies!(mode: pre, client: client) if pre

            step = SearchEngine::Logging::StepLine.new('Partial Indexing', io: SearchEngine::Logging::Output.io)
            step.update('indexing')
            step.yield_line!

            renderer = SearchEngine::Logging::LiveRenderer.new(
              labels: partitions.map(&:inspect), partitions: partitions,
              io: SearchEngine::Logging::Output.io
            )
            renderer.start
            summaries = []
            partitions.each_with_index do |p, idx|
              slot = renderer[idx]
              slot.start
              begin
                on_batch = ->(info) { slot.progress(**info) }
                summary = SearchEngine::Indexer.rebuild_partition!(self, partition: p, into: nil, on_batch: on_batch)
                slot.finish(summary)
                summaries << summary
              rescue StandardError => error
                slot.finish_error(error)
                raise
              end
            end
            begin
              renderer.stop
            rescue StandardError
              nil
            end
            step.finish('done')

            result = __se_build_index_result(summaries)
            __se_cascade_after_indexation!(context: :full) if result[:status] == :ok
            result
          ensure
            renderer&.stop
            step&.close
          end

          # rubocop:disable Metrics/PerceivedComplexity, Metrics/AbcSize
          def __se_cascade_after_indexation!(context: :full)
            if SearchEngine::Instrumentation.context&.[](:bulk_suppress_cascade)
              SearchEngine::Logging::Output.puts
              SearchEngine::Logging::Output.puts(
                SearchEngine::Logging::Color.dim('>>>>>> Cascade Referencers — suppressed (bulk)')
              )
              return
            end
            SearchEngine::Logging::Output.puts
            SearchEngine::Logging::Output.puts(
              SearchEngine::Logging::Color.header(%(>>>>>> Cascade Referencers))
            )
            results = SearchEngine::Cascade.cascade_reindex!(source: self, ids: nil, context: context)
            outcomes = Array(results[:outcomes])
            if outcomes.empty?
              SearchEngine::Logging::Output.puts(SearchEngine::Logging::Color.dim('  none'))
            else
              outcomes.each do |o|
                coll = o[:collection] || o['collection']
                mode = (o[:mode] || o['mode']).to_s
                __se_log_cascade_outcome(coll, mode)
              end
            end
            SearchEngine::Logging::Output.puts(
              SearchEngine::Logging::Color.header('>>>>>> Cascade Done')
            )
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

          def __se_log_cascade_outcome(coll, mode)
            msg = case mode
                  when 'partial'
                    %(  Referencer "#{coll}" → #{SearchEngine::Logging::Color.apply('partial reindex', :green)})
                  when 'full'
                    %(  Referencer "#{coll}" → #{SearchEngine::Logging::Color.apply('full reindex', :green)})
                  when 'skipped_unregistered'
                    SearchEngine::Logging::Color.dim(%(  Referencer "#{coll}" → skipped (unregistered)))
                  when 'skipped_cycle'
                    SearchEngine::Logging::Color.dim(%(  Referencer "#{coll}" → skipped (cycle)))
                  else
                    %(  Referencer "#{coll}" → #{mode})
                  end
            SearchEngine::Logging::Output.puts(msg)
          end

          # Raise {SearchEngine::Errors::IndexationAborted} when the result
          # from {__se_index_partitions!} indicates a non-ok status. Called
          # inside a {Schema.apply!} block to prevent the alias swap.
          def __se_abort_apply_if_failed!(result)
            return unless result.is_a?(Hash) && result[:status] != :ok

            raise SearchEngine::Errors::IndexationAborted, result
          end

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
