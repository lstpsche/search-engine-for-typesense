# frozen_string_literal: true

module SearchEngine
  # Bulk index/reindex multiple collections with reference-aware ordering.
  #
  # Usage:
  #   SearchEngine::Bulk.index(:shops, :promotions, :product_groups)
  #   SearchEngine::Bulk.reindex!(SearchEngine::Promotion, SearchEngine::ProductGroup, SearchEngine::Shop)
  #
  # Behavior:
  # - Stage 1: process inputs that are not referrers of other inputs (referenced-first order among inputs)
  # - Stage 2: process the unique set of referencers of any input exactly once (the "cascade" step)
  # - All internal indexations run with cascade suppressed; only the final collected cascade step is executed
  #   (also suppressed for nested cascades to avoid duplicates).
  module Bulk
    class << self
      # Blue/green indexing (non-destructive), mirroring {SearchEngine::Base.index_collection}.
      # When no targets are provided, all declared/registered collections are indexed
      # (models are eagerly loaded from the configured `search_engine_models` path).
      # @param targets [Array<Symbol, String, Class>] collections or model classes
      # @return [Hash] summary
      def index(*targets, client: nil)
        run!(mode: :index, targets: targets, client: client)
      end

      # Index all registered/declared collections.
      #
      # Ensures models from the configured `search_engine_models` directory are
      # loaded (via the engine's dedicated loader), discovers all collections,
      # and runs indexing as if they were passed to {.index}.
      #
      # @param client [SearchEngine::Client, nil]
      # @return [Hash] summary
      def index_all(client: nil)
        ensure_models_loaded_from_configured_path!
        names = SearchEngine::CollectionResolver.models_map.keys
        run!(mode: :index, targets: names, client: client)
      end

      # Drop+index (destructive), mirroring {SearchEngine::Base.reindex_collection!}.
      # When no targets are provided, all declared/registered collections are reindexed
      # (models are eagerly loaded from the configured `search_engine_models` path).
      # @param targets [Array<Symbol, String, Class>] collections or model classes
      # @return [Hash] summary
      def reindex!(*targets, client: nil)
        run!(mode: :reindex, targets: targets, client: client)
      end

      # Reindex all registered/declared collections.
      #
      # Ensures models from the configured `search_engine_models` directory are
      # loaded (via the engine's dedicated loader), discovers all collections,
      # and runs reindexing as if they were passed to {.reindex!}.
      #
      # @param client [SearchEngine::Client, nil]
      # @return [Hash] summary
      def reindex_all!(client: nil)
        ensure_models_loaded_from_configured_path!
        names = SearchEngine::CollectionResolver.models_map.keys
        run!(mode: :reindex, targets: names, client: client)
      end

      private

      # @param mode [Symbol] :index | :reindex
      # @param targets [Array]
      # @param client [SearchEngine::Client, nil]
      # @return [Hash]
      def run!(mode:, targets:, client: nil)
        raise ArgumentError, 'mode must be :index or :reindex' unless %i[index reindex].include?(mode.to_sym)

        ts_client = client || (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
        input_names = normalize_targets(targets)

        # Fallback to all declared/registered collections when no explicit targets are given.
        if input_names.empty?
          ensure_models_loaded_from_configured_path!
          input_names = SearchEngine::CollectionResolver.models_map.keys
        end

        reverse_graph = SearchEngine::Cascade.build_reverse_graph(client: ts_client)
        input_set = input_names.to_h { |n| [n, true] }

        # Identify inputs that are referrers of other inputs (skip them in stage 1)
        internal_referrers = internal_referrers_within_inputs(reverse_graph, input_set)

        stage1_list = input_names.reject { |n| internal_referrers.include?(n) }

        # Collect unique referencers of any input for the final cascade step
        cascade_candidates = unique_referencers_of_inputs(reverse_graph, input_names)

        # Order cascade candidates among themselves by dependency (referenced first)
        cascade_order = topo_sort_subset(reverse_graph, cascade_candidates)

        stats = {
          inputs: input_names,
          stage_1: stage1_list,
          cascade: cascade_order
        }

        payload = {
          mode: mode.to_sym,
          inputs_count: input_names.size,
          stage_1_count: stage1_list.size,
          cascade_count: cascade_order.size
        }

        SearchEngine::Instrumentation.with_context(bulk: true, bulk_suppress_cascade: true, bulk_mode: mode.to_sym) do
          SearchEngine::Instrumentation.instrument('search_engine.bulk.run', payload.merge(stats)) do
            # Stage 1 — process referenced-first inputs (that are not referrers of other inputs)
            stage1_list.each do |name|
              klass = safe_collection_class(name)
              next unless klass

              case mode.to_sym
              when :index
                klass.index_collection
              else
                klass.reindex_collection!
              end
            end

            # Stage 2 — process collected referencers once
            cascade_order.each do |name|
              klass = safe_collection_class(name)
              next unless klass

              case mode.to_sym
              when :index
                klass.index_collection
              else
                klass.reindex_collection!
              end
            end
          end
        end

        payload.merge(stats)
      end

      # Normalize inputs to logical collection names.
      # @param list [Array<Symbol, String, Class>]
      # @return [Array<String>]
      def normalize_targets(list)
        arr = Array(list).flatten.compact
        mapped = arr.map do |item|
          if item.is_a?(Class)
            item.respond_to?(:collection) ? item.collection.to_s : item.name.to_s
          else
            item.to_s
          end
        end
        filtered = mapped.reject { |s| s.to_s.strip.empty? }
        filtered.uniq
      end

      # Compute the subset of inputs that are referrers of other inputs.
      # reverse_graph: target => [{ referrer, local_key, foreign_key }, ...]
      # @param reverse_graph [Hash]
      # @param input_set [Hash{String=>true}]
      # @return [Set<String>]
      def internal_referrers_within_inputs(reverse_graph, input_set)
        require 'set'
        refs = Set.new
        reverse_graph.each do |target, edges|
          next unless input_set[target]

          Array(edges).each do |e|
            r = (e[:referrer] || e['referrer']).to_s
            refs.add(r) if input_set[r]
          end
        end
        refs
      end

      # Unique list of referencers of any input logical name.
      # @param reverse_graph [Hash]
      # @param inputs [Array<String>]
      # @return [Array<String>]
      def unique_referencers_of_inputs(reverse_graph, inputs)
        require 'set'
        seen = Set.new
        Array(inputs).each do |name|
          Array(reverse_graph[name]).each do |e|
            r = (e[:referrer] || e['referrer']).to_s
            seen.add(r) unless r.strip.empty?
          end
        end
        seen.to_a
      end

      # Topologically sort a subset of nodes using reverse_graph edges.
      # Nodes are referencers; for any edge referrer -> target, ensure target comes first when it is in the subset.
      # @param reverse_graph [Hash]
      # @param subset [Array<String>]
      # @return [Array<String>]
      def topo_sort_subset(reverse_graph, subset)
        require 'set'
        nodes = Array(subset).uniq
        node_set = nodes.to_h { |n| [n, true] }

        # Build forward adjacency among subset nodes and indegree counts
        adj = Hash.new { |h, k| h[k] = Set.new }
        indeg = Hash.new(0)

        nodes.each { |n| indeg[n] = 0 }

        reverse_graph.each do |target, edges|
          Array(edges).each do |e|
            ref = (e[:referrer] || e['referrer']).to_s
            tgt = target.to_s
            next unless node_set[ref] && node_set[tgt]

            # referrer depends on target: target should precede referrer
            unless adj[tgt].include?(ref)
              adj[tgt] << ref
              indeg[ref] += 1
            end
          end
        end

        # Kahn's algorithm (stable by name)
        queue = nodes.select { |n| indeg[n].to_i <= 0 }.sort
        order = []
        until queue.empty?
          n = queue.shift
          order << n
          adj[n].each do |m|
            indeg[m] -= 1
            queue << m if indeg[m] <= 0
          end
          queue.sort!
        end

        # Append any remaining nodes (cycles) in stable name order
        remaining = nodes - order
        order + remaining.sort
      end

      # @param name [String]
      # @return [Class, nil]
      def safe_collection_class(name)
        SearchEngine.collection_for(name)
      rescue StandardError
        nil
      end

      # Ensure host app SearchEngine models are loaded so registry and
      # namespace scans see all declared collections.
      # Uses the engine-managed Zeitwerk loader when available.
      # @return [void]
      def ensure_models_loaded_from_configured_path!
        loader = SearchEngine.instance_variable_get(:@_models_loader)
        return unless loader

        unless SearchEngine.instance_variable_defined?(:@_models_loader_setup)
          loader.setup
          SearchEngine.instance_variable_set(:@_models_loader_setup, true)
        end

        loader.eager_load
        nil
      rescue StandardError
        # Best-effort: proceed even if the dedicated loader is unavailable
        nil
      end
    end
  end
end
