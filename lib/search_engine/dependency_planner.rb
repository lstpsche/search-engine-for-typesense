# frozen_string_literal: true

module SearchEngine
  # Public dependency-ordering helpers for Typesense collection reference graphs.
  module DependencyPlanner
    class << self
      # Build a normalized reverse dependency graph.
      # @param source [Symbol] :registry, :typesense, or :auto
      # @param client [SearchEngine::Client, nil]
      # @return [Hash{String=>Array<Hash>}] target collection => reference edges
      def reverse_graph(source: :registry, client: nil)
        graph = case source.to_sym
                when :registry
                  SearchEngine::Cascade.send(:build_from_registry)
                when :typesense
                  SearchEngine::Cascade.send(:build_from_typesense, client || SearchEngine.client)
                when :auto
                  SearchEngine::Cascade.build_reverse_graph(client: client || SearchEngine.client)
                else
                  raise ArgumentError, 'source must be :registry, :typesense, or :auto'
                end

        normalize_reverse_graph(graph)
      end

      # Order collection names so referenced collections precede referrers.
      # @param collections [Array<Symbol, String, Class>] collections or model classes
      # @param source [Symbol] :registry, :typesense, or :auto
      # @param client [SearchEngine::Client, nil]
      # @param reverse_graph [Hash, nil] prebuilt reverse graph
      # @return [Array<String>]
      def order_collections(collections, source: :registry, client: nil, reverse_graph: nil)
        graph = graph_or_build(reverse_graph, source: source, client: client)
        topo_sort_subset(graph, normalize_collections(collections))
      end

      # Order events by their collection dependency order while preserving per-collection event order.
      # @param events [Array<Object, Hash>]
      # @param collection_method [Symbol, String] event reader or hash key for collection name
      # @param source [Symbol] :registry, :typesense, or :auto
      # @param client [SearchEngine::Client, nil]
      # @param reverse_graph [Hash, nil] prebuilt reverse graph
      # @return [Array<Object, Hash>]
      def order_events(events, collection_method: :collection, source: :registry, client: nil, reverse_graph: nil)
        graph = graph_or_build(reverse_graph, source: source, client: client)
        grouped = Hash.new { |h, k| h[k] = [] }
        without_collection = []

        Array(events).each do |event|
          collection = collection_from_event(event, collection_method)
          if collection.nil? || collection.empty?
            without_collection << event
          else
            grouped[collection] << event
          end
        end

        ordered_collections = topo_sort_subset(graph, grouped.keys)
        ordered_collections.flat_map { |collection| grouped[collection] } + without_collection
      end

      # Return collections that directly reference the given collection.
      # @param collection [Symbol, String, Class]
      # @param source [Symbol] :registry, :typesense, or :auto
      # @param client [SearchEngine::Client, nil]
      # @param reverse_graph [Hash, nil] prebuilt reverse graph
      # @return [Array<String>]
      def referencers_for(collection, source: :registry, client: nil, reverse_graph: nil)
        graph = graph_or_build(reverse_graph, source: source, client: client)
        name = normalize_collection(collection)
        Array(graph[name]).filter_map { |edge| edge[:referrer] }.uniq
      end

      # Return collections directly referenced by the given collection.
      # @param collection [Symbol, String, Class]
      # @param source [Symbol] :registry, :typesense, or :auto
      # @param client [SearchEngine::Client, nil]
      # @param reverse_graph [Hash, nil] prebuilt reverse graph
      # @return [Array<String>]
      def dependencies_for(collection, source: :registry, client: nil, reverse_graph: nil)
        graph = graph_or_build(reverse_graph, source: source, client: client)
        name = normalize_collection(collection)
        deps = []
        graph.each do |target, edges|
          deps << target if Array(edges).any? { |edge| edge[:referrer] == name }
        end
        deps.uniq
      end

      # Build the two Bulk stages from collection dependencies.
      # @param collections [Array<Symbol, String, Class>] collections or model classes
      # @param source [Symbol] :registry, :typesense, or :auto
      # @param client [SearchEngine::Client, nil]
      # @param reverse_graph [Hash, nil] prebuilt reverse graph
      # @return [Hash{Symbol=>Array<String>}] :stage_1 and :cascade collection names
      def bulk_stages(collections, source: :auto, client: nil, reverse_graph: nil)
        inputs = normalize_collections(collections)
        graph = graph_or_build(reverse_graph, source: source, client: client)
        input_set = inputs.to_h { |name| [name, true] }
        internal_referrers = internal_referrers_within_inputs(graph, input_set)
        cascade_candidates = unique_referencers_of_inputs(graph, inputs)

        {
          stage_1: inputs.reject { |name| internal_referrers.include?(name) },
          cascade: topo_sort_subset(graph, cascade_candidates)
        }
      end

      private

      def graph_or_build(graph, source:, client:)
        graph ? normalize_reverse_graph(graph) : reverse_graph(source: source, client: client)
      end

      def normalize_reverse_graph(graph)
        normalized = Hash.new { |h, k| h[k] = [] }
        Hash(graph).each do |target, edges|
          target_name = target.to_s
          next if target_name.empty?

          Array(edges).each do |edge|
            normalized[target_name] << normalize_edge(edge)
          end
        end
        normalized
      end

      def normalize_edge(edge)
        {
          referrer: edge_value(edge, :referrer).to_s,
          local_key: edge_value(edge, :local_key).to_s,
          foreign_key: edge_value(edge, :foreign_key).to_s
        }
      end

      def edge_value(edge, key)
        return edge.public_send(key) if edge.respond_to?(key)

        edge[key] || edge[key.to_s]
      end

      def normalize_collections(collections)
        Array(collections).flatten.compact.filter_map do |collection|
          name = normalize_collection(collection)
          name unless name.empty?
        end.uniq
      end

      def normalize_collection(collection)
        if collection.is_a?(Class)
          collection.respond_to?(:collection) ? collection.collection.to_s : collection.name.to_s
        else
          collection.to_s
        end
      end

      def collection_from_event(event, collection_method)
        if event.respond_to?(collection_method)
          event.public_send(collection_method).to_s
        elsif event.respond_to?(:[])
          (event[collection_method.to_sym] || event[collection_method.to_s]).to_s
        else
          ''
        end
      end

      def internal_referrers_within_inputs(reverse_graph, input_set)
        require 'set'
        refs = Set.new
        reverse_graph.each do |target, edges|
          next unless input_set[target]

          Array(edges).each do |edge|
            referrer = edge[:referrer].to_s
            refs.add(referrer) if input_set[referrer]
          end
        end
        refs
      end

      def unique_referencers_of_inputs(reverse_graph, inputs)
        require 'set'
        seen = Set.new
        inputs.each do |name|
          Array(reverse_graph[name]).each do |edge|
            referrer = edge[:referrer].to_s
            seen.add(referrer) unless referrer.empty?
          end
        end
        seen.to_a
      end

      def topo_sort_subset(reverse_graph, subset)
        require 'set'
        nodes = Array(subset).uniq
        node_set = nodes.to_h { |name| [name, true] }
        adj = Hash.new { |h, k| h[k] = Set.new }
        indeg = Hash.new(0)

        nodes.each { |name| indeg[name] = 0 }

        reverse_graph.each do |target, edges|
          Array(edges).each do |edge|
            referrer = edge[:referrer].to_s
            next unless node_set[referrer] && node_set[target]
            next if adj[target].include?(referrer)

            adj[target] << referrer
            indeg[referrer] += 1
          end
        end

        queue = nodes.select { |name| indeg[name].to_i <= 0 }.sort
        order = []
        until queue.empty?
          name = queue.shift
          order << name
          adj[name].each do |dependent|
            indeg[dependent] -= 1
            queue << dependent if indeg[dependent] <= 0
          end
          queue.sort!
        end

        remaining = nodes - order
        instrument_cycle!(remaining) if remaining.any?
        order + remaining.sort
      end

      def instrument_cycle!(collections)
        payload = { collections: collections.sort }
        SearchEngine::Instrumentation.instrument('search_engine.dependency_planner.cycle', payload) {}
        SearchEngine.config.logger&.warn(
          "search_engine dependency planner cycle detected: #{payload[:collections].join(', ')}"
        )
      rescue StandardError
        nil
      end
    end
  end
end
