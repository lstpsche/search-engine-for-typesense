# frozen_string_literal: true

module SearchEngine
  # Shared deletion helpers for building filters and deleting documents
  # across both the mapper DSL and model-level APIs.
  module Deletion
    module_function

    # Delete documents by filter string or hash from the physical collection
    # resolved for the given klass and optional partition.
    #
    # @param klass [Class] a SearchEngine::Base subclass
    # @param filter [String, nil] Typesense filter string (takes precedence over hash)
    # @param hash [Hash, nil] Hash converted to a filter string via Sanitizer
    # @param into [String, nil] explicit physical collection name override
    # @param partition [Object, nil] partition token for resolvers
    # @param timeout_ms [Integer, nil] optional read timeout override in ms
    # @return [Integer] number of deleted documents as reported by Typesense
    def delete_by(klass:, filter: nil, hash: nil, into: nil, partition: nil, timeout_ms: nil)
      filter_str = build_filter(filter, hash)
      collection = resolve_into(klass: klass, partition: partition, into: into)

      effective_timeout = if timeout_ms&.to_i&.positive?
                            timeout_ms.to_i
                          else
                            begin
                              SearchEngine.config.stale_deletes&.timeout_ms
                            rescue StandardError
                              nil
                            end
                          end

      resp = SearchEngine.client.delete_documents_by_filter(
        collection: collection,
        filter_by: filter_str,
        timeout_ms: effective_timeout
      )
      (resp && (resp[:num_deleted] || resp[:deleted] || resp[:numDeleted])).to_i
    end

    # Build a Typesense filter string from either a string or a hash.
    # @param filter [String, nil]
    # @param hash [Hash, nil]
    # @return [String]
    def build_filter(filter, hash)
      if filter && !filter.to_s.strip.empty?
        filter.to_s
      elsif hash.is_a?(Hash) && !hash.empty?
        fragments = SearchEngine::Filters::Sanitizer.build_from_hash(hash)
        fragments.join(' && ')
      else
        raise ArgumentError, 'delete_by requires a filter string or a non-empty hash'
      end
    end

    # Resolve the physical collection name using the same logic as the indexer.
    # @param klass [Class]
    # @param partition [Object, nil]
    # @param into [String, nil]
    # @return [String]
    def resolve_into(klass:, partition:, into:)
      return into if into && !into.to_s.strip.empty?

      # Prefer a context-provided target collection when present (e.g., inside
      # indexer apply/partial runs). This ensures that delete_by calls inside
      # before/after hooks target the correct physical collection, avoiding 404s
      # during the initial apply before the alias exists.
      begin
        ctx_into = SearchEngine::Instrumentation.context[:into]
        return ctx_into if ctx_into && !ctx_into.to_s.strip.empty?
      rescue StandardError
        # fall through to default resolution
      end

      resolver = begin
        SearchEngine.config.partitioning&.default_into_resolver
      rescue StandardError
        nil
      end

      if resolver.respond_to?(:arity)
        case resolver.arity
        when 1
          val = resolver.call(klass)
          return val if val && !val.to_s.strip.empty?
        when 2, -1
          val = resolver.call(klass, partition)
          return val if val && !val.to_s.strip.empty?
        end
      elsif resolver
        val = resolver.call(klass)
        return val if val && !val.to_s.strip.empty?
      end

      name = if klass.respond_to?(:collection)
               klass.collection
             else
               klass.name.to_s
             end
      name.to_s
    end
  end
end
