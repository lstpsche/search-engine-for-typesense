# frozen_string_literal: true

module SearchEngine
  # Shared updating helpers for building filters and updating documents
  # across both the mapper DSL and model-level APIs.
  module Update
    module_function

    # Update documents by filter string or hash in the physical collection
    # resolved for the given klass and optional partition.
    #
    # @param klass [Class] a SearchEngine::Base subclass
    # @param attributes [Hash] fields to update
    # @param filter [String, nil] Typesense filter string (takes precedence over hash)
    # @param hash [Hash, nil] Hash converted to a filter string via Sanitizer
    # @param into [String, nil] explicit physical collection name override
    # @param partition [Object, nil] partition token for resolvers
    # @param timeout_ms [Integer, nil] optional read timeout override in ms
    # @return [Integer] number of updated documents as reported by Typesense
    def update_by(klass:, attributes:, filter: nil, hash: nil, into: nil, partition: nil, timeout_ms: nil)
      raise ArgumentError, 'attributes must be a non-empty Hash' unless attributes.is_a?(Hash) && !attributes.empty?

      filter_str = SearchEngine::Deletion.build_filter(filter, hash)
      collection = SearchEngine::Deletion.resolve_into(klass: klass, partition: partition, into: into)

      resp = SearchEngine.client.update_documents_by_filter(
        collection: collection,
        filter_by: filter_str,
        fields: attributes,
        timeout_ms: timeout_ms
      )
      (resp && (resp[:num_updated] || resp[:updated] || resp[:numUpdated])).to_i
    end
  end
end
