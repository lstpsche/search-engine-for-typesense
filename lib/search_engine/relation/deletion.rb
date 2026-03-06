# frozen_string_literal: true

module SearchEngine
  class Relation
    # Deletion helpers bound to a relation instance.
    #
    # Provides `delete_all` which deletes documents that match the current
    # relation predicates. When no predicates are present, it deletes all
    # documents from the collection by using a safe match-all filter.
    module Deletion
      # Delete all documents matching the current relation filters.
      #
      # When the relation has no filters, deletes all documents from the
      # collection using a safe match-all filter (`id:!=null`).
      #
      # @param into [String, nil] override physical collection name
      # @param partition [Object, nil] partition token for resolvers
      # @param timeout_ms [Integer, nil] optional read timeout override in ms
      # @return [Integer] number of deleted documents
      def delete_all(into: nil, partition: nil, timeout_ms: nil)
        ast_nodes = Array(@state[:ast]).flatten.compact
        filter = compiled_filter_by(ast_nodes)

        # Fallback to a safe match-all filter when no predicates are present
        filter = 'id:!=null' if filter.to_s.strip.empty?

        SearchEngine::Deletion.delete_by(
          klass: @klass,
          filter: filter,
          into: into,
          partition: partition,
          timeout_ms: timeout_ms
        )
      end

      alias_method :destroy_all, :delete_all
    end
  end
end
