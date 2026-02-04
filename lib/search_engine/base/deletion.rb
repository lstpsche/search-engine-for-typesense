# frozen_string_literal: true

require 'active_support/concern'

module SearchEngine
  class Base
    # Instance-level deletion for a single hydrated record.
    #
    # Provides {#delete} that deletes the current document from the backing
    # Typesense collection using its document id. The id is obtained from the
    # hydrated payload when available, and falls back to computing it via the
    # class-level `identify_by` strategy.
    module Deletion
      extend ActiveSupport::Concern

      # Delete this record from the collection.
      #
      # Accepts the same optional knobs as relation-level {Relation::Deletion#delete_all}
      # for consistency.
      #
      # @param into [String, nil] override physical collection name
      # @param partition [Object, nil] partition token for resolvers
      # @param timeout_ms [Integer, nil] optional read timeout override in ms
      # @return [Integer] number of deleted documents (0 or 1)
      # @raise [SearchEngine::Errors::InvalidParams] when the record id is unavailable
      def delete(into: nil, partition: nil, timeout_ms: nil)
        id_value = __se_effective_document_id_for_deletion
        if id_value.nil? || id_value.to_s.strip.empty?
          raise SearchEngine::Errors::InvalidParams,
                "Cannot delete without document id; include 'id' in selection or provide identifiable attributes"
        end

        # Resolve target collection (alias or physical) consistently with relation/model helpers
        collection = SearchEngine::Deletion.resolve_into(
          klass: self.class,
          partition: partition,
          into: into
        )

        # Apply same timeout fallback policy as delete_by
        effective_timeout = if timeout_ms&.to_i&.positive?
                              timeout_ms.to_i
                            else
                              begin
                                SearchEngine.config.stale_deletes&.timeout_ms
                              rescue StandardError
                                nil
                              end
                            end

        resp = SearchEngine.client.delete_document(
          collection: collection,
          id: id_value,
          timeout_ms: effective_timeout
        )
        # The client returns a Hash or nil when 404; normalize to numeric 0/1
        resp.nil? ? 0 : 1
      end

      private

      # Determine the effective document id for deletion, preferring hydrated
      # `@id` when present and falling back to the class-level identify_by.
      # @return [String, nil]
      def __se_effective_document_id_for_deletion
        v = instance_variable_defined?(:@id) ? instance_variable_get(:@id) : nil
        return v unless v.nil? || v.to_s.strip.empty?

        begin
          computed = self.class.compute_document_id(self)
          return computed unless computed.nil? || computed.to_s.strip.empty?
        rescue StandardError
          # best-effort; nil means we cannot determine id
        end

        nil
      end
    end
  end
end
