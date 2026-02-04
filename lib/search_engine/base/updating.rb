# frozen_string_literal: true

require 'active_support/concern'

module SearchEngine
  class Base
    # Instance-level updating for a single hydrated record.
    #
    # Provides {#update} that partially updates the current document in the
    # Typesense collection using its document id. The id is obtained from the
    # hydrated payload when available, and falls back to computing it via the
    # class-level `identify_by` strategy.
    module Updating
      extend ActiveSupport::Concern

      # Partially update this record in the collection.
      #
      # @param attributes [Hash, nil] fields to update (or pass as kwargs)
      # @param into [String, nil] override physical collection name
      # @param partition [Object, nil] partition token for resolvers
      # @param timeout_ms [Integer, nil] optional read timeout override in ms
      # @param cascade [Boolean, nil] when true, trigger cascade reindex for referencing collections
      # @return [Integer] number of updated documents (0 or 1)
      # @raise [SearchEngine::Errors::InvalidParams] when the record id is unavailable
      def update(attributes = nil, into: nil, partition: nil, timeout_ms: nil, cascade: nil, **kwattrs)
        attrs = __se_coalesce_update_attributes(attributes, kwattrs)
        raise SearchEngine::Errors::InvalidParams, 'attributes must be a non-empty Hash' if attrs.nil? || attrs.empty?

        id_value = __se_effective_document_id_for_update
        if id_value.nil? || id_value.to_s.strip.empty?
          raise SearchEngine::Errors::InvalidParams,
                "Cannot update without document id; include 'id' in selection or provide identifiable attributes"
        end

        collection = SearchEngine::Deletion.resolve_into(
          klass: self.class,
          partition: partition,
          into: into
        )

        resp = SearchEngine.client.update_document(
          collection: collection,
          id: id_value,
          fields: attrs,
          timeout_ms: timeout_ms
        )
        updated = resp ? 1 : 0

        # Best-effort cascade when requested
        if updated.positive? && cascade
          begin
            SearchEngine::Cascade.cascade_reindex!(source: self.class, ids: [id_value], context: :update)
          rescue StandardError
            # swallow cascade errors to keep update semantics stable
          end
        end

        updated
      end

      private

      # Prefer explicit Hash argument when provided; fall back to kwargs.
      # @param attributes [Hash, nil]
      # @param kwattrs [Hash]
      # @return [Hash, nil]
      def __se_coalesce_update_attributes(attributes, kwattrs)
        return attributes if attributes.is_a?(Hash) && !attributes.empty?
        return kwattrs if kwattrs && !kwattrs.empty?

        nil
      end

      # Determine the effective document id for update, preferring hydrated
      # `@id` when present and falling back to the class-level identify_by.
      # @return [String, nil]
      def __se_effective_document_id_for_update
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
