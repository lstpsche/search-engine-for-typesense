# frozen_string_literal: true

module SearchEngine
  module Admin
    # Manage stopword sets for a collection.
    #
    # @since 0.1.0
    # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/synonyms-stopwords#management
    # @see `https://typesense.org/docs/latest/api/stopwords.html`
    module Stopwords
      class << self
        # Upsert a stopword set by ID.
        #
        # @param collection [String]
        # @param id [String]
        # @param terms [Array<#to_s>]
        # @return [Hash] summary { status: :created|:updated, id:, terms_count: Integer }
        # @example
        #   SearchEngine::Admin::Stopwords.upsert!(collection: "products", id: "common", terms: %w[the and])
        # @see SearchEngine::Admin::Synonyms.upsert!
        # @see `https://typesense.org/docs/latest/api/stopwords.html#upsert-a-stopwords`
        def upsert!(collection:, id:, terms:)
          c = normalize_collection!(collection)
          sid = normalize_id!(id)
          list = normalize_terms!(terms)

          existed = exists?(c, sid)
          ts_res = client.stopwords_upsert(collection: c, id: sid, terms: list)
          status = existed ? :updated : :created
          instrument(:upsert, collection: c, id: sid, terms_count: list.size)
          { status: status, id: sid, terms_count: list.size, response: ts_res }
        end

        # Retrieve a stopword set by ID.
        # @param collection [String]
        # @param id [String]
        # @return [Hash, nil] { id:, terms: [] } or nil when not found
        # @see `https://typesense.org/docs/latest/api/stopwords.html#retrieve-a-stopword`
        def get(collection:, id:)
          c = normalize_collection!(collection)
          sid = normalize_id!(id)
          res = client.stopwords_get(collection: c, id: sid)
          return nil unless res

          { id: sid, terms: Array(res[:stopwords] || res['stopwords']).map(&:to_s) }
        rescue SearchEngine::Errors::Api => error
          return nil if error.status.to_i == 404

          raise
        end

        # List all stopword sets for a collection.
        # @param collection [String]
        # @return [Array<Hash>] list of { id:, terms: [] }
        # @see `https://typesense.org/docs/latest/api/stopwords.html#list-all-stopwords-of-a-collection`
        def list(collection:)
          c = normalize_collection!(collection)
          res = client.stopwords_list(collection: c)
          Array(res).map do |item|
            { id: (item[:id] || item['id']).to_s, terms: Array(item[:stopwords] || item['stopwords']).map(&:to_s) }
          end
        end

        # Delete a stopword set by ID (idempotent).
        # @param collection [String]
        # @param id [String]
        # @return [true]
        # @see `https://typesense.org/docs/latest/api/stopwords.html#delete-a-stopword`
        def delete!(collection:, id:)
          c = normalize_collection!(collection)
          sid = normalize_id!(id)
          client.stopwords_delete(collection: c, id: sid)
          instrument(:delete, collection: c, id: sid)
          true
        rescue SearchEngine::Errors::Api => error
          return true if error.status.to_i == 404

          raise
        end

        private

        def client
          @client ||= (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
        end

        def normalize_collection!(value)
          s = value.to_s
          raise ArgumentError, 'collection must be a non-empty String' if s.strip.empty?

          s
        end

        def normalize_id!(value)
          s = value.to_s
          raise ArgumentError, 'id must be a non-empty String' if s.strip.empty?
          raise ArgumentError, 'id too long (max 256)' if s.length > 256
          raise ArgumentError, 'id contains invalid characters' unless s.match?(/\A[\w\-:.]+\z/)

          s
        end

        def normalize_terms!(list)
          arr = Array(list).flatten.compact.map { |t| t.to_s.strip.downcase }.reject(&:empty?)
          arr.uniq!
          raise ArgumentError, 'terms must include at least one non-empty String' if arr.empty?

          arr
        end

        def exists?(collection, id)
          !!client.stopwords_get(collection: collection, id: id)
        rescue SearchEngine::Errors::Api => error
          return false if error.status.to_i == 404

          raise
        end

        def instrument(action, payload)
          SearchEngine::Instrumentation.instrument("search_engine.admin.stopwords.#{action}", payload) {}
        end
      end
    end
  end
end
