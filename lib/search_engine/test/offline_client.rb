# frozen_string_literal: true

# rubocop:disable Lint/UnusedMethodArgument

require 'search_engine/result'

module SearchEngine
  module Test
    # No-op client that mirrors SearchEngine::Client without network I/O.
    # Returns safe empty/ok responses for all operations.
    class OfflineClient
      SUCCESS_JSONL = "{\"success\":true}\n"
      EMPTY_SEARCH = { 'hits' => [], 'found' => 0, 'out_of' => 0 }.freeze

      def search(collection:, params:, url_opts: {})
        SearchEngine::Result.new(EMPTY_SEARCH, klass: nil)
      end

      def multi_search(searches:, url_opts: {})
        { 'results' => [] }
      end

      def import_documents(collection:, jsonl:, action: :upsert)
        SUCCESS_JSONL
      end

      def delete_documents_by_filter(collection:, filter_by:, timeout_ms: nil)
        { 'num_deleted' => 0 }
      end

      def delete_document(collection:, id:, timeout_ms: nil)
        {}
      end

      def retrieve_document(collection:, id:, timeout_ms: nil)
        nil
      end

      def update_document(collection:, id:, fields:, timeout_ms: nil)
        fields.merge('id' => id.to_s)
      end

      def update_documents_by_filter(collection:, filter_by:, fields:, timeout_ms: nil)
        { 'num_updated' => 0 }
      end

      def create_document(collection:, document:)
        document
      end

      def resolve_alias(logical_name, timeout_ms: nil)
        nil
      end

      def retrieve_collection_schema(collection_name, timeout_ms: nil)
        nil
      end

      def upsert_alias(alias_name, physical_name)
        {}
      end

      def create_collection(schema)
        schema
      end

      def update_collection(name, schema)
        schema
      end

      def delete_collection(name, timeout_ms: nil)
        {}
      end

      def list_collections(timeout_ms: nil)
        []
      end

      def health
        { 'ok' => true }
      end

      def metrics
        {}
      end

      def stats
        {}
      end

      def list_api_keys
        []
      end

      def synonyms_upsert(collection:, id:, terms:)
        {}
      end

      def synonyms_list(collection:)
        []
      end

      def synonyms_get(collection:, id:)
        nil
      end

      def synonyms_delete(collection:, id:)
        {}
      end

      def stopwords_upsert(collection:, id:, terms:)
        {}
      end

      def stopwords_list(collection:)
        []
      end

      def stopwords_get(collection:, id:)
        nil
      end

      def stopwords_delete(collection:, id:)
        {}
      end

      def clear_cache
        { 'success' => true }
      end
    end
  end
end

# rubocop:enable Lint/UnusedMethodArgument
