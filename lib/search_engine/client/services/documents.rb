# frozen_string_literal: true

module SearchEngine
  class Client
    module Services
      # Document-level operations (import, CRUD, bulk updates).
      class Documents < Base
        # @param collection [String]
        # @param jsonl [String]
        # @param action [Symbol, String]
        # @return [Object]
        def import(collection:, jsonl:, action: :upsert)
          unless collection.is_a?(String) && !collection.strip.empty?
            raise Errors::InvalidParams, 'collection must be a non-empty String'
          end
          raise Errors::InvalidParams, 'jsonl must be a String' unless jsonl.is_a?(String)

          ts = typesense_for_import
          start = current_monotonic_ms
          path = documents_path(collection)

          result = with_exception_mapping(:post, path, {}, start) do
            ts.collections[collection].documents.import(jsonl, action: action.to_s)
          end

          instrument(:post, path, current_monotonic_ms - start, {})
          result
        end

        # @param collection [String]
        # @param filter_by [String]
        # @param timeout_ms [Integer, nil]
        # @return [Hash]
        def delete_by_filter(collection:, filter_by:, timeout_ms: nil)
          unless collection.is_a?(String) && !collection.strip.empty?
            raise Errors::InvalidParams, 'collection must be a non-empty String'
          end
          unless filter_by.is_a?(String) && !filter_by.strip.empty?
            raise Errors::InvalidParams, 'filter_by must be a non-empty String'
          end

          ts = if timeout_ms&.to_i&.positive?
                 build_typesense_client_with_read_timeout(timeout_ms.to_i / 1000.0)
               else
                 typesense
               end
          start = current_monotonic_ms
          path = documents_path(collection)

          result = with_exception_mapping(:delete, path, {}, start) do
            ts.collections[collection].documents.delete(filter_by: filter_by)
          end

          instrument(:delete, path, current_monotonic_ms - start, {})
          symbolize_keys_deep(result)
        end

        # @param collection [String]
        # @param id [String, #to_s]
        # @param timeout_ms [Integer, nil]
        # @return [Hash, nil]
        def delete(collection:, id:, timeout_ms: nil)
          unless collection.is_a?(String) && !collection.strip.empty?
            raise Errors::InvalidParams, 'collection must be a non-empty String'
          end

          s = id.to_s
          raise Errors::InvalidParams, 'id must be a non-empty String' if s.strip.empty?

          ts = if timeout_ms&.to_i&.positive?
                 build_typesense_client_with_read_timeout(timeout_ms.to_i / 1000.0)
               else
                 typesense
               end
          start = current_monotonic_ms
          path = document_member_path(collection, s)

          result = with_exception_mapping(:delete, path, {}, start) do
            ts.collections[collection].documents[s].delete
          end
          symbolize_keys_deep(result)
        rescue Errors::Api => error
          return nil if error.status.to_i == 404

          raise
        ensure
          instrument(:delete, path, current_monotonic_ms - start, {}) if defined?(start)
        end

        # @param collection [String]
        # @param id [String, #to_s]
        # @param fields [Hash]
        # @param timeout_ms [Integer, nil]
        # @return [Hash]
        def update(collection:, id:, fields:, timeout_ms: nil)
          unless collection.is_a?(String) && !collection.strip.empty?
            raise Errors::InvalidParams, 'collection must be a non-empty String'
          end

          s = id.to_s
          raise Errors::InvalidParams, 'id must be a non-empty String' if s.strip.empty?
          raise Errors::InvalidParams, 'fields must be a Hash' unless fields.is_a?(Hash)

          ts = if timeout_ms&.to_i&.positive?
                 build_typesense_client_with_read_timeout(timeout_ms.to_i / 1000.0)
               else
                 typesense
               end
          start = current_monotonic_ms
          path = document_member_path(collection, s)

          result = with_exception_mapping(:patch, path, {}, start) do
            ts.collections[collection].documents[s].update(fields)
          end
          instrument(:patch, path, current_monotonic_ms - start, {})
          symbolize_keys_deep(result)
        end

        # @param collection [String]
        # @param filter_by [String]
        # @param fields [Hash]
        # @param timeout_ms [Integer, nil]
        # @return [Hash]
        def update_by_filter(collection:, filter_by:, fields:, timeout_ms: nil)
          unless collection.is_a?(String) && !collection.strip.empty?
            raise Errors::InvalidParams, 'collection must be a non-empty String'
          end
          unless filter_by.is_a?(String) && !filter_by.strip.empty?
            raise Errors::InvalidParams, 'filter_by must be a non-empty String'
          end
          raise Errors::InvalidParams, 'fields must be a Hash' unless fields.is_a?(Hash)

          ts = if timeout_ms&.to_i&.positive?
                 build_typesense_client_with_read_timeout(timeout_ms.to_i / 1000.0)
               else
                 typesense
               end
          start = current_monotonic_ms
          path = documents_path(collection)

          result = with_exception_mapping(:patch, path, {}, start) do
            ts.collections[collection].documents.update(fields, filter_by: filter_by)
          end

          instrument(:patch, path, current_monotonic_ms - start, {})
          symbolize_keys_deep(result)
        end

        # @param collection [String]
        # @param document [Hash]
        # @return [Hash]
        def create(collection:, document:)
          unless collection.is_a?(String) && !collection.strip.empty?
            raise Errors::InvalidParams, 'collection must be a non-empty String'
          end
          raise Errors::InvalidParams, 'document must be a Hash' unless document.is_a?(Hash)

          start = current_monotonic_ms
          path = documents_path(collection)

          result = with_exception_mapping(:post, path, {}, start) do
            typesense.collections[collection].documents.create(document)
          end

          instrument(:post, path, current_monotonic_ms - start, {})
          symbolize_keys_deep(result)
        end

        # Retrieve a single document by id.
        # @param collection [String]
        # @param id [String, #to_s]
        # @param timeout_ms [Integer, nil]
        # @return [Hash, nil] document hash or nil when 404
        def retrieve(collection:, id:, timeout_ms: nil)
          unless collection.is_a?(String) && !collection.strip.empty?
            raise Errors::InvalidParams, 'collection must be a non-empty String'
          end

          s = id.to_s
          raise Errors::InvalidParams, 'id must be a non-empty String' if s.strip.empty?

          ts = if timeout_ms&.to_i&.positive?
                 build_typesense_client_with_read_timeout(timeout_ms.to_i / 1000.0)
               else
                 typesense
               end
          start = current_monotonic_ms
          path = document_member_path(collection, s)

          result = with_exception_mapping(:get, path, {}, start) do
            ts.collections[collection].documents[s].retrieve
          end
          symbolize_keys_deep(result)
        rescue Errors::Api => error
          return nil if error.status.to_i == 404

          raise
        ensure
          instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {}) if defined?(start)
        end

        private

        def documents_path(collection)
          Client::RequestBuilder::COLLECTIONS_PREFIX + collection.to_s + Client::RequestBuilder::DOCUMENTS_SUFFIX
        end

        def document_member_path(collection, id)
          documents_path(collection) + "/#{id}"
        end
      end
    end
  end
end
