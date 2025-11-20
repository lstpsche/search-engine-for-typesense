# frozen_string_literal: true

module SearchEngine
  class Client
    module Services
      # Collection-related operations (schema lifecycle, alias management, listings).
      class Collections < Base
        # @param logical_name [String]
        # @return [String, nil]
        def resolve_alias(logical_name, timeout_ms: nil)
          name = logical_name.to_s
          start = current_monotonic_ms
          path = [Client::RequestBuilder::ALIASES_PREFIX, name].join

          ts = if timeout_ms&.to_i&.positive?
                 build_typesense_client_with_read_timeout(timeout_ms.to_i / 1000.0)
               else
                 typesense
               end

          result = with_exception_mapping(:get, path, {}, start) do
            ts.aliases[name].retrieve
          end

          (result && (result['collection_name'] || result[:collection_name])).to_s
        rescue Errors::Api => error
          return nil if error.status.to_i == 404

          raise
        ensure
          instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end

        # @param collection_name [String]
        # @return [Hash, nil]
        def retrieve_schema(collection_name, timeout_ms: nil)
          name = collection_name.to_s
          start = current_monotonic_ms
          path = [Client::RequestBuilder::COLLECTIONS_PREFIX, name].join

          ts = if timeout_ms&.to_i&.positive?
                 build_typesense_client_with_read_timeout(timeout_ms.to_i / 1000.0)
               else
                 typesense
               end

          result = with_exception_mapping(:get, path, {}, start) do
            ts.collections[name].retrieve
          end

          symbolize_keys_deep(result)
        rescue Errors::Api => error
          return nil if error.status.to_i == 404

          raise
        ensure
          instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end

        # @param alias_name [String]
        # @param physical_name [String]
        # @return [Hash]
        def upsert_alias(alias_name, physical_name)
          a = alias_name.to_s
          p = physical_name.to_s
          start = current_monotonic_ms
          path = [Client::RequestBuilder::ALIASES_PREFIX, a].join

          result = with_exception_mapping(:put, path, {}, start) do
            typesense.aliases.upsert(a, collection_name: p)
          end

          symbolize_keys_deep(result)
        ensure
          instrument(:put, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end

        # @param schema [Hash]
        # @return [Hash]
        def create(schema)
          start = current_monotonic_ms
          path = Client::RequestBuilder::COLLECTIONS_ROOT
          body = schema.dup

          result = with_exception_mapping(:post, path, {}, start) do
            typesense.collections.create(body)
          end

          symbolize_keys_deep(result)
        ensure
          instrument(:post, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end

        # @param name [String]
        # @param schema [Hash]
        # @return [Hash]
        def update(name, schema)
          n = name.to_s
          start = current_monotonic_ms
          path = Client::RequestBuilder::COLLECTIONS_PREFIX + n
          body = schema.dup

          result = with_exception_mapping(:patch, path, {}, start) do
            typesense.collections[n].update(body)
          end

          symbolize_keys_deep(result)
        ensure
          instrument(:patch, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end

        # @param name [String]
        # @param timeout_ms [Integer, nil]
        # @return [Hash]
        def delete(name, timeout_ms: nil)
          n = name.to_s
          start = current_monotonic_ms
          path = Client::RequestBuilder::COLLECTIONS_PREFIX + n

          ts = if timeout_ms&.to_i&.positive?
                 build_typesense_client_with_read_timeout(timeout_ms.to_i / 1000.0)
               else
                 typesense
               end

          result = with_exception_mapping(:delete, path, {}, start) do
            ts.collections[n].delete
          end

          symbolize_keys_deep(result)
        rescue Errors::Api => error
          return { status: 404 } if error.status.to_i == 404

          raise
        ensure
          instrument(:delete, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end

        # @return [Array<Hash>]
        def list(timeout_ms: nil)
          start = current_monotonic_ms
          path = Client::RequestBuilder::COLLECTIONS_ROOT

          ts = if timeout_ms&.to_i&.positive?
                 build_typesense_client_with_read_timeout(timeout_ms.to_i / 1000.0)
               else
                 typesense
               end

          result = with_exception_mapping(:get, path, {}, start) do
            ts.collections.retrieve
          end

          symbolize_keys_deep(result)
        ensure
          instrument(:get, path, (start ? (current_monotonic_ms - start) : 0.0), {})
        end
      end
    end
  end
end
