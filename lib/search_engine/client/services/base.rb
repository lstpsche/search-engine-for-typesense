# frozen_string_literal: true

module SearchEngine
  class Client
    module Services
      # Shared helpers for service objects that delegate to {SearchEngine::Client}.
      #
      # Provides convenient access to the parent client and its private helpers
      # without widening the public API surface of the client itself.
      class Base
        # @param client [SearchEngine::Client]
        def initialize(client)
          @client = client
        end

        private

        attr_reader :client

        def config
          client.__send__(:config)
        end

        def typesense
          client.__send__(:typesense)
        end

        def typesense_for_import
          client.__send__(:typesense_for_import)
        end

        def build_typesense_client_with_read_timeout(seconds)
          client.__send__(:build_typesense_client_with_read_timeout, seconds)
        end

        def build_typesense_client
          client.__send__(:build_typesense_client)
        end

        def with_exception_mapping(*args, &block)
          client.__send__(:with_exception_mapping, *args, &block)
        end

        def instrument(*args)
          client.__send__(:instrument, *args)
        end

        def log_success(*args)
          client.__send__(:log_success, *args)
        end

        def current_monotonic_ms
          client.__send__(:current_monotonic_ms)
        end

        def derive_cache_opts(url_opts)
          client.__send__(:derive_cache_opts, url_opts)
        end

        def symbolize_keys_deep(payload)
          client.__send__(:symbolize_keys_deep, payload)
        end

        def validate_single!(*args)
          client.__send__(:validate_single!, *args)
        end

        def validate_multi!(*args)
          client.__send__(:validate_multi!, *args)
        end
      end
    end
  end
end
