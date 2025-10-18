# frozen_string_literal: true

require 'search_engine/client/services/base'
require 'search_engine/client/services/search'
require 'search_engine/client/services/collections'
require 'search_engine/client/services/operations'
require 'search_engine/client/services/documents'

module SearchEngine
  class Client
    # Registry for all service objects used internally by the client.
    module Services
      SERVICES = {
        search: Search,
        collections: Collections,
        operations: Operations,
        documents: Documents
      }.freeze

      module_function

      # @param client [SearchEngine::Client]
      # @return [Hash<Symbol, SearchEngine::Client::Services::Base>]
      def build(client)
        SERVICES.transform_values { |klass| klass.new(client) }
      end
    end
  end
end
