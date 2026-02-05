# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'bundler/setup'
require 'rails'
require 'active_support/notifications'
require 'minitest/autorun'
require 'set'
require 'search_engine'
require 'search_engine/test'

# Provide a simple `pending` helper for Minitest tests
module Minitest
  class Test
    def pending(message = 'pending')
      skip(message)
    end
  end
end

# Reset registry to avoid cross-test contamination of collection mappings
begin
  SearchEngine.send(:__reset_registry_for_tests!)
rescue StandardError
  nil
end

# Shared test models for attribute registries used by selection validation
module SearchEngine
  class Author < SearchEngine::Base
    collection 'authors'
    identify_by :id
    attribute :first_name, :string
    attribute :last_name, :string
  end

  class Brand < SearchEngine::Base
    collection 'brands'
    identify_by :id
    attribute :internal_score, :float
  end
end
