# frozen_string_literal: true

module SearchEngine
  # Test-only utilities for offline execution and assertions.
  #
  # Provides:
  # - SearchEngine::Test::StubClient — a programmable stub client that captures requests
  # - Event capture helpers (SearchEngine::Test.capture_events)
  # - Framework adapters (RSpec matcher `emit_event`, Minitest assertions)
  #
  # These helpers are allocation-light, thread-safe, and never perform network I/O.
  #
  # @since M8
  # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/testing
  module Test
    class << self
      # Subscribe to `search_engine.*` for the duration of the block and return captured events.
      # Each event is a Hash: { name:, payload:, time:, duration: }
      # Payloads are redacted for safety.
      # @param name [String, Regexp, nil]
      # @yield block within which events are captured
      # @return [Array<Hash>]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/testing#event-assertions
      def capture_events(name = nil)
        require 'active_support/notifications'
        pattern = name.is_a?(Regexp) ? name : /^search_engine\./
        captured = []
        handle = ActiveSupport::Notifications.subscribe(pattern) do |*args|
          ev = ActiveSupport::Notifications::Event.new(*args)
          payload = safe_payload(ev.payload)
          captured << { name: ev.name, payload: payload, time: ev.time, duration: ev.duration }
        end
        yield
        captured
      ensure
        ActiveSupport::Notifications.unsubscribe(handle) if defined?(handle)
      end

      # Internal: apply redaction once more to be safe
      def safe_payload(payload)
        p = payload.dup
        p[:params] = SearchEngine::Observability.redact(p[:params]) if p.key?(:params)
        p[:params_preview] = SearchEngine::Observability.redact(p[:params_preview]) if p.key?(:params_preview)
        p
      rescue StandardError
        payload
      end
    end
  end
end

require 'search_engine/test/stub_client'
# Framework adapters are optional; require only when present
begin
  require 'search_engine/test/rspec_matchers'
rescue LoadError
  # no-op
end
begin
  require 'search_engine/test/minitest_assertions'
rescue LoadError
  # no-op
end
