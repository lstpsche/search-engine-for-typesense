# frozen_string_literal: true

module SearchEngine
  class Config
    # Typesense transport/client configuration.
    # Holds connection details, timeouts, and retry knobs.
    # Public fields mirror the facade to preserve API compatibility via forwarders.
    #
    # @example
    #   ts = SearchEngine::Config::Typesense.new
    #   ts.host = 'localhost'
    #   ts.port = 8108
    class Typesense
      # @return [String, nil] secret Typesense API key (redacted separately)
      attr_accessor :api_key
      # @return [String] hostname of the Typesense server
      attr_accessor :host
      # @return [Integer] TCP port for the Typesense server
      attr_accessor :port
      # @return [String] one of "http" or "https"
      attr_reader :protocol
      # @return [Integer] request total timeout in milliseconds
      attr_accessor :timeout_ms
      # @return [Integer] connect/open timeout in milliseconds
      attr_accessor :open_timeout_ms
      # @return [Hash] retry policy with keys { attempts: Integer, backoff: Float or Range<Float> }
      attr_accessor :retries

      def initialize
        @api_key = nil
        @host = 'localhost'
        @port = 8108
        @protocol = 'http'
        @timeout_ms = 3_600_000
        @open_timeout_ms = 1_000
        @retries = { attempts: 2, backoff: (10.0..60.0) }
      end

      # Normalize protocol assignment to default to 'http' when blank.
      # @param value [String, nil]
      # @return [void]
      def protocol=(value)
        s = value.to_s
        @protocol = s.strip.present? ? s : 'http'
      end
    end
  end
end
