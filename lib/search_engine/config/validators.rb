# frozen_string_literal: true

module SearchEngine
  class Config
    # Validation helpers for configuration.
    # Keep messages identical to the previous inline implementations.
    module Validators
      module_function

      def validate_protocol(protocol)
        return [] if %w[http https].include?(protocol.to_s)

        ['protocol must be "http" or "https"']
      end

      def validate_host(host)
        return [] unless host.nil? || host.to_s.strip.empty?

        ['host must be present']
      end

      def validate_port(port)
        return [] if port.is_a?(Integer) && port.positive?

        ['port must be a positive Integer']
      end

      def validate_timeouts(timeout_ms, open_timeout_ms)
        errors = []
        errors << 'timeout_ms must be a non-negative Integer' unless timeout_ms.is_a?(Integer) && !timeout_ms.negative?
        unless open_timeout_ms.is_a?(Integer) && !open_timeout_ms.negative?
          errors << 'open_timeout_ms must be a non-negative Integer'
        end
        errors
      end

      def validate_retries(retries)
        return ['retries must be a Hash with keys :attempts and :backoff'] unless retries_valid_shape?(retries)

        errors = []
        attempts = retries[:attempts]
        backoff = retries[:backoff]

        unless attempts.is_a?(Integer) && !attempts.negative?
          errors << 'retries[:attempts] must be a non-negative Integer'
        end

        if backoff.is_a?(Numeric)
          errors << 'retries[:backoff] must be a non-negative Float or Range of non-negative Floats' if backoff.negative?
        elsif backoff.is_a?(Range)
          b = backoff.begin
          e = backoff.end
          valid = b.is_a?(Numeric) && e.is_a?(Numeric) && b >= 0 && e >= 0 && b <= e
          errors << 'retries[:backoff] must be a non-negative Float or Range of non-negative Floats' unless valid
        else
          errors << 'retries[:backoff] must be a non-negative Float or Range of non-negative Floats'
        end

        errors
      end

      def retries_valid_shape?(retries)
        retries.is_a?(Hash) && retries.key?(:attempts) && retries.key?(:backoff)
      end

      def validate_cache(cache_ttl_s)
        return [] if cache_ttl_s.is_a?(Integer) && !cache_ttl_s.negative?

        ['cache_ttl_s must be a non-negative Integer']
      end

      def validate_multi_search_limit(limit)
        return [] if limit.is_a?(Integer) && !limit.negative?

        ['multi_search_limit must be a non-negative Integer']
      end

      def validate_presets(presets)
        errors = []
        en = presets.enabled
        ns = presets.namespace
        ld = Array(presets.locked_domains)

        errors << 'presets.enabled must be a Boolean' unless [true, false].include?(en)
        unless ns.nil? || (ns.is_a?(String) && !ns.strip.empty?)
          errors << 'presets.namespace must be a non-empty String or nil'
        end
        unless ld.is_a?(Array) && ld.all? { |k| k.is_a?(Symbol) }
          errors << 'presets.locked_domains must be an Array of Symbols'
        end
        errors
      end
    end
  end
end
