# frozen_string_literal: true

module SearchEngine
  # Lightweight utilities for observability concerns (redaction, excerpts).
  #
  # Provides a single public entry point {.redact} used by the client and
  # optional subscribers to produce compact, redacted payloads that avoid
  # leaking secrets while keeping useful context.
  module Observability
    # Keys that are considered sensitive and must be redacted whenever present.
    SENSITIVE_KEY_PATTERN = /key|token|secret|password/i

    # Whitelisted search parameter keys to include in payload excerpts.
    PARAM_WHITELIST = %i[
      q query_by include_fields exclude_fields per_page page infix filter_by sort_by
      group_by group_limit group_missing_values
      facet_by max_facet_values facet_query
      num_typos drop_tokens_threshold prioritize_exact_match query_by_weights
      vector_query
    ].freeze

    # Maximum length for `q` values before truncation.
    MAX_Q_LENGTH = 128
    MAX_DETAIL_STRING_LENGTH = 500
    MAX_DETAIL_ARRAY_LENGTH = 50
    MAX_DETAIL_HASH_LENGTH = 50
    MAX_DETAIL_DEPTH = 6
    DETAIL_SENSITIVE_KEY_PATTERN =
      /(?:\A|_)(?:api_?key|access_?key|private_?key|token|secret|password|authorization|credential)s?(?:\z|_)/i
    DETAIL_PAYLOAD_KEY_PATTERN = /\A(?:body|data|document|documents|jsonl|payload|raw|record|records|response)\z/i

    # Redact a value producing a new structure without mutating the input.
    #
    # - When given a Hash of search params, returns a compact excerpt that only
    #   includes whitelisted keys with secrets redacted and `filter_by` masked.
    # - When given an Array, returns a redacted array by applying the same logic
    #   to each element.
    # - For other values, returns a best-effort redacted representation.
    #
    # @param value [Object]
    # @return [Object]
    def self.redact(value)
      case value
      when Hash
        redact_params_hash(value)
      when Array
        value.map { |v| redact(v) }
      when String
        redact_string(value)
      else
        value
      end
    end

    # Redact structured error metadata while preserving safe diagnostic fields.
    #
    # Unlike {.redact}, which intentionally whitelists Typesense search params,
    # this method preserves arbitrary metadata keys such as +missing_required+
    # and +expected+. Sensitive and payload-bearing keys are always replaced.
    # Collections and strings are bounded so exceptions cannot retain large
    # request payloads accidentally.
    #
    # @param value [Object]
    # @return [Object]
    def self.redact_details(value)
      redact_detail_value(value, depth: 0)
    end

    # Internal: Redact a Hash presumed to be Typesense search params.
    # Returns a new Hash with only whitelisted keys preserved. Sensitive keys
    # are not included; `filter_by` literals are masked.
    def self.redact_params_hash(params)
      result = {}

      PARAM_WHITELIST.each do |key|
        next unless params.key?(key)

        val = params[key]
        case key
        when :q
          result[:q] = truncate_q(val)
        when :filter_by
          result[:filter_by] = redact_filter_by(val)
        when :vector_query
          result[:vector_query] = redact_vector_query(val)
        else
          result[key] = redact_simple_value(val)
        end
      end

      result
    end

    # Internal: Best-effort redaction for simple scalar values.
    def self.redact_simple_value(value)
      return value unless value.is_a?(String)

      redact_string(value)
    end

    # Internal: Truncate overly long query strings.
    def self.truncate_q(query)
      return query unless query.is_a?(String)

      query.length > MAX_Q_LENGTH ? "#{query[0, MAX_Q_LENGTH]}..." : query
    end

    # Internal: Redact secrets in a string and mask obvious literal fragments.
    def self.redact_string(str)
      return str unless str.is_a?(String)

      # Mask obvious quoted literals first
      redacted = str.gsub(/"[^"]*"|'[^']*'/, '***')

      # Mask numeric literals (best-effort)
      redacted.gsub(/\b\d+(?:\.\d+)?\b/, '***')
    end

    # Internal: Mask literal values in Typesense filter expressions while
    # preserving attribute/operator structure. Best-effort and lightweight.
    # Examples:
    #   "category_id:=123" => "category_id:=***"
    #   "price:>10 && brand:='Acme'" => "price:>*** && brand:=***"
    def self.redact_filter_by(filter)
      return filter unless filter.is_a?(String)

      # Replace values that follow a comparator or a colon with *** until a
      # delimiter is reached. Also mask quoted strings and numbers.
      masked = filter.gsub(/([!:><=]{1,2})\s*([^\s)&|]+)/, '\1***')
      masked = masked.gsub(/"[^"]*"|'[^']*'/, '***')
      masked.gsub(/\b\d+(?:\.\d+)?\b/, '***')
    end

    # Internal: Redact raw float arrays in a vector_query string while
    # preserving the structural tokens (field name, k, alpha, etc.).
    # Replaces `[0.1,0.2,...]` with `[<N dims>]`.
    #
    # Disable via +config.observability.redact_vectors = false+ to see
    # raw vectors (useful for debugging).
    #
    # @param vq [String]
    # @return [String]
    def self.redact_vector_query(vq)
      return vq unless vq.is_a?(String)
      return vq if SearchEngine.config.observability.redact_vectors == false

      vq.gsub(/\[(-?\d+(?:\.\d+)?(?:\s*,\s*-?\d+(?:\.\d+?))*)\]/) do
        dims = Regexp.last_match(1).split(',').size
        "[<#{dims} dims>]"
      end
    end

    def self.redact_detail_value(value, depth:)
      return '[TRUNCATED]' if depth >= MAX_DETAIL_DEPTH

      case value
      when Hash
        redact_detail_hash(value, depth: depth)
      when Array
        value.first(MAX_DETAIL_ARRAY_LENGTH).map do |item|
          redact_detail_value(item, depth: depth + 1)
        end
      when String
        truncate_message(value, MAX_DETAIL_STRING_LENGTH)
      when Symbol
        value.to_s
      when Numeric, TrueClass, FalseClass, NilClass
        value
      else
        "[#{value.class.name}]"
      end
    end

    def self.redact_detail_hash(value, depth:)
      value.first(MAX_DETAIL_HASH_LENGTH).each_with_object({}) do |(key, item), result|
        normalized_key = key.to_s
        result[key] = if sensitive_detail_key?(normalized_key) || payload_detail_key?(normalized_key)
                        '[REDACTED]'
                      else
                        redact_detail_value(item, depth: depth + 1)
                      end
      end
    end

    def self.sensitive_detail_key?(key)
      key.match?(DETAIL_SENSITIVE_KEY_PATTERN)
    end

    def self.payload_detail_key?(key)
      key.match?(DETAIL_PAYLOAD_KEY_PATTERN)
    end

    # Build a filtered URL/common options hash for payloads.
    # @param url_opts [Hash]
    # @return [Hash]
    def self.filtered_url_opts(url_opts)
      return {} unless url_opts.is_a?(Hash)

      {
        use_cache: url_opts[:use_cache],
        cache_ttl: url_opts[:cache_ttl]
      }
    end

    # Compute a SHA1 hex digest for a value.
    # @param value [#to_s]
    # @return [String]
    def self.sha_1(value)
      require 'digest'
      Digest::SHA1.hexdigest(value.to_s)
    end

    # Return a shortened hash prefix for display/logging.
    # @param hexdigest [String]
    # @param length [Integer]
    # @return [String]
    def self.short_hash(hexdigest, length = 8)
      s = hexdigest.to_s
      s[0, length]
    end

    # Truncate and normalize a free-text message to a single line.
    # @param message [String]
    # @param max [Integer]
    # @return [String]
    def self.truncate_message(message, max = 200)
      s = message.to_s.gsub(/\s+/, ' ').strip
      s[0, max]
    end

    # Compute partition helpers used in logs: prefer raw numeric; hash strings.
    # @param partition [Object]
    # @return [Hash] { partition: <raw>, partition_hash: <String,nil> }
    def self.partition_fields(partition)
      if partition.is_a?(Numeric)
        { partition: partition, partition_hash: nil }
      else
        hex = sha_1(partition)
        { partition: partition, partition_hash: short_hash(hex) }
      end
    end

    private_class_method :redact_params_hash, :redact_simple_value, :truncate_q,
                         :redact_string, :redact_filter_by, :redact_vector_query,
                         :redact_detail_value, :redact_detail_hash, :sensitive_detail_key?,
                         :payload_detail_key?
  end
end
