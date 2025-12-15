# frozen_string_literal: true

module SearchEngine
  class Relation
    # Options and pagination normalization helpers.
    # Centralizes precedence rules and guards for page/per vs limit/offset.
    module Options
      # Compute page/per_page with precedence:
      # - When page/per are provided, prefer them (fill missing per with nil, page defaults to 1 when only per is set)
      # - Otherwise, when limit is set, derive page from offset and per_page from limit
      # - Otherwise, return empty Hash
      # @return [Hash{Symbol=>Integer}]
      def compute_pagination
        page = @state[:page]
        per = @state[:per_page]

        if page || per
          out = {}
          if per && page
            out[:page] = page
            out[:per_page] = per
          elsif per && !page
            out[:page] = 1
            out[:per_page] = per
          elsif page && !per
            out[:page] = page
          end
          return out
        elsif @state[:limit]
          limit = @state[:limit]
          off = @state[:offset] || 0
          computed_page = (off.to_i / limit.to_i) + 1
          return { page: computed_page, per_page: limit }
        end

        {}
      end

      # Normalize hit limits input into a compact Hash.
      # Accepts keys :early_limit and :max and coerces to positive Integers when possible.
      # @param value [Hash]
      # @return [Hash]
      def normalize_hit_limits_input(value)
        unless value.is_a?(Hash)
          raise SearchEngine::Errors::InvalidOption.new(
            'InvalidOption: hit_limits expects a Hash of options',
            doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/hit-limits'
          )
        end

        out = {}
        if value.key?(:early_limit) || value.key?('early_limit')
          raw = value[:early_limit] || value['early_limit']
          begin
            iv = Integer(raw)
            out[:early_limit] = iv if iv.positive?
          rescue StandardError
            nil
          end
        end
        if value.key?(:max) || value.key?('max')
          raw = value[:max] || value['max']
          begin
            iv = Integer(raw)
            out[:max] = iv if iv.positive?
          rescue StandardError
            nil
          end
        end
        out
      end

      # Coerce integers with a minimum bound; nil passes through.
      # @param value [Object]
      # @param name [Symbol]
      # @param min [Integer]
      # @return [Integer, nil]
      # @raise [ArgumentError] when not coercible or below min
      def coerce_integer_min(value, name, min)
        return nil if value.nil?

        integer =
          case value
          when Integer then value
          else Integer(value)
          end

        raise ArgumentError, "#{name} must be >= #{min}" if integer < min

        integer
      rescue ArgumentError, TypeError
        raise ArgumentError, "#{name} must be an Integer or nil"
      end

      # Strict boolean coercion with helpful errors.
      # @param value [Object]
      # @param name [Symbol]
      # @return [true,false]
      # @raise [ArgumentError]
      def coerce_boolean_strict(value, name)
        case value
        when true, false
          value
        when String
          s = value.to_s.strip.downcase
          return true  if %w[1 true yes on t].include?(s)
          return false if %w[0 false no off f].include?(s)

          raise ArgumentError, "#{name} must be a boolean"
        when Integer
          return true  if value == 1
          return false if value.zero?

          raise ArgumentError, "#{name} must be a boolean"
        else
          raise ArgumentError, "#{name} must be a boolean"
        end
      end

      # Access indifferent key from Hash
      def option_value(hash, key)
        if hash.key?(key)
          hash[key]
        else
          hash[key.to_s]
        end
      end

      # Stable truncation for inspect helpers
      def truncate_for_inspect(str, max = 80)
        return str unless str.is_a?(String)
        return str if str.length <= max

        "#{str[0, max]}..."
      end
    end
  end
end
