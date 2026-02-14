# frozen_string_literal: true

require 'set'

module SearchEngine
  class Config
    # Default presets resolution configuration.
    # Controls namespacing and enablement.
    class Presets
      # @return [Boolean] when false, namespace is ignored but declared tokens remain usable
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/presets
      attr_accessor :enabled
      # @return [String, nil] optional namespace prepended to preset names when enabled
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/presets
      attr_accessor :namespace

      def initialize
        @enabled = true
        @namespace = nil
        @locked_domains = %i[filter_by sort_by include_fields exclude_fields]
        @locked_domains_set = nil
      end

      # Normalize a Boolean-like value.
      # Accepts true/false, or common String forms ("true","false","1","0","yes","no","on","off").
      # @param value [Object]
      # @return [Boolean]
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/presets#config-default-preset
      def self.normalize_enabled(value)
        return true  if value == true
        return false if value == false

        if value.is_a?(String)
          v = value.strip.downcase
          return true  if %w[1 true yes on].include?(v)
          return false if %w[0 false no off].include?(v)
        end

        value
      end

      # Normalize namespace to a non-empty String or return original for validation.
      # @param value [Object]
      # @return [String, nil, Object]
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/presets#config-default-preset
      def self.normalize_namespace(value)
        return nil if value.nil?

        if value.is_a?(String)
          ns = value.strip
          return nil if ns.empty?

          return ns
        end

        value
      end

      # Assign locked domains; accepts Array/Set or a single value. Values are
      # normalized to Symbols. Internal membership checks use a frozen Set.
      # @param value [Array<#to_sym>, Set<#to_sym>, #to_sym, nil]
      # @return [void]
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/presets#strategies-merge-only-lock
      def locked_domains=(value)
        list =
          case value
          when nil then []
          when Set then value.to_a
          when Array then value
          else Array(value)
          end
        syms = list.compact.map { |k| k.respond_to?(:to_sym) ? k.to_sym : k }.grep(Symbol)
        @locked_domains = syms
        @locked_domains_set = syms.to_set.freeze
      end

      # Return the locked domains as an Array of Symbols.
      # @return [Array<Symbol>]
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/presets#strategies-merge-only-lock
      def locked_domains
        Array(@locked_domains).map(&:to_sym)
      end

      # Return a frozen Set of locked domains for fast membership checks.
      # @return [Set<Symbol>]
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/presets#strategies-merge-only-lock
      def locked_domains_set
        @locked_domains_set ||= locked_domains.to_set.freeze
      end
    end
  end
end
