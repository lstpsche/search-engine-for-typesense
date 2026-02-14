# frozen_string_literal: true

module SearchEngine
  module AST
    # Abstract base node for the predicate AST.
    #
    # Provides value semantics, immutability helpers, uniform traversal via
    # #children and #each_child, and compact debug output.
    # Subclasses must implement #type and override readers as needed.
    #
    # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl`
    class Node
      # Maximum preview length for inspect payloads
      INSPECT_PREVIEW_LIMIT = 80

      # Return the symbolic node type, e.g., :eq, :and
      # @return [Symbol]
      def type
        raise NotImplementedError, 'subclasses must implement #type'
      end

      # Children nodes for traversal (default: empty)
      # @return [Array<SearchEngine::AST::Node>]
      def children
        EMPTY_ARRAY
      end

      # Iterate child nodes
      # @yieldparam child [SearchEngine::AST::Node]
      # @return [Enumerator]
      def each_child(&block)
        return enum_for(:each_child) unless block_given?

        children.each(&block)
      end

      # Structural equality: same class and equality payload
      # @param other [Object]
      # @return [Boolean]
      def ==(other)
        other.is_a?(self.class) && equality_key == other.send(:equality_key)
      end
      alias_method :eql?, :==

      # Stable hash for use in sets/maps
      # @return [Integer]
      def hash
        equality_key.hash
      end

      # Human-friendly, stable summary (compiler-agnostic)
      # @return [String]
      def to_s
        "#{type}(#{to_s_payload})"
      end

      # Compact inspect for logs
      # @return [String]
      def inspect
        payload = inspect_payload
        if payload.nil? || payload.empty?
          "#<AST #{type}>"
        else
          "#<AST #{type} #{payload}>"
        end
      end

      protected

      EMPTY_ARRAY = [].freeze

      # Subclasses should override when the default is insufficient
      # @return [Array]
      def equality_key
        [type, children]
      end

      # String payload for #to_s (comma-separated args)
      # @return [String]
      def to_s_payload
        children.map { |c| format_debug_value(c) }.join(', ')
      end

      # Inspect payload (shortened and compact)
      # @return [String]
      def inspect_payload
        to_s_payload.then { |str| truncate_for_inspect(str) }
      end

      # Format a field name for debug (unquoted)
      # @param value [String, Symbol]
      # @return [String]
      def format_field(value)
        value.to_s
      end

      # Format a value for debug (keep compiler-agnostic)
      # @param value [Object]
      # @return [String]
      def format_debug_value(value)
        case value
        when Node
          value.to_s
        when Array
          "[#{value.map { |el| format_debug_value(el) }.join(', ')}]"
        when String, Symbol
          value.inspect
        else
          value.inspect
        end
      end

      # Truncate long payloads deterministically
      # @param s [String]
      # @param limit [Integer]
      # @return [String]
      def truncate_for_inspect(s, limit = INSPECT_PREVIEW_LIMIT)
        str = s.to_s
        return str if str.length <= limit

        "#{str[0, limit]}..."
      end

      # Deep-freeze an Array and its elements (recursively for Arrays)
      # @param array [Array]
      # @return [Array] frozen copy
      def deep_freeze_array(array)
        copy = Array(array).map { |el| deep_freeze_value(el) }
        copy.freeze
      end

      # Freeze supported values; leave others as-is after best-effort freeze
      # @param value [Object]
      # @return [Object] frozen or immutable value
      def deep_freeze_value(value)
        case value
        when Array
          deep_freeze_array(value)
        when String
          value.dup.freeze
        when Node
          value # already frozen at construction
        else
          begin
            value.freeze
          rescue StandardError
            # best effort; ignore
          end
          value
        end
      end

      # Validate field presence and return String form
      # @param field [String, Symbol]
      # @return [String]
      def validate_field!(field)
        raise ArgumentError, 'field must be a String or Symbol' unless field.is_a?(String) || field.is_a?(Symbol)

        str = field.to_s.strip
        raise ArgumentError, 'field cannot be blank' if str.empty?

        str
      end

      # Ensure values is a non-empty Array
      # @param values [Array]
      # @param name [String]
      # @return [void]
      def ensure_non_empty_array!(values, name: 'values')
        return if values.is_a?(Array) && !values.empty?

        raise ArgumentError, "#{name} must be a non-empty Array"
      end
    end
  end
end
