# frozen_string_literal: true

require 'date'
require 'time'

module SearchEngine
  module Filters
    # Encodes structured predicates for the Typesense filter_by grammar.
    module Sanitizer
      module_function

      # A field path is data too. Only AST predicates may use the internal
      # "$association.field" notation, which the compiler turns into a join.
      def field_name(field, joined: false)
        raise ArgumentError, 'filter field must be a String or Symbol' unless field.is_a?(String) || field.is_a?(Symbol)

        name = field.to_s
        path = joined && name.start_with?('$') && name.include?('.') ? name[1..] : name
        raise ArgumentError, 'invalid filter field name' unless path.match?(/\A[[:alnum:]_-]+(?:\.[[:alnum:]_-]+)*\z/)

        name
      end

      # Backticks protect operators during expression parsing. A list (even
      # with a single item) also removes the delimiters before tokenization,
      # including on collections that explicitly index the backtick symbol.
      def quote(value, field: nil, klass: nil, joined: false)
        if value.is_a?(Array)
          items = value.flatten(1).map { |item| literal(item, field: field, klass: klass, joined: joined) }
          return "[#{items.join(', ')}]"
        end

        item = literal(value, field: field, klass: klass, joined: joined)
        item.start_with?('`') ? "[#{item}]" : item
      end

      def quote_scalar_for_filter(value)
        quote(value)
      end

      def literal(value, field:, klass:, joined:)
        case value
        when nil then 'null'
        when true then 'true'
        when false then 'false'
        when Numeric
          number = value.to_s
          unless number.match?(/\A[+-]?[0-9]+(?:\.[0-9]+)?(?:e[+-]?[0-9]+)?\z/i)
            raise ArgumentError, 'invalid numeric filter value'
          end

          number
        when Array
          raise ArgumentError, 'expected a scalar filter list item'
        else
          string = if value.respond_to?(:to_time) && !value.is_a?(String)
                     type = Array(klass.attributes[field.to_sym]).first if field && klass.respond_to?(:attributes)
                     time = value.to_time
                     return time.to_i.to_s if %i[time datetime integer].include?(type)

                     value.respond_to?(:iso8601) ? value.iso8601 : time.iso8601
                   else
                     value.to_s
                   end
          string_literal(string, field: field, joined: joined)
        end
      end
      private_class_method :literal

      def string_literal(string, field:, joined:)
        # The expression scanner and list splitter disagree about escaped
        # backticks. The splitter also trims spaces, recognizes phrases, and
        # gives a trailing asterisk special meaning after removing delimiters.
        id_field = field.to_s == 'id'
        if string.empty? || string.match?(/[`[:cntrl:]]/) || string.start_with?(' ') || string.end_with?(' ') ||
           (string == '*' && (id_field || field.nil?)) || (!id_field && string.length > 1 && string.end_with?('*')) ||
           (!id_field && string.length > 1 && string.start_with?('"') && string.end_with?('"'))
          raise ArgumentError, 'filter value cannot be represented as an exact Typesense literal'
        end

        # Typesense recognizes geopoint syntax even inside a backtick pair.
        # Its outer join scanner also counts parentheses inside quoted values.
        if string.match?(/: *(?:\[ *)?\(/) || (joined && !balanced_parentheses?(string))
          raise ArgumentError, 'filter value contains unrepresentable Typesense delimiters'
        end

        return string if string.match?(/\A[[:alnum:]_\\-]+\z/)
        raise ArgumentError, 'quoted filter value cannot end in a backslash' if string.end_with?('\\')

        "`#{string}`"
      end
      private_class_method :string_literal

      def balanced_parentheses?(string)
        depth = 0
        string.each_char do |char|
          depth += 1 if char == '('
          depth -= 1 if char == ')'
          return false if depth.negative?
        end
        depth.zero?
      end
      private_class_method :balanced_parentheses?

      def build_from_hash(hash, _attributes_map = nil, klass: nil)
        raise ArgumentError, 'filters hash must be a Hash' unless hash.is_a?(Hash)

        hash.map do |key, value|
          field = field_name(key)
          "#{field}:=#{quote(value, field: field, klass: klass)}"
        end
      end

      def apply_placeholders(template, args)
        raise ArgumentError, 'template must be a String' unless template.is_a?(String)
        raise ArgumentError, 'args must be an Array' unless args.is_a?(Array)

        needed = count_placeholders(template)
        unless needed == args.length
          raise ArgumentError,
                "expected #{needed} args for #{needed} placeholders, got #{args.length}"
        end

        return template if needed.zero?

        # A template bind must own an entire comparison operand. Substituting
        # another quoted literal inside caller-supplied quotes can reopen it.
        values = args.each
        template.split(/(&&|\|\|)/, -1).map.with_index do |part, index|
          next part if index.odd?

          match = part.match(/\A\s*\(*\s*([[:alnum:]_.-]+)\s*:?(?:!=|>=|<=|[=<>]|NOT\s+IN|IN)?\s*\?\s*\)*\s*\z/i)
          raise ArgumentError, 'placeholder must be a complete filter operand' unless match

          field = field_name(match[1])
          part.sub('?') { quote(values.next, field: field) }
        end.join
      end

      def count_placeholders(template)
        template.scan(/(?<!\\)\?/).length
      end
    end
  end
end
