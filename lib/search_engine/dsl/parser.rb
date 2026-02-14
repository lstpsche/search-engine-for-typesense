# frozen_string_literal: true

module SearchEngine
  module DSL
    # Safe parser that converts Relation#where inputs into validated AST nodes.
    #
    # Supported inputs:
    # - Hash: { field => value } (scalar => Eq, Array => In)
    # - Raw String: full Typesense fragment (escape hatch) => Raw
    # - Fragment + args: ["price > ?", 100] or ["brand_id IN ?", [1,2,3]]
    # - Joined Hash: { assoc => { field => value } } => LHS "$assoc.field"
    #
    # Validation/coercion:
    # - Field names validated against model attributes (when available)
    # - Booleans coerced from "true"/"false" strings when attribute type is boolean
    # - Date/DateTime coerced to Time.utc; Arrays flattened one level and compacted
    # - When using joined fields, association names are validated via klass.join_for
    #   and (optionally) required to be present in relation joins when +joins+ is provided.
    module Parser
      module_function

      # Accepted hints that should be treated as time-like for coercion purposes.
      TIME_LIKE_HINTS = [
        :time, 'time', :datetime, 'datetime',
        :time_string, 'time_string', :datetime_string, 'datetime_string',
        Time, Date, DateTime
      ].freeze

      # Parse a where input into AST nodes.
      #
      # Return convention:
      # - Single predicate => a single AST node
      # - Hash with multiple keys or list-of-inputs => Array<AST::Node>
      #
      # @param input [Hash, String, Array]
      # @param args [Array] optional, used only when +input+ is a template String
      # @param klass [Class] SearchEngine::Base subclass used for attribute validation
      # @param joins [Array<Symbol>, nil] Optional list of applied join names on the relation
      # @return [SearchEngine::AST::Node, Array<SearchEngine::AST::Node>]
      # @raise [SearchEngine::Errors::InvalidField, SearchEngine::Errors::InvalidOperator, SearchEngine::Errors::InvalidType]
      def parse(input, klass:, args: [], joins: nil)
        case input
        when Hash
          parse_hash(input, klass: klass, joins: joins)
        when String
          if placeholders?(input)
            needed = count_placeholders(input)
            ensure_placeholder_arity!(needed, args.length, input)
            parse_template(input, args, klass: klass)
          else
            parse_raw(input)
          end
        when Array
          parse_array_input(input, klass: klass, joins: joins)
        when Symbol
          # Back-compat: treat symbol as raw fragment name
          parse_raw(input.to_s)
        else
          raise ArgumentError, "Parser: unsupported input #{input.class}"
        end
      end

      # Parse a list of heterogenous where arguments (as passed to Relation#where).
      # @param list [Array]
      # @param klass [Class]
      # @param joins [Array<Symbol>, nil]
      # @return [Array<SearchEngine::AST::Node>]
      def parse_list(list, klass:, joins: nil)
        items = Array(list).flatten.compact
        return [] if items.empty?

        nodes = []
        i = 0
        while i < items.length
          entry = items[i]
          case entry
          when Hash
            nodes.concat(Array(parse_hash(entry, klass: klass, joins: joins)))
            i += 1
          when String
            if placeholders?(entry)
              needed = count_placeholders(entry)
              args_for_template = items[(i + 1)..(i + needed)] || []
              ensure_placeholder_arity!(needed, args_for_template.length, entry)
              nodes << parse_template(entry, args_for_template, klass: klass)
              i += 1 + needed
            else
              nodes << parse_raw(entry)
              i += 1
            end
          when Symbol
            nodes << parse_raw(entry.to_s)
            i += 1
          when Array
            nodes << parse_array_entry(entry, klass: klass, joins: joins)
            i += 1
          else
            raise ArgumentError, "Parser: unsupported where argument #{entry.class}"
          end
        end
        nodes
      end

      # --- Internals -------------------------------------------------------

      def parse_array_entry(entry, klass:, joins: nil)
        return parse_raw(entry.to_s) unless entry.first.is_a?(String)
        return parse_list(entry, klass: klass, joins: joins) unless placeholders?(entry.first)

        template = entry.first
        args_list = if entry.length == 2 && entry[1].is_a?(Array)
                      [entry[1]]
                    else
                      entry[1..]
                    end
        needed = count_placeholders(template)
        ensure_placeholder_arity!(needed, Array(args_list).length, template)
        parse_template(template, Array(args_list), klass: klass)
      end

      def parse_hash(hash, klass:, joins: nil)
        raise ArgumentError, 'Parser: hash input must be a Hash' unless hash.is_a?(Hash)

        attrs = safe_attributes_map(klass)
        validate_hash_keys!(hash, attrs, klass)

        pairs = []

        hash.each do |k, v|
          key_sym = k.to_sym
          value = v

          if value.is_a?(Hash)
            # assoc => { field => value }
            validate_assoc_and_join!(klass, key_sym, joins)

            value.each do |inner_field, inner_value|
              field_sym = inner_field.to_sym
              path = "$#{key_sym}.#{field_sym}"
              SearchEngine::Joins::Guard.ensure_single_hop_path!(path)

              # Resolve target klass for joined-field type coercion and validate field best-effort
              cfg = klass.join_for(key_sym)
              target_klass = begin
                SearchEngine.collection_for(cfg[:collection])
              rescue StandardError
                nil
              end
              begin
                SearchEngine::Joins::Guard.validate_joined_field!(cfg, field_sym)
              rescue StandardError
                # skip strictly when registry/attributes missing or join missing
              end

              coercion_klass = target_klass || klass
              if array_like?(inner_value)
                values = normalize_array_values(inner_value, field: field_sym, klass: coercion_klass)
                ensure_non_empty_values!(values, field: field_sym, klass: coercion_klass)
                pairs << SearchEngine::AST.in_(path, values)
              else
                coerced = coerce_value_for_field(inner_value, field: field_sym, klass: coercion_klass)
                pairs << SearchEngine::AST.eq(path, coerced)
              end
            end
          else
            field = key_sym

            if array_like?(value)
              values = normalize_array_values(value, field: field, klass: klass)
              ensure_non_empty_values!(values, field: field, klass: klass)
              pairs << SearchEngine::AST.in_(field, values)
            else
              coerced = coerce_value_for_field(value, field: field, klass: klass)
              pairs << SearchEngine::AST.eq(field, coerced)
            end
          end
        end

        return pairs.first if pairs.length == 1

        pairs
      end

      def parse_template(template, args, klass:)
        raise ArgumentError, 'Parser: template must be a String' unless template.is_a?(String)

        args = Array(args)
        m = template.match(/\A\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|>=|<=|>|<|IN|NOT\s+IN|MATCHES|PREFIX)\s*\?\s*\z/i)
        unless m
          raise SearchEngine::Errors::InvalidOperator.new(
            "invalid template '#{template}'. Supported: =, !=, >, >=, <, <=, IN, NOT IN, MATCHES, PREFIX",
            doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#troubleshooting',
            details: { template: template }
          )
        end

        field_raw = m[1]
        op = m[2].upcase.gsub(/\s+/, ' ')

        field_sym = field_raw.to_sym
        attrs = safe_attributes_map(klass)
        validate_field!(field_sym, attrs, klass)

        case op
        when '='
          SearchEngine::AST.eq(field_sym, coerce_value_for_field(args.first, field: field_sym, klass: klass))
        when '!='
          SearchEngine::AST.not_eq(field_sym, coerce_value_for_field(args.first, field: field_sym, klass: klass))
        when '>'
          SearchEngine::AST.gt(field_sym, coerce_value_for_field(args.first, field: field_sym, klass: klass))
        when '>='
          SearchEngine::AST.gte(field_sym, coerce_value_for_field(args.first, field: field_sym, klass: klass))
        when '<'
          SearchEngine::AST.lt(field_sym, coerce_value_for_field(args.first, field: field_sym, klass: klass))
        when '<='
          SearchEngine::AST.lte(field_sym, coerce_value_for_field(args.first, field: field_sym, klass: klass))
        when 'IN'
          values = normalize_array_values(args.first, field: field_sym, klass: klass)
          ensure_non_empty_values!(values, field: field_sym, klass: klass)
          SearchEngine::AST.in_(field_sym, values)
        when 'NOT IN'
          values = normalize_array_values(args.first, field: field_sym, klass: klass)
          ensure_non_empty_values!(values, field: field_sym, klass: klass)
          SearchEngine::AST.not_in(field_sym, values)
        when 'MATCHES'
          SearchEngine::AST.matches(field_sym, args.first)
        when 'PREFIX'
          SearchEngine::AST.prefix(field_sym, String(args.first))
        else
          raise SearchEngine::Errors::InvalidOperator, "unsupported operator '#{op}'"
        end
      end

      def parse_raw(fragment)
        SearchEngine::AST.raw(String(fragment))
      end

      # Heuristic: treat an Array input as a template+args when it starts with a
      # String that has placeholders; otherwise treat as list-of-inputs.
      def parse_array_input(arr, klass:, joins: nil)
        return [] if arr.nil?

        arr = Array(arr)

        if arr.first.is_a?(String) && placeholders?(arr.first) && arr.length >= 2
          template = arr.first
          args_list = if arr.length == 2 && arr[1].is_a?(Array)
                        [arr[1]]
                      else
                        arr[1..]
                      end
          needed = count_placeholders(template)
          ensure_placeholder_arity!(needed, Array(args_list).length, template)
          parse_template(template, Array(args_list), klass: klass)
        else
          parse_list(arr, klass: klass, joins: joins)
        end
      end

      # --- Utilities -------------------------------------------------------

      def placeholders?(str)
        str.is_a?(String) && SearchEngine::Filters::Sanitizer.count_placeholders(str).positive?
      end

      def count_placeholders(str)
        SearchEngine::Filters::Sanitizer.count_placeholders(str)
      end

      def ensure_placeholder_arity!(needed, provided, template)
        return if needed == provided

        raise SearchEngine::Errors::InvalidOperator.new(
          "expected #{needed} args for #{needed} placeholders in template '#{template}', got #{provided}.",
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#troubleshooting',
          details: { needed: needed, provided: provided, template: template }
        )
      end

      def safe_attributes_map(klass)
        if klass.respond_to?(:attributes)
          base = klass.attributes || {}
          return base if base.key?(:id)

          # Infer implicit id type:
          # - When identify_by is declared as :id, keep integer to preserve numeric coercions in tests
          # - Otherwise, default to :string (composed or custom ids)
          if klass.instance_variable_defined?(:@__identify_by_kind__) &&
             klass.instance_variable_get(:@__identify_by_kind__) == :symbol &&
             klass.instance_variable_defined?(:@__identify_by_symbol__) &&
             klass.instance_variable_get(:@__identify_by_symbol__) == :id
            base.merge(id: :integer)
          else
            base.merge(id: :string)
          end
        else
          { id: :string }
        end
      end

      def validate_hash_keys!(hash, attributes_map, klass)
        return if hash.nil? || hash.empty?

        known = attributes_map.keys.map(&:to_sym)
        # Exclude keys whose values are Hash (treated as assoc => { ... })
        candidate_keys = hash.reject { |_, v| v.is_a?(Hash) }.keys
        unknown = candidate_keys.map(&:to_sym) - known
        return if unknown.empty?

        return unless strict_fields?

        raise build_invalid_field_error(unknown.first, known, klass)
      end

      def validate_field!(field, attributes_map, klass)
        return if attributes_map.nil? || attributes_map.empty?
        return unless strict_fields?

        sym = field.to_sym
        return if attributes_map.key?(sym)

        known = attributes_map.keys.map(&:to_sym)
        raise build_invalid_field_error(sym, known, klass)
      end

      def array_like?(value)
        value.is_a?(Array)
      end

      def normalize_array_values(value, field:, klass:)
        arr = Array(value).flatten(1).compact
        arr.map { |v| coerce_value_for_field(v, field: field, klass: klass) }
      end

      def ensure_non_empty_values!(values, field:, klass:)
        return if values.is_a?(Array) && !values.empty?

        raise SearchEngine::Errors::InvalidType.new(
          invalid_type_message(field: field, klass: klass, expectation: 'a non-empty Array', got: values),
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#troubleshooting',
          details: { field: field, got_class: values.class.name }
        )
      end

      def coerce_value_for_field(value, field:, klass:)
        # Unwrap single-element array types to their inner hint (e.g., [:string] -> :string)
        type_hint = begin
          t = safe_attributes_map(klass)[field.to_sym]
          t.is_a?(Array) && t.size == 1 ? t.first : t
        rescue StandardError
          nil
        end
        coerced = coerce_value(value, type_hint: type_hint, field: field, klass: klass)
        return coerced unless coerced.equal?(:__no_coercion__)

        # If generic coercion returned :__no_coercion__, try to coerce based on type hint
        case type_hint
        when :boolean
          coerce_boolean(value, type_hint)
        when :time, :datetime, :time_string, :datetime_string
          coerce_time(value, type_hint, field: field, klass: klass)

        when :integer
          coerce_numeric(value, type_hint, field: field, klass: klass)
        when :float, :decimal
          coerce_numeric(value, type_hint, field: field, klass: klass)
        when :string
          # Ensure string-typed fields compare as strings (e.g., 1070 -> "1070")
          value.to_s
        else
          value # No specific coercion for this type hint
        end
      end

      def coerce_value(value, type_hint: nil, field: nil, klass: nil)
        coerced_bool = coerce_boolean(value, type_hint)
        return coerced_bool unless coerced_bool.equal?(:__no_coercion__)

        coerced_time = coerce_time(value, type_hint, field: field, klass: klass)
        return coerced_time unless coerced_time.equal?(:__no_coercion__)

        coerced_number = coerce_numeric(value, type_hint, field: field, klass: klass)
        return coerced_number unless coerced_number.equal?(:__no_coercion__)

        value
      end

      def coerce_boolean(value, type_hint)
        return :__no_coercion__ unless type_boolean?(type_hint) && value.is_a?(String)

        lc = value.strip.downcase
        return true if lc == 'true'
        return false if lc == 'false'

        raise SearchEngine::Errors::InvalidType.new(
          invalid_type_message(field: nil, klass: nil, expectation: 'boolean', got: value),
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#troubleshooting',
          details: { got: value }
        )
      end
      private_class_method :coerce_boolean

      def coerce_time(value, type_hint, field:, klass:)
        # Direct time-like Ruby objects
        return value.to_time.utc if value.is_a?(DateTime) || value.is_a?(Date)
        return (value.utc? ? value : value.utc) if value.is_a?(Time)

        # Only attempt further coercion when hint is time-like
        return :__no_coercion__ unless type_time?(type_hint)

        case value
        when Integer
          Time.at(value).utc
        when Numeric
          Time.at(value.to_i).utc
        when String
          if value.match?(/^\d+$/)
            Time.at(Integer(value, 10)).utc
          else
            require 'time'
            Time.parse(value).utc
          end
        else
          :__no_coercion__
        end
      rescue StandardError
        raise SearchEngine::Errors::InvalidType.new(
          invalid_type_message(field: field, klass: klass, expectation: 'time', got: value),
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#troubleshooting',
          details: { field: field, got: value }
        )
      end
      private_class_method :coerce_time

      def coerce_numeric(value, type_hint, field:, klass:)
        if type_integer?(type_hint)
          return value if value.is_a?(Integer)

          if value.is_a?(String)
            begin
              int_val = Integer(value, 10)
              return int_val
            rescue StandardError
              raise SearchEngine::Errors::InvalidType.new(
                invalid_type_message(field: field, klass: klass, expectation: 'integer', got: value),
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#troubleshooting',
                details: { field: field, got: value }
              )
            end
          end

          if value.is_a?(Numeric)
            raise SearchEngine::Errors::InvalidType.new(
              invalid_type_message(field: field, klass: klass, expectation: 'integer', got: value),
              doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#troubleshooting',
              details: { field: field, got: value }
            )
          end

          return :__no_coercion__
        end

        if type_float?(type_hint)
          return value if value.is_a?(Integer)
          return value.to_f if value.is_a?(Numeric)

          if value.is_a?(String)
            begin
              Float(value)
            rescue StandardError
              raise SearchEngine::Errors::InvalidType.new(
                invalid_type_message(field: field, klass: klass, expectation: 'numeric', got: value),
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#troubleshooting',
                details: { field: field, got: value }
              )
            end
            return value.to_f
          end
        end

        :__no_coercion__
      end
      private_class_method :coerce_numeric

      def type_boolean?(hint)
        case hint
        when :boolean, 'boolean', TrueClass, FalseClass then true
        else false
        end
      end

      def type_integer?(hint)
        case hint
        when :integer, 'integer', Integer then true
        else false
        end
      end

      def type_float?(hint)
        case hint
        when :float, 'float', :decimal, 'decimal', Float then true
        else false
        end
      end

      def type_time?(hint)
        TIME_LIKE_HINTS.include?(hint)
      end

      def strict_fields?
        begin
          cfg = SearchEngine.config
          val = cfg.respond_to?(:strict_fields) ? cfg.strict_fields : nil
          return !!val unless val.nil?
        rescue StandardError
          # default below
        end
        true
      end

      def build_invalid_field_error(field, known, klass)
        klass_name = klass.respond_to?(:name) && klass.name ? klass.name : klass.to_s
        suggestion = did_you_mean(field, known)
        msg = "unknown field #{field.inspect} for #{klass_name}"
        msg += " (did you mean #{suggestion.inspect}?)" if suggestion
        SearchEngine::Errors::InvalidField.new(msg)
      end

      def did_you_mean(field, known)
        return nil if known.nil? || known.empty?

        begin
          require 'did_you_mean'
          require 'did_you_mean/levenshtein'
        rescue StandardError
          return nil
        end

        candidates = known.map(&:to_s)
        input = field.to_s

        # Compute minimal Levenshtein distance deterministically
        distances = candidates.each_with_object({}) do |cand, acc|
          acc[cand] = DidYouMean::Levenshtein.distance(input, cand)
        end
        min = distances.values.min
        return nil if min.nil? || min > 2

        best = distances.select { |_, d| d == min }.keys
        return nil unless best.length == 1

        best.first.to_sym
      end

      def invalid_type_message(field:, klass:, expectation:, got:)
        klass_name = klass.respond_to?(:name) && klass.name ? klass.name : klass.to_s
        %(expected #{field.inspect} to be #{expectation} for #{klass_name} (got #{got.class}: #{got.inspect}))
      end

      def validate_assoc_and_join!(klass, assoc_name, joins)
        # Validate association exists and config completeness
        begin
          SearchEngine::Joins::Guard.ensure_config_complete!(klass, assoc_name)
        rescue SearchEngine::Errors::InvalidJoin
          # Provide high-level guidance with suggestions if available
          model_name = klass.respond_to?(:name) && klass.name ? klass.name : klass.to_s
          msg = "association :#{assoc_name} is not declared on #{model_name} (declare with `join :#{assoc_name}, ...`)"
          raise SearchEngine::Errors::InvalidJoin, msg
        end

        # When enforcing applied joins, ensure relation has the association
        return if joins.nil? || Array(joins).include?(assoc_name)

        raise SearchEngine::Errors::JoinNotApplied.new(
          "Call .joins(:#{assoc_name}) before filtering/sorting on #{assoc_name} fields",
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/joins#troubleshooting',
          details: { assoc: assoc_name, used_for: 'filtering' }
        )
      end
    end
  end
end
