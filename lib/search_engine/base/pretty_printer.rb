# frozen_string_literal: true

require 'active_support/concern'

module SearchEngine
  class Base
    # Pretty printing and inspect helpers for console output.
    # Extracted from Base with identical behavior.
    module PrettyPrinter
      extend ActiveSupport::Concern

      include SearchEngine::Base::DisplayCoercions

      # Human-friendly inspect that lists declared attributes and, when present,
      # unknown attributes captured during hydration.
      # @return [String]
      def inspect
        pairs = __attribute_pairs_for_render
        hex_id = begin
          # Mimic Ruby's default hex object id formatting
          format('0x%014x', object_id << 1)
        rescue StandardError
          object_id
        end
        return "#<#{self.class.name}:#{hex_id}>" if pairs.empty?

        lines = pairs.map do |(k, v)|
          rendered = if v.is_a?(Array) || v.is_a?(Hash)
                       __se_symbolize_for_inspect(v).inspect
                     else
                       v.inspect
                     end
          "#{k}: #{rendered}"
        end
        "#<#{self.class.name}:#{hex_id}\n  #{lines.join(",\n  ")}>"
      end

      # Pretty-print with attributes on separate lines for readability in consoles.
      # Integrates with PP so arrays of models render multiline.
      # @param pp [PP]
      # @return [void]
      def pretty_print(pp)
        hex_id = begin
          format('0x%014x', object_id << 1)
        rescue StandardError
          object_id
        end
        pairs = __attribute_pairs_for_render
        pp.group(2, "#<#{self.class.name}:#{hex_id} ", '>') do
          if pairs.empty?
            pp.breakable ''
          else
            pp.breakable ''
            pairs.each_with_index do |(k, v), idx|
              if v.is_a?(Array) || v.is_a?(Hash)
                pp.text("#{k}:")
                pp.nest(2) do
                  pp.breakable ' '
                  pp.pp(__se_symbolize_for_inspect(v))
                end
              else
                pp.text("#{k}: ")
                pp.pp(v)
              end
              if idx < pairs.length - 1
                pp.text(',')
                pp.breakable ' '
              end
            end
          end
        end
      end

      private

      # Build ordered list of attribute pairs for rendering:
      # - Declared attributes in declaration order (with id rendered first when present)
      # - Followed by unknown attributes (when present)
      # @return [Array<[String, Object]>]
      def __attribute_pairs_for_render
        declared = __se_declared_attributes
        pairs = []

        __se_append_declared_id!(pairs, declared)
        __se_render_present_declared_attributes!(pairs, declared)

        join_names = __se_declared_join_names
        __se_render_present_declared_joins!(pairs, join_names)

        __se_append_unknown_attribute_pairs(pairs, declared)
      end

      # Return declared attribute map with resilience to missing APIs.
      def __se_declared_attributes
        self.class.respond_to?(:attributes) ? (self.class.attributes || {}) : {}
      rescue StandardError
        {}
      end

      # Render id first if declared and present in the hydrated document
      def __se_append_declared_id!(pairs, declared)
        return unless declared.key?(:id) && instance_variable_defined?('@id')

        pairs << ['id', __se_coerce_id_for_display(instance_variable_get('@id'))]
      end

      # Render only declared attributes that were present in the hydrated document
      def __se_render_present_declared_attributes!(pairs, declared)
        declared.each_key do |name|
          next if name.to_s == 'id'

          begin
            next unless self.class.respond_to?(:valid_attribute_reader_name?) &&
                        self.class.valid_attribute_reader_name?(name)
          rescue StandardError
            next if name.to_s.include?('.')
          end

          ivar_name = "@#{name}"
          next unless instance_variable_defined?(ivar_name)

          raw_value = instance_variable_get(ivar_name)
          value_for_render = if name.to_s == 'doc_updated_at' && !raw_value.nil?
                               __se_coerce_doc_updated_at_for_display(raw_value)
                             else
                               raw_value
                             end
          pairs << [name.to_s, value_for_render]
        end
      end

      # Collect declared join names as strings.
      def __se_declared_join_names
        joins = self.class.respond_to?(:joins_config) ? (self.class.joins_config || {}) : {}
        joins.keys.map(&:to_s)
      rescue StandardError
        []
      end

      # Render declared joined attributes that were present in the hydrated document
      def __se_render_present_declared_joins!(pairs, join_names)
        join_names.each do |join_name|
          ivar_name = "@#{join_name}"
          next unless instance_variable_defined?(ivar_name)

          value = instance_variable_get(ivar_name)
          next if value.nil? || (value.respond_to?(:empty?) && value.empty?)

          pairs << [join_name, value]
        end
      end

      # Group "$assoc.field" unknown attributes into a nested Hash under "$assoc" for rendering.
      # Prefers existing shapes when "$assoc" key already exists in the payload (Hash/Array).
      # Returns [passthrough(Map<key->val>), grouped(Map<assoc->Hash>), assoc_order(Array<String>)].
      def __se_group_join_fields_for_render(extras)
        grouped = {}
        assoc_order = []
        passthrough = {}

        extras.each do |k, v|
          key = k.to_s
          if key.start_with?('$') && key.include?('.') && !v.is_a?(Hash) && !v.is_a?(Array)
            assoc_key, field = key.split('.', 2)
            assoc = assoc_key.delete_prefix('$')
            # Respect existing nested shape if present
            if extras.key?(assoc)
              passthrough[key] = v
            else
              unless grouped.key?(assoc)
                grouped[assoc] = {}
                assoc_order << assoc
              end
              grouped[assoc][field] = v
            end
          else
            passthrough[key] = v
          end
        end

        [passthrough, grouped, assoc_order]
      end

      # Append unknown attributes, grouping join fields and preserving nested shapes.
      def __se_append_unknown_attribute_pairs(pairs, declared)
        extras = unknown_attributes
        return pairs if extras.empty?

        __se_maybe_render_unknown_id_first!(pairs, declared, extras)

        selected_nested = __se_selected_nested_assocs_for_render
        __se_render_existing_nested_assoc_pairs!(pairs, extras, selected_nested)

        passthrough, grouped, assoc_order = __se_group_join_fields_for_render(extras)
        __se_render_grouped_scalar_assoc_pairs!(pairs, assoc_order, grouped)
        __se_render_passthrough_unknowns!(pairs, passthrough)
        pairs
      end

      # Ensure id appears first when not declared but present in unknowns
      def __se_maybe_render_unknown_id_first!(pairs, declared, extras)
        return if declared.key?(:id)

        id_v = extras['id'] || extras[:id]
        pairs.unshift(['id', __se_coerce_id_for_display(id_v)]) unless id_v.nil?
      end

      # Return selection context for nested assocs used during render
      # @return [Array<String>]
      def __se_selected_nested_assocs_for_render
        Array(
          instance_variable_defined?(:@__se_selected_nested_assocs__) &&
          instance_variable_get(:@__se_selected_nested_assocs__)
        ).map(&:to_s)
      end

      # Render already nested structures for assoc keys (either "$assoc" or plain assoc)
      def __se_render_existing_nested_assoc_pairs!(pairs, extras, selected_nested)
        declared_joins = begin
          self.class.respond_to?(:joins_config) ? (self.class.joins_config || {}) : {}
        rescue StandardError
          {}
        end
        assoc_names = declared_joins.keys.map(&:to_s)

        extras.each do |k, v|
          key = k.to_s
          next unless v.is_a?(Array) || v.is_a?(Hash)

          assoc = key.start_with?('$') ? key.delete_prefix('$') : key
          next unless assoc_names.include?(assoc)

          already = pairs.any? { |(name, _)| name == assoc }
          next if already
          next if selected_nested.any? && !selected_nested.include?(assoc)

          pairs << [assoc, __se_symbolize_for_inspect(v)]
        end
      end

      # Render grouped scalar $assoc.field maps under assoc as an array-of-hashes
      def __se_render_grouped_scalar_assoc_pairs!(pairs, assoc_order, grouped)
        assoc_order.each do |assoc|
          next if pairs.any? { |(name, _)| name == assoc }

          pairs << [assoc.to_s, [__se_symbolize_for_inspect(grouped[assoc])]]
        end
      end

      # Render remaining passthrough unknowns with special handling for doc_updated_at
      def __se_render_passthrough_unknowns!(pairs, passthrough)
        passthrough.each do |k, v|
          key = k.to_s
          next if key == 'id' || key.start_with?('$', '.')
          # Avoid duplicating assoc entries already rendered
          next if pairs.any? { |(name, _)| name == key }

          rendered = key == 'doc_updated_at' && !v.nil? ? __se_coerce_doc_updated_at_for_display(v) : v
          pairs << [key, rendered]
        end
      end

      # Symbolize keys for inspect to avoid symbol bloat (deep).
      def __se_symbolize_for_inspect(value)
        case value
        when Array
          value.map { |element| __se_symbolize_for_inspect(element) }
        when Hash
          value.each_with_object({}) do |(k, v), acc|
            key = k.is_a?(String) || k.is_a?(Symbol) ? k.to_sym : k
            acc[key] = __se_symbolize_for_inspect(v)
          end
        else
          value
        end
      end

      # Coerce id for display: if it's a numeric-looking string and model's id source is integer-like,
      # render as Integer; otherwise render as-is. This does not mutate underlying value.
      def __se_coerce_id_for_display(value)
        v = value
        return v if v.nil?

        # Determine if this collection is ActiveRecord-sourced with integer/bigint PK and no custom identify_by
        begin
          dsl = self.class.instance_variable_get(:@__mapper_dsl__)
          src = dsl&.dig(:source, :type)
          if src.to_s == 'active_record' && !self.class.instance_variable_defined?(:@identify_by_proc)
            model = dsl&.dig(:source, :options, :model)
            if model.respond_to?(:columns_hash)
              pk = begin
                model.primary_key
              rescue StandardError
                'id'
              end
              col = begin
                model.columns_hash[pk.to_s]
              rescue StandardError
                nil
              end
              if col && %i[integer bigint].include?(col.type) && v.is_a?(String) && v.match?(/^[-+]?\d+$/)
                # Try to display numeric-looking strings as Integer
                return Integer(v)
              end
            end
          end
        rescue StandardError
          # fall through
        end

        v
      end
    end
  end
end
