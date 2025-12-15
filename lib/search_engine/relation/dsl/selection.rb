# frozen_string_literal: true

module SearchEngine
  class Relation
    module DSL
      # Selection-related chainers and normalizers.
      # These methods are mixed into Relation's DSL and must preserve copy-on-write semantics.
      module Selection
        # Select a subset of fields for Typesense `include_fields`.
        # @param fields [Array<Symbol,String,Hash,Array>]
        # @return [SearchEngine::Relation]
        # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/field-selection`
        def select(*fields)
          normalized = normalize_select_input(fields)
          spawn do |s|
            existing_base = Array(s[:select])
            merged_base = (existing_base + normalized[:base]).each_with_object([]) do |f, acc|
              acc << f unless acc.include?(f)
            end
            s[:select] = merged_base

            existing_nested = s[:select_nested] || {}
            existing_order = Array(s[:select_nested_order])

            normalized[:nested_order].each do |assoc|
              new_fields = Array(normalized[:nested][assoc])
              next if new_fields.empty?

              old_fields = Array(existing_nested[assoc])
              merged_fields = (old_fields + new_fields).each_with_object([]) do |name, acc|
                acc << name unless acc.include?(name)
              end
              existing_nested = existing_nested.merge(assoc => merged_fields)
              existing_order << assoc unless existing_order.include?(assoc)
            end

            s[:select_nested] = existing_nested
            s[:select_nested_order] = existing_order
          end
        end

        # Convenience alias for `select` supporting nested include_fields input.
        # @return [SearchEngine::Relation]
        alias_method :include_fields, :select

        # Exclude a subset of fields from the final selection.
        # @param fields [Array<Symbol,String,Hash,Array>]
        # @return [SearchEngine::Relation]
        # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/field-selection`
        #
        # When excluding, you may also pass a joined association name (after calling
        # `joins(:assoc)`) to exclude the entire joined payload from the response.
        # For example: `exclude(:authors)` compiles to Typesense `exclude_fields: "$authors(*,doc_updated_at)"`.
        def exclude(*fields)
          normalized = normalize_select_input(fields, context: 'excluding fields')
          spawn do |s|
            existing_base = Array(s[:exclude])
            merged_base = (existing_base + normalized[:base]).each_with_object([]) do |f, acc|
              acc << f unless acc.include?(f)
            end
            s[:exclude] = merged_base

            existing_nested = s[:exclude_nested] || {}
            existing_order = Array(s[:exclude_nested_order])

            normalized[:nested_order].each do |assoc|
              new_fields = Array(normalized[:nested][assoc])
              next if new_fields.empty?

              old_fields = Array(existing_nested[assoc])
              merged_fields = (old_fields + new_fields).each_with_object([]) do |name, acc|
                acc << name unless acc.include?(name)
              end
              existing_nested = existing_nested.merge(assoc => merged_fields)
              existing_order << assoc unless existing_order.include?(assoc)
            end

            s[:exclude_nested] = existing_nested
            s[:exclude_nested_order] = existing_order
          end
        end

        # Convenience alias for `exclude` supporting nested exclude_fields input.
        # Mirrors Typesense param naming for API symmetry with `include_fields`.
        # @return [SearchEngine::Relation]
        alias_method :exclude_fields, :exclude

        # Replace the selected fields list (Typesense `include_fields`).
        # @param fields [Array<#to_sym,#to_s>]
        # @return [SearchEngine::Relation]
        # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/field-selection`
        def reselect(*fields)
          normalized = normalize_select_input(fields)

          base_empty = Array(normalized[:base]).empty?
          nested_empty = normalized[:nested_order].all? { |a| Array(normalized[:nested][a]).empty? }
          raise ArgumentError, 'reselect: provide at least one non-blank field' if base_empty && nested_empty

          spawn do |s|
            s[:select] = normalized[:base]
            s[:select_nested] = normalized[:nested]
            s[:select_nested_order] = normalized[:nested_order]
            s[:exclude] = []
            s[:exclude_nested] = {}
            s[:exclude_nested_order] = []
          end
        end

        private

        # Base-only normalization used internally and for legacy callers.
        def normalize_select(fields)
          list = Array(fields).flatten.compact
          return [] if list.empty?

          known_attrs = safe_attributes_map
          known = known_attrs.keys.map(&:to_s)

          ordered = []
          list.each do |f|
            name = f.to_s.strip
            raise ArgumentError, 'select: field names must be non-empty' if name.empty?

            if !known.empty? && !known.include?(name)
              suggestions = suggest_fields(name.to_sym, known_attrs.keys.map(&:to_sym))
              suggest = if suggestions.empty?
                          ''
                        elsif suggestions.length == 1
                          " (did you mean :#{suggestions.first}?)"
                        else
                          last = suggestions.last
                          others = suggestions[0..-2].map { |s| ":#{s}" }.join(', ')
                          " (did you mean #{others}, or :#{last}?)"
                        end
              raise SearchEngine::Errors::UnknownField,
                    "UnknownField: unknown field #{name.inspect} for #{klass_name_for_inspect}#{suggest}"
            end

            ordered << name unless ordered.include?(name)
          end
          ordered
        end

        # Extended normalization supporting nested association selections.
        # Returns a Hash with keys: :base, :nested, :nested_order.
        def normalize_select_input(fields, context: 'selecting fields')
          list = Array(fields).flatten.compact
          return { base: [], nested: {}, nested_order: [] } if list.empty?

          base = []
          nested = {}
          nested_order = []

          add_base = build_add_base_proc(context, base, nested, nested_order)
          add_nested = build_add_nested_proc(context, nested, nested_order)

          process_selection_list!(list, add_base, add_nested)

          { base: base.uniq, nested: nested, nested_order: nested_order }
        end

        # Base-field adder that, in excluding context, treats a bare joined assoc
        # token as a request to exclude the entire association payload (sentinel :__all).
        def build_add_base_proc(context, base, nested, nested_order)
          lambda do |val|
            # Only special-case in excluding context; otherwise fallback to base normalization.
            if context.to_s.include?('exclud')
              name = val.to_s.strip
              unless name.empty?
                key = name.to_sym
                begin
                  # Validate join existence and that it was applied on this relation.
                  @klass.join_for(key)
                  SearchEngine::Joins::Guard.ensure_join_applied!(joins_list, key, context: context)

                  # Mark full association exclusion via sentinel; exclude wins later in compiler.
                  nested[key] = [:__all]
                  nested_order << key unless nested_order.include?(key)
                  return
                rescue StandardError
                  # Not a known/applied join; fall through to base normalization
                end
              end
            end

            # Default behavior: treat as base field(s) and validate accordingly.
            base_fields = normalize_select([val])
            base.concat(base_fields)
          end
        end

        def build_add_nested_proc(context, nested, nested_order)
          lambda do |assoc, values|
            key = assoc.to_sym
            @klass.join_for(key)
            SearchEngine::Joins::Guard.ensure_join_applied!(joins_list, key, context: context)

            items = case values
                    when Array then values
                    when nil then []
                    else [values]
                    end
            names = items.flatten.compact.map(&:to_s).map(&:strip).reject(&:empty?)
            return if names.empty?

            cfg = @klass.join_for(key)
            names.each { |fname| SearchEngine::Joins::Guard.validate_joined_field!(cfg, fname, source_klass: @klass) }

            existing = Array(nested[key])
            merged = (existing + names).each_with_object([]) { |n, acc| acc << n unless acc.include?(n) }
            nested[key] = merged
            nested_order << key unless nested_order.include?(key)
          end
        end

        def process_selection_list!(list, add_base, add_nested)
          i = 0
          while i < list.length
            entry = list[i]
            case entry
            when Hash
              entry.each { |k, v| add_nested.call(k, v) }
              i += 1
            when Symbol, String
              add_base.call(entry)
              i += 1
            when Array
              inner = Array(entry).flatten.compact
              inner.each { |el| list << el }
              i += 1
            else
              raise SearchEngine::Errors::ConflictingSelection,
                    "ConflictingSelection: unsupported input #{entry.class} in selection"
            end
          end
        end
      end
    end
  end
end
