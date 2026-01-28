# frozen_string_literal: true

module SearchEngine
  class Relation
    module DSL
      # Filter-related chainers and normalizers.
      # These methods are mixed into Relation's DSL and must preserve copy-on-write semantics.
      module Filters
        # AR-style where.not support via a small chain proxy.
        class WhereChain
          def initialize(relation)
            @relation = relation
          end

          # Replace positive predicates with negated form.
          # Supports Hash, String templates, Arrays (delegated to parser with a negation flag).
          # @param args [Array<Object>]
          # @return [SearchEngine::Relation]
          def not(*args)
            nodes = Array(@relation.send(:build_ast_with_empty_array_rewrites, args, negated: true))

            # Invert non-hidden predicates (Eq, In) returned by the builder
            negated = nodes.map do |node|
              case node
              when SearchEngine::AST::Eq
                SearchEngine::AST.not_eq(node.field, node.value)
              when SearchEngine::AST::In
                SearchEngine::AST.not_in(node.field, node.values)
              else
                node
              end
            end

            @relation.send(:spawn) do |s|
              s[:ast] = Array(s[:ast]) + negated
              s[:filters] = Array(s[:filters])
            end
          end
        end

        # Add filters to the relation.
        # When called without arguments, it's a no-op and returns the relation (idempotent).
        # @param args [Array<Object>] filter arguments
        # @return [SearchEngine::Relation, WhereChain]
        def where(*args)
          return self if args.nil? || args.empty?

          ast_nodes = build_ast_with_empty_array_rewrites(args, negated: false)
          fragments = normalize_where(args)
          spawn do |s|
            s[:ast] = Array(s[:ast]) + Array(ast_nodes)
            s[:filters] = Array(s[:filters]) + fragments
          end
        end

        # AR-style `.where.not(...)` support directly on the relation to keep
        # `.where` with no args as a no-op (per project tests).
        # @param args [Array<Object>]
        # @return [SearchEngine::Relation]
        def not(*args)
          nodes = Array(build_ast_with_empty_array_rewrites(args, negated: true))

          negated = nodes.map do |node|
            case node
            when SearchEngine::AST::Eq
              SearchEngine::AST.not_eq(node.field, node.value)
            when SearchEngine::AST::In
              SearchEngine::AST.not_in(node.field, node.values)
            else
              node
            end
          end

          spawn do |s|
            s[:ast] = Array(s[:ast]) + negated
            s[:filters] = Array(s[:filters])
          end
        end

        # Replace all predicates with a new where input.
        # @param input [Hash, String, Array, Symbol]
        # @param args [Array<Object>]
        # @return [SearchEngine::Relation]
        def rewhere(input, *args)
          if input.nil? || (input.respond_to?(:empty?) && input.empty?) || (input.is_a?(String) && input.strip.empty?)
            raise ArgumentError, 'rewhere: provide a new predicate input'
          end

          nodes = SearchEngine::DSL::Parser.parse(input, klass: @klass, args: args, joins: joins_list)
          list = Array(nodes).flatten.compact
          raise ArgumentError, 'rewhere: produced no predicates' if list.empty?

          spawn do |s|
            s[:ast] = list
            s[:filters] = []
          end
        end

        # Merge another relation or join-scope into this relation.
        #
        # - When merging a relation for a joined model, the association must be
        #   applied via `joins(:assoc)` and the scope predicates are rewritten
        #   into joined filters.
        # - When merging a Hash, it is treated as a join-scope shorthand:
        #   `merge(authors: :published)` mirrors `where(authors: :published)`.
        #
        # @param other [SearchEngine::Relation, Hash]
        # @param assoc [Symbol, String, nil] explicit association for joined relations
        # @return [SearchEngine::Relation]
        def merge(other = nil, assoc: nil)
          raise ArgumentError, 'merge: provide a relation or join-scope Hash' if other.nil?

          case other
          when SearchEngine::Relation
            merge_relation(other, assoc: assoc)
          when Hash
            merge_join_scopes(other)
          else
            raise ArgumentError, "merge: unsupported input #{other.class}"
          end
        end

        private

        def merge_relation(other, assoc: nil)
          return self if other.nil?

          return merge_same_model_relation(other) if other.klass == @klass

          assoc_sym = resolve_merge_assoc_for_relation(other, assoc: assoc)
          SearchEngine::Joins::Guard.ensure_join_applied!(joins_list, assoc_sym, context: 'merging')
          cfg = @klass.join_for(assoc_sym)

          nodes = Array(other.send(:ast)).flatten.compact
          return self if nodes.empty?

          rewritten = rewrite_join_scope_nodes(nodes, assoc_sym, cfg)
          spawn do |s|
            s[:ast] = Array(s[:ast]) + Array(rewritten)
            s[:filters] = Array(s[:filters])
          end
        end

        def merge_same_model_relation(other)
          nodes = Array(other.send(:ast)).flatten.compact
          fragments = merge_relation_filters(other)
          return self if nodes.empty? && fragments.empty?

          spawn do |s|
            s[:ast] = Array(s[:ast]) + nodes
            s[:filters] = Array(s[:filters]) + fragments
          end
        end

        def merge_relation_filters(other)
          state = other.instance_variable_get(:@state)
          Array(state ? state[:filters] : [])
        rescue StandardError
          []
        end

        def resolve_merge_assoc_for_relation(other, assoc: nil)
          return assoc.to_sym unless assoc.nil?

          cfgs = @klass.respond_to?(:joins_config) ? (@klass.joins_config || {}) : {}
          target_collection = other.klass.collection if other.klass.respond_to?(:collection)
          collection_name = target_collection.to_s
          if collection_name.strip.empty?
            raise SearchEngine::Errors::InvalidParams.new(
              "merge: cannot infer association for #{other.klass}",
              hint: 'Declare a collection on the joined model or pass assoc: :name',
              doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/joins#troubleshooting'
            )
          end

          matches = cfgs.values.select { |cfg| cfg[:collection].to_s == collection_name }
          if matches.empty?
            available = cfgs.keys.map { |k| ":#{k}" }.join(', ')
            hint = available.empty? ? 'Declare a join on the base model.' : "Available joins: #{available}."
            raise SearchEngine::Errors::InvalidParams.new(
              "merge: no join association for #{collection_name} on #{klass_name_for_inspect}",
              hint: "#{hint} Pass assoc: :name to disambiguate.",
              doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/joins#troubleshooting',
              details: { target_collection: collection_name, available: cfgs.keys }
            )
          end

          if matches.length > 1
            names = matches.map { |cfg| cfg[:name].to_sym }
            raise SearchEngine::Errors::InvalidParams.new(
              "merge: ambiguous association for #{collection_name} on #{klass_name_for_inspect}",
              hint: "Pass assoc: :#{names.first} (available: #{names.map(&:inspect).join(', ')})",
              doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/joins#troubleshooting',
              details: { target_collection: collection_name, matches: names }
            )
          end

          matches.first[:name].to_sym
        end

        def merge_join_scopes(hash)
          raise ArgumentError, 'merge: join-scope Hash must be non-empty' if hash.empty?

          out_nodes = []
          hash.each do |assoc, scope_value|
            unless join_scope_value?(scope_value)
              raise SearchEngine::Errors::InvalidParams.new(
                "merge: expected a join scope Symbol or Array<Symbol> for #{assoc.inspect}",
                hint: 'Use merge(assoc: :scope) or merge(assoc: [:scope1, :scope2])',
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/query-dsl#join-scope',
                details: { assoc: assoc, value: scope_value }
              )
            end
            process_join_scope(assoc.to_sym, scope_value, out_nodes)
          end

          return self if out_nodes.empty?

          spawn do |s|
            s[:ast] = Array(s[:ast]) + out_nodes
            s[:filters] = Array(s[:filters])
          end
        end

        # Build AST nodes, rewriting:
        # - empty-array predicates to hidden *_empty flags when enabled (existing behavior)
        # - nil predicates to hidden *_blank flags when `optional` is enabled (new behavior)
        # Delegates other inputs to the DSL parser.
        def build_ast_with_empty_array_rewrites(args, negated: false)
          items = Array(args).flatten.compact
          return [] if items.empty?

          out_nodes = []
          non_hash_items = []

          items.each do |entry|
            if entry.is_a?(Hash)
              process_hash_entry(entry, out_nodes, negated)
            else
              non_hash_items << entry
            end
          end

          unless non_hash_items.empty?
            out_nodes.concat(
              Array(SearchEngine::DSL::Parser.parse_list(non_hash_items, klass: @klass, joins: joins_list))
            )
          end

          out_nodes.flatten.compact
        end

        def process_hash_entry(entry, out_nodes, negated)
          entry.each do |k, v|
            # Join-scope shorthand: where(assoc: :scope) or where(assoc: [:s1, :s2])
            if join_scope_value?(v) && join_assoc?(k)
              process_join_scope(k.to_sym, v, out_nodes)
            elsif v.is_a?(Hash)
              process_join_predicate(k, v, out_nodes, negated)
            else
              process_base_predicate(k, v, out_nodes, negated)
            end
          end
        end

        def process_join_predicate(assoc_key, values_hash, out_nodes, negated)
          assoc = assoc_key.to_sym
          values_hash.each do |inner_field, inner_value|
            field_sym = inner_field.to_sym
            if inner_value.nil?
              emit_nil_flags_for_join(out_nodes, assoc, field_sym, negated)
            elsif array_like?(inner_value)
              arr = Array(inner_value).flatten(1).compact
              if arr.empty?
                if joined_empty_filtering_enabled?(assoc, field_sym)
                  emit_empty_array_flag(out_nodes, "$#{assoc}.#{field_sym}_empty", negated)
                else
                  raise_empty_array_type!(field_sym)
                end
              else
                out_nodes << SearchEngine::DSL::Parser.parse(
                  { assoc => { field_sym => inner_value } }, klass: @klass, joins: joins_list
                )
              end
            else
              out_nodes << SearchEngine::DSL::Parser.parse(
                { assoc => { field_sym => inner_value } }, klass: @klass, joins: joins_list
              )
            end
          end
        end

        def process_base_predicate(field_key, value, out_nodes, negated)
          field = field_key.to_sym
          if value.nil?
            emit_nil_flags_for_base(out_nodes, field, negated)
          elsif array_like?(value)
            arr = Array(value).flatten(1).compact
            if arr.empty?
              if base_empty_filtering_enabled?(field)
                emit_empty_array_flag(out_nodes, "#{field}_empty", negated)
              else
                raise_empty_array_type!(field)
              end
            else
              out_nodes << SearchEngine::DSL::Parser.parse({ field => value }, klass: @klass, joins: joins_list)
            end
          else
            out_nodes << SearchEngine::DSL::Parser.parse({ field => value }, klass: @klass, joins: joins_list)
          end
        end

        # -- join-scope support -------------------------------------------------

        # True when the given where value is a Symbol or an Array of Symbols.
        # Accepts [:scope1, :scope2] and :scope forms only.
        def join_scope_value?(value)
          return true if value.is_a?(Symbol)

          value.is_a?(Array) && value.all? { |el| el.is_a?(Symbol) }
        end

        # True when the key refers to a declared join association on @klass.
        # Returns the association config Hash when present; falsey otherwise.
        def join_assoc?(key)
          @klass.join_for(key)
        rescue StandardError
          nil
        end

        # Process where(assoc: :scope) or where(assoc: [:s1, :s2]) by taking the AST
        # produced by the target model's scope(s) and rewriting their fields into
        # joined predicates (e.g., "$assoc.field"). Supports all comparison/node types;
        # nested joins inside the scope are rejected and raw fragments are wrapped
        # into join-scoped expressions when safe.
        def process_join_scope(assoc_sym, scope_value, out_nodes)
          assoc = assoc_sym.to_sym

          # Validate join exists and is applied on this relation
          cfg = @klass.join_for(assoc)
          SearchEngine::Joins::Guard.ensure_join_applied!(joins_list, assoc, context: 'where join-scope')

          collection = cfg[:collection]
          target_klass = SearchEngine.collection_for(collection)

          scope_names = Array(scope_value).flatten.compact
          scope_names.each do |sname|
            sym = sname.to_sym

            unless target_klass.respond_to?(sym)
              raise SearchEngine::Errors::InvalidParams.new(
                %(Unknown join-scope :#{sym} on association :#{assoc} for #{target_klass}),
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/query-dsl#join-scope'
              )
            end

            rel = target_klass.public_send(sym)
            unless rel.is_a?(SearchEngine::Relation)
              raise SearchEngine::Errors::InvalidParams.new(
                %(join-scope :#{sym} on :#{assoc} must return a SearchEngine::Relation (got #{rel.class})),
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/query-dsl#join-scope'
              )
            end

            nodes = Array(rel.send(:ast)).flatten.compact
            next if nodes.empty?

            rewritten = rewrite_join_scope_nodes(nodes, assoc, cfg)
            out_nodes.concat(Array(rewritten))
          end
        end

        # Rewrite a list of AST nodes so that any base-field predicate like
        #   field OP value
        # becomes a joined predicate
        #   "$assoc.field" OP value
        # Boolean/grouping nodes are rewritten recursively. Raw fragments are
        # wrapped into a join-scoped expression when safe; pre-joined fields
        # inside the scope are rejected.
        def rewrite_join_scope_nodes(nodes, assoc_sym, assoc_cfg)
          Array(nodes).flatten.compact.map { |n| rewrite_join_scope_node(n, assoc_sym, assoc_cfg) }
        end

        def rewrite_join_scope_node(node, assoc_sym, assoc_cfg)
          case node
          when SearchEngine::AST::And
            children = node.children.map { |ch| rewrite_join_scope_node(ch, assoc_sym, assoc_cfg) }
            SearchEngine::AST.and_(*children)
          when SearchEngine::AST::Or
            children = node.children.map { |ch| rewrite_join_scope_node(ch, assoc_sym, assoc_cfg) }
            SearchEngine::AST.or_(*children)
          when SearchEngine::AST::Group
            inner = Array(node.children).first
            SearchEngine::AST.group(rewrite_join_scope_node(inner, assoc_sym, assoc_cfg))
          when SearchEngine::AST::Raw
            fragment = node.fragment
            if fragment.include?('$')
              raise SearchEngine::Errors::InvalidParams.new(
                'join-scope raw fragments must use base fields only (no nested join paths)',
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/query-dsl#join-scope',
                details: { fragment: fragment, assoc: assoc_sym }
              )
            end
            SearchEngine::AST.raw("$#{assoc_sym}(#{fragment})")
          when SearchEngine::AST::Eq,
               SearchEngine::AST::NotEq,
               SearchEngine::AST::Gt,
               SearchEngine::AST::Gte,
               SearchEngine::AST::Lt,
               SearchEngine::AST::Lte,
               SearchEngine::AST::In,
               SearchEngine::AST::NotIn,
               SearchEngine::AST::Matches,
               SearchEngine::AST::Prefix
            lhs = node.field.to_s
            if lhs.start_with?('$') || lhs.include?('.')
              raise SearchEngine::Errors::InvalidParams.new(
                %(join-scope cannot reference nested join field #{lhs.inspect}; use base fields only),
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/query-dsl#join-scope',
                details: { field: lhs, assoc: assoc_sym }
              )
            end

            # Best-effort field validation against target collection
            begin
              SearchEngine::Joins::Guard.validate_joined_field!(assoc_cfg, lhs, source_klass: @klass)
            rescue StandardError
              nil
            end

            joined_lhs = "$#{assoc_sym}.#{lhs}"
            builder = case node
                      when SearchEngine::AST::Eq then :eq
                      when SearchEngine::AST::NotEq then :not_eq
                      when SearchEngine::AST::Gt then :gt
                      when SearchEngine::AST::Gte then :gte
                      when SearchEngine::AST::Lt then :lt
                      when SearchEngine::AST::Lte then :lte
                      when SearchEngine::AST::In then :in_
                      when SearchEngine::AST::NotIn then :not_in
                      when SearchEngine::AST::Matches then :matches
                      when SearchEngine::AST::Prefix then :prefix
                      end

            SearchEngine::AST.public_send(builder, joined_lhs, node.right)
          else
            # Unknown node type: keep as-is (defensive)
            node
          end
        end

        def emit_empty_array_flag(out_nodes, lhs, negated)
          out_nodes << SearchEngine::AST.raw("#{lhs}:=#{negated ? 'false' : 'true'}")
        end

        def base_empty_filtering_enabled?(field_sym)
          opts = @klass.respond_to?(:attribute_options) ? (@klass.attribute_options || {}) : {}
          o = opts[field_sym]
          o.is_a?(Hash) && o[:empty_filtering]
        rescue StandardError
          false
        end

        def joined_empty_filtering_enabled?(assoc_sym, field_sym)
          cfg = @klass.join_for(assoc_sym)
          collection = cfg[:collection]
          return false if collection.nil? || collection.to_s.strip.empty?

          target_klass = SearchEngine.collection_for(collection)
          return false unless target_klass.respond_to?(:attribute_options)

          o = (target_klass.attribute_options || {})[field_sym]
          o.is_a?(Hash) && o[:empty_filtering]
        rescue StandardError
          false
        end

        def base_optional_enabled?(field_sym)
          opts = @klass.respond_to?(:attribute_options) ? (@klass.attribute_options || {}) : {}
          o = opts[field_sym]
          o.is_a?(Hash) && o[:optional]
        rescue StandardError
          false
        end

        def joined_optional_enabled?(assoc_sym, field_sym)
          cfg = @klass.join_for(assoc_sym)
          collection = cfg[:collection]
          return false if collection.nil? || collection.to_s.strip.empty?

          target_klass = SearchEngine.collection_for(collection)
          return false unless target_klass.respond_to?(:attribute_options)

          o = (target_klass.attribute_options || {})[field_sym]
          o.is_a?(Hash) && o[:optional]
        rescue StandardError
          false
        end

        def emit_nil_flags_for_base(out_nodes, field_sym, negated)
          has_empty = base_empty_filtering_enabled?(field_sym)
          has_blank = base_optional_enabled?(field_sym)
          fragment = nil
          if has_empty && has_blank
            fragment = if negated
                         "(#{field_sym}_empty:=false && #{field_sym}_blank:=false)"
                       else
                         "(#{field_sym}_empty:=true || #{field_sym}_blank:=true)"
                       end
          elsif has_blank
            fragment = "#{field_sym}_blank:=#{negated ? 'false' : 'true'}"
          elsif has_empty
            fragment = "#{field_sym}_empty:=#{negated ? 'false' : 'true'}"
          end

          out_nodes << if fragment
                         SearchEngine::AST.raw(fragment)
                       else
                         SearchEngine::DSL::Parser.parse({ field_sym => nil }, klass: @klass, joins: joins_list)
                       end
        end

        def emit_nil_flags_for_join(out_nodes, assoc_sym, field_sym, negated)
          has_empty = joined_empty_filtering_enabled?(assoc_sym, field_sym)
          has_blank = joined_optional_enabled?(assoc_sym, field_sym)
          lhs_empty = "$#{assoc_sym}.#{field_sym}_empty"
          lhs_blank = "$#{assoc_sym}.#{field_sym}_blank"
          fragment = nil
          if has_empty && has_blank
            fragment = if negated
                         "(#{lhs_empty}:=false && #{lhs_blank}:=false)"
                       else
                         "(#{lhs_empty}:=true || #{lhs_blank}:=true)"
                       end
          elsif has_blank
            fragment = "#{lhs_blank}:=#{negated ? 'false' : 'true'}"
          elsif has_empty
            fragment = "#{lhs_empty}:=#{negated ? 'false' : 'true'}"
          end

          if fragment
            out_nodes << SearchEngine::AST.raw(fragment)
          else
            parsed = { assoc_sym => { field_sym => nil } }
            out_nodes << SearchEngine::DSL::Parser.parse(parsed, klass: @klass, joins: joins_list)
          end
        end

        def array_like?(value)
          value.is_a?(Array)
        end

        def raise_empty_array_type!(field_sym)
          raise SearchEngine::Errors::InvalidType.new(
            %(expected #{field_sym.inspect} to be a non-empty Array),
            doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/query-dsl#troubleshooting',
            details: { field: field_sym }
          )
        end

        # Normalize where arguments into an array of string fragments safe for Typesense.
        def normalize_where(args)
          list = Array(args).flatten.compact
          return [] if list.empty?

          fragments = []
          i = 0
          known_attrs = safe_attributes_map

          while i < list.length
            entry = list[i]
            case entry
            when Hash
              # Validate only base-like keys here; assoc keys (values as Hash) are handled via AST/Parser
              # and assoc keys with join-scope shorthand (values as Symbol/Array<Symbol>) are ignored for fragments.
              base_like_pairs = entry.reject { |_, v| v.is_a?(Hash) || join_scope_value?(v) }
              validate_hash_keys!(base_like_pairs, known_attrs)
              # Build fragments from base scalar/array pairs only; skip assoc=>{...} and assoc=>:scope
              base_pairs = base_like_pairs
              unless base_pairs.empty?
                fragments.concat(
                  SearchEngine::Filters::Sanitizer.build_from_hash(base_pairs, known_attrs)
                )
              end
              i += 1
            when String
              i = normalize_where_process_string!(fragments, entry, list, i)
            when Symbol
              fragments << entry.to_s
              i += 1
            when Array
              nested = normalize_where(entry)
              fragments.concat(nested)
              i += 1
            else
              raise ArgumentError, "unsupported where argument of type #{entry.class}"
            end
          end

          fragments
        end

        def normalize_where_process_string!(fragments, entry, list, i)
          if entry.match?(/(?<!\\)\?/) # has unescaped placeholders
            tail = list[(i + 1)..] || []
            needed = SearchEngine::Filters::Sanitizer.count_placeholders(entry)
            args_for_template = tail.first(needed)
            if args_for_template.length != needed
              raise ArgumentError, "expected #{needed} args for #{needed} placeholders, got #{args_for_template.length}"
            end

            fragments << SearchEngine::Filters::Sanitizer.apply_placeholders(entry, args_for_template)
            i + 1 + needed
          else
            fragments << entry.to_s
            i + 1
          end
        end

        # (no-op helpers; reference field coercion is handled by the compiler and schema)
      end
    end
  end
end
