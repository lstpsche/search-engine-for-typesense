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
            @relation.not(*args)
          end
        end

        # Add filters to the relation.
        # When called without arguments, it's a no-op and returns the relation (idempotent).
        # @param args [Array<Object>] filter arguments
        # @return [SearchEngine::Relation, WhereChain]
        def where(*args)
          return self if args.nil? || args.empty?
          return self if blank_where_args?(args)

          ast_nodes = build_filter_nodes(args, negated: false)
          spawn do |s|
            s[:ast] = Array(s[:ast]) + Array(ast_nodes)
          end
        end

        # AR-style `.where.not(...)` support directly on the relation to keep
        # `.where` with no args as a no-op (per project tests).
        # @param args [Array<Object>]
        # @return [SearchEngine::Relation]
        def not(*args)
          validate_not_args!(args)
          nodes = build_filter_nodes(args, negated: true)
          raise SearchEngine::Errors::InvalidOperator, 'where.not requires a predicate' if nodes.empty?

          spawn { |state| state[:ast] = Array(state[:ast]) + nodes }
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

        def blank_where_args?(args)
          return false unless args.length == 1

          blank_where_arg?(args.first)
        end

        def blank_where_arg?(arg)
          return true if arg.nil?
          return true if arg.respond_to?(:empty?) && arg.empty?

          arg.is_a?(String) && arg.strip.empty?
        end

        def validate_not_args!(args)
          raise ArgumentError, 'wrong number of arguments (given 0, expected 1+)' if args.nil? || args.empty?

          return unless args.length == 1

          value = args.first
          return unless value.nil? || (value.is_a?(Array) && value.empty?)

          raise ArgumentError, 'Unsupported argument type:  (NilClass)'
        end

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
              doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/joins#troubleshooting'
            )
          end

          matches = cfgs.values.select { |cfg| cfg[:collection].to_s == collection_name }
          if matches.empty?
            available = cfgs.keys.map { |k| ":#{k}" }.join(', ')
            hint = available.empty? ? 'Declare a join on the base model.' : "Available joins: #{available}."
            raise SearchEngine::Errors::InvalidParams.new(
              "merge: no join association for #{collection_name} on #{klass_name_for_inspect}",
              hint: "#{hint} Pass assoc: :name to disambiguate.",
              doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/joins#troubleshooting',
              details: { target_collection: collection_name, available: cfgs.keys }
            )
          end

          if matches.length > 1
            names = matches.map { |cfg| cfg[:name].to_sym }
            raise SearchEngine::Errors::InvalidParams.new(
              "merge: ambiguous association for #{collection_name} on #{klass_name_for_inspect}",
              hint: "Pass assoc: :#{names.first} (available: #{names.map(&:inspect).join(', ')})",
              doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/joins#troubleshooting',
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
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#join-scope',
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

        def build_filter_nodes(args, negated:)
          nodes = []
          expressions = []
          args.each do |entry|
            if entry.is_a?(Hash)
              entry.each do |field, value|
                if join_scope_value?(value) && join_assoc?(field)
                  scoped = []
                  process_join_scope(field, value, scoped)
                  nodes.concat(negated ? scoped.map { |node| invert_predicate(node) } : scoped)
                elsif value.is_a?(Hash)
                  SearchEngine::DSL::Parser.validate_assoc_and_join!(@klass, field.to_sym, joins_list)
                  value.each do |inner_field, inner_value|
                    nodes << hash_predicate(inner_field, inner_value, assoc: field, negated: negated)
                  end
                else
                  nodes << hash_predicate(field, value, negated: negated)
                end
              end
            else
              expressions << entry
            end
          end

          parsed = SearchEngine::DSL::Parser.parse_list(expressions, klass: @klass, joins: joins_list)
          nodes.concat(negated ? parsed.map { |node| invert_predicate(node) } : parsed)
        end

        def hash_predicate(key, value, negated:, assoc: nil)
          field = SearchEngine::Filters::Sanitizer.field_name(key).to_sym
          empty_array = value.is_a?(Array) && value.flatten(1).compact.empty?
          if value.nil? || empty_array
            lhs = assoc ? "$#{assoc}.#{field}" : field.to_s
            empty = assoc ? joined_empty_filtering_enabled?(assoc, field) : base_empty_filtering_enabled?(field)
            blank = assoc ? joined_optional_enabled?(assoc, field) : base_optional_enabled?(field)
            raise_empty_array_type!(field) if empty_array && !empty

            flags = []
            flags << SearchEngine::AST.eq("#{lhs}_empty", !negated) if empty
            flags << SearchEngine::AST.eq("#{lhs}_blank", !negated) if blank && value.nil?
            return flags.first if flags.size == 1
            if flags.size > 1
              return SearchEngine::AST.group(negated ? SearchEngine::AST.and_(*flags) : SearchEngine::AST.or_(*flags))
            end
          end

          predicate = { field => value }
          predicate = { assoc => predicate } if assoc
          node = SearchEngine::DSL::Parser.parse(predicate, klass: @klass, joins: joins_list)
          negated ? invert_predicate(node) : node
        end

        INVERSE_PREDICATES = {
          eq: :not_eq, not_eq: :eq, in: :not_in, not_in: :in_,
          gt: :lte, gte: :lt, lt: :gte, lte: :gt
        }.freeze
        private_constant :INVERSE_PREDICATES

        def invert_predicate(node)
          inverse = INVERSE_PREDICATES[node.type]
          return SearchEngine::AST.public_send(inverse, node.field, node.right) if inverse

          case node
          when SearchEngine::AST::And
            SearchEngine::AST.or_(*node.children.map { |child| invert_predicate(child) })
          when SearchEngine::AST::Or
            SearchEngine::AST.and_(*node.children.map { |child| invert_predicate(child) })
          when SearchEngine::AST::Group
            SearchEngine::AST.group(invert_predicate(node.child))
          else
            raise SearchEngine::Errors::InvalidOperator, "where.not cannot invert a #{node.type} predicate"
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
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#join-scope'
              )
            end

            rel = target_klass.public_send(sym)
            unless rel.is_a?(SearchEngine::Relation)
              raise SearchEngine::Errors::InvalidParams.new(
                %(join-scope :#{sym} on :#{assoc} must return a SearchEngine::Relation (got #{rel.class})),
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#join-scope'
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
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#join-scope',
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
                doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#join-scope',
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

        def raise_empty_array_type!(field_sym)
          raise SearchEngine::Errors::InvalidType.new(
            %(expected #{field_sym.inspect} to be a non-empty Array),
            doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/query-dsl#troubleshooting',
            details: { field: field_sym }
          )
        end

        # Normalize where arguments into an array of string fragments safe for Typesense.
        def normalize_where(args)
          # Preserve input shape so template bind arrays stay intact:
          # where('field IN ?', [1, 2]) => ['field IN ?', [1, 2]]
          list = Array(args)
          return [] if list.empty?

          fragments = []
          i = 0
          known_attrs = safe_attributes_map

          while i < list.length
            entry = list[i]
            case entry
            when nil
              i += 1
            when Hash
              # Validate only base-like keys here; assoc keys (values as Hash) are handled via AST/Parser
              # and assoc keys with join-scope shorthand (values as Symbol/Array<Symbol>) are ignored for fragments.
              base_like_pairs = entry.reject { |_, v| v.is_a?(Hash) || join_scope_value?(v) }
              validate_hash_keys!(base_like_pairs, known_attrs)
              # Build fragments from base scalar/array pairs only; skip assoc=>{...} and assoc=>:scope
              base_pairs = base_like_pairs
              unless base_pairs.empty?
                fragments.concat(
                  SearchEngine::Filters::Sanitizer.build_from_hash(base_pairs, known_attrs, klass: @klass)
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
          needed = SearchEngine::Filters::Sanitizer.count_placeholders(entry)
          if needed.positive?
            tail = list[(i + 1)..] || []
            args_for_template = tail.first(needed)
            if args_for_template.length != needed
              raise ArgumentError, "expected #{needed} args for #{needed} placeholders, got #{args_for_template.length}"
            end

            fragments << SearchEngine::Filters::Sanitizer.apply_placeholders(entry, args_for_template, klass: @klass)
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
