# frozen_string_literal: true

require 'search_engine/filters/sanitizer'
require 'search_engine/ast'

module SearchEngine
  # Compiler for turning Predicate AST into Typesense `filter_by` strings.
  #
  # Pure and deterministic: no side effects, no I/O. Uses
  # `SearchEngine::Filters::Sanitizer` for all quoting/escaping to ensure
  # consistent rendering with the `where` DSL.
  module Compiler
    class Error < StandardError; end
    class UnsupportedNode < Error; end

    module_function

    # Compile an AST node or an array of nodes (implicit AND) into a Typesense
    # `filter_by` string.
    #
    # - Nil or empty input returns an empty String
    # - Arrays at the top-level are treated as implicit AND
    # - Group nodes are always parenthesized
    # - Raw fragments are passed-through
    #
    # @param ast [SearchEngine::AST::Node, Array<SearchEngine::AST::Node>, nil]
    # @param klass [Class] optional model class for context (used for observability)
    # @return [String]
    # @note Emits "search_engine.compile" via ActiveSupport::Notifications with
    #   payload: { collection, klass, node_count, duration_ms, source: :ast }
    def compile(ast, klass: nil)
      root = coerce_root(ast)
      return '' unless root

      compiled = nil
      if defined?(ActiveSupport::Notifications)
        payload = {
          collection: safe_collection_for_klass(klass),
          klass: safe_klass_name(klass),
          node_count: count_nodes(root),
          source: :ast
        }
        SearchEngine::Instrumentation.instrument('search_engine.compile', payload) do |_ctx|
          compiled = compile_node(root, parent_prec: 0, klass: klass)
        end
        compiled
      else
        compile_node(root, parent_prec: 0, klass: klass)
      end
    end

    # --- Internals ---------------------------------------------------------

    def coerce_root(ast)
      return nil if ast.nil?

      if ast.is_a?(Array)
        nodes = ast.flatten.compact.select { |n| n.is_a?(SearchEngine::AST::Node) }
        return nil if nodes.empty?
        return nodes.first if nodes.length == 1

        return SearchEngine::AST.and_(*nodes)
      end

      return ast if ast.is_a?(SearchEngine::AST::Node)

      raise Error, "Compiler: unsupported root input #{ast.class}"
    end
    private_class_method :coerce_root

    def compile_node(node, parent_prec:, klass: nil)
      case node
      when SearchEngine::AST::BinaryOp
        compile_binary(node, klass: klass)
      when SearchEngine::AST::And
        compile_boolean(node.children, ' && ', parent_prec: parent_prec, my_prec: precedence(:and), klass: klass)
      when SearchEngine::AST::Or
        compile_boolean(node.children, ' || ', parent_prec: parent_prec, my_prec: precedence(:or), klass: klass)
      when SearchEngine::AST::Group
        "(#{compile_node(node.children.first, parent_prec: 0, klass: klass)})"
      when SearchEngine::AST::Raw
        node.fragment
      when SearchEngine::AST::Matches
        raise UnsupportedNode, 'Typesense filter_by does not support MATCHES; use AST::Raw if needed.'
      when SearchEngine::AST::Prefix
        raise UnsupportedNode, 'Typesense filter_by does not support PREFIX; use AST::Raw if needed.'
      else
        raise Error, "Compiler: unknown node #{node.class}"
      end
    end
    private_class_method :compile_node

    def compile_binary(node, klass:)
      if (joined = extract_join_inner_binary(node, klass: klass))
        assoc, inner = joined
        return "$#{assoc}(#{inner})"
      end

      "#{node.field}#{op_for(node)}#{quote(node.right, field: node.field, klass: klass)}"
    end
    private_class_method :compile_binary

    def compile_boolean(children, joiner, parent_prec:, my_prec:, klass: nil)
      # For conjunctions only, merge multiple $assoc(inner ...) predicates targeting
      # the same association into a single $assoc(inner && inner ...) expression
      # to satisfy Typesense join filter rules.
      if joiner == ' && '
        return merge_join_predicates(children, parent_prec: parent_prec, my_prec: my_prec, klass: klass)
      end

      parts = children.map do |child|
        cstr = compile_node(child, parent_prec: my_prec, klass: klass)
        if needs_parentheses?(child, parent_prec: my_prec)
          "(#{cstr})"
        else
          cstr
        end
      end
      inner = parts.join(joiner)

      # Parent adds parens if its precedence is greater than ours
      if parent_prec > my_prec
        "(#{inner})"
      else
        inner
      end
    end
    private_class_method :compile_boolean

    # Merge multiple join predicates targeting the same association into consolidated expressions.
    #
    # For AND operations, collects all predicates for each association and merges them
    # into a single $assoc(inner && inner ...) expression at the first position where
    # that association appeared.
    #
    # @param children [Array<SearchEngine::AST::Node>] child nodes to process
    # @param parent_prec [Integer] parent operator precedence
    # @param my_prec [Integer] current operator precedence
    # @param klass [Class, nil] model class for context
    # @return [String] compiled conjunction
    def merge_join_predicates(children, parent_prec:, my_prec:, klass:)
      groups = {}
      parts = children.map.with_index do |child, position|
        if (joined = extract_join_inner_binary(child, klass: klass))
          assoc, predicate = joined
          group = groups[assoc] ||= { position: position, predicates: [] }
          group[:predicates] << predicate
          next
        end

        expression = compile_node(child, parent_prec: my_prec, klass: klass)
        needs_parentheses?(child, parent_prec: my_prec) ? "(#{expression})" : expression
      end

      groups.each do |assoc, group|
        parts[group[:position]] = "$#{assoc}(#{group[:predicates].join(' && ')})"
      end

      inner = parts.compact.join(' && ')
      parent_prec > my_prec ? "(#{inner})" : inner
    end
    private_class_method :merge_join_predicates

    # Try to extract join association and compiled inner expression for a binary node
    # with a joined field like "$assoc.field". Returns [assoc(String), inner(String)] or nil.
    def extract_join_inner_binary(node, klass: nil)
      return unless node.is_a?(SearchEngine::AST::BinaryOp) && node.field.start_with?('$')

      assoc, field = node.field.delete_prefix('$').split('.', 2)
      target = SearchEngine::Joins::Guard.target_model(klass, assoc)
      rhs = quote(node.right, field: field, klass: target, joined: true)
      [assoc, "#{field}#{op_for(node)}#{rhs}"]
    end
    private_class_method :extract_join_inner_binary

    def op_for(node)
      case node
      when SearchEngine::AST::Eq then ':='
      when SearchEngine::AST::NotEq then ':!='
      when SearchEngine::AST::Gt then ':>'
      when SearchEngine::AST::Gte then ':>='
      when SearchEngine::AST::Lt then ':<'
      when SearchEngine::AST::Lte then ':<='
      when SearchEngine::AST::In then ':='
      when SearchEngine::AST::NotIn then ':!='
      else
        raise Error, "Compiler: unsupported binary predicate #{node.class}"
      end
    end
    private_class_method :op_for

    def quote(value, field:, klass:, joined: false)
      # Typesense requires reference fields to use string values, even if the field type is int64
      converted_value = if klass && field && field_reference?(klass, field)
                          convert_for_reference_field(value)
                        else
                          value
                        end

      SearchEngine::Filters::Sanitizer.quote(converted_value, field: field, klass: klass, joined: joined)
    end
    private_class_method :quote

    # Check if a field has a reference attribute in the schema.
    # Uses the same logic as Schema.build_references_by_local_key to ensure consistency.
    def field_reference?(klass, field_name)
      return false unless klass
      return false unless klass.respond_to?(:joins_config)

      configs = klass.joins_config || {}
      configs.each_value do |cfg|
        # Only belongs_to/belongs_to_many contribute references to schema
        kind = (cfg[:kind] || :belongs_to).to_sym
        next if %i[has_one has_many].include?(kind)

        lk = cfg[:local_key]
        next if lk.nil?

        return true if lk.to_sym == field_name.to_sym
      end

      false
    end
    private_class_method :field_reference?

    # Convert numeric values to strings for reference fields
    def convert_for_reference_field(value)
      if value.is_a?(Array)
        value.map { |v| convert_for_reference_field(v) }
      elsif value.is_a?(Numeric)
        value.to_s
      else
        value
      end
    end
    private_class_method :convert_for_reference_field

    def needs_parentheses?(child, parent_prec:)
      child_prec = precedence(child)
      # Parenthesize when child binds looser than parent
      child_prec < parent_prec || (child.is_a?(SearchEngine::AST::And) && parent_prec == precedence(:or))
    end
    private_class_method :needs_parentheses?

    def precedence(node)
      case node
      when Symbol
        return 20 if node == :and
        return 10 if node == :or

        100
      when SearchEngine::AST::And
        20
      when SearchEngine::AST::Or
        10
      when SearchEngine::AST::Group
        # Group is a special case; caller always wraps it explicitly
        100
      else
        # Leaves (Eq, In, etc.) bind tight
        100
      end
    end
    private_class_method :precedence

    def count_nodes(node)
      return 0 unless node.is_a?(SearchEngine::AST::Node)

      1 + Array(node.children).sum { |child| count_nodes(child) }
    end
    private_class_method :count_nodes

    def safe_klass_name(klass)
      return nil unless klass

      klass.respond_to?(:name) && klass.name ? klass.name : klass.to_s
    end
    private_class_method :safe_klass_name

    def safe_collection_for_klass(klass)
      return nil unless klass
      return klass.collection if klass.respond_to?(:collection) && klass.collection

      nil
    end
    private_class_method :safe_collection_for_klass
  end
end
