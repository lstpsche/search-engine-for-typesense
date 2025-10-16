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
          compiled = compile_node(root, parent_prec: 0)
        end
        compiled
      else
        compile_node(root, parent_prec: 0)
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

    def compile_node(node, parent_prec:)
      case node
      when SearchEngine::AST::Eq
        compile_binary(node.field, ':=', node.value)
      when SearchEngine::AST::NotEq
        compile_binary(node.field, ':!=', node.value)
      when SearchEngine::AST::Gt
        compile_binary(node.field, ':>', node.value)
      when SearchEngine::AST::Gte
        compile_binary(node.field, ':>=', node.value)
      when SearchEngine::AST::Lt
        compile_binary(node.field, ':<', node.value)
      when SearchEngine::AST::Lte
        compile_binary(node.field, ':<=', node.value)
      when SearchEngine::AST::In
        compile_binary(node.field, ':=', node.values)
      when SearchEngine::AST::NotIn
        compile_binary(node.field, ':!=', node.values)
      when SearchEngine::AST::And
        compile_boolean(node.children, ' && ', parent_prec: parent_prec, my_prec: precedence(:and))
      when SearchEngine::AST::Or
        compile_or(node, parent_prec)
      when SearchEngine::AST::Group
        "(#{compile_node(node.children.first, parent_prec: 0)})"
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

    def compile_or(node, parent_prec)
      compiled = compile_boolean(node.children, ' || ', parent_prec: parent_prec, my_prec: precedence(:or))
      if node.children.length == 2 && node.children.last.is_a?(SearchEngine::AST::And)
        left_str, right_str = compiled.split(' || ', 2)
        compiled = "#{left_str} || (#{right_str})"
      end
      compiled
    end
    private_class_method :compile_or

    def compile_binary(field, op, value)
      rhs = quote(value)
      fstr = field.to_s
      if (m = fstr.match(/^\$(\w+)\.(.+)$/))
        assoc = m[1]
        inner = m[2]
        # Render joined field as $assoc(inner OP value) per expected Typesense join filter syntax
        "$#{assoc}(#{inner}#{op}#{rhs})"
      else
        binary(field, op, rhs)
      end
    end
    private_class_method :compile_binary

    def binary(field, op, rhs)
      "#{field}#{op}#{rhs}"
    end
    private_class_method :binary

    def compile_boolean(children, joiner, parent_prec:, my_prec:)
      # For conjunctions only, merge multiple $assoc(inner ...) predicates targeting
      # the same association into a single $assoc(inner && inner ...) expression
      # to satisfy Typesense join filter rules.
      if joiner == ' && '
        items = [] # [{ pos:, str: }]
        assoc_first_pos = {}
        assoc_inner_map = {} # { assoc => [inner_str, ...] }

        children.each_with_index do |child, idx|
          inner = extract_join_inner_binary(child)
          if inner
            assoc, inner_expr = inner
            assoc_first_pos[assoc] = idx unless assoc_first_pos.key?(assoc)
            assoc_inner_map[assoc] ||= []
            assoc_inner_map[assoc] << inner_expr
            next
          end

          cstr = compile_node(child, parent_prec: my_prec)
          cstr = "(#{cstr})" if needs_parentheses?(child, parent_prec: my_prec)
          items << { pos: idx, str: cstr }
        end

        unless assoc_inner_map.empty?
          # Emit one consolidated token per assoc at the first position it appeared
          assoc_first_pos.sort_by { |_a, pos| pos }.each do |assoc, pos|
            inners = Array(assoc_inner_map[assoc]).flatten.compact
            next if inners.empty?

            token = "$#{assoc}(#{inners.join(' && ')})"
            items << { pos: pos, str: token }
          end

          inner = items.sort_by { |it| it[:pos] }.map { |it| it[:str] }.join(joiner)

          return parent_prec > my_prec ? "(#{inner})" : inner
        end
      end

      # Fallback/default behavior
      parts = children.map do |child|
        cstr = compile_node(child, parent_prec: my_prec)
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

    # Try to extract join association and compiled inner expression for a binary node
    # with a joined field like "$assoc.field". Returns [assoc(String), inner(String)] or nil.
    def extract_join_inner_binary(node)
      case node
      when SearchEngine::AST::Eq, SearchEngine::AST::NotEq,
           SearchEngine::AST::Gt, SearchEngine::AST::Gte,
           SearchEngine::AST::Lt, SearchEngine::AST::Lte,
           SearchEngine::AST::In, SearchEngine::AST::NotIn
        field = node.respond_to?(:field) ? node.field.to_s : nil
        return nil unless field&.start_with?('$')

        m = field.match(/^\$(\w+)\.(.+)$/)
        return nil unless m

        assoc = m[1]
        inner_field = m[2]
        op = op_for(node)
        rhs = if node.respond_to?(:value)
                quote(node.value)
              elsif node.respond_to?(:values)
                quote(node.values)
              else
                return nil
              end

        [assoc, "#{inner_field}#{op}#{rhs}"]
      end
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
        raise Error, "Unknown binary node for join extraction: #{node.class}"
      end
    end
    private_class_method :op_for

    def quote(value)
      # Use conditional scalar quoting for scalars; preserve array element quoting rules
      if value.is_a?(Array)
        SearchEngine::Filters::Sanitizer.quote(value)
      else
        SearchEngine::Filters::Sanitizer.quote_scalar_for_filter(value)
      end
    end
    private_class_method :quote

    def needs_parentheses?(child, parent_prec:)
      child_prec = precedence(child)
      # Parenthesize when child binds looser than parent
      child_prec < parent_prec
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
