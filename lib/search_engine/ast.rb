# frozen_string_literal: true

require 'search_engine/ast/node'
require 'search_engine/ast/binary_op'
require 'search_engine/ast/unary_op'
require 'search_engine/ast/eq'
require 'search_engine/ast/not_eq'
require 'search_engine/ast/gt'
require 'search_engine/ast/gte'
require 'search_engine/ast/lt'
require 'search_engine/ast/lte'
require 'search_engine/ast/in'
require 'search_engine/ast/not_in'
require 'search_engine/ast/matches'
require 'search_engine/ast/prefix'
require 'search_engine/ast/and'
require 'search_engine/ast/or'
require 'search_engine/ast/group'
require 'search_engine/ast/raw'

module SearchEngine
  # Predicate AST for compiler-agnostic query representation.
  #
  # Exposes ergonomic builders as module functions, returning immutable nodes.
  #
  # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/query-dsl`
  module AST
    module_function

    # -- Comparison
    # @param field [String, Symbol]
    # @param value [Object]
    # @return [SearchEngine::AST::Eq]
    def eq(field, value) = Eq.new(field, value)

    # @param field [String, Symbol]
    # @param value [Object]
    # @return [SearchEngine::AST::NotEq]
    def not_eq(field, value) = NotEq.new(field, value)

    # @param field [String, Symbol]
    # @param value [Object]
    # @return [SearchEngine::AST::Gt]
    def gt(field, value) = Gt.new(field, value)

    # @param field [String, Symbol]
    # @param value [Object]
    # @return [SearchEngine::AST::Gte]
    def gte(field, value) = Gte.new(field, value)

    # @param field [String, Symbol]
    # @param value [Object]
    # @return [SearchEngine::AST::Lt]
    def lt(field, value) = Lt.new(field, value)

    # @param field [String, Symbol]
    # @param value [Object]
    # @return [SearchEngine::AST::Lte]
    def lte(field, value) = Lte.new(field, value)

    # -- Membership
    # @param values [Array]
    # @return [SearchEngine::AST::In]
    def in_(field, values) = In.new(field, values)

    # @param field [String, Symbol]
    # @param values [Array]
    # @return [SearchEngine::AST::NotIn]
    def not_in(field, values) = NotIn.new(field, values)

    # -- Pattern
    # @param field [String, Symbol]
    # @param pattern [String, Regexp]
    # @return [SearchEngine::AST::Matches]
    def matches(field, pattern) = Matches.new(field, pattern)

    # @param field [String, Symbol]
    # @param prefix [String]
    # @return [SearchEngine::AST::Prefix]
    def prefix(field, prefix) = Prefix.new(field, prefix)

    # -- Boolean
    # @param nodes [Array<SearchEngine::AST::Node>]
    # @return [SearchEngine::AST::And]
    def and_(*nodes) = And.new(*nodes)

    # @param nodes [Array<SearchEngine::AST::Node>]
    # @return [SearchEngine::AST::Or]
    def or_(*nodes) = Or.new(*nodes)

    # -- Grouping
    # @param node [SearchEngine::AST::Node]
    # @return [SearchEngine::AST::Group]
    def group(node) = Group.new(node)

    # -- Escape hatch
    # @param fragment [String]
    # @return [SearchEngine::AST::Raw]
    def raw(fragment) = Raw.new(fragment)
  end
end
