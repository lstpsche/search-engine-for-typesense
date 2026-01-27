# frozen_string_literal: true

require 'active_support/concern'

module SearchEngine
  class Base
    # ActiveRecord-like named scopes for SearchEngine models.
    #
    # Scopes are defined on the model class and must return a
    # {SearchEngine::Relation}. They are evaluated against a fresh
    # relation (`all`) and are therefore fully chainable.
    #
    # Examples:
    #   class Product < SearchEngine::Base
    #     scope :active, -> { where(active: true) }
    #     scope :by_store, ->(id) { where(store_id: id) }
    #   end
    #
    #   Product.active.by_store(1).search("shoes")
    module Scopes
      extend ActiveSupport::Concern

      class_methods do
        # Internal registry of declared scopes for this model.
        # Used to apply scopes against an existing Relation (AR parity).
        #
        # @api private
        # @return [Hash{Symbol=>Proc}]
        def __search_engine_scope_registry__
          @__search_engine_scope_registry__ ||= {}
        end

        # Define a named, chainable scope.
        #
        # @param name [#to_sym] public method name for the scope
        # @param body [#call, nil] a Proc/lambda evaluated against a fresh Relation
        # @yield evaluated as the scope body when +body+ is nil (AR-style)
        # @return [void]
        #
        # The scope body is executed with `self` set to a fresh
        # {SearchEngine::Relation} bound to the model. It must return a
        # Relation (or nil, which is treated as `all`).
        #
        # Reserved names: scope names must not conflict with core query or
        # materializer methods (e.g., :all, :first, :last, :find_by, :pluck).
        def scope(name, body = nil, &block)
          raise ArgumentError, 'scope requires a name' if name.nil?

          impl = body || block
          raise ArgumentError, 'scope requires a callable (Proc/lambda)' if impl.nil? || !impl.respond_to?(:call)

          method_name = name.to_sym

          # Avoid overriding core query methods and relation materializers.
          reserved = %i[
            all first last take count exists? find find_by pluck delete_all update_all
            where rewhere order select include_fields exclude reselect joins preset ranking prefix search
            limit offset page per_page per options cache
          ]
          if reserved.include?(method_name)
            raise ArgumentError, "scope :#{method_name} conflicts with a reserved query method"
          end

          define_singleton_method(method_name) do |*args, **kwargs, &_unused_block|
            base = all

            # Evaluate scope body directly against the fresh relation, so `self`
            # inside the scope is a Relation and chaining behaves predictably.
            result = base.instance_exec(*args, **kwargs, &impl)

            # Coerce common mistakes to a usable Relation:
            # - nil (AR parity) -> return a fresh relation
            # - model class returned by accident -> return a fresh relation
            return base if result.nil? || result.equal?(base.klass)
            return result if result.is_a?(SearchEngine::Relation)

            raise ArgumentError,
                  "scope :#{method_name} must return a SearchEngine::Relation (got #{result.class})"
          end

          __search_engine_scope_registry__[method_name] = impl

          nil
        end
      end
    end
  end
end
