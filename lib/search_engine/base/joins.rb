# frozen_string_literal: true

require 'active_support/concern'
require 'active_support/inflector'

module SearchEngine
  class Base
    # Join declarations for server-side joins.
    module Joins
      extend ActiveSupport::Concern

      class_methods do
        # Declare a belongs_to association with auto-resolution of params.
        #
        # Defaults when options are omitted (association-name based):
        # - collection: pluralized first argument (e.g., :brand -> "brands", :promotion_labels -> "promotion_labels")
        # - local_key: singular(name) + "_ids" when the provided name is plural; otherwise singular(name) + "_id"
        #   (e.g., :promotion_labels -> :promotion_label_ids, :brand -> :brand_id)
        # - foreign_key: singular(name) + "_id" (e.g., :promotion_labels -> :promotion_label_id)
        #
        # @param name [#to_sym]
        # @param collection [#to_s, nil]
        # @param local_key [#to_sym, nil]
        # @param foreign_key [#to_sym, nil]
        # @param async_ref [Boolean] when true, mark this reference as asynchronous in schema
        # @return [void]
        def belongs_to(name, collection: nil, local_key: nil, foreign_key: nil, async_ref: nil)
          assoc_name = name.to_sym
          raise ArgumentError, 'belongs_to name must be non-empty' if assoc_name.to_s.strip.empty?

          arg_str = assoc_name.to_s
          arg_plural = ActiveSupport::Inflector.pluralize(arg_str) == arg_str

          target_collection = (collection ? collection.to_s : ActiveSupport::Inflector.pluralize(arg_str))

          assoc_singular = ActiveSupport::Inflector.singularize(arg_str)

          lk = if local_key
                 local_key.to_sym
               else
                 base = assoc_singular
                 (arg_plural ? "#{base}_ids" : "#{base}_id").to_sym
               end

          fk = if foreign_key
                 foreign_key.to_sym
               else
                 "#{assoc_singular}_id".to_sym
               end

          normalized_async = nil
          unless async_ref.nil?
            normalized_async = async_ref ? true : false
          end

          __se_register_join!(
            name: assoc_name,
            collection: target_collection.to_s,
            local_key: lk,
            foreign_key: fk,
            kind: :belongs_to,
            async_ref: normalized_async
          )

          # Define an instance-level association reader for AR-like access.
          # Example: product.brand => returns single associated record or nil; product.brands => Relation
          __se_define_assoc_reader_if_needed!(assoc_name)

          nil
        end
      end

      class_methods do
        # Declare a has association with auto-resolution of params.
        #
        # Defaults when options are omitted:
        # - collection: pluralized first argument
        # - local_key: current collection's singular + "_id"
        # - foreign_key: if argument is plural -> current_singular_ids; else current_singular_id
        #
        # Note: `has` does not contribute a schema reference; only `belongs_to` does.
        #
        # @param name [#to_sym]
        # @param collection [#to_s, nil]
        # @param local_key [#to_sym, nil]
        # @param foreign_key [#to_sym, nil]
        # @return [void]
        def has(name, collection: nil, local_key: nil, foreign_key: nil)
          assoc_name = name.to_sym
          raise ArgumentError, 'has name must be non-empty' if assoc_name.to_s.strip.empty?

          arg_str = assoc_name.to_s
          arg_plural = ActiveSupport::Inflector.pluralize(arg_str) == arg_str

          target_collection = (collection ? collection.to_s : ActiveSupport::Inflector.pluralize(arg_str))

          current_collection = respond_to?(:collection) ? collection : nil
          if current_collection.nil?
            demod = self.name ? ActiveSupport::Inflector.demodulize(self.name) : ''
            current_collection = ActiveSupport::Inflector.underscore(demod).to_s
          end
          current_singular = ActiveSupport::Inflector.singularize(current_collection.to_s)

          lk = local_key ? local_key.to_sym : "#{current_singular}_id".to_sym

          fk = if foreign_key
                 foreign_key.to_sym
               else
                 suffix = arg_plural ? '_ids' : '_id'
                 "#{current_singular}#{suffix}".to_sym
               end

          __se_register_join!(
            name: assoc_name,
            collection: target_collection.to_s,
            local_key: lk,
            foreign_key: fk,
            kind: :has
          )

          # Define an instance-level association reader for AR-like access.
          # Example: author.books => Relation
          __se_define_assoc_reader_if_needed!(assoc_name)

          nil
        end
      end

      class_methods do
        # Define an instance reader for a declared association that resolves
        # the referenced records through the join config.
        #
        # The reader follows these rules:
        # - has :name => returns Relation scoped by foreign_key
        # - belongs_to :singular => returns a single record or nil
        # - belongs_to :plural (local_key array) => returns Relation
        #
        # For array-typed foreign keys on the target, a single local scalar
        # value is wrapped into an Array to preserve IN semantics.
        #
        # @param assoc_name [Symbol]
        # @return [void]
        def __se_define_assoc_reader_if_needed!(assoc_name)
          return if method_defined?(assoc_name)

          define_method(assoc_name) do
            cfg = self.class.join_for(assoc_name)
            target_klass = SearchEngine.collection_for(cfg[:collection])

            local_key = cfg[:local_key].to_sym
            foreign_key = cfg[:foreign_key].to_sym
            kind = (cfg[:kind] || :belongs_to).to_sym

            # Prefer reader when available; fall back to ivar set during hydration
            local_value = if respond_to?(local_key)
                            public_send(local_key)
                          else
                            instance_variable_get("@#{local_key}")
                          end

            # When foreign key on target is an array type, wrap scalar local values
            begin
              attrs = target_klass.respond_to?(:attributes) ? (target_klass.attributes || {}) : {}
              fk_is_array = attrs[foreign_key].is_a?(Array) && attrs[foreign_key].size == 1
            rescue StandardError
              fk_is_array = false
            end

            normalized = local_value
            normalized = [normalized] if fk_is_array && !normalized.nil? && !normalized.is_a?(Array)

            case kind
            when :has
              # For nil local values, return an always-empty relation via id:nil
              return target_klass.where(id: nil) if normalized.nil?

              target_klass.where(foreign_key => normalized)
            else # :belongs_to
              # Singular belongs_to: nil => nil; Array => Relation
              return nil if normalized.nil?

              if normalized.is_a?(Array)
                # Empty local array => empty relation
                return target_klass.where(id: nil) if normalized.empty?

                return target_klass.where(foreign_key => normalized)
              end
              target_klass.find_by(foreign_key => normalized)
            end
          end
        end
        private :__se_define_assoc_reader_if_needed!
      end

      class_methods do
        # Internal: choose foreign_key for belongs_to when not specified.
        # Prefers `<current>_ids` if declared on target; else `<current>_id`.
        # If target has a matching `has` referencing this model's collection, uses plural/singular
        # of that association name to decide ids vs id when attributes are not present.
        def __se_guess_fk_for_belongs_to!(target_collection, current_singular)
          # Try to resolve target model to inspect declared attributes and has-configs.
          target_klass = nil
          begin
            target_klass = SearchEngine.collection_for(target_collection)
          rescue StandardError
            target_klass = nil
          end

          ids_candidate = "#{current_singular}_ids".to_sym
          id_candidate  = "#{current_singular}_id".to_sym

          if target_klass.respond_to?(:attributes)
            attrs = target_klass.attributes || {}
            return ids_candidate if attrs.key?(ids_candidate)
            return id_candidate if attrs.key?(id_candidate)
          end

          # Inspect target joins to see if it declares a has(..) back to our collection.
          if target_klass.respond_to?(:joins_config)
            begin
              cfgs = target_klass.joins_config || {}
              current_coll_name = respond_to?(:collection) ? (collection || '').to_s : ''
              back = cfgs.values.find do |c|
                c[:collection].to_s == current_coll_name && c[:kind].to_s == 'has'
              end
              if back
                # If back assoc name is plural, prefer _ids; else _id
                back_name = back[:name].to_s
                use_ids = ActiveSupport::Inflector.pluralize(back_name) == back_name
                return use_ids ? ids_candidate : id_candidate
              end
            rescue StandardError
              # ignore
            end
          end

          id_candidate
        end
        private :__se_guess_fk_for_belongs_to!
      end

      class_methods do
        # Internal: common registrar used by belongs_to/has and legacy join.
        # Validates, freezes, and stores config with copy-on-write.
        def __se_register_join!(name:, collection:, local_key:, foreign_key:, kind: :belongs_to, async_ref: nil)
          assoc_name = name.to_sym
          raise ArgumentError, 'join name must be non-empty' if assoc_name.to_s.strip.empty?

          coll = collection.to_s
          raise ArgumentError, 'collection must be a non-empty String' if coll.strip.empty?

          lk = local_key.to_sym
          fk = foreign_key.to_sym

          # Validate local_key against declared attributes when available (allow :id implicitly)
          if instance_variable_defined?(:@attributes) && lk != :id && !(@attributes || {}).key?(lk)
            raise SearchEngine::Errors::InvalidField,
                  "Unknown local_key :#{lk} for #{self}. Declare 'attribute :#{lk}, :integer' first."
          end

          rec = {
            name: assoc_name,
            collection: coll,
            local_key: lk,
            foreign_key: fk,
            kind: kind.to_sym
          }.freeze

          # Extend with async_ref when provided (belongs_to only). Keep record frozen at the end.
          unless async_ref.nil?
            base = rec.dup
            base[:async_ref] = async_ref ? true : false
            rec = base.freeze
          end

          current = @joins_config || {}
          if current.key?(assoc_name)
            raise ArgumentError,
                  "Join :#{assoc_name} already defined for #{self}. " \
                  'Use a different name or remove the previous declaration.'
          end

          # copy-on-write write path
          new_map = current.dup
          new_map[assoc_name] = rec
          @joins_config = new_map.freeze

          # lightweight instrumentation (no-op if AS::N is unavailable)
          SearchEngine::Instrumentation.instrument(
            'search_engine.joins.declared',
            model: self.name, name: assoc_name, collection: coll
          )

          nil
        end
        private :__se_register_join!
      end

      class_methods do
        # Declare a joinable association for server-side joins.
        # @param name [#to_sym]
        # @param collection [#to_s]
        # @param local_key [#to_sym]
        # @param foreign_key [#to_sym]
        # @return [void]
        def join(name, collection:, local_key:, foreign_key:)
          __se_register_join!(
            name: name,
            collection: collection,
            local_key: local_key,
            foreign_key: foreign_key,
            kind: :belongs_to
          )
        end
      end

      class_methods do
        # Read-only view of join declarations for this class.
        # @return [Hash{Symbol=>Hash}]
        def joins_config
          (@joins_config || {}).dup.freeze
        end
      end

      class_methods do
        # Lookup a single join configuration by name.
        # @param name [#to_sym]
        # @return [Hash]
        # @raise [SearchEngine::Errors::UnknownJoin]
        def join_for(name)
          key = name.to_sym
          cfg = (@joins_config || {})[key]
          return cfg if cfg

          available = (@joins_config || {}).keys
          raise SearchEngine::Errors::UnknownJoin,
                "Unknown join :#{key} for #{self}. Available: #{available.inspect}."
        end
      end
    end
  end
end
