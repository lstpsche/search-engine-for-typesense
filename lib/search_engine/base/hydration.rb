# frozen_string_literal: true

require 'active_support/concern'

module SearchEngine
  class Base
    # Hydration helpers for building instances from Typesense documents and
    # providing attribute readers and views.
    module Hydration
      extend ActiveSupport::Concern

      include SearchEngine::Base::DisplayCoercions

      class_methods do
        # Build a new instance from a Typesense document assigning only declared
        # attributes and capturing any extra keys in {#unknown_attributes}.
        #
        # Unknown keys are preserved as a String-keyed Hash to avoid symbol bloat.
        #
        # @param doc [Hash] a document as returned by Typesense
        # @return [Object] hydrated instance
        def from_document(doc)
          obj = new
          declared = __se_declared_attributes
          declared_joins = __se_declared_joins

          hidden_local = __se_hidden_local_fields
          hidden_join = __se_hidden_join_fields

          unknown = __se_assign_declared_or_unknown(
            obj,
            doc || {},
            declared: declared,
            declared_joins: declared_joins,
            hidden_local: hidden_local,
            hidden_join: hidden_join
          )

          __se_apply_default_joins!(obj, declared_joins: declared_joins, declared: declared)
          __se_freeze_unknown!(obj, unknown)
          obj
        end
      end

      class_methods do
        # Fetch declared attributes; resilient to missing DSL
        def __se_declared_attributes
          attributes
        rescue StandardError
          {}
        end

        # Fetch join declarations; keep existing behavior (use self.class) to avoid logic change
        def __se_declared_joins
          if respond_to?(:joins_config)
            joins_config || {}
          else
            {}
          end
        rescue StandardError
          {}
        end

        private :__se_declared_attributes, :__se_declared_joins
      end

      class_methods do
        # Build the list of hidden local fields (e.g., name_empty)
        def __se_hidden_local_fields
          attr_opts = begin
            respond_to?(:attribute_options) ? attribute_options : {}
          rescue StandardError
            {}
          end

          hidden = []
          attr_opts.each do |fname, opts|
            next unless opts.is_a?(Hash)

            hidden << "#{fname}_empty" if opts[:empty_filtering]
            hidden << "#{fname}_blank" if opts[:optional]
          end
          hidden
        end

        private :__se_hidden_local_fields
      end

      class_methods do
        # Build the list of hidden join fields (e.g., $assoc.field_empty)
        def __se_hidden_join_fields
          hidden = []
          joins_cfg = begin
            respond_to?(:joins_config) ? joins_config : {}
          rescue StandardError
            {}
          end

          joins_cfg.each do |assoc_name, cfg|
            collection = cfg[:collection]
            next if collection.nil? || collection.to_s.strip.empty?

            begin
              target_klass = SearchEngine.collection_for(collection)
              next unless target_klass.respond_to?(:attribute_options)

              opts = target_klass.attribute_options || {}
              opts.each do |field_sym, o|
                next unless o.is_a?(Hash)

                hidden << "#$#{assoc_name}.#{field_sym}_empty".sub('#$', '$') if o[:empty_filtering]
                hidden << "#$#{assoc_name}.#{field_sym}_blank".sub('#$', '$') if o[:optional]
              end
            rescue StandardError
              # Best-effort; skip when registry/metadata unavailable
            end
          end

          hidden
        rescue StandardError
          []
        end

        private :__se_hidden_join_fields
      end

      class_methods do
        # Assign declared and join attributes; collect unknowns (filtering hidden fields)
        def __se_assign_declared_or_unknown(obj, doc, declared:, declared_joins:, hidden_local:, hidden_join:)
          unknown = {}
          doc.each do |k, v|
            key_str = k.to_s
            key_sym = key_str.to_sym
            if declared.key?(key_sym) || declared_joins.key?(key_sym)
              obj.instance_variable_set("@#{key_sym}", v)
            else
              next if hidden_local.include?(key_str) || hidden_join.include?(key_str)

              unknown[key_str] = v
              obj.instance_variable_set('@id', v) if key_str == 'id'
            end
          end
          unknown
        end

        # Ensure default values for missing join attributes based on local_key type
        def __se_apply_default_joins!(obj, declared_joins:, declared:)
          declared_joins.each do |assoc_name, cfg|
            ivar = "@#{assoc_name}"
            next if obj.instance_variable_defined?(ivar)

            lk = cfg[:local_key]
            lk_type = declared[lk]
            default_val = nil
            default_val = [] if lk_type.is_a?(Array) && lk_type.size == 1
            obj.instance_variable_set(ivar, default_val)
          end
        end

        def __se_freeze_unknown!(obj, unknown)
          obj.instance_variable_set(:@__unknown_attributes__, unknown.freeze) unless unknown.empty?
        end

        private :__se_assign_declared_or_unknown, :__se_apply_default_joins!, :__se_freeze_unknown!
      end

      # Return a shallow copy of unknown attributes captured during hydration.
      # Keys are Strings and values are as returned by the backend.
      # @return [Hash{String=>Object}]
      def unknown_attributes
        h = instance_variable_get(:@__unknown_attributes__)
        h ? h.dup : {}
      end

      # Return the document update timestamp coerced to Time.
      #
      # Prefers a declared attribute reader (when present). Falls back to the
      # unknown attributes payload (as returned by the backend) when the field
      # was not declared via the DSL. The value is coerced using the same logic
      # used for console rendering.
      #
      # @return [Time, nil]
      def doc_updated_at
        value = if instance_variable_defined?(:@doc_updated_at)
                  instance_variable_get(:@doc_updated_at)
                else
                  raw = instance_variable_get(:@__unknown_attributes__)
                  if raw&.key?('doc_updated_at')
                    raw['doc_updated_at']
                  elsif raw&.key?(:doc_updated_at)
                    raw[:doc_updated_at]
                  end
                end

        return nil if value.nil?

        __se_coerce_doc_updated_at_for_display(value)
      rescue StandardError
        nil
      end

      # Return a symbol-keyed Hash of attributes for this record.
      #
      # - Includes declared attributes in declaration order
      # - Ensures :doc_updated_at is present and coerced to Time when available
      # - Includes unknown fields under :unknown_attributes (String-keyed), with
      #   "doc_updated_at" removed to avoid duplication
      #
      # @return [Hash{Symbol=>Object}]
      # rubocop:disable Metrics/PerceivedComplexity
      def attributes
        declared = begin
          self.class.respond_to?(:attributes) ? self.class.attributes : {}
        rescue StandardError
          {}
        end

        out = {}

        declared.each_key do |name|
          # Skip non-base (dotted) attribute names when reading ivars
          begin
            next unless self.class.respond_to?(:valid_attribute_reader_name?) &&
                        self.class.valid_attribute_reader_name?(name)
          rescue StandardError
            next if name.to_s.include?('.')
          end

          var = "@#{name}"
          val = instance_variable_get(var)
          out[name] =
            if name.to_s == 'doc_updated_at' && !val.nil?
              __se_coerce_doc_updated_at_for_display(val)
            else
              val
            end
        end

        raw_unknowns = instance_variable_get(:@__unknown_attributes__)
        unknowns = raw_unknowns ? raw_unknowns.dup : {}

        # Ensure :id is present when available (source may be an ivar or unknowns)
        unless out.key?(:id)
          raw_id = if instance_variable_defined?(:@id)
                     instance_variable_get(:@id)
                   else
                     unknowns['id'] || unknowns[:id]
                   end
          out[:id] = raw_id unless raw_id.nil?
        end

        unless out.key?(:doc_updated_at)
          raw_val = unknowns['doc_updated_at']
          raw_val = unknowns[:doc_updated_at] if raw_val.nil?
          out[:doc_updated_at] = __se_coerce_doc_updated_at_for_display(raw_val) unless raw_val.nil?
        end

        # Remove duplicate source of doc_updated_at from nested unknowns
        unknowns.delete('doc_updated_at')
        unknowns.delete(:doc_updated_at)

        out[:unknown_attributes] = unknowns unless unknowns.empty?
        out
      end
      # rubocop:enable Metrics/PerceivedComplexity

      # Return a symbol-keyed Hash representation of this record.
      # Delegates to {#attributes} for consistency.
      #
      # @return [Hash{Symbol=>Object}]
      alias_method :to_h, :attributes

      # Return the Typesense document id if available.
      #
      # @return [Object, nil]
      def id
        value = instance_variable_defined?(:@id) ? instance_variable_get(:@id) : nil
        return value unless value.nil?

        raw = instance_variable_get(:@__unknown_attributes__)
        return nil unless raw

        raw['id'] || raw[:id]
      rescue StandardError
        nil
      end

      # Attribute lookup by key with indifferent access semantics.
      # Supports symbol or string keys and falls back to unknown attributes.
      #
      # @param key [#to_s, #to_sym]
      # @return [Object, nil]
      def [](key)
        attrs = attributes
        return attrs.with_indifferent_access[key] if attrs.respond_to?(:with_indifferent_access)

        return nil if key.nil?

        # Fast path: exact match
        value = attrs[key]
        return value unless value.nil?

        # Symbol/string coercions without depending on ActiveSupport
        if key.respond_to?(:to_sym)
          sym = key.to_sym
          return attrs[sym] if attrs.key?(sym)
        end

        if key.respond_to?(:to_s)
          str = key.to_s
          return attrs[str] if attrs.key?(str)
        end

        nil
      end
    end
  end
end
