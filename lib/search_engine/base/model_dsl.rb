# frozen_string_literal: true

require 'active_support/concern'

module SearchEngine
  class Base
    # Model-level DSL for declaring collections, attributes, and inheritance.
    module ModelDsl
      extend ActiveSupport::Concern

      EMBEDDING_SUFFIX = '_embedding'

      class_methods do
        # Get or set the Typesense collection name for this model.
        #
        # When setting, the name is normalized to String and the mapping is
        # registered in the global collection registry.
        #
        # @param name [#to_s, nil]
        # @return [String, Class] returns the current collection name when reading;
        #   returns self when setting (for macro chaining)
        def collection(name = nil)
          return @collection if name.nil?

          normalized = name.to_s
          raise ArgumentError, 'collection name must be non-empty' if normalized.strip.empty?

          @collection = normalized
          SearchEngine.register_collection!(@collection, self)
          self
        end
      end

      class_methods do
        # Delete documents by filter for this collection's physical index.
        # Accepts either a Typesense filter string (via first arg or :filter_by)
        # or a hash of field=>value which will be converted to a filter string.
        # Supports optional partition to cooperate with default_into_resolver.
        #
        # @param filter_or_str [String, nil]
        # @param filter_by [String, nil]
        # @param into [String, nil]
        # @param partition [Object, nil]
        # @param timeout_ms [Integer, nil]
        # @param hash [Hash]
        # @return [Integer] number of deleted documents
        def delete_by(filter_or_str = nil, into: nil, partition: nil, timeout_ms: nil, filter_by: nil, **hash)
          SearchEngine::Deletion.delete_by(
            klass: self,
            filter: filter_or_str || filter_by,
            hash: (hash.empty? ? nil : hash),
            into: into,
            partition: partition,
            timeout_ms: timeout_ms
          )
        end

        alias_method :destroy_by, :delete_by
      end

      class_methods do
        # Set or get the per-collection default query_by fields.
        #
        # Accepts a String (comma-separated), a Symbol, or an Array of Strings/Symbols.
        # Values are normalized into a canonical comma-separated String with single
        # spaces after commas (e.g., "name, brand, description"). When called
        # without arguments, returns the canonical String or nil if unset.
        #
        # @param values [Array<String,Symbol,Array>] zero or more field tokens; Arrays are flattened
        # @return [String, Class] returns the canonical String on read; returns self on write
        def query_by(*values)
          return @__model_default_query_by__ if values.nil? || values.empty?

          flat = values.flatten(1).compact

          list = if flat.size == 1 && flat.first.is_a?(String)
                   flat.first.split(',').map { |s| s.to_s.strip }.reject(&:empty?)
                 else
                   flat.map do |v|
                     case v
                     when String, Symbol then v.to_s.strip
                     else
                       raise ArgumentError, 'query_by accepts Symbols, Strings, or Arrays thereof'
                     end
                   end.reject(&:empty?)
                 end

          canonical = list.join(', ')
          @__model_default_query_by__ = canonical.empty? ? nil : canonical
          self
        end
      end

      class_methods do
        # Declare an attribute with an optional type (symbol preferred).
        #
        # @param name [#to_sym]
        # @param type [Object] type descriptor (e.g., :string, :integer)
        # @param index [Boolean, nil] when false, omit from compiled Typesense schema (still
        #   hydrated/displayed). Fields referenced by an embedding's `from:` are kept in the
        #   schema with Typesense-native `"index": false` instead of being omitted entirely.
        # @param locale [String, nil]
        # @param optional [Boolean, nil]
        # @param sort [Boolean, nil]
        # @param infix [Boolean, nil]
        # @param empty_filtering [Boolean, nil]
        # @param nested [Hash, nil]
        # @return [void]
        def attribute(name, type = :string, index: nil, locale: nil, optional: nil, sort: nil, infix: nil,
                      empty_filtering: nil, facet: nil, nested: nil)
          n = name.to_sym
          __se_validate_attribute_name!(n)
          __se_assign_attribute!(n, type)
          __se_update_attribute_options!(n, type, locale: locale, optional: optional, sort: sort, infix: infix,
                                         empty_filtering: empty_filtering, facet: facet, index: index
          )
          __se_define_reader_if_needed!(n)
          __se_expand_nested_fields!(n, type, nested)
          nil
        end
      end

      class_methods do
        # Validate reserved names and raise when invalid.
        def __se_validate_attribute_name!(name_sym)
          return unless name_sym == :id

          raise SearchEngine::Errors::InvalidField,
                'The :id field is reserved; use `identify_by` to set the Typesense document id.'
        end
      end

      class_methods do
        # Assign base attribute type.
        def __se_assign_attribute!(name_sym, type)
          (@attributes ||= {})[name_sym] = type
        end
      end

      class_methods do
        # Update per-attribute options from keyword arguments.
        def __se_update_attribute_options!(
          name_sym,
          type,
          locale:,
          optional:,
          sort:,
          infix:,
          empty_filtering:,
          facet:,
          index:
        )
          has_opts = [locale, optional, sort, infix, empty_filtering, facet, index].any? { |v| !v.nil? }
          if has_opts
            @attribute_options ||= {}
            new_opts = __se_build_attribute_options_for(
              name_sym, type,
              locale: locale, optional: optional, sort: sort, infix: infix,
              empty_filtering: empty_filtering, facet: facet, index: index
            )

            if new_opts.empty?
              @attribute_options = @attribute_options.dup
              @attribute_options.delete(name_sym)
            else
              @attribute_options[name_sym] = new_opts
            end
          elsif instance_variable_defined?(:@attribute_options) && (@attribute_options || {}).key?(name_sym)
            # When re-declared without options, keep prior options as-is (idempotent)
          end
        end
      end

      class_methods do
        # Define an instance reader for the attribute when safe to do so.
        # For boolean attributes, also defines a question-mark alias (e.g., available?).
        def __se_define_reader_if_needed!(name_sym)
          reader_defined = valid_attribute_reader_name?(name_sym) && !method_defined?(name_sym)
          attr_reader name_sym if reader_defined

          # Always check for boolean alias, even if reader was already defined
          # (handles case where attribute type changes to boolean)
          __se_define_boolean_alias_if_needed!(name_sym) if valid_attribute_reader_name?(name_sym)
        end

        # Define a question-mark alias for boolean attributes.
        # @param name_sym [Symbol] attribute name
        def __se_define_boolean_alias_if_needed!(name_sym)
          type = (@attributes || {})[name_sym]
          return unless type == :boolean

          alias_name = "#{name_sym}?".to_sym
          return if method_defined?(alias_name)

          alias_method alias_name, name_sym
        end

        private :__se_define_boolean_alias_if_needed!
      end

      class_methods do
        # Expand nested subfields for object/object[] attributes when nested: is provided.
        def __se_expand_nested_fields!(name_sym, type, nested)
          return if nested.nil? || (nested.respond_to?(:empty?) && nested.empty?)

          unless nested.is_a?(Hash)
            raise SearchEngine::Errors::InvalidOption,
                  '`nested` must be a Hash of field_name => type'
          end

          is_object = type.to_s.downcase == 'object'
          is_object_array = type.is_a?(Array) && type.size == 1 && type.first.to_s.downcase == 'object'
          unless is_object || is_object_array
            raise SearchEngine::Errors::InvalidOption,
                  "`nested:` is only valid for :object or [:object] attributes (got #{type.inspect})"
          end

          nested.each do |child_name, child_type|
            effective = __se_compute_nested_type_descriptor(child_type, array: is_object_array)
            attribute("#{name_sym}.#{child_name}".to_sym, effective)
          end
        end
      end

      class_methods do
        # Declare nested fields under a base object/object[] attribute.
        #
        # Usage:
        #   attribute :retail_prices, [:object]
        #   nested :retail_prices,
        #     current_price: :float,
        #     price_type: :string
        #
        # When the base is :object, nested field types are scalar (e.g., :float -> "float").
        # When the base is [:object], nested field types are array (e.g., :float -> "float[]").
        #
        # @param base [Symbol, String] base field name that must be declared as :object or [:object]
        # @param fields [Hash{Symbol=>Object}] map of nested field name => type descriptor
        # @return [void]
        # @raise [SearchEngine::Errors::InvalidOption] when base is not declared as object/object[]
        def nested(base, **fields)
          base_sym = base.to_sym
          attrs = @attributes || {}
          base_type = attrs[base_sym]

          is_object = base_type.to_s.downcase == 'object'
          is_object_array = base_type.is_a?(Array) && base_type.size == 1 && base_type.first.to_s.downcase == 'object'

          unless is_object || is_object_array
            raise SearchEngine::Errors::InvalidOption,
                  "`nested` requires base attribute #{base_sym.inspect} to be declared as :object or [:object] " \
                  "(got #{base_type.inspect})"
          end

          fields.each do |name, type_descriptor|
            effective_type = __se_compute_nested_type_descriptor(type_descriptor, array: is_object_array)
            # Dotted attribute name is intentional and supported by the schema compiler.
            attribute("#{base_sym}.#{name}".to_sym, effective_type)
          end

          nil
        end

        # Normalize a nested type descriptor to scalar or array form depending on the parent multiplicity.
        # Accepts Symbols (e.g., :float), Arrays (e.g., [:float]), or Strings (e.g., "float", "float[]").
        def __se_compute_nested_type_descriptor(type_descriptor, array:)
          # Already an array type in DSL form ([:float])
          if type_descriptor.is_a?(Array) && type_descriptor.size == 1
            return type_descriptor if array

            return type_descriptor.first
          end

          # String forms like "float[]" or canonical names
          if type_descriptor.is_a?(String)
            s = type_descriptor.strip
            if s.end_with?('[]')
              inner = s[0..-3]
              return array ? [inner.to_sym] : inner.to_sym
            end
            return array ? [s.to_sym] : s.to_sym
          end

          # Symbol or other single token
          array ? [type_descriptor] : type_descriptor
        end
      end

      class_methods do
        # Declare a vector embedding field with automatic name resolution, model
        # inference, and source-field validation.
        #
        # @param name [Symbol, String, nil] field name (auto-derived when omitted)
        # @param from [Array<Symbol>, nil] source attribute names to embed from
        # @param suffix [Boolean] append `_embedding` to the field name (default: true)
        # @param model [String, nil] embedding model override (per-field)
        # @param api_key [String, nil] API key for remote embedding providers
        # @param num_dim [Integer, nil] vector dimensions for external embeddings
        # @param hnsw [Hash, nil] HNSW index tuning parameters
        # @param model_config [Hash, nil] extra model_config overrides
        # @return [void]
        def embedding(name = nil, from: nil, suffix: true, model: nil,
                      api_key: nil, num_dim: nil, hnsw: nil, model_config: nil)
          resolved_name = __se_resolve_embedding_name(name, from: from, suffix: suffix, num_dim: num_dim)
          resolved_sym = resolved_name.to_sym

          __se_check_embedding_duplicate!(resolved_sym)

          external = from.nil? && num_dim
          from_fields = external ? nil : __se_infer_embedding_from(resolved_name, from)

          __se_validate_embedding_sources!(from_fields) if from_fields

          vector_opts = {}

          if external
            vector_opts[:num_dim] = Integer(num_dim)
          else
            resolved_model = __se_resolve_embedding_model(model)
            vector_opts[:embed] = __se_build_embed_hash(
              from_fields, resolved_model,
              api_key: api_key, model_config: model_config
            )
          end

          vector_opts[:hnsw_params] = hnsw if hnsw

          attribute(resolved_name, :vector)
          @attribute_options ||= {}
          @attribute_options[resolved_sym] = (@attribute_options[resolved_sym] || {}).merge(vector_opts)

          __se_store_embedding_metadata!(
            resolved_sym, from: from_fields,
            model: external ? nil : resolved_model,
            external: external ? true : false,
            num_dim: num_dim
          )
        end

        private

        # Resolve the canonical field name for an embedding declaration.
        # @return [String]
        def __se_resolve_embedding_name(name, from:, suffix:, num_dim:)
          if name.nil?
            if from
              "#{self.name.demodulize.underscore}#{EMBEDDING_SUFFIX}"
            elsif num_dim
              raise ArgumentError,
                    'External embedding (num_dim: without from:) requires an explicit field name'
            else
              raise ArgumentError,
                    'embedding requires at least one of: a field name, from: sources, or num_dim: for external vectors'
            end
          else
            n = name.to_s
            if suffix && !n.end_with?(EMBEDDING_SUFFIX)
              "#{n}#{EMBEDDING_SUFFIX}"
            else
              n
            end
          end
        end

        # Infer `from:` source fields when not explicitly provided.
        # @return [Array<Symbol>]
        def __se_infer_embedding_from(resolved_name, from)
          if from
            Array(from).map(&:to_sym)
          else
            bare = resolved_name.to_s.delete_suffix(EMBEDDING_SUFFIX)
            if bare.empty?
              raise ArgumentError,
                    "Cannot infer from: for embedding '#{resolved_name}'; provide from: explicitly"
            end

            [bare.to_sym]
          end
        end

        # Validate that all source fields exist and are string-typed.
        # @raise [ArgumentError]
        def __se_validate_embedding_sources!(from_fields)
          attrs = @attributes || {}
          from_fields.each do |field|
            unless attrs.key?(field)
              raise ArgumentError,
                    "embedding from: references undeclared attribute :#{field}. " \
                    'Declare it with `attribute` before the `embedding` call.'
            end

            ts_type = __se_typesense_type_for(attrs[field])
            next if %w[string string[]].include?(ts_type)

            raise ArgumentError,
                  "embedding from: field :#{field} must be string-typed " \
                  "(got :#{attrs[field]} -> \"#{ts_type}\"). " \
                  'Typesense only auto-embeds text fields.'
          end
        end

        # Resolve the embedding model with fallback to global config.
        # @return [String]
        # @raise [SearchEngine::Errors::ConfigurationError]
        def __se_resolve_embedding_model(per_field_model)
          return per_field_model if per_field_model && !per_field_model.to_s.strip.empty?

          global = SearchEngine.config.embedding.model
          return global if global && !global.to_s.strip.empty?

          raise SearchEngine::Errors::ConfigurationError.new(
            'No embedding model configured. Set `model:` on the embedding declaration ' \
            'or set `SearchEngine.config.embedding.model` globally.',
            hint: "Add `config.embedding.model = 'ts/all-MiniLM-L12-v2'` to your SearchEngine initializer.",
            doc: 'https://typesense.org/docs/30.1/api/vector-search.html#option-b-auto-embedding-generation-within-typesense'
          )
        end

        # Build the Typesense `embed` hash for auto-embedding fields.
        # @return [Hash]
        def __se_build_embed_hash(from_fields, model_name, api_key: nil, model_config: nil)
          mc = {}

          global_mc = SearchEngine.config.embedding.model_config
          mc.merge!(global_mc) if global_mc.is_a?(Hash)
          mc.merge!(model_config) if model_config.is_a?(Hash)
          mc[:model_name] = model_name

          resolved_api_key = api_key || SearchEngine.config.embedding.api_key
          mc[:api_key] = resolved_api_key if resolved_api_key && !resolved_api_key.to_s.strip.empty?

          { from: from_fields.map(&:to_s), model_config: mc }
        end

        # Raise on duplicate embedding field names.
        def __se_check_embedding_duplicate!(resolved_sym)
          return unless (@embeddings_config || {}).key?(resolved_sym)

          raise ArgumentError, "Duplicate embedding field :#{resolved_sym} already declared"
        end

        # Store embedding metadata for downstream consumers (mapper, indexer, compiler).
        def __se_store_embedding_metadata!(resolved_sym, from:, model:, external:, num_dim:)
          @embeddings_config ||= {}
          @embeddings_config[resolved_sym] = {
            field_name: resolved_sym.to_s,
            from: from,
            model: model,
            external: external,
            num_dim: num_dim
          }.compact
        end

        # Minimal type resolution for validation (mirrors Schema.typesense_type_for).
        def __se_typesense_type_for(type_descriptor)
          if type_descriptor.is_a?(Array) && type_descriptor.size == 1
            inner = type_descriptor.first
            mapped = SearchEngine::Schema::TYPE_MAPPING[inner.to_s.downcase.to_sym] || inner.to_s
            return "#{mapped}[]"
          end

          SearchEngine::Schema::TYPE_MAPPING[type_descriptor.to_s.downcase.to_sym] || type_descriptor.to_s
        end
      end

      class_methods do
        # Validate whether an attribute name is a valid Ruby reader method name
        # (skip dotted names and other invalid identifiers).
        def valid_attribute_reader_name?(name)
          s = name.to_s
          return false if s.empty?
          return false unless s.match?(/\A[a-zA-Z_]\w*\z/)

          true
        end

        def __se_build_attribute_options_for(
          n,
          type,
          locale:,
          optional: nil,
          sort: nil,
          infix: nil,
          empty_filtering: nil,
          facet: nil,
          index: nil
        )
          new_opts = (@attribute_options[n] || {}).dup

          # locale
          if locale.nil?
            new_opts.delete(:locale)
          else
            is_string = type.to_s.downcase == 'string'
            is_string_array = type.is_a?(Array) && type.size == 1 && type.first.to_s.downcase == 'string'
            unless is_string || is_string_array
              raise SearchEngine::Errors::InvalidOption,
                    "`locale` is only applicable to :string and [:string] (got #{type.inspect})"
            end
            new_opts[:locale] = locale.to_s
          end

          new_opts = __se_apply_optional_sort_empty_filtering(
            new_opts,
            type,
            optional: optional,
            sort: sort,
            infix: infix,
            empty_filtering: empty_filtering
          )

          # facet
          if facet.nil?
            new_opts.delete(:facet)
          else
            __se_ensure_boolean!(:facet, facet)
            new_opts[:facet] = facet ? true : false
          end

          # index flag (default is true; only store when provided)
          unless index.nil?
            __se_ensure_boolean!(:index, index)
            new_opts[:index] = index ? true : false
          end

          new_opts
        end

        private :__se_build_attribute_options_for
      end

      class_methods do
        # optional, sort, infix, empty_filtering extracted to a separate block to
        # satisfy Metrics/BlockLength without changing semantics.
        def __se_ensure_boolean!(name, value)
          return if [true, false].include?(value)

          raise SearchEngine::Errors::InvalidOption,
                "`#{name}` should be of boolean data type (currently is #{value.class})"
        end

        def __se_apply_optional_sort_empty_filtering(new_opts, type, optional:, sort:, infix:, empty_filtering:)
          # optional
          if optional.nil?
            new_opts.delete(:optional)
          else
            __se_ensure_boolean!(:optional, optional)
            new_opts[:optional] = optional ? true : false
          end

          # sort
          if sort.nil?
            new_opts.delete(:sort)
          else
            __se_ensure_boolean!(:sort, sort)
            new_opts[:sort] = sort ? true : false
          end

          # infix
          if infix.nil?
            new_opts.delete(:infix)
          else
            __se_ensure_boolean!(:infix, infix)
            new_opts[:infix] = infix ? true : false
          end

          # empty_filtering
          if empty_filtering.nil?
            new_opts.delete(:empty_filtering)
          else
            is_array_type = type.is_a?(Array) && type.size == 1
            unless is_array_type
              raise SearchEngine::Errors::InvalidOption,
                    "`empty_filtering` is only applicable to array types (e.g., [:string]); got #{type.inspect}"
            end
            new_opts[:empty_filtering] = empty_filtering ? true : false
          end

          new_opts
        end

        private :__se_apply_optional_sort_empty_filtering
      end

      class_methods do
        # Read-only view of declared attributes for this class.
        def attributes
          (@attributes || {}).dup.freeze
        end
      end

      class_methods do
        # Read-only view of declared per-attribute options (e.g., locale).
        def attribute_options
          (@attribute_options || {}).dup.freeze
        end
      end

      class_methods do
        # Read-only view of declared embedding metadata for this class.
        # @return [Hash{Symbol=>Hash}] frozen hash keyed by embedding field name
        def embeddings_config
          (@embeddings_config || {}).dup.freeze
        end
      end

      class_methods do
        # Configure schema retention policy for this collection.
        # @param keep_last [Integer] how many previous physicals to keep after swap
        # @return [void]
        def schema_retention(keep_last: nil)
          return (@schema_retention || {}).dup.freeze if keep_last.nil?

          value = Integer(keep_last)
          raise ArgumentError, 'keep_last must be >= 0' if value.negative?

          @schema_retention ||= {}
          @schema_retention[:keep_last] = value
          nil
        end
      end

      class_methods do
        # Hook to ensure subclasses inherit attributes and schema retention from their parent.
        def inherited(subclass)
          super
          parent_attrs = @attributes || {}
          subclass.instance_variable_set(:@attributes, parent_attrs.dup)

          parent_attr_opts = @attribute_options || {}
          subclass.instance_variable_set(:@attribute_options, parent_attr_opts.dup)

          parent_retention = @schema_retention || {}
          subclass.instance_variable_set(:@schema_retention, parent_retention.dup)

          parent_embeddings = @embeddings_config || {}
          subclass.instance_variable_set(:@embeddings_config, parent_embeddings.dup)

          parent_joins = @joins_config || {}
          subclass.instance_variable_set(:@joins_config, parent_joins.dup.freeze)

          if instance_variable_defined?(:@__declared_default_preset__)
            token = instance_variable_get(:@__declared_default_preset__)
            subclass.instance_variable_set(:@__declared_default_preset__, token)
          end

          if instance_variable_defined?(:@__model_default_query_by__)
            qb = instance_variable_get(:@__model_default_query_by__)
            subclass.instance_variable_set(:@__model_default_query_by__, qb)
          end

          return unless instance_variable_defined?(:@identify_by_proc)

          subclass.instance_variable_set(:@identify_by_proc, @identify_by_proc)
          # Propagate identify_by metadata for type hints
          if instance_variable_defined?(:@__identify_by_kind__)
            subclass.instance_variable_set(:@__identify_by_kind__, @__identify_by_kind__)
          end
          return unless instance_variable_defined?(:@__identify_by_symbol__)

          subclass.instance_variable_set(:@__identify_by_symbol__, @__identify_by_symbol__)
        end
      end
    end
  end
end
