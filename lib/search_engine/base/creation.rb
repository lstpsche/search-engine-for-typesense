# frozen_string_literal: true

require 'active_support/concern'
require 'set'
require 'json'
require 'search_engine/indexer/batch_planner'

module SearchEngine
  class Base
    # Creation helpers for inserting a single document into a collection.
    #
    # Provides ActiveRecord-like `.create(attrs)` that validates and normalizes
    # attributes against the compiled schema, computes hidden flags and forces
    # the `doc_updated_at` timestamp. Returns a hydrated model instance.
    module Creation
      extend ActiveSupport::Concern

      # Internal helpers extracted to keep the public API lean and within style limits.
      module Helpers
        module_function

        def normalize_attrs_to_document(attrs)
          out = {}
          attrs.each { |k, v| out[k.to_s] = v }
          out
        end

        def compute_id_for_create(klass, attrs)
          src_type = source_type_for(klass)

          if src_type == :active_record
            # Try *_id fallback names first
            fallback_id_field_names_for(klass).each do |fk|
              key_sym = fk.to_sym
              next unless attrs.key?(fk) || attrs.key?(key_sym)

              raw = attrs[fk] || attrs[key_sym]
              return raw.to_s unless raw.nil? || raw.to_s.strip.empty?
            end
            # Fallthrough to identify_by if explicitly defined
            return compute_from_identify_by(klass, attrs) if identify_by_defined?(klass)

            # Otherwise unresolved
            return nil
          end

          # Non-AR source:
          # - If identify_by is defined, use it
          return compute_from_identify_by(klass, attrs) if identify_by_defined?(klass)

          # - Else must be provided via :id (handled by caller); unresolved here
          nil
        end

        def identify_by_defined?(klass)
          klass.instance_variable_defined?(:@identify_by_proc)
        end

        def compute_from_identify_by(klass, attrs)
          require 'ostruct'
          shim = OpenStruct.new(attrs)
          val = klass.compute_document_id(shim)
          v = val.is_a?(String) ? val : val.to_s
          v.to_s.strip.empty? ? nil : v
        rescue StandardError
          nil
        end

        def source_type_for(klass)
          t = klass.instance_variable_get(:@__mapper_dsl__)&.dig(:source, :type)
          t&.to_sym
        rescue StandardError
          nil
        end

        def fallback_id_field_names_for(klass)
          # Prefer AR model name from index DSL when available
          names = []
          base_name = nil
          model = klass.instance_variable_get(:@__mapper_dsl__)&.dig(:source, :options, :model)
          if model.respond_to?(:name)
            base_name = model.name.to_s.split('::').last
          elsif model.is_a?(String)
            base_name = model.to_s.split('::').last
          end

          names << "#{ActiveSupport::Inflector.underscore(base_name)}_id" if base_name
          se_base = klass.name.to_s.split('::').last
          names << "#{ActiveSupport::Inflector.underscore(se_base)}_id"
          names.uniq
        rescue StandardError
          se_base = klass.name.to_s.split('::').last
          ["#{ActiveSupport::Inflector.underscore(se_base)}_id"]
        end

        def update_doc_updated_at!(document)
          now_i = if defined?(Time) && defined?(Time.zone) && Time.zone
                    Time.zone.now.to_i
                  else
                    Time.now.to_i
                  end
          document['doc_updated_at'] = now_i
        end

        def build_types_by_field_from_schema(compiled_schema)
          h = {}
          Array(compiled_schema[:fields]).each do |f|
            h[(f[:name] || f['name']).to_s] = (f[:type] || f['type']).to_s
          end
          h
        end

        def compute_required_keys_from_schema(klass, compiled_schema)
          fields = Array(compiled_schema[:fields]).map { |f| (f[:name] || f['name']).to_s }
          base = fields.reject { |fname| fname.include?('.') }.to_set
          begin
            opts = klass.respond_to?(:attribute_options) ? (klass.attribute_options || {}) : {}
          rescue StandardError
            opts = {}
          end

          opts.each do |fname, o|
            next unless o.is_a?(Hash) && o[:optional]

            base.delete(fname.to_s)
          end

          base
        end

        def append_hidden_flags!(klass, document, allowed_keys)
          begin
            opts = klass.respond_to?(:attribute_options) ? (klass.attribute_options || {}) : {}
          rescue StandardError
            opts = {}
          end

          opts.each do |fname, conf|
            base = fname.to_s
            next unless conf.is_a?(Hash)

            if conf[:empty_filtering]
              hidden = "#{base}_empty"
              next unless allowed_keys.include?(hidden)

              value = document[base]
              document[hidden] = value.nil? || (value.is_a?(Array) && value.empty?)
            end

            next unless conf[:optional]

            hidden = "#{base}_blank"
            next unless allowed_keys.include?(hidden)

            value = document[base]
            document[hidden] = value.nil?
          end

          nil
        end

        def prune_nil_optional_fields!(klass, document)
          begin
            opts = klass.respond_to?(:attribute_options) ? (klass.attribute_options || {}) : {}
          rescue StandardError
            opts = {}
          end

          opts.each do |fname, conf|
            next unless conf.is_a?(Hash) && conf[:optional]

            key = fname.to_s
            document.delete(key) if document[key].nil?
          end

          nil
        end

        def strict_unknown_keys_enabled?
          SearchEngine.config&.mapper&.strict_unknown_keys ? true : false
        rescue StandardError
          false
        end

        def coercions_enabled?
          cfg = SearchEngine.config&.mapper&.coercions || {}
          cfg[:enabled] ? true : false
        rescue StandardError
          false
        end

        def validate_and_coerce_types!(klass, document, types_by_field, coercions_enabled)
          # Collect optional fields from the model DSL to allow nil values for them
          optional_fields =
            begin
              opts = klass.respond_to?(:attribute_options) ? (klass.attribute_options || {}) : {}
              opts.each_with_object(Set.new) do |(fname, conf), acc|
                acc << fname.to_s if conf.is_a?(Hash) && conf[:optional]
              end
            rescue StandardError
              Set.new
            end

          document.each do |key, value|
            expected = types_by_field[key.to_s]
            next unless expected

            # Skip type validation for nil values of optional fields
            next if value.nil? && optional_fields.include?(key.to_s)

            valid, coerced, err = validate_value_for_type(expected, value, coercions_enabled: coercions_enabled)
            if coerced
              document[key.to_s] = coerced
            elsif !valid
              raise SearchEngine::Errors::InvalidParams.new(
                err,
                doc: 'docs/indexer.md#troubleshooting',
                details: { field: key.to_s, expected: expected, got: value.class.name }
              )
            end
          end
        end

        def validate_value_for_type(expected, value, coercions_enabled: false)
          case expected
          when 'int64', 'int32'
            # Accept Time universally by coercing to epoch seconds
            return [true, value.to_i, true] if value.is_a?(Time)

            validate_integer(value, coercions_enabled)
          when 'float'
            validate_float(value, coercions_enabled)
          when 'bool'
            validate_bool(value, coercions_enabled)
          when 'string'
            # Accept Time/Date/DateTime universally by coercing to ISO8601
            if value.is_a?(Time)
              return [true, value.iso8601, true]
            elsif defined?(DateTime) && value.is_a?(DateTime)
              return [true, value.to_time.utc.iso8601, true]
            elsif defined?(Date) && value.is_a?(Date)
              return [true, value.to_time.utc.iso8601, true]
            end

            [value.is_a?(String), nil, invalid_type_message('String', value)]
          when 'string[]'
            return [true, nil, nil] if value.is_a?(Array) && value.all? { |v| v.is_a?(String) }

            [false, nil, invalid_type_message('Array<String>', value)]
          else
            [true, nil, nil]
          end
        end

        def validate_integer(value, coercions_enabled)
          if value.is_a?(Integer)
            [true, nil, nil]
          elsif coercions_enabled && string_integer?(value)
            [true, Integer(value), true]
          else
            [false, nil, invalid_type_message('Integer', value)]
          end
        end

        def validate_float(value, coercions_enabled)
          if value.is_a?(Numeric) && finite_number?(value)
            [true, nil, nil]
          elsif coercions_enabled && string_float?(value)
            f =
              begin
                Float(value)
              rescue StandardError
                nil
              end
            if f && finite_number?(f)
              [true, f, true]
            else
              [false, nil, invalid_type_message('Float', value)]
            end
          else
            [false, nil, invalid_type_message('Float', value)]
          end
        end

        def validate_bool(value, coercions_enabled)
          if [true, false].include?(value)
            [true, nil, nil]
          elsif coercions_enabled && %w[true false 1 0].include?(value.to_s.downcase)
            [true, %w[true 1].include?(value.to_s.downcase), true]
          else
            [false, nil, invalid_type_message('Boolean', value)]
          end
        end

        def string_integer?(v)
          v.is_a?(String) && v.match?(/^[-+]?\d+$/)
        end

        def string_float?(v)
          v.is_a?(String) && v.match?(/^[-+]?\d*(?:\.\d+)?$/)
        end

        def finite_number?(v)
          return v.finite? if v.is_a?(Float)

          true
        end

        def invalid_type_message(expected, got)
          got_class = got.nil? ? 'NilClass' : got.class.name
          got_preview = got.is_a?(String) ? got[0, 50] : got.to_s[0, 50]
          "Invalid type (expected #{expected}, got #{got_class}: \"#{got_preview}\")."
        end

        def validate_required_and_unknown!(klass, present_keys, allowed_keys, required_keys)
          missing = required_keys - present_keys
          unless missing.empty?
            msg = "Missing required fields: #{missing.to_a.sort.inspect} for #{klass.name}."
            raise SearchEngine::Errors::InvalidParams.new(
              msg,
              doc: 'docs/indexer.md#troubleshooting',
              details: { missing_required: missing.to_a.sort }
            )
          end

          extras = present_keys - allowed_keys
          return unless strict_unknown_keys_enabled? && extras.any?

          msg = [
            'Unknown fields detected:',
            "#{extras.to_a.sort.inspect} (set mapper.strict_unknown_keys)."
          ].join(' ')
          raise SearchEngine::Errors::InvalidField.new(
            msg,
            doc: 'docs/indexer.md#troubleshooting',
            details: { extras: extras.to_a.sort }
          )
        end

        def resolve_target_collection(klass, into:, partition: nil)
          return into.to_s if into && !into.to_s.strip.empty?

          begin
            ctx_into = SearchEngine::Instrumentation.context[:into]
            return ctx_into if ctx_into && !ctx_into.to_s.strip.empty?
          rescue StandardError
            # fall through to default resolution
          end

          resolver = begin
            SearchEngine.config.partitioning&.default_into_resolver
          rescue StandardError
            nil
          end

          if resolver.respond_to?(:arity)
            case resolver.arity
            when 1
              val = resolver.call(klass)
              return val if val && !val.to_s.strip.empty?
            when 2, -1
              val = resolver.call(klass, partition)
              return val if val && !val.to_s.strip.empty?
            end
          elsif resolver
            val = resolver.call(klass)
            return val if val && !val.to_s.strip.empty?
          end

          if klass.respond_to?(:collection)
            klass.collection.to_s
          else
            klass.name.to_s
          end
        end

        def ensure_document_id!(klass, document)
          id_value = document['id'] || document[:id]
          return if id_value && !id_value.to_s.strip.empty?

          computed = compute_id_for_create(klass, document)
          if computed.nil? || computed.to_s.strip.empty?
            raise SearchEngine::Errors::InvalidParams,
                  'Document id could not be resolved. Provide :id or define identify_by.'
          end
          document['id'] = computed
        end

        def normalize_document!(klass, document, types_by_field, allowed_keys, required_keys)
          ensure_document_id!(klass, document)
          update_doc_updated_at!(document)
          append_hidden_flags!(klass, document, allowed_keys)
          prune_nil_optional_fields!(klass, document)

          present = document.keys.map(&:to_s).to_set
          validate_required_and_unknown!(klass, present, allowed_keys, required_keys)
          validate_and_coerce_types!(klass, document, types_by_field, coercions_enabled?)
          document
        end

        def normalize_mapped_data!(_klass, hash)
          unless hash.is_a?(Hash)
            raise SearchEngine::Errors::InvalidParams,
                  'Mapped data must be a Hash with string/symbol keys.'
          end

          out = {}
          hash.each do |key, value|
            out[key.to_s] = value
          end
          out
        end

        def mapper_for!(klass)
          mapper = SearchEngine::Mapper.for(klass)
          return mapper if mapper

          raise SearchEngine::Errors::InvalidParams,
                "mapper is not defined for #{klass.name}. Define it via `index do ... end`."
        end

        def map_records!(klass, records)
          mapper = mapper_for!(klass)
          rows = Array(records)
          docs, = mapper.map_batch!(rows, batch_index: 0)
          docs.map do |doc|
            out = {}
            doc.each do |key, value|
              out[key.to_s] = value
            end
            out
          end
        end

        def encode_jsonl!(docs)
          buffer = +''
          count, bytes = SearchEngine::Indexer::BatchPlanner.encode_jsonl!(docs, buffer)
          [count, bytes, buffer]
        end

        def prepare_documents(klass, records:, data:)
          if records && data
            raise SearchEngine::Errors::InvalidParams,
                  'Provide either :records or :data, not both.'
          end

          source_docs =
            if records
              array = normalize_records_input(records)
              return [] if array.empty?

              map_records!(klass, array)
            elsif data
              docs_arr = normalize_data_input(data)
              return [] if docs_arr.empty?

              docs_arr.map { |doc| normalize_mapped_data!(klass, doc) }
            else
              raise SearchEngine::Errors::InvalidParams,
                    'Provide :records or :data.'
            end

          compiled = SearchEngine::Schema.compile(klass)
          types_by_field = build_types_by_field_from_schema(compiled)
          allowed_keys = compute_required_keys_from_schema(klass, compiled)
          required_keys = compute_required_keys_from_schema(klass, compiled)

          source_docs.map do |doc|
            normalize_document!(klass, doc, types_by_field, allowed_keys, required_keys)
          end
        end

        def import_documents!(klass, docs, into:, partition: nil)
          collection = resolve_target_collection(klass, into: into, partition: partition)
          if docs.empty?
            return {
              collection: collection,
              docs_count: 0,
              success_count: 0,
              failure_count: 0,
              bytes_sent: 0,
              response: nil
            }
          end

          count, bytes, jsonl = encode_jsonl!(docs)
          raw = SearchEngine::Client.new.import_documents(collection: collection, jsonl: jsonl, action: :upsert)

          {
            collection: collection,
            docs_count: count,
            success_count: count,
            failure_count: 0,
            bytes_sent: bytes,
            response: raw
          }
        end

        def safe_parse_json(str)
          JSON.parse(str)
        rescue StandardError
          nil
        end

        def normalize_records_input(records)
          if records.is_a?(Array)
            records
          elsif records.respond_to?(:to_a)
            Array(records.to_a)
          else
            Array(records)
          end
        end

        def normalize_data_input(data)
          if data.is_a?(Array)
            data
          elsif data.is_a?(Hash)
            [data]
          elsif data.respond_to?(:to_a)
            Array(data.to_a)
          else
            Array(data)
          end
        end

        def hydrate_from_document(klass, doc)
          hash = doc || {}
          return klass.from_document(hash) if klass.respond_to?(:from_document)

          obj = klass.new
          hash.each do |key, value|
            obj.instance_variable_set("@#{key}", value)
          end
          obj
        end
      end

      class_methods do
        # Create a document in the backing Typesense collection and return a hydrated instance.
        #
        # - Validates required fields (respects `optional` attributes) and rejects unknown fields
        #   when `mapper.strict_unknown_keys` is enabled.
        # - Applies basic type validation and optional coercions for numeric and boolean fields
        #   based on mapper coercion settings.
        # - Computes hidden flags `<name>_empty` and `<name>_blank` when present in the schema.
        # - Sets `doc_updated_at` to the current timestamp (seconds).
        # - Uses provided `:id` when present; otherwise attempts to compute id using `identify_by`.
        #
        # @param attrs [Hash, nil] document attributes when passed as a single Hash
        # @param into [String, nil] explicit physical collection override (falls back to alias or logical)
        # @param timeout_ms [Integer, nil] reserved for future use
        # @return [Object] hydrated instance of this model
        # @raise [SearchEngine::Errors::InvalidParams, SearchEngine::Errors::InvalidField]
        def create(attrs = nil, into: nil, _timeout_ms: nil, **kwargs)
          raw_attrs = attrs.nil? ? kwargs : attrs
          raise SearchEngine::Errors::InvalidParams, 'attrs must be a Hash or keyword args' unless raw_attrs.is_a?(Hash)

          compiled = SearchEngine::Schema.compile(self)
          types_by_field = Helpers.build_types_by_field_from_schema(compiled)
          allowed_keys = Helpers.compute_required_keys_from_schema(self, compiled)
          required_keys = Helpers.compute_required_keys_from_schema(self, compiled)

          # Normalize incoming attributes (Hash or kwargs) to a unified document
          document = Helpers.normalize_attrs_to_document(raw_attrs)

          id_val = document['id']
          if id_val.nil? || id_val.to_s.strip.empty?
            computed_id = Helpers.compute_id_for_create(self, raw_attrs)
            if computed_id.nil? || computed_id.to_s.strip.empty?
              raise SearchEngine::Errors::InvalidParams,
                    'Document id could not be resolved. Provide :id or a *_id matching the source model.'
            end
            document['id'] = computed_id
          end

          Helpers.update_doc_updated_at!(document)
          Helpers.append_hidden_flags!(self, document, allowed_keys)

          present = document.keys.map(&:to_s).to_set
          Helpers.validate_required_and_unknown!(self, present, allowed_keys, required_keys)
          Helpers.validate_and_coerce_types!(self, document, types_by_field, Helpers.coercions_enabled?)

          client = SearchEngine::Client.new
          logical = respond_to?(:collection) ? collection.to_s : name.to_s
          target = if into && !into.to_s.strip.empty?
                     into.to_s
                   else
                     client.resolve_alias(logical) || logical
                   end

          created = client.create_document(collection: target, document: document)
          Helpers.hydrate_from_document(self, created)
        end

        # Upsert a single document into the collection.
        #
        # Accepts either an unmapped source record (mapped via the configured DSL)
        # or pre-mapped data (as emitted by {.mapped_data_for}). The document is
        # normalized against the compiled schema before streaming via JSONL.
        #
        # @param record [Object, nil] source record to map
        # @param data [Hash, nil] pre-mapped document
        # @param into [String, nil] optional physical collection override
        # @param partition [Object, nil] partition token for resolvers
        # @return [Integer] number of successfully upserted documents (0 or 1)
        # @raise [SearchEngine::Errors::InvalidParams]
        def upsert(record: nil, data: nil, into: nil, partition: nil)
          docs = Helpers.prepare_documents(self, records: record ? [record] : nil, data: data)
          return 0 if docs.empty?

          result = Helpers.import_documents!(self, docs, into: into, partition: partition)
          result[:success_count]
        end

        # Upsert many documents into the collection in a single JSONL payload.
        #
        # Accepts either an enumerable of unmapped source records or an enumerable
        # of pre-mapped documents. Each entry is normalized using the same
        # validation path as {.create} to ensure schema compatibility prior to import.
        #
        # @param records [Enumerable<Object>, nil]
        # @param data [Enumerable<Hash>, nil]
        # @param into [String, nil]
        # @param partition [Object, nil]
        # @return [Hash] stats payload with keys: :collection, :docs_count, :success_count, :failure_count, :bytes_sent, :response
        # @raise [SearchEngine::Errors::InvalidParams]
        def upsert_bulk(records: nil, data: nil, into: nil, partition: nil)
          docs = Helpers.prepare_documents(self, records: records, data: data)
          Helpers.import_documents!(self, docs, into: into, partition: partition)
        end
      end
    end
  end
end
