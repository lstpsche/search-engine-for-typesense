# frozen_string_literal: true

require 'set'

module SearchEngine
  # Mapper compiles a per-collection mapping function and validates
  # mapped documents against the compiled schema.
  #
  # Public API:
  # - {SearchEngine::Mapper.for(klass)} -> compiled mapper or nil when undefined
  # - {SearchEngine::Mapper::Compiled#map_batch!(rows, batch_index:)} -> [Array<Hash>, Hash]
  module Mapper
    # Simple DSL holder used by Base#index to capture source and map block.
    #
    # Describes where data is fetched from and how records are transformed into
    # Typesense documents. Compiled by {SearchEngine::Mapper.for}.
    #
    # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer
    class Dsl
      # @return [Hash, nil] original source definition captured from DSL
      attr_reader :source_def
      # @return [Proc, nil] mapping proc captured from DSL
      attr_reader :map_proc

      def initialize(klass)
        @klass = klass
        @source_def = nil
        @map_proc = nil
        @partitions_proc = nil
        @partition_fetch_proc = nil
        @before_partition_proc = nil
        @after_partition_proc = nil
        @max_parallel = 1
        @stale_entries = []
      end

      # Declare a source adapter for this collection. Compatible with
      # SearchEngine::Sources.build signature. Stored for compatibility; the
      # mapper only requires the `map`.
      # @param type [Symbol]
      # @param options [Hash]
      # @yield for :lambda sources
      # @return [void]
      # @raise [ArgumentError] when type is nil/blank
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer
      def source(type, **options, &block)
        @source_def = { type: type.to_sym, options: options, block: block }
        nil
      end

      # Define the mapping block.
      # @yield [record] yields each source record to the block
      # @yieldparam record [Object]
      # @yieldreturn [Hash, #to_h, #as_json] a document-like object
      # @return [void]
      # @raise [ArgumentError] when no block is given
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer
      def map(&block)
        raise ArgumentError, 'map requires a block' unless block

        @map_proc = block
        nil
      end

      # Delete documents by filter before/after a partition import or ad-hoc.
      # Accepts either a raw Typesense filter string or a hash which will be
      # converted to a filter string using the Filters::Sanitizer.
      #
      # Examples:
      #   delete_by filter_by: "store_id:=#{store_id}"
      #   delete_by store_id: store_id
      #
      # @param filter_or_str [String, nil]
      # @param filter_by [String, nil]
      # @param into [String, nil]
      # @param partition [Object, nil]
      # @param timeout_ms [Integer, nil]
      # @param hash [Hash] remaining keyword arguments treated as filter hash
      # @return [Integer] number of deleted documents
      def delete_by(filter_or_str = nil, into: nil, partition: nil, timeout_ms: nil, filter_by: nil, **hash)
        effective_partition = partition || Thread.current[:__se_current_partition__] ||
                              instance_variable_get(:@__current_partition__)
        SearchEngine::Deletion.delete_by(
          klass: @klass,
          filter: filter_or_str || filter_by,
          hash: (hash.empty? ? nil : hash),
          into: into,
          partition: effective_partition,
          timeout_ms: timeout_ms
        )
      end

      # Register a stale-cleanup rule evaluated during {SearchEngine::Base.cleanup}.
      #
      # Accepts one of the following forms:
      # - `stale { where(active: false) }` — block evaluated against the model class
      # - `stale scope: :inactive` — named scope invoked on the model
      # - `stale :archived` or `stale attribute: :archived, value: true` — attribute equality
      # - `stale(filter: 'status:=archived')` — raw Typesense filter fragment
      # - `stale(product_state: 'archived')` — Hash converted to Relation filters
      #
      # Multiple stale entries are OR-ed together when cleanup runs.
      # @param target [Symbol, String, Hash, SearchEngine::Relation, nil]
      # @param scope [Symbol, nil]
      # @param attribute [Symbol, nil]
      # @param value [Object] value used with attribute form (defaults to +true+)
      # @param filter [String, nil]
      # @yield block evaluated against the model class; should return a Relation, String, or Hash
      # @return [void]
      def stale(target = nil, scope: nil, attribute: nil, value: true, filter: nil, &block)
        entry = build_stale_entry(
          target,
          scope: scope,
          attribute: attribute,
          value: value,
          filter: filter,
          block: block
        )
        @stale_entries << entry.freeze
        nil
      end

      # @return [Array<Hash>]
      def stale_entries
        @stale_entries.dup
      end

      # Partitioning: declare how to enumerate partitions for full rebuilds.
      # @yieldreturn [Enumerable] a list/Enumerable of opaque partition keys
      # @return [void]
      # @raise [ArgumentError] when no block is given
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer#partitioning
      def partitions(&block)
        raise ArgumentError, 'partitions requires a block' unless block

        @partitions_proc = block
        nil
      end

      # Configure maximum parallel threads for partitioned indexation.
      # Applies only when partitioning is used and only to full indexation.
      # @param max_parallel [Integer]
      # @return [void]
      # @raise [SearchEngine::Errors::InvalidOption] when n is not a positive Integer
      def partition_max_parallel(max_parallel)
        unless max_parallel.is_a?(Integer) && max_parallel.positive?
          raise SearchEngine::Errors::InvalidOption,
                'partition_max_parallel must be a positive Integer (> 0)'
        end

        @partition_max_parallel = max_parallel
        nil
      end

      # Configure maximum parallel threads for batch processing within a partition (or non-partitioned import).
      # Applies to batch-level parallelism, independent of partition-level parallelism.
      # @param max_parallel [Integer]
      # @return [void]
      # @raise [SearchEngine::Errors::InvalidOption] when n is not a positive Integer
      def max_parallel(max_parallel)
        unless max_parallel.is_a?(Integer) && max_parallel.positive?
          raise SearchEngine::Errors::InvalidOption,
                'max_parallel must be a positive Integer (> 0)'
        end

        @max_parallel = max_parallel
        nil
      end

      # Partitioning: provide a per-partition batch enumerator.
      # The block receives the partition key and must return an Enumerable of batches (Arrays of records).
      # @yieldparam partition [Object]
      # @yieldreturn [Enumerable<Array>] yields Arrays of records per batch
      # @return [void]
      # @raise [ArgumentError] when no block is given
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer#partitioning
      def partition_fetch(&block)
        raise ArgumentError, 'partition_fetch requires a block' unless block

        @partition_fetch_proc = block
        nil
      end

      # Hook executed before importing a partition.
      # The block receives the partition key.
      # @yieldparam partition [Object]
      # @return [void]
      # @raise [ArgumentError] when no block is given
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer#partitioning
      def before_partition(&block)
        raise ArgumentError, 'before_partition requires a block' unless block

        # Wrap to expose current partition on the DSL instance for helpers
        @before_partition_proc = lambda do |partition|
          instance_variable_set(:@__current_partition__, partition)
          prev = Thread.current[:__se_current_partition__]
          Thread.current[:__se_current_partition__] = partition
          if block.arity == 1
            yield(partition)
          else
            instance_exec(partition, &block)
          end
        ensure
          Thread.current[:__se_current_partition__] = prev
          remove_instance_variable(:@__current_partition__) if instance_variable_defined?(:@__current_partition__)
        end
        nil
      end

      # Hook executed after importing a partition.
      # The block receives the partition key.
      # @yieldparam partition [Object]
      # @return [void]
      # @raise [ArgumentError] when no block is given
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer#partitioning
      def after_partition(&block)
        raise ArgumentError, 'after_partition requires a block' unless block

        @after_partition_proc = lambda do |partition|
          instance_variable_set(:@__current_partition__, partition)
          prev = Thread.current[:__se_current_partition__]
          Thread.current[:__se_current_partition__] = partition
          if block.arity == 1
            yield(partition)
          else
            instance_exec(partition, &block)
          end
        ensure
          Thread.current[:__se_current_partition__] = prev
          remove_instance_variable(:@__current_partition__) if instance_variable_defined?(:@__current_partition__)
        end
        nil
      end

      # Freeze internal state for immutability and return a definition Hash.
      # @return [Hash]
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer
      def to_definition
        {
          source: @source_def,
          map: @map_proc,
          partitions: @partitions_proc,
          partition_fetch: @partition_fetch_proc,
          before_partition: @before_partition_proc,
          after_partition: @after_partition_proc,
          partition_max_parallel: @partition_max_parallel,
          max_parallel: @max_parallel,
          stale: @stale_entries.dup.freeze
        }.freeze
      end

      private

      def build_stale_entry(target, scope:, attribute:, value:, filter:, block:)
        if block
          { type: :block, block: block }
        elsif scope
          ensure_symbol!(:scope, scope)
          { type: :scope, name: scope.to_sym }
        elsif attribute || attribute_target?(target)
          attr_name = (attribute || target).to_sym
          { type: :attribute, name: attr_name, value: value }
        elsif hash_target?(target)
          normalized_hash = normalize_stale_hash(target)
          { type: :hash, hash: normalized_hash }
        elsif filter || filter_target?(target)
          str = filter || target
          ensure_string!(:filter, str)
          { type: :filter, value: str.to_s }
        elsif relation_target?(target)
          { type: :relation, relation: target }
        else
          raise ArgumentError,
                'stale requires a block, scope:, attribute:, filter:, relation, or hash input'
        end
      end

      def attribute_target?(target)
        target.is_a?(Symbol)
      end

      def hash_target?(target)
        target.is_a?(Hash)
      end

      def relation_target?(target)
        defined?(SearchEngine::Relation) && target.is_a?(SearchEngine::Relation)
      end

      def filter_target?(target)
        target.is_a?(String)
      end

      def normalize_stale_hash(hash)
        hash.each_with_object({}) do |(key, value), acc|
          sym_key = key.respond_to?(:to_sym) ? key.to_sym : key
          acc[sym_key] = value
        end.freeze
      end

      def ensure_symbol!(name, value)
        return if value.is_a?(Symbol)

        raise ArgumentError, "#{name} must be a Symbol"
      end

      def ensure_string!(name, value)
        return if value.respond_to?(:to_s)

        raise ArgumentError, "#{name} must be convertible to String"
      end
    end

    # Immutable compiled mapper for a specific collection class.
    #
    # Validates mapped documents against the compiled schema, sets hidden flags
    # for array/optional fields and emits instrumentation.
    #
    # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer
    class Compiled
      attr_reader :klass

      def initialize(klass:, map_proc:, schema_fields:, types_by_field:, options: {})
        @klass = klass
        @map_proc = map_proc
        @schema_fields = schema_fields.freeze # Array of field names (String)
        @types_by_field = types_by_field.freeze # { "field" => "int64" }
        # Allow all schema fields; treat required as schema fields minus optional attributes
        @allowed_keys = @schema_fields.map(&:to_sym).to_set.freeze
        @required_keys = compute_required_keys
        @options = default_options.merge(options || {})
        @__empty_filtering_targets__ = compute_empty_filtering_targets
        @__optional_blank_targets__ = compute_optional_blank_targets
        freeze
      end

      # Map and validate a batch of rows.
      # @param rows [Array<Object>] source records to map
      # @param batch_index [Integer, nil] optional index for instrumentation
      # @return [Array<Array<Hash>, Hash>] [documents, report]
      # @raise [SearchEngine::Errors::InvalidParams] on missing required fields or invalid document shape
      # @raise [SearchEngine::Errors::InvalidField] when strict_unknown_keys is enabled and extras are present
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer#troubleshooting
      def map_batch!(rows, batch_index: nil)
        start_ms = monotonic_ms
        docs = []
        stats = init_stats

        rows.each do |row|
          hash = normalize_document(@map_proc.call(row))
          # Ignore any provided id from map; always inject computed document id
          hash.delete(:id)
          hash.delete('id')
          begin
            computed_id = @klass.compute_document_id(row)
          rescue NoMethodError
            # Fallback for older compiled mappers if needed; derive from record.id
            rid = row.respond_to?(:id) ? row.id : nil
            computed_id = rid.is_a?(String) ? rid : rid.to_s
          end
          hash[:id] = computed_id
          # Force system timestamp field on every document; developers cannot override.
          now_i = if defined?(Time) && defined?(Time.zone) && Time.zone
                    Time.zone.now.to_i
                  else
                    Time.now.to_i
                  end
          # Overwrite any provided value
          hash[:doc_updated_at] = now_i

          normalize_optional_blank_strings!(hash)

          # Populate hidden flags
          set_hidden_empty_flags!(hash)
          set_hidden_blank_flags!(hash)

          update_stats_for_doc!(stats, hash)
          validate_and_coerce_types!(stats, hash)
          docs << hash
        end

        ensure_required_present!(stats)
        ensure_no_unknowns!(stats)

        duration = monotonic_ms - start_ms
        instrument_batch_mapped(
          batch_index: batch_index,
          docs_count: docs.size,
          duration_ms: duration,
          missing_required_count: stats[:missing_required].size,
          extra_keys_count: stats[:extras_samples].size,
          invalid_type_count: stats[:invalid_type_samples].size,
          coerced_count: stats[:coerced_count]
        )

        report = build_report(stats, docs.size, batch_index, duration)
        [docs, report]
      end

      private

      def default_options
        {
          strict_unknown_keys: false,
          coercions_enabled: false,
          coercion_rules: {},
          max_error_samples: 5
        }
      end

      def init_stats
        {
          missing_required: [],
          extras_samples: [],
          invalid_type_samples: [],
          coerced_count: 0,
          total_keys: 0,
          nil_id: 0
        }
      end

      def update_stats_for_doc!(stats, hash)
        stats[:total_keys] += hash.size

        id_has_key = hash.key?(:id) || hash.key?('id')
        id_value = hash[:id] || hash['id']
        stats[:nil_id] += 1 if id_has_key && id_value.nil?

        present_keys = hash.keys.map(&:to_sym)
        missing = @required_keys - present_keys
        stats[:missing_required] |= missing.to_a unless missing.empty?

        extras = present_keys.to_set - @allowed_keys
        stats[:extras_samples] |= extras.to_a unless extras.empty?
      end

      def validate_and_coerce_types!(stats, hash)
        hash.each do |key, value|
          fname = key.to_s
          expected = @types_by_field[fname]
          next unless expected

          # Allow nil for attributes declared as optional in the model DSL
          next if value.nil? && @__optional_blank_targets__.include?(fname)

          valid, coerced, err = validate_value(expected, value, field: fname)
          if coerced
            stats[:coerced_count] += 1
            hash[key] = coerced
          elsif !valid && stats[:invalid_type_samples].size < @options[:max_error_samples]
            stats[:invalid_type_samples] << err
          end
        end
      end

      # Compute and set hidden *_empty flags based on configured array fields.
      # Adds the hidden flag only when the field is present in the schema (allowed_keys).
      def set_hidden_empty_flags!(doc)
        return if @__empty_filtering_targets__.empty?

        @__empty_filtering_targets__.each do |base_name|
          value = doc[base_name.to_sym]
          value = doc[base_name.to_s] if value.nil?
          flag_name = "#{base_name}_empty"
          doc[flag_name.to_sym] = value.nil? || (value.is_a?(Array) && value.empty?)
        end
      end

      # Normalize empty-string values for optional fields to nil.
      def normalize_optional_blank_strings!(doc)
        return if @__optional_blank_targets__.empty?

        @__optional_blank_targets__.each do |base_name|
          key_sym = base_name.to_sym
          if doc.key?(key_sym)
            value = doc[key_sym]
            doc[key_sym] = nil if value.is_a?(String) && value.empty?
            next
          end

          next unless doc.key?(base_name)

          value = doc[base_name]
          doc[base_name] = nil if value.is_a?(String) && value.empty?
        end
      end

      # Compute and set hidden *_blank flags based on optional fields.
      # Adds the hidden flag only when the field is present in the schema (allowed_keys).
      def set_hidden_blank_flags!(doc)
        return if @__optional_blank_targets__.empty?

        @__optional_blank_targets__.each do |base_name|
          value = doc[base_name.to_sym]
          value = doc[base_name.to_s] if value.nil?
          flag_name = "#{base_name}_blank"
          doc[flag_name.to_sym] = value.nil?
        end
      end

      def ensure_required_present!(stats)
        return if stats[:missing_required].empty?

        message = "Missing required fields: #{stats[:missing_required].sort.inspect} for #{klass.name} mapper."
        instrument_error(error_class: 'SearchEngine::Errors::InvalidParams', message: message)
        raise SearchEngine::Errors::InvalidParams.new(
          message,
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer#troubleshooting',
          details: { missing_required: stats[:missing_required].sort }
        )
      end

      def ensure_no_unknowns!(stats)
        return unless @options[:strict_unknown_keys] && !stats[:extras_samples].empty?

        message = [
          'Unknown fields detected:',
          "#{stats[:extras_samples].sort.inspect} (set mapper.strict_unknown_keys)."
        ].join(' ')
        instrument_error(error_class: 'SearchEngine::Errors::InvalidField', message: message)
        raise SearchEngine::Errors::InvalidField.new(
          message,
          doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/indexer#troubleshooting',
          details: { extras: stats[:extras_samples].sort }
        )
      end

      def build_report(stats, docs_size, batch_index, duration)
        {
          collection: klass.respond_to?(:collection) ? klass.collection : nil,
          batch_index: batch_index,
          docs_count: docs_size,
          missing_required: stats[:missing_required].sort,
          extras_sample: stats[:extras_samples].sort[0, @options[:max_error_samples]],
          invalid_type_sample: stats[:invalid_type_samples][0, @options[:max_error_samples]],
          coerced_count: stats[:coerced_count],
          total_keys: stats[:total_keys],
          nil_id: stats[:nil_id],
          duration_ms: duration.round(1)
        }
      end

      # Compute required keys as all schema fields minus attributes marked optional in the model DSL.
      # Hidden flags like <name>_blank remain required; they are populated automatically by the mapper.
      #
      # Nested fields (dotted names like "retail_prices.current_price") are excluded from
      # presence checks by default, since nested object/object[] subfields may be sparsely
      # populated. Typesense accepts documents missing nested subfields.
      def compute_required_keys
        begin
          opts = @klass.respond_to?(:attribute_options) ? (@klass.attribute_options || {}) : {}
        rescue StandardError
          opts = {}
        end

        # Start with all schema fields and drop dotted nested field names from required set
        base_fields = @schema_fields.map(&:to_sym)
        base_fields.reject! { |fname| fname.to_s.include?('.') }
        required = base_fields.to_set
        opts.each do |fname, o|
          next unless o.is_a?(Hash) && o[:optional]

          required.delete(fname.to_sym)
        end
        required.freeze
      end

      def normalize_document(obj)
        return obj if obj.is_a?(Hash)
        return obj.to_h if obj.respond_to?(:to_h)
        return obj.as_json if obj.respond_to?(:as_json)

        raise SearchEngine::Errors::InvalidParams,
              'Mapper map block must return a Hash-like document (Hash/#to_h/#as_json)'
      end

      def validate_value(expected_type, value, field:)
        # Returns [valid(Boolean), coerced_value_or_nil, error_message]
        case expected_type
        when 'int64', 'int32'
          # Accept Time universally by coercing to epoch seconds
          return [true, value.to_i, true] if value.is_a?(Time)

          validate_integer(value, field)
        when 'float'
          validate_float(value, field)
        when 'bool'
          validate_bool(value, field)
        when 'string'
          # Accept Time/Date/DateTime universally by coercing to ISO8601
          if value.is_a?(Time)
            return [true, value.iso8601, true]
          elsif defined?(DateTime) && value.is_a?(DateTime)
            return [true, value.to_time.utc.iso8601, true]
          elsif defined?(Date) && value.is_a?(Date)
            return [true, value.to_time.utc.iso8601, true]
          end

          [value.is_a?(String), nil, invalid_type_message(field, 'String', value)]
        when 'string[]'
          return [true, nil, nil] if value.is_a?(Array) && value.all? { |v| v.is_a?(String) }

          [false, nil, invalid_type_message(field, 'Array<String>', value)]
        else
          # Unknown/opaque type: accept
          [true, nil, nil]
        end
      end

      def validate_integer(value, field)
        if value.is_a?(Integer)
          [true, nil, nil]
        elsif @options[:coercions_enabled] && string_integer?(value)
          [true, Integer(value), true]
        else
          [false, nil, invalid_type_message(field, 'Integer', value)]
        end
      end

      def validate_float(value, field)
        if value.is_a?(Numeric) && finite_number?(value)
          [true, nil, nil]
        elsif @options[:coercions_enabled] && string_float?(value)
          f = begin
            Float(value)
          rescue StandardError
            nil
          end
          f && finite_number?(f) ? [true, f, true] : [false, nil, invalid_type_message(field, 'Float', value)]
        else
          [false, nil, invalid_type_message(field, 'Float', value)]
        end
      end

      def validate_bool(value, field)
        if [true, false].include?(value)
          [true, nil, nil]
        elsif @options[:coercions_enabled] && %w[true false 1 0].include?(value.to_s.downcase)
          [true, %w[true 1].include?(value.to_s.downcase), true]
        else
          [false, nil, invalid_type_message(field, 'Boolean', value)]
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

      def invalid_type_message(field, expected, got)
        got_class = got.nil? ? 'NilClass' : got.class.name
        got_preview = got.is_a?(String) ? got[0, 50] : got.to_s[0, 50]
        "Invalid type for field :#{field} (expected #{expected}, got #{got_class}: \"#{got_preview}\")."
      end

      # Determine which declared array attributes have empty_filtering enabled.
      # Returns an Array of base field names as Strings.
      def compute_empty_filtering_targets
        begin
          opts = @klass.respond_to?(:attribute_options) ? (@klass.attribute_options || {}) : {}
        rescue StandardError
          opts = {}
        end
        targets = []
        opts.each do |fname, o|
          next unless o.is_a?(Hash) && o[:empty_filtering]

          hidden = "#{fname}_empty"
          targets << fname.to_s if @types_by_field.key?(hidden) || @required_keys.include?(hidden.to_sym)
        end
        targets.freeze
      end

      # Determine which declared attributes have optional enabled.
      # Returns an Array of base field names as Strings, only when corresponding _blank is present in schema.
      def compute_optional_blank_targets
        begin
          opts = @klass.respond_to?(:attribute_options) ? (@klass.attribute_options || {}) : {}
        rescue StandardError
          opts = {}
        end
        targets = []
        opts.each do |fname, o|
          next unless o.is_a?(Hash) && o[:optional]

          hidden = "#{fname}_blank"
          targets << fname.to_s if @types_by_field.key?(hidden) || @required_keys.include?(hidden.to_sym)
        end
        targets.freeze
      end

      def instrument_batch_mapped(batch_index:, docs_count:, duration_ms:,
                                  missing_required_count:, extra_keys_count:,
                                  invalid_type_count:, coerced_count:)
        return unless defined?(ActiveSupport::Notifications)

        payload = {
          collection: klass.respond_to?(:collection) ? klass.collection : nil,
          batch_index: batch_index,
          docs_count: docs_count,
          duration_ms: duration_ms.round(1),
          missing_required_count: missing_required_count,
          extra_keys_count: extra_keys_count,
          invalid_type_count: invalid_type_count,
          coerced_count: coerced_count
        }
        SearchEngine::Instrumentation.instrument('search_engine.mapper.batch_mapped', payload) {}
      end

      def instrument_error(error_class:, message:)
        return unless defined?(ActiveSupport::Notifications)

        payload = {
          collection: klass.respond_to?(:collection) ? klass.collection : nil,
          error_class: error_class,
          message: message.to_s[0, 200]
        }
        SearchEngine::Instrumentation.instrument('search_engine.mapper.error', payload) {}
      end

      def monotonic_ms
        SearchEngine::Instrumentation.monotonic_ms
      end
    end

    class << self
      # Resolve a compiled mapper for a model class, or nil if no DSL is defined.
      # @param klass [Class]
      # @return [SearchEngine::Mapper::Compiled, nil]
      def for(klass)
        dsl = mapper_dsl_for(klass)
        return nil unless dsl && dsl[:map].respond_to?(:call)

        cache[klass] ||= compile(klass, dsl)
      end

      private

      def cache
        @cache ||= {}
      end

      def compile(klass, dsl)
        compiled_schema = SearchEngine::Schema.compile(klass)
        fields = Array(compiled_schema[:fields]).map { |f| f[:name].to_s }
        types_by_field = {}
        Array(compiled_schema[:fields]).each do |f|
          types_by_field[f[:name].to_s] = f[:type].to_s
        end

        mapper_cfg = SearchEngine.config&.mapper
        coercions_cfg = mapper_cfg&.coercions || {}
        options = {
          strict_unknown_keys: mapper_cfg&.strict_unknown_keys ? true : false,
          coercions_enabled: coercions_cfg[:enabled] ? true : false,
          coercion_rules: coercions_cfg[:rules].is_a?(Hash) ? coercions_cfg[:rules] : {},
          max_error_samples: (mapper_cfg&.max_error_samples.to_i.positive? ? mapper_cfg.max_error_samples.to_i : 5)
        }

        Compiled.new(
          klass: klass,
          map_proc: dsl[:map],
          schema_fields: fields,
          types_by_field: types_by_field,
          options: options
        )
      end

      def mapper_dsl_for(klass)
        return unless klass.instance_variable_defined?(:@__mapper_dsl__)

        klass.instance_variable_get(:@__mapper_dsl__)
      end
    end
  end
end
