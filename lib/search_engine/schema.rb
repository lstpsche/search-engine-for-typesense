# frozen_string_literal: true

module SearchEngine
  # Schema utilities to compile model DSL into a Typesense-compatible schema
  # hash and to diff it against a live collection.
  #
  # Public API:
  # - {.compile(klass)} => Hash
  # - {.diff(klass, client: SearchEngine::Client.new)} => { diff: Hash, pretty: String }
  module Schema
    # Deterministic mapping from DSL types to Typesense field types.
    #
    # Policy:
    # - :integer -> "int64" (consistent; prefer wider range)
    # - :float/:decimal -> "float"
    # - :string -> "string"
    # - :boolean -> "bool"
    # - :time/:datetime -> "int64" (epoch seconds)
    # - :time_string/:datetime_string -> "string" (ISO8601 timestamps)
    # - Array types (e.g. [:string]) -> "string[]" (when present)
    TYPE_MAPPING = {
      string: 'string',
      integer: 'int64',
      float: 'float',
      decimal: 'float',
      boolean: 'bool',
      time: 'int64',
      datetime: 'int64',
      time_string: 'string',
      datetime_string: 'string'
    }.freeze

    FIELD_COMPARE_KEYS = %i[type reference async_reference locale sort optional infix].freeze

    class << self
      # Build a Typesense-compatible schema hash from a model class DSL.
      #
      # The output includes only keys that are supported and declared via the DSL.
      # Since attribute-level flags (facet/index/optional/sort) are not supported
      # by the current DSL, they are omitted to avoid noisy diffs.
      #
      # @param klass [Class] model class inheriting from {SearchEngine::Base}
      # @return [Hash] frozen schema hash with symbol keys
      # @raise [ArgumentError] if the class has no collection name defined
      # @note Automatically sets `enable_nested_fields: true` at collection level when
      #   any attribute is declared with type `:object` or `[:object]`.
      def compile(klass)
        collection_name = collection_name_for!(klass)

        fields_array, needs_nested_fields = compile_fields_for(klass)
        # Do NOT include implicit `id` in compiled schema: Typesense treats `id` as
        # a special string identifier and it is not declared in collection schema.
        # Keeping it out avoids confusing diffs and mismatches with live schema.
        coerce_doc_updated_at_type!(fields_array)

        schema = build_schema_hash(collection_name, fields_array, needs_nested_fields)
        deep_freeze(schema)
      end

      # No longer used: id is not included in compiled schema.
      def infer_id_field_type(_klass)
        'string'
      end

      # Diff the compiled schema for +klass+ against the live physical collection
      # in Typesense, resolving aliases when present. Returns both a structured
      # diff Hash and a compact human-readable summary string.
      #
      # @param klass [Class] model class inheriting from {SearchEngine::Base}
      # @param client [SearchEngine::Client] optional client wrapper (for tests)
      # @return [Hash] { diff: Hash, pretty: String }
      # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Schema-Indexer-E2E`
      # @see `https://typesense.org/docs/latest/api/collections.html`
      def diff(klass, client: SearchEngine::Client.new)
        compiled = compile(klass)
        logical_name = compiled[:name]

        physical_name = client.resolve_alias(logical_name) || logical_name
        live_schema = client.retrieve_collection_schema(physical_name)

        if live_schema.nil?
          diff_hash = {
            collection: { name: logical_name, physical: physical_name },
            added_fields: compiled[:fields].dup.first(2),
            removed_fields: [],
            changed_fields: {},
            collection_options: { live: :missing }
          }
          payload = {
            collection: klass.name.to_s,
            logical: logical_name,
            physical_current: nil,
            fields_changed_count: 0,
            added_count: diff_hash[:added_fields].size,
            removed_count: 0,
            in_sync: false
          }
          SearchEngine::Instrumentation.instrument('search_engine.schema.diff', payload) {}
          return { diff: diff_hash, pretty: pretty_print(diff_hash) }
        end

        normalized_compiled = normalize_schema(compiled)
        normalized_live = normalize_schema(live_schema)

        added, removed, changed = diff_fields(normalized_compiled[:fields], normalized_live[:fields])
        collection_opts_changes = diff_collection_options(normalized_compiled, normalized_live)

        diff_hash = {
          collection: { name: logical_name, physical: physical_name },
          added_fields: added,
          removed_fields: removed,
          changed_fields: changed,
          collection_options: collection_opts_changes
        }

        payload = {
          collection: klass.name.to_s,
          logical: logical_name,
          physical_current: physical_name,
          fields_changed_count: changed.size,
          added_count: added.size,
          removed_count: removed.size,
          in_sync: added.empty? && removed.empty? && changed.empty? && collection_opts_changes.empty?
        }
        SearchEngine::Instrumentation.instrument('search_engine.schema.diff', payload) {}

        { diff: diff_hash, pretty: pretty_print(diff_hash) }
      end

      # Apply schema lifecycle: create a new physical collection, reindex data into it,
      # atomically point the alias (logical name) to it, and enforce retention.
      #
      # The reindexing step can be provided via an optional block (yielded with the new
      # physical name). If no block is given, and the klass responds to
      # `reindex_all_to(physical_name)`, that method will be called. If neither is available,
      # an ArgumentError is raised and no alias swap occurs. If reindexing fails, the
      # newly created physical is left intact for inspection; retention cleanup only runs
      # after a successful alias swap.
      #
      # @param klass [Class] model class inheriting from {SearchEngine::Base}
      # @param client [SearchEngine::Client] optional client wrapper (for tests)
      # @yieldparam physical_name [String] the newly created physical collection name
      # @return [Hash] { logical: String, new_physical: String, previous_physical: String, alias_target: String, dropped_physicals: Array<String> }
      # @raise [SearchEngine::Errors::Api, ArgumentError]
      # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Schema#lifecycle`
      # @see `https://typesense.org/docs/latest/api/collections.html`
      def apply!(klass, client: SearchEngine::Client.new)
        compiled = compile(klass)
        logical = compiled[:name]

        start_ms = monotonic_ms
        current_target = client.resolve_alias(logical)

        new_physical = generate_physical_name(logical, client: client)
        create_schema = { name: new_physical, fields: compiled[:fields].map(&:dup) }
        create_schema[:enable_nested_fields] = true if compiled[:enable_nested_fields]
        client.create_collection(create_schema)

        if block_given?
          yield new_physical
        elsif klass.respond_to?(:reindex_all_to)
          klass.reindex_all_to(new_physical)
        else
          raise ArgumentError, 'reindex step is required: provide a block or implement klass.reindex_all_to(name)'
        end

        # Idempotent: if alias already points to new physical, treat as no-op
        current_after_reindex = client.resolve_alias(logical)
        swapped = current_after_reindex != new_physical
        client.upsert_alias(logical, new_physical) if swapped

        # Retention cleanup
        _, dropped = enforce_retention!(logical, new_physical, client: client, keep_last: effective_keep_last(klass))

        if defined?(ActiveSupport::Notifications)
          # Preserve legacy payload shape while adding canonical keys expected by the subscriber
          SearchEngine::Instrumentation.instrument('search_engine.schema.apply',
                                                   logical: logical,
                                                   new_physical: new_physical,
                                                   previous_physical: current_target,
                                                   dropped_count: dropped.size,
                                                   # canonical keys
                                                   collection: klass.name.to_s,
                                                   physical_new: new_physical,
                                                   alias_swapped: swapped,
                                                   retention_deleted_count: dropped.size,
                                                   status: :ok,
                                                   duration_ms: (monotonic_ms - start_ms)
                                                  ) {}
        end

        {
          logical: logical,
          new_physical: new_physical,
          previous_physical: current_target,
          alias_target: new_physical,
          dropped_physicals: dropped
        }
      end

      # Roll back the alias for the given klass to the previous retained physical collection.
      #
      # Chooses the most recent retained physical behind the current alias target. If none
      # is available, raises an ArgumentError explaining that retention may be set to 0.
      # The method is idempotent: if the alias already points to the chosen target, no-op.
      #
      # @param klass [Class]
      # @param client [SearchEngine::Client]
      # @return [Hash] { logical: String, new_target: String, previous_target: String }
      # @raise [ArgumentError] when no previous physical exists
      # @see `https://github.com/lstpsche/search-engine-for-typesense/wiki/Schema#retention`
      def rollback(klass, client: SearchEngine::Client.new)
        compiled = compile(klass)
        logical = compiled[:name]

        start_ms = monotonic_ms
        current_target = client.resolve_alias(logical)

        physicals = list_physicals(logical, client: client)
        ordered = order_physicals_desc(logical, physicals)
        previous = ordered.find { |name| name != current_target }
        if previous.nil?
          raise ArgumentError,
                'No previous physical available for rollback; retention keep_last may be 0'
        end

        # Idempotent swap
        client.upsert_alias(logical, previous) unless current_target == previous

        if defined?(ActiveSupport::Notifications)
          SearchEngine::Instrumentation.instrument('search_engine.schema.rollback',
                                                   logical: logical,
                                                   new_target: previous,
                                                   previous_target: current_target,
                                                   duration_ms: (monotonic_ms - start_ms)
                                                  ) {}
        end

        { logical: logical, new_target: previous, previous_target: current_target }
      end

      private

      # Generate a new physical name using UTC timestamp + 3-digit sequence.
      # Example: "products_20250131_235959_001"
      def generate_physical_name(logical, client:)
        now = Time.now.utc
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        prefix = "#{logical}_#{timestamp}_"

        existing = list_physicals_starting_with(prefix, client: client)
        used_sequences = existing.map { |name| name.split('_').last.to_i }

        seq = 1
        seq += 1 while used_sequences.include?(seq) && seq < 999
        format('%<prefix>s%<seq>03d', prefix: prefix, seq: seq)
      end

      # Return current alias target or nil.
      def current_alias_target(logical, client:)
        client.resolve_alias(logical)
      end

      # Atomically swap alias to the provided physical.
      def swap_alias!(logical, physical, client:)
        client.upsert_alias(logical, physical)
      end

      # Enumerate all physicals that match the naming pattern for the logical name.
      def list_physicals(logical, client:)
        meta_timeout = begin
          t = SearchEngine.config.timeout_ms.to_i
          t = 5_000 if t <= 0
          t < 10_000 ? 10_000 : t
        rescue StandardError
          10_000
        end
        collections = Array(client.list_collections(timeout_ms: meta_timeout))
        re = /^#{Regexp.escape(logical)}_\d{8}_\d{6}_\d{3}$/
        names = collections.map { |c| (c[:name] || c['name']).to_s }
        names.select { |n| re.match?(n) }
      rescue StandardError
        []
      end

      # Internal: list physicals that share the same timestamp prefix (for sequence calculation)
      def list_physicals_starting_with(prefix, client:)
        meta_timeout = begin
          t = SearchEngine.config.timeout_ms.to_i
          t = 5_000 if t <= 0
          t < 10_000 ? 10_000 : t
        rescue StandardError
          10_000
        end
        collections = Array(client.list_collections(timeout_ms: meta_timeout))
        names = collections.map { |c| (c[:name] || c['name']).to_s }
        names.select { |n| n.start_with?(prefix) }
      rescue StandardError
        []
      end

      def enforce_retention!(logical, new_target, client:, keep_last:)
        keep = Integer(keep_last || 0)
        keep = 0 if keep.negative?

        physicals = list_physicals(logical, client: client)
        ordered = order_physicals_desc(logical, physicals)
        candidates = ordered.reject { |name| name == new_target }
        to_keep = candidates.first(keep)
        to_drop = candidates.drop(keep)

        to_drop.each do |name|
          # Safety: best-effort delete; ignore 404
          client.delete_collection(name)
        end

        [to_keep, to_drop]
      end

      def order_physicals_desc(logical, names)
        names.sort_by { |n| [-extract_timestamp(logical, n).to_i, -extract_sequence(logical, n)] }
      end

      def extract_timestamp(logical, name)
        # name format logical_YYYYMMDD_HHMMSS_###
        base = name.delete_prefix("#{logical}_")
        parts = base.split('_')
        return 0 unless parts.size == 3

        (parts[0] + parts[1]).to_i
      end

      def extract_sequence(_logical, name)
        name.split('_').last.to_i
      end

      def effective_keep_last(klass)
        per = klass.respond_to?(:schema_retention) && klass.schema_retention ? klass.schema_retention[:keep_last] : nil
        return per unless per.nil?

        SearchEngine.config.schema.retention.keep_last
      end

      def monotonic_ms
        SearchEngine::Instrumentation.monotonic_ms
      end

      # Extract and validate collection name as a non-empty string.
      def collection_name_for!(klass)
        collection_name = klass.respond_to?(:collection) ? klass.collection : nil
        if collection_name.nil? || collection_name.to_s.strip.empty?
          raise ArgumentError, 'klass must define a collection name'
        end

        collection_name.to_s
      end

      # Compile attributes from the model DSL into a fields array and detect nested fields requirement.
      def compile_fields_for(klass)
        attributes_map = klass.respond_to?(:attributes) ? klass.attributes : {}
        attribute_options = klass.respond_to?(:attribute_options) ? (klass.attribute_options || {}) : {}
        references_by_local_key = build_references_by_local_key(klass)
        async_reference_by_local_key = build_async_reference_by_local_key(klass)

        fields_array = []
        needs_nested_fields = false

        attributes_map.each do |attribute_name, type_descriptor|
          validate_attribute_type!(attribute_name, type_descriptor)

          ts_type = typesense_type_for(type_descriptor)
          opts = attribute_options[attribute_name.to_sym] || {}
          needs_nested_fields ||= nested_type?(ts_type)

          fields_array << build_field_entry(
            attribute_name,
            ts_type,
            references_by_local_key,
            async_reference_by_local_key,
            opts
          )

          # Hidden flags:
          # - <name>_empty for array attributes with empty_filtering enabled
          # - <name>_blank for any attribute with optional enabled
          append_hidden_empty_field(fields_array, attribute_name, type_descriptor, opts)
        end

        [fields_array, needs_nested_fields]
      end

      # Validate unsupported or unsafe type descriptors early.
      def validate_attribute_type!(attribute_name, type_descriptor)
        return unless type_descriptor.to_s.downcase == 'auto'

        raise SearchEngine::Errors::InvalidOption,
              "Unsupported attribute type :auto for #{attribute_name}. Use a concrete type or :object/[:object]."
      end

      def nested_type?(ts_type)
        %w[object object[]].include?(ts_type)
      end

      def build_field_entry(attribute_name, ts_type, references_by_local_key, async_reference_by_local_key, opts)
        {
          name: attribute_name.to_s,
          type: ts_type,
          **{
            locale: opts[:locale],
            sort: opts[:sort],
            optional: opts[:optional],
            infix: opts[:infix],
            reference: references_by_local_key[attribute_name.to_sym],
            async_reference: async_reference_by_local_key[attribute_name.to_sym]
          }.compact
        }
      end

      # Ensure mandatory system field is present with enforced type when declared by developers.
      def coerce_doc_updated_at_type!(fields_array)
        fields_array.each do |f|
          fname = (f[:name] || f['name']).to_s
          next unless fname == 'doc_updated_at'

          if f.key?(:type)
            f[:type] = 'int64'
          elsif f.key?('type')
            f['type'] = 'int64'
          else
            f[:type] = 'int64'
          end
          break
        end
      end

      # Build the final schema hash and set collection-level nested fields when needed.
      def build_schema_hash(collection_name, fields_array, needs_nested_fields)
        schema = { name: collection_name.to_s, fields: fields_array }
        schema[:enable_nested_fields] = true if needs_nested_fields
        schema
      end

      def typesense_type_for(type_descriptor)
        # Array types (e.g., [:string]) => "string[]"; support nested symbol or string
        if type_descriptor.is_a?(Array) && type_descriptor.size == 1
          inner = type_descriptor.first
          mapped = TYPE_MAPPING[inner.to_s.downcase.to_sym] || inner.to_s
          return "#{mapped}[]"
        end

        TYPE_MAPPING[type_descriptor.to_s.downcase.to_sym] || type_descriptor.to_s
      end

      def normalize_schema(schema)
        # Accept either compiled or live schema; return shape with symbol keys
        name = (schema[:name] || schema['name']).to_s
        fields = Array(schema[:fields] || schema['fields'])

        normalized_fields = {}
        fields.each do |field|
          fname = (field[:name] || field['name']).to_s

          ftype = (field[:type] || field['type']).to_s
          fref = field[:reference] || field['reference']
          entry = { name: fname, type: normalize_type(ftype) }
          entry[:reference] = fref.to_s unless fref.nil? || fref.to_s.strip.empty?
          # Preserve attribute-level flags from either compiled or live schemas.
          %i[locale sort optional infix async_reference].each do |k|
            val = field[k] || field[k.to_s]
            entry[k] = val unless val.nil?
          end
          normalized_fields[fname] = entry
        end

        {
          name: name,
          fields: normalized_fields,
          default_sorting_field: schema[:default_sorting_field] || schema['default_sorting_field'],
          token_separators: schema[:token_separators] || schema['token_separators'],
          symbols_to_index: schema[:symbols_to_index] || schema['symbols_to_index'],
          enable_nested_fields: schema[:enable_nested_fields] || schema['enable_nested_fields']
        }
      end

      def normalize_type(type_string)
        s = type_string.to_s
        return 'string[]' if s.casecmp('string[]').zero?
        return 'int64' if s.casecmp('int64').zero?
        return 'int32' if s.casecmp('int32').zero?
        return 'float' if s.casecmp('float').zero?
        return 'bool' if %w[bool boolean].include?(s.downcase)
        return 'string' if s.casecmp('string').zero?

        # Fallback: return as-is
        s
      end

      def diff_fields(compiled_fields_by_name, live_fields_by_name)
        compiled_names = compiled_fields_by_name.keys
        live_names = live_fields_by_name.keys

        added_names = compiled_names - live_names
        removed_names = live_names - compiled_names
        shared_names = compiled_names & live_names

        added = added_names.map { |n| compiled_fields_by_name[n] }
        removed = removed_names.map { |n| live_fields_by_name[n] }

        changed = {}
        shared_names.each do |name|
          compiled_field = compiled_fields_by_name[name]
          live_field = live_fields_by_name[name]

          field_changes = {}
          FIELD_COMPARE_KEYS.each do |key|
            # Only compare attribute-level flags when declared in compiled schema.
            next unless key == :type || key == :reference || compiled_field.key?(key)

            cval = compiled_field[key]
            lval = live_field[key]
            next if values_equal?(cval, lval)

            field_changes[key.to_s] = [cval, lval]
          end

          changed[name] = field_changes unless field_changes.empty?
        end

        [added, removed, changed]
      end

      def diff_collection_options(compiled, live)
        # Compare only keys present in compiled to avoid noisy diffs when DSL
        # does not declare collection-level options.
        keys = %i[default_sorting_field token_separators symbols_to_index enable_nested_fields]
        differences = {}
        keys.each do |key|
          cval = compiled[key]
          next if cval.nil?

          lval = live[key]
          next if values_equal?(cval, lval)

          differences[key] = [cval, lval]
        end
        differences
      end

      def values_equal?(a, b)
        if a.is_a?(Array) && b.is_a?(Array)
          a == b
        else
          a.to_s == b.to_s
        end
      end

      def deep_freeze(object)
        case object
        when Hash
          object.each_value { |v| deep_freeze(v) }
        when Array
          object.each { |v| deep_freeze(v) }
        end
        object.freeze
      end

      def pretty_print(diff)
        lines = []
        lines << format_header(diff[:collection] || {})

        added = diff[:added_fields] || []
        removed = diff[:removed_fields] || []
        changed = diff[:changed_fields] || {}
        coll_opts = diff[:collection_options] || {}

        if added.empty? && removed.empty? && changed.empty? && (coll_opts.nil? || coll_opts.empty?)
          lines << 'No changes'
          return lines.join("\n")
        end

        lines.concat(format_added_fields(added)) unless added.empty?
        lines.concat(format_removed_fields(removed)) unless removed.empty?
        lines.concat(format_changed_fields(changed)) unless changed.empty?
        lines.concat(format_collection_options(coll_opts)) unless coll_opts.empty?

        lines.join("\n")
      end

      def format_header(collection)
        logical = collection[:name]
        physical = collection[:physical]
        if physical && physical != logical
          "Collection: #{logical} -> #{physical}"
        else
          "Collection: #{logical}"
        end
      end
      private :format_header

      def format_added_fields(list)
        lines = ['+ Added fields:']
        list.each do |f|
          lines << "  - #{f[:name]}:#{f[:type]}"
        end
        lines
      end
      private :format_added_fields

      def format_removed_fields(list)
        lines = ['- Removed fields:']
        list.each do |f|
          lines << "  - #{f[:name]}:#{f[:type]}"
        end
        lines
      end
      private :format_removed_fields

      def format_changed_fields(map)
        lines = ['~ Changed fields:']
        map.keys.sort.each do |fname|
          pairs = map[fname]
          pairs.each do |attr, (cval, lval)|
            lines << "  - #{fname}.#{attr}: #{cval} -> #{lval}"
          end
        end
        lines
      end
      private :format_changed_fields

      def format_collection_options(opts)
        lines = ['~ Collection options:']
        opts.each do |key, (cval, lval)|
          next if key == :live && cval.nil? && lval.nil?

          lines << if key == :live && (cval == :missing || lval == :missing)
                     "  - live: #{cval || lval}"
                   else
                     "  - #{key}: #{cval} -> #{lval}"
                   end
        end
        lines
      end
      private :format_collection_options

      # Build a mapping of local attribute names to referenced collection names based on join declarations.
      # @param klass [Class]
      # @return [Hash{Symbol=>String}]
      def build_references_by_local_key(klass)
        refs = {}
        return refs unless klass.respond_to?(:joins_config)

        (klass.joins_config || {}).each_value do |cfg|
          # Only belongs_to/belongs_to_many contribute references to schema
          kind = (cfg[:kind] || :belongs_to).to_sym
          next if %i[has_one has_many].include?(kind)

          lk = cfg[:local_key]
          coll = cfg[:collection]
          fk = cfg[:foreign_key]
          next if lk.nil?

          coll_name = coll.to_s
          fk_name = fk.to_s
          next if coll_name.strip.empty? || fk_name.strip.empty?

          key = lk.to_sym
          refs[key] ||= "#{coll_name}.#{fk_name}"
        end
        refs
      end

      # Build a mapping of local attribute names to async_reference flag based on belongs_to declarations.
      # @param klass [Class]
      # @return [Hash{Symbol=>Boolean}]
      def build_async_reference_by_local_key(klass)
        out = {}
        return out unless klass.respond_to?(:joins_config)

        (klass.joins_config || {}).each_value do |cfg|
          kind = (cfg[:kind] || :belongs_to).to_sym
          next if %i[has_one has_many].include?(kind)

          lk = cfg[:local_key]
          async = cfg[:async_ref]
          next if lk.nil?

          out[lk.to_sym] = true if async
        end
        out
      end

      # Append hidden flags based on attribute options:
      # - <name>_empty for array attributes with empty_filtering enabled
      # - <name>_blank for any attribute with optional enabled
      def append_hidden_empty_field(fields_array, attribute_name, type_descriptor, opts)
        add_empty = opts[:empty_filtering] && type_descriptor.is_a?(Array) && type_descriptor.size == 1
        add_blank = opts[:optional]

        return unless add_empty || add_blank

        fields_array << { name: "#{attribute_name}_empty", type: 'bool' } if add_empty
        fields_array << { name: "#{attribute_name}_blank", type: 'bool' } if add_blank
      end
    end
  end
end
