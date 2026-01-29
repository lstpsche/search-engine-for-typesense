# frozen_string_literal: true

module SearchEngine
  # Compile partition-aware stale cleanup rules into Typesense filter strings.
  #
  # Evaluates entries declared via the indexing DSL `stale ...` and returns a
  # list of filter fragments or a merged OR expression suitable for delete-by
  # or Relation.where. Resilient to errors in individual entries.
  module StaleRules
    module_function

    # Check whether any stale configuration is defined for this model.
    #
    # @param klass [Class]
    # @return [Boolean]
    def defined_for?(klass)
      entries = begin
        klass.respond_to?(:stale_entries) ? Array(klass.stale_entries) : []
      rescue StandardError
        []
      end
      return true if entries.any?

      false
    rescue StandardError
      false
    end

    # Build an Array of Typesense filter fragments from stale rules.
    #
    # @param klass [Class]
    # @param partition [Object, nil]
    # @return [Array<String>]
    def compile_filters(klass, partition: nil)
      entries = begin
        klass.respond_to?(:stale_entries) ? Array(klass.stale_entries) : []
      rescue StandardError
        []
      end

      filters = []
      filters.concat(build_scope_filters(klass, entries, partition: partition))
      filters.concat(build_attribute_filters(klass, entries))
      filters.concat(build_hash_filters(klass, entries))
      filters.concat(build_raw_filters(klass, entries, partition: partition))

      filters.compact!
      filters.reject { |f| f.to_s.strip.empty? }
    rescue StandardError
      []
    end

    # Merge multiple filter fragments with OR semantics.
    #
    # @param filters [Array<String>]
    # @return [String, nil]
    def merge_filters(filters)
      list = Array(filters).compact.reject { |f| f.to_s.strip.empty? }
      return nil if list.empty?
      return list.first if list.size == 1

      list.map { |f| "(#{f})" }.join(' || ')
    end

    # --- helpers ------------------------------------------------------------

    def build_scope_filters(klass, entries, partition: nil)
      entries
        .select { |entry| entry[:type] == :scope }
        .map do |entry|
          scope = entry[:name]
          next unless klass.respond_to?(scope)

          rel = invoke_scope(klass, scope, partition)
          next unless defined?(SearchEngine::Relation) && rel.is_a?(SearchEngine::Relation)

          rel.filter_params
        end
        .compact
    rescue StandardError
      []
    end

    def build_attribute_filters(klass, entries)
      entries
        .select { |entry| entry[:type] == :attribute }
        .map do |entry|
          attr = entry[:name]
          val = entry[:value]
          relation_for(klass, { attr => val })&.filter_params
        end
        .compact
    rescue StandardError
      []
    end

    def build_hash_filters(klass, entries)
      entries
        .select { |entry| entry[:type] == :hash }
        .map { |entry| relation_for(klass, entry[:hash])&.filter_params }
        .compact
    rescue StandardError
      []
    end

    def build_raw_filters(klass, entries, partition: nil)
      raw = entries.select { |entry| %i[filter relation block].include?(entry[:type]) }
      Array(
        raw.flat_map do |entry|
          case entry[:type]
          when :filter then entry[:value]
          when :relation then entry[:relation]&.filter_params
          when :block then evaluate_block_entry(klass, entry[:block], partition: partition)
          end
        end
      ).compact
    rescue StandardError
      []
    end

    def relation_for(klass, hash)
      SearchEngine::Relation.new(klass).where(hash)
    end

    def evaluate_block_entry(klass, block, partition: nil)
      params = block.parameters
      result = if params.any? { |(kind, name)| %i[key keyreq].include?(kind) && name == :partition }
                 klass.instance_exec(partition: partition, &block)
               elsif block.arity.positive?
                 klass.instance_exec(partition, &block)
               else
                 klass.instance_exec(&block)
               end

      case result
      when String then result
      when Hash then relation_for(klass, result)&.filter_params
      when SearchEngine::Relation then result.filter_params
      end
    rescue StandardError
      nil
    end

    def invoke_scope(klass, scope, partition)
      method_obj = klass.method(scope)
      params = method_obj.parameters
      if params.empty?
        klass.public_send(scope)
      elsif params.any? { |(kind, name)| %i[key keyreq].include?(kind) && %i[partition _partition].include?(name) }
        klass.public_send(scope, partition: partition)
      elsif params.first && %i[req opt].include?(params.first.first)
        klass.public_send(scope, partition)
      else
        klass.public_send(scope)
      end
    rescue ArgumentError
      klass.public_send(scope)
    end
  end
end
