# frozen_string_literal: true

require 'json'
require 'search_engine'
require 'search_engine/collection_resolver'

module SearchEngine
  module CLI
    # Minimal menu-driven Terminal UI for Typesense management.
    #
    # Public API: SearchEngine::CLI::Tuit.run(argv = []) -> exit code
    #
    # Dependencies: tty-prompt, tty-table (declared in gemspec). We load them lazily
    # with friendly errors when missing.
    module Tuit
      module_function

      # Entry point
      # @param argv [Array<String>]
      # @return [Integer]
      def run(argv = [])
        opts = parse_argv(argv)
        prompt = require_prompt!
        table = require_table!

        rails_boot_if_requested!(opts)
        ensure_models_loaded!
        ts_client = resolve_client

        loop do
          main_choices = [
            ['Collections', :collections],
            ['API Keys', :api_keys],
            ['Settings', :settings],
            ['Quit', :quit]
          ]
          choice = safe_select(prompt, 'TUIT — Typesense UI', main_choices, cycle: true, force_fallback: opts[:no_tty])

          case choice
          when :collections
            show_collections(prompt, table, ts_client)
          when :api_keys
            show_api_keys(prompt, table, ts_client)
          when :settings
            show_settings(prompt)
          when :quit
            break
          end
        end

        0
      rescue Interrupt
        130
      rescue StandardError => error
        warn("tuit error: #{error.class}: #{error.message}")
        begin
          dbg = ENV['TUIT_DEBUG']
          if dbg && !dbg.to_s.strip.empty?
            if error.respond_to?(:full_message)
              warn(error.full_message(highlight: false))
            else
              Array(error.backtrace).each { |ln| warn(ln) }
            end
          end
        rescue StandardError
          # never raise from error reporting
        end
        1
      end

      # --- Screens -----------------------------------------------------------

      def show_collections(prompt, table, client)
        return unless ensure_configured?(prompt, 'list collections')

        loop do
          index = build_collections_index(client)

          rows = index[:rows]
          header = ['Name', 'Num Documents']
          print_table(table, header: header, rows: rows)

          break if index[:choices].empty?

          pairs = index[:choices] + [['Back', :back]]
          selected = safe_select(prompt, 'Select a collection', pairs, cycle: true)
          break if selected == :back

          case selected[:type]
          when :resolved
            show_collection_actions(prompt, client, selected[:logical])
          when :physical
            show_collection_actions(prompt, client, selected[:physical])
          end
          # After returning from actions, loop back to refresh Collections list
        end
      end

      # Build grouped view of collections from physical -> logical and resolve models.
      # Returns a Hash with :rows for printing and :choices for selection.
      # rubocop:disable Metrics/AbcSize
      def build_collections_index(client)
        list = Array(client.list_collections)
        phys_items = list.map do |h|
          name = (h[:name] || h['name']).to_s
          num = (h[:num_documents] || h['num_documents']).to_i
          [name, num]
        end

        # 1) Build a deterministic logical => physicals map (no alias special-casing here)
        groups = Hash.new { |h, k| h[k] = { physicals: [], total: 0, klass: nil } }
        phys_items.each do |(physical, num)|
          logical = SearchEngine::CollectionResolver.logical_from_physical(physical)
          entry = groups[logical]
          entry[:physicals] << [physical, num]
          entry[:total] += num.to_i
        end

        # 2) Resolve klass per logical (best-effort) once per group
        groups.each do |logical, info|
          info[:klass] = SearchEngine::CollectionResolver.model_for_logical(logical)
        end

        # Resolve class per logical (best-effort): try logical first, then per-physical alias-aware
        # rubocop:disable Style/CombinableLoops
        groups.each do |logical, info|
          klass = SearchEngine::CollectionResolver.model_for_logical(logical)
          if klass.nil?
            info[:physicals].each do |(physical, _num)|
              klass = SearchEngine::CollectionResolver.model_for_physical(physical, client: client)
              break if klass
            end
          end
          info[:klass] = klass
        end
        # rubocop:enable Style/CombinableLoops

        rows = []
        choices = []

        groups.each do |logical, info|
          klass = info[:klass]
          physicals = info[:physicals]
          total = info[:total]

          if klass
            logical_label = klass.respond_to?(:collection) ? klass.collection.to_s : logical.to_s
            label = "#{klass.name} (#{logical_label})"
            rows << [label, total]
            choices << [label, { type: :resolved, logical: logical_label, klass: klass, physicals: physicals }]
          else
            # No model: list each physical separately
            physicals.each do |(physical, num)|
              rows << [physical, num]
              choices << [physical, { type: :physical, physical: physical, num: num }]
            end
          end
        end

        { rows: rows, choices: choices }
      end
      # rubocop:enable Metrics/AbcSize

      def show_collection_actions(prompt, client, logical_name)
        # New conditional actions per spec.
        klass = SearchEngine::CollectionResolver.model_for_logical(logical_name)

        if klass
          show_resolved_collection_actions(prompt, client, klass, logical_name)
          return
        end

        # Unresolved logical (or physical passed through): open physical menu directly.
        physical = client.resolve_alias(logical_name) || logical_name
        show_physical_collection_actions(prompt, client, physical, after_drop: :return)
      end

      # Actions for a resolved model collection.
      def show_resolved_collection_actions(prompt, client, klass, logical)
        loop do
          actions = []
          # Physical collections submenu is always available for resolved collections
          actions << ['Physical collections', :physical_collections]

          # Drop collection
          actions << ['Drop collection', :drop_collection]

          # Conditional reindex options based on partitioning
          compiled = SearchEngine::Partitioner.for(klass)
          if compiled
            actions << ['(Re)Index full collection', :reindex_full]
            actions << ['(Re)Index partition', :reindex_partition]
          else
            actions << ['(Re)Index', :reindex]
          end

          pairs = actions + [['Back', :back]]
          choice = safe_select(prompt, "#{klass.name} (#{logical}) — choose action", pairs, cycle: true)
          return if choice == :back

          case choice
          when :physical_collections
            show_physical_collections_menu(prompt, client, logical)
          when :drop_collection
            confirm_and_run(prompt, 'Drop this collection? This cannot be undone.') { klass.drop_collection! }
            # Return to Collections list after drop
            return
          when :reindex
            confirm_and_run(prompt, 'Drop and reindex the collection?') { klass.reindex_collection! }
          when :reindex_full
            confirm_and_run(prompt, 'Drop and reindex the full collection?') { klass.reindex_collection! }
          when :reindex_partition
            parts = prompt_for_partitions(prompt)
            klass.rebuild_partition!(partition: parts) if parts
          end
        end
      end

      # Show list of physical collections for a resolved logical.
      def show_physical_collections_menu(prompt, client, logical)
        loop do
          physicals = SearchEngine::CollectionResolver.physicals_for_logical(client, logical)
          names = Array(physicals).map { |(n, _)| n }
          pairs = names.map { |n| [n, n] } + [['Back', :back]]
          selected = safe_select(prompt, "#{logical} — physical collections", pairs, cycle: true)
          return if selected == :back

          # After drop inside actions, we return here and the loop will refresh the list
          show_physical_collection_actions(prompt, client, selected)
        end
      end

      # Physical collection actions: View schema, Drop physical, Back.
      def show_physical_collection_actions(prompt, client, physical, after_drop: nil)
        loop do
          actions = [
            ['View schema', :schema],
            ['Drop physical collection', :drop_physical]
          ]
          pairs = actions + [['Back', :back]]
          choice = safe_select(prompt, "#{physical} — choose action", pairs, cycle: true)
          return if choice == :back

          case choice
          when :schema
            schema = client.retrieve_collection_schema(physical)
            puts(JSON.pretty_generate(schema || {}))
          when :drop_physical
            confirm_and_run(prompt, 'Drop this physical collection? This cannot be undone.') do
              client.delete_collection(physical)
            end
            # After drop: return to previous menu. Caller will refresh accordingly.
            return if after_drop == :return

            return
          end
        end
      end

      # Prompt for partition(s); returns nil (cancel), a single value, or an Array of values.
      def prompt_for_partitions(prompt)
        input = nil
        begin
          input = prompt.ask('Enter partition (single) or comma-separated list (e.g., a,b,42):')
        rescue StandardError
          input = nil
        end
        return nil if input.nil?

        str = input.to_s.strip
        return nil if str.empty?

        if str.include?(',')
          tokens = str.split(',').map(&:strip).reject(&:empty?)
          parsed = tokens.map { |t| SearchEngine::CLI.parse_partition(t) }
          return parsed
        end

        SearchEngine::CLI.parse_partition(str)
      end

      # Resolve physical collections for a logical alias (best-effort).
      def resolve_physicals_for_logical(client, logical)
        # Try alias first
        aliased = nil
        begin
          aliased = client.resolve_alias(logical)
        rescue StandardError
          aliased = nil
        end
        return [[aliased.to_s, 0]] if aliased && !aliased.to_s.strip.empty?

        # Fallback: scan all collections and group
        list = Array(client.list_collections)
        pairs = list.map do |h|
          name = (h[:name] || h['name']).to_s
          num = (h[:num_documents] || h['num_documents']).to_i
          [name, num]
        end
        pairs.select do |(physical, _num)|
          SearchEngine::Cascade.normalize_physical_to_logical(physical).to_s == logical.to_s
        rescue StandardError
          false
        end
      end

      def show_api_keys(prompt, table, client)
        return unless ensure_configured?(prompt, 'list API keys')

        keys = Array(client.list_api_keys)
        rows = keys.map do |k|
          id = (k[:id] || k['id']).to_s
          desc = (k[:description] || k['description'] || k[:comment] || k['comment']).to_s
          actions = Array(k[:actions] || k['actions']).compact.map(&:to_s).join(', ')
          cols = Array(k[:collections] || k['collections']).compact.map(&:to_s).join(', ')
          scopes = Array(k[:scopes] || k['scopes']).compact.map(&:to_s).join(', ')
          [id, desc, actions, cols, scopes]
        end
        header = %w[ID Description Actions Collections Scopes]
        print_table(table, header: header, rows: rows)
      end

      def show_settings(prompt)
        cfg = SearchEngine.config
        puts(JSON.pretty_generate(cfg.to_h_redacted))
        prompt.keypress('Press any key to return')
      end

      # --- Helpers -----------------------------------------------------------

      def require_prompt!
        require 'tty-prompt'
        TTY::Prompt.new
      rescue LoadError
        warn('Please add gem "tty-prompt" to use TUIT')
        raise
      end

      def require_table!
        require 'tty-table'
        TTY::Table
      rescue LoadError
        warn('Please add gem "tty-table" to use TUIT')
        raise
      end

      def resolve_client
        (SearchEngine.config.respond_to?(:client) && SearchEngine.config.client) || SearchEngine::Client.new
      end

      def rails_boot_if_requested!(_opts)
        # TUIT is intended to run ONLY from Rails app root; boot the app unconditionally.
        env_path = File.expand_path('config/environment.rb', Dir.pwd)
        require env_path
      end

      # Ensure host app SearchEngine models are loaded so registry/namespace lookups work.
      # This uses the dedicated models loader configured by the engine when available.
      def ensure_models_loaded!
        loader = SearchEngine.instance_variable_get(:@_models_loader)
        return unless loader

        unless SearchEngine.instance_variable_defined?(:@_models_loader_setup)
          loader.setup
          SearchEngine.instance_variable_set(:@_models_loader_setup, true)
        end

        loader.eager_load
      rescue StandardError
        # ignore loader issues; resolver will still attempt lazy constantization
      end

      def parse_argv(argv)
        { no_tty: Array(argv).include?('--no-tty') }
      end

      def print_model_settings(klass)
        dsl = klass.instance_variable_defined?(:@__mapper_dsl__) ? klass.instance_variable_get(:@__mapper_dsl__) : {}
        src = dsl && dsl[:source]
        part = SearchEngine::Partitioner.for(klass)
        stale = SearchEngine::StaleFilter.for(klass)

        info = {
          model: klass.name,
          collection: (klass.respond_to?(:collection) ? klass.collection : nil),
          attributes_count: (klass.respond_to?(:attributes) ? klass.attributes.size : nil),
          source: src && { type: src[:type], options: src[:options] },
          partitioning: part && { max_parallel: part.max_parallel },
          stale_filter_defined: !stale.nil?
        }
        puts(JSON.pretty_generate(info))
      end

      def select_partition(prompt, klass)
        compiled = SearchEngine::Partitioner.for(klass)
        unless compiled
          warn('No partitioner is defined for this model')
          return nil
        end

        keys = Array(compiled.partitions).map(&:to_s)
        return nil if keys.empty?

        pairs = (keys + ['Back']).map { |k| [k, k] }
        v = safe_select(prompt, 'Select a partition', pairs, cycle: true)
        v == 'Back' ? nil : v
      end

      def confirm_and_run(prompt, message)
        confirmed = prompt.yes?(message)
        return unless confirmed

        yield
      end

      def print_table(_table, header:, rows:)
        safe_header = Array(header).map { |v| v.nil? ? '' : v.to_s }
        safe_rows = Array(rows).map { |r| Array(r).map { |v| v.nil? ? '' : v.to_s } }
        # Simple, robust ASCII table render (no external width/TTY dependencies)
        lines = []
        lines << safe_header.join(' | ')
        lines << '-' * [lines.first.size, 40].max
        safe_rows.each { |r| lines << r.join(' | ') }
        puts(lines.join("\n"))
      end

      # Configuration guards -------------------------------------------------

      # Return true when a non-empty API key is configured.
      # @return [Boolean]
      def configured?
        key = SearchEngine.config.api_key
        !(key.nil? || key.to_s.strip.empty?)
      rescue StandardError
        false
      end

      # Display a friendly notice when Typesense is not configured.
      # @param prompt [TTY::Prompt]
      # @param action [String] description of attempted action
      # @return [Boolean] true when configured; false when the action should be skipped
      def ensure_configured?(prompt, action)
        return true if configured?

        warn("Typesense is not configured (missing api_key). Cannot #{action}.")
        warn('Set TYPESENSE_API_KEY (and host/port/protocol if needed), or configure via SearchEngine.configure.')
        begin
          prompt&.keypress('Press any key to return')
        rescue StandardError
          # ignore
        end
        false
      end

      # Render a selection menu using TTY::Prompt when possible, with a
      # numeric STDIN fallback when TTY raises or when forced via --no-tty.
      # Accepts either an Array of [label, value] pairs or a Hash of
      # label=>value. When given an Array of scalars, label and value are the same.
      # @param prompt [TTY::Prompt]
      # @param message [String]
      # @param choices [Array, Hash]
      # @param cycle [Boolean]
      # @param force_fallback [Boolean]
      # @return [Object] selected value
      def safe_select(prompt, message, choices, cycle: true, force_fallback: false)
        pairs = normalize_choice_pairs(choices)
        unless force_fallback
          begin
            return prompt.select(message, cycle: cycle) do |menu|
              pairs.each { |(label, value)| menu.choice(label.to_s, value) }
            end
          rescue StandardError
            # fall through to fallback
          end
        end

        fallback_select(message, pairs)
      end

      # Normalize choices to an Array of [label, value] pairs.
      def normalize_choice_pairs(choices)
        return [] if choices.nil?

        if choices.is_a?(Hash)
          choices.map { |k, v| [k, v] }
        elsif choices.is_a?(Array)
          if choices.all? { |e| e.is_a?(Array) && e.size >= 2 }
            choices.map { |e| [e[0], e[1]] }
          else
            choices.map { |e| [e, e] }
          end
        else
          [[choices.to_s, choices]]
        end
      end

      # Minimal numeric menu fallback for non-TTY contexts.
      def fallback_select(message, pairs)
        puts(message)
        pairs.each_with_index do |(label, _val), idx|
          puts("  #{idx + 1}) #{label}")
        end
        print('Enter choice number: ')
        $stdout.flush
        input = $stdin.gets
        index = begin
          Integer(input.to_s.strip) - 1
        rescue StandardError
          -1
        end
        index = 0 unless index && index >= 0 && index < pairs.size
        pairs[index][1]
      end
    end
  end
end
