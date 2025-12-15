# frozen_string_literal: true

require 'json'
require 'active_support/notifications'
require 'search_engine/logging/format_helpers'

module SearchEngine
  # Structured logging subscriber for unified instrumentation events.
  #
  # Consumes events emitted via {SearchEngine::Instrumentation.instrument}
  # and writes either a compact single-line entry or a JSON object per event.
  #
  # Configuration:
  #
  #   SearchEngine.configure do |c|
  #     c.logging = OpenStruct.new(mode: :compact, level: :info, sample: 1.0, logger: Rails.logger)
  #   end
  #
  # Modes: :compact (default) or :json. Supports sampling and opt-out via
  # sample: 0.0 or mode: nil.
  #
  # @since M8
  # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/observability#logging
  # @see `https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/observability`
  module LoggingSubscriber
    class << self
      # Install the subscriber in a reloader-safe and idempotent way.
      #
      # @param config [#mode,#level,#sample,#logger,nil]
      # @return [Object, nil] subscription handle
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/observability#logging
      def install!(config = nil)
        uninstall!

        cfg = normalize_config(config)
        return nil if cfg[:mode].nil? || cfg[:sample] <= 0.0
        return nil unless defined?(ActiveSupport::Notifications)

        @mode = cfg[:mode]
        @level = cfg[:level]
        @logger = cfg[:logger]
        @sample = cfg[:sample]

        @handle = ActiveSupport::Notifications.subscribe(/^search_engine\./) do |*args|
          # Fast sampling decision before formatting or allocations beyond Event
          next unless sampled?(@sample)

          event = ActiveSupport::Notifications::Event.new(*args)
          begin
            log_line = (@mode == :json ? format_json(event) : format_compact(event))
            emit(@logger, @level, log_line)
          rescue StandardError
            # Never raise from logging
            nil
          end
        end
      end

      # Uninstall previously installed subscriber.
      # @return [Boolean]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/observability#logging
      def uninstall!
        return false unless defined?(ActiveSupport::Notifications)
        return false unless @handle

        ActiveSupport::Notifications.unsubscribe(@handle)
        @handle = nil
        true
      end

      private

      def normalize_config(config)
        c = config || (SearchEngine.respond_to?(:config) ? SearchEngine.config.logging : nil)
        logger =
          if c.respond_to?(:logger) && c.logger
            c.logger
          elsif SearchEngine.respond_to?(:config) && SearchEngine.config&.logger
            SearchEngine.config.logger
          else
            require 'logger'
            Logger.new($stdout)
          end

        mode = c&.mode || :compact
        level = c&.level || :info
        sample = begin
          val = c&.sample
          val = 1.0 if val.nil?
          val = 0.0 if val.respond_to?(:to_f) && val.to_f.negative?
          val = 1.0 if val.respond_to?(:to_f) && val.to_f > 1.0
          val.to_f
        rescue StandardError
          1.0
        end

        { mode: mode&.to_sym, level: level.to_sym, sample: sample, logger: logger }
      end

      def emit(logger, level, line)
        return unless logger && level && line

        if logger.respond_to?(level)
          logger.public_send(level, line)
        elsif logger.respond_to?(:add)
          # Fallback: map to Logger::Severity if available
          sev = severity_map[level] || severity_map[:info]
          logger.add(sev, line)
        end
      end

      def severity_map
        @severity_map ||= if defined?(::Logger)
                            {
                              debug: ::Logger::DEBUG,
                              info: ::Logger::INFO,
                              warn: ::Logger::WARN,
                              error: ::Logger::ERROR,
                              fatal: ::Logger::FATAL
                            }
                          else
                            { debug: 0, info: 1, warn: 2, error: 3, fatal: 4 }
                          end
      end

      def sampled?(rate)
        return false if rate <= 0.0
        return true if rate >= 1.0

        rng = (Thread.current[:__se_log_rng__] ||= Random.new)
        rng.rand < rate
      end

      # --- Formatting helpers ---

      def format_compact(event)
        p = event.payload || {}
        name = event.name
        short = name.sub(/^search_engine\./, 'se.')
        cid = short_correlation_id(p[:correlation_id])
        duration = safe_duration(event, p)
        status = pick_status(p)
        collection = p[:collection] || p[:logical]

        # Specialized single-line renderers (return String or nil)
        specialized = format_compact_event(name: name, short: short, cid: cid, collection: collection,
                                           duration: duration, status: status, payload: p
        )
        return specialized if specialized

        parts = compact_base_parts(short, cid, collection, status, duration, p)
        parts.join(' ')
      end

      # Build base compact parts for generic events (single-line, allocation-light)
      def compact_base_parts(short, cid, collection, status, duration, p)
        groups = SearchEngine::Logging::FormatHelpers.value_or_dash(p[:groups_count])
        preset = p[:preset_name] || SearchEngine::Logging::FormatHelpers.value_or_dash(nil)
        pinned = p[:curation_pinned_count] || p[:pinned_count] || 0
        hidden = p[:curation_hidden_count] || p[:hidden_count] || 0

        parts = []
        parts << "[#{short}]"
        parts << "id=#{cid}"
        parts << "coll=#{collection}" if collection
        parts << "status=#{status}"
        parts << "dur=#{duration}ms"
        parts << "groups=#{groups}"
        parts << "preset=#{preset}"
        parts << "cur=#{pinned}/#{hidden}"
        parts
      end
      private :compact_base_parts

      def format_json(event)
        p = event.payload || {}
        base = base_json_fields(event, p)
        merge_event_extras!(base, event.name, p)
        attach_params_preview_count!(base, p)
        JSON.generate(safe_jsonable(base))
      rescue StandardError
        # As a last resort, log a minimal JSON line
        duration = safe_duration(event, p)
        cid = short_correlation_id(p[:correlation_id])
        status = pick_status(p)
        JSON.generate({ 'event' => event.name, 'cid' => cid, 'status' => status, 'duration_ms' => duration })
      end

      def base_json_fields(event, p)
        duration = safe_duration(event, p)
        cid = short_correlation_id(p[:correlation_id])
        status = pick_status(p)
        h = {}
        h['event'] = event.name
        h['cid'] = cid
        h['collection'] = (p[:collection] || p[:logical]) if p[:collection] || p[:logical]
        h['status'] = status
        h['duration_ms'] = duration
        h['group_count'] = p[:groups_count] if p.key?(:groups_count)
        h['preset_mode'] = p[:preset_mode] if p.key?(:preset_mode)
        if p.key?(:curation_pinned_count) || p.key?(:pinned_count)
          h['curation_pinned_count'] = (p[:curation_pinned_count] || p[:pinned_count])
        end
        if p.key?(:curation_hidden_count) || p.key?(:hidden_count)
          h['curation_hidden_count'] = (p[:curation_hidden_count] || p[:hidden_count])
        end
        h
      end

      def merge_event_extras!(h, name, p)
        extras = build_event_json_extras(name, p)
        extras.each { |k, v| h[k] = v } unless extras.empty?
      end

      def attach_params_preview_count!(h, p)
        return unless p.key?(:params_preview)

        preview = p[:params_preview]
        preview = SearchEngine::Instrumentation.redact(preview)
        h['params_preview_keys'] = (preview.is_a?(Hash) ? preview.keys.size : nil)
      end

      def format_compact_event(name:, short:, cid:, collection:, duration:, status:, payload:)
        return compact_facet(short, cid, collection, duration, payload) if name == 'search_engine.facet.compile'
        return compact_highlight(short, cid, collection, duration, payload) if name == 'search_engine.highlight.compile'
        return compact_synonyms(short, cid, collection, duration, payload) if name == 'search_engine.synonyms.apply'
        return compact_geo(short, cid, collection, duration, payload) if name == 'search_engine.geo.compile'
        return compact_vector(short, cid, collection, duration, payload) if name == 'search_engine.vector.compile'

        if name == 'search_engine.hits.limit'
          return compact_hits_limit(short, cid, collection, duration, status, payload)
        end

        nil
      end

      def compact_facet(short, cid, collection, duration, p)
        parts = []
        parts << "[#{short}]"
        parts << "id=#{cid}"
        parts << "coll=#{collection}" if collection
        parts << "fields=#{p[:fields_count] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "queries=#{p[:queries_count] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "max=#{p[:max_facet_values] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "dur=#{duration}ms"
        parts.join(' ')
      end

      def compact_highlight(short, cid, collection, duration, p)
        parts = []
        parts << "[#{short}]"
        parts << "id=#{cid}"
        parts << "coll=#{collection}" if collection
        parts << "fields=#{p[:fields_count] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "full=#{p[:full_fields_count] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "affix=#{SearchEngine::Logging::FormatHelpers.display_or_dash(p, :affix_tokens)}"
        parts << "tag=#{p[:tag_kind] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "dur=#{duration}ms"
        parts.join(' ')
      end

      def compact_synonyms(short, cid, collection, duration, p)
        parts = []
        parts << "[#{short}]"
        parts << "id=#{cid}"
        parts << "coll=#{collection}" if collection
        parts << "syn=#{SearchEngine::Logging::FormatHelpers.display_or_dash(p, :use_synonyms)}"
        parts << "stop=#{SearchEngine::Logging::FormatHelpers.display_or_dash(p, :use_stopwords)}"
        parts << "src=#{p[:source] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "dur=#{duration}ms"
        parts.join(' ')
      end

      def compact_geo(short, cid, collection, duration, p)
        shapes = p[:shapes] || {}
        point = shapes[:point] || 0
        rect = shapes[:rect] || 0
        circle = shapes[:circle] || 0
        parts = []
        parts << "[#{short}]"
        parts << "id=#{cid}"
        parts << "coll=#{collection}" if collection
        parts << "shapes=#{point}/#{rect}/#{circle}"
        parts << "sort=#{p[:sort_mode] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "radius=#{p[:radius_bucket] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "dur=#{duration}ms"
        parts.join(' ')
      end

      def compact_vector(short, cid, collection, duration, p)
        parts = []
        parts << "[#{short}]"
        parts << "id=#{cid}"
        parts << "coll=#{collection}" if collection
        parts << "qvec=#{SearchEngine::Logging::FormatHelpers.display_or_dash(p, :query_vector_present)}"
        parts << "dims=#{p[:dims] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "hybrid=#{p[:hybrid_weight] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "ann=#{SearchEngine::Logging::FormatHelpers.display_or_dash(p, :ann_params_present)}"
        parts << "dur=#{duration}ms"
        parts.join(' ')
      end

      def compact_hits_limit(short, cid, collection, duration, status, p)
        parts = []
        parts << "[#{short}]"
        parts << "id=#{cid}"
        parts << "coll=#{collection}" if collection
        parts << "early=#{SearchEngine::Logging::FormatHelpers.display_or_dash(p, :early_limit)}"
        parts << "max=#{SearchEngine::Logging::FormatHelpers.display_or_dash(p, :validate_max)}"
        parts << "strat=#{p[:applied_strategy] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "trig=#{p[:triggered] || SearchEngine::Logging::FormatHelpers::DASH}"
        parts << "total=#{SearchEngine::Logging::FormatHelpers.display_or_dash(p, :total_hits)}"
        parts << "status=#{status}"
        parts << "dur=#{duration}ms"
        parts.join(' ')
      end

      def build_event_json_extras(name, p)
        case name
        when 'search_engine.facet.compile'
          keys = %i[fields_count queries_count max_facet_values sort_flags conflicts]
          keys.each_with_object({}) { |k, h| h[k.to_s] = p[k] if p.key?(k) }
        when 'search_engine.highlight.compile'
          keys = %i[fields_count full_fields_count affix_tokens snippet_threshold tag_kind]
          keys.each_with_object({}) { |k, h| h[k.to_s] = p[k] if p.key?(k) }
        when 'search_engine.synonyms.apply'
          keys = %i[use_synonyms use_stopwords source]
          keys.each_with_object({}) { |k, h| h[k.to_s] = p[k] if p.key?(k) }
        when 'search_engine.geo.compile'
          h = {}
          h['filters_count'] = p[:filters_count] if p.key?(:filters_count)
          h['shapes'] = p[:shapes] if p.key?(:shapes)
          h['sort_mode'] = p[:sort_mode] if p.key?(:sort_mode)
          h['radius_bucket'] = p[:radius_bucket] if p.key?(:radius_bucket)
          h
        when 'search_engine.vector.compile'
          keys = %i[query_vector_present dims hybrid_weight ann_params_present]
          keys.each_with_object({}) { |k, h| h[k.to_s] = p[k] if p.key?(k) }
        when 'search_engine.hits.limit'
          keys = %i[early_limit validate_max applied_strategy triggered total_hits]
          keys.each_with_object({}) { |k, h| h[k.to_s] = p[k] if p.key?(k) }
        else
          {}
        end
      end

      def safe_duration(event, payload)
        d = (event.respond_to?(:duration) ? event.duration : payload[:duration_ms]).to_f
        d = payload[:duration_ms].to_f if d.zero? && payload[:duration_ms]
        d.round(1)
      end

      def pick_status(payload)
        payload.key?(:http_status) ? payload[:http_status] : (payload[:status] || 'ok')
      end

      def short_correlation_id(cid)
        str = cid.to_s
        return random_hex_4 if str.empty?

        str[0, 4]
      end

      def random_hex_4
        rng = (Thread.current[:__se_log_rng__] ||= Random.new)
        val = rng.rand(0x10000)
        val.to_s(16).rjust(4, '0')
      end

      def safe_jsonable(obj)
        case obj
        when Hash
          obj.each_with_object({}) do |(k, v), h|
            h[k.to_s] = safe_jsonable(v)
          end
        when Array
          obj.map { |v| safe_jsonable(v) }
        when Numeric, TrueClass, FalseClass, NilClass
          obj
        else
          obj.to_s
        end
      end
    end
  end
end
