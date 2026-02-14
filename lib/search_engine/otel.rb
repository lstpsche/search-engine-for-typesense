# frozen_string_literal: true

module SearchEngine
  # Optional OpenTelemetry adapter that translates unified instrumentation
  # events into OpenTelemetry spans. Activation is gated by the presence of the
  # OpenTelemetry SDK and by `SearchEngine.config.opentelemetry.enabled`.
  #
  # @since M8
  # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/observability#opentelemetry
  #
  # Public API:
  # - .installed? => Boolean
  # - .enabled?   => Boolean (config + SDK present)
  # - .start!     => idempotently subscribes to events
  # - .stop!      => unsubscribes
  module Otel
    class << self
      # @return [Boolean] whether the OpenTelemetry SDK is available
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/observability#opentelemetry
      def installed?
        defined?(::OpenTelemetry::SDK)
      end

      # @return [Boolean] whether the adapter should be active
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/observability#opentelemetry
      def enabled?
        installed? && SearchEngine.respond_to?(:config) && SearchEngine.config&.opentelemetry&.enabled
      end

      # Start the adapter (idempotent). No-ops when disabled or SDK unavailable.
      # @return [Object, nil] subscription handle or nil when not installed/enabled
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/observability#opentelemetry
      def start!
        stop!
        return nil unless enabled?
        return nil unless defined?(ActiveSupport::Notifications)

        @service_name = begin
          SearchEngine.config.opentelemetry.service_name
        rescue StandardError
          'search_engine'
        end
        @tracer = tracer_provider&.tracer('search_engine', SearchEngine::VERSION)

        @handle = ActiveSupport::Notifications.subscribe(/^search_engine\./) do |*args|
          # Lazily build Event only when sampled downstream; allocation kept minimal otherwise
          event = ActiveSupport::Notifications::Event.new(*args)
          handle_event(event)
        end
      rescue StandardError
        # Never raise from adapter startup
        nil
      end

      # Stop the adapter if previously started.
      # @return [Boolean]
      # @since M8
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/observability#opentelemetry
      def stop!
        return false unless defined?(ActiveSupport::Notifications)
        return false unless @handle

        ActiveSupport::Notifications.unsubscribe(@handle)
        @handle = nil
        true
      end

      private

      def tracer_provider
        return nil unless installed?

        ::OpenTelemetry.tracer_provider
      rescue StandardError
        nil
      end

      attr_reader :tracer

      def handle_event(event)
        return unless tracer

        payload = event.payload || {}
        duration = compute_duration(event, payload)

        tracer.in_span(event.name) do |span|
          apply_common_attributes(span, event, payload, duration)
          apply_url_attributes(span, payload)
          apply_feature_attributes(span, payload)
          apply_indexer_schema_attributes(span, payload)
          apply_params_preview(span, payload)
          apply_status(span, payload)
        end
      rescue StandardError
        # Never raise from subscriber
        nil
      end

      def compute_duration(event, payload)
        d = (event.respond_to?(:duration) ? event.duration : payload[:duration_ms]).to_f
        d = payload[:duration_ms].to_f if d.zero? && payload[:duration_ms]
        d.round(1)
      end

      def apply_common_attributes(span, event, payload, duration)
        assign_attr(span, 'service.name', @service_name)
        assign_attr(span, 'se.event', event.name)
        assign_attr(span, 'se.cid', payload[:correlation_id]) if payload.key?(:correlation_id)
        assign_attr(span, 'http.status_code', payload[:http_status]) if payload.key?(:http_status)
        assign_attr(span, 'se.duration_ms', duration) if duration.positive?
        return unless payload[:collection] || payload[:logical]

        assign_attr(span, 'se.collection', payload[:collection] || payload[:logical])
      end

      def apply_url_attributes(span, payload)
        url_opts = payload[:url_opts]
        return unless url_opts.is_a?(Hash)

        assign_attr(span, 'se.url_use_cache', url_opts[:use_cache]) if url_opts.key?(:use_cache)
        assign_attr(span, 'se.url_cache_ttl', url_opts[:cache_ttl]) if url_opts.key?(:cache_ttl)
      end

      def apply_feature_attributes(span, p)
        assign_attr(span, 'se.labels_count', Array(p[:labels]).size) if p.key?(:labels)
        assign_attr(span, 'se.searches_count', p[:searches_count]) if p.key?(:searches_count)
        assign_attr(span, 'se.node_count', p[:node_count]) if p.key?(:node_count)
        assign_attr(span, 'se.join_count', p[:join_count]) if p.key?(:join_count)
        assign_attr(span, 'se.groups_count', p[:groups_count]) if p.key?(:groups_count)
        assign_attr(span, 'se.group_by', p[:field] || p[:group_by]) if p.key?(:field) || p.key?(:group_by)
        assign_attr(span, 'se.group_limit', p[:limit] || p[:group_limit]) if p.key?(:limit) || p.key?(:group_limit)
        return unless p.key?(:missing_values) || p.key?(:group_missing_values)

        assign_attr(span, 'se.group_missing_values', p[:missing_values] || p[:group_missing_values])
      end

      def apply_indexer_schema_attributes(span, p)
        %i[into partition partition_hash batch_index docs_count success_count failure_count attempts bytes_sent
           deleted_count searches_count fields_changed_count added_count removed_count in_sync].each do |k|
          assign_attr(span, "se.#{k}", p[k]) if p.key?(k)
        end
        # New event attributes (redacted/summarized)
        %i[fields_count queries_count max_facet_values sort_flags conflicts].each do |k|
          assign_attr(span, "se.#{k}", p[k]) if p.key?(k)
        end
        %i[full_fields_count affix_tokens snippet_threshold tag_kind].each do |k|
          assign_attr(span, "se.#{k}", p[k]) if p.key?(k)
        end
        %i[use_synonyms use_stopwords source].each do |k|
          assign_attr(span, "se.#{k}", p[k]) if p.key?(k)
        end
        if p.key?(:shapes)
          shapes = p[:shapes] || {}
          assign_attr(span, 'se.shapes.point', shapes[:point]) if shapes.key?(:point)
          assign_attr(span, 'se.shapes.rect', shapes[:rect]) if shapes.key?(:rect)
          assign_attr(span, 'se.shapes.circle', shapes[:circle]) if shapes.key?(:circle)
        end
        %i[sort_mode radius_bucket].each do |k|
          assign_attr(span, "se.#{k}", p[k]) if p.key?(k)
        end
        %i[query_vector_present dims hybrid_weight ann_params_present].each do |k|
          assign_attr(span, "se.#{k}", p[k]) if p.key?(k)
        end
        %i[early_limit validate_max applied_strategy triggered total_hits].each do |k|
          assign_attr(span, "se.#{k}", p[k]) if p.key?(k)
        end
      end

      def apply_params_preview(span, payload)
        return unless payload.key?(:params_preview)

        red = SearchEngine::Instrumentation.redact(payload[:params_preview])
        keys_count = (red.is_a?(Hash) ? red.keys.size : nil)
        assign_attr(span, 'se.params_preview_keys', keys_count) if keys_count
      rescue StandardError
        nil
      end

      def apply_status(span, payload)
        http = payload[:http_status]
        status = payload[:status]
        err_class = payload[:error_class]
        err_msg = payload[:error_message]

        if (status && status.to_sym == :error) || (http && http.to_i >= 400) || err_class
          # Record a lightweight exception event with sanitized message
          if err_class || err_msg
            msg = SearchEngine::Observability.truncate_message(err_msg || err_class.to_s, 200)
            span.add_event(
              'exception',
              attributes: {
                'exception.type' => (err_class || 'Error').to_s,
                'exception.message' => msg
              }
            )
          end
          span_status_error(span, err_msg || err_class)
        else
          span_status_ok(span)
        end
      end

      def span_status_error(span, description = nil)
        span.status = ::OpenTelemetry::Trace::Status.error(description.to_s) if defined?(::OpenTelemetry::Trace::Status)
      rescue StandardError
        nil
      end

      def span_status_ok(span)
        span.status = ::OpenTelemetry::Trace::Status.ok if defined?(::OpenTelemetry::Trace::Status)
      rescue StandardError
        nil
      end

      def assign_attr(span, key, value)
        return if value.nil?

        span.set_attribute(key, value)
      rescue StandardError
        nil
      end
    end
  end
end
