# frozen_string_literal: true

module SearchEngine
  # Public error hierarchy for the SearchEngine client wrapper.
  #
  # These exceptions provide a stable contract to callers regardless of the
  # underlying HTTP client or the Typesense gem's internal error types.
  module Errors
    # Base error for all SearchEngine failures.
    # Carries optional structured metadata for enhanced DX.
    #
    # Keyword options are optional and backwards-compatible. Existing call sites
    # that pass only a message remain valid.
    #
    # @!attribute [r] hint
    #   @return [String, nil] short actionable suggestion (no secrets)
    # @!attribute [r] doc
    #   @return [String, nil] docs URL with optional anchor (e.g., "https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/query-dsl#operators")
    # @!attribute [r] details
    #   @return [Object, nil] machine-readable context (JSON-serializable)
    # @!attribute [r] code
    #   @return [Symbol, nil] stable symbolic code when defined by subclasses
    # @abstract
    class Error < StandardError
      attr_reader :hint, :doc, :details, :code

      # @param message [String]
      # @param hint [String, nil]
      # @param doc [String, nil]
      # @param details [Object, nil]
      # @param code [Symbol, nil]
      def initialize(message = nil, hint: nil, doc: nil, details: nil, code: nil, **_ignore)
        super(message)
        @hint = presence_or_nil(hint)
        @doc = presence_or_nil(doc)
        @details = sanitize_details(details)
        @code = code
      end

      # Return a stable, redaction-aware hash for logging/telemetry.
      # Keys are predictable for downstream processing.
      # @return [Hash]
      def to_h
        base = {
          type: self.class.name,
          message: to_base_message,
          hint: @hint,
          doc: @doc,
          details: @details
        }
        base[:code] = @code if @code
        prune_nils(base)
      end

      # Preserve historic message but append a concise suffix when hint/doc present.
      # Single-line for log friendliness.
      # @return [String]
      def to_s
        base = to_base_message
        suffix = []
        suffix << "Hint: #{@hint}" if @hint
        suffix << "see #{@doc}" if @doc
        return base if suffix.empty?

        "#{base} — #{suffix.join(' ')}"
      end

      private

      def to_base_message
        # Call Exception#to_s directly to avoid our overridden to_s suffix
        Exception.instance_method(:to_s).bind_call(self).to_s
      end

      def sanitize_details(obj)
        return nil if obj.nil?

        if defined?(SearchEngine::Observability)
          begin
            red = SearchEngine::Observability.redact(obj)
            return jsonable(red)
          rescue StandardError
            return jsonable(obj)
          end
        end

        jsonable(obj)
      end

      def jsonable(obj)
        case obj
        when Hash
          obj.each_with_object({}) { |(k, v), h| h[k.to_sym] = jsonable(v) }
        when Array
          obj.map { |v| jsonable(v) }
        when Numeric, TrueClass, FalseClass, NilClass, String
          obj
        else
          obj.to_s
        end
      end

      def prune_nils(h)
        h.each_with_object({}) do |(k, v), acc|
          acc[k] = v unless v.nil?
        end
      end

      def presence_or_nil(v)
        return nil if v.nil?

        s = v.to_s
        s.strip.empty? ? nil : s
      end
    end

    # Raised when a request exceeds the configured timeout budget.
    #
    # Typical causes include connect/open timeouts or read timeouts surfaced by
    # the underlying HTTP client used by the official Typesense gem.
    class Timeout < Error; end

    # Raised for network-level connectivity issues prior to receiving a response.
    #
    # Examples: DNS resolution failures, refused TCP connections, TLS handshake
    # errors, or other socket-level errors.
    class Connection < Error; end

    # Raised when Typesense responds with a non-2xx HTTP status code.
    #
    # Carries the HTTP status and the parsed error body (when available) to aid
    # in debugging and programmatic handling upstream.
    class Api < Error
      # @return [Integer] HTTP status code
      attr_reader :status

      # @return [Object, nil] Parsed error body (Hash/String), when available
      attr_reader :body

      # @param msg [String]
      # @param status [Integer]
      # @param body [Object, nil]
      def initialize(msg, status:, body: nil, **kw)
        super(msg, **kw)
        @status = status
        @body = body
      end
    end

    # Raised when wrapper-level validation fails before making a request.
    #
    # Use this for actionable, developer-facing messages that indicate a caller
    # constructed an invalid request (e.g., blank collection name).
    class InvalidParams < Error; end

    # Raised when a provided field name is unknown or disallowed for a model.
    #
    # Typical cause: a typo or using a field that is not declared via
    # {SearchEngine::Base.attribute}.
    class InvalidField < Error; end

    # Raised when a base attribute referenced by the Field Selection DSL is not
    # declared on the model.
    #
    # Prefer this over {InvalidField} for selection-time validation to provide
    # developer-friendly guidance and suggestions.
    class UnknownField < Error; end

    # Raised when an operator or fragment token is not recognized by the SQL-ish
    # grammar accepted by the Parser.
    class InvalidOperator < Error; end

    # Raised when a value cannot be coerced to the declared attribute type, or
    # when its shape is incompatible (e.g., empty array for IN/NOT IN).
    class InvalidType < Error; end

    # Raised when a requested join association is not declared for a model.
    #
    # Typical cause: a typo or referencing an association that has not been
    # registered via {SearchEngine::Base.join}.
    class UnknownJoin < Error; end

    # Raised when an association reference is invalid for the model and should
    # be declared via {SearchEngine::Base.join}.
    #
    # Prefer this for high-level validation messaging with guidance and
    # suggestions ("did you mean ..."), while keeping {UnknownJoin} for
    # lower-level registry lookups.
    class InvalidJoin < Error; end

    # Raised when a query references a joined association field without applying
    # the association on the relation via {SearchEngine::Relation#joins}.
    #
    # Example: calling `where(authors: { last_name: "Rowling" })` without
    # `.joins(:authors)` on the relation first.
    class JoinNotApplied < Error; end

    # Raised when a nested attribute referenced by the Field Selection DSL is
    # not declared on the joined association's target model.
    #
    # Typical cause: a typo in a nested field name or a stale attribute map.
    class UnknownJoinField < Error; end

    # Raised when selection inputs are malformed or ambiguous and cannot be
    # deterministically normalized (e.g., invalid nested shapes or incompatible
    # payload types).
    class ConflictingSelection < Error; end

    # Raised when grouping DSL is used with invalid inputs.
    #
    # Use for actionable messages like unknown field names, invalid limit values,
    # or non-boolean missing_values.
    #
    # @example Unknown field with suggestion
    #   raise SearchEngine::Errors::InvalidGroup, "InvalidGroup: unknown field :brand for grouping on SearchEngine::Product (did you mean :brand_id?)"
    class InvalidGroup < Error; end

    # Raised when grouping references unsupported constructs such as joined/path fields
    # (e.g., "$assoc.field"). Only base fields are supported for grouping.
    #
    # @example
    #   raise SearchEngine::Errors::UnsupportedGroupField, 'UnsupportedGroupField: grouping supports base fields only (got "$authors.last_name")'
    class UnsupportedGroupField < Error; end

    # Raised when strict selection is enabled and a requested field is absent
    # in the hydrated document (e.g., excluded by API mapping).
    #
    # This error is actionable and guides remediation: adjust the relation's
    # selection (select/exclude/reselect), relax strictness, or ensure the
    # upstream Typesense include/exclude mapping includes the fields.
    class MissingField < Error; end

    # Raised when a materializer requests fields that are not permitted by the
    # relation's effective selection (include − exclude, with exclude taking precedence).
    #
    # Used by selection-aware materializers like {SearchEngine::Relation#pluck},
    # {SearchEngine::Relation#pick}, and {SearchEngine::Relation#ids} to fail fast
    # before any network call.
    class InvalidSelection < Error; end

    # Raised when a curated ID does not match the configured pattern.
    #
    # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/curation
    # @example
    #   raise SearchEngine::Errors::InvalidCuratedId, 'InvalidCuratedId: "foo bar" is not a valid curated ID. Expected pattern: /\A[\w\-:\.]+\z/. Try removing illegal characters.'
    class InvalidCuratedId < Error; end

    # Raised when pinned/hidden lists exceed configured limits after normalization.
    #
    # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/curation
    # @example
    #   raise SearchEngine::Errors::CurationLimitExceeded, 'CurationLimitExceeded: pinned list exceeds max_pins=50 (attempted 51). Reduce inputs or raise the limit in SearchEngine.config.curation.'
    class CurationLimitExceeded < Error; end

    # Raised when an override tag is blank or invalid per allowed pattern.
    #
    # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/curation
    # @example
    #   raise SearchEngine::Errors::InvalidOverrideTag, 'InvalidOverrideTag: "" is invalid. Use non-blank strings that match the allowed pattern.'
    class InvalidOverrideTag < Error; end

    # Raised when an option value is invalid or unsupported for a public API.
    #
    # Used by DSL methods to fail fast with actionable hints.
    # Typical causes: invalid HTML tag tokens, negative integers for thresholds,
    # or empty field lists where at least one field is required.
    #
    # @example
    #   raise SearchEngine::Errors::InvalidOption.new(
    #     'InvalidOption: tag must be a simple HTML-like token',
    #     hint: 'Use a simple tag like <em> or <mark>',
    #     doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/highlighting#options'
    #   )
    class InvalidOption < Error; end

    # Raised when a query result exceeds the configured post-fetch hits ceiling.
    #
    # Use this for actionable messages when {Relation#validate_hits!} is set and
    # the backend reports a total hits count above the allowed maximum.
    #
    # @example
    #   raise SearchEngine::Errors::HitLimitExceeded.new(
    #     'HitLimitExceeded: 12000 results exceed max=10000',
    #     hint: 'Increase `validate_hits!(max:)` or narrow your filters.',
    #     doc: 'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/hit-limits#validation',
    #     details: { total_hits: 12_000, max: 10_000, collection: 'products' }
    #   )
    class HitLimitExceeded < Error; end
  end
end
