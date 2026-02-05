# frozen_string_literal: true

require 'json'

module SearchEngine
  class Relation
    module Dx
      # Pure helpers for producing redacted previews without I/O.
      module DryRun
        # Redact a compiled params hash, preserving only whitelisted keys and
        # masking sensitive fields. Returns a new Hash.
        # @param params [Hash]
        # @return [Hash]
        def self.redact_params(params)
          redacted = SearchEngine::Observability.redact(params)
          return redacted unless params.is_a?(Hash)

          hits = params[:_hits] || params['_hits']
          return redacted unless hits

          out = redacted.is_a?(Hash) ? redacted.dup : {}
          out[:_hits] = hits
          out
        end

        # Return pretty or compact JSON with stable key ordering when pretty.
        # @param value [Object]
        # @param pretty [Boolean]
        # @return [String]
        def self.to_json(value, pretty: true)
          if pretty && value.is_a?(Hash)
            ordered = value.sort_by { |(k, _v)| k.to_s }.to_h
            JSON.pretty_generate(ordered)
          else
            JSON.generate(value)
          end
        end

        # Build a single-line curl string with redacted body.
        # @param url [String]
        # @param params [Hash]
        # @return [String]
        def self.curl(url, params)
          body_json = JSON.generate(redact_params(params))
          %(curl -X POST #{url} -H 'Content-Type: application/json' -H 'X-TYPESENSE-API-KEY: ***' -d '#{body_json}')
        end

        # Return a structured dry-run payload with redacted JSON body and url options.
        # @param url [String]
        # @param params [Hash]
        # @param url_opts [Hash]
        # @return [Hash]
        def self.payload(url:, params:, url_opts: {})
          { url: url, body: JSON.generate(redact_params(params)), url_opts: url_opts.freeze }
        end
      end
    end
  end
end
