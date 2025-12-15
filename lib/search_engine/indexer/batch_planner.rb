# frozen_string_literal: true

require 'json'

module SearchEngine
  class Indexer
    # Plans and produces batches from a stream of documents.
    #
    # Takes either pre-batched arrays from an enumerable or a flat enumerable of
    # docs and emits JSONL-encoded buffers alongside counts and stats.
    # The Indexer currently receives already-batched arrays; this planner keeps
    # that contract and focuses on JSONL encoding with minimal allocations.
    #
    # @since M8
    class BatchPlanner
      # Produce a JSONL buffer and counts for a provided docs array.
      #
      # @param docs [Array<Hash>]
      # @param buffer [String] a reusable String buffer to encode into
      # @return [Array(Integer, Integer)] [docs_count, bytes_sent]
      # @raise [SearchEngine::Errors::InvalidParams] when a document is not a Hash or missing :id
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer
      def self.encode_jsonl!(docs, buffer)
        count = 0
        buffer.clear
        size = docs.size
        docs.each_with_index do |raw, idx|
          doc = ensure_hash_document(raw)
          ensure_id!(doc)
          # Force system timestamp field prior to serialization to Typesense
          now_i = if defined?(Time) && defined?(Time.zone) && Time.zone
                    Time.zone.now.to_i
                  else
                    Time.now.to_i
                  end
          doc[:doc_updated_at] = now_i if doc.is_a?(Hash)
          buffer << JSON.generate(doc)
          buffer << "\n" if idx < (size - 1)
          count += 1
        end
        # Ensure trailing newline for non-empty payloads for consistency
        buffer << "\n" if size.positive? && !buffer.end_with?("\n")
        [count, buffer.bytesize]
      end

      # Utility: normalize a batch-like object to an Array.
      # @param batch [Object]
      # @return [Array]
      def self.to_array(batch)
        return batch if batch.is_a?(Array)

        batch.respond_to?(:to_a) ? batch.to_a : Array(batch)
      end

      class << self
        private

        def ensure_hash_document(obj)
          if obj.is_a?(Hash)
            obj
          else
            raise SearchEngine::Errors::InvalidParams,
                  'Indexer requires batches of Hash-like documents with at least an :id key. ' \
                  'Mapping DSL is not available yet. See ' \
                  'https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/indexer.'
          end
        end

        def ensure_id!(doc)
          has_id = doc.key?(:id) || doc.key?('id')
          raise SearchEngine::Errors::InvalidParams, 'document is missing required id' unless has_id
        end
      end
    end
  end
end
