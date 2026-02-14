# frozen_string_literal: true

module SearchEngine
  class Relation
    # Materializers delegated to Hydration layer (single network call per Relation instance).
    module Materializers
      # Execute the relation and return the memoized Result.
      # @return [SearchEngine::Result]
      def execute
        SearchEngine::Hydration::Materializers.execute(self)
      end

      # Return a shallow copy of hydrated hits.
      # @return [Array<Object>]
      def to_a
        SearchEngine::Hydration::Materializers.to_a(self)
      end

      # Iterate over hydrated hits.
      # @yieldparam obj [Object]
      # @return [Enumerator] when no block is given
      def each(&block)
        SearchEngine::Hydration::Materializers.each(self, &block)
      end

      # Return the first element or the first N elements from the loaded page.
      # @param n [Integer, nil]
      # @return [Object, Array<Object>]
      def first(n = nil)
        SearchEngine::Hydration::Materializers.first(self, n)
      end

      # Return the last element or the last N elements from the currently fetched page.
      # @param n [Integer, nil]
      # @return [Object, Array<Object>]
      def last(n = nil)
        SearchEngine::Hydration::Materializers.last(self, n)
      end

      # Take N elements from the head. When N==1, returns a single object.
      # @param n [Integer]
      # @return [Object, Array<Object>]
      def take(n = 1)
        SearchEngine::Hydration::Materializers.take(self, n)
      end

      # Return raw Typesense response for this relation.
      # Executes the query (memoized) and returns Result#raw.
      # @return [Hash]
      def raw
        SearchEngine::Hydration::Materializers.execute(self).raw
      end

      # Find the first matching record using where-like inputs.
      # Accepts the same arguments as `.where` (Hash, String, Array, Symbol),
      # applies them if provided, then limits to a single result and returns it.
      #
      # @param args [Array<Object>] where-compatible arguments
      # @return [Object, nil]
      # @example
      #   SearchEngine::Product.find_by(article_code: 12312, store_id: 1031)
      #   SearchEngine::Product.where(active: true).find_by('price:>100')
      def find_by(*args)
        relation = args.nil? || args.empty? ? self : where(*args)
        relation.per(1).page(1).first
      end

      # Convenience for plucking :id values.
      # @return [Array<Object>]
      def ids
        SearchEngine::Hydration::Materializers.ids(self)
      end

      # Pick one or multiple fields from the first matching record.
      # Returns a single value for one field, or an Array for multiple fields.
      # Returns nil when no records match.
      # @param fields [Array<#to_sym,#to_s>]
      # @return [Object, Array<Object>, nil]
      # @see https://nikita-shkoda.mintlify.app/projects/search-engine-for-typesense/v30.1/materializers#pick
      def pick(*fields)
        SearchEngine::Hydration::Materializers.pick(self, *fields)
      end

      # Fetch and hydrate all matching records across all pages.
      # Performs a count first, then retrieves pages in batches via multi-search.
      # Warning: This can be memory and time intensive.
      # @return [Array<Object>]
      def all!
        SearchEngine::Hydration::Materializers.all!(self)
      end

      # Pluck one or multiple fields.
      # @param fields [Array<#to_sym,#to_s>]
      # @return [Array<Object>, Array<Array<Object>>]
      def pluck(*fields)
        SearchEngine::Hydration::Materializers.pluck(self, *fields)
      end

      # Whether any matching documents exist.
      # @return [Boolean]
      def exists?
        SearchEngine::Hydration::Materializers.exists?(self)
      end

      # Return total number of matching documents.
      # @return [Integer]
      def count
        SearchEngine::Hydration::Materializers.count(self)
      end

      # Return total number of pages for this relation based on total hits and per-page size.
      # @return [Integer]
      def pages_count
        SearchEngine::Hydration::Materializers.pages_count(self)
      end
    end
  end
end
