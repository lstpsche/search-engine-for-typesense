# frozen_string_literal: true

require 'active_support/concern'

module SearchEngine
  class Base
    # Delegates class-level query methods to `.all` relation instance.
    module RelationDelegation
      extend ActiveSupport::Concern

      class_methods do
        # Return a fresh, immutable relation bound to this model class.
        # @return [SearchEngine::Relation]
        def all
          SearchEngine::Relation.new(self)
        end

        # Delegate materializers and query dsl to `.all` so callers can do `Model.first` etc.
        %i[
          where rewhere merge order preset ranking prefix search
          pin hide curate clear_curation
          facet_by facet_query group_by unscope
          limit offset page per_page per options cache
          joins use_synonyms use_stopwords
          select include_fields exclude reselect
          limit_hits validate_hits!
          first last take pluck exists? count find_by all!
          delete_all update_all
          raw
        ].each { |method| delegate method, to: :all }

        # Find a record by its document id (model-level only).
        # Equivalent to `find_by(id: id)`.
        # @param id [#to_s] Document id
        # @return [Object, nil]
        def find(id)
          find_by(id: id)
        end
      end
    end
  end
end
