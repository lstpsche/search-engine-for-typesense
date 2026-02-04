# frozen_string_literal: true

require 'active_support/concern'

module SearchEngine
  # ActiveRecord concern to keep a Typesense collection in sync.
  #
  # Include into an AR model and call {.search_engine_syncable} to install
  # lifecycle callbacks that upsert on create/update and delete on destroy.
  #
  # @example
  #   class Product < ApplicationRecord
  #     include SearchEngine::ActiveRecordSyncable
  #     search_engine_syncable on: %i[create update destroy], collection: :products
  #   end
  module ActiveRecordSyncable
    extend ActiveSupport::Concern

    class_methods do
      # Configure SearchEngine synchronization for this ActiveRecord model.
      #
      # - collection: defaults to the AR class tableized name (snake_case, plural)
      # - on: one or many of :create, :update, :destroy (strings or symbols)
      #
      # Validates that either a physical Typesense collection exists for the
      # given name or a SearchEngine model is registered for it. Mapping for
      # create/update requires a SearchEngine model; when missing, an error is
      # raised with guidance.
      #
      # @param collection [Symbol, String, nil]
      # @param on [Array<Symbol,String>, Symbol, String, nil]
      # @return [Class] self (for macro chaining)
      def search_engine_syncable(collection: nil, on: nil)
        effective_actions = on

        cfg = SearchEngine::ActiveRecordSyncable.__normalize_config_for(
          self,
          collection: collection,
          actions: effective_actions
        )

        # Store config on the AR class (used by instance methods)
        instance_variable_set(:@__se_syncable_cfg__, cfg)

        SearchEngine::ActiveRecordSyncable.__register_callbacks_for(self, cfg)
        self
      end
    end

    # Upsert this record into the configured Typesense collection using the
    # mapping defined on the corresponding SearchEngine model.
    # @return [void]
    def __se_syncable_upsert!
      cfg = self.class.instance_variable_get(:@__se_syncable_cfg__) || {}
      se_klass = cfg[:se_klass]
      unless se_klass
        # Lazy-resolve the SearchEngine model to avoid boot-time ordering issues
        begin
          se_klass = SearchEngine.collection_for(cfg[:logical])
          cfg[:se_klass] = se_klass if se_klass
        rescue StandardError
          se_klass = nil
        end
      end
      unless se_klass
        SearchEngine.config.logger&.warn(
          "search_engine_syncable: no SearchEngine model registered for '#{cfg[:logical]}'; skip upsert"
        )
        return
      end

      se_klass.upsert(record: self)
    rescue StandardError => error
      SearchEngine.config.logger&.error("search_engine_syncable upsert failed: #{error}")
    end

    # Delete this record's document from the configured Typesense collection.
    # Uses the SearchEngine model's identity strategy when available.
    # @return [void]
    def __se_syncable_delete!
      cfg = self.class.instance_variable_get(:@__se_syncable_cfg__) || {}
      logical = cfg[:logical]
      se_klass = cfg[:se_klass]

      document_id = if se_klass
                      se_klass.compute_document_id(self)
                    else
                      respond_to?(:id) ? id.to_s : nil
                    end

      if document_id.nil? || document_id.strip.empty?
        SearchEngine.config.logger&.warn(
          "search_engine_syncable: cannot delete without id for '#{logical}'"
        )
        return
      end

      client = SearchEngine.client
      into = client.resolve_alias(logical) || logical
      client.delete_document(collection: into, id: document_id)
    rescue StandardError => error
      SearchEngine.config.logger&.error("search_engine_syncable delete failed: #{error}")
    end

    # Return the associated SearchEngine record for this ActiveRecord instance.
    #
    # Resolves the SearchEngine model lazily when necessary and computes the
    # document id via the model's `identify_by` strategy. Returns nil when the
    # model mapping is unavailable or the id cannot be determined.
    #
    # @return [Object, nil] hydrated SearchEngine model instance or nil when not found
    def search_engine_record
      cfg = self.class.instance_variable_get(:@__se_syncable_cfg__) || {}
      se_klass = cfg[:se_klass]
      if se_klass.nil?
        begin
          logical = cfg[:logical] || self.class.name.to_s
          se_klass = SearchEngine.collection_for(logical)
          cfg[:se_klass] = se_klass if se_klass
        rescue StandardError
          return nil
        end
      end

      doc_id = begin
        se_klass.compute_document_id(self)
      rescue StandardError => error
        # When a custom identify_by is configured on the SearchEngine model,
        # do not fall back to the ActiveRecord id on computation errors as it
        # may point to a different document. Only fall back to AR id when
        # identify_by is not defined at all.
        if se_klass.instance_variable_defined?(:@identify_by_proc)
          SearchEngine.config.logger&.warn(
            "search_engine_syncable: identify_by failed to compute id for '#{cfg[:logical]}' (#{error.class})"
          )
          nil
        else
          respond_to?(:id) ? id.to_s : nil
        end
      end
      return nil if doc_id.nil? || doc_id.to_s.strip.empty?

      se_klass.find(doc_id)
    rescue StandardError
      nil
    end

    # Map this ActiveRecord instance using the associated SearchEngine model and
    # upsert it into the collection.
    #
    # @return [Integer] number of upserted documents (0 or 1)
    def sync_search_engine_record
      cfg = self.class.instance_variable_get(:@__se_syncable_cfg__) || {}
      se_klass = cfg[:se_klass]
      if se_klass.nil?
        begin
          logical = cfg[:logical] || self.class.name.to_s
          se_klass = SearchEngine.collection_for(logical)
          cfg[:se_klass] = se_klass if se_klass
        rescue StandardError => error
          SearchEngine.config.logger&.warn(
            "search_engine_syncable: cannot resolve model for sync (#{error.class})"
          )
          return 0
        end
      end

      se_klass.upsert(record: self)
    rescue StandardError => error
      SearchEngine.config.logger&.error("search_engine_syncable sync failed: #{error}")
      0
    end

    module_function

    # @api private
    # @param ar_klass [Class]
    # @param collection [String,Symbol,nil]
    # @param actions [Array<String,Symbol>, String, Symbol, nil]
    # @return [Hash]
    def __normalize_config_for(ar_klass, collection:, actions:)
      require 'active_support/inflector'

      logical = (collection || ActiveSupport::Inflector.tableize(ar_klass.name)).to_s
      normalized_actions = __normalize_actions(actions)

      # Best-effort resolve SearchEngine model now; fall back to lazy resolution
      se_klass = begin
        SearchEngine.collection_for(logical)
      rescue StandardError
        nil
      end

      if se_klass.nil? && (normalized_actions.include?(:create) || normalized_actions.include?(:update))
        SearchEngine.config.logger&.warn(
          "search_engine_syncable: mapping for '#{logical}' not found at boot; will resolve lazily at runtime"
        )
      end

      {
        logical: logical,
        actions: normalized_actions,
        se_klass: se_klass
      }
    end

    # @api private
    # @param actions [Array<String,Symbol>, String, Symbol, nil]
    # @return [Array<Symbol>]
    def __normalize_actions(actions)
      allowed = %i[create update destroy]
      list =
        if actions.nil?
          allowed
        else
          Array(actions).map { |a| a.to_s.downcase.strip.to_sym }
        end

      invalid = list - allowed
      raise ArgumentError, "search_engine_syncable: actions must be within #{allowed.inspect}" unless invalid.empty?

      list.uniq
    end

    # (no-op placeholder kept for backwards compatibility of method table in case of reloads)

    # @api private
    # @param ar_klass [Class]
    # @param cfg [Hash]
    # @return [void]
    def __register_callbacks_for(ar_klass, cfg)
      if ar_klass.instance_variable_defined?(:@__se_syncable_callbacks_installed__) &&
         ar_klass.instance_variable_get(:@__se_syncable_callbacks_installed__)
        return
      end

      actions = cfg[:actions]

      ar_klass.after_create :__se_syncable_upsert! if actions.include?(:create)
      ar_klass.after_update :__se_syncable_upsert! if actions.include?(:update)
      ar_klass.after_destroy :__se_syncable_delete! if actions.include?(:destroy)

      ar_klass.instance_variable_set(:@__se_syncable_callbacks_installed__, true)
      nil
    end
  end
end
