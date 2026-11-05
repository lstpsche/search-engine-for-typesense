# frozen_string_literal: true

require 'active_support/concern'
require 'active_support/core_ext/class/attribute'
require 'active_support/inflector'

module SearchEngine
  # ActiveRecord concern to keep a Typesense collection in sync.
  #
  # Include into an AR model and call {.search_engine_syncable} to store sync
  # metadata. The default callback strategy upserts on create/update and deletes
  # on destroy. The postgres_outbox strategy stores metadata without installing
  # callbacks. Subclasses inherit the configuration unless they declare their own.
  #
  # @example
  #   class Product < ApplicationRecord
  #     include SearchEngine::ActiveRecordSyncable
  #     search_engine_syncable on: %i[create update destroy], collection: :products
  #   end
  module ActiveRecordSyncable
    extend ActiveSupport::Concern

    # Rails deduplicates after_commit filters by method name, even for separate actions.
    CALLBACKS = {
      create: :__se_syncable_create!,
      update: :__se_syncable_upsert!,
      destroy: :__se_syncable_delete!
    }.freeze
    private_constant :CALLBACKS

    included do
      class_attribute :_se_syncable_config, instance_writer: false, instance_predicate: false
    end

    class_methods do
      # Configure SearchEngine synchronization for this model and its subclasses.
      # SearchEngine models can register after this declaration.
      #
      # @param collection [Symbol, String, nil] defaults to the tableized model name
      # @param on [Array<Symbol,String>, Symbol, String, nil] one or more of create, update and destroy
      # @param sync_strategy [Symbol, String, nil] callbacks (default) or postgres_outbox
      # @return [Class] self
      def search_engine_syncable(collection: nil, on: nil, sync_strategy: :callbacks)
        previous = _se_syncable_config
        config = {
          logical: (collection || ActiveSupport::Inflector.tableize(name)).to_s.dup.freeze,
          actions: SearchEngine::ActiveRecordSyncable.__normalize_actions(on).freeze,
          sync_strategy: SearchEngine::ActiveRecordSyncable.__normalize_sync_strategy(sync_strategy),
          timing: SearchEngine.config.syncable_callback_timing
        }.freeze
        self._se_syncable_config = config
        if config[:sync_strategy] == :callbacks && config != previous
          SearchEngine::ActiveRecordSyncable.__register_callbacks_for(self, config)
        end
        self
      end
    end

    # @api private
    def __se_syncable_create!
      __se_syncable_upsert!
    end

    # Upsert this record using the configured SearchEngine model.
    # @return [void]
    def __se_syncable_upsert!
      sync_search_engine_record
    end

    # Delete the mapped document. Collections without a model use the ActiveRecord primary key.
    # @return [void]
    def __se_syncable_delete!
      logical = __se_syncable_logical
      model = __se_syncable_model(logical)
      document_id = model ? model.compute_document_id(self) : id.to_s
      if document_id.blank?
        SearchEngine.config.logger&.warn("search_engine_syncable: cannot delete without id for '#{logical}'")
        return
      end

      client = SearchEngine.client
      into = client.resolve_alias(logical) || logical
      client.delete_document(collection: into, id: document_id)
    rescue StandardError => error
      SearchEngine.config.logger&.error("search_engine_syncable delete failed: #{error}")
    end

    # Return the associated SearchEngine record using the current model and identity strategy.
    # @return [Object, nil] hydrated SearchEngine model instance or nil when unavailable
    def search_engine_record
      model = __se_syncable_model
      return unless model

      document_id = model.compute_document_id(self)
      model.find(document_id) if document_id.present?
    rescue StandardError => error
      SearchEngine.config.logger&.error("search_engine_syncable lookup failed: #{error}")
      nil
    end

    # Map this ActiveRecord instance using the associated SearchEngine model and
    # upsert it into the collection.
    # @return [Integer] number of upserted documents (0 or 1)
    def sync_search_engine_record
      model = __se_syncable_model
      return model.upsert(record: self) if model

      SearchEngine.config.logger&.warn(
        "search_engine_syncable: no SearchEngine model registered for '#{__se_syncable_logical}'; skip upsert"
      )
      0
    rescue StandardError => error
      SearchEngine.config.logger&.error("search_engine_syncable sync failed: #{error}")
      0
    end

    private

    def __se_syncable_model(logical = __se_syncable_logical)
      SearchEngine.collection_for(logical) { nil }
    end

    def __se_syncable_logical
      _se_syncable_config ? _se_syncable_config.fetch(:logical) : self.class.name.to_s
    end

    module_function

    # @api private
    def __normalize_actions(actions)
      allowed = CALLBACKS.keys
      list = actions.nil? ? allowed : Array(actions).map { |action| action.to_s.downcase.strip.to_sym }
      invalid = list - allowed
      raise ArgumentError, "search_engine_syncable: actions must be within #{allowed.inspect}" unless invalid.empty?

      list.uniq
    end

    # @api private
    def __normalize_sync_strategy(strategy)
      allowed = %i[callbacks postgres_outbox]
      normalized = strategy.nil? ? :callbacks : strategy.to_s.downcase.strip.to_sym

      unless allowed.include?(normalized)
        raise ArgumentError, "search_engine_syncable: sync_strategy must be within #{allowed.inspect}"
      end

      normalized
    end

    # @api private
    def __register_callbacks_for(ar_klass, config)
      timing = config.fetch(:timing)
      config.fetch(:actions).each do |action|
        # An inherited callback can be narrowed or disabled by a subclass declaration.
        enabled = lambda do
          current = _se_syncable_config
          current[:sync_strategy] == :callbacks && current[:timing] == timing && current[:actions].include?(action)
        end
        method = CALLBACKS.fetch(action)
        if timing == :after_commit
          ar_klass.after_commit method, on: action, if: enabled
        else
          ar_klass.public_send("after_#{action}", method, if: enabled)
        end
      end
    end
  end
end
