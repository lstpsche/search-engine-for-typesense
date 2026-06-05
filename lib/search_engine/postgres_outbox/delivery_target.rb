# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Generic destination for PostgreSQL outbox delivery processing.
    class DeliveryTarget
      # @return [String] stable target identifier stored in delivery rows
      attr_reader :key
      # @return [String] ActiveJob queue name used to process this target
      attr_reader :queue_name
      # @return [Integer] maximum concurrent drain jobs for this target
      attr_reader :parallelism

      # @param key [String, Symbol] stable target identifier
      # @param queue_name [String, Symbol] queue name for target-specific drain jobs
      # @param parallelism [Integer, #to_i, nil] optional target concurrency cap
      def initialize(key:, queue_name:, parallelism: nil)
        @key = normalize_value(key, 'key')
        @queue_name = normalize_value(queue_name, 'queue_name')
        @parallelism = normalize_parallelism(parallelism)
      end

      # Normalize a configured target into a DeliveryTarget.
      #
      # @param value [DeliveryTarget, Hash, #key] target-like object
      # @return [DeliveryTarget]
      def self.normalize(value)
        return value if value.is_a?(self)

        if value.respond_to?(:to_hash)
          hash = value.to_hash
          return new(
            key: fetch_hash_value(hash, :key),
            queue_name: fetch_hash_value(hash, :queue_name),
            parallelism: fetch_hash_value(hash, :parallelism)
          )
        end

        if value.respond_to?(:key) && value.respond_to?(:queue_name)
          parallelism = value.parallelism if value.respond_to?(:parallelism)
          return new(key: value.key, queue_name: value.queue_name, parallelism: parallelism)
        end

        raise ArgumentError, 'delivery target must be a DeliveryTarget, Hash, or target-like object'
      end

      class << self
        private

        def fetch_hash_value(hash, key)
          return hash[key] if hash.key?(key)

          hash[key.to_s]
        end
      end

      private

      def normalize_value(value, name)
        normalized = value.to_s
        raise ArgumentError, "#{name} must be present" if normalized.strip.empty?

        normalized
      end

      def normalize_parallelism(value)
        normalized = value.nil? ? SearchEngine.config.postgres_outbox.drain_target_parallelism : value
        [normalized.to_i, 1].max
      end
    end
  end
end
