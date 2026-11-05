# frozen_string_literal: true

require 'digest'
require 'json'

module SearchEngine
  module PostgresOutbox
    # Read under the outbox materialization lock. One page follows new event IDs;
    # the other sweeps backwards to revisit late commits, due retries and existing events.
    class MaterializationCursor
      PAGE_SIZE = 1000

      def initialize(connection, events_table, cursor_table, target_keys)
        @connection = connection
        @events = events_table
        @table = connection.quote_table_name(cursor_table)
        @scope = connection.quote(Digest::SHA256.hexdigest(JSON.generate([events_table, target_keys.uniq.sort])))
      end

      # @param limit [Integer] upper bound on the number of event IDs to read in each direction
      # @return [Array<Integer>] event IDs to check against the configured delivery targets
      def candidate_ids(limit:)
        @page_size = [PAGE_SIZE, limit].min
        state = @connection.select_all(<<~SQL).first
          SELECT forward_id, backfill_through_id FROM #{@table} WHERE target_scope = #{@scope}
        SQL
        if state
          @forward_id = Integer(state.fetch('forward_id'))
          @backfill_through_id = Integer(state.fetch('backfill_through_id'))
        else
          @forward_id, @backfill_through_id = initialize_positions
        end

        # Recheck a recent ID range for concurrent inserts that commit behind the watermark.
        # Even for dense IDs, at least half of the page remains available for new events.
        @forward_page = event_ids("id > #{@forward_id - @page_size / 2} ORDER BY id ASC")
        @backfill_page = event_ids("id <= #{@backfill_through_id} ORDER BY id DESC")
        @forward_page | @backfill_page
      end

      # Save progress in the same transaction as the new deliveries.
      def advance!
        forward = [@forward_page.last || @forward_id, @forward_id].max
        # Freeze the upper boundary until the backwards sweep reaches the beginning.
        backfill = @backfill_page.size < @page_size ? forward : @backfill_page.last - 1
        return if forward == @forward_id && backfill == @backfill_through_id

        @connection.execute(<<~SQL)
          UPDATE #{@table} SET forward_id = #{forward}, backfill_through_id = #{backfill}
          WHERE target_scope = #{@scope}
        SQL
      end

      private

      def initialize_positions
        head = Integer(@connection.select_value("SELECT COALESCE(MAX(id), 0) FROM #{@events}"))
        @connection.execute(<<~SQL)
          INSERT INTO #{@table} (target_scope, forward_id, backfill_through_id)
          VALUES (#{@scope}, #{head}, #{head})
        SQL
        [head, head]
      end

      def event_ids(order)
        @connection.select_all("SELECT id FROM #{@events} WHERE #{order} LIMIT #{@page_size}").map do |row|
          Integer(row.fetch('id'))
        end
      end
    end
  end
end
