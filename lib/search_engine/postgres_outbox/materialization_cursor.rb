# frozen_string_literal: true

require 'digest'
require 'json'

module SearchEngine
  module PostgresOutbox
    # Scan under the materialization lock. Keep moving through history even when new IDs arrive faster.
    class MaterializationCursor
      include DatabaseQueries

      PAGE_SIZE = 1000

      attr_reader :checkpoint_ids

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
        state = select_rows(<<~SQL).first
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
        # A returning target can be far behind. Reserve some of the second page for the current head.
        @recent_page = if @forward_page.size == @page_size
                         event_ids("id > #{@forward_page.last} ORDER BY id DESC", [@page_size / 2, 1].max)
                       else
                         []
                       end
        @backfill_limit = [@page_size - @recent_page.size, 1].max
        @backfill_page = event_ids("id <= #{@backfill_through_id} ORDER BY id DESC", @backfill_limit)
        @checkpoint_ids = @forward_page | @backfill_page
        @checkpoint_ids | @recent_page
      end

      # Save progress in the same transaction as the new deliveries.
      def advance!
        forward = [@forward_page.last || @forward_id, @forward_id].max
        # Freeze the upper boundary until the backwards sweep reaches the beginning.
        head = [forward, @recent_page.first || forward].max
        backfill = @backfill_page.size < @backfill_limit ? head : @backfill_page.last - 1
        return if forward == @forward_id && backfill == @backfill_through_id

        connection.execute(<<~SQL)
          UPDATE #{@table} SET forward_id = #{forward}, backfill_through_id = #{backfill}
          WHERE target_scope = #{@scope}
        SQL
      end

      private

      attr_reader :connection

      def initialize_positions
        head = Integer(select_rows("SELECT COALESCE(MAX(id), 0) AS head FROM #{@events}").first.fetch('head'))
        connection.execute(<<~SQL)
          INSERT INTO #{@table} (target_scope, forward_id, backfill_through_id)
          VALUES (#{@scope}, #{head}, #{head})
        SQL
        [head, head]
      end

      def event_ids(order, limit = @page_size)
        select_rows("SELECT id FROM #{@events} WHERE #{order} LIMIT #{limit}").map do |row|
          Integer(row.fetch('id'))
        end
      end
    end
  end
end
