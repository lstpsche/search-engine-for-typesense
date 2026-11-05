# frozen_string_literal: true

module SearchEngine
  module PostgresOutbox
    # Operational reads must observe current delivery state and acquire row locks on each poll.
    module DatabaseQueries
      private

      def select_rows(sql)
        result = connection.respond_to?(:exec_query) ? connection.exec_query(sql) : connection.select_all(sql)
        return result.to_a if result.respond_to?(:to_a)

        Array(result)
      end
    end
  end
end
