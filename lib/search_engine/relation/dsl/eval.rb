# frozen_string_literal: true

module SearchEngine
  class Relation
    module DSL
      # Typesense `_eval()` conditional sort expressions.
      # Mixed into Relation's DSL; preserves copy-on-write semantics.
      module Eval
        # Sort by a Typesense `_eval()` conditional expression.
        #
        # Accepts a plain filter-syntax string (simple form) or an Array of
        # `{ expr:, weight: }` hashes (weighted multi-expression form).
        #
        # @param expression [String, Array<Hash>] filter expression(s)
        # @param direction [Symbol] `:desc` (default, matches first) or `:asc`
        # @return [SearchEngine::Relation]
        def order_eval(expression, direction: :desc)
          dir = direction.to_s.downcase
          unless %w[asc desc].include?(dir)
            raise ArgumentError, "order_eval: direction must be :asc or :desc (got #{direction.inspect})"
          end

          sort_token = case expression
                       when String
                         raise ArgumentError, 'order_eval: expression must not be blank' if expression.strip.empty?

                         "_eval(#{expression}):#{dir}"
                       when Array
                         validate_weighted_expressions!(expression)
                         weighted = expression.map { |e| "(#{e[:expr]}):#{e[:weight]}" }.join(', ')
                         "_eval([ #{weighted} ]):#{dir}"
                       else
                         raise ArgumentError,
                               'order_eval: expression must be a String or Array of { expr:, weight: }'
                       end

          spawn do |s|
            existing = Array(s[:orders])
            s[:orders] = dedupe_orders_last_wins(existing + [sort_token])
          end
        end

        private

        def validate_weighted_expressions!(expressions)
          unless expressions.is_a?(Array) && !expressions.empty? && expressions.all? { |e| e.is_a?(Hash) }
            raise ArgumentError, 'order_eval: weighted form expects a non-empty Array of { expr:, weight: }'
          end

          expressions.each_with_index do |entry, i|
            unless entry[:expr].is_a?(String) && !entry[:expr].strip.empty?
              raise ArgumentError, "order_eval: entry #{i} must have a non-blank :expr"
            end
            unless entry[:weight].is_a?(Integer) && entry[:weight].positive?
              raise ArgumentError, "order_eval: entry #{i} :weight must be a positive Integer"
            end
          end
        end
      end
    end
  end
end
