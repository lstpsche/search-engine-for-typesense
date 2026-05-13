# frozen_string_literal: true

module SearchEngine
  class Relation
    module DSL
      # Geo search chainers: filtering by radius/polygon, geo distance sorting.
      # Mixed into Relation's DSL; preserves copy-on-write semantics.
      module Geo
        # Filter by geographic proximity (radius) or containment (polygon).
        #
        # @param field [Symbol, String] a `:geopoint` or `[:geopoint]` attribute
        # @param within_radius [Hash, nil] `{ lat:, lng:, radius: "10 km" }`
        # @param within_polygon [Array<Array(Numeric,Numeric)>, nil] three or more `[lat, lng]` pairs
        # @return [SearchEngine::Relation]
        def where_geo(field, within_radius: nil, within_polygon: nil)
          validate_geo_field!(field)
          validate_geo_predicate_exclusivity!(within_radius, within_polygon)

          fragment = if within_radius
                       build_radius_filter(field, within_radius)
                     else
                       build_polygon_filter(field, within_polygon)
                     end

          spawn do |s|
            s[:ast] = Array(s[:ast]) + [SearchEngine::AST.raw(fragment)]
            s[:filters] = Array(s[:filters])
          end
        end

        # Sort by geographic distance from a reference point.
        #
        # @param field [Symbol, String] a `:geopoint` or `[:geopoint]` attribute
        # @param from [Hash] `{ lat:, lng: }` — the reference point
        # @param direction [Symbol] `:asc` (nearest first, default) or `:desc`
        # @param exclude_radius [String, nil] e.g. `"2 km"` — exclude results within this radius from distance scoring
        # @param precision [String, nil] e.g. `"1 km"` — bucket precision for distance sort
        # @return [SearchEngine::Relation]
        def order_geo(field, from:, direction: :asc, exclude_radius: nil, precision: nil)
          validate_geo_field!(field, context: 'order_geo')

          lat = from[:lat]
          lng = from[:lng]
          validate_geo_coordinate!(lat, lng, context: 'order_geo')

          dir = direction.to_s.downcase
          unless %w[asc desc].include?(dir)
            raise ArgumentError, "order_geo: direction must be :asc or :desc (got #{direction.inspect})"
          end

          sort_token = build_geo_sort_token(field, lat, lng, dir, exclude_radius, precision)

          spawn do |s|
            existing = Array(s[:orders])
            s[:orders] = dedupe_orders_last_wins(existing + [sort_token])
          end
        end

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

        def validate_geo_field!(field, context: 'where_geo')
          attrs = safe_attributes_map
          return unless attrs && !attrs.empty?

          type = attrs[field.to_sym]
          return if [:geopoint, [:geopoint]].include?(type)

          raise ArgumentError,
                "#{context}: field :#{field} must be declared as :geopoint or [:geopoint]"
        end

        def validate_geo_predicate_exclusivity!(within_radius, within_polygon)
          if within_radius.nil? && within_polygon.nil?
            raise ArgumentError, 'where_geo: provide either within_radius: or within_polygon:'
          end
          return unless within_radius && within_polygon

          raise ArgumentError, 'where_geo: within_radius: and within_polygon: are mutually exclusive'
        end

        def validate_geo_coordinate!(lat, lng, context: 'where_geo')
          unless lat.is_a?(Numeric) && lat >= -90 && lat <= 90
            raise ArgumentError, "#{context}: lat must be a number in [-90, 90] (got #{lat.inspect})"
          end
          return if lng.is_a?(Numeric) && lng >= -180 && lng <= 180

          raise ArgumentError, "#{context}: lng must be a number in [-180, 180] (got #{lng.inspect})"
        end

        def validate_radius!(radius, context: 'where_geo')
          return if radius.is_a?(String) && radius.match?(/\A\d+(\.\d+)?\s*(km|mi)\z/)

          raise ArgumentError,
                "#{context}: radius must be a string like '10 km' or '5 mi' (got #{radius.inspect})"
        end

        def build_radius_filter(field, opts)
          lat = opts[:lat]
          lng = opts[:lng]
          radius = opts[:radius]
          validate_geo_coordinate!(lat, lng)
          validate_radius!(radius)
          "#{field}:(#{lat}, #{lng}, #{radius})"
        end

        def build_polygon_filter(field, points)
          unless points.is_a?(Array) && points.size >= 3
            raise ArgumentError, "where_geo: polygon must have >= 3 points (got #{points&.size || 0})"
          end

          points.each_with_index do |point, i|
            unless point.is_a?(Array) && point.size == 2
              raise ArgumentError, "where_geo: polygon point #{i} must be [lat, lng]"
            end

            validate_geo_coordinate!(point[0], point[1])
          end

          coords = points.map { |p| "#{p[0]}, #{p[1]}" }.join(', ')
          "#{field}:(#{coords})"
        end

        def build_geo_sort_token(field, lat, lng, dir, exclude_radius, precision)
          parts = +"#{field}(#{lat}, #{lng}"
          if exclude_radius
            validate_radius!(exclude_radius, context: 'order_geo')
            parts << ", exclude_radius: #{exclude_radius}"
          end
          if precision
            validate_radius!(precision, context: 'order_geo')
            parts << ", precision: #{precision}"
          end
          "#{parts}):#{dir}"
        end

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
