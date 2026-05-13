# frozen_string_literal: true

module SearchEngine
  class Relation
    module DSL
      # Geo search chainers: filtering by radius/polygon and (future) geo sorting.
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

        private

        def validate_geo_field!(field)
          attrs = safe_attributes_map
          return unless attrs && !attrs.empty?

          type = attrs[field.to_sym]
          return if [:geopoint, [:geopoint]].include?(type)

          raise ArgumentError,
                "where_geo: field :#{field} must be declared as :geopoint or [:geopoint]"
        end

        def validate_geo_predicate_exclusivity!(within_radius, within_polygon)
          if within_radius.nil? && within_polygon.nil?
            raise ArgumentError, 'where_geo: provide either within_radius: or within_polygon:'
          end
          return unless within_radius && within_polygon

          raise ArgumentError, 'where_geo: within_radius: and within_polygon: are mutually exclusive'
        end

        def validate_geo_coordinate!(lat, lng)
          unless lat.is_a?(Numeric) && lat >= -90 && lat <= 90
            raise ArgumentError, "where_geo: lat must be a number in [-90, 90] (got #{lat.inspect})"
          end
          return if lng.is_a?(Numeric) && lng >= -180 && lng <= 180

          raise ArgumentError, "where_geo: lng must be a number in [-180, 180] (got #{lng.inspect})"
        end

        def validate_radius!(radius)
          return if radius.is_a?(String) && radius.match?(/\A\d+(\.\d+)?\s*(km|mi)\z/)

          raise ArgumentError,
                "where_geo: radius must be a string like '10 km' or '5 mi' (got #{radius.inspect})"
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
      end
    end
  end
end
