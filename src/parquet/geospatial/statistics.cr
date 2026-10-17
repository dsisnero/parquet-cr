require "./bounding_box"

# Ported from `parquet/src/geospatial/statistics.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `GeospatialStatistics`: the bounding box and geometry-type list attached
# to a column chunk of a GEOMETRY or GEOGRAPHY column.
#
# Divergence: `HeapSize` has no analogue and is dropped.
module Parquet
  module Geospatial
    # Geospatial statistics for a Parquet column or dataset.
    class GeospatialStatistics
      # Optional bounding box defining the spatial extent, where `nil`
      # represents a lack of information.
      getter bounding_box : BoundingBox?

      # Optional list of geometry type identifiers, where `nil` represents a
      # lack of information.
      getter geospatial_types : Array(Int32)?

      def initialize(@bounding_box : BoundingBox? = nil, @geospatial_types : Array(Int32)? = nil)
      end

      def ==(other : GeospatialStatistics) : Bool
        bounding_box == other.bounding_box &&
          geospatial_types == other.geospatial_types
      end
    end
  end
end
