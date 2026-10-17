require "./geospatial/bounding_box"
require "./geospatial/statistics"
require "./geospatial/accumulator"

# Ported from `parquet/src/geospatial/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Core geospatial support for Parquet: `GeospatialStatistics` (the thrift column
# chunk metadata) and `BoundingBox`, plus the accumulator that computes them
# while writing. The separate `parquet-geospatial` crate lives in
# `src/parquet_geospatial/`.
module Parquet
  # Geospatial data support for Parquet files, as defined in the parquet format
  # [spec](https://github.com/apache/parquet-format/blob/master/Geospatial.md).
  module Geospatial
  end
end
