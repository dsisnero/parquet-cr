# Ported from `parquet/src/geospatial/bounding_box.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `BoundingBox`: the min/max value pair of coordinates from each axis of a
# GEOMETRY or GEOGRAPHY column, as defined by the parquet format
# [spec](https://github.com/apache/parquet-format/blob/master/Geospatial.md).
#
# Divergences: the Rust `(f64, f64)` range tuples become scalar `min`/`max`
# getters, the consuming builders (`with_xrange` etc.) mutate and return `self`
# (Rust takes `mut self`), and native `HeapSize` lives in file/metadata/memory.cr.
module Parquet
  module Geospatial
    # The bounding box of a geospatial column: X and Y ranges are always set,
    # while Z (elevation) and M (linear reference) ranges are optional.
    struct BoundingBox
      # X coordinates (longitude or easting): min and max.
      getter xmin : Float64
      getter xmax : Float64
      # Y coordinates (latitude or northing): min and max.
      getter ymin : Float64
      getter ymax : Float64
      # Z coordinates (elevation/height): min and max, if present.
      getter zmin : Float64?
      getter zmax : Float64?
      # M coordinates (measured value): min and max, if present.
      getter mmin : Float64?
      getter mmax : Float64?

      # Creates a new bounding box with the specified 2D coordinates.
      def initialize(@xmin : Float64, @xmax : Float64, @ymin : Float64, @ymax : Float64)
        @zmin = nil
        @zmax = nil
        @mmin = nil
        @mmax = nil
      end

      # Updates the bounding box with the specified X-coordinate range.
      def with_xrange(xmin : Float64, xmax : Float64) : BoundingBox
        @xmin = xmin
        @xmax = xmax
        self
      end

      # Updates the bounding box with the specified Y-coordinate range.
      def with_yrange(ymin : Float64, ymax : Float64) : BoundingBox
        @ymin = ymin
        @ymax = ymax
        self
      end

      # Updates the bounding box with the specified Z-coordinate range.
      def with_zrange(zmin : Float64, zmax : Float64) : BoundingBox
        @zmin = zmin
        @zmax = zmax
        self
      end

      # Updates the bounding box with the specified M-coordinate range.
      def with_mrange(mmin : Float64, mmax : Float64) : BoundingBox
        @mmin = mmin
        @mmax = mmax
        self
      end

      # Returns `true` if both zmin and zmax are present.
      # ameba:disable Naming/PredicateName
      def is_z_valid : Bool
        !@zmin.nil?
      end

      # Returns `true` if both mmin and mmax are present.
      # ameba:disable Naming/PredicateName
      def is_m_valid : Bool
        !@mmin.nil?
      end
    end
  end
end
