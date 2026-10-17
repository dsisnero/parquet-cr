require "../basic"
require "../schema/types"
require "../../parquet_geospatial"
require "./statistics"

# Ported from `parquet/src/geospatial/accumulator.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `try_new_geo_stats_accumulator`, `init_geo_stats_accumulator_factory`,
# `GeoStatsAccumulatorFactory`, `GeoStatsAccumulator`,
# `DefaultGeoStatsAccumulatorFactory`, `VoidGeoStatsAccumulator`, and
# `ParquetGeoStatsAccumulator`.
#
# Divergences: the Rust `OnceLock<Arc<dyn GeoStatsAccumulatorFactory>>` global
# becomes a module class variable (not thread-safe; initialization is still
# once-only and `init_geo_stats_accumulator_factory` raises when the global was
# already initialized, matching upstream `ParquetError::General`); the traits are
# abstract classes; `Box<GeospatialStatistics>` is a plain reference. The
# `geospatial` cargo feature is always on, so the factory always builds a
# `ParquetGeoStatsAccumulator` for Geometry columns and a
# `VoidGeoStatsAccumulator` otherwise. `ParquetGeoStatsAccumulator#update_wkb`
# catches `ParquetGeospatial::InvalidArgumentError` (the bounder's only error)
# where upstream tests `Result::is_err`.
module Parquet
  module Geospatial
    # The global accumulator factory instance.
    @@accumulator_factory : GeoStatsAccumulatorFactory? = nil

    # Creates a new `GeoStatsAccumulator` instance if `descr` represents a
    # Geometry or Geography `LogicalType`, returning `nil` otherwise.
    def self.try_new_geo_stats_accumulator(descr : Schema::ColumnDescriptor) : GeoStatsAccumulator?
      logical_type = descr.logical_type_ref
      unless logical_type.is_a?(LogicalType::Geometry) || logical_type.is_a?(LogicalType::Geography)
        return nil
      end

      accumulator_factory.new_accumulator(descr)
    end

    # The global `GeoStatsAccumulatorFactory`, initialized with the default
    # factory on first use (upstream `OnceLock::get_or_init`).
    def self.accumulator_factory : GeoStatsAccumulatorFactory
      @@accumulator_factory ||= DefaultGeoStatsAccumulatorFactory.new
    end

    # Initializes the global `GeoStatsAccumulatorFactory`.
    #
    # This may only be done once before any call to
    # `try_new_geo_stats_accumulator`; a second initialization raises.
    def self.init_geo_stats_accumulator_factory(factory : GeoStatsAccumulatorFactory) : Nil
      unless @@accumulator_factory.nil?
        raise general_err!("Global GeoStatsAccumulatorFactory already set")
      end

      @@accumulator_factory = factory
      nil
    end

    # Factory for `GeospatialStatistics` accumulators.
    abstract class GeoStatsAccumulatorFactory
      # Creates a new `GeoStatsAccumulator` appropriate for the logical type of
      # `descr`.
      abstract def new_accumulator(descr : Schema::ColumnDescriptor) : GeoStatsAccumulator
    end

    # Dynamic `GeospatialStatistics` accumulator.
    #
    # Implementors ingest the (non-null) elements of a column and return
    # compliant `GeospatialStatistics` (or `nil`).
    abstract class GeoStatsAccumulator
      # Returns `true` if this instance can return `GeospatialStatistics` from
      # `finish`.
      #
      # This returns `false` when built from the `VoidGeoStatsAccumulator`, or if
      # the accumulator encountered invalid or unsupported elements for which it
      # cannot compute valid statistics.
      abstract def is_valid : Bool

      # Updates this accumulator with a single WKB-encoded value.
      #
      # This method is infallible; in the event of improperly encoded values,
      # implementations must ensure that `finish` returns `nil`.
      abstract def update_wkb(wkb : Bytes) : Nil

      # Computes the final statistics and resets internal state.
      abstract def finish : GeospatialStatistics?
    end

    # The default accumulator factory for `GeospatialStatistics`.
    #
    # Geometry columns get a `ParquetGeoStatsAccumulator` so they are written
    # with statistics when statistics for that column are enabled. Geography
    # bounding is not currently implemented by `parquet-geospatial`, so those
    # columns always get a `VoidGeoStatsAccumulator`.
    class DefaultGeoStatsAccumulatorFactory < GeoStatsAccumulatorFactory
      def new_accumulator(descr : Schema::ColumnDescriptor) : GeoStatsAccumulator
        if descr.logical_type_ref.is_a?(LogicalType::Geometry)
          ParquetGeoStatsAccumulator.new
        else
          VoidGeoStatsAccumulator.new
        end
      end
    end

    # A `GeoStatsAccumulator` that never computes any `GeospatialStatistics`.
    class VoidGeoStatsAccumulator < GeoStatsAccumulator
      def is_valid : Bool
        false
      end

      def update_wkb(wkb : Bytes) : Nil
        nil
      end

      def finish : GeospatialStatistics?
        nil
      end
    end

    # A `GeoStatsAccumulator` that uses the `parquet-geospatial` crate to
    # compute Geometry statistics.
    #
    # This accumulator only supports Geometry types and returns invalid
    # statistics for non-point Geography input
    # (`GeoStatsAccumulatorFactory#new_accumulator` is responsible for ensuring a
    # suitable accumulator for a given logical type).
    class ParquetGeoStatsAccumulator < GeoStatsAccumulator
      def initialize
        @bounder = ::ParquetGeospatial::GeometryBounder.empty
        @invalid = false
      end

      def is_valid : Bool
        !@invalid
      end

      def update_wkb(wkb : Bytes) : Nil
        @bounder.update_wkb(wkb)
      rescue ::ParquetGeospatial::InvalidArgumentError
        @invalid = true
        nil
      end

      def finish : GeospatialStatistics?
        if @invalid
          # Reset
          @invalid = false
          @bounder = ::ParquetGeospatial::GeometryBounder.empty
          return nil
        end

        bbox =
          if @bounder.x.empty? || @bounder.y.empty?
            nil
          else
            out = BoundingBox.new(
              @bounder.x.lo,
              @bounder.x.hi,
              @bounder.y.lo,
              @bounder.y.hi,
            )

            out = out.with_zrange(@bounder.z.lo, @bounder.z.hi) unless @bounder.z.empty?
            out = out.with_mrange(@bounder.m.lo, @bounder.m.hi) unless @bounder.m.empty?
            out
          end

        bounder_geometry_types = @bounder.geometry_types
        geometry_types = bounder_geometry_types.empty? ? nil : bounder_geometry_types

        # Reset
        @bounder = ::ParquetGeospatial::GeometryBounder.empty

        GeospatialStatistics.new(bbox, geometry_types)
      end
    end
  end
end
