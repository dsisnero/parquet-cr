require "../../spec_helper"

# Ported from `parquet/src/geospatial/accumulator.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: the upstream tests build column descriptors with
# `ColumnPath::new(vec![])` and encode points with `parquet_geospatial::testing`;
# both map directly (`ColumnPath.new([] of String)` and
# `ParquetGeospatial::Testing`). `init_geo_stats_accumulator_factory(..).is_err()`
# becomes `expect_raises`.

private alias Geo = Parquet::Geospatial
private alias BBox = Parquet::Geospatial::BoundingBox
private alias PLogical = Parquet::LogicalType
private alias PType = Parquet::Type
private alias Wkb = ParquetGeospatial::Testing

private def geo_descr(logical : PLogical? = nil) : Parquet::Schema::ColumnDescriptor
  builder = Parquet::Schema::Type.primitive_type_builder("geom", PType::BYTE_ARRAY)
  builder = builder.with_logical_type(logical) unless logical.nil?
  ty = builder.build
  Parquet::Schema::ColumnDescriptor.new(
    ty, 0_i16, 0_i16, Parquet::Schema::ColumnPath.new([] of String)
  )
end

private def stats_or_fail(stats : Geo::GeospatialStatistics?) : Geo::GeospatialStatistics
  stats || fail "expected geospatial statistics"
end

describe Parquet::Geospatial::GeoStatsAccumulator do
  # Upstream `test_void_accumulator`.
  it "never computes statistics with the void accumulator" do
    accumulator = Geo::VoidGeoStatsAccumulator.new
    accumulator.is_valid.should be_false
    accumulator.update_wkb(Bytes[0x01, 0x02, 0x03])
    accumulator.finish.should be_nil
  end

  # Upstream `test_default_accumulator_geospatial_factory`.
  it "selects accumulators by logical type" do
    # Check that we have a working accumulator for Geometry
    accumulator = Geo.try_new_geo_stats_accumulator(geo_descr(PLogical.geometry(nil)))
    accumulator.should_not be_nil
    accumulator = accumulator.not_nil!

    accumulator.is_valid.should be_true
    accumulator.update_wkb(Wkb.wkb_point_xy(1.0, 2.0))
    accumulator.update_wkb(Wkb.wkb_point_xy(11.0, 12.0))
    stats = stats_or_fail(accumulator.finish)
    stats.bounding_box.should eq(BBox.new(1.0, 11.0, 2.0, 12.0))

    # Check that we have a void accumulator for Geography
    accumulator = Geo.try_new_geo_stats_accumulator(
      geo_descr(PLogical.geography(nil, nil))
    )
    accumulator.should_not be_nil
    accumulator = accumulator.not_nil!

    accumulator.is_valid.should be_false
    accumulator.finish.should be_nil

    # Check that we return `nil` if the type is not geometry or geography
    Geo.try_new_geo_stats_accumulator(geo_descr).should be_nil

    # We should not be able to initialize a global accumulator after we've
    # initialized at least one accumulator.
    expect_raises(Parquet::GeneralError, "already set") do
      Geo.init_geo_stats_accumulator_factory(Geo::DefaultGeoStatsAccumulatorFactory.new)
    end
  end

  # Upstream `test_geometry_accumulator`.
  it "accumulates geometry statistics and resets between chunks" do
    accumulator = Geo::ParquetGeoStatsAccumulator.new

    # A fresh instance should be able to bound input
    accumulator.is_valid.should be_true
    accumulator.update_wkb(Wkb.wkb_point_xy(1.0, 2.0))
    accumulator.update_wkb(Wkb.wkb_point_xy(11.0, 12.0))
    stats = stats_or_fail(accumulator.finish)
    stats.geospatial_types.should eq([1])
    stats.bounding_box.should eq(BBox.new(1.0, 11.0, 2.0, 12.0))

    # finish() should have reset the bounder such that the first values
    # aren't when computing the next bound of statistics.
    accumulator.is_valid.should be_true
    accumulator.update_wkb(Wkb.wkb_point_xy(21.0, 22.0))
    accumulator.update_wkb(Wkb.wkb_point_xy(31.0, 32.0))
    stats = stats_or_fail(accumulator.finish)
    stats.geospatial_types.should eq([1])
    stats.bounding_box.should eq(BBox.new(21.0, 31.0, 22.0, 32.0))

    # When an accumulator encounters invalid input, it reports is_valid() false
    # and does not compute subsequent statistics
    accumulator.is_valid.should be_true
    accumulator.update_wkb(Wkb.wkb_point_xy(41.0, 42.0))
    accumulator.update_wkb("these bytes are not WKB".to_slice)
    accumulator.is_valid.should be_false
    accumulator.finish.should be_nil

    # Subsequent rounds of accumulation should work as expected
    accumulator.is_valid.should be_true
    accumulator.update_wkb(Wkb.wkb_point_xy(41.0, 42.0))
    accumulator.update_wkb(Wkb.wkb_point_xy(51.0, 52.0))
    stats = stats_or_fail(accumulator.finish)
    stats.geospatial_types.should eq([1])
    stats.bounding_box.should eq(BBox.new(41.0, 51.0, 42.0, 52.0))

    # When there was no input at all (occurs in the all null case), both geometry
    # types and bounding box will be None. This is because Parquet Thrift
    # statistics have no mechanism to communicate "empty". (The all null
    # situation may be determined from the null count in this case).
    accumulator.is_valid.should be_true
    stats = stats_or_fail(accumulator.finish)
    stats.geospatial_types.should be_nil
    stats.bounding_box.should be_nil

    # When there was 100% "empty" input (i.e., non-null geometries without
    # coordinates), there should be statistics with geometry types but no
    # bounding box.
    accumulator.is_valid.should be_true
    accumulator.update_wkb(Wkb.wkb_point_xy(Float64::NAN, Float64::NAN))
    stats = stats_or_fail(accumulator.finish)
    stats.geospatial_types.should eq([1])
    stats.bounding_box.should be_nil

    # If Z and/or M are present, they should be reported in the bounding box
    accumulator.is_valid.should be_true
    accumulator.update_wkb(Wkb.wkb_point_xyzm(1.0, 2.0, 3.0, 4.0))
    accumulator.update_wkb(Wkb.wkb_point_xyzm(5.0, 6.0, 7.0, 8.0))
    stats = stats_or_fail(accumulator.finish)
    stats.geospatial_types.should eq([3001])
    stats.bounding_box.should eq(
      BBox.new(1.0, 5.0, 2.0, 6.0).with_zrange(3.0, 7.0).with_mrange(4.0, 8.0)
    )
  end
end
