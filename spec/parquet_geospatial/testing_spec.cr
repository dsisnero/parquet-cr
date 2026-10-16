require "../spec_helper"

# Ported from `parquet-geospatial/src/testing.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptation: the upstream tests decode the WKB and re-encode it as WKT (via the
# `wkb`/`wkt` crates); Crystal has no WKT writer, so the parsed geometry model is
# asserted directly instead.

private alias Testing = ParquetGeospatial::Testing
private alias WkbReader = ParquetGeospatial::WkbReader
private alias WkbPoint = ParquetGeospatial::WkbPoint
private alias WkbDimension = ParquetGeospatial::WkbDimension

describe ParquetGeospatial::Testing do
  # Upstream `test_wkb_item`.
  it "builds a point with an XY coordinate" do
    bytes = Testing.wkb_point_xy(1.0, 2.0)
    geometry = WkbReader.parse(bytes)
    geometry.should be_a(WkbPoint)
    point = geometry.as(WkbPoint)
    point.dimension.should eq(WkbDimension::Xy)
    point.coord.should eq([1.0, 2.0])
  end

  # Upstream `test_wkb_point_xyzm`.
  it "builds a point with an XYZM coordinate" do
    bytes = Testing.wkb_point_xyzm(1.0, 2.0, 3.0, 4.0)
    geometry = WkbReader.parse(bytes)
    geometry.should be_a(WkbPoint)
    point = geometry.as(WkbPoint)
    point.dimension.should eq(WkbDimension::Xyzm)
    point.coord.should eq([1.0, 2.0, 3.0, 4.0])
  end
end
