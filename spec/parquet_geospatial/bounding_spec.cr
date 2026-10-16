require "../spec_helper"

# Ported from `parquet-geospatial/src/bounding.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: the upstream tests build geometries from WKT (via the `wkt`
# crate) and encode them with the `wkb` crate; Crystal has neither, so the tests
# encode the equivalent WKB directly with the helpers below and exercise the
# ported `WkbReader` + `GeometryBounder` (`update_wkb`) end to end.
# `(0, 2).into()` becomes `WraparoundInterval.new(0.0, 2.0)`.

private alias GB = ParquetGeospatial::GeometryBounder
private alias Interval = ParquetGeospatial::Interval
private alias WraparoundInterval = ParquetGeospatial::WraparoundInterval
private alias Dim = ParquetGeospatial::WkbDimension
private alias Kind = ParquetGeospatial::WkbGeometryKind

private LE = IO::ByteFormat::LittleEndian

private def dim_code(dim : Dim) : UInt32
  case dim
  when Dim::Xy  then 0_u32
  when Dim::Xyz then 1000_u32
  when Dim::Xym then 2000_u32
  else               3000_u32
  end
end

private def wkb_header(io : IO, kind : Kind, dim : Dim) : Nil
  io.write_byte(1_u8)
  io.write_bytes((dim_code(dim) + kind.value).to_u32, LE)
end

private def write_coords(io : IO, coords : Array(Float64)) : Nil
  coords.each { |c| io.write_bytes(c, LE) }
end

private def wkb_point(coords : Array(Float64), dim : Dim = Dim::Xy) : Bytes
  io = IO::Memory.new
  wkb_header(io, Kind::Point, dim)
  write_coords(io, coords)
  io.to_slice
end

private def wkb_point_empty(dim : Dim = Dim::Xy) : Bytes
  wkb_point(Array.new(dim.coord_count, Float64::NAN), dim)
end

private def wkb_line_string(coords : Array(Array(Float64)), dim : Dim = Dim::Xy) : Bytes
  io = IO::Memory.new
  wkb_header(io, Kind::LineString, dim)
  io.write_bytes(coords.size.to_u32, LE)
  coords.each { |c| write_coords(io, c) }
  io.to_slice
end

private def wkb_line_string_empty(dim : Dim = Dim::Xy) : Bytes
  wkb_line_string([] of Array(Float64), dim)
end

private def wkb_polygon(rings : Array(Array(Array(Float64))), dim : Dim = Dim::Xy) : Bytes
  io = IO::Memory.new
  wkb_header(io, Kind::Polygon, dim)
  io.write_bytes(rings.size.to_u32, LE)
  rings.each do |ring|
    io.write_bytes(ring.size.to_u32, LE)
    ring.each { |c| write_coords(io, c) }
  end
  io.to_slice
end

private def wkb_polygon_empty(dim : Dim = Dim::Xy) : Bytes
  wkb_polygon([] of Array(Array(Float64)), dim)
end

private def wkb_multi_point(points : Array(Array(Float64)), dim : Dim = Dim::Xy) : Bytes
  io = IO::Memory.new
  wkb_header(io, Kind::MultiPoint, dim)
  io.write_bytes(points.size.to_u32, LE)
  points.each { |p| io.write(wkb_point(p, dim)) }
  io.to_slice
end

private def wkb_multi_line_string(lines : Array(Array(Array(Float64))), dim : Dim = Dim::Xy) : Bytes
  io = IO::Memory.new
  wkb_header(io, Kind::MultiLineString, dim)
  io.write_bytes(lines.size.to_u32, LE)
  lines.each { |line| io.write(wkb_line_string(line, dim)) }
  io.to_slice
end

private def wkb_multi_polygon(polygons : Array(Array(Array(Array(Float64)))), dim : Dim = Dim::Xy) : Bytes
  io = IO::Memory.new
  wkb_header(io, Kind::MultiPolygon, dim)
  io.write_bytes(polygons.size.to_u32, LE)
  polygons.each { |polygon| io.write(wkb_polygon(polygon, dim)) }
  io.to_slice
end

private def wkb_geometry_collection(geometries : Array(Bytes), dim : Dim = Dim::Xy) : Bytes
  io = IO::Memory.new
  wkb_header(io, Kind::GeometryCollection, dim)
  io.write_bytes(geometries.size.to_u32, LE)
  geometries.each { |geom| io.write(geom) }
  io.to_slice
end

private def wkb_bounds(
  values : Array(Bytes),
  wraparound : Interval = Interval.empty,
) : GB
  bounder = GB.empty.with_wraparound_hint(wraparound)
  values.each { |wkb| bounder.update_wkb(wkb) }
  bounder
end

private def empty_geoms(dim : Dim) : Array(Bytes)
  [
    wkb_point_empty(dim),
    wkb_line_string_empty(dim),
    wkb_polygon_empty(dim),
    wkb_multi_point([] of Array(Float64), dim),
    wkb_multi_line_string([] of Array(Array(Float64)), dim),
    wkb_multi_polygon([] of Array(Array(Array(Float64))), dim),
    wkb_geometry_collection([] of Bytes, dim),
  ]
end

describe ParquetGeospatial::GeometryBounder do
  # Upstream `test_wkb`.
  it "reads WKB" do
    bounds = GB.empty
    bounds.update_wkb(wkb_line_string([[0.0, 1.0], [2.0, 3.0]]))

    bounds.x.should eq(WraparoundInterval.new(0.0, 2.0))
    bounds.y.should eq(Interval.new(1.0, 3.0))
  end

  # Upstream `test_geometry_types`.
  it "reports geometry types" do
    wkb_bounds(empty_geoms(Dim::Xy)).geometry_types.should eq([1, 2, 3, 4, 5, 6, 7])
    wkb_bounds(empty_geoms(Dim::Xyz)).geometry_types.should eq([1001, 1002, 1003, 1004, 1005, 1006, 1007])
    wkb_bounds(empty_geoms(Dim::Xym)).geometry_types.should eq([2001, 2002, 2003, 2004, 2005, 2006, 2007])
    wkb_bounds(empty_geoms(Dim::Xyzm)).geometry_types.should eq([3001, 3002, 3003, 3004, 3005, 3006, 3007])
  end

  # Upstream `test_bounds_empty`.
  it "bounds empty geometries" do
    bounds = wkb_bounds(empty_geoms(Dim::Xy))
    bounds.x.empty?.should be_true
    bounds.y.empty?.should be_true
    bounds.z.empty?.should be_true
    bounds.m.empty?.should be_true

    bounds = wkb_bounds(empty_geoms(Dim::Xy), Interval.new(-180.0, 180.0))
    bounds.x.empty?.should be_true
    bounds.y.empty?.should be_true
    bounds.z.empty?.should be_true
    bounds.m.empty?.should be_true
  end

  # Upstream `test_bounds_coord`.
  it "bounds points" do
    bounds = wkb_bounds([wkb_point([0.0, 1.0]), wkb_point([2.0, 3.0])])
    bounds.x.should eq(WraparoundInterval.new(0.0, 2.0))
    bounds.y.should eq(Interval.new(1.0, 3.0))
    bounds.z.empty?.should be_true
    bounds.m.empty?.should be_true

    bounds = wkb_bounds([
      wkb_point([0.0, 1.0, 2.0], Dim::Xyz),
      wkb_point([3.0, 4.0, 5.0], Dim::Xyz),
    ])
    bounds.x.should eq(WraparoundInterval.new(0.0, 3.0))
    bounds.y.should eq(Interval.new(1.0, 4.0))
    bounds.z.should eq(Interval.new(2.0, 5.0))
    bounds.m.empty?.should be_true

    bounds = wkb_bounds([
      wkb_point([0.0, 1.0, 2.0], Dim::Xym),
      wkb_point([3.0, 4.0, 5.0], Dim::Xym),
    ])
    bounds.x.should eq(WraparoundInterval.new(0.0, 3.0))
    bounds.y.should eq(Interval.new(1.0, 4.0))
    bounds.z.empty?.should be_true
    bounds.m.should eq(Interval.new(2.0, 5.0))

    bounds = wkb_bounds([
      wkb_point([0.0, 1.0, 2.0, 3.0], Dim::Xyzm),
      wkb_point([4.0, 5.0, 6.0, 7.0], Dim::Xyzm),
    ])
    bounds.x.should eq(WraparoundInterval.new(0.0, 4.0))
    bounds.y.should eq(Interval.new(1.0, 5.0))
    bounds.z.should eq(Interval.new(2.0, 6.0))
    bounds.m.should eq(Interval.new(3.0, 7.0))
  end

  # Upstream `test_bounds_sequence`.
  it "bounds linestrings" do
    bounds = wkb_bounds([wkb_line_string([[0.0, 1.0], [2.0, 3.0]])])
    bounds.x.should eq(WraparoundInterval.new(0.0, 2.0))
    bounds.y.should eq(Interval.new(1.0, 3.0))
    bounds.z.empty?.should be_true
    bounds.m.empty?.should be_true

    bounds = wkb_bounds([wkb_line_string([[0.0, 1.0, 2.0], [3.0, 4.0, 5.0]], Dim::Xyz)])
    bounds.x.should eq(WraparoundInterval.new(0.0, 3.0))
    bounds.y.should eq(Interval.new(1.0, 4.0))
    bounds.z.should eq(Interval.new(2.0, 5.0))
    bounds.m.empty?.should be_true

    bounds = wkb_bounds([wkb_line_string([[0.0, 1.0, 2.0], [3.0, 4.0, 5.0]], Dim::Xym)])
    bounds.x.should eq(WraparoundInterval.new(0.0, 3.0))
    bounds.y.should eq(Interval.new(1.0, 4.0))
    bounds.z.empty?.should be_true
    bounds.m.should eq(Interval.new(2.0, 5.0))

    bounds = wkb_bounds([wkb_line_string([[0.0, 1.0, 2.0, 3.0], [4.0, 5.0, 6.0, 7.0]], Dim::Xyzm)])
    bounds.x.should eq(WraparoundInterval.new(0.0, 4.0))
    bounds.y.should eq(Interval.new(1.0, 5.0))
    bounds.z.should eq(Interval.new(2.0, 6.0))
    bounds.m.should eq(Interval.new(3.0, 7.0))
  end

  # Upstream `test_bounds_geometry_type`.
  it "bounds each geometry type" do
    bounds = wkb_bounds([wkb_point([0.0, 1.0]), wkb_point([2.0, 3.0])])
    bounds.x.should eq(WraparoundInterval.new(0.0, 2.0))
    bounds.y.should eq(Interval.new(1.0, 3.0))

    bounds = wkb_bounds([wkb_line_string([[0.0, 1.0], [2.0, 3.0]])])
    bounds.x.should eq(WraparoundInterval.new(0.0, 2.0))
    bounds.y.should eq(Interval.new(1.0, 3.0))

    bounds = wkb_bounds([wkb_polygon([
      [[0.0, 0.0], [0.0, 1.0], [1.0, 0.0], [0.0, 0.0]],
      [[10.0, 10.0], [10.0, 11.0], [11.0, 10.0], [10.0, 10.0]],
    ])])
    bounds.x.should eq(WraparoundInterval.new(0.0, 11.0))
    bounds.y.should eq(Interval.new(0.0, 11.0))

    bounds = wkb_bounds([wkb_multi_point([[0.0, 1.0], [2.0, 3.0]])])
    bounds.x.should eq(WraparoundInterval.new(0.0, 2.0))
    bounds.y.should eq(Interval.new(1.0, 3.0))

    bounds = wkb_bounds([wkb_multi_line_string([[[0.0, 1.0], [2.0, 3.0]]])])
    bounds.x.should eq(WraparoundInterval.new(0.0, 2.0))
    bounds.y.should eq(Interval.new(1.0, 3.0))

    bounds = wkb_bounds([wkb_multi_polygon([[[[0.0, 0.0], [0.0, 1.0], [1.0, 0.0], [0.0, 0.0]]]])])
    bounds.x.should eq(WraparoundInterval.new(0.0, 1.0))
    bounds.y.should eq(Interval.new(0.0, 1.0))

    bounds = wkb_bounds([wkb_geometry_collection([wkb_point([0.0, 1.0]), wkb_point([2.0, 3.0])])])
    bounds.x.should eq(WraparoundInterval.new(0.0, 2.0))
    bounds.y.should eq(Interval.new(1.0, 3.0))
  end

  # Upstream `test_bounds_wrap_basic`.
  it "computes wraparound bounds" do
    geoms = [wkb_point([-170.0, 0.0]), wkb_point([170.0, 0.0])]

    bounds = wkb_bounds(geoms, Interval.empty)
    bounds.x.should eq(WraparoundInterval.new(-170.0, 170.0))

    geoms_with_mid = geoms + [wkb_line_string([[-10.0, 0.0], [10.0, 0.0]])]
    bounds = wkb_bounds(geoms_with_mid, Interval.new(-180.0, 180.0))
    bounds.x.should eq(WraparoundInterval.new(-170.0, 170.0))

    bounds = wkb_bounds(geoms, Interval.new(-1000.0, 1000.0))
    bounds.x.should eq(WraparoundInterval.new(-170.0, 170.0))

    bounds = wkb_bounds(geoms, Interval.new(-10.0, 10.0))
    bounds.x.should eq(WraparoundInterval.new(-170.0, 170.0))

    bounds = wkb_bounds(geoms, Interval.new(-180.0, 180.0))
    bounds.x.should eq(WraparoundInterval.new(170.0, -170.0))

    geoms = [
      wkb_point([-10.0, 0.0]),
      wkb_point([-2.0, 0.0]),
      wkb_point([170.0, 0.0]),
      wkb_point([175.0, 0.0]),
    ]
    bounds = wkb_bounds(geoms, Interval.new(-180.0, 180.0))
    bounds.x.should eq(WraparoundInterval.new(-10.0, 175.0))
  end

  # Upstream `test_bounds_wrap_multipart`.
  it "computes wraparound bounds for multipart geometries" do
    fiji = wkb_multi_polygon([
      [
        [
          [-180.0, -15.51], [-180.0, -19.78], [-178.61, -21.14],
          [-178.02, -18.22], [-178.57, -16.04], [-180.0, -15.51],
        ],
      ],
      [
        [
          [180.0, -15.51], [177.98, -16.25], [176.67, -17.14],
          [177.83, -19.31], [180.0, -19.78], [180.0, -15.51],
        ],
      ],
    ])

    bounds = wkb_bounds([fiji], Interval.new(-180.0, 180.0))
    bounds.x.wraparound?.should be_true
    bounds.x.should eq(WraparoundInterval.new(176.67, -178.02))
    bounds.y.should eq(Interval.new(-21.14, -15.51))
  end
end
