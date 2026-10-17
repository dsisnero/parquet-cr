require "../../spec_helper"

# Characterization of the core geospatial write/read path
# (`parquet/src/geospatial/accumulator.rs` + `file/metadata`) against
# apache/arrow-rs 60.0.0 (ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream 60.0.0 only asserts geospatial statistics through the arrow writer;
# the same behavior is verified here at the file layer so it does not depend on
# the deferred arrow interop module: a GEOMETRY column gets a bbox and geometry
# types but no min/max statistics, and a plain BYTE_ARRAY column gets neither.

private alias FWriter = Parquet::File::SerializedFileWriter
private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader
private alias WP = Parquet::File::Properties::WriterProperties
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PRep = Parquet::Repetition
private alias PLogical = Parquet::LogicalType
private alias BBox = Parquet::Geospatial::BoundingBox
private alias Wkb = ParquetGeospatial::Testing

private def column_schema(name : String, logical : PLogical?) : ST
  builder = ST.primitive_type_builder(name, PType::BYTE_ARRAY).with_repetition(PRep::OPTIONAL)
  builder = builder.with_logical_type(logical) unless logical.nil?
  ST.group_type_builder("schema").with_fields([builder.build.as(ST)]).build
end

private def write_points(schema : ST, values : Array(Parquet::ByteArray)) : IO::Memory
  io = IO::Memory.new
  file_writer = FWriter.new(io, schema, WP.builder.build)
  row_group_writer = file_writer.next_row_group
  column_writer = row_group_writer.next_column.not_nil!
  def_levels = Array(Int16).new(values.size, 1_i16)
  column_writer.typed(Parquet::ByteArray).write_batch(values, def_levels, nil)
  column_writer.close
  row_group_writer.close
  file_writer.close
  io
end

describe "geospatial statistics" do
  it "writes and reads the bounding box of a GEOMETRY column" do
    io = write_points(
      column_schema("geom", PLogical.geometry(nil)),
      [
        Parquet::ByteArray.new(Wkb.wkb_point_xy(1.0, 2.0)),
        Parquet::ByteArray.new(Wkb.wkb_point_xy(11.0, 12.0)),
      ],
    )

    chunk = FReader.new(BytesCR.new(io.to_slice)).metadata.row_group(0).column(0)
    stats = chunk.geo_statistics
    stats.should_not be_nil
    stats = stats.not_nil!
    stats.bounding_box.should eq(BBox.new(1.0, 11.0, 2.0, 12.0))
    stats.geospatial_types.should eq([1])

    # GEOMETRY has an undefined sort order, so min/max statistics are skipped
    # (the statistics struct itself is still written with the null count).
    statistics = chunk.statistics
    statistics.should_not be_nil
    statistics = statistics.not_nil!
    statistics.min_bytes_opt.should be_nil
    statistics.max_bytes_opt.should be_nil
    statistics.null_count_opt.should eq(0_u64)
  end

  it "round-trips the optional z/m ranges of a GEOMETRY column" do
    io = write_points(
      column_schema("geom", PLogical.geometry(nil)),
      [
        Parquet::ByteArray.new(Wkb.wkb_point_xyzm(1.0, 2.0, 3.0, 4.0)),
        Parquet::ByteArray.new(Wkb.wkb_point_xyzm(5.0, 6.0, 7.0, 8.0)),
      ],
    )

    chunk = FReader.new(BytesCR.new(io.to_slice)).metadata.row_group(0).column(0)
    stats = chunk.geo_statistics.not_nil!
    stats.geospatial_types.should eq([3001])
    stats.bounding_box.should eq(
      BBox.new(1.0, 5.0, 2.0, 6.0).with_zrange(3.0, 7.0).with_mrange(4.0, 8.0)
    )
  end

  it "does not write geospatial statistics for non-geospatial columns" do
    io = write_points(
      column_schema("plain", nil),
      [
        Parquet::ByteArray.new("abc"),
        Parquet::ByteArray.new("xyz"),
      ],
    )

    chunk = FReader.new(BytesCR.new(io.to_slice)).metadata.row_group(0).column(0)
    chunk.geo_statistics.should be_nil

    # Plain BYTE_ARRAY keeps its min/max statistics.
    statistics = chunk.statistics.not_nil!
    statistics.min_bytes_opt.should eq("abc".to_slice)
    statistics.max_bytes_opt.should eq("xyz".to_slice)
  end
end
