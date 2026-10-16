require "../../spec_helper"

# Ported from `parquet/src/file/page_index/index_reader.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream covers `decode_column_index` indirectly through the
# `SerializedFileReader` page-index tests (which need the file reader). Here the
# wire struct is built directly and decoded, keeping the same expectations.

private def le32(value : Int32) : Bytes
  io = IO::Memory.new(4)
  io.write_bytes(value, IO::ByteFormat::LittleEndian)
  io.to_slice.dup
end

private def le64(value : Int64) : Bytes
  io = IO::Memory.new(8)
  io.write_bytes(value, IO::ByteFormat::LittleEndian)
  io.to_slice.dup
end

private def lef32(value : Float32) : Bytes
  io = IO::Memory.new(4)
  io.write_bytes(value, IO::ByteFormat::LittleEndian)
  io.to_slice.dup
end

private def lef64(value : Float64) : Bytes
  io = IO::Memory.new(8)
  io.write_bytes(value, IO::ByteFormat::LittleEndian)
  io.to_slice.dup
end

describe Parquet::File::PageIndex do
  it "decodes a BOOLEAN column index" do
    thrift = PPI::ThriftColumnIndex.new(
      [false], [Bytes[1_u8]], [Bytes[0_u8]],
      Parquet::BoundaryOrder::ASCENDING, nil, nil, nil, nil,
    )

    meta = PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::BOOLEAN)

    meta.kind.should eq(Parquet::Type::BOOLEAN)
    index = meta.inner.as(PPI::PrimitiveColumnIndex(Bool))
    index.min_value(0).should be_true
    index.max_value(0).should be_false
  end

  it "decodes an INT32 column index" do
    thrift = PPI::ThriftColumnIndex.new(
      [false], [le32(-123)], [le32(234)],
      Parquet::BoundaryOrder::ASCENDING, [0_i64], nil, nil, nil,
    )

    meta = PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::INT32)

    index = meta.inner.as(PPI::PrimitiveColumnIndex(Int32))
    index.min_value(0).should eq(-123)
    index.max_value(0).should eq(234)
    index.null_count(0).should eq(0_i64)
  end

  it "decodes an INT64 column index" do
    thrift = PPI::ThriftColumnIndex.new(
      [false], [le64(-9_000_000_000_i64)], [le64(9_000_000_000_i64)],
      Parquet::BoundaryOrder::DESCENDING, nil, nil, nil, nil,
    )

    meta = PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::INT64)

    index = meta.inner.as(PPI::PrimitiveColumnIndex(Int64))
    index.min_value(0).should eq(-9_000_000_000_i64)
    index.max_value(0).should eq(9_000_000_000_i64)
  end

  it "decodes an INT96 column index" do
    lo = Parquet::Int96.from([1_u32, 2_u32, 3_u32])
    hi = Parquet::Int96.from([4_u32, 5_u32, 6_u32])
    thrift = PPI::ThriftColumnIndex.new(
      [false], [lo.as_bytes], [hi.as_bytes],
      Parquet::BoundaryOrder::ASCENDING, nil, nil, nil, nil,
    )

    meta = PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::INT96)

    index = meta.inner.as(PPI::PrimitiveColumnIndex(Parquet::Int96))
    index.min_value(0).should eq(lo)
    index.max_value(0).should eq(hi)
  end

  it "decodes a FLOAT column index" do
    thrift = PPI::ThriftColumnIndex.new(
      [false], [lef32(-0.0_f32)], [lef32(9.9_f32)],
      Parquet::BoundaryOrder::ASCENDING, nil, nil, nil, nil,
    )

    meta = PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::FLOAT)

    index = meta.inner.as(PPI::PrimitiveColumnIndex(Float32))
    index.min_value(0).should eq(-0.0_f32)
    index.max_value(0).should eq(9.9_f32)
  end

  it "decodes a DOUBLE column index" do
    thrift = PPI::ThriftColumnIndex.new(
      [false], [lef64(-0.0_f64)], [lef64(90.9_f64)],
      Parquet::BoundaryOrder::UNORDERED, nil, nil, nil, nil,
    )

    meta = PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::DOUBLE)

    index = meta.inner.as(PPI::PrimitiveColumnIndex(Float64))
    index.min_value(0).should eq(-0.0_f64)
    index.max_value(0).should eq(90.9_f64)
  end

  it "decodes a BYTE_ARRAY column index" do
    thrift = PPI::ThriftColumnIndex.new(
      [false], ["Hello".to_slice], ["today".to_slice],
      Parquet::BoundaryOrder::ASCENDING, nil, nil, nil, nil,
    )

    meta = PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::BYTE_ARRAY)

    meta.kind.should eq(Parquet::Type::BYTE_ARRAY)
    index = meta.inner.as(PPI::ByteArrayColumnIndex)
    index.min_value(0).should eq("Hello".to_slice)
    index.max_value(0).should eq("today".to_slice)
  end

  it "decodes a FIXED_LEN_BYTE_ARRAY column index" do
    thrift = PPI::ThriftColumnIndex.new(
      [false], [Bytes.new(11, 0_u8)], [Bytes.new(11, 5_u8)],
      Parquet::BoundaryOrder::UNORDERED, [1_i64], nil, nil, nil,
    )

    meta = PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::FIXED_LEN_BYTE_ARRAY)

    meta.kind.should eq(Parquet::Type::FIXED_LEN_BYTE_ARRAY)
    index = meta.inner.as(PPI::ByteArrayColumnIndex)
    index.num_pages.should eq(1)
    index.null_count(0).should eq(1_i64)
    index.min_value(0).should eq(Bytes.new(11, 0_u8))
    index.max_value(0).should eq(Bytes.new(11, 5_u8))
  end

  it "raises when a non-null page value is too short" do
    thrift = PPI::ThriftColumnIndex.new(
      [false], [Bytes.new(0)], [Bytes.new(0)],
      Parquet::BoundaryOrder::UNORDERED, nil, nil, nil, nil,
    )

    expect_raises(Parquet::ParquetError, "error converting value, expected 4 bytes got 0") do
      PPI.decode_column_index(ThriftRoundTrip.encode(thrift), Parquet::Type::INT32)
    end
  end
end
