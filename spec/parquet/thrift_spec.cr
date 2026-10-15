require "../spec_helper"

# Ported from `parquet/src/parquet_thrift.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream `test_enum_roundtrip` and `test_union_all_empty_roundtrip` depend on
# `basic::{Type, TimeUnit}` and are ported alongside the `basic` feature.
describe Parquet::Thrift do
  # Upstream `test_enum_roundtrip`.
  it "round-trips every Type variant" do
    [
      Parquet::Type::BOOLEAN,
      Parquet::Type::INT32,
      Parquet::Type::INT64,
      Parquet::Type::INT96,
      Parquet::Type::FLOAT,
      Parquet::Type::DOUBLE,
      Parquet::Type::BYTE_ARRAY,
      Parquet::Type::FIXED_LEN_BYTE_ARRAY,
    ].each do |value|
      ThriftRoundTrip.roundtrip(value).should eq(value)
    end
  end

  # Upstream `test_union_all_empty_roundtrip`.
  it "round-trips every TimeUnit variant" do
    [
      Parquet::TimeUnit::MILLIS,
      Parquet::TimeUnit::MICROS,
      Parquet::TimeUnit::NANOS,
    ].each do |value|
      ThriftRoundTrip.roundtrip(value).should eq(value)
    end
  end

  # Upstream `test_decode_empty_list`.
  it "decodes an empty list" do
    data = Bytes.new(1)
    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(data)
    header = prot.read_list_begin
    header.size.should eq(0)
    header.element_type.should eq(Parquet::Thrift::ElementType::Byte)
  end

  # Upstream `test_read_list_begin_size_above_i32_max_returns_err`.
  it "rejects a list size varint above i32::MAX" do
    data = Bytes[0xF8_u8, 0x80_u8, 0x80_u8, 0x80_u8, 0x80_u8, 0x08_u8]
    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(data)
    expect_raises(Parquet::ParquetError, "integer overflow decoding thrift value") do
      prot.read_list_begin
    end
  end

  # Upstream `test_read_list_wrong_type`.
  it "rejects a list with the wrong element type" do
    data = Bytes.new(2)
    data[0] = 0x42_u8 # 4 elements of Byte (bool group)
    data[1] = 0x01_u8
    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(data)
    error = expect_raises(Parquet::ParquetError) do
      Parquet::Thrift.read_thrift_vec(prot, Int32)
    end
    error.to_s.should contain("Expected list element type of I32 but got Bool")
  end

  # Upstream `test_read_thrift_vec_roundtrip_i32`.
  it "round-trips a list of i32" do
    data = Bytes[0x25_u8, 0x00_u8, 0x00_u8]
    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(data)
    result = Parquet::Thrift.read_thrift_vec(prot, Int32)
    result.should eq([0, 0])
  end

  # Upstream `test_read_thrift_vec_size_exceeds_remaining_returns_err`.
  it "rejects a list size exceeding the remaining input" do
    data = Bytes[0xE5_u8]
    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(data)
    error = expect_raises(Parquet::ParquetError) do
      Parquet::Thrift.read_thrift_vec(prot, Int32)
    end
    error.to_s.should contain("Thrift list size 14 exceeds remaining input length 0")
  end

  # Upstream `test_read_thrift_vec_huge_declared_size_returns_err`.
  it "rejects a huge declared list size" do
    data = Bytes[0xF5_u8, 0xfc_u8, 0xfc_u8, 0xfc_u8, 0x33_u8, 0x00_u8, 0x00_u8]
    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(data)
    error = expect_raises(Parquet::ParquetError) do
      Parquet::Thrift.read_thrift_vec(prot, Int32)
    end
    error.to_s.should contain("Thrift list size 109002364 exceeds remaining input length 2")
  end

  describe "scalar round-trips" do
    it "round-trips i32 values including zig-zag boundaries" do
      [-2_147_483_648, -1, 0, 1, 2_147_483_647, 64, -64].each do |value|
        roundtrip(value).should eq(value)
      end
    end

    it "round-trips i64 values including zig-zag boundaries" do
      [-9_223_372_036_854_775_808_i64, -1_i64, 0_i64, 9_223_372_036_854_775_807_i64].each do |value|
        roundtrip(value).should eq(value)
      end
    end

    it "round-trips booleans" do
      roundtrip(true).should eq(true)
      roundtrip(false).should eq(false)
    end

    it "round-trips strings" do
      roundtrip("parquet").should eq("parquet")
      roundtrip("").should eq("")
    end

    it "round-trips lists of i32" do
      data = [1_i32, -2, 3, 40, 5000]
      parsed = Parquet::Thrift.read_value(prot_for(data), Array(Int32))
      parsed.should eq(data)
    end
  end

  describe "OrderedF64" do
    it "orders by IEEE 754 total order" do
      neg_inf = Parquet::Thrift::OrderedF64.new(-Float64::INFINITY)
      neg = Parquet::Thrift::OrderedF64.new(-1.0)
      zero = Parquet::Thrift::OrderedF64.new(0.0)
      pos = Parquet::Thrift::OrderedF64.new(1.0)
      pos_inf = Parquet::Thrift::OrderedF64.new(Float64::INFINITY)

      (neg_inf < neg).should be_true
      (neg < zero).should be_true
      (zero < pos).should be_true
      (pos < pos_inf).should be_true
      (zero == Parquet::Thrift::OrderedF64.new(0.0)).should be_true
    end
  end
end

# Round-trip a value of type `T` through the compact protocol.
private def roundtrip(value : T) : T forall T
  io = IO::Memory.new
  writer = Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
  Parquet::Thrift.write_thrift_value(writer, value)
  Parquet::Thrift.read_value(prot_for_bytes(io.to_slice), T)
end

private def prot_for(value : Array(Int32)) : Parquet::Thrift::ThriftSliceInputProtocol
  io = IO::Memory.new
  writer = Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
  Parquet::Thrift.write_thrift_value(writer, value)
  prot_for_bytes(io.to_slice)
end

private def prot_for_bytes(data : Bytes) : Parquet::Thrift::ThriftSliceInputProtocol
  Parquet::Thrift::ThriftSliceInputProtocol.new(data)
end
