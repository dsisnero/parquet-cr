require "../spec_helper"

# Ported from `parquet/src/data_type.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
describe Parquet do
  # Upstream `test_as_bytes`.
  it "converts values to bytes" do
    i96 = Parquet::Int96.from([1_u32, 2_u32, 3_u32])
    i96.as_bytes.should eq(Bytes[1_u8, 0_u8, 0_u8, 0_u8, 2_u8, 0_u8, 0_u8, 0_u8, 3_u8, 0_u8, 0_u8, 0_u8])

    byte_arr = Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8])
    byte_arr.as_bytes.should eq(Bytes[1_u8, 2_u8, 3_u8])

    Parquet::Decimal.from_i32(123, 5, 2).as_bytes.should eq(Bytes[0_u8, 0_u8, 0_u8, 123_u8])
    Parquet::Decimal.from_i64(123, 5, 2).as_bytes
      .should eq(Bytes[0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 123_u8])
    Parquet::Decimal.from_bytes(Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8]), 5, 2)
      .as_bytes.should eq(Bytes[1_u8, 2_u8, 3_u8])
  end

  # Upstream `test_int96_from`.
  it "builds Int96 from a vector" do
    Parquet::Int96.from([1_u32, 12345_u32, 1234567890_u32]).data.to_a
      .should eq([1_u32, 12345_u32, 1234567890_u32])
  end

  # Upstream `test_byte_array_from`.
  it "builds ByteArray from bytes, strings, and arrays" do
    Parquet::ByteArray.new(Bytes[65_u8, 66_u8, 67_u8]).data.should eq(Bytes[65_u8, 66_u8, 67_u8])
    Parquet::ByteArray.new("ABC").data.should eq(Bytes[65_u8, 66_u8, 67_u8])
    Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8]).data
      .should eq(Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8])
    buf = [6_u8, 7_u8, 8_u8, 9_u8, 10_u8]
    Parquet::ByteArray.new(buf).data.should eq(Bytes[6_u8, 7_u8, 8_u8, 9_u8, 10_u8])
  end

  # Upstream `test_decimal_partial_eq`.
  it "compares Decimal values by data, precision, and scale" do
    Parquet::Decimal.default.should eq(Parquet::Decimal.from_i32(0, 0, 0))
    Parquet::Decimal.from_i32(222, 5, 2).should eq(Parquet::Decimal.from_i32(222, 5, 2))
    Parquet::Decimal.from_bytes(Parquet::ByteArray.new(Bytes[0_u8, 0_u8, 0_u8, 3_u8]), 5, 2)
      .should eq(Parquet::Decimal.from_i32(3, 5, 2))

    Parquet::Decimal.from_i32(222, 5, 2).should_not eq(Parquet::Decimal.from_i32(111, 5, 2))
    Parquet::Decimal.from_i32(222, 5, 2).should_not eq(Parquet::Decimal.from_i32(222, 6, 2))
    Parquet::Decimal.from_i32(222, 5, 2).should_not eq(Parquet::Decimal.from_i32(222, 5, 3))
    Parquet::Decimal.from_i64(222, 5, 2).should_not eq(Parquet::Decimal.from_i32(222, 5, 2))
  end

  # Upstream `test_byte_array_ord`.
  it "orders ByteArray lexicographically with nulls first" do
    byte_arr1 = Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8])
    byte_arr11 = Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8])
    byte_arr2 = Parquet::ByteArray.new(Bytes[3_u8, 4_u8])
    byte_arr3 = Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 4_u8])
    byte_arr4 = Parquet::ByteArray.new(Bytes.new(0))
    byte_arr5 = Parquet::ByteArray.new(Bytes[2_u8, 2_u8, 3_u8])

    (byte_arr1 < byte_arr2).should be_true
    (byte_arr3 > byte_arr1).should be_true
    (byte_arr1 > byte_arr4).should be_true
    byte_arr1.should eq(byte_arr11)
    (byte_arr5 > byte_arr1).should be_true
  end
end
