require "../../spec_helper"

# Ported from `parquet/src/encodings/encoding/mod.rs` (RleValueEncoder),
# `parquet/src/encodings/decoding.rs` (RleValueDecoder), and
# `parquet/src/encodings/encoding/mod.rs::test_estimated_data_encoded_size`
# (RLE case) (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

describe Parquet::Encodings::RleValueEncoder do
  # Upstream `test_bool` (Encoding::RLE round-trip).
  it "round-trips boolean values" do
    values = Array(Bool).new(16, true) + Array(Bool).new(16, false)

    encoder = Parquet::Encodings::RleValueEncoder(Bool).new
    encoder.put(values)
    encoded = encoder.flush_buffer

    decoder = Parquet::Encodings::RleValueDecoder(Bool).new
    decoder.set_data(encoded, values.size)
    output = Array(Bool).new(values.size, false)
    decoder.get(output).should eq(values.size)
    decoder.values_left.should eq(0)
    output.should eq(values)
  end

  it "round-trips mixed boolean values" do
    values = [true, false, true, true, false, true, false, false, true]

    encoder = Parquet::Encodings::RleValueEncoder(Bool).new
    encoder.put(values)
    encoded = encoder.flush_buffer

    decoder = Parquet::Encodings::RleValueDecoder(Bool).new
    decoder.set_data(encoded, values.size)
    output = Array(Bool).new(values.size, false)
    decoder.get(output).should eq(values.size)
    output.should eq(values)
  end

  # Upstream `test_estimated_data_encoded_size` (RLE case):
  # values 16 true + 16 false -> 0, 6, 0.
  it "estimates the data page size" do
    values = Array(Bool).new(16, true) + Array(Bool).new(16, false)

    encoder = Parquet::Encodings::RleValueEncoder(Bool).new
    encoder.estimated_data_encoded_size.should eq(0)

    encoder.put(values)
    encoder.estimated_data_encoded_size.should eq(6)

    encoder.flush_buffer
    encoder.estimated_data_encoded_size.should eq(0)
  end

  it "rejects non-boolean types" do
    encoder = Parquet::Encodings::RleValueEncoder(Int32).new
    expect_raises(Parquet::ParquetError, "RleValueEncoder only supports BoolType") do
      encoder.put([1_i32])
    end
  end
end

describe Parquet::Encodings::RleValueDecoder do
  it "skips boolean values" do
    values = Array(Bool).new(8, true) + Array(Bool).new(8, false)

    encoder = Parquet::Encodings::RleValueEncoder(Bool).new
    encoder.put(values)
    encoded = encoder.flush_buffer

    decoder = Parquet::Encodings::RleValueDecoder(Bool).new
    decoder.set_data(encoded, values.size)
    decoder.skip(8).should eq(8)
    decoder.values_left.should eq(8)

    output = Array(Bool).new(8, true)
    decoder.get(output).should eq(8)
    output.should eq(Array(Bool).new(8, false))
  end

  it "rejects truncated data" do
    decoder = Parquet::Encodings::RleValueDecoder(Bool).new
    expect_raises(Parquet::ParquetError, "Not enough bytes to decode") do
      decoder.set_data(Bytes[1_u8, 0_u8], 1)
    end

    expect_raises(Parquet::ParquetError, "Not enough bytes to decode") do
      decoder.set_data(Bytes[10_u8, 0_u8, 0_u8, 0_u8], 1)
    end
  end
end
