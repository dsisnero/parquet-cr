require "../../spec_helper"

# Ported from `parquet/src/encodings/encoding/byte_stream_split_encoder.rs`,
# `parquet/src/encodings/decoding/byte_stream_split_decoder.rs`, and
# `parquet/src/encodings/encoding/mod.rs`
# (`test_byte_stream_split_example_f32`, `test_estimated_data_encoded_size`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private def bss_roundtrip(values : Array(T)) : Nil forall T
  encoder = Parquet::Encodings::ByteStreamSplitEncoder(T).new
  encoder.put(values)
  encoded = encoder.flush_buffer

  decoder = Parquet::Encodings::ByteStreamSplitDecoder(T).new
  decoder.set_data(encoded, values.size)
  output = Array(T).new(values.size, values.first)
  decoder.get(output).should eq(values.size)
  decoder.values_left.should eq(0)
  output.should eq(values)
end

private def variable_bss_roundtrip(type_width : Int32, values : Array(PFixedLenByteArray)) : Nil
  encoder = Parquet::Encodings::VariableWidthByteStreamSplitEncoder(PFixedLenByteArray).new(type_width)
  encoder.put(values)
  encoded = encoder.flush_buffer

  decoder = Parquet::Encodings::VariableWidthByteStreamSplitDecoder(PFixedLenByteArray).new(type_width)
  decoder.set_data(encoded, values.size)
  output = Array(PFixedLenByteArray).new(values.size, values.first)
  decoder.get(output).should eq(values.size)
  decoder.values_left.should eq(0)
  output.should eq(values)
end

private def f32_from_le(bytes : Array(UInt8)) : Float32
  IO::Memory.new(Bytes.new(bytes.to_unsafe, bytes.size)).read_bytes(Float32, IO::ByteFormat::LittleEndian)
end

describe Parquet::Encodings::ByteStreamSplitEncoder do
  # Upstream `test_byte_stream_split_example_f32`.
  it "splits byte streams for the documented example" do
    input = [
      f32_from_le([0xAA_u8, 0xBB_u8, 0xCC_u8, 0xDD_u8]),
      f32_from_le([0x00_u8, 0x11_u8, 0x22_u8, 0x33_u8]),
      f32_from_le([0xA3_u8, 0xB4_u8, 0xC5_u8, 0xD6_u8]),
    ]

    encoder = Parquet::Encodings::ByteStreamSplitEncoder(Float32).new
    encoder.put(input)
    encoded = encoder.flush_buffer
    encoded.should eq(Bytes[
      0xAA_u8, 0x00, 0xA3, 0xBB, 0x11, 0xB4, 0xCC, 0x22, 0xC5, 0xDD, 0x33, 0xD6,
    ])

    decoder = Parquet::Encodings::ByteStreamSplitDecoder(Float32).new
    decoder.set_data(encoded, input.size)
    output = Array(Float32).new(input.size, 0.0_f32)
    decoder.get(output).should eq(input.size)
    output.should eq(input)
  end

  # Upstream `test_i32` / `test_i64` / `test_float` / `test_double`
  # (BYTE_STREAM_SPLIT round-trips).
  it "round-trips 4- and 8-byte physical types" do
    bss_roundtrip((0...13).map { |i| (i * 7919 - 3).to_i32 })
    bss_roundtrip((0...13).map { |i| (i * 1_000_003_i64 - 5).to_i64 })
    bss_roundtrip((0...13).map { |i| i.to_f32 * 1.5_f32 })
    bss_roundtrip((0...13).map { |i| i.to_f64 * 2.25_f64 })
  end

  # Upstream `test_fixed_len_byte_array` (BYTE_STREAM_SPLIT via the
  # variable-width encoder/decoder).
  it "round-trips fixed-len byte arrays" do
    variable_bss_roundtrip(3, (0...13).map { |i| PFixedLenByteArray.new([i.to_u8!, (i + 1).to_u8!, (i + 2).to_u8!]) })
    variable_bss_roundtrip(16, (0...13).map do |i|
      PFixedLenByteArray.new(Array(UInt8).new(16) { |j| (i + j).to_u8! })
    end)
    variable_bss_roundtrip(5, (0...9).map { |i| PFixedLenByteArray.new(Array(UInt8).new(5) { |j| (i * 3 + j).to_u8! }) })
  end

  # Upstream `test_estimated_data_encoded_size` (BYTE_STREAM_SPLIT case).
  it "estimates the data page size" do
    encoder = Parquet::Encodings::ByteStreamSplitEncoder(Float32).new
    encoder.estimated_data_encoded_size.should eq(0)

    encoder.put([0.1_f32, 0.2_f32])
    encoder.estimated_data_encoded_size.should eq(8)

    encoder.flush_buffer
    encoder.estimated_data_encoded_size.should eq(0)
  end
end

describe Parquet::Encodings::ByteStreamSplitDecoder do
  it "skips values without decoding them" do
    values = (0...10).map { |i| (i * 11).to_i32 }
    encoder = Parquet::Encodings::ByteStreamSplitEncoder(Int32).new
    encoder.put(values)
    encoded = encoder.flush_buffer

    decoder = Parquet::Encodings::ByteStreamSplitDecoder(Int32).new
    decoder.set_data(encoded, values.size)
    decoder.skip(4).should eq(4)
    decoder.values_left.should eq(6)

    output = Array(Int32).new(6, 0)
    decoder.get(output).should eq(6)
    output.should eq(values[4..])
  end
end

describe Parquet::Encodings::VariableWidthByteStreamSplitDecoder do
  it "rejects data whose length is not a multiple of the type width" do
    decoder = Parquet::Encodings::VariableWidthByteStreamSplitDecoder(PFixedLenByteArray).new(3)
    expect_raises(Parquet::ParquetError, "not a multiple of type width") do
      decoder.set_data(Bytes[1_u8, 2_u8, 3_u8, 4_u8], 1)
    end
  end
end
