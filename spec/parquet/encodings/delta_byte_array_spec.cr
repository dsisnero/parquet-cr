require "../../spec_helper"

# Ported from `parquet/src/encodings/encoding/mod.rs`
# (DeltaLengthByteArrayEncoder/DeltaByteArrayEncoder) and
# `parquet/src/encodings/decoding.rs` (their decoders) tests
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private def ba(values : Array(Int32)) : Parquet::ByteArray
  Parquet::ByteArray.new(values.map(&.to_u8!))
end

private def delta_length_roundtrip(chunks : Array(Array(Parquet::ByteArray))) : Nil
  # Multiple put() calls:
  encoder = DeltaLengthByteArrayEncoder(Parquet::ByteArray).new
  chunks.each { |chunk| encoder.put(chunk) }
  bytes = encoder.flush_buffer

  expected = chunks.flatten
  decoder = DeltaLengthByteArrayDecoder(Parquet::ByteArray).new
  decoder.set_data(bytes, expected.size)
  result = Array(Parquet::ByteArray).new(expected.size, Parquet::ByteArray.new)
  decoder.get(result).should eq(expected.size)
  decoder.values_left.should eq(0)
  result.should eq(expected)
end

private def delta_byte_array_roundtrip(chunks : Array(Array(Parquet::ByteArray))) : Nil
  encoder = DeltaByteArrayEncoder(Parquet::ByteArray).new
  chunks.each { |chunk| encoder.put(chunk) }
  bytes = encoder.flush_buffer

  expected = chunks.flatten
  decoder = DeltaByteArrayDecoder(Parquet::ByteArray).new
  decoder.set_data(bytes, expected.size)
  result = Array(Parquet::ByteArray).new(expected.size, Parquet::ByteArray.new)
  decoder.get(result).should eq(expected.size)
  decoder.values_left.should eq(0)
  result.should eq(expected)
end

describe Parquet::Encodings::DeltaLengthByteArrayEncoder do
  it "round-trips byte array lengths and data" do
    delta_length_roundtrip([[ba([1, 2, 3, 4, 5, 6])]])
    delta_length_roundtrip([[ba([1])], [ba([2, 3]), ba([4, 5, 6])]])
    delta_length_roundtrip([[ba([1, 2, 3, 4, 5, 6]), ba([1, 2, 3, 4, 5, 6]), ba([7, 8]), ba([] of Int32)]])
  end
end

describe Parquet::Encodings::DeltaByteArrayEncoder do
  # Upstream `test_delta_byte_array_same_arrays`.
  it "round-trips identical arrays" do
    data = [
      [ba([1, 2, 3, 4, 5, 6])],
      [ba([1, 2, 3, 4, 5, 6]), ba([1, 2, 3, 4, 5, 6])],
      [ba([1, 2, 3, 4, 5, 6]), ba([1, 2, 3, 4, 5, 6])],
    ]
    delta_byte_array_roundtrip(data)
  end

  # Upstream `test_delta_byte_array_unique_arrays`.
  it "round-trips unique arrays" do
    data = [
      [ba([1])],
      [ba([2, 3]), ba([4, 5, 6])],
      [ba([7, 8]), ba([9, 0, 1, 2])],
    ]
    delta_byte_array_roundtrip(data)
  end

  # Upstream `test_delta_byte_array_single_array`.
  it "round-trips a single array" do
    delta_byte_array_roundtrip([[ba([1, 2, 3, 4, 5, 6])]])
  end
end

describe Parquet::Encodings::DeltaByteArrayDecoder do
  # Upstream `test_delta_byte_array_invalid_prefix_len_returns_error`.
  it "rejects a prefix length longer than the previous value" do
    input = [Parquet::ByteArray.new("a"), Parquet::ByteArray.new("ab")]
    encoder = DeltaByteArrayEncoder(Parquet::ByteArray).new
    encoder.put(input)
    encoded = encoder.flush_buffer

    # Valid encoding should produce prefix lengths [0, 1].
    prefix_decoder = DeltaBitPackDecoder(Int32).new
    prefix_decoder.set_data(encoded, input.size)
    num_prefixes = prefix_decoder.values_left
    prefix_lengths = Array(Int32).new(num_prefixes, 0)
    prefix_decoder.get(prefix_lengths)
    prefix_lengths.should eq([0, 1])
    prefix_stream_end = prefix_decoder.get_offset

    # Corrupt the prefix stream to [1, 1].
    prefix_encoder = DeltaBitPackEncoder(Int32).new
    prefix_encoder.put([1, 1])
    corrupted_prefix = prefix_encoder.flush_buffer

    buffer = [] of UInt8
    buffer.concat(corrupted_prefix)
    buffer.concat(encoded[prefix_stream_end, encoded.size - prefix_stream_end])
    corrupted = Slice.new(buffer.to_unsafe, buffer.size).dup

    decoder = DeltaByteArrayDecoder(Parquet::ByteArray).new
    decoder.set_data(corrupted, input.size)
    output = Array(Parquet::ByteArray).new(input.size, Parquet::ByteArray.new)
    err = expect_raises(Parquet::ParquetError) { decoder.get(output) }
    err.to_s.should contain("Invalid DELTA_BYTE_ARRAY prefix length")
  end

  # Upstream `test_delta_byte_array_negative_prefix_len_returns_error`.
  it "rejects a negative prefix length" do
    input = [Parquet::ByteArray.new("a"), Parquet::ByteArray.new("ab")]
    encoder = DeltaByteArrayEncoder(Parquet::ByteArray).new
    encoder.put(input)
    encoded = encoder.flush_buffer

    decoder = DeltaByteArrayDecoder(Parquet::ByteArray).new
    decoder.set_data(encoded, input.size)
    decoder.prefix_lengths[0] = -1
    output = Array(Parquet::ByteArray).new(input.size, Parquet::ByteArray.new)
    err = expect_raises(Parquet::ParquetError) { decoder.get(output) }
    err.to_s.should contain("Invalid DELTA_BYTE_ARRAY prefix length")
  end
end
