require "../../spec_helper"

# Ported from `parquet/src/arrow/decoder/dictionary_index.rs` and
# `parquet/src/arrow/decoder/delta_byte_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `dictionary_index.rs` ships two error-path tests; the read/skip behaviour is
# characterized here (upstream exercises it through the byte array readers).
private alias Decoders = Parquet::Arrow::Decoders
private alias Enc = Parquet::Encoding
private alias RleEncoder = Parquet::Encodings::RleEncoder
private alias DeltaByteArrayEncoder = Parquet::Encodings::DeltaByteArrayEncoder

private def encode_indices(bit_width : Int32, indices : Array(Int32)) : Bytes
  encoder = RleEncoder.new(bit_width, 1024)
  indices.each { |index| encoder.put(index.to_u64) }
  encoder.consume
end

describe Parquet::Arrow::Decoders::DictIndexDecoder do
  # Upstream `test_dict_index_decoder_empty_data`.
  it "rejects empty data" do
    expect_raises(Parquet::GeneralError, "dictionary index page is empty") do
      Decoders::DictIndexDecoder.new(Bytes.empty, 0, nil)
    end
  end

  # Upstream `test_dict_index_decoder_invalid_bit_width`.
  it "rejects an invalid bit width" do
    expect_raises(Parquet::GeneralError, "Invalid or corrupted RLE bit width 33") do
      Decoders::DictIndexDecoder.new(Bytes[33_u8, 0_u8, 0_u8, 0_u8], 1, nil)
    end
  end

  it "decodes and skips dictionary indices in chunks" do
    indices = [1_i32, 2, 3, 4, 5, 6, 7, 0]
    data = Bytes.new(1) { 3_u8 }
    encoded = encode_indices(3, indices)
    payload = Bytes.new(1 + encoded.size)
    payload[0] = 3_u8
    encoded.copy_to(payload[1, encoded.size])

    decoder = Decoders::DictIndexDecoder.new(payload, indices.size, indices.size)
    read = [] of Int32
    # Read in chunks of 3.
    while (n = decoder.read(3) { |chunk| read.concat(chunk); nil }) > 0
      # continue
    end
    read.should eq(indices)

    # Skipping is bounded by the remaining values.
    decoder = Decoders::DictIndexDecoder.new(payload, indices.size, indices.size)
    decoder.skip(3).should eq(3)
    read = [] of Int32
    decoder.read(10) { |chunk| read.concat(chunk); nil }
    read.should eq([4, 5, 6, 7, 0])
    decoder.read(10) { |_chunk| nil }.should eq(0)
    _ = data
  end
end

describe Parquet::Arrow::Decoders::DeltaByteArrayDecoder do
  it "decodes and skips delta byte arrays" do
    values = ["apple", "application", "apply", "banana", "band", "bandana"]
    encoder = DeltaByteArrayEncoder(Parquet::ByteArray).new
    encoder.put(values.map { |value| Parquet::ByteArray.new(value) })
    encoded = encoder.flush_buffer

    decoder = Decoders::DeltaByteArrayDecoder.new(encoded)
    decoder.remaining.should eq(values.size)

    read = [] of String
    decoder.read(3) { |value| read << String.new(value); nil }.should eq(3)
    read.should eq(values[0, 3])
    decoder.remaining.should eq(3)

    # Skipping consumes "banana"; the remaining values follow.
    decoder.skip(1).should eq(1)
    decoder.read(10) { |value| read << String.new(value); nil }.should eq(2)
    read.should eq(values[0, 3] + values[4, 2])
    decoder.read(10) { |_value| nil }.should eq(0)
  end

  it "rejects truncated suffix data" do
    values = ["abcdef", "abcxyz"]
    encoder = DeltaByteArrayEncoder(Parquet::ByteArray).new
    encoder.put(values.map { |value| Parquet::ByteArray.new(value) })
    encoded = encoder.flush_buffer

    # Drop the trailing suffix bytes so the first read hits EOF.
    truncated = encoded[0, encoded.size - 2]
    decoder = Decoders::DeltaByteArrayDecoder.new(truncated)
    expect_raises(Parquet::EOFError, "eof decoding byte array") do
      decoder.read(values.size) { |_value| nil }
    end
  end
end
