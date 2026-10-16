require "../../spec_helper"

# Ported from `parquet/src/encodings/encoding/alp_encoder.rs` and
# `parquet/src/encodings/decoding/alp_decoder.rs` test modules
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias AlpC = Parquet::Encodings::Alp
private alias AlpHeader = Parquet::Encodings::AlpHeader
private alias AlpInfo = Parquet::Encodings::AlpInfo
private alias AlpEncoder = Parquet::Encodings::AlpEncoder
private alias AlpDecoder = Parquet::Encodings::AlpDecoder

private def alp_bits(value : Float32) : UInt32
  value.unsafe_as(UInt32)
end

private def alp_bits(value : Float64) : UInt64
  value.unsafe_as(UInt64)
end

private def zero_value(type : Float32.class) : Float32
  0.0_f32
end

private def zero_value(type : Float64.class) : Float64
  0.0_f64
end

private def assert_bits_eq(actual : Array(F), expected : Array(F)) : Nil forall F
  actual.size.should eq(expected.size)
  actual.each_with_index do |value, idx|
    alp_bits(value).should eq(alp_bits(expected[idx]))
  end
end

private def alp_roundtrip(values : Array(F)) : Array(F) forall F
  encoder = AlpEncoder(F).new
  encoder.put(values)
  page = encoder.flush_buffer

  decoder = AlpDecoder(F).new
  decoder.set_data(page, values.size)
  output = Array(F).new(values.size) { zero_value(F) }
  decoder.get(output).should eq(values.size)
  output
end

private def push_u16(output : Array(UInt8), value : UInt16) : Nil
  output << (value & 0xFF).to_u8!
  output << ((value >> 8) & 0xFF).to_u8!
end

private def push_u32(output : Array(UInt8), value : UInt32) : Nil
  4.times { |i| output << ((value >> (8 * i)) & 0xFF).to_u8! }
end

private def push_u64(output : Array(UInt8), value : UInt64) : Nil
  8.times { |i| output << ((value >> (8 * i)) & 0xFF).to_u8! }
end

private def make_alp_page_bytes(
  compression_mode : UInt8,
  integer_encoding : UInt8,
  log_vector_size : UInt8,
  num_elements : Int32,
  offsets : Array(UInt32),
  body_tail_len : Int32,
) : Array(UInt8)
  output = [] of UInt8
  output << compression_mode
  output << integer_encoding
  output << log_vector_size
  push_u32(output, num_elements.to_u32!)
  offsets.each { |offset| push_u32(output, offset) }
  body_tail_len.times { output << 0_u8 }
  output
end

private def make_vector(
  exponent : UInt8,
  factor : UInt8,
  frame_of_reference : UInt64,
  bit_width : UInt8,
  packed_values : Array(UInt8),
  exception_positions : Array(UInt16),
  exception_values : Array(UInt64),
  width : Int32,
) : Array(UInt8)
  num_exceptions = exception_positions.size
  output = [] of UInt8
  output << exponent
  output << factor
  push_u16(output, num_exceptions.to_u16!)
  width.times { |i| output << ((frame_of_reference >> (8 * i)) & 0xFF).to_u8! }
  output << bit_width
  output.concat(packed_values)
  exception_positions.each { |position| push_u16(output, position) }
  exception_values.each { |value| width.times { |i| output << ((value >> (8 * i)) & 0xFF).to_u8! } }
  output
end

private def make_page_from_vectors(log_vector_size : UInt8, num_elements : Int32, vectors : Array(Array(UInt8))) : Array(UInt8)
  vector_size = 1 << log_vector_size
  expected = num_elements <= 0 ? 0 : (num_elements + vector_size - 1) // vector_size
  vectors.size.should eq(expected)

  offsets_section_size = vectors.size * 4
  offsets = [] of UInt32
  running = offsets_section_size.to_u32!
  vectors.each do |vector|
    offsets << running
    running += vector.size.to_u32!
  end

  page = make_alp_page_bytes(0_u8, 0_u8, log_vector_size, num_elements, offsets, 0)
  vectors.each { |vector| page.concat(vector) }
  page
end

private def to_bytes(array : Array(UInt8)) : Bytes
  Slice.new(array.to_unsafe, array.size).dup
end

private def decode_page(type : F.class, page : Array(UInt8), num_values : Int32) : Array(F) forall F
  decoder = AlpDecoder(F).new
  decoder.set_data(to_bytes(page), num_values)
  output = Array(F).new(num_values) { zero_value(F) }
  decoder.get(output).should eq(num_values)
  output
end

private def decode_err(type : F.class, page : Array(UInt8), num_values : Int32) : Parquet::ParquetError forall F
  decoder = AlpDecoder(F).new
  begin
    decoder.set_data(to_bytes(page), num_values)
  rescue ex : Parquet::ParquetError
    return ex
  end
  output = Array(F).new(num_values) { zero_value(F) }
  err = expect_raises(Parquet::ParquetError) { decoder.get(output) }
  err
end

describe Parquet::Encodings::AlpHeader do
  it "round-trips serialize/deserialize" do
    header = AlpHeader.new(AlpC::ALP_COMPRESSION_MODE, AlpC::ALP_INTEGER_ENCODING_FOR_BIT_PACK, 8, 1234)
    bytes = header.serialize
    parsed = AlpHeader.deserialize(bytes)
    parsed.compression_mode.should eq(header.compression_mode)
    parsed.integer_encoding.should eq(header.integer_encoding)
    parsed.vector_size.should eq(header.vector_size)
    parsed.num_elements.should eq(header.num_elements)
  end

  it "rejects a num_elements overflow" do
    header = AlpHeader.new(0_u8, 0_u8, 8, Int32::MAX.to_i64 + 1)
    err = expect_raises(Parquet::ParquetError) { header.serialize }
    err.to_s.should contain("exceeds i32::MAX")
  end

  it "rejects a non-power-of-two vector size" do
    header = AlpHeader.new(0_u8, 0_u8, 100, 4)
    err = expect_raises(Parquet::ParquetError) { header.serialize }
    err.to_s.should contain("is not a power of two")
  end

  it "rejects a short header" do
    err = expect_raises(Parquet::ParquetError) { AlpHeader.deserialize(Bytes[0_u8, 1_u8, 2_u8]) }
    err.to_s.should contain("Invalid ALP page: expected at least 7 bytes for header")
  end

  it "rejects a negative num_elements" do
    bytes = Array(UInt8).new(AlpC::ALP_HEADER_SIZE, 0_u8)
    bytes[2] = AlpC::ALP_MIN_LOG_VECTOR_SIZE
    4.times { |i| bytes[3 + i] = (((-1_i32) >> (8 * i)) & 0xFF).to_u8! }
    err = expect_raises(Parquet::ParquetError) { AlpHeader.deserialize(to_bytes(bytes)) }
    err.to_s.should contain("Invalid ALP page: num_elements -1 must be >= 0")
  end

  it "rejects a log_vector_size shift overflow" do
    bytes = Array(UInt8).new(AlpC::ALP_HEADER_SIZE, 0_u8)
    bytes[2] = 64_u8
    err = expect_raises(Parquet::ParquetError) { AlpHeader.deserialize(to_bytes(bytes)) }
    err.to_s.should contain("too large to represent a vector size")
  end
end

describe Parquet::Encodings::AlpEncoder do
  it "round-trips f64 decimals" do
    values = (0...500).map { |i| i.to_f64 * 0.01 + 1.23 }
    assert_bits_eq(alp_roundtrip(values), values)
  end

  it "round-trips f32 decimals" do
    values = (0...500).map { |i| i.to_f32 * 0.5_f32 + 1.25_f32 }
    assert_bits_eq(alp_roundtrip(values), values)
  end

  it "round-trips multiple vectors with a short trailing one" do
    values = (0...2600).map { |i| i.to_f64 * 0.001 }
    assert_bits_eq(alp_roundtrip(values), values)
  end

  it "round-trips exceptions including -0.0, NaN and infinities" do
    values = [
      1.5_f64,
      Float64::NAN,
      2.5_f64,
      Float64::INFINITY,
      -0.0_f64,
      -Float64::INFINITY,
      3.5_f64,
      0.0_f64,
      Float64::MAX,
      Float64::MIN,
    ]
    decoded = alp_roundtrip(values)
    assert_bits_eq(decoded, values)
    decoded[1].nan?.should be_true
    decoded[4].sign.should eq(0)
    (1.0 / decoded[4]).should eq(-Float64::INFINITY)
  end

  it "round-trips all-identical values with a zero bit width" do
    values = Array(Float64).new(3000, 42.42_f64)
    assert_bits_eq(alp_roundtrip(values), values)
  end

  it "round-trips all-NaN values" do
    values = Array(Float64).new(100, Float64::NAN)
    decoded = alp_roundtrip(values)
    decoded.each(&.nan?.should be_true)
  end

  it "round-trips a single value" do
    assert_bits_eq(alp_roundtrip([3.25_f64]), [3.25_f64])
  end

  it "round-trips an empty page" do
    encoder = AlpEncoder(Float64).new
    page = encoder.flush_buffer
    page.size.should eq(AlpC::ALP_HEADER_SIZE)

    decoder = AlpDecoder(Float64).new
    decoder.set_data(page, 0)
    decoder.values_left.should eq(0)
  end

  it "round-trips multiple pages with a reused encoder" do
    encoder = AlpEncoder(Float64).new
    3.times do |page_idx|
      values = (0...1500).map { |i| i.to_f64 * 0.01 + page_idx.to_f64 }
      encoder.put(values)
      page = encoder.flush_buffer

      decoder = AlpDecoder(Float64).new
      decoder.set_data(page, values.size)
      output = Array(Float64).new(values.size, 0.0_f64)
      decoder.get(output).should eq(values.size)
      assert_bits_eq(output, values)
    end
  end

  it "selects decimal parameters and compresses" do
    values = (0...1024).map { |i| i.to_f64 * 0.01 }
    encoder = AlpEncoder(Float64).new
    encoder.put(values)
    page = encoder.flush_buffer

    exponent = page[AlpC::ALP_HEADER_SIZE + 4]
    factor = page[AlpC::ALP_HEADER_SIZE + 5]
    num_exceptions = page[AlpC::ALP_HEADER_SIZE + 6].to_u16! | (page[AlpC::ALP_HEADER_SIZE + 7].to_u16! << 8)

    (exponent - factor).should eq(2)
    num_exceptions.should eq(0)

    plain_size = values.size * 8
    (page.size * 4).should be < plain_size
  end

  it "keeps the bit width tight for a single exception" do
    values = (0...1024).map { |i| i.to_f64 * 0.01 }
    values[500] = Float64::NAN

    encoder = AlpEncoder(Float64).new
    encoder.put(values)
    page = encoder.flush_buffer

    bit_width = page[AlpC::ALP_HEADER_SIZE + 4 + AlpInfo::STORED_SIZE + 8]
    bit_width.should be <= 17

    assert_bits_eq(alp_roundtrip(values), values)
  end

  it "streams pages byte-for-byte identically to single-pass encoding" do
    page1 = (0...3000).map { |i| i.to_f64 * 0.01 + 1.23 }
    page2 = (0...3000).map { |i| i.to_f64 * 0.03 - 7.0 }

    encoder = AlpEncoder(Float64).new
    encoder.put(page1)
    encoder.flush_buffer

    page2.each_slice(997) { |chunk| encoder.put(chunk) }
    streamed = encoder.flush_buffer

    preset = AlpC.build_preset(Float64, page1)
    scratch = AlpC::Scratch(Float64).new
    reference = AlpC.encode_page(Float64, page2, preset, scratch)

    streamed.should eq(reference)
  end

  it "reassembles vectors across irregular puts" do
    page1 = (0...2048).map { |i| i.to_f64 * 0.01 }
    page2 = (0...5000).map { |i| i.to_f64 * 0.01 + 100.0 }

    encoder = AlpEncoder(Float64).new
    encoder.put(page1)
    encoder.flush_buffer

    sizes = [1, 1023, 2, 1024, 1025, 7, 900, 118]
    offset = 0
    i = 0
    while offset < page2.size
      n = Math.min(sizes[i % sizes.size], page2.size - offset)
      encoder.put(page2[offset, n])
      offset += n
      i += 1
    end
    page = encoder.flush_buffer

    decoder = AlpDecoder(Float64).new
    decoder.set_data(page, page2.size)
    output = Array(Float64).new(page2.size, 0.0_f64)
    decoder.get(output).should eq(page2.size)
    assert_bits_eq(output, page2)
  end

  it "finishes a streaming page on a vector boundary" do
    preset_page = (0...100).map { |i| i.to_f64 * 0.01 }
    values = (0...(2 * AlpC::VECTOR_SIZE)).map { |i| i.to_f64 * 0.01 }

    encoder = AlpEncoder(Float64).new
    encoder.put(preset_page)
    encoder.flush_buffer

    empty_page_estimate = encoder.estimated_data_encoded_size
    encoder.put(values)
    (encoder.estimated_data_encoded_size > empty_page_estimate).should be_true
    page = encoder.flush_buffer

    decoder = AlpDecoder(Float64).new
    decoder.set_data(page, values.size)
    output = Array(Float64).new(values.size, 0.0_f64)
    decoder.get(output).should eq(values.size)
    assert_bits_eq(output, values)
  end

  it "tracks buffers with estimated_memory_size" do
    encoder = AlpEncoder(Float64).new
    encoder.estimated_memory_size.should eq(0)

    values = (0...1500).map { |i| i.to_f64 * 0.01 }
    encoder.put(values)
    (encoder.estimated_memory_size >= values.size * 8).should be_true

    encoder.flush_buffer
    after_flush = encoder.estimated_memory_size

    encoder.put(values)
    (encoder.estimated_memory_size > after_flush).should be_true
  end

  it "covers wide packed values with exceptions in the data size estimate" do
    values = Array(Float64).new(AlpC::VECTOR_SIZE, Float64::NAN)
    values[0] = AlpC.encoding_lower_limit(Float64)
    values[4] = 1024.0_f64

    encoder = AlpEncoder(Float64).new
    encoder.put(values)
    estimated_size = encoder.estimated_data_encoded_size
    page = encoder.flush_buffer

    vector_start = AlpC::ALP_HEADER_SIZE + 4
    num_exceptions = page[vector_start + 2].to_u16! | (page[vector_start + 3].to_u16! << 8)
    bit_width = page[vector_start + AlpInfo::STORED_SIZE + 8]
    num_exceptions.should eq((AlpC::VECTOR_SIZE - 2).to_u16!)
    bit_width.should eq(64_u8)
    (estimated_size >= page.size).should be_true
  end

  it "fills the preset and handles an all-exception vector" do
    scales = [0.01, 0.01, 0.001, 0.01, 0.0001, 0.01, 0.00001, 0.01]
    values = [] of Float64
    scales.each { |scale| values.concat((0...AlpC::VECTOR_SIZE).map { |i| i.to_f64 * scale }) }
    values.concat(Array(Float64).new(AlpC::VECTOR_SIZE, Float64::NAN))

    preset = AlpC.build_preset(Float64, values)
    preset.size.should eq(AlpC::MAX_COMBINATIONS)

    nan_vector = values[8 * AlpC::VECTOR_SIZE, AlpC::VECTOR_SIZE]
    sample = [] of Float64
    AlpC.sample_values(Float64, nan_vector, sample)
    costs = preset.map { |params| AlpC.estimate_size_bits(Float64, sample, params, false) }
    costs.each_cons(2) { |pair| pair[0].should eq(pair[1]) }

    assert_bits_eq(alp_roundtrip(values), values)
  end
end

describe Parquet::Encodings::AlpDecoder do
  it "decodes a valid page of zeros" do
    data = make_alp_page_bytes(0_u8, 0_u8, 3_u8, 4, [4_u32], 13)
    decoded = decode_page(Float64, data, 4)
    decoded.should eq(Array(Float64).new(4, 0.0_f64))
  end

  it "rejects a small vector size" do
    data = make_alp_page_bytes(0_u8, 0_u8, 2_u8, 1, [4_u32], 8)
    decode_err(Float64, data, 1).to_s.should contain("Invalid ALP page: log_vector_size 2 below min 3")
  end

  it "rejects a large vector size" do
    data = make_alp_page_bytes(0_u8, 0_u8, 16_u8, 1, [4_u32], 8)
    decode_err(Float64, data, 1).to_s.should contain("Invalid ALP page: log_vector_size 16 exceeds max 15")
  end

  it "rejects an integer encoding" do
    data = make_alp_page_bytes(0_u8, 1_u8, 3_u8, 1, [4_u32], 8)
    decode_err(Float64, data, 1).to_s.should contain("Invalid ALP page: unsupported integer encoding 1")
  end

  it "rejects a compression mode" do
    data = make_alp_page_bytes(1_u8, 0_u8, 3_u8, 1, [4_u32], 8)
    decode_err(Float64, data, 1).to_s.should contain("Invalid ALP page: unsupported compression mode 1")
  end

  it "rejects num_elements exceeding num_values" do
    data = make_alp_page_bytes(0_u8, 0_u8, 3_u8, 5, [4_u32], 8)
    decode_err(Float32, data, 2).to_s.should contain("Invalid ALP page: header num_elements 5 exceeds page num_values 2")
  end

  it "rejects a short offsets section" do
    data = make_alp_page_bytes(0_u8, 0_u8, 3_u8, 9, [4_u32], 0)
    decode_err(Float32, data, 9).to_s.should contain("Invalid ALP page: expected at least 8 bytes for 2 offsets, got 4")
  end

  it "rejects an invalid f32 exponent" do
    vector = make_vector(11_u8, 0_u8, 0_u64, 0_u8, [] of UInt8, [] of UInt16, [] of UInt64, 4)
    page = make_page_from_vectors(3_u8, 1, [vector])
    decode_err(Float32, page, 1).to_s.should contain("Invalid ALP page: exponent 11 exceeds max 10")
  end

  it "rejects a factor exceeding the exponent" do
    vector = make_vector(2_u8, 3_u8, 0_u64, 0_u8, [] of UInt8, [] of UInt16, [] of UInt64, 4)
    page = make_page_from_vectors(3_u8, 1, [vector])
    decode_err(Float32, page, 1).to_s.should contain("Invalid ALP page: factor 3 exceeds exponent 2")
  end

  it "rejects an invalid num_exceptions" do
    vector = make_vector(0_u8, 0_u8, 0_u64, 0_u8, [] of UInt8, [0_u16, 0_u16], [0_u64, 0_u64], 4)
    page = make_page_from_vectors(3_u8, 1, [vector])
    decode_err(Float32, page, 1).to_s.should contain("Invalid ALP page: num_exceptions 2 exceeds vector num_elements 1")
  end

  it "rejects short vector metadata" do
    page = make_page_from_vectors(3_u8, 1, [[0_u8, 0_u8, 0_u8]])
    decode_err(Float32, page, 1).to_s.should contain("Invalid ALP page: vector metadata too short, expected at least 9 bytes, got 3")
  end

  it "rejects a bit width overflow" do
    vector = make_vector(0_u8, 0_u8, 0_u64, 33_u8, [] of UInt8, [] of UInt16, [] of UInt64, 4)
    page = make_page_from_vectors(3_u8, 1, [vector])
    decode_err(Float32, page, 1).to_s.should contain("Invalid ALP page: bit width 33 exceeds 32")
  end

  it "rejects an out-of-bounds vector offset" do
    vector0 = make_vector(0_u8, 0_u8, 0_u64, 1_u8, [0_u8], [] of UInt16, [] of UInt64, 4)
    page = make_alp_page_bytes(0_u8, 0_u8, 3_u8, 12, [8_u32, 200_u32], 0)
    page.concat(vector0)
    decode_err(Float32, page, 12).to_s.should contain("Invalid ALP page: vector offset 200 out of bounds at index 0")
  end

  it "rejects an invalid exception position" do
    vector = make_vector(0_u8, 0_u8, 0_u64, 0_u8, [] of UInt8, [1_u16], [123_u64], 4)
    page = make_page_from_vectors(3_u8, 1, [vector])
    decode_err(Float32, page, 1).to_s.should contain("Invalid ALP page: exception position 1 out of bounds for vector length 1")
  end

  it "rejects a non-contiguous offset" do
    data = make_alp_page_bytes(0_u8, 0_u8, 3_u8, 9, [12_u32, 8_u32], 12)
    decode_err(Float64, data, 9).to_s.should contain("Invalid ALP page: vector offset 12 at index 0 does not match expected 8")
  end

  it "rejects a truncated vector" do
    vector = [] of UInt8
    vector << 0_u8
    vector << 0_u8
    push_u16(vector, 0_u16)
    push_u32(vector, 10_u32)
    vector << 1_u8
    page = make_page_from_vectors(3_u8, 2, [vector])
    decode_err(Float32, page, 2).to_s.should contain("Invalid ALP page: vector data too short")
  end

  it "rejects a vector that is too long" do
    vector = make_vector(0_u8, 0_u8, 0_u64, 0_u8, [] of UInt8, [] of UInt16, [] of UInt64, 4)
    vector << 0xAB_u8
    page = make_page_from_vectors(3_u8, 1, [vector])
    decode_err(Float32, page, 1).to_s.should contain("Invalid ALP page: vector data too long, expected 9 bytes, got 10")
  end

  it "rejects unclaimed bytes between offsets and vector data" do
    vector = make_vector(0_u8, 0_u8, 0_u64, 0_u8, [] of UInt8, [] of UInt16, [] of UInt64, 4)
    page = make_alp_page_bytes(0_u8, 0_u8, 3_u8, 1, [5_u32], 0)
    page << 0_u8
    page.concat(vector)
    decode_err(Float32, page, 1).to_s.should contain("Invalid ALP page: vector offset 5 at index 0 does not match expected 4")
  end

  it "parses exception sections" do
    vector = [] of UInt8
    vector << 2_u8
    vector << 0_u8
    push_u16(vector, 1_u16)
    push_u64(vector, 10_u64)
    vector << 0_u8
    push_u16(vector, 0_u16)
    push_u64(vector, 42.5_f64.unsafe_as(UInt64))

    body = to_bytes(vector)
    view = AlpC.parse_vector_view(Float64, body, 0, body.size, 1)
    view.num_elements.should eq(1)
    view.alp_info.num_exceptions.should eq(1)
    view.for_info.bit_width.should eq(0)

    positions = read_positions(body, view.exception_positions, view.alp_info.num_exceptions)
    values = read_values(body, view.exception_values, view.alp_info.num_exceptions, 8)
    positions.should eq([0_u16])
    values.should eq([42.5_f64.unsafe_as(UInt64)])
  end

  it "decodes f32 values with no exceptions" do
    vector = make_vector(0_u8, 0_u8, 10_u64, 2_u8, [0b1110_0100_u8], [] of UInt16, [] of UInt64, 4)
    page = make_page_from_vectors(3_u8, 4, [vector])
    decode_page(Float32, page, 4).should eq([10.0_f32, 11.0_f32, 12.0_f32, 13.0_f32])
  end

  it "decodes f64 multi-vector with exceptions" do
    vector0 = make_vector(0_u8, 0_u8, 10_u64, 1_u8, [0b0000_0010_u8], [1_u16], [42.5_f64.unsafe_as(UInt64)], 8)
    vector1 = make_vector(0_u8, 0_u8, 7_u64, 0_u8, [] of UInt8, [] of UInt16, [] of UInt64, 8)
    page = make_page_from_vectors(3_u8, 9, [vector0, vector1])
    decode_page(Float64, page, 9).should eq([10.0, 42.5, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 7.0])
  end

  it "round-trips f32 edge values via exceptions" do
    edge = [Float32::NAN.unsafe_as(UInt64), (-0.0_f32).unsafe_as(UInt64), Float32::INFINITY.unsafe_as(UInt64)]
    vector = make_vector(0_u8, 0_u8, 0_u64, 0_u8, [] of UInt8, [0_u16, 1_u16, 2_u16], edge, 4)
    page = make_page_from_vectors(3_u8, 3, [vector])
    decoded = decode_page(Float32, page, 3)

    decoded[0].nan?.should be_true
    decoded[1].should eq(-0.0_f32)
    (1.0_f32 / decoded[1]).should eq(-Float32::INFINITY)
    decoded[2].should eq(Float32::INFINITY)
  end

  it "uses a two-step decimal multiply for f32" do
    encoded = 1_970_570_984_i32
    vector = make_vector(1_u8, 1_u8, encoded.to_u32!.to_u64, 0_u8, [] of UInt8, [] of UInt16, [] of UInt64, 4)
    page = make_page_from_vectors(3_u8, 1, [vector])
    decoded = decode_page(Float32, page, 1)

    two_step = ((encoded.to_f32) * AlpC::ALP_POW10_F32[1]) * AlpC::ALP_NEG_POW10_F32[1]
    one_step = (encoded.to_f32) * (AlpC::ALP_POW10_F32[1] * AlpC::ALP_NEG_POW10_F32[1])
    decoded[0].unsafe_as(UInt32).should eq(two_step.unsafe_as(UInt32))
    decoded[0].unsafe_as(UInt32).should_not eq(one_step.unsafe_as(UInt32))
  end

  it "uses a two-step decimal multiply for f64" do
    encoded = -3_900_047_474_048_127_703_i64
    vector = make_vector(1_u8, 1_u8, encoded.unsafe_as(UInt64), 0_u8, [] of UInt8, [] of UInt16, [] of UInt64, 8)
    page = make_page_from_vectors(3_u8, 1, [vector])
    decoded = decode_page(Float64, page, 1)

    two_step = ((encoded.to_f64) * AlpC::ALP_POW10_F64[1]) * AlpC::ALP_NEG_POW10_F64[1]
    one_step = (encoded.to_f64) * (AlpC::ALP_POW10_F64[1] * AlpC::ALP_NEG_POW10_F64[1])
    decoded[0].unsafe_as(UInt64).should eq(two_step.unsafe_as(UInt64))
    decoded[0].unsafe_as(UInt64).should_not eq(one_step.unsafe_as(UInt64))
  end

  it "gets values across vectors" do
    vector0 = make_vector(0_u8, 0_u8, 10_u64, 1_u8, [0b0000_0010_u8], [] of UInt16, [] of UInt64, 4)
    vector1 = make_vector(0_u8, 0_u8, 20_u64, 1_u8, [0b0000_0010_u8], [] of UInt16, [] of UInt64, 4)
    page = make_page_from_vectors(3_u8, 12, [vector0, vector1])

    decoder = AlpDecoder(Float32).new
    decoder.set_data(to_bytes(page), 12)

    first = Array(Float32).new(9, 0.0_f32)
    decoder.get(first).should eq(9)
    first.should eq([10.0_f32, 11.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 20.0])
    decoder.values_left.should eq(3)

    second = Array(Float32).new(3, 0.0_f32)
    decoder.get(second).should eq(3)
    second.should eq([21.0_f32, 20.0, 20.0])
    decoder.values_left.should eq(0)
  end

  it "skips values across vectors" do
    vector0 = make_vector(0_u8, 0_u8, 10_u64, 1_u8, [0b0000_0010_u8], [] of UInt16, [] of UInt64, 4)
    vector1 = make_vector(0_u8, 0_u8, 20_u64, 1_u8, [0b0000_0010_u8], [] of UInt16, [] of UInt64, 4)
    page = make_page_from_vectors(3_u8, 12, [vector0, vector1])

    decoder = AlpDecoder(Float32).new
    decoder.set_data(to_bytes(page), 12)

    decoder.skip(9).should eq(9)
    decoder.values_left.should eq(3)

    output = Array(Float32).new(1, 0.0_f32)
    decoder.get(output).should eq(1)
    output[0].should eq(21.0_f32)
    decoder.values_left.should eq(2)
  end

  it "skips within the current vector" do
    vector0 = make_vector(0_u8, 0_u8, 10_u64, 1_u8, [0b0000_0010_u8], [] of UInt16, [] of UInt64, 4)
    vector1 = make_vector(0_u8, 0_u8, 20_u64, 1_u8, [0b0000_0010_u8], [] of UInt16, [] of UInt64, 4)
    page = make_page_from_vectors(3_u8, 12, [vector0, vector1])

    decoder = AlpDecoder(Float32).new
    decoder.set_data(to_bytes(page), 12)

    head = Array(Float32).new(2, 0.0_f32)
    decoder.get(head).should eq(2)
    head.should eq([10.0_f32, 11.0_f32])

    decoder.skip(3).should eq(3)
    decoder.values_left.should eq(7)

    tail = Array(Float32).new(4, 0.0_f32)
    decoder.get(tail).should eq(4)
    tail.should eq([10.0_f32, 10.0, 10.0, 20.0])
  end

  it "reads a full page" do
    vector0 = make_vector(0_u8, 0_u8, 10_u64, 1_u8, [0b0000_0010_u8], [] of UInt16, [] of UInt64, 4)
    vector1 = make_vector(0_u8, 0_u8, 20_u64, 1_u8, [0b0000_0010_u8], [] of UInt16, [] of UInt64, 4)
    page = make_page_from_vectors(3_u8, 12, [vector0, vector1])

    decoder = AlpDecoder(Float32).new
    decoder.set_data(to_bytes(page), 12)

    output = Array(Float32).new(12, 0.0_f32)
    decoder.get(output).should eq(12)
    output.should eq([10.0_f32, 11.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 20.0, 21.0, 20.0, 20.0])
    decoder.values_left.should eq(0)

    extra = Array(Float32).new(1, 0.0_f32)
    decoder.get(extra).should eq(0)
  end

  it "skips to the same place as a sequential decode" do
    specs = [
      {1000_u64, 4, 1024, 100_u16, Float64::NAN},
      {5000_u64, 8, 1024, 300_u16, Float64::INFINITY},
      {100_u64, 2, 552, 200_u16, -0.0_f64},
    ]

    vectors = [] of Array(UInt8)
    expected = [] of Float64
    specs.each do |(for_ref, bit_width, len, exc_pos, exc_val)|
      modulo = 1_u64 << bit_width
      deltas = (0...len).map { |j| j.to_u64! % modulo }
      packed = pack_deltas(deltas, bit_width)
      vectors << make_vector(0_u8, 0_u8, for_ref, bit_width.to_u8!, packed, [exc_pos], [exc_val.unsafe_as(UInt64)], 8)
      deltas.each_with_index do |delta, j|
        expected << (j.to_u16 == exc_pos ? exc_val : (for_ref + delta).to_f64)
      end
    end
    n = expected.size
    page = make_page_from_vectors(10_u8, n, vectors)

    assert_bits_eq(decode_page(Float64, page, n), expected)

    [0, 1, 1023, 1024, 1025, 2048, 2222, 2599, 2600].each do |k|
      decoder = AlpDecoder(Float64).new
      decoder.set_data(to_bytes(page), n)
      decoder.skip(k).should eq(k)
      decoder.values_left.should eq(n - k)

      tail = Array(Float64).new(n - k, 0.0_f64)
      decoder.get(tail).should eq(n - k)
      assert_bits_eq(tail, expected[k..])
    end

    decoder = AlpDecoder(Float64).new
    decoder.set_data(to_bytes(page), n)
    head = Array(Float64).new(500, 0.0_f64)
    decoder.get(head).should eq(500)
    assert_bits_eq(head, expected[0, 500])
    decoder.skip(700).should eq(700)
    decoder.values_left.should eq(n - 1200)
    tail = Array(Float64).new(n - 1200, 0.0_f64)
    decoder.get(tail).should eq(n - 1200)
    assert_bits_eq(tail, expected[1200..])

    decoder = AlpDecoder(Float64).new
    decoder.set_data(to_bytes(page), n)
    decoder.skip(1024).should eq(1024)
    decoder.skip(600).should eq(600)
    tail = Array(Float64).new(n - 1624, 0.0_f64)
    decoder.get(tail).should eq(n - 1624)
    assert_bits_eq(tail, expected[1624..])
  end
end

private def pack_deltas(deltas : Array(UInt64), bit_width : Int32) : Array(UInt8)
  writer = BitWriter.new(deltas.size * 2 + 16)
  deltas.each { |delta| writer.put_value(delta, bit_width) }
  writer.flush
  writer.consume.to_a
end

private def read_positions(body : Bytes, offset : Int32, count : UInt16) : Array(UInt16)
  (0...count).map do |i|
    body[offset + i * 2].to_u16! | (body[offset + i * 2 + 1].to_u16! << 8)
  end
end

private def read_values(body : Bytes, offset : Int32, count : UInt16, width : Int32) : Array(UInt64)
  (0...count).map do |i|
    value = 0_u64
    width.times { |b| value |= body[offset + i * width + b].to_u64 << (8 * b) }
    value
  end
end
