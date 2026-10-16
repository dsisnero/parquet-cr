require "../../spec_helper"

# Ported from `parquet/src/encodings/rle.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# The rand-driven `test_random` is ported with a deterministic parity generator.

BIT_PACK_GROUP_SIZE = 8

private def validate_rle(values : Array(Int64), bit_width : Int32, expected : Bytes?, expected_len : Int32) : Nil
  encoder = RleEncoder.new(bit_width, 64 * 1024)
  values.each { |v| encoder.put(v.to_u64!) }
  buffer = encoder.consume

  buffer.size.should eq(expected_len) unless expected_len == -1
  buffer.should eq(expected) unless expected.nil?

  decoder = RleDecoder.new(bit_width)
  decoder.set_data(buffer)
  values.each do |expected_value|
    decoder.get(Int64).should eq(expected_value)
  end

  decoder.set_data(buffer)
  values_read = Array(Int64).new(values.size, 0_i64)
  decoder.get_batch(Int64, values_read)
  values_read.should eq(values)
end

describe Parquet::Encodings::RleDecoder do
  # Upstream `test_rle_decode_int32`.
  it "decodes a bit-packed run" do
    decoder = RleDecoder.new(3)
    decoder.set_data(Bytes[0x03_u8, 0x88_u8, 0xC6_u8, 0xFA_u8])
    buffer = Array(Int32).new(BIT_PACK_GROUP_SIZE, 0)
    decoder.get_batch(Int32, buffer).should eq(BIT_PACK_GROUP_SIZE)
    buffer.should eq([0, 1, 2, 3, 4, 5, 6, 7])
  end

  # Upstream `test_rle_skip_int32`.
  it "skips and reads the remainder" do
    decoder = RleDecoder.new(3)
    decoder.set_data(Bytes[0x03_u8, 0x88_u8, 0xC6_u8, 0xFA_u8])
    decoder.skip(2).should eq(2)
    buffer = Array(Int32).new(6, 0)
    decoder.get_batch(Int32, buffer).should eq(6)
    buffer.should eq([2, 3, 4, 5, 6, 7])
  end

  # Upstream `test_rle_decode_bool`.
  it "decodes boolean runs" do
    rle_data = Bytes[0x64_u8, 0x01_u8, 0x64_u8, 0x00_u8]
    packed_data = Bytes[0x1B_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0x0A_u8]

    decoder = RleDecoder.new(1)
    decoder.set_data(rle_data)
    buffer = Array(Bool).new(100, false)
    decoder.get_batch(Bool, buffer)
    buffer.should eq((0...100).map { |i| i < 50 })

    decoder.set_data(packed_data)
    buffer = Array(Bool).new(100, false)
    decoder.get_batch(Bool, buffer)
    buffer.should eq((0...100).map { |i| i.odd? })
  end

  # Upstream `test_rle_skip_bool`.
  it "skips boolean runs" do
    rle_data = Bytes[0x64_u8, 0x01_u8, 0x64_u8, 0x00_u8]
    packed_data = Bytes[0x1B_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0xAA_u8, 0x0A_u8]

    decoder = RleDecoder.new(1)
    decoder.set_data(rle_data)
    buffer = Array(Bool).new(50, true)
    decoder.skip(50).should eq(50)
    decoder.get_batch(Bool, buffer).should eq(50)
    buffer.should eq(Array(Bool).new(50, false))

    decoder.set_data(packed_data)
    buffer = Array(Bool).new(50, false)
    decoder.skip(50).should eq(50)
    decoder.get_batch(Bool, buffer).should eq(50)
    buffer.should eq((0...50).map { |i| i.odd? })
  end

  # Upstream `test_rle_decode_with_dict_int32`.
  it "decodes dictionary indices" do
    dict = [10, 20, 30]
    decoder = RleDecoder.new(3)
    decoder.set_data(Bytes[0x06_u8, 0x00_u8, 0x08_u8, 0x01_u8, 0x0A_u8, 0x02_u8])
    buffer = Array(Int32).new(12, 0)
    decoder.get_batch_with_dict(Int32, dict, buffer, 12).should eq(12)
    buffer.should eq([10, 10, 10, 20, 20, 20, 20, 30, 30, 30, 30, 30])

    str_dict = ["aaa", "bbb", "ccc", "ddd", "eee", "fff"]
    decoder = RleDecoder.new(3)
    decoder.set_data(Bytes[0x03_u8, 0x63_u8, 0xC7_u8, 0x8E_u8, 0x03_u8, 0x65_u8, 0x0B_u8])
    buffer = Array(String).new(12, "")
    decoder.get_batch_with_dict(String, str_dict, buffer, 12).should eq(12)
    buffer.should eq(["ddd", "eee", "fff", "ddd", "eee", "fff", "ddd", "eee", "fff", "eee", "fff", "fff"])
  end

  # Upstream `test_rle_skip_dict`.
  it "skips dictionary indices" do
    dict = [10, 20, 30]
    decoder = RleDecoder.new(3)
    decoder.set_data(Bytes[0x06_u8, 0x00_u8, 0x08_u8, 0x01_u8, 0x0A_u8, 0x02_u8])
    decoder.skip(2).should eq(2)
    buffer = Array(Int32).new(10, 0)
    decoder.get_batch_with_dict(Int32, dict, buffer, 10).should eq(10)
    buffer.should eq([10, 20, 20, 20, 20, 30, 30, 30, 30, 30])

    str_dict = ["aaa", "bbb", "ccc", "ddd", "eee", "fff"]
    decoder = RleDecoder.new(3)
    decoder.set_data(Bytes[0x03_u8, 0x63_u8, 0xC7_u8, 0x8E_u8, 0x03_u8, 0x65_u8, 0x0B_u8])
    decoder.skip(4).should eq(4)
    buffer = Array(String).new(BIT_PACK_GROUP_SIZE, "")
    decoder.get_batch_with_dict(String, str_dict, buffer, BIT_PACK_GROUP_SIZE).should eq(BIT_PACK_GROUP_SIZE)
    buffer.should eq(["eee", "fff", "ddd", "eee", "fff", "eee", "fff", "fff"])
  end

  # Upstream `test_truncated_rle`.
  it "handles truncated final bit-packed runs" do
    data = [((3 << 1) | 1).to_u8]
    data.concat(Array(UInt8).new(20, 0xFF_u8))
    bytes = Slice.new(data.to_unsafe, data.size).dup

    decoder = RleDecoder.new(8)
    decoder.set_data(bytes)
    output = Array(UInt16).new(100, 0_u16)
    decoder.get_batch(UInt16, output).should eq(20)
    output[0, 20].each { |x| x.should eq(255_u16) }

    decoder.set_data(bytes)
    dict = (0...256).map(&.to_u16)
    output = Array(UInt16).new(100, 0_u16)
    decoder.get_batch_with_dict(UInt16, dict, output, 100).should eq(20)
    output[0, 20].each { |x| x.should eq(255_u16) }
  end

  # Upstream `test_long_run`.
  it "decodes runs longer than 504 values" do
    writer = BitWriter.new(1024)
    num_values = 2002
    run_bytes = Parquet::Util::BitUtil.ceil(num_values, 8).to_u64
    writer.put_vlq_int((run_bytes << 1) | 1)
    run_bytes.times { writer.put_aligned(0xFF_u8, 1) }
    buffer = writer.consume

    decoder = RleDecoder.new(1)
    decoder.set_data(buffer)
    decoded = Array(Int16).new(num_values, 0_i16)
    decoder.get_batch(Int16, decoded).should eq(num_values)
    decoded.should eq(Array(Int16).new(num_values, 1_i16))

    decoder.set_data(buffer)
    decoder.get_batch_with_dict(Int16, [0_i16, 23_i16], decoded, num_values).should eq(num_values)
    decoded.should eq(Array(Int16).new(num_values, 23_i16))
  end
end

describe Parquet::Encodings::RleEncoder do
  # Upstream `test_rle_consume_flush_buffer`.
  it "flushes the same bytes as consume" do
    data = [1, 1, 1, 2, 2, 3, 3, 3]
    encoder1 = RleEncoder.new(3, 256)
    encoder2 = RleEncoder.new(3, 256)
    data.each do |value|
      encoder1.put(value.to_u64)
      encoder2.put(value.to_u64)
    end
    encoder1.flush_buffer.should eq(encoder2.consume)
  end

  # Upstream `test_rle_specific_sequences`.
  it "encodes specific sequences" do
    values = Array(Int64).new(50, 0_i64) + Array(Int64).new(50, 1_i64)
    expected = Bytes[(50 << 1).to_u8, 0_u8, (50 << 1).to_u8, 1_u8]
    (1..8).each { |width| validate_rle(values, width, expected, 4) }
    (9..32).each do |width|
      validate_rle(values, width, nil, 2 * (1 + Parquet::Util::BitUtil.ceil(width, 8)))
    end

    alternating = Array(Int64).new(101) { |i| (i % 2).to_i64 }
    num_groups = Parquet::Util::BitUtil.ceil(100, BIT_PACK_GROUP_SIZE)
    expected2 = [((num_groups << 1) | 1).to_u8]
    (100 // BIT_PACK_GROUP_SIZE).times { expected2 << 0b10101010_u8 }
    expected2 << 0b00001010_u8
    validate_rle(alternating, 1, Slice.new(expected2.to_unsafe, expected2.size).dup, 1 + num_groups)
    (2..32).each do |width|
      num_values = Parquet::Util::BitUtil.ceil(100, BIT_PACK_GROUP_SIZE) * BIT_PACK_GROUP_SIZE
      validate_rle(alternating, width, nil, 1 + Parquet::Util::BitUtil.ceil(width * num_values, 8))
    end
  end

  # Upstream `test_values`.
  it "round-trips values across bit widths" do
    (1..32).each do |width|
      test_rle_values(width, 1, -1)
      test_rle_values(width, 1024, -1)
      test_rle_values(width, 1024, 0)
      test_rle_values(width, 1024, 1)
    end
  end

  # Upstream `test_rle_padded`.
  it "decodes padded bit-packed runs" do
    values = [0_i16, 1_i16, 1_i16, 3_i16, 1_i16, 0_i16]
    bit_width = 2
    buffer_len = RleEncoder.max_buffer_size(bit_width, values.size)
    encoder = RleEncoder.new(bit_width, buffer_len + 1)
    values.each { |v| encoder.put(v.to_u64) }

    buffer = encoder.consume
    data = Array(UInt8).new(buffer.size + 1, 0_u8)
    buffer.each_with_index { |b, i| data[i] = b }
    data[buffer.size] = 0_u8
    padded = Slice.new(data.to_unsafe, data.size).dup

    decoder = RleDecoder.new(bit_width)
    decoder.set_data(padded)
    actual = Array(Int16).new(12, 0_i16)
    decoded = decoder.get_batch(Int16, actual)
    decoded.should eq(BIT_PACK_GROUP_SIZE)
    actual[0, 6].should eq(values)
    actual[6].should eq(0_i16)
    actual[7].should eq(0_i16)
  end

  # Upstream `test_is_accumulating_rle_boundary`.
  it "enters RLE accumulation on the 8th identical value" do
    enc = RleEncoder.new(2, 256)
    7.times { enc.put(1_u64) }
    enc.accumulating_rle?(1_u64).should be_false
    enc.put(1_u64)
    enc.accumulating_rle?(1_u64).should be_true

    enc.extend_run(92)
    encoded = enc.consume
    dec = RleDecoder.new(2)
    dec.set_data(encoded)
    out_values = Array(Int32).new(100, 0)
    dec.get_batch(Int32, out_values).should eq(100)
    out_values.each { |v| v.should eq(1) }
  end

  # Upstream `test_rle_specific_roundtrip`.
  it "round-trips a specific bit-packed sequence" do
    values = [0_i16, 1_i16, 1_i16, 1_i16, 1_i16, 0_i16, 0_i16, 0_i16, 0_i16, 1_i16]
    encoder = RleEncoder.new(1, RleEncoder.max_buffer_size(1, values.size))
    values.each { |v| encoder.put(v.to_u64) }
    decoder = RleDecoder.new(1)
    decoder.set_data(encoder.consume)
    actual = Array(Int16).new(values.size, 0_i16)
    decoder.get_batch(Int16, actual)
    actual.should eq(values)
  end

  # Upstream `test_random` (deterministic parity generator).
  it "round-trips deterministic parity runs" do
    [5, 20, 100, 300].each do |ngroups|
      values = [] of Int32
      parity = false
      ngroups.times do |g|
        group_size = (g % 19) + 1
        group_size = 1 if group_size > 15
        group_size.times { values << (parity ? 1 : 0) }
        parity = !parity
      end
      required = Parquet::Util::BitUtil.num_required_bits(values.size.to_u64)
      next if required >= 64_u8
      bit_width = required.to_i32

      buffer_len = 64 * 1024
      encoder = RleEncoder.new(bit_width, buffer_len)
      values.each { |v| encoder.put(v.to_u64) }
      buffer = encoder.consume

      decoder = RleDecoder.new(bit_width)
      decoder.set_data(buffer)
      values.each do |expected|
        decoder.get(Int32).should eq(expected)
      end
    end
  end
end

# Upstream `test_rle_values` helper.
private def test_rle_values(bit_width : Int32, num_vals : Int32, value : Int32) : Nil
  mod_val = bit_width == 64 ? 1_u64 : (1_u64 << bit_width)
  values = Array(Int64).new(num_vals) do |v|
    value == -1 ? (v.to_i64 % mod_val.to_i64) : value.to_i64
  end
  validate_rle(values, bit_width, nil, -1)
end
