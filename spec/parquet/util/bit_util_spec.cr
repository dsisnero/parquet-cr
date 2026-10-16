require "../../spec_helper"

# Ported from `parquet/src/util/bit_util.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# The upstream `random_numbers`-driven cases are ported with a small
# deterministic generator so the expectations are reproducible.

private def bitutil_deterministic(count : Int32, num_bits : Int32) : Array(UInt64)
  mask = num_bits == 64 ? UInt64::MAX : ((1_u64 << num_bits) - 1_u64)
  state = 0x9E3779B97F4A7C15_u64
  Array(UInt64).new(count) do
    state = state &* 6364136223846793005_u64 &+ 1442695040888963407_u64
    (state >> 16) & mask
  end
end

describe Parquet::Util::BitUtil do
  # Upstream `test_ceil`.
  it "computes ceil division" do
    BitUtil.ceil(0, 1).should eq(0)
    BitUtil.ceil(1, 1).should eq(1)
    BitUtil.ceil(1, 2).should eq(1)
    BitUtil.ceil(9, 8).should eq(2)
    BitUtil.ceil(9, 9).should eq(1)
    BitUtil.ceil(10_000_000_000_u64, 10).should eq(1_000_000_000_u64)
    BitUtil.ceil(10_u64, 10_000_000_000_u64).should eq(1_u64)
  end

  # Upstream `test_num_required_bits`.
  it "computes the minimum number of required bits" do
    BitUtil.num_required_bits(0_u64).should eq(0_u8)
    BitUtil.num_required_bits(1_u64).should eq(1_u8)
    BitUtil.num_required_bits(2_u64).should eq(2_u8)
    BitUtil.num_required_bits(4_u64).should eq(3_u8)
    BitUtil.num_required_bits(8_u64).should eq(4_u8)
    BitUtil.num_required_bits(16_u64).should eq(5_u8)
    BitUtil.num_required_bits(UInt64::MAX).should eq(64_u8)
  end

  # Upstream `test_get_bit`.
  it "reads individual bits" do
    BitUtil.get_bit(Bytes[0b00001101_u8], 0).should be_true
    BitUtil.get_bit(Bytes[0b00001101_u8], 1).should be_false
    BitUtil.get_bit(Bytes[0b00001101_u8], 2).should be_true
    BitUtil.get_bit(Bytes[0b00001101_u8], 3).should be_true

    data = Bytes[0b01001001_u8, 0b01010010_u8]
    [true, false, false, true, false, false, true, false,
     false, true, false, false, true, false, true, false].each_with_index do |expected, i|
      BitUtil.get_bit(data, i).should eq(expected)
    end
  end

  # Upstream `test_compress`.
  it "extracts mask-selected bits" do
    BitUtil.compress(0b1010_u64, 0b1111_u64).should eq(0b1010_u64)
    BitUtil.compress(0b1010_u64, 0b1010_u64).should eq(0b11_u64)
    BitUtil.compress(0b1010_u64, 0b0101_u64).should eq(0_u64)
    BitUtil.compress(UInt64::MAX, 0_u64).should eq(0_u64)
    BitUtil.compress(0_u64, UInt64::MAX).should eq(0_u64)
    BitUtil.compress(UInt64::MAX, UInt64::MAX).should eq(UInt64::MAX)
  end
end

describe Parquet::Util::BitReader do
  # Upstream `test_bit_reader_get_byte_offset`.
  it "reports the byte offset" do
    reader = BitReader.new(Bytes.new(10, 255_u8))
    reader.get_byte_offset.should eq(0)
    reader.get_value(Int32, 6)
    reader.get_byte_offset.should eq(1)
    reader.get_value(Int32, 10)
    reader.get_byte_offset.should eq(2)
    reader.get_value(Int32, 20)
    reader.get_byte_offset.should eq(5)
    reader.get_value(Int32, 30)
    reader.get_byte_offset.should eq(9)
  end

  # Upstream `test_bit_reader_get_value`.
  it "reads bit-packed values" do
    reader = BitReader.new(Bytes[255_u8, 0_u8])
    reader.get_value(Int32, 1).should eq(1)
    reader.get_value(Int32, 2).should eq(3)
    reader.get_value(Int32, 3).should eq(7)
    reader.get_value(Int32, 4).should eq(3)
  end

  # Upstream `test_bit_reader_skip`.
  it "skips bit-packed values" do
    reader = BitReader.new(Bytes[255_u8, 0_u8])
    reader.skip(1, 1).should eq(1)
    reader.get_value(Int32, 1).should eq(1)
    reader.skip(2, 2).should eq(2)
    reader.get_value(Int32, 2).should eq(3)
    reader.skip(4, 1).should eq(4)
    reader.get_value(Int32, 4).should eq(0)
    reader.skip(1, 1).should eq(0)
  end

  # Upstream `test_bit_reader_get_value_boundary`.
  it "reads values across 64-bit boundaries" do
    reader = BitReader.new(Bytes[10_u8, 0_u8, 0_u8, 0_u8, 20_u8, 0_u8, 30_u8, 0_u8, 0_u8, 0_u8, 40_u8, 0_u8])
    reader.get_value(Int64, 32).should eq(10)
    reader.get_value(Int64, 16).should eq(20)
    reader.get_value(Int64, 32).should eq(30)
    reader.get_value(Int64, 16).should eq(40)
  end

  # Upstream `test_bit_reader_skip_boundary`.
  it "skips values across 64-bit boundaries" do
    reader = BitReader.new(Bytes[10_u8, 0_u8, 0_u8, 0_u8, 20_u8, 0_u8, 30_u8, 0_u8, 0_u8, 0_u8, 40_u8, 0_u8])
    reader.get_value(Int64, 32).should eq(10)
    reader.skip(1, 16).should eq(1)
    reader.get_value(Int64, 32).should eq(30)
    reader.get_value(Int64, 16).should eq(40)
  end

  # Upstream `test_bit_reader_get_aligned`.
  it "reads byte-aligned values" do
    buffer = Bytes[0x75_u8, 0xCB_u8]
    reader = BitReader.new(buffer)
    reader.get_value(Int32, 3).should eq(5)
    reader.get_aligned(Int32, 1).should eq(203)
    reader.get_value(Int32, 1).should be_nil
    reader.reset(buffer)
    reader.get_aligned(Int32, 3).should be_nil
  end

  # Upstream `test_bit_reader_get_vlq_int`.
  it "reads VLQ integers" do
    reader = BitReader.new(Bytes[0x89_u8, 0x01_u8, 0xF2_u8, 0xB5_u8, 0x06_u8])
    reader.get_vlq_int.should eq(137)
    reader.get_vlq_int.should eq(105202)
  end

  # Upstream `test_bit_reader_get_zigzag_vlq_int`.
  it "reads zigzag-VLQ integers" do
    reader = BitReader.new(Bytes[0_u8, 1_u8, 2_u8, 3_u8])
    reader.get_zigzag_vlq_int.should eq(0)
    reader.get_zigzag_vlq_int.should eq(-1)
    reader.get_zigzag_vlq_int.should eq(1)
    reader.get_zigzag_vlq_int.should eq(-2)
  end

  # Upstream `test_get_batch_zero_extend`.
  it "zero extends batched values" do
    reader = BitReader.new(Bytes.new(4, 0xFF_u8))
    output = Array(UInt64).new(32, UInt64::MAX)
    reader.get_batch(UInt64, output, 1)
    output.each { |v| v.should eq(1_u64) }
  end

  # Upstream `test_get_batch` (deterministic generator).
  it "reads batches of packed values" do
    [1, 31, 32, 33].each do |total|
      (1_i32..32_i32).each do |num_bits|
        values = bitutil_deterministic(total, num_bits)
        writer = BitWriter.new(8 * total)
        values.each { |v| writer.put_value(v, num_bits) }
        reader = BitReader.new(writer.consume)
        batch = Array(UInt32).new(total, 0_u32)
        reader.get_batch(UInt32, batch, num_bits).should eq(total)
        batch.each_with_index { |v, i| v.should eq(values[i].to_u32!) }
      end
    end
  end
end

describe Parquet::Util::BitWriter do
  # Upstream `test_skip`.
  it "reserves space and back-fills bytes" do
    writer = BitWriter.new(5)
    old_offset = writer.skip(1)
    writer.put_aligned(42, 4)
    writer.put_aligned_offset(0x10, 1, old_offset)
    writer.consume.should eq(Bytes[0x10_u8, 42_u8, 0, 0, 0])

    writer = BitWriter.new(4)
    writer.skip(5).should eq(0)
    writer.buffer.should eq(Bytes[0, 0, 0, 0, 0])
  end

  # Upstream `test_get_next_byte_ptr`.
  it "returns a writable pointer to reserved bytes" do
    writer = BitWriter.new(5)
    writer.get_next_byte_ptr(1)[0] = 0x10_u8
    writer.put_aligned(42, 4)
    writer.consume.should eq(Bytes[0x10_u8, 42_u8, 0, 0, 0])
  end

  # Upstream `test_consume_flush_buffer`.
  it "flushes the same bytes as consume" do
    writer1 = BitWriter.new(3)
    writer2 = BitWriter.new(3)
    (1..9).each do |i|
      writer1.put_value(i.to_u64, 4)
      writer2.put_value(i.to_u64, 4)
    end
    writer1.flush_buffer.should eq(writer2.consume)
  end

  # Upstream `test_put_get_bool`.
  it "round-trips single bits" do
    writer = BitWriter.new(8)
    (0...8).each { |i| writer.put_value((i % 2).to_u64, 1) }
    writer.flush
    writer.buffer[0].should eq(0b10101010_u8)

    (0...8).each do |i|
      case i
      when 0, 1, 4, 5 then writer.put_value(0_u64, 1)
      else                 writer.put_value(1_u64, 1)
      end
    end
    writer.flush
    writer.buffer[0].should eq(0b10101010_u8)
    writer.buffer[1].should eq(0b11001100_u8)

    reader = BitReader.new(writer.consume)
    (0...8).each { |i| reader.get_value(UInt8, 1).should eq((i % 2).to_u8) }
    (0...8).each do |i|
      expected = ![0, 1, 4, 5].includes?(i)
      reader.get_value(Bool, 1).should eq(expected)
    end
  end

  # Upstream `test_put_value_roundtrip` (deterministic generator).
  it "round-trips packed values" do
    [{32, 2}, {32, 3}, {32, 8}, {64, 16}, {64, 32}].each do |total, num_bits|
      values = bitutil_deterministic(total, num_bits)
      writer = BitWriter.new(BitUtil.ceil(num_bits, 8) * total)
      values.each { |v| writer.put_value(v, num_bits) }
      reader = BitReader.new(writer.consume)
      values.each_with_index do |expected, i|
        reader.get_value(UInt64, num_bits).should eq(expected), "index #{i}"
      end
    end
  end

  # Upstream `test_put_aligned_roundtrip` (deterministic generator).
  it "round-trips interleaved packed and aligned values" do
    total = 32
    num_bits = 11
    values = bitutil_deterministic(total // 2, num_bits)
    aligned = bitutil_deterministic(total // 2, 31)

    writer = BitWriter.new(total * 8)
    (0...total).each do |i|
      j = i // 2
      if i.even?
        writer.put_value(values[j], num_bits)
      else
        writer.put_aligned(aligned[j].to_i32!, 4)
      end
    end

    reader = BitReader.new(writer.consume)
    (0...total).each do |i|
      j = i // 2
      if i.even?
        reader.get_value(UInt64, num_bits).should eq(values[j])
      else
        reader.get_aligned(Int32, 4).should eq(aligned[j].to_i32!)
      end
    end
  end

  # Upstream `test_put_vlq_int` (deterministic generator).
  it "round-trips VLQ integers" do
    values = bitutil_deterministic(64, 32)
    writer = BitWriter.new(64 * 32)
    values.each { |v| writer.put_vlq_int(v) }
    reader = BitReader.new(writer.consume)
    values.each_with_index do |expected, i|
      reader.get_vlq_int.should eq(expected), "index #{i}"
    end
  end

  # Upstream `test_put_zigzag_vlq_int` (deterministic generator).
  it "round-trips zigzag-VLQ integers" do
    values = [-1_i64, 0_i64, 1_i64, -12345_i64, 12345_i64, -2_000_000_i64]
    writer = BitWriter.new(64 * 32)
    values.each { |v| writer.put_zigzag_vlq_int(v) }
    reader = BitReader.new(writer.consume)
    values.each { |expected| reader.get_zigzag_vlq_int.should eq(expected) }
  end
end
