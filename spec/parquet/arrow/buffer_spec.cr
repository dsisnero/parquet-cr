require "../../spec_helper"

# Ported from `parquet/src/arrow/buffer/bit_util.rs` (`count_set_bits`,
# `iter_set_bits_rev`, `sign_extend_be`, `test_bit_fns`) and
# `parquet/src/arrow/record_reader/buffer.rs` (`ValuesBuffer::pad_nulls`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
private alias PBuffer = Parquet::Arrow::Buffer
private alias PValues = Parquet::Arrow::RecordReader

private def bool_array_to_mask(bools : Array(Bool)) : Bytes
  bytes = Bytes.new((bools.size + 7) // 8, 0_u8)
  bools.each_with_index do |value, i|
    bytes[i // 8] |= (1_u8 << (i % 8)) if value
  end
  bytes
end

describe Parquet::Arrow::Buffer do
  # Upstream `test_bit_fns` (randomized mask over 1..1024 bits).
  it "counts and iterates set bits" do
    random = Random.new(42)
    10.times do
      mask_length = random.rand(1..1024)
      bools = Array.new(mask_length) { random.next_bool }
      mask = bool_array_to_mask(bools)

      PBuffer.iter_set_bits_rev(mask).should eq(
        bools.each_index.select { |i| bools[i] }.to_a.reverse
      )

      count = bools.size
      20.times do
        start = random.rand(0...count)
        finish = random.rand(start...count)
        PBuffer.count_set_bits(mask, start, finish).should eq(
          bools[start...finish].count(true)
        )
      end
    end

    PBuffer.iter_set_bits_rev(Bytes.empty).should be_empty
    PBuffer.count_set_bits(Bytes.empty, 0, 0).should eq(0)
    PBuffer.count_set_bits(Bytes[0xFF_u8], 1, 1).should eq(0)
  end

  it "counts set bits across byte boundaries" do
    mask = Bytes[0b1010_1010_u8, 0b0000_1111_u8]
    PBuffer.count_set_bits(mask, 0, 16).should eq(8)
    PBuffer.count_set_bits(mask, 0, 8).should eq(4)
    PBuffer.count_set_bits(mask, 4, 12).should eq(6)
    PBuffer.count_set_bits(mask, 6, 10).should eq(3)
  end

  # Upstream `sign_extend_be`.
  it "sign extends big-endian values" do
    PBuffer.sign_extend_be(Bytes[0x01_u8], 3).should eq(Bytes[0x00_u8, 0x00_u8, 0x01_u8])
    PBuffer.sign_extend_be(Bytes[0x80_u8], 3).should eq(Bytes[0xFF_u8, 0xFF_u8, 0x80_u8])
    PBuffer.sign_extend_be(Bytes[0xFF_u8, 0x01_u8], 4).should eq(Bytes[0xFF_u8, 0xFF_u8, 0xFF_u8, 0x01_u8])
    PBuffer.sign_extend_be(Bytes[0xDE_u8, 0xAD_u8], 2).should eq(Bytes[0xDE_u8, 0xAD_u8])
  end
end

describe Parquet::Arrow::RecordReader::ArrayValuesBuffer do
  # Null slots keep whatever was in the buffer (upstream only guarantees the
  # values at valid positions), so these helpers check the valid positions.
  it "pads nulls by moving values into their valid positions" do
    buffer = PValues::ArrayValuesBuffer(Int32).new(0)
    buffer.concat([1, 2])
    buffer.pad_nulls(0, 2, 3, bool_array_to_mask([true, false, true]))
    buffer.size.should eq(3)
    buffer[0].should eq(1)
    buffer[2].should eq(2)
  end

  it "keeps all values in place when every level is valid" do
    buffer = PValues::ArrayValuesBuffer(Int32).new(0)
    buffer.concat([10, 20, 30])
    buffer.pad_nulls(0, 3, 3, bool_array_to_mask([true, true, true]))
    buffer.to_a.should eq([10, 20, 30])
  end

  it "pads trailing nulls" do
    buffer = PValues::ArrayValuesBuffer(Int32).new(0)
    buffer.concat([7])
    buffer.pad_nulls(0, 1, 4, bool_array_to_mask([true, false, false, false]))
    buffer.to_a.should eq([7, 0, 0, 0])
  end

  it "pads from the read offset using absolute mask positions" do
    buffer = PValues::ArrayValuesBuffer(Int32).new(0)
    buffer.concat([1, 2, 3, 4])
    # Levels: [valid, valid] (already written), then valid, null, valid.
    buffer.pad_nulls(2, 2, 3, bool_array_to_mask([true, true, true, false, true]))
    buffer.size.should eq(5)
    buffer[0].should eq(1)
    buffer[1].should eq(2)
    buffer[2].should eq(3)
    buffer[4].should eq(4)
  end

  it "handles masks spanning multiple bytes" do
    bools = [true, false, false, true, false, true, true, false, true, false]
    buffer = PValues::ArrayValuesBuffer(Int32).new(0)
    buffer.concat([1, 2, 3, 4, 5])
    buffer.pad_nulls(0, 5, 10, bool_array_to_mask(bools))
    buffer.size.should eq(10)
    [0, 3, 5, 6, 8].map { |i| buffer[i] }.should eq([1, 2, 3, 4, 5])
  end

  it "supports byte-valued buffers" do
    buffer = PValues::ArrayValuesBuffer(Parquet::ByteArray).new(Parquet::ByteArray.new)
    buffer.concat([Parquet::ByteArray.new("a"), Parquet::ByteArray.new("b")])
    buffer.pad_nulls(0, 2, 3, bool_array_to_mask([false, true, true]))
    buffer.size.should eq(3)
    buffer[1].data.should eq("a".to_slice)
    buffer[2].data.should eq("b".to_slice)
  end
end
