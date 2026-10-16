require "../../spec_helper"

# Ported from `parquet/src/util/bit_pack.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: the const-generic `unpack::<NUM_BITS>` becomes a `num_bits`
# parameter; the fixed `[T; bits]` output is an `Array(T)` of the same length.

private alias BitPack = Parquet::Util::BitPack

# Crystal has no `trailing_ones`; count the low-order set bits.
private def trailing_ones(value : T) : Int32 forall T
  count = 0
  (0...(sizeof(T) * 8)).each do |bit|
    break if (value >> bit) & 1 == 0
    count += 1
  end
  count
end

describe Parquet::Util::BitPack do
  # Upstream `test_basic`.
  it "unpacks bit-packed values" do
    input = Bytes.new(4096, 0xFF_u8)

    (0..8).each do |i|
      output = Array(UInt8).new(8, 0_u8)
      BitPack.unpack8(input, output, i)
      output.each { |value| trailing_ones(value).should eq(i) }
    end

    (0..16).each do |i|
      output = Array(UInt16).new(16, 0_u16)
      BitPack.unpack16(input, output, i)
      output.each { |value| trailing_ones(value).should eq(i) }
    end

    (0..32).each do |i|
      output = Array(UInt32).new(32, 0_u32)
      BitPack.unpack32(input, output, i)
      output.each { |value| trailing_ones(value).should eq(i) }
    end

    (0..64).each do |i|
      output = Array(UInt64).new(64, 0_u64)
      BitPack.unpack64(input, output, i)
      output.each { |value| trailing_ones(value).should eq(i) }
    end
  end
end
