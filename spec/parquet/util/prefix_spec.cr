require "../../spec_helper"

# Ported from `parquet/src/util/prefix.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias Prefix = Parquet::Util::Prefix

private def bytes(values : Array(UInt8)) : Bytes
  Slice.new(values.to_unsafe, values.size).dup
end

private def str_bytes(value : String) : Bytes
  value.to_slice
end

private def naive(a : Bytes, b : Bytes) : Int32
  i = 0
  while i < a.size && i < b.size && a[i] == b[i]
    i += 1
  end
  i
end

private def x_bytes(len : Int32) : Array(UInt8)
  Array(UInt8).new(len, 'x'.ord.to_u8!)
end

describe Parquet::Util::Prefix do
  # Upstream `test_common_prefix_length_edge_cases`.
  it "handles edge cases" do
    Prefix.common_prefix_length(str_bytes(""), str_bytes("")).should eq(0)
    Prefix.common_prefix_length(str_bytes(""), str_bytes("abc")).should eq(0)
    Prefix.common_prefix_length(str_bytes("abc"), str_bytes("")).should eq(0)
    Prefix.common_prefix_length(str_bytes("abc"), str_bytes("xyz")).should eq(0)
    Prefix.common_prefix_length(str_bytes("abc"), str_bytes("abc")).should eq(3)
    Prefix.common_prefix_length(str_bytes("abc"), str_bytes("abcdef")).should eq(3)
    Prefix.common_prefix_length(str_bytes("abcdef"), str_bytes("abc")).should eq(3)
  end

  # Upstream `test_common_prefix_length_around_block_boundaries`.
  it "handles mismatches around block boundaries" do
    [31, 32, 33, 63, 64, 65, 127, 128, 129, 1024].each do |len|
      (0..len).each do |mismatch|
        a = x_bytes(len)
        b = a.dup
        b[mismatch] = 'y'.ord.to_u8! if mismatch < len
        expected = mismatch < len ? mismatch : len
        result = Prefix.common_prefix_length(bytes(a), bytes(b))
        result.should eq(expected)
        result.should eq(naive(bytes(a), bytes(b)))
      end
    end
  end

  # Upstream `test_common_prefix_length_unequal_lengths`.
  it "caps at the shorter length" do
    (0...80).each do |a_len|
      (0...80).each do |b_len|
        a = x_bytes(a_len)
        b = x_bytes(b_len)
        Prefix.common_prefix_length(bytes(a), bytes(b)).should eq(Math.min(a_len, b_len))
      end
    end
  end

  # Upstream `test_common_prefix_length_matches_naive_on_varied_data`.
  it "matches a naive scan on varied data" do
    a = Array(UInt8).new(500) { |i| ((i * 7) % 251).to_u8! }
    (0...a.size).each do |mismatch|
      b = a.dup
      b[mismatch] = b[mismatch] &+ 1_u8
      result = Prefix.common_prefix_length(bytes(a), bytes(b))
      result.should eq(naive(bytes(a), bytes(b)))
      result.should eq(mismatch)
    end
  end
end
