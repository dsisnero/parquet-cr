require "../../spec_helper"

# Ported from `parquet/src/encodings/encoding/mod.rs` (DeltaBitPackEncoder) and
# `parquet/src/encodings/decoding.rs` (DeltaBitPackDecoder) tests
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergence: trait dispatch is `DeltaBitPackEncoder(T)`/`DeltaBitPackDecoder(T)`
# with `T.class`.

private def delta_default(type : T.class) : T forall T
  {% if T == Int32 %}
    0
  {% else %}
    0_i64
  {% end %}
end

private def delta_encode(type : T.class, chunks : Array(Array(T))) : Bytes forall T
  encoder = DeltaBitPackEncoder(T).new
  chunks.each { |chunk| encoder.put(chunk) }
  encoder.flush_buffer
end

private def delta_roundtrip(type : T.class, chunks : Array(Array(T))) : Nil forall T
  bytes = delta_encode(T, chunks)
  expected = chunks.flatten

  decoder = DeltaBitPackDecoder(T).new
  decoder.set_data(bytes, expected.size)
  result = Array(T).new(expected.size) { delta_default(T) }
  decoder.get(result).should eq(expected.size)
  decoder.values_left.should eq(0)
  result.should eq(expected)
end

private def delta_skip(type : T.class, data : Array(T), skip : Int32) : Nil forall T
  bytes = delta_encode(T, [data])
  decoder = DeltaBitPackDecoder(T).new
  decoder.set_data(bytes, data.size)
  skipped = decoder.skip(skip)

  if skip >= data.size
    skipped.should eq(data.size)
    decoder.skip(skip).should eq(0)
  else
    skipped.should eq(skip)
    remaining = data.size - skip
    buffer = Array(T).new(remaining) { delta_default(T) }
    decoder.get(buffer).should eq(remaining)
    buffer.should eq(data[skip..])
  end
end

describe Parquet::Encodings::DeltaBitPackEncoder do
  it "round-trips empty, repeated, uneven, uniform, and extreme values" do
    delta_roundtrip(Int32, [Array(Int32).new(0, 0)])
    delta_roundtrip(Int32, [[1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8]])
    delta_roundtrip(Int32, [[1, -2, 3, -4, 5, 6, 7, 8, 9, 10, 11]])
    delta_roundtrip(Int32, [Array(Int32).new(16, 127)])
    delta_roundtrip(Int32, [Array(Int32).new(16, -127)])
    delta_roundtrip(Int32, [[Int32::MIN, Int32::MIN, Int32::MIN, Int32::MAX, Int32::MIN, Int32::MAX, Int32::MIN, Int32::MAX]])
    delta_roundtrip(Int64, [[Int64::MIN, Int64::MAX, Int64::MIN, Int64::MAX, Int64::MIN, Int64::MAX, Int64::MIN, Int64::MAX]])
  end

  it "supports multiple put calls and multi-block data" do
    delta_roundtrip(Int32, [Array(Int32).new(64, -1), Array(Int32).new(128, -1), Array(Int32).new(64, -1)])
    delta_roundtrip(Int32, [Array(Int32).new(128, 7), Array(Int32).new(0, 0), Array(Int32).new(64, 9)])
    delta_roundtrip(Int64, [Array(Int64).new(64, -1), Array(Int64).new(128, -1), Array(Int64).new(64, -1)])
  end
end

describe Parquet::Encodings::DeltaBitPackDecoder do
  it "skips values (partial and past the end)" do
    repeated = [1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8]
    delta_skip(Int32, repeated, 10)
    delta_skip(Int32, repeated, 100)
    delta_skip(Int32, [1, -2, 3, -4, 5, 6, 7, 8, 9, 10, 11], 5)
    delta_skip(Int32, Array(Int32).new(16, 127), 5)
    delta_skip(Int32, [Int32::MIN, Int32::MIN, Int32::MIN, Int32::MAX, Int32::MIN, Int32::MAX, Int32::MIN, Int32::MAX], 5)

    uniform_i32 = Array(Int32).new(128) { |i| i * 7 }
    delta_skip(Int32, uniform_i32, 50)
    delta_skip(Int32, uniform_i32, 200)
    uniform_i64 = Array(Int64).new(128) { |i| i.to_i64 * 100 }
    delta_skip(Int64, uniform_i64, 50)
    delta_skip(Int64, uniform_i64, 200)
  end

  it "rejects zero mini blocks per block" do
    decoder = DeltaBitPackDecoder(Int32).new
    # block_size = 128 (VLQ 128,1), mini_blocks_per_block = 0
    expect_raises(Parquet::ParquetError, "cannot have zero miniblocks per block") do
      decoder.set_data(Bytes[128_u8, 1_u8, 0_u8], 0)
    end
  end
end
