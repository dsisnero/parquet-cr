require "../../spec_helper"

# Characterization specs for the in-tree xxHash implementations.
#
# Upstream uses the `twox_hash` crate (`XxHash64::oneshot`) for bloom filters and
# `lz4_flex`'s XXH32 for LZ4 frame checksums; Crystal has no `twox_hash` shard,
# so both algorithms are implemented in `src/parquet/util/xxhash.cr` and must
# keep producing the reference values. The XXH32 vectors below were produced by
# the reference `xxhash` implementation (v0.x, `xxhash.xxh32`).
describe Parquet::XxHash32 do
  it "matches the reference XXH32 vectors for seed 0" do
    Parquet::XxHash32.oneshot(Bytes.empty).should eq(0x02CC5D05_u32)
    Parquet::XxHash32.oneshot("a".to_slice).should eq(0x550D7456_u32)
    Parquet::XxHash32.oneshot("abc".to_slice).should eq(0x32D153FF_u32)
    Parquet::XxHash32.oneshot("abcd".to_slice).should eq(0xA3643705_u32)
    Parquet::XxHash32.oneshot("hello".to_slice).should eq(0xFB0077F9_u32)
    Parquet::XxHash32.oneshot("Hello, world!".to_slice).should eq(0x31B7405D_u32)
    # 15 bytes stays on the < 16-byte (seed-only) path, 16 bytes takes one
    # 4-lane round.
    Parquet::XxHash32.oneshot("0123456789abcde".to_slice).should eq(0x1DBDFA0F_u32)
    Parquet::XxHash32.oneshot("0123456789abcdef".to_slice).should eq(0xC2C45B69_u32)
    Parquet::XxHash32.oneshot(("the quick brown fox " * 7).to_slice).should eq(0x5AA383B4_u32)
  end

  it "matches the reference XXH32 vectors for seed 1234" do
    Parquet::XxHash32.oneshot(Bytes.empty, 1234_u32).should eq(0xB625A4DB_u32)
    Parquet::XxHash32.oneshot("abc".to_slice, 1234_u32).should eq(0xE727CDB9_u32)
    Parquet::XxHash32.oneshot("0123456789abcdef".to_slice, 1234_u32).should eq(0x1D8394D1_u32)
    Parquet::XxHash32.oneshot(("the quick brown fox " * 7).to_slice, 1234_u32).should eq(0x236DE4B0_u32)
  end
end
