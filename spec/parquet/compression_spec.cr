require "../spec_helper"

# Ported from `parquet/src/basic.rs` test module (`test_compression_conversion`,
# `test_display_compression`, `test_parse_compression`) and the compression level
# types in `parquet/src/compression.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias Comp = Parquet::Compression
private alias Codec = Parquet::CompressionCodec
private alias GzipLevel = Parquet::GzipLevel
private alias BrotliLevel = Parquet::BrotliLevel
private alias ZstdLevel = Parquet::ZstdLevel

describe Parquet::Compression do
  # Upstream `test_compression_conversion`.
  it "converts to and from CompressionCodec" do
    Codec.from(Comp::UNCOMPRESSED).should eq(Codec::UNCOMPRESSED)
    Codec.from(Comp::SNAPPY).should eq(Codec::SNAPPY)
    Codec.from(Comp.gzip).should eq(Codec::GZIP)
    Codec.from(Comp::LZO).should eq(Codec::LZO)
    Codec.from(Comp.brotli).should eq(Codec::BROTLI)
    Codec.from(Comp::LZ4).should eq(Codec::LZ4)
    Codec.from(Comp.zstd).should eq(Codec::ZSTD)
    Codec.from(Comp::LZ4_RAW).should eq(Codec::LZ4_RAW)

    Comp.from_codec(Codec::UNCOMPRESSED).should eq(Comp::UNCOMPRESSED)
    Comp.from_codec(Codec::SNAPPY).should eq(Comp::SNAPPY)
    Comp.from_codec(Codec::GZIP).should eq(Comp.gzip)
    Comp.from_codec(Codec::LZO).should eq(Comp::LZO)
    Comp.from_codec(Codec::BROTLI).should eq(Comp.brotli)
    Comp.from_codec(Codec::LZ4).should eq(Comp::LZ4)
    Comp.from_codec(Codec::ZSTD).should eq(Comp.zstd)
    Comp.from_codec(Codec::LZ4_RAW).should eq(Comp::LZ4_RAW)
  end

  # Upstream `test_display_compression`.
  it "displays" do
    Comp::UNCOMPRESSED.to_s.should eq("UNCOMPRESSED")
    Comp::SNAPPY.to_s.should eq("SNAPPY")
    Comp.gzip.to_s.should eq("GZIP(GzipLevel(6))")
    Comp::LZO.to_s.should eq("LZO")
    Comp.brotli.to_s.should eq("BROTLI(BrotliLevel(1))")
    Comp::LZ4.to_s.should eq("LZ4")
    Comp.zstd.to_s.should eq("ZSTD(ZstdLevel(1))")
  end

  # Upstream `test_parse_compression` (valid cases).
  it "parses from strings" do
    Comp.from_str("snappy").should eq(Comp::SNAPPY)
    Comp.from_str("lzo").should eq(Comp::LZO)
    Comp.from_str("zstd(3)").should eq(Comp.zstd(3))
    Comp.from_str("zstd(-3)").should eq(Comp.zstd(-3))
    Comp.from_str("LZ4_RAW").should eq(Comp::LZ4_RAW)
    Comp.from_str("uncompressed").should eq(Comp::UNCOMPRESSED)
    Comp.from_str("gzip(9)").should eq(Comp.gzip(9))
    Comp.from_str("brotli(3)").should eq(Comp.brotli(3))
    Comp.from_str("lz4").should eq(Comp::LZ4)
  end

  it "defaults to uncompressed" do
    Comp.default.should eq(Comp::UNCOMPRESSED)
  end

  it "rejects unsupported compression" do
    expect_raises(Parquet::ParquetError, "unsupported compression plain_xxx") do
      Comp.from_str("plain_xxx")
    end
  end

  it "rejects a level where none is allowed" do
    expect_raises(Parquet::ParquetError, "compression level is not supported") do
      Comp.from_str("snappy(3)")
    end
  end

  it "requires a level" do
    expect_raises(Parquet::ParquetError, "gzip requires a compression level") do
      Comp.from_str("gzip")
    end
  end
end

describe Parquet::GzipLevel do
  it "defaults to 6 and validates the range" do
    GzipLevel.default.compression_level.should eq(6)
    GzipLevel.try_new(9).compression_level.should eq(9)
    err = expect_raises(Parquet::ParquetError) { GzipLevel.try_new(10) }
    err.to_s.should eq(general_err!("valid compression range 0..=9 exceeded.").to_s)
  end
end

describe Parquet::BrotliLevel do
  it "defaults to 1 and validates the range" do
    BrotliLevel.default.compression_level.should eq(1)
    BrotliLevel.try_new(11).compression_level.should eq(11)
    expect_raises(Parquet::ParquetError) { BrotliLevel.try_new(12) }
  end
end

describe Parquet::ZstdLevel do
  it "defaults to 1 and validates the range" do
    ZstdLevel.default.compression_level.should eq(1)
    ZstdLevel.try_new(-131072).compression_level.should eq(-131072)
    ZstdLevel.try_new(22).compression_level.should eq(22)
    expect_raises(Parquet::ParquetError) { ZstdLevel.try_new(23) }
  end
end
