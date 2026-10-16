require "../spec_helper"

# Ported from `parquet/src/compression.rs` test module (`test_codec_gzip` +
# `test_roundtrip`) (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Only the GZIP codec is ported (Crystal stdlib `Compress::Gzip`); the other
# codecs need shards and are intentional divergences (see `plans/parity.md`).

private alias CC = Parquet::CompressionCodecs
private alias Comp = Parquet::Compression
private alias CodecOptionsBuilder = Parquet::CodecOptionsBuilder

private def slice_of(values : Array(UInt8)) : Bytes
  Slice.new(values.to_unsafe, values.size).dup
end

private def test_roundtrip(compression : Comp, data : Bytes, uncompress_size : Int32?)
  codec_options = CodecOptionsBuilder.default.set_backward_compatible_lz4(false).build
  c1 = CC.create_codec(compression, codec_options).not_nil!
  c2 = CC.create_codec(compression, codec_options).not_nil!

  compressed = [] of UInt8
  decompressed = [] of UInt8
  c1.compress(data, compressed)
  decompressed_size = c2.decompress(slice_of(compressed), decompressed, uncompress_size)
  decompressed_size.should eq(data.size)
  slice_of(decompressed).should eq(data)

  decompressed.clear
  compressed.clear
  c2.compress(data, compressed)
  decompressed_size = c1.decompress(slice_of(compressed), decompressed, uncompress_size)
  decompressed_size.should eq(data.size)
  slice_of(decompressed).should eq(data)

  decompressed.clear
  compressed.clear
  prefix = [0xDE_u8, 0xAD_u8, 0xBE_u8, 0xEF_u8]
  decompressed.concat(prefix)
  compressed.concat(prefix)

  c2.compress(data, compressed)
  compressed[0, 4].should eq(prefix)

  decompressed_size = c2.decompress(slice_of(compressed)[4, compressed.size - 4], decompressed, uncompress_size)
  decompressed_size.should eq(data.size)
  slice_of(decompressed)[4, decompressed.size - 4].should eq(data)
  decompressed[0, 4].should eq(prefix)
end

private def test_codec_with_size(c : Comp)
  [100, 10000, 100000].each do |size|
    data = Random.new(1234).random_bytes(size)
    test_roundtrip(c, data, data.size.to_i32)
  end
end

private def test_codec_without_size(c : Comp)
  [100, 10000, 100000].each do |size|
    data = Random.new(1234).random_bytes(size)
    test_roundtrip(c, data, nil)
  end
end

describe Parquet::CompressionCodecs do
  # Upstream `test_codec_gzip`.
  it "round-trips gzip for every level" do
    (0..9).each do |level|
      Parquet::GzipLevel.try_new(level)
      test_codec_with_size(Comp.gzip(level))
      test_codec_without_size(Comp.gzip(level))
    end
  end

  it "returns no codec for uncompressed" do
    options = CodecOptionsBuilder.default.build
    CC.create_codec(Comp::UNCOMPRESSED, options).should be_nil
  end

  it "rejects unsupported codecs" do
    options = CodecOptionsBuilder.default.build
    expect_raises(Parquet::ParquetError, "is not supported yet") do
      CC.create_codec(Comp::SNAPPY, options)
    end
  end
end
