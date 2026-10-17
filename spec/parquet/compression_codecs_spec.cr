require "../spec_helper"

# Ported from `parquet/src/compression.rs` test module (`test_codec_snappy`,
# `test_codec_gzip`, `test_codec_lz4`, `test_codec_lz4_raw` + `test_roundtrip`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# SNAPPY, GZIP, and the three LZ4 codecs are ported. The remaining codecs
# (brotli/lzo/zstd) still raise from `create_codec` and are intentional
# divergences (see `plans/parity.md` Phase 1.8).

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
  # Upstream `test_codec_snappy`.
  it "round-trips snappy with and without an expected size" do
    test_codec_with_size(Comp::SNAPPY)
    test_codec_without_size(Comp::SNAPPY)
  end

  # Upstream `test_codec_gzip`.
  it "round-trips gzip for every level" do
    (0..9).each do |level|
      Parquet::GzipLevel.try_new(level)
      test_codec_with_size(Comp.gzip(level))
      test_codec_without_size(Comp.gzip(level))
    end
  end

  # Characterization: random bytes never match, so the upstream roundtrip above
  # only exercises literal runs. Repetitive input exercises the copy/match path
  # and must actually shrink.
  it "compresses repetitive data with snappy" do
    data = ("abcdefgh" * 1024).to_slice
    options = CodecOptionsBuilder.default.set_backward_compatible_lz4(false).build
    codec = CC.create_codec(Comp::SNAPPY, options).not_nil!

    compressed = [] of UInt8
    codec.compress(data, compressed)
    compressed.size.should be < data.size

    decompressed = [] of UInt8
    size = codec.decompress(slice_of(compressed), decompressed, data.size)
    size.should eq(data.size)
    slice_of(decompressed).should eq(data)
  end

  # Golden streams derived from the snappy raw format spec (not from this
  # port's encoder), pinning the decoder against the wire format:
  # - `[0x05, 0x10, "hello"]`: uncompressed length 5, one 5-byte literal.
  # - `[0x0b, 0x10, "abcde", 0x16, 0x05, 0x00]`: length 11, a 5-byte literal
  #   then a copy of 6 bytes from offset 5 (yielding "abcdeabcdea").
  it "decodes snappy wire-format streams" do
    options = CodecOptionsBuilder.default.build
    codec = CC.create_codec(Comp::SNAPPY, options).not_nil!

    literal = Bytes[0x05, 0x10, 0x68, 0x65, 0x6c, 0x6c, 0x6f]
    decompressed = [] of UInt8
    codec.decompress(literal, decompressed, nil).should eq(5)
    slice_of(decompressed).should eq("hello".to_slice)

    with_copy = Bytes[
      0x0b, 0x10, 0x61, 0x62, 0x63, 0x64, 0x65, 0x16, 0x05, 0x00,
    ]
    decompressed.clear
    codec.decompress(with_copy, decompressed, nil).should eq(11)
    slice_of(decompressed).should eq("abcdeabcdea".to_slice)

    # An empty snappy stream is a single zero-length preamble.
    decompressed.clear
    codec.decompress(Bytes[0x00], decompressed, nil).should eq(0)
    decompressed.should be_empty
  end

  it "rejects corrupt snappy streams" do
    options = CodecOptionsBuilder.default.build
    codec = CC.create_codec(Comp::SNAPPY, options).not_nil!

    # Literal length larger than the remaining input.
    expect_raises(Parquet::ParquetError) do
      codec.decompress(Bytes[0x0a, 0x24, 0x61, 0x62], [] of UInt8, nil)
    end

    # Copy offset pointing before the start of the output.
    expect_raises(Parquet::ParquetError) do
      codec.decompress(Bytes[0x04, 0x0e, 0x61, 0x62], [] of UInt8, nil)
    end
  end

  # Characterization: the `naqvis/snappy` shard's `encode` raises `IndexError`
  # for empty input, so the codec emits the canonical empty block itself (a lone
  # zero-length preamble).
  it "round-trips empty input" do
    options = CodecOptionsBuilder.default.build
    codec = CC.create_codec(Comp::SNAPPY, options).not_nil!

    compressed = [] of UInt8
    codec.compress(Bytes.empty, compressed)
    compressed.should eq([0x00_u8])

    decompressed = [] of UInt8
    codec.decompress(slice_of(compressed), decompressed, nil).should eq(0)
    decompressed.should be_empty

    decompressed.clear
    codec.decompress(slice_of(compressed), decompressed, 0).should eq(0)
    decompressed.should be_empty
  end

  # Upstream `snap::raw::Decoder` requires the destination length to equal the
  # length recorded in the preamble.
  it "rejects a mismatched decompressed size" do
    options = CodecOptionsBuilder.default.build
    codec = CC.create_codec(Comp::SNAPPY, options).not_nil!

    compressed = [] of UInt8
    codec.compress("hello".to_slice, compressed)

    expect_raises(Parquet::ParquetError, "Recorded length") do
      codec.decompress(slice_of(compressed), [] of UInt8, 6)
    end
  end

  # Upstream `test_codec_lz4`: the deprecated `LZ4` codec writes Hadoop framing
  # and (with `backward_compatible_lz4 == false`, as `test_roundtrip` sets)
  # rejects the legacy frame/raw fallbacks.
  it "round-trips lz4 with hadoop framing" do
    test_codec_with_size(Comp::LZ4)
  end

  # Upstream `test_codec_lz4_raw`.
  it "round-trips lz4 raw blocks" do
    test_codec_with_size(Comp::LZ4_RAW)
  end

  # Characterization: the Hadoop framing is a big-endian
  # `(uncompressed_size, compressed_size)` prefix followed by a raw LZ4 block.
  it "encodes the lz4 hadoop prefix" do
    data = ("hello hadoop lz4 " * 100).to_slice
    options = CodecOptionsBuilder.default.set_backward_compatible_lz4(false).build
    codec = CC.create_codec(Comp::LZ4, options).not_nil!

    compressed = [] of UInt8
    codec.compress(data, compressed)

    prefix_uncompressed =
      (compressed[0].to_u32 << 24) | (compressed[1].to_u32 << 16) |
        (compressed[2].to_u32 << 8) | compressed[3].to_u32
    prefix_compressed =
      (compressed[4].to_u32 << 24) | (compressed[5].to_u32 << 16) |
        (compressed[6].to_u32 << 8) | compressed[7].to_u32

    prefix_uncompressed.should eq(data.size.to_u32)
    prefix_compressed.should eq((compressed.size - 8).to_u32)
  end

  # Golden raw blocks derived from the LZ4 block format specification (not from
  # this port's encoder), pinning the decoder against the wire format.
  it "decodes lz4 blocks written to the format spec" do
    codec = CC.create_codec(Comp::LZ4_RAW, CodecOptionsBuilder.default.build).not_nil!

    # A literal-only sequence: token 0x50 (5 literals), then "hello".
    literal = Bytes[0x50, 0x68, 0x65, 0x6c, 0x6c, 0x6f]
    decompressed = [] of UInt8
    codec.decompress(literal, decompressed, 5).should eq(5)
    slice_of(decompressed).should eq("hello".to_slice)

    # "abcdabcdabcdabcd": a 4-byte literal run "abcd" (token high nibble 4), a
    # match at offset 4 with length 7 (token low nibble 3 => 3 + MINMATCH), then
    # the mandatory 5 trailing literals "dabcd".
    matched = Bytes[0x43, 0x61, 0x62, 0x63, 0x64, 0x04, 0x00, 0x50, 0x64, 0x61, 0x62, 0x63, 0x64]
    decompressed.clear
    codec.decompress(matched, decompressed, 16).should eq(16)
    String.new(slice_of(decompressed)).should eq("abcdabcdabcdabcd")
  end

  it "decodes raw lz4 blocks through the lz4 hadoop fallback" do
    data = ("fallback " * 64).to_slice
    raw = CC.create_codec(Comp::LZ4_RAW, CodecOptionsBuilder.default.build).not_nil!
    raw_block = [] of UInt8
    raw.compress(data, raw_block)

    backward = CC.create_codec(Comp::LZ4, CodecOptionsBuilder.default.build).not_nil!
    decompressed = [] of UInt8
    backward.decompress(slice_of(raw_block), decompressed, data.size).should eq(data.size)
    slice_of(decompressed).should eq(data)

    # Upstream only falls back when `backward_compatible_lz4` is enabled.
    strict = CC.create_codec(Comp::LZ4, CodecOptionsBuilder.default.set_backward_compatible_lz4(false).build).not_nil!
    expect_raises(Parquet::ExternalError) do
      strict.decompress(slice_of(raw_block), [] of UInt8, data.size)
    end
  end

  it "decodes the lz4 frame format through the lz4 hadoop fallback" do
    data = ("legacy frame " * 100).to_slice
    framed = [] of UInt8
    CC::LZ4Codec.new.compress(data, framed)

    hadoop = CC.create_codec(Comp::LZ4, CodecOptionsBuilder.default.build).not_nil!
    decompressed = [] of UInt8
    hadoop.decompress(slice_of(framed), decompressed, data.size).should eq(data.size)
    slice_of(decompressed).should eq(data)
  end

  # The frame container is the `lz4_flex::frame` replacement (see
  # `plans/parity.md`); its blocks are raw LZ4 blocks.
  it "round-trips lz4 frames" do
    codec = CC::LZ4Codec.new

    [0, 5, 4096, 3 * 1024 * 1024 + 7].each do |size|
      data = size == 0 ? Bytes.empty : Random.new(size).random_bytes(size)
      compressed = [] of UInt8
      codec.compress(data, compressed)

      # Frame magic `0x184D2204`, little-endian.
      compressed[0, 4].should eq([0x04_u8, 0x22_u8, 0x4D_u8, 0x18_u8])

      decompressed = [] of UInt8
      codec.decompress(slice_of(compressed), decompressed, nil).should eq(size)
      slice_of(decompressed).should eq(data)
    end
  end

  it "rejects corrupt lz4 frames" do
    codec = CC::LZ4Codec.new

    expect_raises(Parquet::ExternalError, "invalid LZ4 frame magic") do
      codec.decompress(Bytes[0x00, 0x01, 0x02, 0x03], [] of UInt8, nil)
    end

    compressed = [] of UInt8
    codec.compress("hello".to_slice, compressed)
    corrupted = slice_of(compressed)
    corrupted[6] = (corrupted[6] ^ 0xFF).to_u8
    expect_raises(Parquet::ExternalError, "invalid LZ4 frame header checksum") do
      codec.decompress(corrupted, [] of UInt8, nil)
    end
  end

  it "rejects corrupt lz4 blocks" do
    codec = CC.create_codec(Comp::LZ4_RAW, CodecOptionsBuilder.default.build).not_nil!

    # A match offset pointing before the start of the output.
    expect_raises(Parquet::ExternalError) do
      codec.decompress(Bytes[0x10, 0x61, 0x00, 0x00], [] of UInt8, 8)
    end

    # A literal run longer than the remaining input.
    expect_raises(Parquet::ExternalError) do
      codec.decompress(Bytes[0x50, 0x61], [] of UInt8, 5)
    end
  end

  # Upstream `lz4_flex::block::decompress_into` needs the decoded size, which is
  # why the LZ4 codecs reject a missing `uncompress_size` (LZ4_FRAME is the only
  # LZ4 variant that carries it on the wire).
  it "rejects lz4 streams without an uncompress size" do
    options = CodecOptionsBuilder.default.build

    expect_raises(Parquet::GeneralError, "LZ4RawCodec unsupported without uncompress_size") do
      CC.create_codec(Comp::LZ4_RAW, options).not_nil!.decompress(Bytes[0x00], [] of UInt8, nil)
    end

    expect_raises(Parquet::GeneralError, "LZ4HadoopCodec unsupported without uncompress_size") do
      CC.create_codec(Comp::LZ4, options).not_nil!.decompress(Bytes[0x00], [] of UInt8, nil)
    end
  end

  it "rejects a mismatched lz4 decompressed size" do
    codec = CC.create_codec(Comp::LZ4_RAW, CodecOptionsBuilder.default.build).not_nil!
    compressed = [] of UInt8
    codec.compress("hello".to_slice, compressed)

    expect_raises(Parquet::GeneralError, "LZ4RawCodec uncompress_size is not the expected one") do
      codec.decompress(slice_of(compressed), [] of UInt8, 6)
    end
  end

  # Divergence: `Flint::BlockCodec#compress_raw` emits a zero-length block for
  # empty input, so `LZ4RawCodec#compress` writes the canonical single-token
  # empty block itself.
  it "round-trips empty lz4 input" do
    codec = CC.create_codec(Comp::LZ4_RAW, CodecOptionsBuilder.default.build).not_nil!

    compressed = [] of UInt8
    codec.compress(Bytes.empty, compressed)
    compressed.should eq([0x00_u8])

    decompressed = [] of UInt8
    codec.decompress(slice_of(compressed), decompressed, 0).should eq(0)
    decompressed.should be_empty
  end

  it "returns no codec for uncompressed" do
    options = CodecOptionsBuilder.default.build
    CC.create_codec(Comp::UNCOMPRESSED, options).should be_nil
  end

  it "rejects unsupported codecs" do
    options = CodecOptionsBuilder.default.build
    expect_raises(Parquet::ParquetError, "is not supported yet") do
      CC.create_codec(Comp.zstd, options)
    end
  end
end
