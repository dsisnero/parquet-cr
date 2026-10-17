require "../../spec_helper"
require "digest/crc32"

# Ported from `parquet/src/file/serialized_reader.rs` (`decode_page`'s
# `crc`-feature page-checksum verification)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Crystal has no feature flags, so the CRC check is always performed.

private alias F = Parquet::File
private alias Format = Parquet::Format
private alias PType = Parquet::Type
private alias PEnc = Parquet::Encoding
private alias PPageType = Parquet::PageType

private def data_page_header(crc : Int32?) : Format::PageHeader
  buffer_size = 5
  Format::PageHeader.new(
    PPageType::DATA_PAGE,
    buffer_size,
    buffer_size,
    crc,
    Format::DataPageHeader.new(1, PEnc::PLAIN, PEnc::RLE, PEnc::RLE, nil),
    nil,
    nil,
    nil,
  )
end

describe Parquet::File do
  it "verifies the page CRC checksum" do
    buffer = Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8]
    crc = Digest::CRC32.checksum(buffer).to_i32

    page = F.decode_page(data_page_header(crc), buffer, PType::INT32, nil)
    page.should be_a(Parquet::Column::Page)

    expect_raises(Parquet::GeneralError, "Page CRC checksum mismatch") do
      F.decode_page(data_page_header(crc + 1), buffer, PType::INT32, nil)
    end
  end

  it "skips the CRC check when no checksum is present" do
    buffer = Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8]
    page = F.decode_page(data_page_header(nil), buffer, PType::INT32, nil)
    page.should be_a(Parquet::Column::Page)
  end

  # The thrift `crc` field is a signed `i32`; upstream compares it with
  # `expected_crc as u32`, so checksums with the high bit set (negative as i32)
  # must verify instead of raising an integer-overflow error. Real files contain
  # them (`hadoop_lz4_compressed_larger.parquet`, for one).
  it "verifies CRC checksums whose high bit is set" do
    # `[1, 2, 3, 4, 2]` has CRC32 `0xd96f0c57`, i.e. `-647033769` as an `i32`.
    buffer = Bytes[1_u8, 2_u8, 3_u8, 4_u8, 2_u8]
    crc = Digest::CRC32.checksum(buffer).to_i32!
    crc.should be < 0

    page = F.decode_page(data_page_header(crc), buffer, PType::INT32, nil)
    page.should be_a(Parquet::Column::Page)
  end
end
