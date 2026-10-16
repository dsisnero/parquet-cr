require "../../spec_helper"

# Ported from `parquet/src/file/writer.rs` (`test_page_writer_data_pages`,
# `test_page_writer_dict_pages`, `test_page_roundtrip`, `assert_page`) and
# `parquet/src/file/serialized_reader.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# The SNAPPY round-trip is adapted to GZIP (snappy needs a Crystal shard).

private alias Page = Parquet::Column::Page
private alias CompressedPage = Parquet::Column::CompressedPage
private alias Writer = Parquet::File::SerializedPageWriter
private alias Reader = Parquet::File::SerializedPageReader
private alias TrackedWrite = Parquet::File::TrackedWrite
private alias Stats = Parquet::Statistics
private alias Comp = Parquet::Compression
private alias CompCodec = Parquet::CompressionCodec
private alias PType = Parquet::Type
private alias Enc = Parquet::Encoding

private def slice_of(values : Array(UInt8)) : Bytes
  Slice.new(values.to_unsafe, values.size).dup
end

private def compress_helper(compressor : Parquet::CompressionCodecs::Codec?, data : Bytes) : Bytes
  output = [] of UInt8
  if compressor
    compressor.compress(data, output)
  else
    output.concat(data)
  end
  slice_of(output)
end

private def roundtrip_desc(physical : PType) : Parquet::Schema::ColumnDescriptor
  ty = Parquet::Schema::Type.primitive_type_builder("t", physical).build
  Parquet::Schema::ColumnDescriptor.new(ty, 0_i16, 0_i16, Parquet::Schema::ColumnPath.new([] of String))
end

private def assert_page(left : Page, right : Page) : Nil
  left.page_type.should eq(right.page_type)
  left.buffer.should eq(right.buffer)
  left.num_values.should eq(right.num_values)
  left.encoding.should eq(right.encoding)
  Stats.page_stats_to_thrift(left.statistics).should eq(Stats.page_stats_to_thrift(right.statistics))
end

private def test_page_roundtrip(pages : Array(Page), compression : Comp, physical_type : PType) : Nil
  codec_options = Parquet::CodecOptionsBuilder.default.set_backward_compatible_lz4(false).build
  compressor = Parquet::CompressionCodecs.create_codec(compression, codec_options)

  compressed_pages = [] of CompressedPage
  total_num_values = 0_i64

  pages.each do |page|
    uncompressed_len = page.buffer.size
    compressed_page =
      case page
      when Page::DataPage
        total_num_values += page.num_values
        Page::DataPage.new(
          compress_helper(compressor, page.buf),
          page.num_values,
          page.encoding,
          page.def_level_encoding,
          page.rep_level_encoding,
          page.statistics,
        )
      when Page::DataPageV2
        total_num_values += page.num_values
        offset = page.def_levels_byte_len + page.rep_levels_byte_len
        cmp = compress_helper(compressor, page.buf[offset, page.buf.size - offset])
        combined = [] of UInt8
        combined.concat(page.buf[0, offset])
        combined.concat(cmp)
        Page::DataPageV2.new(
          slice_of(combined),
          page.num_values,
          page.encoding,
          page.num_nulls,
          page.num_rows,
          page.def_levels_byte_len,
          page.rep_levels_byte_len,
          !compressor.nil?,
          page.statistics,
        )
      when Page::DictionaryPage
        Page::DictionaryPage.new(
          compress_helper(compressor, page.buf),
          page.num_values,
          page.encoding,
          page.is_sorted,
        )
      else
        raise "unreachable"
      end
    compressed_pages << CompressedPage.new(compressed_page, uncompressed_len)
  end

  buffer_io = IO::Memory.new
  tracked = TrackedWrite.new(buffer_io)
  writer = Writer.new(tracked)
  compressed_pages.each { |page| writer.write_page(page) }
  writer.close
  buffer = buffer_io.to_slice

  meta = Parquet::File::Metadata::ColumnChunkMetaData.builder(roundtrip_desc(physical_type))
    .set_compression_codec(CompCodec.from(compression))
    .set_total_compressed_size(buffer.size.to_i64)
    .set_num_values(total_num_values)
    .build

  props = Parquet::File::Properties::ReaderProperties.builder
    .set_backward_compatible_lz4(false)
    .set_read_page_statistics(true)
    .build

  chunk_reader = Parquet::File::BytesChunkReader.new(buffer)
  page_reader = Reader.new_for_column(chunk_reader, meta, total_num_values.to_i32, nil, props)

  result_pages = [] of Page
  while page = page_reader.get_next_page
    result_pages << page
  end

  result_pages.size.should eq(pages.size)
  pages.size.times { |i| assert_page(result_pages[i], pages[i]) }
end

describe Parquet::File::SerializedPageWriter do
  # Upstream `test_page_writer_data_pages`.
  it "round-trips data pages" do
    pages = [
      Page::DataPage.new(Bytes[1_u8, 2, 3, 4, 5, 6, 7, 8], 10_u32, Enc::DELTA_BINARY_PACKED, Enc::RLE, Enc::RLE, Stats.int32(1, 3, nil, 7_u64, true)),
      Page::DataPageV2.new(Bytes.new(128, 4_u8), 10_u32, Enc::DELTA_BINARY_PACKED, 2_u32, 12_u32, 24_u32, 32_u32, false, Stats.int32(1, 3, nil, 7_u64, true)),
    ] of Page

    test_page_roundtrip(pages, Comp.gzip, PType::INT32)
    test_page_roundtrip(pages, Comp::UNCOMPRESSED, PType::INT32)
  end

  # Upstream `test_page_writer_dict_pages`.
  it "round-trips dictionary pages" do
    pages = [
      Page::DictionaryPage.new(Bytes[1_u8, 2, 3, 4, 5], 5_u32, Enc::RLE_DICTIONARY, false),
      Page::DataPage.new(Bytes[1_u8, 2, 3, 4, 5, 6, 7, 8], 10_u32, Enc::DELTA_BINARY_PACKED, Enc::RLE, Enc::RLE, Stats.int32(1, 3, nil, 7_u64, true)),
      Page::DataPageV2.new(Bytes.new(128, 4_u8), 10_u32, Enc::DELTA_BINARY_PACKED, 2_u32, 12_u32, 24_u32, 32_u32, false, nil),
    ] of Page

    test_page_roundtrip(pages, Comp.gzip, PType::INT32)
    test_page_roundtrip(pages, Comp::UNCOMPRESSED, PType::INT32)
  end
end
