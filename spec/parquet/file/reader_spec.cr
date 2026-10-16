require "../../spec_helper"

# Ported from `parquet/src/file/reader.rs` and
# `parquet/src/file/serialized_reader.rs` test modules
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `SerializedFileReader` is exercised over `BytesChunkReader` (the upstream
# `TryFrom<File>`/path tests are adapted to in-memory bytes); tests needing the
# record API (`get_row_iter`) are deferred to the record chunk.

private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader
private alias ReadOptionsBuilder = Parquet::File::ReadOptionsBuilder
private alias Page = Parquet::Column::Page
private alias PType = Parquet::Type
private alias Enc = Parquet::Encoding
private alias COrder = Parquet::ColumnOrder

private def alltypes_plain : Bytes
  ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")
end

describe Parquet::File::BytesChunkReader do
  # Upstream `test_bytes_chunk_reader_get_read_out_of_bounds`.
  it "errors when get_read is out of bounds" do
    data = Bytes[0_u8, 1_u8, 2_u8, 3_u8]
    reader = BytesCR.new(data)
    expect_raises(Parquet::EOFError, "EOF: Expected to read at offset 5, while file has length 4") do
      reader.get_read(5_u64)
    end
  end

  # Upstream `test_bytes_chunk_reader_get_bytes_out_of_bounds`.
  it "errors when get_bytes is out of bounds" do
    data = Bytes[0_u8, 1_u8, 2_u8, 3_u8]
    reader = BytesCR.new(data)
    expect_raises(Parquet::EOFError, "EOF: Expected to read 1 bytes at offset 5, while file has length 4") do
      reader.get_bytes(5_u64, 1)
    end
    expect_raises(Parquet::EOFError, "EOF: Expected to read 3 bytes at offset 2, while file has length 4") do
      reader.get_bytes(2_u64, 3)
    end
  end
end

describe Parquet::File::SerializedFileReader do
  # Upstream `test_file_reader`.
  it "reads metadata and pages from a file" do
    reader = FReader.new(BytesCR.new(alltypes_plain))

    metadata = reader.metadata
    metadata.num_row_groups.should eq(1)

    file_metadata = metadata.file_metadata
    file_metadata.created_by.should eq(
      "impala version 1.3.0-INTERNAL (build 8a48ddb1eff84592b3fc06bc6f51ec120e1fffc9)"
    )
    file_metadata.key_value_metadata.should be_nil
    file_metadata.num_rows.should eq(8)
    file_metadata.version.should eq(1)
    file_metadata.column_orders.should be_nil

    row_group_metadata = metadata.row_group(0)
    row_group_metadata.num_columns.should eq(11)
    row_group_metadata.num_rows.should eq(8)
    row_group_metadata.total_byte_size.should eq(671)
    11.times do |i|
      file_metadata.column_order(i).should eq(COrder::UNDEFINED)
    end

    row_group_reader = reader.get_row_group(0)
    row_group_reader.num_columns.should eq(row_group_metadata.num_columns)
    row_group_reader.metadata.total_byte_size.should eq(row_group_metadata.total_byte_size)

    page_reader_0 = row_group_reader.get_column_page_reader(0)
    page_count = 0
    while page = page_reader_0.get_next_page
      case page
      when Page::DictionaryPage
        page.buf.size.should eq(32)
        page.num_values.should eq(8)
        page.encoding.should eq(Enc::PLAIN_DICTIONARY)
        page.is_sorted.should be_false
      when Page::DataPage
        page.buf.size.should eq(11)
        page.num_values.should eq(8)
        page.encoding.should eq(Enc::PLAIN_DICTIONARY)
        page.def_level_encoding.should eq(Enc::RLE)
        page.rep_level_encoding.should eq(Enc::BIT_PACKED)
        page.statistics.should be_nil
      when Page::DataPageV2
        raise "unexpected data page v2"
      end
      page_count += 1
    end
    page_count.should eq(2)
  end

  # Upstream `test_reuse_file_chunk`.
  it "reuses file chunk readers across columns" do
    reader = FReader.new(BytesCR.new(alltypes_plain))
    row_group = reader.get_row_group(0)

    page_readers = [] of Parquet::Column::PageReader
    row_group.num_columns.times do |i|
      page_readers << row_group.get_column_page_reader(i)
    end

    page_readers.each do |page_reader|
      page_reader.get_next_page
    end
  end

  # Upstream `test_file_reader_try_from` (adapted to bytes).
  it "builds from valid bytes and raises on invalid bytes" do
    FReader.new(BytesCR.new(alltypes_plain)).num_row_groups.should eq(1)

    expect_raises(Parquet::ParquetError) do
      FReader.new(BytesCR.new(Bytes[0_u8, 1_u8, 2_u8, 3_u8]))
    end
  end

  # Upstream `test_file_reader_with_no_filter`.
  it "reads with no filter" do
    reader = FReader.new(BytesCR.new(alltypes_plain))
    reader.metadata.num_row_groups.should eq(1)
  end

  # Upstream `test_file_reader_filter_row_groups_with_predicate`.
  it "filters row groups with a predicate" do
    options = ReadOptionsBuilder.new
      .with_predicate(->(_rg : Parquet::File::Metadata::RowGroupMetaData, _i : Int32) { false })
      .build
    reader = FReader.new_with_options(BytesCR.new(alltypes_plain), options)
    reader.metadata.num_row_groups.should eq(0)
  end

  # Upstream `test_file_reader_filter_row_groups_with_range`.
  it "filters row groups with a range" do
    origin = FReader.new(BytesCR.new(alltypes_plain))
    mid = Parquet::File.get_midpoint_offset(origin.metadata.row_group(0))

    options = ReadOptionsBuilder.new.with_range(0_i64, mid + 1).build
    FReader.new_with_options(BytesCR.new(alltypes_plain), options).metadata.num_row_groups.should eq(1)

    options = ReadOptionsBuilder.new.with_range(0_i64, mid).build
    FReader.new_with_options(BytesCR.new(alltypes_plain), options).metadata.num_row_groups.should eq(0)
  end
end
