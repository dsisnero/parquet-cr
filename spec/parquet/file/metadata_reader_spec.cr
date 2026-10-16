require "../../spec_helper"

# Ported from `parquet/src/file/metadata/reader.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# The async/suffix tests and `test_page_index_policy` (which writes files with
# the Arrow writer) are not portable; `test_page_index_policy` is characterized
# against golden fixtures instead.
private def tiny_pages_bytes : Bytes
  ParquetTestHelpers.fixture_bytes("alltypes_tiny_pages.parquet")
end

private def range_bytes(bytes : Bytes, range : Range(UInt64, UInt64)) : Bytes
  bytes[range.begin.to_i32, (range.end - range.begin).to_i32]
end

private def reader_for(bytes : Bytes) : Parquet::File::BytesChunkReader
  Parquet::File::BytesChunkReader.new(bytes)
end

describe Parquet::File::Metadata::ParquetMetaDataReader do
  # Upstream `test_parse_metadata_size_smaller_than_footer`.
  it "requests more data when the file is smaller than the footer" do
    err = expect_raises(Parquet::NeedMoreDataError) do
      PMeta::ParquetMetaDataReader.new.parse_metadata(reader_for(Bytes.new(0)))
    end
    err.needed.should eq(8_u64)
  end

  # Upstream `test_parse_metadata_corrupt_footer`.
  it "rejects a corrupt footer" do
    err = expect_raises(Parquet::ParquetError) do
      PMeta::ParquetMetaDataReader.new.parse_metadata(reader_for(Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 7_u8, 8_u8]))
    end
    err.to_s.should eq("Parquet error: Invalid Parquet file. Corrupt footer")
  end

  # Upstream `test_parse_metadata_invalid_start`.
  it "requests the full footer + metadata when the declared length is too large" do
    bytes = Bytes[255_u8, 0_u8, 0_u8, 0_u8, 0x50_u8, 0x41_u8, 0x52_u8, 0x31_u8]
    err = expect_raises(Parquet::NeedMoreDataError) do
      PMeta::ParquetMetaDataReader.new.parse_metadata(reader_for(bytes))
    end
    err.needed.should eq(263_u64)
  end

  # Upstream `test_try_parse`.
  it "parses metadata and page indexes with retries" do
    bytes = tiny_pages_bytes
    len = bytes.size.to_u64

    reader = PMeta::ParquetMetaDataReader.new
      .with_page_index_policy(Parquet::File::Metadata::PageIndexPolicy::Required)

    # read entire file
    reader.try_parse(reader_for(range_bytes(bytes, 0_u64...len)))
    reader.finish.page_index.not_nil!.complete?.should be_true

    # read more than enough of the file
    reader.try_parse_sized(reader_for(range_bytes(bytes, 320000_u64...len)), len)
    reader.finish.page_index.not_nil!.complete?.should be_true

    # exactly enough
    reader.try_parse_sized(reader_for(range_bytes(bytes, 323583_u64...len)), len)
    reader.finish.page_index.not_nil!.complete?.should be_true

    # not enough for the page index
    err = expect_raises(Parquet::NeedMoreDataError) do
      reader.try_parse_sized(reader_for(range_bytes(bytes, 323584_u64...len)), len)
    end
    reader.try_parse_sized(reader_for(range_bytes(bytes, len - err.needed...len)), len)
    reader.finish.page_index.not_nil!.complete?.should be_true

    # not enough for the file metadata, keep trying until page indexes are read
    reader = PMeta::ParquetMetaDataReader.new
      .with_page_index_policy(Parquet::File::Metadata::PageIndexPolicy::Required)
    tail = range_bytes(bytes, 452505_u64...len)
    loop do
      begin
        reader.try_parse_sized(reader_for(tail), len)
        break
      rescue err : Parquet::NeedMoreDataError
        tail = range_bytes(bytes, len - err.needed...len)
        if reader.has_metadata?
          reader.read_page_indexes_sized(reader_for(tail), len)
          break
        end
      end
    end
    reader.finish.page_index.not_nil!.complete?.should be_true

    # not enough for the page index but lie about the file size
    err = expect_raises(Parquet::ParquetError) do
      reader.try_parse_sized(reader_for(range_bytes(bytes, 323584_u64...len)), len - 323584)
    end
    err.to_s.should eq("EOF: Parquet file too small. Range 323583..452504 is beyond file bounds 130649")

    # not enough for the file metadata
    reader = PMeta::ParquetMetaDataReader.new
    short_tail = range_bytes(bytes, 452505_u64...len)
    err = expect_raises(Parquet::NeedMoreDataError) do
      reader.try_parse_sized(reader_for(short_tail), len)
    end
    full_tail = range_bytes(bytes, len - err.needed...len)
    reader.try_parse_sized(reader_for(full_tail), len)
    reader.finish.should_not be_nil

    # not enough for the file metadata but use try_parse() with the short tail
    err = expect_raises(Parquet::ParquetError) do
      reader.try_parse(reader_for(short_tail))
    end
    err.to_s.should eq("EOF: Parquet file too small. Size is 1728 but need 1729")

    # read the head of the file rather than the tail
    err = expect_raises(Parquet::ParquetError) do
      reader.try_parse_sized(reader_for(range_bytes(bytes, 0_u64...1000_u64)), len)
    end
    err.to_s.should eq("Parquet error: Invalid Parquet file. Corrupt footer")

    # lie about the file size
    err = expect_raises(Parquet::ParquetError) do
      reader.try_parse_sized(reader_for(range_bytes(bytes, 452510_u64...len)), len - 452505)
    end
    err.to_s.should eq("EOF: Parquet file too small. Size is 1728 but need 1729")
  end

  it "reports the serialized metadata size" do
    bytes = tiny_pages_bytes
    reader = PMeta::ParquetMetaDataReader.new
    reader.parse_metadata(reader_for(bytes))
    reader.metadata_size.should_not be_nil
  end

  it "exposes metadata and supports new_with_metadata / read_page_indexes" do
    bytes = tiny_pages_bytes
    len = bytes.size.to_u64

    metadata = PMeta::ParquetMetaDataReader.new
      .with_page_index_policy(Parquet::File::Metadata::PageIndexPolicy::Skip)
      .parse_and_finish(reader_for(bytes))

    metadata.page_index.should be_nil
    metadata.num_row_groups.should be > 0

    reader = PMeta::ParquetMetaDataReader.new_with_metadata(metadata)
      .with_page_index_policy(Parquet::File::Metadata::PageIndexPolicy::Required)
    reader.has_metadata?.should be_true
    reader.read_page_indexes(reader_for(bytes))
    reader.finish.page_index.not_nil!.complete?.should be_true
  end

  it "raises when finish is called without metadata" do
    expect_raises(Parquet::ParquetError, "could not parse parquet metadata") do
      PMeta::ParquetMetaDataReader.new.finish
    end
  end

  # Characterization of `test_page_index_policy` (upstream writes files with
  # the Arrow writer; here golden fixtures are used).
  it "honors the page index policy" do
    with_index = tiny_pages_bytes

    [Parquet::File::Metadata::PageIndexPolicy::Required,
     Parquet::File::Metadata::PageIndexPolicy::Optional,
     Parquet::File::Metadata::PageIndexPolicy::Skip].each do |policy|
      PMeta::ParquetMetaDataReader.new
        .with_page_index_policy(policy)
        .parse_and_finish(reader_for(with_index)).should_not be_nil
    end

    without_index = ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")
    [Parquet::File::Metadata::PageIndexPolicy::Required,
     Parquet::File::Metadata::PageIndexPolicy::Optional,
     Parquet::File::Metadata::PageIndexPolicy::Skip].each do |policy|
      PMeta::ParquetMetaDataReader.new
        .with_page_index_policy(policy)
        .parse_and_finish(reader_for(without_index)).should_not be_nil
    end

    # Required errors when a column lacks an offset index.
    schema_descr = ParquetTestHelpers.test_schema_descr
    columns = schema_descr.columns.map { |col| PMeta::ColumnChunkMetaData.builder(col).build }
    row_group = PMeta::RowGroupMetaData.builder(schema_descr)
      .set_num_rows(1_i64)
      .set_total_byte_size(1_i64)
      .set_column_metadata(columns)
      .build
    metadata = PMeta::ParquetMetaData.new(
      PMeta::FileMetaData.new(1, 1_i64, nil, nil, schema_descr, nil), [row_group]
    )

    expect_raises(Parquet::ParquetError, "missing offset index") do
      PMeta.parse_page_index(
        metadata,
        Parquet::File::Metadata::PageIndexPolicy::Required,
        Parquet::File::Metadata::PageIndexPolicy::Required,
        Bytes.new(0),
        0_u64,
      )
    end
  end

  it "decodes metadata and schema from raw footer bytes" do
    bytes = tiny_pages_bytes
    file_len = bytes.size
    reader = PMeta::ParquetMetaDataReader.new
    reader.parse_metadata(reader_for(bytes))
    metadata_size = reader.metadata_size.not_nil!
    raw = bytes[file_len - metadata_size, metadata_size - 8]

    decoded = PMeta::ParquetMetaDataReader.decode_metadata(raw)
    decoded.num_row_groups.should be > 0

    PMeta::ParquetMetaDataReader.decode_schema(raw).num_columns.should be > 0
  end
end
