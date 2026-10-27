require "../../spec_helper"
require "../../support/async_reader_helpers"

alias AsyncMeta = Parquet::File::Metadata
alias FetchA = Parquet::Arrow

private class RecordingMetadataFetch
  include FetchA::MetadataSuffixFetch
  getter ranges = [] of Range(UInt64, UInt64)
  getter suffixes = [] of Int32
  property? short_read = false

  def initialize(@bytes : Bytes)
  end

  def fetch(range : Range(UInt64, UInt64)) : Bytes
    @ranges << range
    Fiber.yield
    result = Parquet::File::BytesChunkReader.new(@bytes).get_bytes(range.begin, (range.end - range.begin).to_i32)
    @short_read ? result[0, result.size - 1] : result
  end

  def fetch_suffix(length : Int32) : Bytes
    @suffixes << length
    Fiber.yield
    count = Math.min(length, @bytes.size)
    @bytes[@bytes.size - count, count]
  end
end

describe "Async metadata fetches (file/metadata/reader.rs)" do
  it "ports prefetch request counts for bounded reads" do
    bytes = AsyncReaderSpecs.fixture
    expected = AsyncReaderSpecs.file(bytes).metadata
    [nil, 7, 10, bytes.size, bytes.size * 2].each do |hint|
      source = RecordingMetadataFetch.new(bytes)
      result = AsyncMeta::ParquetMetaDataReader.new.with_prefetch_hint(hint).load_and_finish(source, bytes.size.to_u64)
      result.file_metadata.should eq expected.file_metadata
      source.ranges.size.should eq(hint && hint >= bytes.size ? 1 : 2)
      source.suffixes.should be_empty
    end
  end

  it "ports suffix request counts without HEAD or known size" do
    bytes = AsyncReaderSpecs.fixture
    [nil, 7, 10, bytes.size, bytes.size * 2].each do |hint|
      source = RecordingMetadataFetch.new(bytes)
      result = AsyncMeta::ParquetMetaDataReader.new.with_prefetch_hint(hint).load_via_suffix_and_finish(source)
      result.file_metadata.num_rows.should eq 12
      source.suffixes.size.should eq(hint && hint >= bytes.size ? 1 : 2)
      source.ranges.should be_empty
    end
  end

  it "loads column/offset indexes and reuses prefetched index bytes" do
    bytes = AsyncReaderSpecs.fixture
    source = RecordingMetadataFetch.new(bytes)
    metadata = AsyncMeta::ParquetMetaDataReader.new.with_page_index_policy(AsyncMeta::PageIndexPolicy::Required)
      .with_prefetch_hint(bytes.size).load_and_finish(source, bytes.size.to_u64)
    metadata.page_index.should_not be_nil
    metadata.page_index.try(&.offset_indexes?).should eq true
    source.ranges.size.should eq 1
    source = RecordingMetadataFetch.new(bytes)
    AsyncMeta::ParquetMetaDataReader.new.with_page_index_policy(AsyncMeta::PageIndexPolicy::Required).load_via_suffix_and_finish(source).page_index.should_not be_nil
    source.ranges.size.should eq 1
  end

  it "rejects short, malformed and undersized metadata with upstream errors" do
    source = RecordingMetadataFetch.new(AsyncReaderSpecs.fixture)
    expect_raises(Parquet::EOFError, "file size of 4 is less than footer") { AsyncMeta::ParquetMetaDataReader.new.load_and_finish(source, 4_u64) }
    source.short_read = true
    expect_raises(Parquet::EOFError, "metadata requires") { AsyncMeta::ParquetMetaDataReader.new.load_and_finish(source, 20_u64) }
    expect_raises(Parquet::EOFError, "footer metadata requires 8 bytes") do
      AsyncMeta::ParquetMetaDataReader.new.load_via_suffix_and_finish(RecordingMetadataFetch.new(Bytes[1, 2, 3]))
    end
    expect_raises(Parquet::ParquetError) { AsyncMeta::ParquetMetaDataReader.new.load_via_suffix_and_finish(RecordingMetadataFetch.new("not a parquet file".to_slice)) }
  end

  it "accepts fetch closures and loads indexes onto existing metadata" do
    bytes = AsyncReaderSpecs.fixture
    source = RecordingMetadataFetch.new(bytes)
    fetch = FetchA::MetadataFetchFn.new { |range| source.fetch(range) }
    metadata = AsyncMeta::ParquetMetaDataReader.new.load_and_finish(fetch, bytes.size.to_u64)
    reader = AsyncMeta::ParquetMetaDataReader.new_with_metadata(metadata).with_page_index_policy(AsyncMeta::PageIndexPolicy::Required)
    reader.load_page_index(fetch)
    reader.finish.page_index.should_not be_nil
    suffix = FetchA::MetadataSuffixFetchFn.new(->(range : Range(UInt64, UInt64)) { source.fetch(range) }, ->(count : Int32) { source.fetch_suffix(count) })
    AsyncMeta::ParquetMetaDataReader.new.load_via_suffix_and_finish(suffix).file_metadata.num_rows.should eq 12
  end

  it "threads Arrow metadata policies and schema hints into async footer decoding" do
    bytes = AsyncReaderSpecs.fixture
    descriptor = AsyncReaderSpecs.file(bytes).metadata.file_metadata.schema_descr
    policy = AsyncMeta::ParquetStatisticsPolicy::SkipAll
    options = FetchA::ArrowReaderOptions.new.with_column_stats_policy(policy).with_size_stats_policy(policy)
      .with_encoding_stats_policy(policy).with_encoding_stats_as_mask(false).with_parquet_schema(descriptor)
    options.metadata_options.encoding_stats_as_mask?.should be_false
    options.metadata_options.schema.should eq descriptor
    reader = FetchA::AsyncSeekableReader.new(IO::Memory.new(bytes))
    metadata = reader.get_metadata(options)
    metadata.row_groups.each do |group|
      group.columns.each do |column|
        column.statistics.should be_nil
        column.encoding_stats.should be_nil
      end
    end
    metadata.file_metadata.schema_descr.should eq descriptor
  end

  it "honors per-column policies and full encoding-stat lists without changing defaults" do
    bytes = AsyncReaderSpecs.fixture
    options = FetchA::ArrowReaderOptions.new.with_column_stats_policy(AsyncMeta::ParquetStatisticsPolicy.skip_except([0]))
      .with_encoding_stats_policy(AsyncMeta::ParquetStatisticsPolicy.skip_except([1])).with_encoding_stats_as_mask(false)
      .with_size_stats_policy(AsyncMeta::ParquetStatisticsPolicy::SkipAll)
    metadata = FetchA::AsyncSeekableReader.new(IO::Memory.new(bytes)).get_metadata(options)
    metadata.row_groups.each do |group|
      group.column(0).statistics.should_not be_nil
      group.column(1).statistics.should be_nil
      group.column(0).encoding_stats.should be_nil
      group.column(1).page_encoding_stats.should_not be_nil
      group.column(1).encoding_stats.try(&.mask).should be_nil
      group.columns.each do |column|
        column.unencoded_byte_array_data_bytes.should be_nil
        column.repetition_level_histogram.should be_nil
        column.definition_level_histogram.should be_nil
      end
    end
  end
end
