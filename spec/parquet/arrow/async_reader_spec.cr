require "../../spec_helper"
require "../../support/async_reader_helpers"

# Ported/characterized from arrow/async_reader/mod.rs at apache/arrow-rs
# 60.0.0 (ef1fa157977633f0ba21aa921f9ef3d5c669235b).
alias AsyncSpecs = AsyncReaderSpecs
alias AsyncA = Parquet::Arrow

describe AsyncA::ParquetRecordBatchStreamBuilder do
  it "ports test_async_reader with exact values and projected range requests" do
    bytes = ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")
    source = AsyncSpecs::TestReader.new(bytes)
    builder = AsyncA::ParquetRecordBatchStreamBuilder.new(source)
    builder.metadata.num_row_groups.should eq 1
    mask = AsyncA::ProjectionMask.leaves(builder.parquet_schema, [1, 2])
    stream = builder.with_projection(mask).with_batch_size(1024).build
    source.requests.should be_empty
    expected = AsyncA::ParquetRecordBatchReaderBuilder.try_new(AsyncSpecs.file(bytes)).with_projection(mask).with_batch_size(104).build.to_a
    stream.to_a.should eq expected
    expected_ranges = [1, 2].map do |index|
      start, length = builder.metadata.row_group(0).column(index).byte_range
      start...(start + length)
    end
    source.requests.should eq expected_ranges
  end

  it "ports row-group-local selections in supplied order" do
    source = AsyncSpecs::TestReader.new(AsyncSpecs.fixture)
    stream = AsyncA::ParquetRecordBatchStreamBuilder.new(source).with_row_group_selections([
      AsyncA::RowGroupSelection.new(1, AsyncA::RowSelection.new([AsyncA::RowSelector.select(1)])),
      AsyncA::RowGroupSelection.new(0, AsyncA::RowSelection.new([AsyncA::RowSelector.skip(1), AsyncA::RowSelector.select(2)])),
    ]).build
    AsyncSpecs.ids(stream.to_a).should eq [4, 1, 2]
    expect_raises(Parquet::ParquetError) do
      AsyncA::ParquetRecordBatchStreamBuilder.new(source).with_row_group_selections([] of AsyncA::RowGroupSelection).with_row_groups([0]).build
    end
  end

  it "ports test_async_reader_with_next_row_group and independent readers" do
    bytes = AsyncSpecs.fixture
    builder = AsyncA::ParquetRecordBatchStreamBuilder.new(AsyncSpecs::TestReader.new(bytes))
    stream = builder.with_batch_size(2).build
    readers = [] of AsyncA::ParquetRecordBatchReader
    while reader = stream.next_row_group
      readers << reader
    end
    readers.size.should eq 3
    AsyncSpecs.ids(readers.flat_map(&.to_a)).should eq (0...12).to_a
    stream.next_row_group.should be_nil
  end

  it "ports limit/offset across reordered row groups and selection" do
    bytes = AsyncSpecs.fixture
    selection = AsyncA::RowSelection.new([AsyncA::RowSelector.skip(1), AsyncA::RowSelector.select(7), AsyncA::RowSelector.skip(4)])
    builder = AsyncA::ParquetRecordBatchStreamBuilder.new(AsyncSpecs::TestReader.new(bytes))
    stream = builder.with_row_groups([2, 0, 1]).with_row_selection(selection).with_offset(2).with_limit(4).with_batch_size(2).build
    AsyncSpecs.ids(stream.to_a).should eq [11, 0, 1, 2]
  end

  it "ports zero selectors, zero limits, empty projection and batch clamping" do
    bytes = AsyncSpecs.fixture
    source = AsyncSpecs::TestReader.new(bytes)
    builder = AsyncA::ParquetRecordBatchStreamBuilder.new(source).with_batch_size(1024)
    builder.batch_size.should eq 12
    builder.with_limit(0).build.to_a.should be_empty
    source.requests.should be_empty
    builder = AsyncA::ParquetRecordBatchStreamBuilder.new(source)
    builder.with_row_selection(AsyncA::RowSelection.new([AsyncA::RowSelector.select(0), AsyncA::RowSelector.skip(12)])).build.to_a.should be_empty
    stream = AsyncA::ParquetRecordBatchStreamBuilder.new(source).with_projection(AsyncA::ProjectionMask.none(2)).build
    stream.to_a.sum(&.num_rows).should eq 12
    source.requests.should be_empty
    AsyncA::ParquetRecordBatchStreamBuilder.new(AsyncSpecs::TestReader.new(AsyncSpecs.fixture(0))).build.to_a.should be_empty
  end

  it "reuses ArrowReaderMetadata without loading it again" do
    source = AsyncSpecs::TestReader.new(AsyncSpecs.fixture)
    metadata = AsyncA::ArrowReaderMetadata.load_async(source)
    left = AsyncA::ParquetRecordBatchStreamBuilder.new_with_metadata(source, metadata).build
    right = AsyncA::ParquetRecordBatchStreamBuilder.new_with_metadata(source, metadata).build
    left.to_a.should eq right.to_a
    source.metadata_calls.should eq 1
  end

  it "ports projected stream schema without top-level user metadata" do
    source = AsyncSpecs::TestReader.new(AsyncSpecs.fixture)
    builder = AsyncA::ParquetRecordBatchStreamBuilder.new(source)
    builder.schema.metadata["user"].should eq "metadata"
    mask = AsyncA::ProjectionMask.leaves(builder.parquet_schema, [1])
    stream = builder.with_projection(mask).build
    stream.schema.fields.to_a.map(&.name).should eq ["name"]
    stream.schema.metadata.should be_empty
    stream.to_a.each { |batch| batch.schema.should eq stream.schema }
  end

  it "applies predicate pushdown before the output limit and avoids unused columns" do
    source = AsyncSpecs::TestReader.new(AsyncSpecs.fixture)
    builder = AsyncA::ParquetRecordBatchStreamBuilder.new(source)
    mask = AsyncA::ProjectionMask.leaves(builder.parquet_schema, [0])
    predicate = AsyncA::ArrowPredicateFn.new(mask) do |batch|
      values = batch.column(0).as(Arrow::PrimitiveArray(Int32))
      Arrow::BooleanArray.from_values(values.to_a.map { |value| (value || -1).even? })
    end
    stream = builder.with_projection(mask).with_row_filter(AsyncA::RowFilter.new([predicate] of AsyncA::ArrowPredicate)).with_offset(1).with_limit(3).build
    AsyncSpecs.ids(stream.to_a).should eq [2, 4, 6]
    name_ranges = builder.metadata.row_groups.map do |group|
      start, length = group.column(1).byte_range
      start...(start + length)
    end
    source.requests.each { |range| name_ranges.should_not contain(range) }
  end

  it "propagates a storage error once and remains finished" do
    source = AsyncSpecs::TestReader.new(AsyncSpecs.fixture)
    stream = AsyncA::ParquetRecordBatchStreamBuilder.new(source).build
    error = IO::Error.new("range fetch failed")
    source.failure = error
    expect_raises(IO::Error, "range fetch failed") { stream.next }
    requests = source.requests.size
    stream.next.should be_a(Iterator::Stop)
    stream.next_row_group.should be_nil
    source.requests.size.should eq requests
  end

  it "rejects short range responses and fuses the failed stream" do
    source = AsyncSpecs::TestReader.new(AsyncSpecs.fixture)
    source.short_read = true
    stream = AsyncA::ParquetRecordBatchStreamBuilder.new(source).build
    expect_raises(Parquet::ParquetError) { stream.next }
    stream.next.should be_a(Iterator::Stop)
  end

  it "loads bloom filters with or without recorded lengths" do
    source = AsyncSpecs::TestReader.new(AsyncSpecs.fixture(12, true))
    builder = AsyncA::ParquetRecordBatchStreamBuilder.new(source)
    bloom = builder.get_row_group_column_bloom_filter(0, 0) || raise "Missing bloom"
    (0...4).each { |value| bloom.check(value).should be_true }
    column = builder.metadata.row_group(0).column(0)
    column.bloom_filter_length = nil
    builder.get_row_group_column_bloom_filter(0, 0).try(&.check(3)).should eq true
    no_bloom = AsyncA::ParquetRecordBatchStreamBuilder.new(AsyncSpecs::TestReader.new(AsyncSpecs.fixture))
    no_bloom.get_row_group_column_bloom_filter(0, 0).should be_nil
  end

  it "ports page-index loading and exact tiny-page selection" do
    bytes = ParquetTestHelpers.fixture_bytes("alltypes_tiny_pages_plain.parquet")
    source = AsyncSpecs::TestReader.new(bytes)
    options = AsyncA::ArrowReaderOptions.new.with_page_index_policy(Parquet::File::Metadata::PageIndexPolicy::Required)
    builder = AsyncA::ParquetRecordBatchStreamBuilder.new_with_options(source, options)
    builder.metadata.page_index.try(&.complete?).should eq true
    selection = AsyncA::RowSelection.new([
      AsyncA::RowSelector.skip(21), AsyncA::RowSelector.select(21), AsyncA::RowSelector.skip(41), AsyncA::RowSelector.select(41),
      AsyncA::RowSelector.skip(25), AsyncA::RowSelector.select(25), AsyncA::RowSelector.skip(7116), AsyncA::RowSelector.select(10),
    ])
    mask = AsyncA::ProjectionMask.leaves(builder.parquet_schema, [9])
    actual = builder.with_projection(mask).with_row_selection(selection).with_batch_size(1024).build.to_a
    expected = AsyncA::ParquetRecordBatchReaderBuilder.try_new(AsyncSpecs.file(bytes)).with_projection(mask).with_row_selection(selection).with_batch_size(1024).build.to_a
    actual.should eq expected
    actual.sum(&.num_rows).should eq 97
    start, length = builder.metadata.row_group(0).column(9).byte_range
    source.requests.sum { |range| range.end - range.begin }.should be < length
    source.requests.each { |range| range.begin.should be >= start }
  end

  it "ports the 100-case async selection fuzz test with a reproducible seed" do
    bytes = ParquetTestHelpers.fixture_bytes("alltypes_tiny_pages_plain.parquet")
    random = Random.new(42_u64)
    100.times do
      selectors = [] of AsyncA::RowSelector
      rows = 0
      expected = 0
      skip = false
      while rows < 7300
        count = Math.min(random.rand(1..99), 7300 - rows)
        selectors << AsyncA::RowSelector.new(count, skip)
        expected += count unless skip
        rows += count
        skip = !skip
      end
      options = AsyncA::ArrowReaderOptions.new.with_page_index_policy(Parquet::File::Metadata::PageIndexPolicy::Required)
      builder = AsyncA::ParquetRecordBatchStreamBuilder.new_with_options(AsyncSpecs::TestReader.new(bytes), options)
      builder.metadata.num_row_groups.should eq 1
      mask = AsyncA::ProjectionMask.leaves(builder.parquet_schema, [random.rand(0..12)])
      stream = builder.with_projection(mask).with_row_selection(AsyncA::RowSelection.new(selectors)).build
      stream.to_a.sum(&.num_rows).should eq expected
    end
  end

  it "ports nested lists/structs with null parents, null elements and empty lists" do
    lists = Arrow::GenericListArray(Int32).from_iter_primitive(Int32, [[1, nil], nil, [] of Int32?, ([2] of Int32?)] of Array(Int32?)?)
    values = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 4])
    fields = Arrow::Fields.from([Arrow::Field.new("id", Arrow::DataType.int32, true), Arrow::Field.new("items", lists.data_type, true)])
    array = Arrow::StructArray.new(fields, [values, lists] of Arrow::ArrayBase, Arrow::NullBuffer.from_slice([true, false, true, true]))
    schema = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("nested", array.data_type, true)]))
    output = IO::Memory.new
    writer = AsyncA::ArrowWriter.try_new(output, schema)
    writer.write(Arrow::RecordBatch.try_new(schema, [array] of Arrow::ArrayBase))
    writer.close
    stream = AsyncA::ParquetRecordBatchStreamBuilder.new(AsyncSpecs::TestReader.new(output.to_slice)).build
    stream.schema.fields.should eq schema.fields
    stream.to_a.first.column(0).should eq array
  end

  it "preserves global virtual row/group indices in reordered asynchronous scans" do
    fields = [Arrow::Field.new("row_number", Arrow::DataType.int64, false).with_extension_type(AsyncA::RowNumber.new),
              Arrow::Field.new("group_index", Arrow::DataType.int64, false).with_extension_type(AsyncA::RowGroupIndex.new)]
    options = AsyncA::ArrowReaderOptions.new.with_virtual_columns(fields)
    builder = AsyncA::ParquetRecordBatchStreamBuilder.new_with_options(AsyncSpecs::TestReader.new(AsyncSpecs.fixture), options)
    stream = builder.with_projection(AsyncA::ProjectionMask.none(2)).with_row_groups([2, 0]).with_offset(1).with_limit(5).build
    batches = stream.to_a
    batches.flat_map { |batch| batch.column(0).as(Arrow::PrimitiveArray(Int64)).to_a }.should eq [9_i64, 10_i64, 11_i64, 0_i64, 1_i64]
    batches.flat_map { |batch| batch.column(1).as(Arrow::PrimitiveArray(Int64)).to_a }.should eq [2_i64, 2_i64, 2_i64, 0_i64, 0_i64]
    batches.each { |batch| batch.schema.should eq stream.schema }
  end

  it "reads encrypted footer metadata and pages through suffix/range fetches" do
    key = Bytes.new(16, 3_u8)
    array = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 4])
    schema = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("encrypted", array.data_type, true)]))
    properties = Parquet::File::Properties::WriterProperties.builder.set_file_encryption_properties(Parquet::Encryption::FileEncryptionProperties.builder(key).build).build
    output = IO::Memory.new
    writer = AsyncA::ArrowWriter.try_new(output, schema, properties)
    writer.write(Arrow::RecordBatch.try_new(schema, [array] of Arrow::ArrayBase))
    writer.close
    source = AsyncA::AsyncSeekableReader.new(IO::Memory.new(output.to_slice))
    options = AsyncA::ArrowReaderOptions.new.with_file_decryption_properties(Parquet::Encryption::FileDecryptionProperties.builder(key).build)
    stream = AsyncA::ParquetRecordBatchStreamBuilder.new_with_options(source, options).build
    stream.to_a.first.column(0).should eq array
  end

  it "ports sparse-page predicate-cache enabled/disabled behavior and metrics" do
    array = Arrow::PrimitiveArray(Int32).from_values((0...10).to_a)
    schema = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("k", array.data_type, false)]))
    properties = Parquet::File::Properties::WriterProperties.builder.set_data_page_row_count_limit(1).set_write_batch_size(1)
      .set_max_row_group_row_count(10).set_write_page_header_statistics(true).build
    output = IO::Memory.new
    writer = AsyncA::ArrowWriter.try_new(output, schema, properties)
    writer.write(Arrow::RecordBatch.try_new(schema, [array] of Arrow::ArrayBase))
    writer.close
    [0, 1024 * 1024].each do |cache_limit|
      options = AsyncA::ArrowReaderOptions.new.with_page_index_policy(Parquet::File::Metadata::PageIndexPolicy::Required)
      builder = AsyncA::ParquetRecordBatchStreamBuilder.new_with_options(AsyncSpecs::TestReader.new(output.to_slice), options)
      mask = AsyncA::ProjectionMask.leaves(builder.parquet_schema, [0])
      predicate = AsyncA::ArrowPredicateFn.new(mask) do |batch|
        Arrow::BooleanArray.from_values(batch.column(0).as(Arrow::PrimitiveArray(Int32)).to_a.map { |value| value == 5 })
      end
      selection = AsyncA::RowSelection.new([AsyncA::RowSelector.skip(5), AsyncA::RowSelector.select(1)])
      metrics = AsyncA::ArrowReaderMetrics.enabled
      stream = builder.with_projection(mask).with_row_selection(selection).with_row_filter(AsyncA::RowFilter.new([predicate] of AsyncA::ArrowPredicate))
        .with_max_predicate_cache_size(cache_limit).with_metrics(metrics).build
      AsyncSpecs.ids(stream.to_a).should eq [5]
      if cache_limit == 0
        metrics.records_read_from_inner.should eq 0
        metrics.records_read_from_cache.should eq 0
      else
        (metrics.records_read_from_inner || 0).should be > 0
        (metrics.records_read_from_cache || 0).should be > 0
      end
    end
  end
end
