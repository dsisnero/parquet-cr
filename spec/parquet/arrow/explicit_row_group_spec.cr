require "../../spec_helper"
require "../../support/async_reader_helpers"

# Characterizations of the complete parquet/examples/read_with_rowgroup.rs
# workflow, arrow-rs 60.0.0 ef1fa157. The example has no unit tests.
private alias ER = Parquet::Examples::ReadWithRowGroup
private alias EA = Parquet::Arrow
private alias ES = AsyncReaderSpecs

describe "Explicit row-group fetching" do
  it "ports the pinned alltypes_plain example with exact batch values and tables" do
    bytes = ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")
    expected = EA::ParquetRecordBatchReaderBuilder.try_new(ES.file(bytes)).with_batch_size(1024).build.to_a
    source = ES::TestReader.new(bytes)
    output = IO::Memory.new
    ER.run(source, output)
    source.metadata_calls.should eq 1
    output.to_s.should eq expected.map { |batch| Arrow.pretty_format_batches([batch]) + "\n" }.join
    metadata = ES.file(bytes).metadata
    source.requests.should eq metadata.row_groups.flat_map { |group| group.columns.map { |column| start, size = column.byte_range; start...(start + size) } }
  end

  it "fetches only projected whole column chunks and uses selection only during decoding" do
    source = ES::TestReader.new(ES.fixture)
    metadata = source.get_metadata
    mask = EA::ProjectionMask.leaves(metadata.file_metadata.schema_descr, [0])
    group = ER::InMemoryRowGroup.create(metadata, 1, mask)
    group.num_rows.should eq 4
    group.row_groups.to_a.should eq [metadata.row_group(1)]
    group.metadata.same?(metadata).should be_true
    expect_raises(Parquet::GeneralError, "Invalid column index 0, column was not fetched") { group.build_reader(2) }
    selection = EA::RowSelection.new([EA::RowSelector.skip(1), EA::RowSelector.select(2), EA::RowSelector.skip(1)])
    group.async_fetch_data(source, selection)
    start, size = metadata.row_group(1).column(0).byte_range
    source.requests.should eq [start...(start + size)]
    ES.ids(group.build_reader(1, selection).to_a).should eq [5, 6]
    expect_raises(Parquet::GeneralError, "Invalid column index 1, column was not fetched") { group.column_chunks(1) }
    # Every reader is independent, including its single-page-reader iterator.
    ES.ids(group.build_reader(3).to_a).should eq [4, 5, 6, 7]
    ES.ids(group.build_reader(2).to_a).should eq [4, 5, 6, 7]
    iterator = group.column_chunks(0)
    iterator.next.should_not be_a(Iterator::Stop)
    iterator.next.should be_a(Iterator::Stop)
    iterator.next.should be_a(Iterator::Stop)
  end

  it "reads all row groups with nullable values across batch boundaries" do
    bytes = ES.fixture
    source = ES::TestReader.new(bytes)
    metadata = source.get_metadata
    3.times do |index|
      group = ER::InMemoryRowGroup.create(metadata, index, EA::ProjectionMask.all)
      group.async_fetch_data(source)
      expected = EA::ParquetRecordBatchReaderBuilder.try_new(ES.file(bytes)).with_row_groups([index]).with_batch_size(3).build.to_a
      group.build_reader(3).to_a.should eq expected
      group.row_group_metadata.same?(metadata.row_group(index)).should be_true
    end
  end

  it "creates independent clones sharing fetched chunk bytes like source Arc" do
    source = ES::TestReader.new(ES.fixture)
    group = ER::InMemoryRowGroup.create(source.get_metadata, 0, EA::ProjectionMask.all)
    group.async_fetch_data(source)
    clone = group.clone
    clone.metadata.same?(group.metadata).should be_false
    clone.column_chunks[0].same?(group.column_chunks[0]).should be_true
    clone.column_chunks.same?(group.column_chunks).should be_false
    clone.column_chunks.clear
    group.column_chunks.size.should eq 2
    ES.ids(group.build_reader(4).to_a).should eq [0, 1, 2, 3]
  end

  it "refetches whole chunks on each explicit fetch, even with an empty selection" do
    source = ES::TestReader.new(ES.fixture)
    group = ER::InMemoryRowGroup.create(source.get_metadata, 0, EA::ProjectionMask.all)
    group.async_fetch_data(source)
    first = source.requests.dup
    group.async_fetch_data(source, EA::RowSelection.empty)
    source.requests.should eq first + first
    group.build_reader(2, EA::RowSelection.empty).to_a.should be_empty
  end

  it "preserves the source's taken-column state on a failed fetch" do
    source = ES::TestReader.new(ES.fixture)
    group = ER::InMemoryRowGroup.create(source.get_metadata, 0, EA::ProjectionMask.all)
    source.failure = Parquet::GeneralError.new("fetch failed")
    expect_raises(Parquet::GeneralError, "fetch failed") { group.async_fetch_data(source) }
    group.column_chunks.should be_empty
  end

  it "supports empty projection without any data requests" do
    source = ES::TestReader.new(ES.fixture)
    metadata = source.get_metadata
    group = ER::InMemoryRowGroup.create(metadata, 2, EA::ProjectionMask.none(2))
    group.async_fetch_data(source)
    batches = group.build_reader(3).to_a
    batches.map(&.num_rows).should eq [3, 1]
    batches.each { |batch| batch.num_columns.should eq 0 }
    source.requests.should be_empty
  end

  it "reuses dense chunk readers with absolute offsets and byte bounds" do
    chunk = EA::ColumnChunkData::Dense.new(20, Bytes[1, 2, 3, 4])
    chunk.len.should eq 4_u64
    chunk.get(22_u64).should eq Bytes[3, 4]
    chunk.get_bytes(21_u64, 2).should eq Bytes[2, 3]
    chunk.get_read(20_u64).read_bytes(UInt8).should eq 1_u8
    expect_raises(Parquet::GeneralError) { chunk.get(19_u64) }
  end
end
