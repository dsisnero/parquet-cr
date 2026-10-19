require "../../spec_helper"

# Ported from `parquet/src/arrow/in_memory_row_group.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `InMemoryRowGroup`, `FetchRanges` and `ColumnChunkData` (dense and sparse
# column chunks over caller-supplied bytes, as used by the push decoder).
#
# Upstream has no dedicated tests for this module (the push decoder exercises
# it); the port characterizes it directly: whole-chunk fetching (dense),
# page-pruned fetching with an offset index and a row selection (sparse), the
# `ColumnChunkData` accessors/errors, and decoding the selected rows from an
# in-memory row group through the regular array readers and read plan.
#
# Divergences: `usize` counts are `Int32`, byte ranges are `Range(UInt64,
# UInt64)`, the `ColumnChunkData` enum is an abstract class with `Dense`/
# `Sparse` subclasses, and `fetch_ranges` takes an explicit `batch_size`
# (upstream takes it from the reader's configuration).
private alias PArrow = Parquet::Arrow
private alias BytesCR = Parquet::File::BytesChunkReader
private alias Metadata = Parquet::File::Metadata
private alias InMemoryRowGroup = Parquet::Arrow::InMemoryRowGroup
private alias ColumnChunkData = Parquet::Arrow::ColumnChunkData
private alias RowSelection = Parquet::Arrow::RowSelection
private alias RowSelector = Parquet::Arrow::RowSelector

private def fixture_reader(name : String, page_index : Bool = false) : Parquet::File::SerializedFileReader
  options =
    if page_index
      Parquet::File::ReadOptionsBuilder.new.with_page_index.build
    else
      Parquet::File::ReadOptionsBuilder.new.build
    end
  Parquet::File::SerializedFileReader.new_with_options(
    BytesCR.new(ParquetTestHelpers.fixture_bytes(name)), options
  )
end

private def leaf_projection(reader : Parquet::File::SerializedFileReader, leaves : Array(Int32)) : PArrow::ProjectionMask
  PArrow::ProjectionMask.leaves(reader.metadata.file_metadata.schema_descr, leaves)
end

# Builds a row group for row group 0 with one slot per column (all unfetched).
private def row_group_shell(reader : Parquet::File::SerializedFileReader) : InMemoryRowGroup
  metadata = reader.metadata
  row_group = metadata.row_group(0)
  num_columns = metadata.file_metadata.schema_descr.num_columns
  page_index = metadata.page_index.try { metadata.page_index_for_row_group(0) }
  InMemoryRowGroup.new(
    row_group.num_rows.to_i32,
    Array(ColumnChunkData?).new(num_columns, nil),
    page_index,
    0,
    metadata,
  )
end

# Fetches the ranges requested by `fetch_ranges` from `bytes` and fills them in.
private def fill_from(
  group : InMemoryRowGroup,
  bytes : Bytes,
  projection : PArrow::ProjectionMask,
  selection : RowSelection? = nil,
  batch_size : Int32 = 1024,
) : InMemoryRowGroup
  fetch = group.fetch_ranges(projection, selection, batch_size, nil)
  chunks = fetch.ranges.map { |range| bytes[range.begin.to_i32, (range.end - range.begin).to_i32] }
  group.fill_column_chunks(projection, fetch.page_start_offsets, chunks)
  group
end

private def ids(batches : Array(Arrow::RecordBatch)) : Array(Int32)
  batches.flat_map do |batch|
    column = batch.column(0).as(Arrow::PrimitiveArray(Int32))
    (0...column.len).map { |idx| column.value(idx).as(Int32) }
  end
end

# Reads the selected rows from an (in-memory) row group through the regular
# array readers and read plan.
private def read_ids(
  row_groups : PArrow::RowGroups,
  fields : PArrow::ParquetField?,
  projection : PArrow::ProjectionMask,
  selection : RowSelection? = nil,
  batch_size : Int32 = 1024,
) : Array(Int32)
  array_reader = PArrow::ArrayReaderBuilder.new(row_groups, batch_size)
    .build_array_reader(fields, projection)
  plan = PArrow::ReadPlanBuilder.new(batch_size).with_selection(selection).build
  ids(PArrow::ParquetRecordBatchReader.new(array_reader, plan).to_a)
end

describe Parquet::Arrow::InMemoryRowGroup do
  it "fetches whole column chunks and decodes them (dense)" do
    reader = fixture_reader("alltypes_plain.parquet")
    bytes = ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")
    fields = PArrow::ArrowReaderMetadata.load(reader).fields
    projection = leaf_projection(reader, [0])

    group = fill_from(row_group_shell(reader), bytes, projection)
    group.num_rows.should eq(8)

    # Only the projected column was fetched; row counts come from the metadata.
    read_ids(group, fields, projection).should eq([4, 5, 6, 7, 2, 3, 0, 1])

    # The page reader yields a single column chunk's pages.
    page_reader = group.column_chunks(0)
    page_reader.next.should_not be_a(Iterator::Stop)
  end

  it "computes whole-chunk ranges without a selection" do
    reader = fixture_reader("alltypes_plain.parquet")
    group = row_group_shell(reader)
    projection = leaf_projection(reader, [0, 1])

    fetch = group.fetch_ranges(projection, nil, 1024, nil)
    fetch.page_start_offsets.should be_nil

    columns = reader.metadata.row_group(0)
    expected = [0, 1].map do |idx|
      start, length = columns.column(idx).byte_range
      start.to_u64...(start + length).to_u64
    end
    fetch.ranges.should eq(expected)
  end

  it "prunes pages for a selection with an offset index (sparse)" do
    bytes = ParquetTestHelpers.fixture_bytes("alltypes_tiny_pages.parquet")
    reader = fixture_reader("alltypes_tiny_pages.parquet", page_index: true)
    fields = PArrow::ArrowReaderMetadata.load(reader).fields
    projection = leaf_projection(reader, [0])

    selection = RowSelection.new([RowSelector.skip(400), RowSelector.select(50)])
    batch_size = 50

    group = row_group_shell(reader)
    fetch = group.fetch_ranges(projection, selection, batch_size, nil)

    chunk_start, chunk_length = reader.metadata.row_group(0).column(0).byte_range
    fetch.page_start_offsets.should_not be_nil
    offsets = fetch.page_start_offsets.not_nil!
    offsets.size.should eq(1)
    offsets[0].size.should eq(fetch.ranges.size)
    offsets[0].should eq(fetch.ranges.map(&.begin))
    fetch.ranges.size.should be > 0
    fetch.ranges.each do |range|
      range.begin.should be >= chunk_start.to_u64
      range.end.should be <= (chunk_start + chunk_length).to_u64
    end
    # Page pruning fetched far less than the whole 37 KiB chunk.
    fetch.ranges.sum(0_u64) { |range| range.end - range.begin }.should be < chunk_length.to_u64

    group = fill_from(row_group_shell(reader), bytes, projection, selection, batch_size)
    from_memory = read_ids(group, fields, projection, selection, batch_size)

    # Cross-check against decoding the same selection from the file.
    file_group = PArrow::FileRowGroups.new(reader)
    from_file = read_ids(file_group, fields, projection, selection, batch_size)
    from_memory.should eq(from_file)
    from_memory.size.should eq(50)
  end

  it "falls back to whole chunks without an offset index" do
    reader = fixture_reader("alltypes_plain.parquet")
    group = row_group_shell(reader)
    projection = leaf_projection(reader, [0])

    selection = RowSelection.new([RowSelector.skip(2), RowSelector.select(2)])
    fetch = group.fetch_ranges(projection, selection, 1024, nil)
    fetch.page_start_offsets.should be_nil

    start, length = reader.metadata.row_group(0).column(0).byte_range
    fetch.ranges.should eq([start.to_u64...(start + length).to_u64])
  end

  it "rejects reading a column that was not fetched" do
    reader = fixture_reader("alltypes_plain.parquet")
    bytes = ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")
    projection = leaf_projection(reader, [0])
    group = fill_from(row_group_shell(reader), bytes, projection)

    expect_raises(Parquet::GeneralError, /Invalid column index 1, column was not fetched/) do
      group.column_chunks(1)
    end
  end
end

describe Parquet::Arrow::ColumnChunkData do
  it "reads dense column chunks by offset" do
    data = Bytes[1, 2, 3, 4, 5, 6, 7, 8]
    chunk = ColumnChunkData::Dense.new(offset: 100, data: data)

    chunk.len.should eq(8)
    chunk.get(100).should eq(data)
    chunk.get(104).should eq(Bytes[5, 6, 7, 8])
    chunk.get_bytes(102, 3).should eq(Bytes[3, 4, 5])

    io = chunk.get_read(106)
    io.to_slice.should eq(Bytes[7, 8])
  end

  it "reads sparse column chunks page by page" do
    first = Bytes[10, 11, 12]
    second = Bytes[20, 21]
    chunk = ColumnChunkData::Sparse.new(
      length: 100,
      data: [{40_u64, first}, {80_u64, second}],
    )

    # The length is the full column chunk's length, not the fetched bytes.
    chunk.len.should eq(100)
    chunk.get(40).should eq(first)
    chunk.get(80).should eq(second)
    chunk.get_bytes(80, 1).should eq(Bytes[20])

    expect_raises(Parquet::GeneralError, /Invalid offset in sparse column chunk data: 50, no matching page found/) do
      chunk.get(50)
    end
  end
end
