require "../../spec_helper"

# Ported from `parquet/src/arrow/push_decoder/{mod,remaining,reader_builder/{mod,filter}}.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `ParquetPushDecoderBuilder`/`ParquetPushDecoder` pull-based decoding over
# caller-supplied bytes (`DecodeResult::NeedsData`/`Data`/`Finished`),
# including row-group selection, row selection, offset/limit budgeting and
# partial/cleared buffers.
#
# The upstream tests build their fixture with `ArrowWriter` (400 rows of
# `a: Int64 = 0..400`, `b: Int64 = 400..800`, `c: Utf8` alternating short/long
# strings, two 200-row row groups, 100-row data pages, written in 50-row
# batches). The port writes the same shape with `SerializedFileWriter`.
#
# Deferred in this slice (documented in `plans/parity.md`): the predicate cache
# (`with_max_predicate_cache_size`, `RowGroupCache`/`CachedArrayReader`), so the
# filter-stage requests are built exactly as upstream builds them with the cache
# disabled.
private alias PArrow = Parquet::Arrow
private alias BytesCR = Parquet::File::BytesChunkReader
private alias FWriter = Parquet::File::SerializedFileWriter
private alias WP = Parquet::File::Properties::WriterProperties
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PRep = Parquet::Repetition
private alias RowSelection = Parquet::Arrow::RowSelection
private alias RowSelector = Parquet::Arrow::RowSelector
private alias ParquetPushDecoder = Parquet::Arrow::ParquetPushDecoder
private alias ParquetPushDecoderBuilder = Parquet::Arrow::ParquetPushDecoderBuilder
private alias DecodeResult = Parquet::DecodeResult(Arrow::RecordBatch)

private ROWS           = 400
private ROW_GROUP_ROWS = 200

private def test_string(i : Int32) : String
  i % 2 == 0 ? "string_#{i}" : "A string larger than 12 bytes and thus not inlined #{i}"
end

# The upstream `TEST_FILE_DATA`: 400 rows in two 200-row row groups with
# 100-row data pages (written in 50-row batches so the page limit applies).
private def test_file_bytes : Bytes
  leaf_a = ST.primitive_type_builder("a", PType::INT64)
    .with_repetition(PRep::REQUIRED).build.as(ST)
  leaf_b = ST.primitive_type_builder("b", PType::INT64)
    .with_repetition(PRep::REQUIRED).build.as(ST)
  leaf_c = ST.primitive_type_builder("c", PType::BYTE_ARRAY)
    .with_repetition(PRep::REQUIRED)
    .with_converted_type(Parquet::ConvertedType::UTF8)
    .build.as(ST)
  schema = ST.group_type_builder("schema").with_fields([leaf_a, leaf_b, leaf_c]).build

  props = WP.builder
    .set_max_row_group_row_count(ROW_GROUP_ROWS)
    .set_data_page_row_count_limit(100)
    .build

  io = IO::Memory.new
  writer = FWriter.new(io, schema, props)
  # The writer requires each column to be written and closed before the next
  # one, so write columns in order, in 50-row batches (the page row count limit
  # is applied on batch boundaries, giving two 100-row pages per column).
  2.times do |row_group_idx|
    row_group = writer.next_row_group

    column_a = row_group.next_column.not_nil!
    (0...ROW_GROUP_ROWS).step(50) do |offset|
      start = row_group_idx * ROW_GROUP_ROWS + offset
      column_a.typed(Int64).write_batch(((start...(start + 50)).map(&.to_i64)), nil, nil)
    end
    column_a.close

    column_b = row_group.next_column.not_nil!
    (0...ROW_GROUP_ROWS).step(50) do |offset|
      start = row_group_idx * ROW_GROUP_ROWS + offset
      column_b.typed(Int64).write_batch(((start...(start + 50)).map { |i| (400 + i).to_i64 }), nil, nil)
    end
    column_b.close

    column_c = row_group.next_column.not_nil!
    (0...ROW_GROUP_ROWS).step(50) do |offset|
      start = row_group_idx * ROW_GROUP_ROWS + offset
      column_c.typed(Parquet::ByteArray).write_batch(
        (start...(start + 50)).map { |i| Parquet::ByteArray.new(test_string(i)) }, nil, nil
      )
    end
    column_c.close

    row_group.close
  end
  writer.close
  io.to_slice
end

private def test_metadata(bytes : Bytes) : Parquet::File::Metadata::ParquetMetaData
  Parquet::File::SerializedFileReader.new(BytesCR.new(bytes)).metadata
end

private def expect_data(result : DecodeResult) : Arrow::RecordBatch
  result.data?.should be_true
  result.data
end

private def expect_needs_data(result : DecodeResult) : Array(Range(UInt64, UInt64))
  result.needs_data?.should be_true
  result.ranges
end

private def expect_finished(result : DecodeResult) : Nil
  result.finished?.should be_true
end

# `try_next_reader` hands back a whole row group's `ParquetRecordBatchReader`
# rather than a single batch, so it needs its own unwrappers.
private alias ReaderResult = Parquet::DecodeResult(Parquet::Arrow::ParquetRecordBatchReader)

private def expect_reader(result : ReaderResult) : Parquet::Arrow::ParquetRecordBatchReader
  result.data?.should be_true
  result.data
end

private def expect_reader_needs_data(result : ReaderResult) : Array(Range(UInt64, UInt64))
  result.needs_data?.should be_true
  result.ranges
end

private def expect_reader_finished(result : ReaderResult) : Nil
  result.finished?.should be_true
end

# Drains a reader, returning every batch it yields.
private def drain_reader(reader : Parquet::Arrow::ParquetRecordBatchReader) : Array(Arrow::RecordBatch)
  batches = [] of Arrow::RecordBatch
  while batch = reader.next_batch
    batches << batch
  end
  batches
end

# Drains a reader and returns all values of its first column.
private def drain_first_column(reader : Parquet::Arrow::ParquetRecordBatchReader) : Array(Int64)
  drain_reader(reader).flat_map { |batch| int64_values(batch, 0) }
end

private def push_ranges_to_decoder(decoder : ParquetPushDecoder, bytes : Bytes,
                                   ranges : Array(Range(UInt64, UInt64))) : Nil
  data = ranges.map { |range| bytes[range.begin.to_i32, (range.end - range.begin).to_i32] }
  decoder.push_ranges(ranges, data)
end

# The byte range of one column chunk (upstream tests hardcode these; the port
# derives them from metadata because its fixture writes BYTE_ARRAY instead of
# StringView, so the layout differs from upstream).
private def column_range(
  metadata : Parquet::File::Metadata::ParquetMetaData,
  row_group_idx : Int32,
  column_idx : Int32,
) : Range(UInt64, UInt64)
  start, length = metadata.row_group(row_group_idx).column(column_idx).byte_range
  start.to_u64...(start + length).to_u64
end

# The column byte ranges of a row group, in column order.
private def column_ranges(
  metadata : Parquet::File::Metadata::ParquetMetaData,
  row_group_idx : Int32,
) : Array(Range(UInt64, UInt64))
  row_group = metadata.row_group(row_group_idx)
  (0...row_group.columns.size).map { |idx| column_range(metadata, row_group_idx, idx) }
end

# Builds a decoder with `batch_size` and pushes the whole fixture up front
# (upstream `prefetched_decoder`).
private def prefetched_decoder(batch_size : Int32, bytes : Bytes) : ParquetPushDecoder
  decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
    .with_batch_size(batch_size)
    .build
  prefetch_test_file(decoder, bytes)
  decoder
end

private def int64_values(batch : Arrow::RecordBatch, column : Int32) : Array(Int64)
  array = batch.column(column).as(Arrow::PrimitiveArray(Int64))
  (0...array.len).map { |i| array.value(i).as(Int64) }
end

private def string_values(batch : Arrow::RecordBatch, column : Int32) : Array(String)
  array = batch.column(column).as(Arrow::GenericStringArray(Int32))
  (0...array.len).map { |i| array.value(i) }
end

# Checks that a batch matches rows `[start, start + count)` of the fixture.
private def check_rows(batch : Arrow::RecordBatch, start : Int32, count : Int32) : Nil
  batch.num_rows.should eq(count)
  expected_rows = (start...(start + count)).to_a
  int64_values(batch, 0).should eq(expected_rows.map(&.to_i64))
  int64_values(batch, 1).should eq(expected_rows.map { |i| (400 + i).to_i64 })
  string_values(batch, 2).should eq(expected_rows.map { |i| test_string(i) })
end

describe Parquet::Arrow::ParquetPushDecoder do
  it "decodes the whole file when all data is pushed (upstream test_decoder_all_data)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes)).build
    decoder.push_range(0_u64...bytes.size.to_u64, bytes)

    # One batch per row group, without needing more data.
    first = expect_data(decoder.try_decode)
    check_rows(first, 0, ROW_GROUP_ROWS)
    second = expect_data(decoder.try_decode)
    check_rows(second, ROW_GROUP_ROWS, ROW_GROUP_ROWS)
    expect_finished(decoder.try_decode)
  end

  it "decodes incrementally, fetching one row group at a time (upstream test_decoder_incremental)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes)).build
    results = [] of Arrow::RecordBatch

    2.times do |row_group|
      ranges = expect_needs_data(decoder.try_decode)
      requested = ranges.sum(0_u64) { |range| range.end - range.begin }
      push_ranges_to_decoder(decoder, bytes, ranges)

      # Only the bytes needed for this row group are staged.
      decoder.buffered_bytes.should eq(requested)
      batch = expect_data(decoder.try_decode)
      check_rows(batch, row_group * ROW_GROUP_ROWS, ROW_GROUP_ROWS)
      results << batch
      # The row group's data was consumed and freed.
      decoder.buffered_bytes.should eq(0)
    end

    expect_finished(decoder.try_decode)
    results.size.should eq(2)
  end

  it "keeps reading the active row group after clearing all ranges (upstream test_decoder_clear_all_ranges)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
      .with_batch_size(100)
      .build
    decoder.push_range(0_u64...bytes.size.to_u64, bytes)
    decoder.buffered_bytes.should eq(bytes.size.to_u64)

    # The first row group reads from the prefetched bytes, but the speculative
    # full-file range stays staged.
    first = expect_data(decoder.try_decode)
    check_rows(first, 0, 100)
    decoder.buffered_bytes.should eq(bytes.size.to_u64)

    decoder.clear_all_ranges
    decoder.buffered_bytes.should eq(0)

    # The active reader still owns the row group's bytes.
    second = expect_data(decoder.try_decode)
    check_rows(second, 100, 100)
    decoder.buffered_bytes.should eq(0)

    # The next row group needs its data again.
    ranges = expect_needs_data(decoder.try_decode)
    requested = ranges.sum(0_u64) { |range| range.end - range.begin }
    push_ranges_to_decoder(decoder, bytes, ranges)
    decoder.buffered_bytes.should eq(requested)

    third = expect_data(decoder.try_decode)
    check_rows(third, 200, 100)
    decoder.buffered_bytes.should eq(0)
    fourth = expect_data(decoder.try_decode)
    check_rows(fourth, 300, 100)
    decoder.buffered_bytes.should eq(0)

    expect_finished(decoder.try_decode)
  end

  it "asks for the missing ranges when only part of a row group is pushed (upstream test_decoder_partial)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes)).build

    ranges = expect_needs_data(decoder.try_decode)
    push_ranges_to_decoder(decoder, bytes, ranges)
    check_rows(expect_data(decoder.try_decode), 0, ROW_GROUP_ROWS)

    ranges = expect_needs_data(decoder.try_decode)
    ranges.size.should be > 1
    split = ranges.size // 2
    first_half = ranges[0, split]
    second_half = ranges[split, ranges.size - split]
    push_ranges_to_decoder(decoder, bytes, first_half)

    # Still missing the second half; an empty push is a no-op.
    expect_needs_data(decoder.try_decode).should eq(second_half)
    push_ranges_to_decoder(decoder, bytes, [] of Range(UInt64, UInt64))
    expect_needs_data(decoder.try_decode).should eq(second_half)
    push_ranges_to_decoder(decoder, bytes, second_half)

    check_rows(expect_data(decoder.try_decode), ROW_GROUP_ROWS, ROW_GROUP_ROWS)
    expect_finished(decoder.try_decode)
  end

  it "reads a projection in a single request per row group (upstream test_decoder_selection_does_one_request)" do
    bytes = test_file_bytes
    metadata = test_metadata(bytes)
    schema_descr = metadata.file_metadata.schema_descr

    decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
      .with_projection(PArrow::ProjectionMask.columns(schema_descr, ["a", "b"]))
      .build

    2.times do |row_group|
      ranges = expect_needs_data(decoder.try_decode)
      push_ranges_to_decoder(decoder, bytes, ranges)

      batch = expect_data(decoder.try_decode)
      batch.num_columns.should eq(2)
      start = row_group * ROW_GROUP_ROWS
      int64_values(batch, 0).should eq((start...(start + ROW_GROUP_ROWS)).map(&.to_i64))
      int64_values(batch, 1).should eq((start...(start + ROW_GROUP_ROWS)).map { |i| (400 + i).to_i64 })
    end

    expect_finished(decoder.try_decode)
  end

  it "decodes only the selected row group (upstream test_decoder_row_group_selection)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
      .with_row_groups([1])
      .build

    ranges = expect_needs_data(decoder.try_decode)
    push_ranges_to_decoder(decoder, bytes, ranges)

    check_rows(expect_data(decoder.try_decode), ROW_GROUP_ROWS, ROW_GROUP_ROWS)
    expect_finished(decoder.try_decode)
  end

  it "skips rows across row groups (upstream test_decoder_row_selection)" do
    bytes = test_file_bytes
    selection = RowSelection.new([
      RowSelector.skip(225),
      RowSelector.select(20),
    ])
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
      .with_row_selection(selection)
      .build

    ranges = expect_needs_data(decoder.try_decode)
    push_ranges_to_decoder(decoder, bytes, ranges)

    check_rows(expect_data(decoder.try_decode), 225, 20)
    expect_finished(decoder.try_decode)
  end

  it "applies offset and limit across row groups (upstream test_decoder_offset_limit)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
      # Skip the whole first row group and 25 rows of the second, then take 20.
      .with_offset(225)
      .with_limit(20)
      .build

    ranges = expect_needs_data(decoder.try_decode)
    push_ranges_to_decoder(decoder, bytes, ranges)

    check_rows(expect_data(decoder.try_decode), 225, 20)
    expect_finished(decoder.try_decode)
  end

  it "reports row-group boundaries and remaining row groups" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes)).build

    # Between row groups (nothing handed to the reader yet).
    decoder.is_at_row_group_boundary.should be_true
    decoder.row_groups_remaining.should eq(2)

    # After a row group is handed out a data request is in flight.
    ranges = expect_needs_data(decoder.try_decode)
    decoder.is_at_row_group_boundary.should be_false
    decoder.row_groups_remaining.should eq(1)

    push_ranges_to_decoder(decoder, bytes, ranges)
    expect_data(decoder.try_decode)
    # The active reader keeps the decoder mid-row-group.
    decoder.is_at_row_group_boundary.should be_false
    decoder.row_groups_remaining.should eq(1)

    ranges = expect_needs_data(decoder.try_decode)
    decoder.row_groups_remaining.should eq(0)
    push_ranges_to_decoder(decoder, bytes, ranges)
    expect_data(decoder.try_decode)
    decoder.row_groups_remaining.should eq(0)
    expect_finished(decoder.try_decode)
  end
end

private alias RowGroupSelection = Parquet::Arrow::RowGroupSelection

private def prefetch_test_file(decoder : ParquetPushDecoder, bytes : Bytes) : Nil
  decoder.push_range(0_u64...bytes.size.to_u64, bytes)
end

describe "ParquetPushDecoder row-group-local selections" do
  it "uses local coordinates and preserves the supplied order (upstream test_decoder_row_group_local_selections)" do
    bytes = test_file_bytes
    metadata = test_metadata(bytes)

    # A bitmap selection covering local rows 25..45 of row group 1 (the bitmap
    # is shorter than the row group, so later rows are skipped).
    bitmap = Arrow::BooleanBuffer.from_slice(
      Array(Bool).new(45) { |row| row >= 25 }
    )
    rle = RowSelection.new([RowSelector.skip(190), RowSelector.select(10)])

    decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
      .with_row_group_selections([
        RowGroupSelection.new(1, RowSelection.from_boolean_buffer(bitmap)),
        RowGroupSelection.new(0, rle),
      ])
      .build
    prefetch_test_file(decoder, bytes)

    check_rows(expect_data(decoder.try_decode), 225, 20)
    check_rows(expect_data(decoder.try_decode), 190, 10)
    expect_finished(decoder.try_decode)
  end

  it "applies offset and limit across local selections (upstream test_row_group_local_selections_respect_offset_and_limit)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
      .with_row_group_selections([
        RowGroupSelection.new(1),
        RowGroupSelection.new(0),
      ])
      .with_offset(195)
      .with_limit(10)
      .build
    prefetch_test_file(decoder, bytes)

    check_rows(expect_data(decoder.try_decode), 395, 5)
    check_rows(expect_data(decoder.try_decode), 0, 5)
    expect_finished(decoder.try_decode)
  end

  it "allows duplicate row groups (upstream test_row_group_local_selections_allow_duplicate_row_groups)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
      .with_row_group_selections([
        RowGroupSelection.new(0),
        RowGroupSelection.new(0),
      ])
      .build
    prefetch_test_file(decoder, bytes)

    check_rows(expect_data(decoder.try_decode), 0, 200)
    check_rows(expect_data(decoder.try_decode), 0, 200)
    expect_finished(decoder.try_decode)
  end

  it "skips trailing rows of a short local selection (upstream test_short_row_group_local_selection_skips_trailing_rows)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
      .with_row_group_selections([
        RowGroupSelection.new(1, RowSelection.new([
          RowSelector.skip(5),
          RowSelector.select(3),
        ])),
      ])
      .build
    prefetch_test_file(decoder, bytes)

    check_rows(expect_data(decoder.try_decode), 205, 3)
    expect_finished(decoder.try_decode)
  end

  it "reads nothing for an empty local selection list (upstream test_empty_row_group_local_selections_read_nothing)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
      .with_row_group_selections([] of RowGroupSelection)
      .build

    expect_finished(decoder.try_decode)
  end

  it "replaces earlier local configuration (upstream test_row_group_local_selection_replaces_previous_configuration)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
      .with_row_group_selections([RowGroupSelection.new(0)])
      .with_row_group_selections([RowGroupSelection.new(1)])
      .build
    prefetch_test_file(decoder, bytes)

    check_rows(expect_data(decoder.try_decode), 200, 200)
    expect_finished(decoder.try_decode)
  end

  it "keeps legacy row groups and selections composable (upstream test_legacy_row_groups_and_row_selection_remain_composable)" do
    bytes = test_file_bytes
    decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
      .with_row_groups([1])
      .with_row_selection(RowSelection.new([RowSelector.skip(25), RowSelector.select(20)]))
      .build
    prefetch_test_file(decoder, bytes)

    check_rows(expect_data(decoder.try_decode), 225, 20)
    expect_finished(decoder.try_decode)
  end

  it "is mutually exclusive with the legacy configuration (upstream test_row_group_local_selection_is_mutually_exclusive_with_legacy_configuration)" do
    bytes = test_file_bytes
    metadata = test_metadata(bytes)
    message = "with_row_group_selections cannot be combined with with_row_groups or with_row_selection"

    conflict = ->(builder : ParquetPushDecoderBuilder) do
      expect_raises(Parquet::GeneralError, message) { builder.build }
    end

    conflict.call(
      ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_row_groups([0])
        .with_row_group_selections([RowGroupSelection.new(0)])
    )
    conflict.call(
      ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_row_group_selections([RowGroupSelection.new(0)])
        .with_row_groups([0])
    )
    conflict.call(
      ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_row_selection(RowSelection.new([RowSelector.select(1)]))
        .with_row_group_selections([RowGroupSelection.new(0)])
    )
    conflict.call(
      ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_row_group_selections([RowGroupSelection.new(0)])
        .with_row_selection(RowSelection.new([RowSelector.select(1)]))
    )
  end

  it "validates the row group and selection length at build (upstream test_row_group_local_selection_validates_row_group_and_length_at_build)" do
    bytes = test_file_bytes
    metadata = test_metadata(bytes)

    expect_raises(Parquet::GeneralError, /Row selection for row group 0 contains 201 rows, but the row group has 200/) do
      ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_row_group_selections([
          RowGroupSelection.new(0, RowSelection.new([RowSelector.select(201)])),
        ])
        .build
    end

    expect_raises(Parquet::GeneralError, /Row group index 2 out of bounds for file with 2 row groups/) do
      ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_row_group_selections([RowGroupSelection.new(2)])
        .build
    end
  end
end

# Adaptive scans: `peek_next_row_group`, `try_next_reader` and `into_builder`.
#
# Ported from `parquet/src/arrow/push_decoder/mod.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Deferred (documented in `plans/parity.md`): the row-filter variants
# (`test_into_builder_installs_filter_between_row_groups`,
# `test_into_builder_preserves_local_selections_with_row_filter`), which need
# `with_row_filter` and the predicate cache in the push path.
#
# Divergence: upstream asserts hardcoded byte ranges. The port's fixture writes
# column "c" as BYTE_ARRAY instead of StringView, so page and chunk sizes
# differ; the specs assert the exact chunk ranges derived from the file
# metadata instead, which pins the same behavior (one range per projected
# column chunk).
describe Parquet::Arrow::ParquetPushDecoder do
  describe "adaptive scans" do
    it "peeks the row group the next reader will produce (upstream test_peek_next_row_group_basic)" do
      bytes = test_file_bytes
      decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes)).build

      # Two row groups (0, 1). At the boundary before any read, peek sees RG 0.
      decoder.peek_next_row_group.should eq(0)
      decoder.is_at_row_group_boundary.should be_true

      ranges = expect_reader_needs_data(decoder.try_next_reader)
      push_ranges_to_decoder(decoder, bytes, ranges)
      reader = expect_reader(decoder.try_next_reader)
      # Once RG 0's reader is handed off, the decoder is back at a boundary and
      # peek must reflect that RG 1 comes next.
      decoder.is_at_row_group_boundary.should be_true
      decoder.peek_next_row_group.should eq(1)

      drain_reader(reader)

      ranges = expect_reader_needs_data(decoder.try_next_reader)
      push_ranges_to_decoder(decoder, bytes, ranges)
      drain_reader(expect_reader(decoder.try_next_reader))

      decoder.peek_next_row_group.should be_nil
    end

    it "restricts peeks to with_row_groups (upstream test_peek_next_row_group_respects_with_row_groups)" do
      bytes = test_file_bytes
      decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
        .with_row_groups([1])
        .build

      decoder.peek_next_row_group.should eq(1)
    end

    it "skips a row group left with no selected rows (upstream test_peek_next_row_group_skips_empty_selection)" do
      bytes = test_file_bytes
      # Each row group has 200 rows: skip all of RG 0 plus 50 of RG 1.
      decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
        .with_row_selection(RowSelection.new([
          RowSelector.skip(250),
          RowSelector.select(100),
        ]))
        .build

      decoder.peek_next_row_group.should eq(1)
    end

    it "returns nil on a finished decoder (upstream test_peek_next_row_group_finished)" do
      bytes = test_file_bytes
      decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
        .with_row_groups([] of Int32)
        .build

      expect_reader_finished(decoder.try_next_reader)
      decoder.peek_next_row_group.should be_nil
    end

    it "skips row groups consumed by the offset budget (upstream test_peek_next_row_group_skips_for_budget)" do
      bytes = test_file_bytes
      # OFFSET 200 drains RG 0 entirely, so the first reader is for RG 1.
      decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
        .with_offset(200)
        .build

      decoder.peek_next_row_group.should eq(1)
    end

    it "returns nil when the offset drains every row group (upstream test_peek_next_row_group_budget_drains_all)" do
      bytes = test_file_bytes
      # 2 row groups x 200 rows = 400 total; OFFSET 500 lands nowhere.
      decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
        .with_offset(500)
        .build

      decoder.peek_next_row_group.should be_nil
    end

    it "peeks past the active row group while decoding (upstream test_peek_next_row_group_during_decoding_row_group)" do
      bytes = test_file_bytes
      # Small batches keep the decoder in `DecodingRowGroup` across calls.
      decoder = prefetched_decoder(100, bytes)

      # First batch of RG 0: the reader is retained inside the decoder.
      check_rows(expect_data(decoder.try_decode), 0, 100)
      decoder.is_at_row_group_boundary.should be_false
      decoder.peek_next_row_group.should eq(1)

      # Rest of RG 0; peek still reports RG 1.
      check_rows(expect_data(decoder.try_decode), 100, 100)
      decoder.peek_next_row_group.should eq(1)

      # Into RG 1: nothing further to peek.
      check_rows(expect_data(decoder.try_decode), 200, 100)
      decoder.is_at_row_group_boundary.should be_false
      decoder.peek_next_row_group.should be_nil

      check_rows(expect_data(decoder.try_decode), 300, 100)
      expect_finished(decoder.try_decode)
    end

    it "does not mutate state when peeking (upstream test_peek_next_row_group_does_not_mutate_state)" do
      bytes = test_file_bytes
      decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
        .with_batch_size(1024)
        .build

      {0, 1}.each do |expected_rg|
        decoder.is_at_row_group_boundary.should be_true

        first_peek = decoder.peek_next_row_group
        second_peek = decoder.peek_next_row_group
        third_peek = decoder.peek_next_row_group
        first_peek.should eq(expected_rg)
        second_peek.should eq(first_peek)
        third_peek.should eq(second_peek)

        # The read path must agree with what peek promised.
        ranges = expect_reader_needs_data(decoder.try_next_reader)
        push_ranges_to_decoder(decoder, bytes, ranges)
        drain_reader(expect_reader(decoder.try_next_reader))
      end

      decoder.peek_next_row_group.should be_nil
      expect_reader_finished(decoder.try_next_reader)
    end

    it "is rejected mid-row-group (upstream test_into_builder_rejected_mid_row_group)" do
      bytes = test_file_bytes
      decoder = prefetched_decoder(50, bytes)

      # Decode one batch to land inside `DecodingRowGroup`.
      expect_data(decoder.try_decode)
      decoder.is_at_row_group_boundary.should be_false

      expect_raises(Parquet::GeneralError, /is_at_row_group_boundary/) do
        decoder.into_builder
      end
    end

    it "is rejected on a finished decoder (upstream test_into_builder_rejected_on_finished_decoder)" do
      bytes = test_file_bytes
      decoder = prefetched_decoder(1024, bytes)
      expect_data(decoder.try_decode)
      expect_data(decoder.try_decode)
      expect_finished(decoder.try_decode)
      decoder.is_at_row_group_boundary.should be_false

      expect_raises(Parquet::GeneralError, /finished/) do
        decoder.into_builder
      end
    end

    it "is allowed while iterating a handed-off reader (upstream test_into_builder_allowed_while_iterating_handed_off_reader)" do
      bytes = test_file_bytes
      decoder = prefetched_decoder(1024, bytes)

      reader0 = expect_reader(decoder.try_next_reader)
      # The decoder no longer owns the reader, so it is between row groups.
      decoder.is_at_row_group_boundary.should be_true
      # Recovering the builder consumes the decoder but leaves `reader0` valid.
      decoder.into_builder
      drain_first_column(reader0).should eq((0...200).map(&.to_i64))
    end

    it "resumes the remaining budget (upstream test_into_builder_resumes_remaining_budget)" do
      bytes = test_file_bytes
      # limit = 250 spans both row groups: all 200 rows of RG 0 plus 50 of RG 1.
      decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
        .with_batch_size(1024)
        .with_limit(250)
        .build
      prefetch_test_file(decoder, bytes)

      reader0 = expect_reader(decoder.try_next_reader)
      drain_first_column(reader0).should eq((0...200).map(&.to_i64))

      # Rebuilding without changes carries the remaining 50-row limit and RG 1.
      decoder.is_at_row_group_boundary.should be_true
      decoder = decoder.into_builder.build

      reader1 = expect_reader(decoder.try_next_reader)
      drain_first_column(reader1).should eq((200...250).map(&.to_i64))
      expect_reader_finished(decoder.try_next_reader)
    end

    it "preserves buffered bytes (upstream test_into_builder_preserves_buffered_bytes)" do
      bytes = test_file_bytes
      decoder = prefetched_decoder(1024, bytes)
      decoder.buffered_bytes.should eq(bytes.size.to_u64)

      # Drain RG 0; RG 1's bytes are still staged inside the decoder.
      drain_reader(expect_reader(decoder.try_next_reader))
      buffered = decoder.buffered_bytes
      buffered.should be > 0

      decoder = decoder.into_builder.build
      decoder.buffered_bytes.should eq(buffered)

      # RG 1's bytes are already buffered, so no `NeedsData` round-trip.
      reader1 = expect_reader(decoder.try_next_reader)
      drain_first_column(reader1).should eq((200...400).map(&.to_i64))
      expect_reader_finished(decoder.try_next_reader)
    end

    it "preserves remaining row-group-local selections (upstream test_into_builder_preserves_remaining_row_group_local_selections)" do
      bytes = test_file_bytes
      bitmap = Arrow::BooleanBuffer.from_slice(Array(Bool).new(45) { |row| row >= 25 })
      decoder = ParquetPushDecoderBuilder.try_new_decoder(test_metadata(bytes))
        .with_row_group_selections([
          RowGroupSelection.new(0, RowSelection.new([RowSelector.skip(190), RowSelector.select(10)])),
          RowGroupSelection.new(1, RowSelection.from_boolean_buffer(bitmap)),
        ])
        .build
      prefetch_test_file(decoder, bytes)

      reader0 = expect_reader(decoder.try_next_reader)
      drain_first_column(reader0).should eq((190...200).map(&.to_i64))

      # Only RG 1 and its still-local bitmap selection carry over.
      decoder.is_at_row_group_boundary.should be_true
      decoder.row_groups_remaining.should eq(1)
      decoder = decoder.into_builder.build

      reader1 = expect_reader(decoder.try_next_reader)
      drain_first_column(reader1).should eq((225...245).map(&.to_i64))
      expect_reader_finished(decoder.try_next_reader)
    end

    it "requests new bytes after a widened projection (upstream test_into_builder_expand_projection_requests_new_bytes)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_batch_size(1024)
        .with_projection(PArrow::ProjectionMask.leaves(metadata.file_metadata.schema_descr, [0]))
        .build

      # RG 0 with column "a" only: a single contiguous range.
      ranges_rg0 = expect_reader_needs_data(decoder.try_next_reader)
      ranges_rg0.should eq([column_range(metadata, 0, 0)])
      push_ranges_to_decoder(decoder, bytes, ranges_rg0)

      reader0 = expect_reader(decoder.try_next_reader)
      batches0 = drain_reader(reader0)
      batches0.each { |batch| batch.num_columns.should eq(1) }
      batches0.flat_map { |batch| int64_values(batch, 0) }.should eq((0...200).map(&.to_i64))

      # Widen the projection at the boundary: RG 1 must request all three
      # column chunks instead of just "a".
      decoder.is_at_row_group_boundary.should be_true
      decoder = decoder.into_builder
        .with_projection(PArrow::ProjectionMask.all)
        .build

      ranges_rg1 = expect_reader_needs_data(decoder.try_next_reader)
      ranges_rg1.should eq(column_ranges(metadata, 1))
      ranges_rg1.size.should eq(3)
      push_ranges_to_decoder(decoder, bytes, ranges_rg1)

      reader1 = expect_reader(decoder.try_next_reader)
      batches1 = drain_reader(reader1)
      batches1.each { |batch| batch.num_columns.should eq(3) }
      batches1.flat_map { |batch| int64_values(batch, 0) }.should eq((200...400).map(&.to_i64))
      expect_reader_finished(decoder.try_next_reader)
    end

    it "requests fewer bytes after a narrowed projection (upstream test_into_builder_narrow_projection_requests_fewer_bytes)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_batch_size(1024)
        .build

      # RG 0 with the full projection: three column ranges.
      ranges_rg0 = expect_reader_needs_data(decoder.try_next_reader)
      ranges_rg0.should eq(column_ranges(metadata, 0))
      push_ranges_to_decoder(decoder, bytes, ranges_rg0)

      reader0 = expect_reader(decoder.try_next_reader)
      batches0 = drain_reader(reader0)
      batches0.flat_map { |batch| int64_values(batch, 0) }.should eq((0...200).map(&.to_i64))

      # Narrow to column "a" at the boundary.
      decoder.is_at_row_group_boundary.should be_true
      decoder = decoder.into_builder
        .with_projection(PArrow::ProjectionMask.leaves(metadata.file_metadata.schema_descr, [0]))
        .build

      ranges_rg1 = expect_reader_needs_data(decoder.try_next_reader)
      ranges_rg1.should eq([column_range(metadata, 1, 0)])
      push_ranges_to_decoder(decoder, bytes, ranges_rg1)

      reader1 = expect_reader(decoder.try_next_reader)
      batches1 = drain_reader(reader1)
      batches1.each { |batch| batch.num_columns.should eq(1) }
      batches1.flat_map { |batch| int64_values(batch, 0) }.should eq((200...400).map(&.to_i64))
      expect_reader_finished(decoder.try_next_reader)
    end
  end
end

# Row filters in the push path (`with_row_filter`, `FilterInfo`/`CacheInfo`,
# `has_predicates`).
#
# Ported from `parquet/src/arrow/push_decoder/{mod,reader_builder/filter}.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Deferred (documented in `plans/parity.md`): the predicate cache
# (`RowGroupCache`/`CachedArrayReader`/`CacheOptions`), so
# `with_max_predicate_cache_size` is accepted but has no effect and the
# cache-projection expansion of the filter-stage requests (`cache_mask`) is a
# no-op — equivalent to upstream with the cache disabled.
private def threshold_predicate(
  mask : Parquet::Arrow::ProjectionMask,
  column : Int32,
  threshold : Int64,
) : Parquet::Arrow::ArrowPredicate
  Parquet::Arrow::ArrowPredicateFn.new(mask) do |batch|
    values = batch.column(column).as(Arrow::PrimitiveArray(Int64))
    Arrow.gt(values, Arrow::PrimitiveArray(Int64).new_scalar(threshold))
  end
end

private def below_predicate(
  mask : Parquet::Arrow::ProjectionMask,
  column : Int32,
  threshold : Int64,
) : Parquet::Arrow::ArrowPredicate
  Parquet::Arrow::ArrowPredicateFn.new(mask) do |batch|
    values = batch.column(column).as(Arrow::PrimitiveArray(Int64))
    Arrow.lt(values, Arrow::PrimitiveArray(Int64).new_scalar(threshold))
  end
end

# Checks the rows of a batch projected to a single column.
private def check_column_a(batch : Arrow::RecordBatch, start : Int32, count : Int32) : Nil
  batch.num_rows.should eq(count)
  int64_values(batch, 0).should eq((start...(start + count)).map(&.to_i64))
end

private def check_column_b(batch : Arrow::RecordBatch, start : Int32, count : Int32) : Nil
  batch.num_rows.should eq(count)
  int64_values(batch, 0).should eq((start...(start + count)).map { |i| (400 + i).to_i64 })
end

private def check_column_c(batch : Arrow::RecordBatch, start : Int32, count : Int32) : Nil
  batch.num_rows.should eq(count)
  string_values(batch, 0).should eq((start...(start + count)).map { |i| test_string(i) })
end

describe Parquet::Arrow::ParquetPushDecoder do
  describe "row filters" do
    it "evaluates a single filter across partial data (upstream test_decoder_single_filter_partial)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      # Claim to use both "a" and "b" so the filter pages come back as two
      # range requests.
      predicate = threshold_predicate(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0, 1]), 0, 250_i64)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        # Read only "a": the filter pages must be reused for the output.
        .with_projection(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]))
        .with_row_filter(Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate))
        .build

      # First row group, evaluating the filter.
      ranges = expect_needs_data(decoder.try_decode)
      half = ranges.size // 2
      ranges1 = ranges[0...half]
      ranges2 = ranges[half..]
      ranges1.should_not be_empty
      ranges2.should_not be_empty
      push_ranges_to_decoder(decoder, bytes, ranges1)

      # Still expect the remaining ranges, repeatedly without mutating state.
      expect_needs_data(decoder.try_decode).should eq(ranges2)
      expect_needs_data(decoder.try_decode).should eq(ranges2)
      push_ranges_to_decoder(decoder, bytes, ranges2)

      # No rows in row group 0 pass, so no data request follows the filter.

      # Second row group: filter pages, then the projected data.
      ranges = expect_needs_data(decoder.try_decode)
      push_ranges_to_decoder(decoder, bytes, ranges)

      check_column_a(expect_data(decoder.try_decode), 251, 149)
      expect_finished(decoder.try_decode)
    end

    it "combines a filter with a row selection (upstream test_decoder_single_filter_and_row_selection)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      predicate = threshold_predicate(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]), 0, 250_i64)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_projection(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [1]))
        .with_row_filter(Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate))
        .with_row_selection(RowSelection.new([
          RowSelector.skip(200),   # skip the first row group entirely
          RowSelector.select(100), # first 100 rows of the second row group
          RowSelector.skip(100),
        ]))
        .build

      # Row group 0 is filtered out by the row selection, with no request.
      ranges = expect_needs_data(decoder.try_decode)
      push_ranges_to_decoder(decoder, bytes, ranges)

      ranges = expect_needs_data(decoder.try_decode)
      push_ranges_to_decoder(decoder, bytes, ranges)

      check_column_b(expect_data(decoder.try_decode), 251, 49)
      expect_finished(decoder.try_decode)
    end

    it "evaluates multiple filters in order (upstream test_decoder_multi_filters)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      filter_a = threshold_predicate(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]), 0, 175_i64)
      filter_b = below_predicate(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [1]), 0, 625_i64)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_projection(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [2]))
        .with_row_filter(Parquet::Arrow::RowFilter.new([
          filter_a, filter_b,
        ] of Parquet::Arrow::ArrowPredicate))
        .build

      # Row group 0: filter "a", filter "b", then the projected data for "c".
      3.times do
        push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))
      end
      check_column_c(expect_data(decoder.try_decode), 176, 24)

      # Row group 1: same three stages.
      3.times do
        push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))
      end
      check_column_c(expect_data(decoder.try_decode), 200, 25)

      expect_finished(decoder.try_decode)
    end

    it "reuses filter pages for the output (upstream test_decoder_reuses_filter_pages)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      predicate = threshold_predicate(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]), 0, 250_i64)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_projection(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]))
        .with_row_filter(Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate))
        .build

      # Row group 0: the filter rules every row out.
      push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))

      # Row group 1: filter pages, then the output reusing them (no new request).
      push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))
      check_column_a(expect_data(decoder.try_decode), 251, 149)

      expect_finished(decoder.try_decode)
    end

    it "ignores an empty filter (upstream test_decoder_empty_filters)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_projection(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [2]))
        # Empty filters should be ignored.
        .with_row_filter(Parquet::Arrow::RowFilter.new([] of Parquet::Arrow::ArrowPredicate))
        .build

      # Expect the first two batches to be decoded as rows 0..199 and 200..399
      # of column "c", with no predicate stage in between.
      push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))
      check_column_c(expect_data(decoder.try_decode), 0, 200)

      push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))
      check_column_c(expect_data(decoder.try_decode), 200, 200)

      expect_finished(decoder.try_decode)
    end

    it "short-circuits the filter once the limit is reached (upstream test_decoder_filter_with_limit_short_circuits_within_row_group)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      evaluated = 0
      predicate = Parquet::Arrow::ArrowPredicateFn.new(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0])) do |batch|
        evaluated += batch.num_rows
        values = batch.column(0).as(Arrow::PrimitiveArray(Int64))
        Arrow.gt(values, Arrow::PrimitiveArray(Int64).new_scalar(175_i64))
      end

      # Use a small batch size so the row group is evaluated across multiple
      # predicate batches; that is the regime where the limit short-circuit
      # saves predicate evaluation work. Matching rows are 176..199 (24 rows);
      # with batch_size = 10 those span batches 17, 18 and 19 (rows 170..199).
      # A limit of 10 should stop filter evaluation in the middle of batch 18.
      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_projection(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]))
        .with_row_filter(Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate))
        .with_batch_size(10)
        .with_limit(10)
        .build

      push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))
      check_column_a(expect_data(decoder.try_decode), 176, 10)

      # No data for row group 1 is requested: the limit was satisfied by row
      # group 0 and its `Start` state short-circuits to `Finished`.
      expect_finished(decoder.try_decode)

      # Row 186 is the 11th match; the scan should stop no later than the batch
      # containing it (rows 180..189), so at most 190 rows are evaluated.
      evaluated.should be <= 190
    end

    it "skips later row groups once the limit is reached (upstream test_decoder_filter_with_limit_skips_later_row_groups)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      # `a > 175` matches rows 176..199 in row group 0 (24 matches) and
      # 200..399 in row group 1 (200 matches). With limit = 5, all matches must
      # come from row group 0.
      predicate = threshold_predicate(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]), 0, 175_i64)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_projection(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]))
        .with_row_filter(Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate))
        .with_limit(5)
        .build

      push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))
      check_column_a(expect_data(decoder.try_decode), 176, 5)

      # Row group 1 must not request data: the limit is already satisfied, so
      # its `Start` state short-circuits to `Finished`.
      expect_finished(decoder.try_decode)
    end

    it "counts the offset towards the predicate limit (upstream test_decoder_filter_with_offset_and_limit)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      # The predicate short-circuit must account for the offset as well as the
      # limit: the post-predicate offset skips already-selected rows before the
      # limit counts output rows, so the predicate must retain
      # `offset + limit` matches. `a > 175` matches rows 176..199 (24 matches);
      # with offset = 10 and limit = 5 the output is rows 186..190 (the 11th
      # through 15th matches).
      predicate = threshold_predicate(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]), 0, 175_i64)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_projection(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]))
        .with_row_filter(Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate))
        .with_offset(10)
        .with_limit(5)
        .build

      push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))
      check_column_a(expect_data(decoder.try_decode), 186, 5)

      expect_finished(decoder.try_decode)
    end

    it "limits the last predicate of a chain (upstream test_decoder_multi_filters_with_limit)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      first_rows = 0
      second_rows = 0
      filter_a = Parquet::Arrow::ArrowPredicateFn.new(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0])) do |batch|
        first_rows += batch.num_rows
        values = batch.column(0).as(Arrow::PrimitiveArray(Int64))
        Arrow.gt(values, Arrow::PrimitiveArray(Int64).new_scalar(175_i64))
      end
      filter_b = Parquet::Arrow::ArrowPredicateFn.new(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [1])) do |batch|
        second_rows += batch.num_rows
        values = batch.column(0).as(Arrow::PrimitiveArray(Int64))
        Arrow.lt(values, Arrow::PrimitiveArray(Int64).new_scalar(625_i64))
      end

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_projection(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [2]))
        .with_row_filter(Parquet::Arrow::RowFilter.new([
          filter_a, filter_b,
        ] of Parquet::Arrow::ArrowPredicate))
        .with_batch_size(10)
        .with_limit(10)
        .build

      3.times do
        push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))
      end
      check_column_c(expect_data(decoder.try_decode), 176, 10)

      expect_finished(decoder.try_decode)

      # Only the last predicate is limited: the first one filters all 200 rows
      # of row group 0, while the final one short-circuits before consuming all
      # 24 rows the first predicate selected.
      first_rows.should eq(200)
      second_rows.should be < 24
    end

    it "preserves an explicit row selection while limiting (upstream test_decoder_filter_with_row_selection_and_limit)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      evaluated = 0
      predicate = Parquet::Arrow::ArrowPredicateFn.new(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0])) do |batch|
        evaluated += batch.num_rows
        values = batch.column(0).as(Arrow::PrimitiveArray(Int64))
        Arrow.gt(values, Arrow::PrimitiveArray(Int64).new_scalar(175_i64))
      end

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_projection(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]))
        .with_row_selection(RowSelection.new([
          RowSelector.skip(150),
          RowSelector.select(50),
        ]))
        .with_row_filter(Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate))
        .with_batch_size(10)
        .with_limit(10)
        .build

      push_ranges_to_decoder(decoder, bytes, expect_needs_data(decoder.try_decode))
      check_column_a(expect_data(decoder.try_decode), 176, 10)

      expect_finished(decoder.try_decode)

      # The explicit selection narrows row group 0 to rows 150..199 and
      # `a > 175` keeps 176..199; the limit of 10 stops before evaluating all
      # 50 selected rows.
      evaluated.should be < 50
    end

    it "filters within row-group-local selections (upstream test_row_group_local_selections_with_row_filter)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      predicate = threshold_predicate(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]), 0, 195_i64)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_row_filter(Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate))
        .with_row_group_selections([
          RowGroupSelection.new(1, RowSelection.new([RowSelector.skip(10), RowSelector.select(10)])),
          RowGroupSelection.new(0, RowSelection.new([RowSelector.skip(190), RowSelector.select(10)])),
        ])
        .build
      prefetch_test_file(decoder, bytes)

      # RG1's local rows 10..19 all pass `a > 195`; RG0's local rows 190..199
      # keep only 196..199.
      check_rows(expect_data(decoder.try_decode), 210, 10)
      check_rows(expect_data(decoder.try_decode), 196, 4)
      expect_finished(decoder.try_decode)
    end

    it "carries local selections and the filter through into_builder (upstream test_into_builder_preserves_local_selections_with_row_filter)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)
      schema_descr = metadata.file_metadata.schema_descr

      predicate = threshold_predicate(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]), 0, 195_i64)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_row_filter(Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate))
        .with_row_group_selections([
          RowGroupSelection.new(1, RowSelection.new([RowSelector.skip(10), RowSelector.select(10)])),
          RowGroupSelection.new(0, RowSelection.new([RowSelector.skip(190), RowSelector.select(10)])),
        ])
        .build
      prefetch_test_file(decoder, bytes)

      reader1 = expect_reader(decoder.try_next_reader)
      drain_reader(reader1).each { |batch| check_rows(batch, 210, 10) }

      # Only RG0 and its local `skip(190) + select(10)` remain.
      decoder.is_at_row_group_boundary.should be_true
      decoder.row_groups_remaining.should eq(1)
      decoder = decoder.into_builder.build

      reader0 = expect_reader(decoder.try_next_reader)
      drain_reader(reader0).each { |batch| check_rows(batch, 196, 4) }
      expect_reader_finished(decoder.try_next_reader)
    end

    it "installs a filter between row groups (upstream test_into_builder_installs_filter_between_row_groups)" do
      bytes = test_file_bytes
      metadata = test_metadata(bytes)

      decoder = ParquetPushDecoderBuilder.try_new_decoder(metadata)
        .with_batch_size(1024)
        .build
      prefetch_test_file(decoder, bytes)

      # Reader for row group 0 — no filter.
      reader0 = expect_reader(decoder.try_next_reader)
      drain_reader(reader0).each { |batch| check_rows(batch, 0, 200) }

      # Between row groups: rebuild with a filter on column "a". The rebuilt
      # decoder keeps the buffered bytes, so no data is re-supplied.
      decoder.is_at_row_group_boundary.should be_true
      decoder.row_groups_remaining.should eq(1)
      schema_descr = metadata.file_metadata.schema_descr
      predicate = threshold_predicate(Parquet::Arrow::ProjectionMask.leaves(schema_descr, [0]), 0, 250_i64)
      decoder = decoder.into_builder
        .with_row_filter(Parquet::Arrow::RowFilter.new([predicate] of Parquet::Arrow::ArrowPredicate))
        .build

      # RG1's "a" is 200..399, so `a > 250` keeps 251..399 (149 rows).
      reader1 = expect_reader(decoder.try_next_reader)
      drain_reader(reader1).each { |batch| check_rows(batch, 251, 149) }
      expect_reader_finished(decoder.try_next_reader)
    end
  end
end
