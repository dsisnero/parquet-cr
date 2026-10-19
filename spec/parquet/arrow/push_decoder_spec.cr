require "../../spec_helper"

# Ported from `parquet/src/arrow/push_decoder/{mod,remaining}.rs`
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
# Deferred in this slice (documented in `plans/parity.md`): row filters in the
# push path, row-group-local selections, `peek_next_row_group`,
# `try_next_reader` and `into_builder` (adaptive scans).
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

private def push_ranges_to_decoder(decoder : ParquetPushDecoder, bytes : Bytes,
                                   ranges : Array(Range(UInt64, UInt64))) : Nil
  data = ranges.map { |range| bytes[range.begin.to_i32, (range.end - range.begin).to_i32] }
  decoder.push_ranges(ranges, data)
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
