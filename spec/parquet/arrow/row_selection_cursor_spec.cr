require "../../spec_helper"

# Ported from `parquet/src/arrow/arrow_reader/selection/{cursor,ranges}.rs` and
# the `RowSelectionPolicy`/mask-cursor parts of `read_plan.rs` plus the
# mask-batch reading path of `arrow_reader/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Covers the mask-backed row-selection cursor (`RowSelectionPolicy`,
# `RowSelectionStrategy`, `MaskCursor`/`MaskChunk`, `LoadedRowRanges`), page
# pruning (`RowSelection#scan_ranges`/`#row_ranges_for_selected_pages`),
# `expand_to_batch_boundaries`/`auto_selection_strategy`, the plan's strategy
# resolution and the reader's `read_mask_batch` path.
#
# Divergences: `usize` counts/offsets are `Int32` (`Int32::MAX` stands in for
# `usize::MAX`), `Range(UInt64, UInt64)` page ranges are kept as `UInt64`
# because page offsets are `i64` on disk, and the reader's mask path is
# exercised over the ported `SerializedFileWriter` fixture
# `alltypes_plain.parquet` (ids 4, 5, 6, 7, 2, 3, 0, 1) instead of an
# `ArrowWriter`-built file.
private alias PArrow = Parquet::Arrow
private alias BytesCR = Parquet::File::BytesChunkReader
private alias PPI = Parquet::File::PageIndex
private alias RowSelector = Parquet::Arrow::RowSelector
private alias RowSelection = Parquet::Arrow::RowSelection
private alias RowSelectionPolicy = Parquet::Arrow::RowSelectionPolicy
private alias RowSelectionStrategy = Parquet::Arrow::RowSelectionStrategy
private alias RowSelectionCursor = Parquet::Arrow::RowSelectionCursor
private alias MaskChunk = Parquet::Arrow::MaskChunk
private alias LoadedRowRanges = Parquet::Arrow::LoadedRowRanges

private def serialized_reader(name : String) : Parquet::File::SerializedFileReader
  Parquet::File::SerializedFileReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(name)))
end

private def id_projection(reader_file : Parquet::File::SerializedFileReader) : PArrow::ProjectionMask
  PArrow::ProjectionMask.leaves(reader_file.metadata.file_metadata.schema_descr, [0])
end

private def id_reader(*, batch_size : Int32 = 1024) : PArrow::ParquetRecordBatchReaderBuilder
  reader_file = serialized_reader("alltypes_plain.parquet")
  PArrow::ParquetRecordBatchReaderBuilder.try_new(reader_file)
    .with_projection(id_projection(reader_file))
    .with_batch_size(batch_size)
end

private def ids(batches : Array(Arrow::RecordBatch)) : Array(Int32)
  batches.flat_map do |batch|
    column = batch.column(0).as(Arrow::PrimitiveArray(Int32))
    (0...column.len).map { |idx| column.value(idx).as(Int32) }
  end
end

# Seven 10-row pages at offsets 0, 10, ..., 60 (upstream `test_scan_ranges`).
private def page_locations : Array(PPI::PageLocation)
  (0...7).map { |index| PPI::PageLocation.new(index.to_i64 * 10, 10, index.to_i64 * 10) }
end

private def builder_with_selection(selection : RowSelection) : PArrow::ReadPlanBuilder
  PArrow::ReadPlanBuilder.new(1024).with_selection(selection)
end

describe "RowSelection page ranges (selection/ranges.rs)" do
  it "scans data page byte ranges (upstream test_scan_ranges)" do
    selection = RowSelection.new([
      # Skip first page
      RowSelector.skip(10),
      # Multiple selects in same page
      RowSelector.select(3),
      RowSelector.skip(3),
      RowSelector.select(4),
      # Select to page boundary
      RowSelector.skip(5),
      RowSelector.select(5),
      # Skip full page past page boundary
      RowSelector.skip(12),
      # Select across page boundaries
      RowSelector.select(12),
      # Skip final page
      RowSelector.skip(12),
    ])

    selection.scan_ranges(page_locations).should eq([
      10_u64...20_u64,
      20_u64...30_u64,
      40_u64...50_u64,
      50_u64...60_u64,
    ])
    selection.row_ranges_for_selected_pages(page_locations, 70).should eq([
      10...20,
      20...30,
      40...50,
      50...60,
    ])

    selection = RowSelection.new([
      RowSelector.skip(10),
      RowSelector.select(3),
      RowSelector.skip(3),
      RowSelector.select(4),
      RowSelector.skip(5),
      RowSelector.select(5),
      RowSelector.skip(12),
      RowSelector.select(12),
      RowSelector.skip(1),
      # Select across page boundaries including the final page
      RowSelector.select(8),
    ])

    selection.scan_ranges(page_locations).should eq([
      10_u64...20_u64,
      20_u64...30_u64,
      40_u64...50_u64,
      50_u64...60_u64,
      60_u64...70_u64,
    ])

    selection = RowSelection.new([
      RowSelector.skip(10),
      RowSelector.select(3),
      RowSelector.skip(3),
      RowSelector.select(4),
      RowSelector.skip(5),
      RowSelector.select(5),
      RowSelector.skip(12),
      RowSelector.select(12),
      RowSelector.skip(1),
      RowSelector.skip(8),
      # Select from the final page
      RowSelector.select(4),
    ])

    selection.scan_ranges(page_locations).should eq([
      10_u64...20_u64,
      20_u64...30_u64,
      40_u64...50_u64,
      50_u64...60_u64,
      60_u64...70_u64,
    ])

    selection = RowSelection.new([
      RowSelector.skip(10),
      RowSelector.select(3),
      RowSelector.skip(3),
      RowSelector.select(4),
      RowSelector.skip(5),
      # Select to the remaining rows in the page and the first row of the next
      RowSelector.select(6),
      RowSelector.skip(50),
    ])

    selection.scan_ranges(page_locations).should eq([
      10_u64...20_u64,
      20_u64...30_u64,
      30_u64...40_u64,
    ])
  end

  it "scans the page index of a real fixture" do
    reader = Parquet::File::SerializedFileReader.new_with_options(
      BytesCR.new(ParquetTestHelpers.fixture_bytes("alltypes_tiny_pages.parquet")),
      Parquet::File::ReadOptionsBuilder.new.with_page_index.build,
    )
    locations = reader.metadata.page_index.not_nil!.offset_index(0, 0).not_nil!.page_locations
    locations.size.should be > 1
    total_rows = reader.metadata.row_group(0).num_rows.to_i32

    # A sparse selection over the first pages, and a dense one.
    sparse = RowSelection.new([RowSelector.skip(1), RowSelector.select(1)])
    ranges = sparse.scan_ranges(locations)
    ranges.should_not be_empty
    sparse.row_ranges_for_selected_pages(locations, total_rows).size.should eq(ranges.size)

    dense = RowSelection.new([RowSelector.skip(10), RowSelector.select(100)])
    dense.scan_ranges(locations).should_not be_empty
    dense.row_ranges_for_selected_pages(locations, total_rows).should_not be_empty
  end

  it "scans mask-backed selections through their runs" do
    mask = Arrow::BooleanBuffer.from_slice(Array(Bool).new(30) { |index| index >= 10 && index < 12 })
    selection = RowSelection.from_boolean_buffer(mask)

    selection.scan_ranges(page_locations).should eq([10_u64...20_u64])
    selection.row_ranges_for_selected_pages(page_locations, 70).should eq([10...20])
  end

  it "expands a selection to batch boundaries" do
    selection = RowSelection.new([
      RowSelector.select(1),
      RowSelector.skip(9),
      RowSelector.select(1),
      RowSelector.skip(5),
    ])

    # Selected rows 0 and 10 with 4-row batches become [0, 4) and [8, 12).
    selection.expand_to_batch_boundaries(4, 16).should eq(RowSelection.new([
      RowSelector.select(4),
      RowSelector.skip(4),
      RowSelector.select(4),
      RowSelector.skip(4),
    ]))

    # Overlapping expanded ranges merge.
    selection = RowSelection.new([
      RowSelector.select(1),
      RowSelector.skip(1),
      RowSelector.select(1),
    ])
    selection.expand_to_batch_boundaries(4, 16).should eq(RowSelection.new([
      RowSelector.select(4),
      RowSelector.skip(12),
    ]))

    # A zero batch size is the identity (upstream
    # test_offset_zero_and_zero_batch_expand_are_identity).
    identity = RowSelection.from_boolean_buffer(Arrow::BooleanBuffer.from_slice([true, false, true]))
    identity.expand_to_batch_boundaries(0, 3).should eq(identity)
  end
end

describe "RowSelection cursor (selection/cursor.rs)" do
  it "chunks a mask until the cursor is empty (upstream test_next_mask_chunk_until_cursor_is_empty)" do
    cursor = RowSelectionCursor.new_mask_from_selectors([
      RowSelector.skip(2),
      RowSelector.select(2),
      RowSelector.skip(1),
      RowSelector.select(1),
    ], nil).as(RowSelectionCursor::MaskCursor)

    first = cursor.next_mask_chunk(2).not_nil!
    first.initial_skip.should eq(2)
    first.chunk_rows.should eq(2)
    first.selected_rows.should eq(2)

    second = cursor.next_mask_chunk(2).not_nil!
    second.initial_skip.should eq(1)
    second.chunk_rows.should eq(1)
    second.selected_rows.should eq(1)

    cursor.next_mask_chunk(2).should be_nil
    cursor.empty?.should be_true
  end

  it "stops a loaded mask chunk at the trimmed mask end (upstream test_loaded_mask_chunk_stops_at_trimmed_mask_end)" do
    loaded = LoadedRowRanges.from_selection(
      RowSelection.from_consecutive_ranges([0...5], 10)
    )
    cursor = RowSelectionCursor.new_mask_from_selectors(
      [RowSelector.select(1)], loaded
    ).as(RowSelectionCursor::MaskCursor)

    chunk = cursor.next_chunk(10).not_nil!
    chunk.chunk_rows.should eq(1)
    cursor.empty?.should be_true
  end

  it "materialises mask chunk values and rejects chunks past the mask" do
    cursor = RowSelectionCursor.new_mask_from_selectors([
      RowSelector.skip(1),
      RowSelector.select(3),
    ], nil).as(RowSelectionCursor::MaskCursor)

    chunk = cursor.next_chunk(2).not_nil!
    chunk.initial_skip.should eq(1)
    chunk.chunk_rows.should eq(2)
    chunk.selected_rows.should eq(2)
    chunk.mask_start.should eq(1)

    values = cursor.mask_values_for(chunk)
    values.len.should eq(2)
    (0...values.len).each { |index| values.value(index).should be_true }

    bogus = MaskChunk.new(initial_skip: 0, chunk_rows: 100, selected_rows: 1, mask_start: 3)
    expect_raises(Parquet::GeneralError, /MaskChunk exceeds mask length/) do
      cursor.mask_values_for(bogus)
    end
  end
end

describe "ReadPlanBuilder selection strategy (read_plan.rs)" do
  it "prefers mask by default (upstream preferred_selection_strategy_prefers_mask_by_default)" do
    builder = builder_with_selection(RowSelection.new([RowSelector.select(8)]))
    builder.resolve_selection_strategy.should eq(RowSelectionStrategy::Mask)
  end

  it "prefers selectors when the threshold is small (upstream preferred_selection_strategy_prefers_selectors_when_threshold_small)" do
    builder = builder_with_selection(RowSelection.new([RowSelector.select(8)]))
      .with_row_selection_policy(RowSelectionPolicy.auto(1))
    builder.resolve_selection_strategy.should eq(RowSelectionStrategy::Selectors)
  end

  it "handles dense mask backing (upstream preferred_selection_strategy_handles_dense_mask_backing)" do
    mask = Arrow::BooleanBuffer.from_slice(Array(Bool).new(16) { |index| index % 2 == 0 })
    builder = builder_with_selection(RowSelection.from_boolean_buffer(mask))
      .with_row_selection_policy(RowSelectionPolicy.auto(4))
    builder.resolve_selection_strategy.should eq(RowSelectionStrategy::Mask)
  end

  it "handles sparse mask backing (upstream preferred_selection_strategy_handles_sparse_mask_backing)" do
    mask = Arrow::BooleanBuffer.from_slice(Array(Bool).new(128) { |index| index < 64 })
    builder = builder_with_selection(RowSelection.from_boolean_buffer(mask))
      .with_row_selection_policy(RowSelectionPolicy.auto(32))
    builder.resolve_selection_strategy.should eq(RowSelectionStrategy::Selectors)
  end

  it "preserves mask threshold boundaries (upstream preferred_selection_strategy_preserves_mask_threshold_boundaries)" do
    mask = Arrow::BooleanBuffer.from_slice(Array(Bool).new(8, true))

    empty = builder_with_selection(
      RowSelection.from_boolean_buffer(Arrow::BooleanBuffer.new_unset(0))
    )
    empty.resolve_selection_strategy.should eq(RowSelectionStrategy::Mask)

    disabled = builder_with_selection(RowSelection.from_boolean_buffer(mask))
      .with_row_selection_policy(RowSelectionPolicy.auto(0))
    disabled.resolve_selection_strategy.should eq(RowSelectionStrategy::Selectors)

    # Equality does not satisfy the policy's strict inequality: 8 < 1 * 8.
    equal = builder_with_selection(RowSelection.from_boolean_buffer(mask))
      .with_row_selection_policy(RowSelectionPolicy.auto(8))
    equal.resolve_selection_strategy.should eq(RowSelectionStrategy::Selectors)

    above = builder_with_selection(RowSelection.from_boolean_buffer(mask))
      .with_row_selection_policy(RowSelectionPolicy.auto(9))
    above.resolve_selection_strategy.should eq(RowSelectionStrategy::Mask)
  end

  it "matches the selector and mask backings (upstream preferred_selection_strategy_mask_matches_selector_backing)" do
    rng = Random.new(31)
    200.times do
      len = rng.rand(256)
      bits = Array(Bool).new(len) { rng.rand < 0.5 }
      mask_backed = RowSelection.from_boolean_buffer(Arrow::BooleanBuffer.from_slice(bits))
      selector_backed = RowSelection.from_filters([Arrow::BooleanArray.from_values(bits)])

      [0, 1, 2, 8, 32, 64, Int32::MAX].each do |threshold|
        mask_backed.auto_selection_strategy(threshold)
          .should eq(selector_backed.auto_selection_strategy(threshold))
      end
    end
  end

  it "exposes the policy (upstream RowSelectionPolicy::default)" do
    builder = builder_with_selection(RowSelection.new([RowSelector.select(1)]))
    builder.row_selection_policy.should eq(RowSelectionPolicy.default)
    builder.row_selection_policy.should eq(RowSelectionPolicy.auto(32))

    mask_policy = RowSelectionPolicy::MASK
    builder.with_row_selection_policy(mask_policy).row_selection_policy.should eq(mask_policy)
  end
end

describe "mask-backed read plans (read_plan.rs)" do
  it "trims trailing skips before chunking (upstream mask_plan_trims_trailing_skips_before_chunking)" do
    plan = PArrow::ReadPlanBuilder.new(8)
      .with_selection(RowSelection.new([RowSelector.select(1), RowSelector.skip(7)]))
      .with_row_selection_policy(RowSelectionPolicy::MASK)
      .build

    cursor = plan.cursor.as(RowSelectionCursor::MaskCursor)
    chunk = cursor.next_chunk(8).not_nil!
    chunk.chunk_rows.should eq(1)
    chunk.selected_rows.should eq(1)
    cursor.empty?.should be_true
  end

  it "lowers a mask-backed selection under the selectors policy (upstream selectors_policy_lowers_mask_backed_selection)" do
    selection = RowSelection.from_boolean_buffer(
      Arrow::BooleanBuffer.from_slice([true, false, false, true, true])
    )
    plan = PArrow::ReadPlanBuilder.new(4)
      .with_selection(selection)
      .with_row_selection_policy(RowSelectionPolicy::SELECTORS)
      .build

    cursor = plan.cursor.as(RowSelectionCursor::SelectorsCursor)
    cursor.next_selector.should eq(RowSelector.select(1))
    cursor.next_selector.should eq(RowSelector.skip(2))
    cursor.next_selector.should eq(RowSelector.select(2))
    cursor.empty?.should be_true
  end

  it "respects loaded row ranges (upstream mask_backed_plan_respects_loaded_row_ranges)" do
    # Rows 0 and 11 of 12 are selected; pages for rows [4, 10) are unloaded.
    # (Upstream builds this mask from a non-byte-aligned `BooleanBuffer::slice`;
    # the port's cursor only depends on the resulting bits.)
    bits = [true] + Array(Bool).new(10, false) + [true]
    selection = RowSelection.from_boolean_buffer(Arrow::BooleanBuffer.from_slice(bits))
    selection.as_mask.should_not be_nil

    loaded = LoadedRowRanges.from_selection(RowSelection.new([
      RowSelector.select(4),
      RowSelector.skip(6),
      RowSelector.select(2),
    ]))
    loaded.ranges.should eq([0...4, 10...12])

    plan = PArrow::ReadPlanBuilder.new(12)
      .with_selection(selection)
      .with_row_selection_policy(RowSelectionPolicy::MASK)
      .with_loaded_row_ranges(loaded)
      .build

    cursor = plan.cursor.as(RowSelectionCursor::MaskCursor)

    # The first chunk stops at its final selected row instead of carrying
    # trailing skipped rows to the loaded range boundary.
    first = cursor.next_chunk(12).not_nil!
    first.initial_skip.should eq(0)
    first.chunk_rows.should eq(1)
    first.selected_rows.should eq(1)

    # The second chunk skips directly to the next selected row.
    second = cursor.next_chunk(12).not_nil!
    second.initial_skip.should eq(10)
    second.chunk_rows.should eq(1)
    second.selected_rows.should eq(1)
    cursor.empty?.should be_true
  end
end

describe "ParquetRecordBatchReader mask batches" do
  it "reads a mask-backed selection end to end" do
    selection = RowSelection.new([
      RowSelector.select(2),
      RowSelector.skip(2),
      RowSelector.select(2),
      RowSelector.skip(2),
    ])
    builder = id_reader.with_row_selection(selection)
    builder.build.read_plan.cursor.is_a?(RowSelectionCursor::MaskCursor).should be_true

    ids(builder.build.to_a).should eq([4, 5, 2, 3])
  end

  it "splits mask selections into batches" do
    selection = RowSelection.new([RowSelector.select(2), RowSelector.skip(2), RowSelector.select(2)])
    batches = id_reader(batch_size: 2).with_row_selection(selection).build.to_a
    batches.map(&.num_rows).should eq([2, 2])
    ids(batches).should eq([4, 5, 2, 3])
  end

  it "honours an explicit selectors policy for a mask-backed selection" do
    selection = RowSelection.from_boolean_buffer(
      Arrow::BooleanBuffer.from_slice([true, false, true, true, false, true, false, false])
    )
    batches = id_reader
      .with_row_selection(selection)
      .with_row_selection_policy(RowSelectionPolicy::SELECTORS)
      .build
      .to_a
    ids(batches).should eq([4, 6, 7, 3])
  end

  it "reads a mask-backed selection combined with a row filter" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    mask = id_projection(reader_file)
    predicate = PArrow::ArrowPredicateFn.new(mask) do |batch|
      column = batch.column(0).as(Arrow::PrimitiveArray(Int32))
      Arrow.gt(column, Arrow::PrimitiveArray(Int32).new_scalar(2))
    end

    # A mask-backed prior selection keeps the predicate result mask-backed, so
    # the final read goes through the mask cursor.
    selection = RowSelection.from_boolean_buffer(
      Arrow::BooleanBuffer.from_slice([true, true, true, true, false, false, false, false])
    )
    builder = id_reader
      .with_row_selection(selection)
      .with_row_filter(PArrow::RowFilter.new(predicate))
    builder.build.read_plan.cursor.is_a?(RowSelectionCursor::MaskCursor).should be_true

    # Rows 0..4 (ids 4, 5, 6, 7) filtered by `id > 2` keeps all four.
    ids(builder.build.to_a).should eq([4, 5, 6, 7])
  end
end
