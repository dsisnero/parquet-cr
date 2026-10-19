require "../../spec_helper"

# Ported/characterized from `parquet/src/arrow/arrow_reader/filter.rs` and the
# `RowFilter` support in `arrow_reader/read_plan.rs` (`with_predicate`,
# `PredicateOptions`, `filters_to_boolean_buffer`) and
# `arrow_reader/mod.rs` (`with_row_filter`, the eager predicate evaluation in
# `build`) (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream's row-filter tests live in the `arrow_reader` test harness and build
# their fixtures with `ArrowWriter`, so the port characterizes the same
# behaviour over the ported `SerializedFileWriter` fixtures plus the
# `alltypes_plain.parquet` file (ids 4, 5, 6, 7, 2, 3, 0, 1).
#
# Divergences: predicates are `RecordBatch -> BooleanArray` blocks (upstream
# `FnMut(..) -> Result<BooleanArray, ArrowError>`, so errors raise), `usize`
# counts are `Int32`, and the port's plan is always selector-backed (no mask
# cursor), so predicate evaluations are applied through `RowSelection#and_then`
# on selectors.
private alias PArrow = Parquet::Arrow
private alias BytesCR = Parquet::File::BytesChunkReader

private def serialized_reader(name : String) : Parquet::File::SerializedFileReader
  Parquet::File::SerializedFileReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(name)))
end

# The `id` leaf (index 0) of `alltypes_plain.parquet`.
private def id_projection(reader_file : Parquet::File::SerializedFileReader) : PArrow::ProjectionMask
  PArrow::ProjectionMask.leaves(reader_file.metadata.file_metadata.schema_descr, [0])
end

# A predicate over the projected `id` column (upstream's doc example builds
# `b > 0` the same way with `arrow::compute::kernels::cmp::gt`).
private def id_predicate(mask : PArrow::ProjectionMask, threshold : Int32) : PArrow::ArrowPredicate
  PArrow::ArrowPredicateFn.new(mask) do |batch|
    column = batch.column(0).as(Arrow::PrimitiveArray(Int32))
    Arrow.gt(column, Arrow::PrimitiveArray(Int32).new_scalar(threshold))
  end
end

private def id_array_reader(reader_file : Parquet::File::SerializedFileReader, batch_size : Int32 = 1024) : PArrow::ArrayReader
  fields = PArrow::ArrowReaderMetadata.load(reader_file).fields
  PArrow::ArrayReaderBuilder.new(PArrow::FileRowGroups.new(reader_file), batch_size)
    .build_array_reader(fields, id_projection(reader_file))
end

private def id_reader(batch_size : Int32 = 1024) : PArrow::ParquetRecordBatchReaderBuilder
  reader_file = serialized_reader("alltypes_plain.parquet")
  PArrow::ParquetRecordBatchReaderBuilder.try_new(reader_file)
    .with_projection(id_projection(reader_file))
    .with_batch_size(batch_size)
end

private def filtered_id_reader(threshold : Int32, batch_size : Int32 = 1024) : PArrow::ParquetRecordBatchReaderBuilder
  reader_file = serialized_reader("alltypes_plain.parquet")
  mask = id_projection(reader_file)
  id_reader(batch_size).with_row_filter(row_filter(id_predicate(mask, threshold)))
end

private def row_filter(*predicates : PArrow::ArrowPredicate) : PArrow::RowFilter
  PArrow::RowFilter.new(*predicates)
end

private def ids(batches : Array(Arrow::RecordBatch)) : Array(Int32)
  batches.flat_map do |batch|
    column = batch.column(0).as(Arrow::PrimitiveArray(Int32))
    (0...column.len).map { |idx| column.value(idx).as(Int32) }
  end
end

# The RLE form of a selection's plan, for precise predicate-plan assertions.
private def selector_runs(selection : PArrow::RowSelection) : Array(PArrow::RowSelector)
  selection.to_a
end

describe Parquet::Arrow::RowFilter do
  it "holds its predicates in order" do
    mask = PArrow::ProjectionMask.all
    first = PArrow::ArrowPredicateFn.new(mask) { |batch| Arrow::BooleanArray.from_values(Array(Bool).new(batch.num_rows, true)) }
    second = PArrow::ArrowPredicateFn.new(mask) { |batch| Arrow::BooleanArray.from_values(Array(Bool).new(batch.num_rows, true)) }

    filter = PArrow::RowFilter.new([first, second] of PArrow::ArrowPredicate)
    filter.predicates.size.should eq(2)
    filter.predicates[0].should be(first)
    filter.predicates[1].should be(second)
    filter.to_s.should eq("RowFilter { 2 predicates: }")

    PArrow::RowFilter.new([] of PArrow::ArrowPredicate).predicates.should be_empty
  end

  it "exposes an arrow predicate's projection and evaluates its block" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    mask = id_projection(reader_file)
    evaluated = 0

    predicate = PArrow::ArrowPredicateFn.new(mask) do |batch|
      evaluated += 1
      # The predicate only sees the columns in its projection mask.
      batch.num_columns.should eq(1)
      batch.num_rows.should eq(8)
      Arrow::BooleanArray.from_values(Array(Bool).new(batch.num_rows, true))
    end

    predicate.projection.should be(mask)

    batches = id_reader.with_row_filter(row_filter(predicate)).build.to_a
    evaluated.should eq(1)
    ids(batches).should eq([4, 5, 6, 7, 2, 3, 0, 1])
  end
end

describe "ParquetRecordBatchReader row filters" do
  it "keeps only the rows a predicate selects" do
    batches = filtered_id_reader(4).build.to_a
    ids(batches).should eq([5, 6, 7])

    # Nothing selected, and everything selected.
    filtered_id_reader(100).build.to_a.should be_empty
    ids(filtered_id_reader(-1).build.to_a).should eq([4, 5, 6, 7, 2, 3, 0, 1])
  end

  it "splits the selected rows into batches of the requested size" do
    batches = filtered_id_reader(4, batch_size: 2).build.to_a
    batches.map(&.num_rows).should eq([2, 1])
    ids(batches).should eq([5, 6, 7])
  end

  it "applies a predicate after a row selection" do
    selection = PArrow::RowSelection.new([
      PArrow::RowSelector.select(2),
      PArrow::RowSelector.skip(4),
      PArrow::RowSelector.select(2),
    ])
    # The selection keeps ids 4, 5, 0, 1; `id > 4` keeps 5.
    batches = filtered_id_reader(4).with_row_selection(selection).build.to_a
    ids(batches).should eq([5])
  end

  it "applies an offset and a limit after the filter" do
    batches = filtered_id_reader(4).with_offset(1).with_limit(1).build.to_a
    ids(batches).should eq([6])

    filtered_id_reader(4).with_offset(3).build.to_a.should be_empty
  end

  it "applies multiple predicates in order" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    mask = id_projection(reader_file)
    seen = [] of Array(Int32)

    greater_than_two = PArrow::ArrowPredicateFn.new(mask) do |batch|
      column = batch.column(0).as(Arrow::PrimitiveArray(Int32))
      seen << (0...column.len).map { |idx| column.value(idx).as(Int32) }
      Arrow.gt(column, Arrow::PrimitiveArray(Int32).new_scalar(2))
    end
    less_than_six = PArrow::ArrowPredicateFn.new(mask) do |batch|
      column = batch.column(0).as(Arrow::PrimitiveArray(Int32))
      seen << (0...column.len).map { |idx| column.value(idx).as(Int32) }
      Arrow.lt(column, Arrow::PrimitiveArray(Int32).new_scalar(6))
    end

    batches = id_reader
      .with_row_filter(row_filter(greater_than_two, less_than_six))
      .build.to_a

    # `id > 2` keeps ids 4, 5, 6, 7, 3; `id < 6` then keeps 4, 5, 3.
    ids(batches).should eq([4, 5, 3])
    # The second predicate is evaluated over the rows the first one kept.
    seen.size.should eq(2)
    seen[0].should eq([4, 5, 6, 7, 2, 3, 0, 1])
    seen[1].should eq([4, 5, 6, 7, 3])
  end

  it "treats null predicate results as not selected" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    mask = id_projection(reader_file)

    predicate = PArrow::ArrowPredicateFn.new(mask) do |batch|
      # `id > 4` with the second row's result nulled out.
      column = batch.column(0).as(Arrow::PrimitiveArray(Int32))
      filter = Arrow.gt(column, Arrow::PrimitiveArray(Int32).new_scalar(4))
      validity = ::Arrow::BooleanBuffer.from_slice(
        Array(Bool).new(batch.num_rows) { |idx| idx != 1 }
      )
      ::Arrow::BooleanArray.new(filter.values, ::Arrow::NullBuffer.new(validity))
    end

    # Row 1 (id 5) would match, but its predicate result is null.
    ids(id_reader.with_row_filter(row_filter(predicate)).build.to_a).should eq([6, 7])
  end

  it "rejects a predicate that returns the wrong number of rows" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    mask = id_projection(reader_file)

    predicate = PArrow::ArrowPredicateFn.new(mask) do |_batch|
      Arrow::BooleanArray.from_values([true])
    end

    expect_raises(Parquet::ArrowError, /ArrowPredicate predicate returned 1 rows, expected 8/) do
      id_reader.with_row_filter(row_filter(predicate)).build.to_a
    end
  end
end

describe "Parquet::Arrow::ReadPlanBuilder predicates" do
  it "narrows the selection to the predicate's rows" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    builder = PArrow::ReadPlanBuilder.new(1024)
      .with_predicate(id_array_reader(reader_file), id_predicate(id_projection(reader_file), 4))

    builder.num_rows_selected.should eq(3)
    selector_runs(builder.selection.not_nil!).should eq([
      PArrow::RowSelector.skip(1),
      PArrow::RowSelector.select(3),
      PArrow::RowSelector.skip(4),
    ])
  end

  it "leaves the selection empty when the predicate selects every row" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    builder = PArrow::ReadPlanBuilder.new(1024)
      .with_predicate(id_array_reader(reader_file), id_predicate(id_projection(reader_file), -1))

    builder.selection.should be_nil
    builder.num_rows_selected.should be_nil
    builder.selects_any?.should be_true
  end

  it "combines the predicate with a pre-existing selection" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    selection = PArrow::RowSelection.new([
      PArrow::RowSelector.skip(2),
      PArrow::RowSelector.select(4),
      PArrow::RowSelector.skip(2),
    ])
    builder = PArrow::ReadPlanBuilder.new(1024)
      .with_selection(selection)
      .with_predicate(id_array_reader(reader_file), id_predicate(id_projection(reader_file), 4))

    # Rows 2..6 (ids 6, 7, 2, 3) filtered by `id > 4` keeps ids 6 and 7.
    selector_runs(builder.selection.not_nil!).should eq([
      PArrow::RowSelector.skip(2),
      PArrow::RowSelector.select(2),
      PArrow::RowSelector.skip(4),
    ])
  end

  it "limits to zero matches and to more matches than the predicate selects" do
    reader_file = serialized_reader("alltypes_plain.parquet")

    # A zero limit keeps no rows at all (every truncated filter bit is false).
    zero = PArrow::ReadPlanBuilder.new(1024).with_predicate_options(
      PArrow::PredicateOptions.new(
        id_array_reader(reader_file),
        id_predicate(id_projection(reader_file), 4),
      ).with_limit(0, 8)
    )
    zero.num_rows_selected.should eq(0)
    zero.selects_any?.should be_false
    selector_runs(zero.selection.not_nil!).should eq([PArrow::RowSelector.skip(8)])

    # A limit beyond the number of matches leaves the filter untouched.
    generous = PArrow::ReadPlanBuilder.new(1024).with_predicate_options(
      PArrow::PredicateOptions.new(
        id_array_reader(reader_file),
        id_predicate(id_projection(reader_file), 4),
      ).with_limit(10, 8)
    )
    generous.num_rows_selected.should eq(3)
    selector_runs(generous.selection.not_nil!).should eq([
      PArrow::RowSelector.skip(1),
      PArrow::RowSelector.select(3),
      PArrow::RowSelector.skip(4),
    ])
  end

  it "stops at the match limit and pads the tail" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    options = PArrow::PredicateOptions.new(
      id_array_reader(reader_file),
      id_predicate(id_projection(reader_file), 4),
    ).with_limit(2, 10)

    builder = PArrow::ReadPlanBuilder.new(1024).with_predicate_options(options)

    builder.num_rows_selected.should eq(2)
    selector_runs(builder.selection.not_nil!).should eq([
      PArrow::RowSelector.skip(1),
      PArrow::RowSelector.select(2),
      PArrow::RowSelector.skip(7),
    ])
    # The padded tail keeps the selection spanning `total_rows`.
    builder.selection.not_nil!.total_row_count.should eq(10)
  end
end
