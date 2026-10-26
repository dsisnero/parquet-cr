require "../../spec_helper"

# Ported from `parquet/src/arrow/arrow_reader/mod.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `RowGroupSelection`, `RowGroupPlan` (the internal `Global`/`PerRowGroup`/
# `Conflicting` state machine behind `with_row_groups`/`with_row_selection`) and
# the reader's row-group selection.
#
# Upstream's reader-level test (`test_read_row_numbers_row_group_order`) also
# reads virtual row-number columns; virtual_reader_spec.cr now covers that
# integrated path, while this file checks the value-order planning contract. The fixtures are written
# with the ported `SerializedFileWriter` (two 50-row row groups holding
# 5000..5100) instead of `ArrowWriter`.
#
# Divergences: `usize` row-group indices are `Int32`, `RowGroupPlan` is a public
# mutable class carrying a `Kind` (upstream `pub(crate)` enum, transitions by
# reassigning `self`), and `RowGroupSelection#selection` is a nilable getter.
private alias PArrow = Parquet::Arrow
private alias BytesCR = Parquet::File::BytesChunkReader
private alias FWriter = Parquet::File::SerializedFileWriter
private alias WP = Parquet::File::Properties::WriterProperties
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PRep = Parquet::Repetition
private alias RowSelector = Parquet::Arrow::RowSelector
private alias RowSelection = Parquet::Arrow::RowSelection
private alias RowGroupSelection = Parquet::Arrow::RowGroupSelection
private alias RowGroupPlan = Parquet::Arrow::RowGroupPlan

private def row_selection(rows : Int32) : RowSelection
  RowSelection.new([RowSelector.select(rows)])
end

# Writes `values` as REQUIRED INT64 column "col", one row group per entry.
private def write_int64_row_groups(row_groups : Array(Array(Int64))) : Parquet::File::SerializedFileReader
  leaf = ST.primitive_type_builder("col", PType::INT64)
    .with_repetition(PRep::REQUIRED)
    .build.as(ST)
  schema = ST.group_type_builder("schema").with_fields([leaf]).build

  io = IO::Memory.new
  writer = FWriter.new(io, schema, WP.builder.build)
  row_groups.each do |values|
    row_group = writer.next_row_group
    column_writer = row_group.next_column.not_nil!
    column_writer.typed(Int64).write_batch(values, nil, nil)
    column_writer.close
    row_group.close
  end
  writer.close
  Parquet::File::SerializedFileReader.new(BytesCR.new(io.to_slice))
end

# 100 rows in two 50-row row groups: row group 0 holds 5000..5050 and row group
# 1 holds 5050..5100 (upstream `test_read_row_numbers_row_group_order`).
private def two_row_group_reader : Parquet::File::SerializedFileReader
  write_int64_row_groups([
    (5000_i64...5050_i64).to_a,
    (5050_i64...5100_i64).to_a,
  ])
end

private def builder_for(file : Parquet::File::SerializedFileReader) : PArrow::ParquetRecordBatchReaderBuilder
  PArrow::ParquetRecordBatchReaderBuilder.try_new(file)
end

private def values(batches : Array(Arrow::RecordBatch)) : Array(Int64)
  batches.flat_map do |batch|
    column = batch.column(0).as(Arrow::PrimitiveArray(Int64))
    (0...column.len).map { |idx| column.value(idx).as(Int64) }
  end
end

describe Parquet::Arrow::RowGroupSelection do
  it "exposes its row group and local selection (upstream row_group_selection_accessors)" do
    row_group = RowGroupSelection.new(3, row_selection(5))
    row_group.row_group_index.should eq(3)
    row_group.selection.not_nil!.row_count.should eq(5)

    row_group = RowGroupSelection.new(4)
    row_group.row_group_index.should eq(4)
    row_group.selection.should be_nil

    RowGroupSelection.new(3, row_selection(5)).should eq(RowGroupSelection.new(3, row_selection(5)))
    RowGroupSelection.new(3).should_not eq(RowGroupSelection.new(4))
  end
end

describe Parquet::Arrow::RowGroupPlan do
  it "tracks global configuration (upstream row_group_plan_tracks_global_configuration)" do
    plan = RowGroupPlan.global
    plan.global?.should be_true

    plan.set_row_groups([0])
    plan.set_row_groups([1, 2])
    plan.set_row_selection(row_selection(3))
    plan.set_row_selection(row_selection(4))

    row_groups, selection = plan.into_global
    row_groups.should eq([1, 2])
    selection.not_nil!.row_count.should eq(4)
  end

  it "replaces local configuration (upstream row_group_plan_replaces_local_configuration)" do
    plan = RowGroupPlan.global
    plan.set_row_group_selections([RowGroupSelection.new(0)])
    plan.set_row_group_selections([RowGroupSelection.new(1)])

    plan.per_row_group?.should be_true
    plan.selections.should eq([RowGroupSelection.new(1)])
  end

  it "rejects mixed configuration (upstream row_group_plan_rejects_mixed_configuration)" do
    row_groups_then_local = RowGroupPlan.global
    row_groups_then_local.set_row_groups([0])
    row_groups_then_local.set_row_group_selections([RowGroupSelection.new(0)])
    row_groups_then_local.conflicting?.should be_true

    # Further configuration is ignored once conflicting.
    row_groups_then_local.set_row_groups([1])
    row_groups_then_local.set_row_selection(row_selection(1))
    row_groups_then_local.set_row_group_selections([RowGroupSelection.new(1)])
    row_groups_then_local.conflicting?.should be_true
    expect_raises(Parquet::GeneralError, /with_row_group_selections cannot be combined with with_row_groups or with_row_selection/) do
      row_groups_then_local.into_global
    end

    local_then_row_groups = RowGroupPlan.per_row_group([RowGroupSelection.new(0)])
    local_then_row_groups.set_row_groups([0])
    local_then_row_groups.conflicting?.should be_true

    local_then_selection = RowGroupPlan.per_row_group([RowGroupSelection.new(0)])
    local_then_selection.set_row_selection(row_selection(1))
    local_then_selection.conflicting?.should be_true

    # The synchronous reader cannot apply per-row-group selections.
    local = RowGroupPlan.per_row_group([RowGroupSelection.new(0)])
    expect_raises(Parquet::GeneralError, /Row-group-local selections are not supported by the synchronous reader/) do
      local.into_global
    end
  end
end

describe "ParquetRecordBatchReader row-group selection" do
  it "reads every row group in file order by default" do
    values(builder_for(two_row_group_reader).build.to_a)
      .should eq((5000_i64...5100_i64).to_a)
  end

  it "reads only the requested row groups, in the requested order" do
    values(builder_for(two_row_group_reader).with_row_groups([1, 0]).build.to_a)
      .should eq((5050_i64...5100_i64).to_a + (5000_i64...5050_i64).to_a)

    values(builder_for(two_row_group_reader).with_row_groups([1]).build.to_a)
      .should eq((5050_i64...5100_i64).to_a)
  end

  it "applies offset and limit to the selected rows" do
    # Selection order is [row group 1, row group 0]; skipping 45 rows starts at
    # 5095 and the limit then crosses into row group 0.
    values(builder_for(two_row_group_reader).with_row_groups([1, 0]).with_offset(45).with_limit(10).build.to_a)
      .should eq([5095_i64, 5096_i64, 5097_i64, 5098_i64, 5099_i64,
                  5000_i64, 5001_i64, 5002_i64, 5003_i64, 5004_i64])
  end

  it "applies a row selection to the concatenated selected rows" do
    selection = RowSelection.new([RowSelector.select(60)])
    values(builder_for(two_row_group_reader).with_row_groups([1, 0]).with_row_selection(selection).build.to_a)
      .should eq((5050_i64...5100_i64).to_a + (5000_i64...5010_i64).to_a)
  end

  it "applies a row filter to the selected row groups" do
    reader_file = two_row_group_reader
    mask = PArrow::ProjectionMask.leaves(reader_file.metadata.file_metadata.schema_descr, [0])
    predicate = PArrow::ArrowPredicateFn.new(mask) do |batch|
      column = batch.column(0).as(Arrow::PrimitiveArray(Int64))
      Arrow.gt(column, Arrow::PrimitiveArray(Int64).new_scalar(5074_i64))
    end

    values(
      builder_for(reader_file)
        .with_row_groups([1])
        .with_row_filter(PArrow::RowFilter.new(predicate))
        .build
        .to_a
    ).should eq((5075_i64...5100_i64).to_a)
  end

  it "reads nothing when no row group is selected" do
    builder_for(two_row_group_reader).with_row_groups([] of Int32).build.to_a.should be_empty
  end
end
