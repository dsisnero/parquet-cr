require "../../spec_helper"

# Ported from `parquet/src/arrow/arrow_reader/read_plan.rs` and
# `selection/cursor.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b): `ReadPlanBuilder`,
# `LimitedReadPlanBuilder`, `ReadPlan` and the selector cursor.
#
# Divergences: `usize` counts are `Int32`. The plan's helpers default to
# `RowSelectionPolicy::SELECTORS` so these examples keep exercising the selector
# cursor and the offset/limit transforms; the auto policy, the mask cursor and
# loaded-row-ranges are covered by `row_selection_cursor_spec.cr`.
private alias PArrow = Parquet::Arrow
private alias RowSelector = Parquet::Arrow::RowSelector
private alias RowSelection = Parquet::Arrow::RowSelection

private def plan_builder(selection : RowSelection?, row_count : Int32,
                         offset : Int32? = nil, limit : Int32? = nil) : PArrow::ReadPlanBuilder
  limited = PArrow::ReadPlanBuilder.new(1024)
    .with_selection(selection)
    .with_row_selection_policy(PArrow::RowSelectionPolicy::SELECTORS)
    .limited(row_count)
  limited = limited.with_offset(offset) if offset
  limited = limited.with_limit(limit) if limit
  limited.build_limited
end

private def plan_for(selection : RowSelection?, row_count : Int32,
                     offset : Int32? = nil, limit : Int32? = nil) : PArrow::ReadPlan
  plan_builder(selection, row_count, offset, limit).build
end

describe Parquet::Arrow::ReadPlanBuilder do
  it "defaults to reading all rows" do
    plan = plan_for(nil, 8)
    plan.batch_size.should eq(1024)
    plan.cursor.empty?.should be_false
    plan.cursor.is_a?(PArrow::RowSelectionCursor::AllCursor).should be_true
  end

  it "tracks the selection" do
    selection = RowSelection.new([RowSelector.select(3), RowSelector.skip(5)])
    builder = PArrow::ReadPlanBuilder.new(128).with_selection(selection)

    builder.selection.should eq(selection)
    builder.selects_any?.should be_true
    builder.num_rows_selected.should eq(3)

    empty_builder = PArrow::ReadPlanBuilder.new(128)
    empty_builder.selection.should be_nil
    empty_builder.selects_any?.should be_true
    empty_builder.num_rows_selected.should be_nil
  end

  it "builds a selectors cursor for a selection" do
    selection = RowSelection.new([RowSelector.skip(1), RowSelector.select(2)])
    cursor = plan_for(selection, 8).cursor
      .as(PArrow::RowSelectionCursor::SelectorsCursor)

    cursor.empty?.should be_false
    cursor.next_selector.should eq(RowSelector.skip(1))
    cursor.next_selector.should eq(RowSelector.select(2))
    cursor.empty?.should be_true
  end

  it "rewinds returned selectors" do
    selection = RowSelection.new([RowSelector.select(5)])
    cursor = plan_for(selection, 5).cursor
      .as(PArrow::RowSelectionCursor::SelectorsCursor)

    first = cursor.next_selector
    cursor.return_selector(RowSelector.select(2))
    cursor.next_selector.should eq(RowSelector.select(2))
    first.should eq(RowSelector.select(5))
    cursor.empty?.should be_true
  end
end

describe Parquet::Arrow::LimitedReadPlanBuilder do
  it "applies an offset to an empty selection" do
    builder = plan_builder(nil, 8, offset: 2)
    builder.num_rows_selected.should eq(6)
    plan_for(nil, 8, offset: 2).cursor.as(PArrow::RowSelectionCursor::SelectorsCursor)
      .next_selector.should eq(RowSelector.skip(2))
  end

  it "applies a limit to an empty selection" do
    plan_builder(nil, 8, limit: 3).num_rows_selected.should eq(3)
  end

  it "clamps the limit to the row count" do
    plan_builder(nil, 8, limit: 100).num_rows_selected.should eq(8)
  end

  it "applies an offset and a limit together" do
    plan_builder(nil, 8, offset: 2, limit: 3).num_rows_selected.should eq(3)
  end

  it "produces an empty plan when the offset covers every row" do
    builder = plan_builder(nil, 8, offset: 8)
    builder.num_rows_selected.should eq(0)
    builder.selects_any?.should be_false
  end

  it "applies the offset and limit to an existing selection" do
    selection = RowSelection.new([
      RowSelector.select(5),
      RowSelector.skip(3),
      RowSelector.select(4),
    ])

    plan_builder(selection, 12, offset: 2).num_rows_selected.should eq(7)
    plan_builder(selection, 12, limit: 3).num_rows_selected.should eq(3)
    plan_builder(selection, 12, offset: 6, limit: 2).num_rows_selected.should eq(2)
  end

  it "trims trailing skips when building" do
    plan = plan_for(nil, 8, limit: 3)
    cursor = plan.cursor.as(PArrow::RowSelectionCursor::SelectorsCursor)
    cursor.next_selector.should eq(RowSelector.select(3))
    cursor.empty?.should be_true
  end
end
