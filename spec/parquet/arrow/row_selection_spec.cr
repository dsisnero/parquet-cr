require "../../spec_helper"

# Ported from `parquet/src/arrow/arrow_reader/selection/selector.rs` and the
# selector-backed parts of `selection/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b): `RowSelector`, the RLE-backed
# `RowSelection` (normalization, consecutive ranges, offset/limit, trim,
# split_off, counts) and the selector transforms.
#
# Divergences: the bitmap ("mask") backing (`RowSelection::from_boolean_buffer`,
# `from_filters`, `as_mask`, `MaskRunIter`, and the mask algebra module) is not
# ported, so every `RowSelection` here is selector-backed; `usize` counts are
# `Int32`.
private alias PArrow = Parquet::Arrow
private alias RowSelector = Parquet::Arrow::RowSelector
private alias RowSelection = Parquet::Arrow::RowSelection

private def selectors(selection : RowSelection) : Array(RowSelector)
  selection.to_a
end

describe Parquet::Arrow::RowSelector do
  it "creates select and skip selectors" do
    RowSelector.select(5).row_count.should eq(5)
    RowSelector.select(5).skip?.should be_false
    RowSelector.skip(3).row_count.should eq(3)
    RowSelector.skip(3).skip?.should be_true
    RowSelector.select(5).should eq(RowSelector.select(5))
    RowSelector.select(5).should_not eq(RowSelector.skip(5))
  end
end

describe Parquet::Arrow::RowSelection do
  it "skips empty selectors and combines adjacent ones" do
    selection = RowSelection.new([
      RowSelector.select(0),
      RowSelector.skip(0),
      RowSelector.select(2),
      RowSelector.select(0),
      RowSelector.skip(1),
    ])
    selectors(selection).should eq([RowSelector.select(2), RowSelector.skip(1)])

    selection = RowSelection.new([
      RowSelector.skip(0),
      RowSelector.select(2),
      RowSelector.skip(0),
      RowSelector.select(2),
    ])
    selectors(selection).should eq([RowSelector.select(4)])

    selection = RowSelection.new([
      RowSelector.select(0),
      RowSelector.skip(2),
      RowSelector.select(0),
      RowSelector.skip(2),
    ])
    selectors(selection).should eq([RowSelector.skip(4)])
  end

  it "starts from and stays empty" do
    RowSelection.new([] of RowSelector).to_a.should be_empty
    RowSelection.empty.to_a.should be_empty
  end

  it "combines selectors like upstream from_iter" do
    a = [RowSelector.skip(3), RowSelector.skip(3), RowSelector.select(10), RowSelector.skip(4)]
    b = a + [RowSelector.skip(0)]
    c = [
      RowSelector.skip(2),
      RowSelector.skip(4),
      RowSelector.select(3),
      RowSelector.select(3),
      RowSelector.select(4),
      RowSelector.skip(3),
      RowSelector.skip(1),
      RowSelector.skip(0),
    ]
    expected = RowSelection.new([
      RowSelector.skip(6),
      RowSelector.select(10),
      RowSelector.skip(4),
    ])

    RowSelection.new(a).should eq(expected)
    RowSelection.new(b).should eq(expected)
    RowSelection.new(c).should eq(expected)

    selectors(RowSelection.new([RowSelector.select(10), RowSelector.select(5)]))
      .should eq([RowSelector.select(15)])
    selectors(RowSelection.new([RowSelector.select(10), RowSelector.skip(5)]))
      .should eq([RowSelector.select(10), RowSelector.skip(5)])
    selectors(RowSelection.new([RowSelector.skip(10), RowSelector.select(5)]))
      .should eq([RowSelector.skip(10), RowSelector.select(5)])
    selectors(RowSelection.new([RowSelector.skip(10), RowSelector.skip(5)]))
      .should eq([RowSelector.skip(15)])
  end

  it "builds selections from consecutive ranges" do
    selection = RowSelection.from_consecutive_ranges([1...3, 4...6, 6...6, 8...8, 9...10], 10)
    selectors(selection).should eq([
      RowSelector.skip(1),
      RowSelector.select(2),
      RowSelector.skip(1),
      RowSelector.select(2),
      RowSelector.skip(3),
      RowSelector.select(1),
    ])

    # Select everything.
    selection = RowSelection.from_consecutive_ranges([0...10], 10)
    selectors(selection).should eq([RowSelector.select(10)])

    # Out-of-order ranges are rejected.
    expect_raises(Parquet::ParquetError, /out of order/) do
      RowSelection.from_consecutive_ranges([1...3, 8...10, 4...7], 10)
    end
  end

  it "reports row counts" do
    selection = RowSelection.new([RowSelector.select(10), RowSelector.skip(20), RowSelector.select(5)])
    selection.row_count.should eq(15)
    selection.skipped_row_count.should eq(20)
    selection.total_row_count.should eq(35)
    selection.selects_any?.should be_true
    RowSelection.new([RowSelector.skip(4)]).selects_any?.should be_false
  end

  it "applies an offset" do
    selection = RowSelection.new([
      RowSelector.select(5),
      RowSelector.skip(23),
      RowSelector.select(7),
      RowSelector.skip(33),
      RowSelector.select(6),
    ])

    selection = selection.offset(2)
    selectors(selection).should eq([
      RowSelector.skip(2),
      RowSelector.select(3),
      RowSelector.skip(23),
      RowSelector.select(7),
      RowSelector.skip(33),
      RowSelector.select(6),
    ])

    selection = selection.offset(5)
    selectors(selection).should eq([
      RowSelector.skip(30),
      RowSelector.select(5),
      RowSelector.skip(33),
      RowSelector.select(6),
    ])

    selection = selection.offset(3)
    selectors(selection).should eq([
      RowSelector.skip(33),
      RowSelector.select(2),
      RowSelector.skip(33),
      RowSelector.select(6),
    ])

    selection = selection.offset(2)
    selectors(selection).should eq([RowSelector.skip(68), RowSelector.select(6)])

    selection = selection.offset(3)
    selectors(selection).should eq([RowSelector.skip(71), RowSelector.select(3)])
  end

  it "applies a limit" do
    # A limit equal to the selection is a no-op (trailing skips are dropped).
    selection = RowSelection.new([RowSelector.select(10), RowSelector.skip(90)])
    selectors(selection.limit(10)).should eq([RowSelector.select(10)])

    selection = RowSelection.new([
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(10),
    ])

    selectors(selection.limit(5)).should eq([RowSelector.select(5)])
    selectors(selection.limit(15)).should eq([
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(5),
    ])
    selectors(selection.limit(0)).should be_empty
    selectors(selection.limit(30)).should eq([
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(10),
    ])
    selectors(selection.limit(100)).should eq(selectors(selection))
  end

  it "splits off prefixes" do
    selection = RowSelection.new([
      RowSelector.skip(34),
      RowSelector.select(12),
      RowSelector.skip(3),
      RowSelector.select(35),
    ])

    split = selection.split_off(34)
    selectors(split).should eq([RowSelector.skip(34)])
    selectors(selection).should eq([
      RowSelector.select(12),
      RowSelector.skip(3),
      RowSelector.select(35),
    ])

    split = selection.split_off(5)
    selectors(split).should eq([RowSelector.select(5)])
    selectors(selection).should eq([
      RowSelector.select(7),
      RowSelector.skip(3),
      RowSelector.select(35),
    ])

    split = selection.split_off(8)
    selectors(split).should eq([RowSelector.select(7), RowSelector.skip(1)])
    selectors(selection).should eq([RowSelector.skip(2), RowSelector.select(35)])

    split = selection.split_off(200)
    selectors(split).should eq([RowSelector.skip(2), RowSelector.select(35)])
    selectors(selection).should be_empty
  end

  it "trims trailing skips" do
    selection = RowSelection.new([
      RowSelector.skip(34),
      RowSelector.select(12),
      RowSelector.skip(3),
      RowSelector.select(35),
    ])
    selectors(selection.trim).should eq([
      RowSelector.skip(34),
      RowSelector.select(12),
      RowSelector.skip(3),
      RowSelector.select(35),
    ])

    selection = RowSelection.new([
      RowSelector.skip(34),
      RowSelector.select(12),
      RowSelector.skip(3),
    ])
    selectors(selection.trim).should eq([RowSelector.skip(34), RowSelector.select(12)])
  end
end
