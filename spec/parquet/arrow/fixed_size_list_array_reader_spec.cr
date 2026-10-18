require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/fixed_size_list_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# `FixedSizeListArrayReader`.
#
# The upstream `test_nested_var_list` (built with `FixedSizeListBuilder` over a
# `ListBuilder`) and the two `ArrowWriter` round-trip tests
# (`test_read_list_column`/`test_read_as_dyn_list`) are not ported yet; the
# former is covered structurally by the nested fixed-size list test.
private alias PArrow = Parquet::Arrow
private alias ARUtil = ParquetTestArrayReaderUtil

private def int32_reader(values : Array(Int32), def_levels : Array(Int16),
                         rep_levels : Array(Int16), max_def_level : Int16,
                         max_rep_level : Int16, padding_threshold : Int16? = nil) : PArrow::ArrayReader
  ARUtil.make_int32_page_reader(
    values: values,
    def_levels: def_levels,
    rep_levels: rep_levels,
    max_def_level: max_def_level,
    max_rep_level: max_rep_level,
    padding_threshold: padding_threshold
  )
end

private def fixed_list_type(item_type : Arrow::DataType, item_nullable : Bool,
                            size : Int32) : Arrow::DataType
  Arrow::DataType.fixed_size_list(Arrow::Field.new_list_field(item_type, item_nullable), size)
end

describe Parquet::Arrow::FixedSizeListArrayReader do
  it "reads nullable fixed-size lists" do
    # [null, [1, null, 2], null, [3, 4, 5], [null, null, null]]
    items = Array(Array(Int32?)?).new
    items << nil
    items << ([1, nil, 2] of Int32?)
    items << nil
    items << ([3, 4, 5] of Int32?)
    items << ([nil, nil, nil] of Int32?)
    expected = Arrow::FixedSizeListArray.from_iter_primitive(Int32, items, 3)

    reader = PArrow::FixedSizeListArrayReader.new(
      int32_reader(
        [1, 2, 3, 4, 5],
        [0_i16, 3_i16, 2_i16, 3_i16, 0_i16, 3_i16, 3_i16, 3_i16, 2_i16, 2_i16, 2_i16],
        [0_i16, 0_i16, 1_i16, 1_i16, 0_i16, 0_i16, 1_i16, 1_i16, 0_i16, 1_i16, 1_i16],
        3_i16, 1_i16, 2_i16
      ),
      3,
      fixed_list_type(Arrow::DataType.int32, true, 3),
      2_i16, 1_i16, true, nil
    )

    reader.next_batch(1024).should eq(expected)
  end

  it "reads required fixed-size lists" do
    # [[1, null], [2, 3], [null, null], [4, 5]]
    items = Array(Array(Int32?)?).new
    items << ([1, nil] of Int32?)
    items << ([2, 3] of Int32?)
    items << ([nil, nil] of Int32?)
    items << ([4, 5] of Int32?)
    expected = Arrow::FixedSizeListArray.from_iter_primitive(Int32, items, 2)

    reader = PArrow::FixedSizeListArrayReader.new(
      int32_reader(
        [1, 2, 3, 4, 5],
        [2_i16, 1_i16, 2_i16, 2_i16, 1_i16, 1_i16, 2_i16, 2_i16],
        [0_i16, 1_i16, 0_i16, 1_i16, 0_i16, 1_i16, 0_i16, 1_i16],
        2_i16, 1_i16, 1_i16
      ),
      2,
      fixed_list_type(Arrow::DataType.int32, true, 2),
      1_i16, 1_i16, false, nil
    )

    array = reader.next_batch(1024).as(Arrow::FixedSizeListArray)
    array.should eq(expected)
    array.nulls.should be_nil
  end

  it "rejects misaligned rows" do
    # Two rows with fixed_size == 2 encoded with row lengths 1 and 3: the child
    # count matches, so only the per-row check catches it.
    reader = PArrow::FixedSizeListArrayReader.new(
      int32_reader(
        [1, 2, 3, 4], [2_i16, 2_i16, 2_i16, 2_i16], [0_i16, 0_i16, 1_i16, 1_i16],
        2_i16, 1_i16, 1_i16
      ),
      2,
      fixed_list_type(Arrow::DataType.int32, true, 2),
      1_i16, 1_i16, false, nil
    )

    expect_raises(Parquet::ParquetError,
      "Encountered misaligned row with length 1 (expected length 2)") do
      reader.next_batch(1024)
    end
  end

  it "reads empty (zero-size) fixed-size lists" do
    # [null, [], null, []]
    items = Array(Array(Int32?)?).new
    items << nil
    items << ([] of Int32?)
    items << nil
    items << ([] of Int32?)
    expected = Arrow::FixedSizeListArray.from_iter_primitive(Int32, items, 0)

    reader = PArrow::FixedSizeListArrayReader.new(
      int32_reader(
        [] of Int32, [0_i16, 1_i16, 0_i16, 1_i16], [0_i16, 0_i16, 0_i16, 0_i16],
        2_i16, 1_i16, 2_i16
      ),
      0,
      fixed_list_type(Arrow::DataType.int32, true, 0),
      2_i16, 1_i16, true, nil
    )

    reader.next_batch(1024).should eq(expected)
  end

  it "reads nested fixed-size lists across batches" do
    # [
    #   null,
    #   [[1, 2]],
    #   [[null, 3]],
    #   null,
    #   [[4, 5]],
    #   [[null, null]],
    # ]
    l2_type = fixed_list_type(Arrow::DataType.int32, true, 2)
    l1_type = fixed_list_type(l2_type, false, 1)

    leaf = Arrow::PrimitiveArray(Int32).new(
      Arrow::ScalarBuffer(Int32).from([0, 0, 1, 2, 0, 3, 0, 0, 4, 5, 0, 0]),
      Arrow::NullBuffer.from_slice(
        [false, false, true, true, false, true, false, false, true, true, false, false]
      )
    )

    l2 = Arrow::ArrayData.builder(l2_type)
      .len(6)
      .add_child_data(leaf.to_data)
      .build

    l1 = Arrow::ArrayData.builder(l1_type)
      .len(6)
      .add_child_data(l2)
      .null_bit_buffer(Arrow::Buffer.from_slice_ref(Bytes[0b0011_0110]))
      .build

    expected = Arrow::FixedSizeListArray.from(l1)

    item_reader = int32_reader(
      [1, 2, 3, 4, 5],
      [0_i16, 5_i16, 5_i16, 4_i16, 5_i16, 0_i16, 5_i16, 5_i16, 4_i16, 4_i16],
      [0_i16, 0_i16, 2_i16, 0_i16, 2_i16, 0_i16, 0_i16, 2_i16, 0_i16, 2_i16],
      5_i16, 2_i16, 4_i16
    )

    l2_reader = PArrow::FixedSizeListArrayReader.new(
      item_reader, 2, l2_type, 4_i16, 2_i16, false, 3_i16
    )
    l1_reader = PArrow::FixedSizeListArrayReader.new(
      l2_reader, 1, l1_type, 3_i16, 1_i16, true, nil
    )

    l1_reader.next_batch(2).should eq(expected.slice(0, 2))
    l1_reader.next_batch(1024).should eq(expected.slice(2, 4))
  end

  it "exposes the data type, levels and max def level" do
    reader = PArrow::FixedSizeListArrayReader.new(
      int32_reader([1, 2, 3, 4], [2_i16, 2_i16, 2_i16, 2_i16], [0_i16, 1_i16, 0_i16, 1_i16],
        2_i16, 1_i16, 1_i16),
      2,
      fixed_list_type(Arrow::DataType.int32, true, 2),
      1_i16, 1_i16, true, nil
    )

    reader.data_type.should eq(fixed_list_type(Arrow::DataType.int32, true, 2))
    reader.max_def_level.should eq(1)
    array = reader.next_batch(2).as(Arrow::FixedSizeListArray)
    array.len.should eq(2)
    reader.def_levels.should eq([2_i16, 2_i16, 2_i16, 2_i16])
    reader.rep_levels.should eq([0_i16, 1_i16, 0_i16, 1_i16])
  end

  it "returns an empty fixed-size list array for an empty batch" do
    reader = PArrow::FixedSizeListArrayReader.new(
      int32_reader([] of Int32, [] of Int16, [] of Int16, 2_i16, 1_i16, 1_i16),
      2,
      fixed_list_type(Arrow::DataType.int32, true, 2),
      1_i16, 1_i16, true, nil
    )

    array = reader.next_batch(0).as(Arrow::FixedSizeListArray)
    array.len.should eq(0)
    array.data_type.should eq(fixed_list_type(Arrow::DataType.int32, true, 2))
  end

  it "skips records through the item reader" do
    reader = PArrow::FixedSizeListArrayReader.new(
      int32_reader([1, 2, 3, 4], [2_i16, 2_i16, 2_i16, 2_i16], [0_i16, 1_i16, 0_i16, 1_i16],
        2_i16, 1_i16, 1_i16),
      2,
      fixed_list_type(Arrow::DataType.int32, true, 2),
      1_i16, 1_i16, true, nil
    )

    reader.skip_records(1).should eq(1)
    array = reader.next_batch(1).as(Arrow::FixedSizeListArray)
    array.len.should eq(1)
    Array(Int32?).new(2) { |idx| array.value(0).as(Arrow::PrimitiveArray(Int32)).value(idx) }
      .should eq([3, 4])
  end
end
