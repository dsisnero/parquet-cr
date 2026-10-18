require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/list_view_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# `ListViewArrayReader` for `ListView` (Int32 offsets) and `LargeListView`
# (Int64 offsets). It wraps a `ListArrayReader` and converts its output to a
# list-view array.
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

# Upstream `Field::new_list_field(Int32, true)` + `ListView`/`LargeListView`.
private def list_view_type(offset_type : O.class) : Arrow::DataType forall O
  field = Arrow::Field.new_list_field(Arrow::DataType.int32, true)
  {% if O == Int64 %}
    Arrow::DataType.large_list_view(field)
  {% else %}
    Arrow::DataType.list_view(field)
  {% end %}
end

# Upstream `from_iter_primitive` builds the matching list-view array.
private def list_view_array(items : Array(Array(Int32?)?), offset_type : O.class) forall O
  Arrow::GenericListViewArray(O).from_iter_primitive(Int32, items)
end

private def nullable_list_view(offset_type : O.class) : Nil forall O
  # [[1, null, 2], null, [], [3, 4], [], [], null, [], [null, 1]]
  items = Array(Array(Int32?)?).new
  items << ([1, nil, 2] of Int32?)
  items << nil
  items << ([] of Int32?)
  items << ([3, 4] of Int32?)
  items << ([] of Int32?)
  items << ([] of Int32?)
  items << nil
  items << ([] of Int32?)
  items << ([nil, 1] of Int32?)

  reader = PArrow::ListViewArrayReader(O).new(
    int32_reader(
      [1, 2, 3, 4, 1],
      [3_i16, 2_i16, 3_i16, 0_i16, 1_i16, 3_i16, 3_i16, 1_i16, 1_i16, 0_i16, 1_i16, 2_i16, 3_i16],
      [0_i16, 1_i16, 1_i16, 0_i16, 0_i16, 0_i16, 1_i16, 0_i16, 0_i16, 0_i16, 0_i16, 0_i16, 1_i16],
      3_i16, 1_i16, 2_i16
    ),
    list_view_type(O), 2_i16, 1_i16, true, nil
  )

  reader.next_batch(1024).should eq(list_view_array(items, O))
end

private def required_list_view(offset_type : O.class) : Nil forall O
  # [[1, null, 2], [], [3, 4], [], [], [null, 1]]
  items = Array(Array(Int32?)?).new
  items << ([1, nil, 2] of Int32?)
  items << ([] of Int32?)
  items << ([3, 4] of Int32?)
  items << ([] of Int32?)
  items << ([] of Int32?)
  items << ([nil, 1] of Int32?)

  reader = PArrow::ListViewArrayReader(O).new(
    int32_reader(
      [1, 2, 3, 4, 1],
      [2_i16, 1_i16, 2_i16, 0_i16, 2_i16, 2_i16, 0_i16, 0_i16, 1_i16, 2_i16],
      [0_i16, 1_i16, 1_i16, 0_i16, 0_i16, 1_i16, 0_i16, 0_i16, 0_i16, 1_i16],
      2_i16, 1_i16, 1_i16
    ),
    list_view_type(O), 1_i16, 1_i16, false, nil
  )

  reader.next_batch(1024).should eq(list_view_array(items, O))
end

describe Parquet::Arrow::ListViewArrayReader do
  it "reads nullable list views" do
    nullable_list_view(Int32)
    nullable_list_view(Int64)
  end

  it "reads required list views" do
    required_list_view(Int32)
    required_list_view(Int64)
  end

  it "exposes the list-view data type and forwards levels" do
    reader = PArrow::ListViewArrayReader(Int32).new(
      int32_reader([1, 2], [2_i16, 2_i16], [0_i16, 1_i16], 2_i16, 1_i16, 1_i16),
      list_view_type(Int32), 1_i16, 1_i16, true, nil
    )

    reader.data_type.should eq(list_view_type(Int32))
    reader.max_def_level.should eq(1)
    array = reader.next_batch(2).as(Arrow::GenericListViewArray(Int32))
    array.len.should eq(1)
    reader.def_levels.should eq([2_i16, 2_i16])
    reader.rep_levels.should eq([0_i16, 1_i16])
  end

  it "returns an empty list-view array for an empty batch" do
    reader = PArrow::ListViewArrayReader(Int32).new(
      int32_reader([] of Int32, [] of Int16, [] of Int16, 2_i16, 1_i16, 1_i16),
      list_view_type(Int32), 1_i16, 1_i16, true, nil
    )

    array = reader.next_batch(0).as(Arrow::GenericListViewArray(Int32))
    array.len.should eq(0)
    array.data_type.should eq(list_view_type(Int32))
  end

  it "skips records through the list reader" do
    reader = PArrow::ListViewArrayReader(Int32).new(
      int32_reader(
        [1, 2, 3], [2_i16, 2_i16, 2_i16], [0_i16, 0_i16, 0_i16], 2_i16, 1_i16, 1_i16
      ),
      list_view_type(Int32), 1_i16, 1_i16, true, nil
    )

    reader.skip_records(2).should eq(2)
    array = reader.next_batch(1).as(Arrow::GenericListViewArray(Int32))
    array.len.should eq(1)
    array.value(0).as(Arrow::PrimitiveArray(Int32)).values[0].should eq(3)
  end

  it "rejects a non-list-view data type" do
    expect_raises(Parquet::ParquetError, /expected list view type/) do
      PArrow::ListViewArrayReader(Int32).new(
        int32_reader([1], [2_i16], [0_i16], 2_i16, 1_i16, 1_i16),
        Arrow::DataType.list(Arrow::Field.new_list_field(Arrow::DataType.int32, true)),
        1_i16, 1_i16, true, nil
      )
    end
  end
end
