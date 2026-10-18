require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/list_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# `ListArrayReader` for `List` (Int32 offsets) and `LargeList` (Int64 offsets).
#
# The upstream tests build their expected arrays with
# `GenericListArray::from_iter_primitive`/`ArrayDataBuilder`; the port does the
# same through arrow-cr's builders, so the assertions stay upstream-faithful.
# `test_list_multi_row_group_selective_padding` (a two-page row-group test) is
# not ported yet — see `plans/parity.md`.
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

# Upstream `to_offsets`: an offsets buffer of the reader's offset type.
private def offsets_buffer(values : Array(Int32), offset_type : O.class) : Arrow::Buffer forall O
  {% if O == Int64 %}
    Arrow::Buffer.from_iter(values.map(&.to_i64))
  {% else %}
    Arrow::Buffer.from_iter(values.map(&.to_i32))
  {% end %}
end

# Upstream `list_type`: a list data type over `data_type` with the given item
# nullability.
private def list_type(item_type : Arrow::DataType, item_nullable : Bool,
                      offset_type : O.class) : Arrow::DataType forall O
  field = Arrow::Field.new_list_field(item_type, item_nullable)
  {% if O == Int64 %}
    Arrow::DataType.large_list(field)
  {% else %}
    Arrow::DataType.list(field)
  {% end %}
end

private def nullable_list(offset_type : O.class) : Nil forall O
  # [[1, null, 2], null, [], [3, 4], [], [], null, [], [null, 1]]
  expected_values = Array(Array(Int32?)?).new
  expected_values << ([1, nil, 2] of Int32?)
  expected_values << nil
  expected_values << [] of Int32?
  expected_values << ([3, 4] of Int32?)
  expected_values << [] of Int32?
  expected_values << [] of Int32?
  expected_values << nil
  expected_values << [] of Int32?
  expected_values << ([nil, 1] of Int32?)
  expected = Arrow::GenericListArray(O).from_iter_primitive(Int32, expected_values)

  item_reader = int32_reader(
    [1, 2, 3, 4, 1],
    [3_i16, 2_i16, 3_i16, 0_i16, 1_i16, 3_i16, 3_i16, 1_i16, 1_i16, 0_i16, 1_i16, 2_i16, 3_i16],
    [0_i16, 1_i16, 1_i16, 0_i16, 0_i16, 0_i16, 1_i16, 0_i16, 0_i16, 0_i16, 0_i16, 0_i16, 1_i16],
    3_i16, 1_i16, 2_i16
  )

  reader = PArrow::ListArrayReader(O).new(
    item_reader, list_type(Arrow::DataType.int32, true, O), 2_i16, 1_i16, true, nil
  )

  actual = reader.next_batch(1024)
  actual.should eq(expected)
end

private def required_list(offset_type : O.class) : Nil forall O
  # [[1, null, 2], [], [3, 4], [], [], [null, 1]]
  expected_values = Array(Array(Int32?)?).new
  expected_values << ([1, nil, 2] of Int32?)
  expected_values << [] of Int32?
  expected_values << ([3, 4] of Int32?)
  expected_values << [] of Int32?
  expected_values << [] of Int32?
  expected_values << ([nil, 1] of Int32?)
  expected = Arrow::GenericListArray(O).from_iter_primitive(Int32, expected_values)

  item_reader = int32_reader(
    [1, 2, 3, 4, 1],
    [2_i16, 1_i16, 2_i16, 0_i16, 2_i16, 2_i16, 0_i16, 0_i16, 1_i16, 2_i16],
    [0_i16, 1_i16, 1_i16, 0_i16, 0_i16, 1_i16, 0_i16, 0_i16, 0_i16, 1_i16],
    2_i16, 1_i16, 1_i16
  )

  reader = PArrow::ListArrayReader(O).new(
    item_reader, list_type(Arrow::DataType.int32, true, O), 1_i16, 1_i16, false, nil
  )

  reader.next_batch(1024).should eq(expected)
end

private def nested_list(offset_type : O.class) : Nil forall O
  # [
  #     [[1, null], null, [4], []],
  #     [],
  #     [[7]],
  #     [[]],
  #     [[1, 2, 3], [4, null, 6], null],
  #     [[11]]
  # ]
  l3_type = list_type(Arrow::DataType.int32, true, O)
  l2_type = list_type(l3_type, true, O)
  l1_type = list_type(l2_type, false, O)

  leaf = Arrow::PrimitiveArray(Int32).new(
    Arrow::ScalarBuffer(Int32).from([1, 0, 4, 7, 1, 2, 3, 4, 0, 6, 11]),
    Arrow::NullBuffer.from_slice([true, false, true, true, true, true, true, true, false, true, true])
  )

  l3 = Arrow::ArrayData.builder(l3_type)
    .len(10)
    .add_buffer(offsets_buffer([0, 2, 2, 3, 3, 4, 4, 7, 10, 10, 11], O))
    .add_child_data(leaf.to_data)
    .null_bit_buffer(Arrow::Buffer.from_slice_ref(Bytes[0b1111_1101, 0b0000_0010]))
    .build

  l2 = Arrow::ArrayData.builder(l2_type)
    .len(6)
    .add_buffer(offsets_buffer([0, 4, 4, 5, 6, 9, 10], O))
    .add_child_data(l3)
    .build

  l1 = Arrow::ArrayData.builder(l1_type)
    .len(4)
    .add_buffer(offsets_buffer([0, 5, 5, 5, 6], O))
    .add_child_data(l2)
    .null_bit_buffer(Arrow::Buffer.from_slice_ref(Bytes[0b0000_1101]))
    .build

  expected = Arrow::GenericListArray(O).from(l1)

  item_reader = int32_reader(
    [1, 4, 7, 1, 2, 3, 4, 6, 11],
    [6_i16, 5_i16, 3_i16, 6_i16, 4_i16, 2_i16, 6_i16, 4_i16, 6_i16, 6_i16, 6_i16, 6_i16, 5_i16, 6_i16, 3_i16, 0_i16, 1_i16, 6_i16],
    [0_i16, 3_i16, 2_i16, 2_i16, 2_i16, 1_i16, 1_i16, 1_i16, 1_i16, 3_i16, 3_i16, 2_i16, 3_i16, 3_i16, 2_i16, 0_i16, 0_i16, 0_i16],
    6_i16, 3_i16, 5_i16
  )

  l3_reader = PArrow::ListArrayReader(O).new(item_reader, l3_type, 5_i16, 3_i16, true, 3_i16)
  l2_reader = PArrow::ListArrayReader(O).new(l3_reader, l2_type, 3_i16, 2_i16, false, 2_i16)
  l1_reader = PArrow::ListArrayReader(O).new(l2_reader, l1_type, 2_i16, 1_i16, true, nil)

  l1_reader.next_batch(2).should eq(expected.slice(0, 2))
  l1_reader.next_batch(1024).should eq(expected.slice(2, 2))
end

describe Parquet::Arrow::ListArrayReader do
  it "reads nullable lists" do
    nullable_list(Int32)
    nullable_list(Int64)
  end

  it "reads required lists" do
    required_list(Int32)
    required_list(Int64)
  end

  it "reads nested lists" do
    nested_list(Int32)
    nested_list(Int64)
  end

  it "exposes the list data type, levels and max def level" do
    item_reader = int32_reader([1, 2], [2_i16, 2_i16], [0_i16, 1_i16], 2_i16, 1_i16, 1_i16)
    reader = PArrow::ListArrayReader(Int32).new(
      item_reader, list_type(Arrow::DataType.int32, true, Int32), 1_i16, 1_i16, true, nil
    )

    reader.data_type.should eq(list_type(Arrow::DataType.int32, true, Int32))
    reader.max_def_level.should eq(1)
    reader.next_batch(2)
    reader.def_levels.should eq([2_i16, 2_i16])
    reader.rep_levels.should eq([0_i16, 1_i16])
  end

  it "skips records through the item reader" do
    # Three single-item lists (one record per list).
    item_reader = int32_reader(
      [1, 2, 3], [2_i16, 2_i16, 2_i16], [0_i16, 0_i16, 0_i16], 2_i16, 1_i16, 1_i16
    )
    reader = PArrow::ListArrayReader(Int32).new(
      item_reader, list_type(Arrow::DataType.int32, true, Int32), 1_i16, 1_i16, true, nil
    )

    reader.skip_records(2).should eq(2)
    reader.read_records(1).should eq(1)
    array = reader.consume_batch.as(Arrow::GenericListArray(Int32))
    array.len.should eq(1)
    array.value(0).as(Arrow::PrimitiveArray(Int32)).values[0].should eq(3)
  end

  it "returns an empty list array when there are no definition levels" do
    item_reader = int32_reader([] of Int32, [] of Int16, [] of Int16, 2_i16, 1_i16, 1_i16)
    reader = PArrow::ListArrayReader(Int32).new(
      item_reader, list_type(Arrow::DataType.int32, true, Int32), 1_i16, 1_i16, true, nil
    )

    array = reader.next_batch(0).as(Arrow::GenericListArray(Int32))
    array.len.should eq(0)
    array.data_type.should eq(list_type(Arrow::DataType.int32, true, Int32))
  end

  it "rejects batches that do not start on a record boundary" do
    item_reader = int32_reader([1], [2_i16], [1_i16], 2_i16, 1_i16, 1_i16)
    reader = PArrow::ListArrayReader(Int32).new(
      item_reader, list_type(Arrow::DataType.int32, true, Int32), 1_i16, 1_i16, true, nil
    )

    expect_raises(Parquet::ParquetError, /first repetition level of batch must be 0/) do
      reader.next_batch(1)
    end
  end

  it "rejects level data whose child value count does not match" do
    # The second entry continues a nested list (rep 2 > this list's rep level),
    # so it is not a direct child but the child array still holds its value.
    item_reader = int32_reader([1, 2], [2_i16, 2_i16], [0_i16, 2_i16], 2_i16, 2_i16, 1_i16)
    reader = PArrow::ListArrayReader(Int32).new(
      item_reader, list_type(Arrow::DataType.int32, true, Int32), 1_i16, 1_i16, true, nil
    )

    expect_raises(Parquet::ParquetError, /Failed to reconstruct list from level data/) do
      reader.next_batch(2)
    end
  end
end
