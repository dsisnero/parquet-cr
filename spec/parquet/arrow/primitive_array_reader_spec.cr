require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/primitive_array.rs`
# (`PrimitiveArrayReader`, `coerce_array`-free subset) and the
# `read_records`/`skip_records` helpers in
# `parquet/src/arrow/array_reader/mod.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the INT32/INT64/FLOAT/DOUBLE primitive paths (BOOLEAN and the byte
# arrays have their own readers upstream; `coerce_array` coercion to
# dictionary/temporal targets is deferred with the arrow cast layer).
private alias PArrow = Parquet::Arrow
private alias AR = PArrow::ArrayReader
private alias PageUtil = ParquetTestPageUtil

private def page_of(pages : Array(Parquet::Column::Page)) : Array(Parquet::Column::Page)
  pages
end

private def int32_array_reader(pages : Array(Array(Parquet::Column::Page)),
                               message_type : String = "message test_schema { OPTIONAL INT32 leaf; }",
                               arrow_type : Arrow::DataType? = nil,
                               batch_size : Int32 = 1024,
                               padding_threshold : Int16? = nil) : PArrow::PrimitiveArrayReader(Int32)
  desc = PageUtil.column_descriptor(message_type)
  PArrow::PrimitiveArrayReader(Int32).new(
    PageUtil::InMemoryPageIterator.new(pages),
    desc,
    arrow_type,
    batch_size,
    padding_threshold
  )
end

private def int32_pages(values : Array(Int32), def_levels : Array(Int16)? = nil,
                        rep_levels : Array(Int16)? = nil, v2 : Bool = true,
                        message_type : String = "message test_schema { OPTIONAL INT32 leaf; }") : Array(Parquet::Column::Page)
  desc = PageUtil.column_descriptor(message_type)
  num_values = def_levels.try(&.size) || rep_levels.try(&.size) || values.size
  builder = PageUtil::DataPageBuilderImpl.new(desc, num_values.to_u32, v2)
  builder.add_rep_levels(desc.max_rep_level, rep_levels.not_nil!) if rep_levels
  builder.add_def_levels(desc.max_def_level, def_levels.not_nil!) if def_levels
  builder.add_values(Parquet::Encoding::PLAIN, values)
  [builder.consume]
end

private def boolean_array_reader(pages : Array(Array(Parquet::Column::Page)),
                                 message_type : String = "message test_schema { OPTIONAL BOOLEAN leaf; }",
                                 arrow_type : Arrow::DataType? = nil,
                                 batch_size : Int32 = 1024) : Parquet::Arrow::PrimitiveArrayReader(Bool)
  desc = PageUtil.column_descriptor(message_type)
  Parquet::Arrow::PrimitiveArrayReader(Bool).new(
    PageUtil::InMemoryPageIterator.new(pages), desc, arrow_type, batch_size, nil
  )
end

describe Parquet::Arrow::PrimitiveArrayReader do
  # Upstream `Vec<bool>.into_buffer` + `BooleanArray` (BOOLEAN leaf reader).
  it "reads boolean data with nulls" do
    message_type = "message test_schema { OPTIONAL BOOLEAN leaf; }"
    desc = PageUtil.column_descriptor(message_type)
    builder = PageUtil::DataPageBuilderImpl.new(desc, 4_u32, true)
    builder.add_def_levels(1_i16, [1_i16, 0_i16, 1_i16, 1_i16])
    builder.add_values(Parquet::Encoding::PLAIN, [true, false, true])
    reader = boolean_array_reader([page_of([builder.consume])], message_type: message_type)

    reader.data_type.should eq(Arrow::DataType.boolean)
    reader.read_records(4).should eq(4)

    array = reader.consume_batch.as(Arrow::BooleanArray)
    array.len.should eq(4)
    array.null_count.should eq(1)
    array.null?(1).should be_true
    array.value(0).should be_true
    array.value(2).should be_false
    array.value(3).should be_true
  end

  # Upstream `test_primitive_array_reader_data` (INT32 path).
  it "reads optional int32 data" do
    reader = int32_array_reader([int32_pages([1, 2, 3, 4], [1_i16, 0_i16, 1_i16, 1_i16])])

    reader.data_type.should eq(Arrow::DataType.int32)
    reader.max_def_level.should eq(1)
    reader.read_records(4).should eq(4)

    batch = reader.consume_batch
    batch.len.should eq(4)
    array = batch.as(Arrow::PrimitiveArray(Int32))
    array.null_count.should eq(1)
    array.null?(1).should be_true
    # Null slots keep stale values (as upstream); only valid slots are defined.
    [0, 2, 3].each { |i| array.values[i].should eq([1, 2, 3][[0, 2, 3].index(i).not_nil!]) }
  end

  it "reads required int32 data across pages" do
    message_type = "message test_schema { REQUIRED INT32 leaf; }"
    reader = int32_array_reader(
      [int32_pages([1, 2, 3], message_type: message_type), int32_pages([4, 5], message_type: message_type)],
      message_type: message_type
    )

    reader.read_records(10).should eq(5)
    array = reader.consume_batch.as(Arrow::PrimitiveArray(Int32))
    array.values.to_a.should eq([1, 2, 3, 4, 5])
    array.null_count.should eq(0)
  end

  # Upstream `test_primitive_array_reader_empty_pages`.
  it "handles empty page iterators" do
    reader = int32_array_reader([] of Array(Parquet::Column::Page))
    reader.read_records(10).should eq(0)
    reader.consume_batch.len.should eq(0)
  end

  # Upstream `test_primitive_array_reader_def_and_rep_levels`.
  it "exposes definition and repetition levels" do
    message_type = "message test_schema { REPEATED Group test_struct { REPEATED INT32 leaf; } }"
    reader = int32_array_reader(
      [int32_pages(
        [4, 7, 6, 3, 2],
        def_levels: [2_i16, 0_i16, 1_i16, 2_i16, 2_i16, 2_i16, 2_i16],
        rep_levels: [0_i16, 0_i16, 0_i16, 1_i16, 2_i16, 2_i16, 1_i16],
        message_type: message_type
      )],
      message_type: message_type
    )

    # Three complete records in this page (the last group has no page-boundary
    # marker); the consumed batch has one slot per level (full padding).
    array = reader.next_batch(10)
    array.len.should eq(7)
    reader.def_levels.should eq([2_i16, 0_i16, 1_i16, 2_i16, 2_i16, 2_i16, 2_i16])
    reader.rep_levels.should eq([0_i16, 0_i16, 0_i16, 1_i16, 2_i16, 2_i16, 1_i16])
  end

  # Upstream `ArrayReader::skip_records` over pages.
  it "skips records across pages" do
    message_type = "message test_schema { REQUIRED INT32 leaf; }"
    reader = int32_array_reader(
      [int32_pages([1, 2, 3], message_type: message_type), int32_pages([4, 5], message_type: message_type)],
      message_type: message_type
    )

    reader.skip_records(2).should eq(2)
    reader.read_records(10).should eq(3)
    reader.consume_batch.as(Arrow::PrimitiveArray(Int32)).values.to_a.should eq([3, 4, 5])
  end
end
