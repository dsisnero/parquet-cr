require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/struct_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# `StructArrayReader`.
#
# The upstream `test_struct_array_reader_list` test needs `ListArrayReader`,
# which is not ported yet; its selective-padding behaviour is covered here with
# a flat child reader instead.
private alias PArrow = Parquet::Arrow
private alias ARUtil = ParquetTestArrayReaderUtil

private def int32_reader(values : Array(Int32), def_levels : Array(Int16),
                         rep_levels : Array(Int16), max_def_level : Int16,
                         max_rep_level : Int16,
                         padding_threshold : Int16? = nil) : PArrow::ArrayReader
  ARUtil.make_int32_page_reader(
    values: values,
    def_levels: def_levels,
    rep_levels: rep_levels,
    max_def_level: max_def_level,
    max_rep_level: max_rep_level,
    padding_threshold: padding_threshold
  )
end

private def struct_type(*names : String) : Arrow::DataType
  fields = Arrow::Fields.new(
    names.to_a.map { |name| Arrow::Field.new(name, Arrow::DataType.int32, true) }
  )
  Arrow::DataType.struct(fields)
end

private def struct_reader(children : Array(PArrow::ArrayReader),
                          def_level : Int16 = 1_i16,
                          rep_level : Int16 = 1_i16,
                          nullable : Bool = true,
                          padding_threshold : Int16? = nil,
                          data_type : Arrow::DataType = struct_type("f1", "f2")) : PArrow::StructArrayReader
  PArrow::StructArrayReader.new(
    data_type, children, def_level, rep_level, nullable, padding_threshold
  )
end

describe Parquet::Arrow::StructArrayReader do
  it "reads structs from int32 children" do
    reader = struct_reader([
      int32_reader([4], [0_i16, 1_i16, 2_i16, 3_i16, 1_i16], [0_i16, 1_i16, 1_i16, 1_i16, 1_i16], 3_i16, 1_i16),
      int32_reader([3], [0_i16, 1_i16, 3_i16, 1_i16, 2_i16], [0_i16, 1_i16, 1_i16, 1_i16, 1_i16], 3_i16, 1_i16),
    ])

    reader.data_type.should eq(struct_type("f1", "f2"))
    array = reader.next_batch(5).as(Arrow::StructArray)

    array.len.should eq(5)
    Array.new(5) { |idx| array.null?(idx) }.should eq([true, false, false, false, false])
    reader.def_levels.should eq([0_i16, 1_i16, 2_i16, 3_i16, 1_i16])
    reader.rep_levels.should eq([0_i16, 1_i16, 1_i16, 1_i16, 1_i16])
    array.column(0).as(Arrow::PrimitiveArray(Int32)).len.should eq(5)
    array.column(1).as(Arrow::PrimitiveArray(Int32)).len.should eq(5)
  end

  it "returns the struct definition level" do
    reader = struct_reader(
      [int32_reader([1], [0_i16, 3_i16], [0_i16, 0_i16], 3_i16, 0_i16)],
      def_level: 3_i16, rep_level: 0_i16, data_type: struct_type("f1")
    )
    reader.max_def_level.should eq(3)
  end

  it "has no null bitmap when the struct is not nullable" do
    reader = struct_reader([
      int32_reader([1, 2], [3_i16, 3_i16], [] of Int16, 3_i16, 0_i16),
    ], def_level: 3_i16, rep_level: 0_i16, nullable: false, data_type: struct_type("f1"))

    array = reader.next_batch(2).as(Arrow::StructArray)
    array.len.should eq(2)
    array.nulls.should be_nil
  end

  it "reads empty structs when there are no children" do
    reader = struct_reader([] of PArrow::ArrayReader)
    array = reader.next_batch(3).as(Arrow::StructArray)
    array.len.should eq(0)
    array.num_columns.should eq(0)
  end

  it "skips records on every child" do
    reader = struct_reader([
      int32_reader([1, 2, 3], [3_i16, 3_i16, 3_i16], [] of Int16, 3_i16, 0_i16),
      int32_reader([4, 5, 6], [3_i16, 3_i16, 3_i16], [] of Int16, 3_i16, 0_i16),
    ], def_level: 3_i16, rep_level: 0_i16)

    reader.skip_records(2).should eq(2)
    array = reader.next_batch(1).as(Arrow::StructArray)
    array.len.should eq(1)
    array.column(0).as(Arrow::PrimitiveArray(Int32)).values[0].should eq(3)
    array.column(1).as(Arrow::PrimitiveArray(Int32)).values[0].should eq(6)
  end

  it "rejects out-of-sync children on read" do
    reader = struct_reader([
      int32_reader([1, 2, 3], [3_i16, 3_i16, 3_i16], [] of Int16, 3_i16, 0_i16),
      int32_reader([4, 5], [3_i16, 3_i16], [] of Int16, 3_i16, 0_i16),
    ], def_level: 3_i16, rep_level: 0_i16)

    expect_raises(Parquet::ParquetError, /out of sync in read_records/) do
      reader.read_records(3)
    end
  end

  it "rejects out-of-sync children on skip" do
    reader = struct_reader([
      int32_reader([1, 2, 3], [3_i16, 3_i16, 3_i16], [] of Int16, 3_i16, 0_i16),
      int32_reader([4, 5], [3_i16, 3_i16], [] of Int16, 3_i16, 0_i16),
    ], def_level: 3_i16, rep_level: 0_i16)

    expect_raises(Parquet::ParquetError, /out of sync/) do
      reader.skip_records(3)
    end
  end

  it "drops parent-level entries with selective padding" do
    # A struct inside a list: def 4 is a value, def 3 a null value, def 2/1/0
    # are parent-level padding entries that the threshold (3) excludes.
    reader = struct_reader([
      int32_reader([1, 2], [4_i16, 4_i16, 3_i16, 2_i16, 1_i16, 0_i16],
        [0_i16, 1_i16, 1_i16, 0_i16, 0_i16, 0_i16], 4_i16, 1_i16, padding_threshold: 3_i16),
    ], def_level: 4_i16, rep_level: 1_i16, padding_threshold: 3_i16, data_type: struct_type("f1"))

    array = reader.next_batch(6).as(Arrow::StructArray)
    array.len.should eq(3)
    Array.new(3) { |idx| array.null?(idx) }.should eq([false, false, true])
  end
end
