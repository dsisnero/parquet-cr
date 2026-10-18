require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/map_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# `MapArrayReader` (a `ListArrayReader` over a two-field `StructArrayReader`).
#
# The upstream test (`read_map_array_column`) round-trips a map column through
# `ArrowWriter` + `ParquetRecordBatchReader`, neither of which is ported yet; the
# port characterizes the reader directly instead, using the same data shape
# (two null maps, then one map with five string -> int32 entries) built from
# level data.
private alias PArrow = Parquet::Arrow
private alias PageUtil = ParquetTestPageUtil
private alias ARUtil = ParquetTestArrayReaderUtil

# OPTIONAL map of STRING -> INT32: map def 1, repeated key_value def 2 (the list
# level), required key def 2, optional value def 3.
private MAP_KEY_COLUMN = "message test_schema { OPTIONAL GROUP m (MAP) { REPEATED GROUP key_value { REQUIRED BINARY key (STRING); } } }"

private def map_key_reader(values : Array(String), def_levels : Array(Int16),
                           rep_levels : Array(Int16)) : PArrow::ArrayReader
  desc = PageUtil.column_descriptor(MAP_KEY_COLUMN)
  builder = PageUtil::DataPageBuilderImpl.new(desc, def_levels.size.to_u32, true)
  builder.add_rep_levels(desc.max_rep_level, rep_levels)
  builder.add_def_levels(desc.max_def_level, def_levels)
  builder.add_values(
    Parquet::Encoding::PLAIN,
    values.map { |value| Parquet::ByteArray.new(value) }
  )
  pages = PageUtil::InMemoryPageIterator.new([[builder.consume]])
  PArrow.make_byte_array_reader(pages, desc, nil, 1024, 2_i16)
end

private def map_value_reader(values : Array(Int32), def_levels : Array(Int16),
                             rep_levels : Array(Int16)) : PArrow::ArrayReader
  ARUtil.make_int32_page_reader(
    values: values,
    def_levels: def_levels,
    rep_levels: rep_levels,
    max_def_level: 3_i16,
    max_rep_level: 1_i16,
    padding_threshold: 2_i16
  )
end

# The arrow map type: `Map(entries: Struct<key: Utf8 not null, value: Int32>)`,
# with a non-nullable entries field as required by the reader.
private def map_type : Arrow::DataType
  entries = Arrow::DataType.struct(
    Arrow::Fields.new([
      Arrow::Field.new("key", Arrow::DataType.utf8, false),
      Arrow::Field.new("value", Arrow::DataType.int32, true),
    ])
  )
  Arrow::DataType.map(Arrow::Field.new("entries", entries, false), false)
end

private def map_reader(keys : PArrow::ArrayReader, values : PArrow::ArrayReader,
                       data_type : Arrow::DataType = map_type,
                       def_level : Int16 = 2_i16, rep_level : Int16 = 1_i16,
                       nullable : Bool = true,
                       parent_threshold : Int16? = nil) : PArrow::MapArrayReader
  PArrow::MapArrayReader.new(
    keys, values, data_type, def_level, rep_level, nullable, parent_threshold
  )
end

describe Parquet::Arrow::MapArrayReader do
  it "reads a map column with null and non-null maps" do
    # null, null, {three -> 3, four -> 4, five -> 5, six -> 6, seven -> 7}
    key_defs = [0_i16, 0_i16, 2_i16, 2_i16, 2_i16, 2_i16, 2_i16]
    value_defs = [0_i16, 0_i16, 3_i16, 3_i16, 3_i16, 3_i16, 3_i16]
    reps = [0_i16, 0_i16, 0_i16, 1_i16, 1_i16, 1_i16, 1_i16]

    reader = map_reader(
      map_key_reader(%w[three four five six seven], key_defs, reps),
      map_value_reader([3, 4, 5, 6, 7], value_defs, reps)
    )

    reader.data_type.should eq(map_type)
    array = reader.next_batch(3).as(Arrow::MapArray)

    array.len.should eq(3)
    array.null?(0).should be_true
    array.null?(1).should be_true
    array.null?(2).should be_false

    entry = array.value(2)
    keys = entry.column(0).as(Arrow::GenericStringArray(Int32))
    values = entry.column(1).as(Arrow::PrimitiveArray(Int32))
    Array(String?).new(keys.len) { |idx| keys.value(idx) }
      .should eq(%w[three four five six seven])
    Array(Int32?).new(values.len) { |idx| values.value(idx) }.should eq([3, 4, 5, 6, 7])
  end

  it "exposes the map keys and values helpers" do
    key_defs = [0_i16, 2_i16]
    value_defs = [0_i16, 3_i16]
    reps = [0_i16, 0_i16]

    reader = map_reader(
      map_key_reader(%w[one], key_defs, reps),
      map_value_reader([1], value_defs, reps)
    )

    array = reader.next_batch(2).as(Arrow::MapArray)
    array.len.should eq(2)
    array.keys.as(Arrow::GenericStringArray(Int32)).value(0).should eq("one")
    array.values.as(Arrow::PrimitiveArray(Int32)).values[0].should eq(1)
    array.value_length(1).should eq(1)
    array.value_length(0).should eq(0)
  end

  it "forwards levels and the max def level to the list reader" do
    key_defs = [0_i16, 2_i16, 2_i16]
    value_defs = [0_i16, 3_i16, 3_i16]
    reps = [0_i16, 0_i16, 1_i16]

    reader = map_reader(
      map_key_reader(%w[a b], key_defs, reps),
      map_value_reader([1, 2], value_defs, reps)
    )

    reader.max_def_level.should eq(2)
    reader.next_batch(2)
    reader.def_levels.should eq(key_defs)
    reader.rep_levels.should eq(reps)
  end

  it "skips records" do
    # Three single-entry maps. Skipping is only well defined when every level
    # carries a value (the column reader skips levels, not null slots).
    key_defs = [2_i16, 2_i16, 2_i16]
    value_defs = [3_i16, 3_i16, 3_i16]
    reps = [0_i16, 0_i16, 0_i16]

    reader = map_reader(
      map_key_reader(%w[a b c], key_defs, reps),
      map_value_reader([1, 2, 3], value_defs, reps)
    )

    reader.skip_records(2).should eq(2)
    array = reader.next_batch(1).as(Arrow::MapArray)
    array.len.should eq(1)
    array.value(0).column(0).as(Arrow::GenericStringArray(Int32)).value(0).should eq("c")
    array.values.as(Arrow::PrimitiveArray(Int32)).values[0].should eq(3)
  end

  it "rejects a nullable entries field" do
    entries = Arrow::DataType.struct(
      Arrow::Fields.new([
        Arrow::Field.new("key", Arrow::DataType.utf8, false),
        Arrow::Field.new("value", Arrow::DataType.int32, true),
      ])
    )
    nullable_map = Arrow::DataType.map(Arrow::Field.new("entries", entries, true), false)

    expect_raises(Parquet::ParquetError, /map struct cannot be nullable/) do
      map_reader(
        map_key_reader(%w[a], [2_i16], [0_i16]),
        map_value_reader([1], [3_i16], [0_i16]),
        data_type: nullable_map
      )
    end
  end

  it "rejects a non-map data type" do
    expect_raises(Parquet::ParquetError, /expected map type/) do
      map_reader(
        map_key_reader(%w[a], [2_i16], [0_i16]),
        map_value_reader([1], [3_i16], [0_i16]),
        data_type: Arrow::DataType.list(Arrow::Field.new_list_field(Arrow::DataType.int32, true))
      )
    end
  end
end
