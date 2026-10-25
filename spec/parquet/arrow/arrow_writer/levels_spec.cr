require "../../../spec_helper"

# Upstream parquet/src/arrow/arrow_writer/levels.rs, arrow-rs 60.0.0
# (ef1fa157977633f0ba21aa921f9ef3d5c669235b).
private alias AW = Parquet::Arrow
private alias DT = Arrow::DataType

private def check_writer_levels(array : Arrow::ArrayBase, field : Arrow::Field, defs : Array(Int16)?, reps : Array(Int16)?, indices : Array(Int32), max_def : Int16, max_rep : Int16) : Nil
  levels = AW.calculate_array_levels(array, field)
  levels.size.should eq 1
  leaf = levels[0]
  leaf.def_levels.should eq defs
  leaf.rep_levels.should eq reps
  leaf.non_null_indices.should eq indices
  leaf.max_def_level.should eq max_def
  leaf.max_rep_level.should eq max_rep
end

describe "Arrow writer levels" do
  it "calculates required primitive levels (test_calculate_one_level_1)" do
    array = Arrow::PrimitiveArray(Int32).from_values((0...10).to_a)
    check_writer_levels(array, Arrow::Field.new("item", DT.int32, false), nil, nil, (0...10).to_a, 0_i16, 0_i16)
  end

  it "calculates optional primitive levels (test_calculate_one_level_2)" do
    array = Arrow::PrimitiveArray(Int32).from_nullable([0, nil, 0, 0, nil])
    check_writer_levels(array, Arrow::Field.new("item", DT.int32, true), [1, 0, 1, 1, 0].map(&.to_i16), nil, [0, 2, 3], 1_i16, 0_i16)
  end

  it "calculates the nested Twitter example" do
    values = Arrow::PrimitiveArray(Int32).from_values((0...10).to_a)
    inner_field = Arrow::Field.new("item", DT.int32, false)
    inner = Arrow::GenericListArray(Int32).new(inner_field, Arrow::OffsetBuffer(Int32).new(Arrow::ScalarBuffer(Int32).from([0, 3, 7, 8, 10])), values)
    outer_field = Arrow::Field.new("l2", inner.data_type, false)
    outer = Arrow::GenericListArray(Int32).new(outer_field, Arrow::OffsetBuffer(Int32).new(Arrow::ScalarBuffer(Int32).from([0, 2, 4])), inner)
    check_writer_levels(outer, Arrow::Field.new("l1", outer.data_type, false), Array.new(10, 2_i16), [0, 2, 2, 1, 2, 2, 2, 0, 1, 2].map(&.to_i16), (0...10).to_a, 2_i16, 2_i16)
  end

  {% for offset in [Int32, Int64] %}
    it "distinguishes null lists, empty lists and null items with {{offset}} offsets" do
      array = Arrow::GenericListArray({{offset}}).from_iter_primitive(Int32, [[1, nil, 2], nil, [] of Int32?, ([3, 4] of Int32?), [] of Int32?, [] of Int32?, nil, [] of Int32?, [nil, 1]] of Array(Int32?)?)
      check_writer_levels(array, Arrow::Field.new("list", array.data_type, true), [3, 2, 3, 0, 1, 3, 3, 1, 1, 0, 1, 2, 3].map(&.to_i16), [0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1].map(&.to_i16), [0, 2, 3, 4, 6], 3_i16, 1_i16)
    end
  {% end %}

  it "uses dictionary logical nulls (test_null_dictionary_values)" do
    values = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 4])
    keys = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 2, 0])
    array = Arrow::DictionaryArray(Int32).try_new(keys, values)
    check_writer_levels(array, Arrow::Field.new("item", array.data_type, true), [0, 0, 1, 1].map(&.to_i16), nil, [2, 3], 1_i16, 0_i16)
  end

  it "rejects mismatched field and array types" do
    array = Arrow::PrimitiveArray(Int32).from_values([1])
    expect_raises(Parquet::ArrowError, "Incompatible type. Field 'item'") do
      AW.calculate_array_levels(array, Arrow::Field.new("item", DT.float64, false))
    end
  end

  it "slices CDC levels and rebases indices" do
    array = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, nil, 5, 6])
    levels = AW.calculate_array_levels(array, Arrow::Field.new("item", DT.int32, true))[0]
    sliced = levels.slice_for_chunk(Parquet::Column::Chunker::CdcChunk.new(1, 3, 1, 1))
    sliced.def_levels.should eq [0_i16, 1_i16, 0_i16]
    sliced.rep_levels.should be_nil
    sliced.non_null_indices.should eq [0]
    sliced.array.len.should eq 1
  end
end
