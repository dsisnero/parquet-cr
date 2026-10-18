require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/null_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# `NullArrayReader`, which reads an all-null parquet column (`LogicalType::Unknown`
# → `DataType::Null`) as an `Arrow::NullArray`.
#
# Divergences: like upstream, the underlying record reader is typed on INT32
# (a Null column carries no non-null values, so only the level data is used),
# and `usize` counts are `Int32`.
private alias PArrow = Parquet::Arrow
private alias PageUtil = ParquetTestPageUtil
private alias BytesCR = Parquet::File::BytesChunkReader

private NULL_COLUMN = "message test_schema { OPTIONAL INT32 leaf; }"

private def null_pages(def_levels : Array(Int16)) : Array(Parquet::Column::Page)
  desc = PageUtil.column_descriptor(NULL_COLUMN)
  builder = PageUtil::DataPageBuilderImpl.new(desc, def_levels.size.to_u32, true)
  builder.add_def_levels(desc.max_def_level, def_levels)
  # No values: a Null column carries only level data.
  builder.add_values(Parquet::Encoding::PLAIN, [] of Int32)
  [builder.consume]
end

private def null_reader(def_levels : Array(Int16),
                        padding_threshold : Int16? = nil) : PArrow::NullArrayReader
  desc = PageUtil.column_descriptor(NULL_COLUMN)
  PArrow::NullArrayReader.new(
    PageUtil::InMemoryPageIterator.new([null_pages(def_levels)]), desc, 1024, padding_threshold
  )
end

describe Parquet::Arrow::NullArrayReader do
  it "reads an all-null column as a NullArray" do
    reader = null_reader([0_i16, 0_i16, 0_i16, 0_i16])

    reader.data_type.should eq(Arrow::DataType.null)
    reader.read_records(4).should eq(4)
    array = reader.consume_batch.as(Arrow::NullArray)

    array.len.should eq(4)
    array.logical_null_count.should eq(4)
    # `max_def_level == 1` on an optional column uses the mask-only definition
    # level buffer, which does not retain the levels (upstream
    # `DefinitionLevelBuffer` behaves the same way).
    reader.def_levels.should be_nil
    reader.rep_levels.should be_nil # max_rep_level is 0
    reader.max_def_level.should eq(1)
  end

  it "returns an empty NullArray for an empty batch" do
    reader = null_reader([] of Int16)
    reader.next_batch(0).as(Arrow::NullArray).len.should eq(0)
  end

  it "reads batches in pieces" do
    levels = Array(Int16).new(8, 0_i16)
    reader = null_reader(levels)

    reader.next_batch(3).as(Arrow::NullArray).len.should eq(3)
    reader.next_batch(3).as(Arrow::NullArray).len.should eq(3)
    reader.next_batch(3).as(Arrow::NullArray).len.should eq(2)
  end

  it "is built by the array reader builder for a Null arrow field" do
    reader_file = Parquet::File::SerializedFileReader.new(
      BytesCR.new(ParquetTestHelpers.fixture_bytes("nulls.snappy.parquet"))
    )
    leaf = reader_file.metadata.file_metadata.schema_descr.leaves[0]
    leaf_type = leaf.self_type
    # `b_c_int` is an OPTIONAL INT32 column of nulls inside an optional struct
    # (its definitions level is 2); with a Null arrow type it is read through
    # the NullArrayReader.
    field = PArrow::ParquetField.new(
      0_i16,
      leaf.max_def_level,
      true,
      Arrow::DataType.null,
      PArrow::ParquetFieldType.primitive(0, leaf_type)
    )

    reader = PArrow::ArrayReaderBuilder.new(PArrow::FileRowGroups.new(reader_file))
      .build_array_reader(field, PArrow::ProjectionMask.all)

    reader.data_type.should eq(Arrow::DataType.null)
    array = reader.next_batch(1024).as(Arrow::NullArray)
    array.len.should eq(8)
    array.logical_null_count.should eq(8)
  end
end
