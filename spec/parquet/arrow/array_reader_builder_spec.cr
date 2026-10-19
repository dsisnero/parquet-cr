require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/builder.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# `ArrayReaderBuilder` that turns a `ParquetField` + projection mask into an
# `ArrayReader` over `RowGroups`.
#
# Divergences (see `plans/parity.md`): no virtual columns yet, so upstream's
# `test_create_array_reader_with_row_numbers` is not ported; the cache-options
# wiring has no upstream unit test (it is exercised end-to-end through the async
# reader), so it is characterized here.
private alias PArrow = Parquet::Arrow
private alias BytesCR = Parquet::File::BytesChunkReader

private def file_reader(name : String) : Parquet::File::SerializedFileReader
  Parquet::File::SerializedFileReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(name)))
end

private def root_field(file_reader : Parquet::File::SerializedFileReader,
                       virtual_fields : Array(Arrow::Field) = [] of Arrow::Field) : PArrow::ParquetField?
  file_metadata = file_reader.metadata.file_metadata
  _, fields = PArrow.parquet_to_arrow_schema_and_fields(
    file_metadata.schema_descr, PArrow::ProjectionMask.all, file_metadata.key_value_metadata
  )
  fields
end

private def builder_for(file_reader : Parquet::File::SerializedFileReader) : PArrow::ArrayReaderBuilder
  PArrow::ArrayReaderBuilder.new(PArrow::FileRowGroups.new(file_reader))
end

describe Parquet::Arrow::ArrayReaderBuilder do
  it "builds a struct reader for a projected leaf" do
    reader_file = file_reader("nulls.snappy.parquet")
    file_metadata = reader_file.metadata.file_metadata
    mask = PArrow::ProjectionMask.leaves(file_metadata.schema_descr, [0])

    reader = builder_for(reader_file).build_array_reader(root_field(reader_file), mask)

    expected = Arrow::DataType.struct(
      Arrow::Fields.new([
        Arrow::Field.new(
          "b_struct",
          Arrow::DataType.struct(
            Arrow::Fields.new([Arrow::Field.new("b_c_int", Arrow::DataType.int32, true)])
          ),
          true
        ),
      ])
    )
    reader.data_type.should eq(expected)

    # The fixture has 8 rows, all null in the projected leaf.
    array = reader.next_batch(1024).as(Arrow::StructArray)
    array.len.should eq(8)
    array.column(0).as(Arrow::StructArray).column(0).as(Arrow::PrimitiveArray(Int32))
      .null_count.should eq(8)
  end

  it "builds an empty reader when no leaf is projected" do
    reader_file = file_reader("nulls.snappy.parquet")
    mask = PArrow::ProjectionMask.none(reader_file.metadata.file_metadata.schema_descr.num_columns)

    reader = builder_for(reader_file).build_array_reader(root_field(reader_file), mask)

    reader.data_type.should eq(Arrow::DataType.struct(Arrow::Fields.empty))
    reader.next_batch(1024).len.should eq(8)
  end

  it "builds an empty reader without a root field" do
    reader_file = file_reader("nulls.snappy.parquet")
    mask = PArrow::ProjectionMask.all

    reader = builder_for(reader_file).build_array_reader(nil, mask)

    reader.data_type.should eq(Arrow::DataType.struct(Arrow::Fields.empty))
    reader.next_batch(1024).len.should eq(8)
  end

  it "builds list readers" do
    reader_file = file_reader("list_columns.parquet")
    mask = PArrow::ProjectionMask.all

    reader = builder_for(reader_file).build_array_reader(root_field(reader_file), mask)
    root = reader.data_type.as(Arrow::DataType)
    root.kind.struct?.should be_true
    root.fields.not_nil!.to_a.any? { |field| field.data_type.kind.list? }.should be_true

    array = reader.next_batch(1024).as(Arrow::StructArray)
    array.len.should be > 0
  end

  it "builds map readers" do
    reader_file = file_reader("nested_maps.snappy.parquet")
    mask = PArrow::ProjectionMask.all

    reader = builder_for(reader_file).build_array_reader(root_field(reader_file), mask)
    root = reader.data_type.as(Arrow::DataType)
    root.kind.struct?.should be_true
    root.fields.not_nil!.to_a.any? { |field| field.data_type.kind.map? }.should be_true

    array = reader.next_batch(1024).as(Arrow::StructArray)
    array.len.should be > 0
  end

  it "applies the batch size hint" do
    reader_file = file_reader("nulls.snappy.parquet")
    file_metadata = reader_file.metadata.file_metadata
    mask = PArrow::ProjectionMask.leaves(file_metadata.schema_descr, [0])

    reader = builder_for(reader_file)
      .with_batch_size(3)
      .build_array_reader(root_field(reader_file), mask)

    reader.next_batch(3).len.should eq(3)
  end

  it "builds fixed length byte array readers for decimal columns" do
    reader_file = file_reader("fixed_length_decimal.parquet")
    mask = PArrow::ProjectionMask.all

    reader = builder_for(reader_file).build_array_reader(root_field(reader_file), mask)
    root_type = reader.data_type.as(Arrow::DataType)
    field = root_type.fields.not_nil!.to_a.first
    field.data_type.should eq(Arrow::DataType.decimal128(25, 2))

    array = reader.next_batch(1024).as(Arrow::StructArray)
    array.len.should be > 0
    array.column(0).as(Arrow::PrimitiveArray(Int128)).data_type
      .should eq(Arrow::DataType.decimal128(25, 2))
  end

  it "wraps cached leaves so their batches land in the cache" do
    reader_file = file_reader("alltypes_plain.parquet")
    file_metadata = reader_file.metadata.file_metadata
    mask = PArrow::ProjectionMask.leaves(file_metadata.schema_descr, [0])
    cache = PArrow::RowGroupCache.new(1024, 100 * 1024 * 1024)
    options = PArrow::CacheOptionsBuilder.new(mask, cache).producer

    reader = builder_for(reader_file)
      .with_cache_options(options)
      .build_array_reader(root_field(reader_file), mask)

    reader.next_batch(1024).len.should eq(8)

    # The decoded leaf batch was inserted into the shared cache.
    cache.get(0, PArrow::BatchID.new(0)).should_not be_nil
  end

  it "does not cache leaves outside the cache mask" do
    reader_file = file_reader("alltypes_plain.parquet")
    file_metadata = reader_file.metadata.file_metadata
    mask = PArrow::ProjectionMask.leaves(file_metadata.schema_descr, [0])
    cache = PArrow::RowGroupCache.new(1024, 100 * 1024 * 1024)
    # An empty cache mask (as `compute_cache_projection` builds when the cache
    # is disabled) leaves every leaf reader unwrapped.
    options = PArrow::CacheOptionsBuilder.new(
      PArrow::ProjectionMask.none(file_metadata.schema_descr.num_columns), cache
    ).producer

    reader = builder_for(reader_file)
      .with_cache_options(options)
      .build_array_reader(root_field(reader_file), mask)

    reader.next_batch(1024).len.should eq(8)
    cache.get(0, PArrow::BatchID.new(0)).should be_nil
  end
end
