require "../../spec_helper"

# Ported from `parquet/src/arrow/arrow_reader/mod.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the
# `ParquetRecordBatchReader`/`ParquetRecordBatchReaderBuilder` batch-reading
# path.
#
# Divergences (see `plans/parity.md`): the port reads every row of every row
# group, so upstream's row selection (`RowSelection`), row-group/offset/limit
# plans, `RowFilter` evaluation, statistics and reader metrics are not available
# yet.
private alias PArrow = Parquet::Arrow
private alias BytesCR = Parquet::File::BytesChunkReader

private def serialized_reader(name : String) : Parquet::File::SerializedFileReader
  Parquet::File::SerializedFileReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(name)))
end

private def read_batches(name : String, batch_size : Int32 = 1024,
                         projection : PArrow::ProjectionMask? = nil) : Array(Arrow::RecordBatch)
  builder = PArrow::ParquetRecordBatchReaderBuilder.try_new(serialized_reader(name))
    .with_batch_size(batch_size)
  builder = builder.with_projection(projection.not_nil!) if projection
  builder.build.to_a
end

describe Parquet::Arrow::ParquetRecordBatchReader do
  it "reads a struct column of nulls" do
    builder = PArrow::ParquetRecordBatchReaderBuilder.try_new(serialized_reader("nulls.snappy.parquet"))

    builder.metadata.num_row_groups.should eq(1)
    builder.metadata.file_metadata.num_rows.should eq(8)
    builder.schema.fields.len.should eq(1)
    builder.parquet_schema.num_columns.should eq(1)

    reader = builder.build
    reader.schema.fields.len.should eq(1)

    batch = reader.next_batch.not_nil!
    batch.num_rows.should eq(8)
    batch.num_columns.should eq(1)
    batch.column(0).as(Arrow::StructArray).column(0)
      .as(Arrow::PrimitiveArray(Int32)).null_count.should eq(8)
    reader.next_batch.should be_nil
  end

  it "splits rows into batches of the requested size" do
    batches = read_batches("nulls.snappy.parquet", batch_size: 3)
    batches.map(&.num_rows).should eq([3, 3, 2])
  end

  it "reads primitive columns with a projection mask" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    builder = PArrow::ParquetRecordBatchReaderBuilder.try_new(reader_file)
    # Exclude the INT96 timestamp column (not ported).
    mask = PArrow::ProjectionMask.leaves(
      reader_file.metadata.file_metadata.schema_descr, (0...10).to_a
    )

    batches = builder.with_projection(mask).build.to_a
    batch = batches.first
    batch.num_rows.should eq(8)
    batch.num_columns.should eq(10)

    ids = batch.column(0).as(Arrow::PrimitiveArray(Int32))
    Array(Int32?).new(3) { |idx| ids.value(idx) }.should eq([4, 5, 6])
    bools = batch.column(1).as(Arrow::BooleanArray)
    Array(Bool?).new(3) { |idx| bools.value(idx) }.should eq([true, false, true])
    batch.column(2).as(Arrow::PrimitiveArray(Int32)).value(1).should eq(1)
    batch.column(3).as(Arrow::PrimitiveArray(Int32)).value(1).should eq(1)
    batch.column(4).as(Arrow::PrimitiveArray(Int32)).value(1).should eq(1)
    batch.column(5).as(Arrow::PrimitiveArray(Int64)).value(1).should eq(10)
    batch.column(6).as(Arrow::PrimitiveArray(Float32)).value(1).should be_close(1.1_f32, 0.001)
    batch.column(7).as(Arrow::PrimitiveArray(Float64)).value(1).should be_close(10.1, 0.001)
    String.new(batch.column(9).as(Arrow::GenericBinaryArray(Int32)).value(1)).should eq("1")
  end

  it "reads list columns" do
    batches = read_batches("list_columns.parquet")
    batches.size.should eq(1)
    batch = batches.first
    batch.num_rows.should eq(3)

    ints = batch.column(0).as(Arrow::GenericListArray(Int32))
    ints.null_count.should eq(0)
    Array(Int64?).new(3) { |idx| ints.value(0).as(Arrow::PrimitiveArray(Int64)).value(idx) }
      .should eq([1, 2, 3])
    ints.value(1).as(Arrow::PrimitiveArray(Int64)).null?(0).should be_true
    ints.value(2).as(Arrow::PrimitiveArray(Int64)).value(0).should eq(4)

    strings = batch.column(1).as(Arrow::GenericListArray(Int32))
    strings.null?(1).should be_true
    first = strings.value(0).as(Arrow::GenericStringArray(Int32))
    Array(String?).new(3) { |idx| first.value(idx) }.should eq(%w[abc efg hij])
    last = strings.value(2).as(Arrow::GenericStringArray(Int32))
    last.null?(1).should be_true
    last.value(3).should eq("xyz")
  end

  it "reads nested list columns" do
    batches = read_batches("nested_lists.snappy.parquet")
    batch = batches.first
    batch.num_rows.should eq(3)
    batch.column(0).as(Arrow::GenericListArray(Int32)).len.should eq(3)
  end

  it "provides the convenience constructor" do
    reader = PArrow::ParquetRecordBatchReader.try_new(
      serialized_reader("nulls.snappy.parquet"), 4
    )
    batches = reader.to_a
    batches.map(&.num_rows).should eq([4, 4])
  end

  it "returns no batches for an empty projection" do
    reader_file = serialized_reader("nulls.snappy.parquet")
    mask = PArrow::ProjectionMask.none(reader_file.metadata.file_metadata.schema_descr.num_columns)
    batches = PArrow::ParquetRecordBatchReaderBuilder.try_new(reader_file)
      .with_projection(mask)
      .build
      .to_a

    # The empty reader still yields the row count, with no columns.
    batches.map(&.num_rows).should eq([8])
    batches.first.num_columns.should eq(0)
  end
end

describe Parquet::Arrow::ArrowReaderMetadata do
  it "loads the arrow schema from the parquet metadata" do
    metadata = PArrow::ArrowReaderMetadata.load(serialized_reader("list_columns.parquet"))

    metadata.metadata.file_metadata.num_rows.should eq(3)
    metadata.schema.fields.len.should eq(2)
    metadata.schema.fields[0].data_type.kind.list?.should be_true
    metadata.schema.fields[1].data_type.kind.list?.should be_true
    metadata.fields.not_nil!.arrow_type.kind.struct?.should be_true
  end

  it "can skip the embedded arrow schema" do
    metadata = PArrow::ArrowReaderMetadata.try_new(
      serialized_reader("list_columns.parquet").metadata, skip_arrow_metadata: true
    )
    metadata.schema.fields.len.should eq(2)
  end
end
