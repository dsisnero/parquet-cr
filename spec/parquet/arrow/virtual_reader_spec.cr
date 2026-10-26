require "../../spec_helper"

# Upstream arrow/array_reader/{row_number,row_group_index}.rs and
# arrow/arrow_reader/mod.rs virtual-column tests, apache/arrow-rs 60.0.0.
private alias VA = Parquet::Arrow
private alias VM = Parquet::File::Metadata

private def virtual_file_bytes(rows = 100, group_size = 50) : Bytes
  array = Arrow::PrimitiveArray(Int64).from_values((5000_i64...5000_i64 + rows).to_a)
  schema = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("col", Arrow::DataType.int64, false)]))
  batch = Arrow::RecordBatch.try_new(schema, [array] of Arrow::ArrayBase)
  sink = IO::Memory.new
  props = Parquet::File::Properties::WriterProperties.builder.set_max_row_group_row_count(group_size).build
  writer = VA::ArrowWriter.try_new(sink, schema, props)
  writer.write(batch)
  writer.close
  sink.to_slice
end

private def virtual_file(rows = 100, group_size = 50) : Parquet::File::SerializedFileReader
  Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(virtual_file_bytes(rows, group_size)))
end

private def virtual_fields : Array(Arrow::Field)
  [Arrow::Field.new("row_number", Arrow::DataType.int64, false).with_extension_type(VA::RowNumber.new),
   Arrow::Field.new("row_group_index", Arrow::DataType.int64, false).with_extension_type(VA::RowGroupIndex.new)]
end

private def virtual_builder(file : Parquet::File::SerializedFileReader, supplied = false) : VA::ParquetRecordBatchReaderBuilder
  options = VA::ArrowReaderOptions.new.with_virtual_columns(virtual_fields)
  options.with_schema(Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("col", Arrow::DataType.int64, false)]))) if supplied
  VA::ParquetRecordBatchReaderBuilder.try_new_with_options(file, options)
end

private def virtual_column_values(batches : Array(Arrow::RecordBatch), column : Int32) : Array(Int64)
  batches.flat_map { |batch| batch.column(column).as(Arrow::PrimitiveArray(Int64)).values.to_slice.to_a }
end

private def virtual_metadata(counts : Array(Int64), ordinals : Array(Int32?)) : VM::ParquetMetaData
  metadata = virtual_file(1, 1).metadata
  groups = counts.map_with_index do |count, index|
    group = VM::RowGroupMetaData.new(metadata.file_metadata.schema_descr)
    group.num_rows = count
    group.ordinal = ordinals[index]
    group
  end
  VM::ParquetMetaData.new(metadata.file_metadata, groups)
end

describe "Virtual readers" do
  it "reads reverse selected groups in file-global row-number order" do
    metadata = virtual_metadata([2_i64, 2_i64, 2_i64], [0, 1, 2] of Int32?)
    reader = VA::RowNumberReader.try_new(metadata, [metadata.row_group(2), metadata.row_group(0)].each)
    reader.read_records(6).should eq 4
    reader.consume_batch.as(Arrow::PrimitiveArray(Int64)).to_a.should eq [4, 5, 0, 1]
    reader.def_levels.should be_nil
    reader.rep_levels.should be_nil
    reader.consume_batch.len.should eq 0
  end

  it "splits ranges across batches" do
    metadata = virtual_metadata([10_i64], [0] of Int32?)
    reader = VA::RowNumberReader.try_new(metadata, metadata.row_groups.each)
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]].each do |expected|
      reader.read_records(3).should eq expected.size
      reader.consume_batch.as(Arrow::PrimitiveArray(Int64)).to_a.should eq expected
    end
    reader.read_records(1).should eq 0
  end

  it "buffers reads across interleaved skips" do
    metadata = virtual_metadata([5_i64, 5_i64], [0, 1] of Int32?)
    reader = VA::RowNumberReader.try_new(metadata, metadata.row_groups.each)
    reader.skip_records(2).should eq 2
    reader.read_records(2).should eq 2
    reader.skip_records(3).should eq 3
    reader.read_records(3).should eq 3
    reader.consume_batch.as(Arrow::PrimitiveArray(Int64)).to_a.should eq [2, 3, 7, 8, 9]
    reader.skip_records(10).should eq 0
  end

  it "rejects mixed ordinals regardless of selected groups and rejects absent ordinals" do
    metadata = virtual_metadata([2_i64, 2_i64, 2_i64], [0, nil, 2] of Int32?)
    [[metadata.row_group(0), metadata.row_group(2)], [metadata.row_group(1)]].each do |selected|
      expect_raises(Parquet::GeneralError, "inconsistent row-group ordinals") { VA::RowNumberReader.try_new(metadata, selected.each) }
    end
    metadata = virtual_metadata([2_i64, 2_i64], [nil, nil] of Int32?)
    expect_raises(Parquet::GeneralError, "missing ordinal field") { VA::RowNumberReader.try_new(metadata, [metadata.row_group(0)].each) }
  end

  it "emits group indices and skips across groups" do
    metadata = virtual_metadata([3_i64, 4_i64, 2_i64], [10, 20, 30] of Int32?)
    reader = VA::RowGroupIndexReader.try_new(metadata, metadata.row_groups.each)
    reader.skip_records(5).should eq 5
    reader.read_records(10).should eq 4
    reader.consume_batch.as(Arrow::PrimitiveArray(Int64)).to_a.should eq [1, 1, 2, 2]
    reader.read_records(1).should eq 0
    reader.consume_batch.len.should eq 0
    reader = VA::RowGroupIndexReader.try_new(metadata, [metadata.row_group(2), metadata.row_group(0)].each)
    reader.next_batch(6).as(Arrow::PrimitiveArray(Int64)).to_a.should eq [2, 2, 0, 0, 0]
  end

  it "handles the single-group fast path and skip" do
    metadata = virtual_metadata([10_i64, 10_i64, 10_i64], [0, 1, 2] of Int32?)
    reader = VA::RowGroupIndexReader.try_new(metadata, [metadata.row_group(1)].each)
    reader.skip_records(3).should eq 3
    reader.read_records(5).should eq 5
    reader.consume_batch.as(Arrow::PrimitiveArray(Int64)).to_a.should eq [1, 1, 1, 1, 1]
    reader.skip_records(10).should eq 2
  end
end

describe "Virtual columns through public Parquet readers" do
  it "appends fields and values with inferred and supplied schemas" do
    [false, true].each do |supplied|
      reader = virtual_builder(virtual_file, supplied).with_batch_size(13).build
      batches = reader.to_a
      batches.first.schema.fields.to_a.map(&.name).should eq ["col", "row_number", "row_group_index"]
      batches.first.schema.fields.to_a[1..].should eq virtual_fields
      virtual_column_values(batches, 0).should eq (5000_i64...5100_i64).to_a
      virtual_column_values(batches, 1).should eq (0_i64...100_i64).to_a
      virtual_column_values(batches, 2).should eq([0_i64] * 50 + [1_i64] * 50)
    end
  end

  it "preserves global row numbers and physical group indices when groups are reordered" do
    batches = virtual_builder(virtual_file).with_row_groups([1, 0]).with_batch_size(17).build.to_a
    virtual_column_values(batches, 0).should eq((5050_i64...5100_i64).to_a + (5000_i64...5050_i64).to_a)
    virtual_column_values(batches, 1).should eq((50_i64...100_i64).to_a + (0_i64...50_i64).to_a)
    virtual_column_values(batches, 2).should eq([1_i64] * 50 + [0_i64] * 50)
  end

  it "reads only virtual columns with an empty physical projection" do
    reader = virtual_builder(virtual_file).with_projection(VA::ProjectionMask.none(1)).build
    batches = reader.to_a
    batches.first.num_columns.should eq 2
    virtual_column_values(batches, 0).should eq (0_i64...100_i64).to_a
    virtual_column_values(batches, 1).should eq([0_i64] * 50 + [1_i64] * 50)
  end

  it "preserves row numbers across selections, offset and limit" do
    selection = VA::RowSelection.new([VA::RowSelector.skip(45), VA::RowSelector.select(15), VA::RowSelector.skip(40)])
    batches = virtual_builder(virtual_file).with_row_selection(selection).with_offset(2).with_limit(10).with_batch_size(4).build.to_a
    virtual_column_values(batches, 1).should eq (47_i64...57_i64).to_a
    virtual_column_values(batches, 2).should eq([0_i64] * 3 + [1_i64] * 7)
  end

  it "rejects nonvirtual fields at both option and field-level entry points" do
    field = Arrow::Field.new("plain", Arrow::DataType.int64, false)
    expect_raises(Parquet::GeneralError, "is not a virtual column") { VA::ArrowReaderOptions.new.with_virtual_columns([field]) }
    schema = virtual_file.metadata.file_metadata.schema_descr
    expect_raises(Parquet::GeneralError, "is not a virtual column") do
      VA.parquet_to_arrow_field_levels_with_virtual(schema, VA::ProjectionMask.all, nil, [field])
    end
    unknown = field.with_metadata({"ARROW:extension:name" => "parquet.virtual.future"})
    expect_raises(Parquet::ArrowError, "unsupported virtual column type") do
      VA.parquet_to_arrow_field_levels_with_virtual(schema, VA::ProjectionMask.none(1), nil, [unknown])
    end
  end
end

describe "Virtual reader integration boundaries" do
  it "keeps virtual columns aligned after a physical-column predicate" do
    file = virtual_file
    mask = VA::ProjectionMask.all
    predicate = VA::ArrowPredicateFn.new(mask) do |batch|
      column = batch.column(0).as(Arrow::PrimitiveArray(Int64))
      Arrow::BooleanArray.from_values((0...column.len).map { |index| column.value(index) % 7 == 0 })
    end
    batches = virtual_builder(file).with_row_filter(VA::RowFilter.new(predicate)).with_batch_size(8).build.to_a
    expected = (0_i64...100_i64).select { |index| (5000 + index) % 7 == 0 }
    virtual_column_values(batches, 1).should eq expected
    virtual_column_values(batches, 2).should eq expected.map { |index| index // 50 }
  end

  it "requires file metadata when a low-level builder encounters a virtual field" do
    file = virtual_file
    levels = VA.parquet_to_arrow_field_levels_with_virtual(file.metadata.file_metadata.schema_descr, VA::ProjectionMask.none(1), nil, virtual_fields)
    builder = VA::ArrayReaderBuilder.new(VA::FileRowGroups.new(file))
    expect_raises(Parquet::GeneralError, "ParquetMetaData is required") do
      builder.build_array_reader(levels.levels, VA::ProjectionMask.none(1))
    end
  end

  it "computes nullable virtual levels and handles empty selections" do
    file = virtual_file
    field = Arrow::Field.new("number", Arrow::DataType.int64, true).with_extension_type(VA::RowNumber.new)
    levels = VA.convert_virtual_field(field, 0_i16, 0_i16)
    levels.def_level.should eq 1
    levels.rep_level.should eq 0
    options = VA::ArrowReaderOptions.new.with_virtual_columns([field])
    builder = VA::ParquetRecordBatchReaderBuilder.try_new_with_options(file, options)
    batches = builder.with_projection(VA::ProjectionMask.none(1)).build.to_a
    virtual_column_values(batches, 0).should eq (0_i64...100_i64).to_a
    batches.first.column(0).null_count.should eq 0
    virtual_builder(file).with_row_groups([] of Int32).build.to_a.should be_empty
  end

  it "supports missing/unknown ordinals errors and empty group-index readers" do
    metadata = virtual_metadata([2_i64], [nil] of Int32?)
    expect_raises(Parquet::GeneralError, "missing ordinal field") { VA::RowGroupIndexReader.try_new(metadata, metadata.row_groups.each) }
    missing = VM::RowGroupMetaData.new(metadata.file_metadata.schema_descr)
    missing.ordinal = 99
    expect_raises(Parquet::GeneralError, "Row group with ordinal 99 not found in metadata") { VA::RowNumberReader.try_new(metadata, [missing].each) }
    expect_raises(Parquet::GeneralError, "Row group with ordinal 99 not found in metadata") { VA::RowGroupIndexReader.try_new(metadata, [missing].each) }
    reader = VA::RowGroupIndexReader.try_new(metadata, ([] of VM::RowGroupMetaData).each)
    reader.read_records(10).should eq 0
    reader.skip_records(10).should eq 0
    reader.consume_batch.len.should eq 0
  end
end

describe "Virtual columns through the push decoder" do
  it "keeps file-global values with reordered groups and streamed physical bytes" do
    bytes = virtual_file_bytes
    file = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(bytes))
    options = VA::ArrowReaderOptions.new.with_virtual_columns(virtual_fields)
    decoder = VA::ParquetPushDecoderBuilder.try_new_decoder_with_options(file.metadata, options)
      .with_row_groups([1, 0]).with_batch_size(17).build
    batches = [] of Arrow::RecordBatch
    loop do
      result = decoder.try_decode
      if result.needs_data?
        ranges = result.ranges
        decoder.push_ranges(ranges, ranges.map { |range| bytes[range.begin.to_i32, (range.end - range.begin).to_i32] })
      elsif result.data?
        batches << result.data
      else
        break
      end
    end
    virtual_column_values(batches, 1).should eq((50_i64...100_i64).to_a + (0_i64...50_i64).to_a)
    virtual_column_values(batches, 2).should eq([1_i64] * 50 + [0_i64] * 50)
  end
end
