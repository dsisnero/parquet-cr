require "../../spec_helper"

# Characterization specs for the pluggable-decoder column reader used by the
# arrow record reader (upstream `GenericColumnReader::new_with_decoders` and
# `read_records_with_reservation` in `parquet/src/column/reader.rs`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# The record reader decodes definition levels straight into an arrow
# `DefinitionLevelBuffer` and asks the caller to reserve value capacity once the
# level decoders know how many output slots a read needs (`reserve_values`).
private alias WP = Parquet::File::Properties::WriterProperties
private alias FWriter = Parquet::File::SerializedFileWriter
private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader
private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias PRep = Parquet::Repetition
private alias RR = Parquet::Arrow::RecordReader
private alias Decoder = Parquet::Column::Reader::ColumnValueDecoderImpl(Int32)

# Writes an INT32 column with the given repetition/definition levels and returns
# a page reader positioned at its first page.
private def int32_column_pages(
  repetition : PRep,
  def_levels : Array(Int16),
  values : Array(Int32),
) : Tuple(Parquet::File::SerializedFileReader, Parquet::Column::PageReader)
  schema = ST.group_type_builder("schema").with_fields([
    ST.primitive_type_builder("a", PType::INT32)
      .with_repetition(repetition)
      .build.as(ST),
  ]).build

  io = IO::Memory.new
  writer = FWriter.new(io, schema, WP.builder.build)
  row_group = writer.next_row_group
  column_writer = row_group.next_column.not_nil!
  typed = column_writer.typed(Int32)
  typed.write_batch(values, repetition.required? ? nil : def_levels, nil)
  column_writer.close
  row_group.close
  writer.close

  reader = FReader.new(BytesCR.new(io.to_slice))
  {reader, reader.get_row_group(0).get_column_page_reader(0)}
end

describe Parquet::Column::ColumnReaderImpl do
  it "decodes definition levels into an arrow definition level buffer" do
    reader, pages = int32_column_pages(
      PRep::OPTIONAL, [1_i16, 0_i16, 1_i16, 1_i16], [10_i32, 20, 30]
    )
    descriptor = reader.metadata.file_metadata.schema_descr.column(0)

    buffer = RR::DefinitionLevelBuffer.new(descriptor, null_mask_only: true)
    decoder = RR::DefinitionLevelBufferDecoder.new(descriptor.max_def_level, packed: true)
    column_reader = Parquet::Column::ColumnReaderImpl(Array(Int32), Decoder).new_with_decoders(
      descriptor, pages, Decoder.new(descriptor), buffer, decoder
    )

    values = [] of Int32
    reserved = [] of Tuple(Int32, Int32)
    records, values_read, levels_read = column_reader.read_records_with_reservation(
      4, nil, nil, values
    ) do |_values, values_to_read, levels_to_read, _def_levels|
      reserved << {values_to_read, levels_to_read}
    end

    records.should eq(4)
    levels_read.should eq(4)
    values_read.should eq(3)
    values.should eq([10, 20, 30])

    # The reservation callback sees the decoded value/level counts.
    reserved.should eq([{3, 4}])

    # The definition levels landed in the arrow buffer.
    buffer.nulls.len.should eq(4)
    mask = Array(Bool).new(4) { |i| Parquet::Util::BitUtil.get_bit(buffer.nulls.as_slice, i) }
    mask.should eq([true, false, true, true])
  end

  it "reads required columns without definition levels" do
    reader, pages = int32_column_pages(PRep::REQUIRED, [] of Int16, [1_i32, 2, 3])
    descriptor = reader.metadata.file_metadata.schema_descr.column(0)

    column_reader = Parquet::Column::ColumnReaderImpl(Array(Int32), Decoder).new_with_decoders(
      descriptor, pages, Decoder.new(descriptor), nil, nil
    )

    values = [] of Int32
    reserved = [] of Tuple(Int32, Int32)
    records, values_read, levels_read = column_reader.read_records_with_reservation(
      3, nil, nil, values
    ) do |_values, values_to_read, levels_to_read, _def_levels|
      reserved << {values_to_read, levels_to_read}
    end

    records.should eq(3)
    levels_read.should eq(3)
    values_read.should eq(3)
    values.should eq([1, 2, 3])
    reserved.should eq([{3, 3}])
  end

  # A nullable column needs a definition-level decoder (the record reader always
  # supplies the arrow one); a bare buffer is a misconfigured reader.
  it "requires a definition level decoder for nullable columns" do
    reader, pages = int32_column_pages(PRep::OPTIONAL, [1_i16], [1_i32])
    descriptor = reader.metadata.file_metadata.schema_descr.column(0)
    buffer = RR::DefinitionLevelBuffer.new(descriptor, null_mask_only: true)

    column_reader = Parquet::Column::ColumnReaderImpl(Array(Int32), Decoder).new_with_decoders(
      descriptor, pages, Decoder.new(descriptor), buffer, nil
    )

    expect_raises(Parquet::GeneralError, "definition level decoder is not initialized") do
      column_reader.read_records_with_reservation(1, nil, nil, [] of Int32) do |_values, _values_to_read, _levels_to_read, _def_levels|
      end
    end
  end
end
