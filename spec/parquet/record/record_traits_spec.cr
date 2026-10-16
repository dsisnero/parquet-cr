require "../../spec_helper"

# Characterization specs for `parquet/src/record/record_reader.rs` and
# `record_writer.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream defines only the traits (`RecordReader<T>` / `RecordWriter<T>`), which
# are implemented by `parquet_derive`-generated code. The trait surface is
# characterized here with hand-written implementations.

private alias FReader = Parquet::File::SerializedFileReader
private alias BytesCR = Parquet::File::BytesChunkReader

# A `RecordReader(Row)` that appends up to `num_records` rows.
private class RowCollector < Parquet::Record::RecordReader(Parquet::Record::Row)
  getter rows = [] of Parquet::Record::Row

  def read_from_row_group(row_group_reader : Parquet::File::RowGroupReader, num_records : Int32) : Nil
    iter = row_group_reader.get_row_iter(nil)
    count = 0
    while count < num_records
      value = iter.next
      break if value.is_a?(Iterator::Stop)

      @rows << value
      count += 1
    end
  end
end

# A `RecordWriter(Row)` that writes a single REQUIRED INT32 column.
private class Int32RecordWriter < Parquet::Record::RecordWriter(Parquet::Record::Row)
  def initialize(@values : Array(Int32))
  end

  def schema : Parquet::Schema::Type
    Parquet::Schema::Type.group_type_builder("schema")
      .with_fields([
        Parquet::Schema::Type.primitive_type_builder("col", Parquet::Type::INT32)
          .with_repetition(Parquet::Repetition::REQUIRED)
          .build,
      ] of Parquet::Schema::Type)
      .build
  end

  def write_to_row_group(row_group_writer : Parquet::File::SerializedRowGroupWriter) : Nil
    column_writer = row_group_writer.next_column.not_nil!
    column_writer.typed(Int32).write_batch(@values, nil, nil)
    column_writer.close
  end
end

describe Parquet::Record::RecordReader do
  it "reads records from a row group" do
    reader = FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")))
    collector = RowCollector.new
    collector.read_from_row_group(reader.get_row_group(0), 3)
    collector.rows.size.should eq(3)
    collector.rows.map(&.get_int(0)).should eq([4, 5, 6])
  end

  it "reads all records when the requested count exceeds the data" do
    reader = FReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes("alltypes_plain.parquet")))
    collector = RowCollector.new
    collector.read_from_row_group(reader.get_row_group(0), 100)
    collector.rows.size.should eq(8)
  end
end

describe Parquet::Record::RecordWriter do
  it "writes records into a row group" do
    writer_impl = Int32RecordWriter.new([1, 2, 3])
    io = IO::Memory.new
    writer = Parquet::File::SerializedFileWriter.new(
      io, writer_impl.schema, Parquet::File::Properties::WriterProperties.default
    )
    row_group = writer.next_row_group
    writer_impl.write_to_row_group(row_group)
    row_group.close
    writer.close

    reader = FReader.new(BytesCR.new(io.to_slice))
    reader.get_row_iter(nil).flat_map { |row| [row.get_int(0)] }.to_a.should eq([1, 2, 3])
  end
end
