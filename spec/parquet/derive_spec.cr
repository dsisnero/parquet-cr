require "../spec_helper"

# Ported from `parquet_derive_test/src/lib.rs` test module (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: Rust `#[derive(ParquetRecordWriter/Reader)]` is
# `include Parquet::Derive::Record`, which generates nested `Writer`/`Reader`
# classes. The reader constructs records positionally (fields declared in
# `initialize` order). Borrowed Rust shapes (`&str`, `Arc<str>`, `&Option<..>`,
# `Vec<u8>`) are expressed as Crystal owned types (`String`, `Bytes`); `NaiveDate`
# is `Parquet::Derive::Date`. The raw-identifier test has no Crystal analogue and
# is characterized with a normal field name.

private alias RecordWriterBase = Parquet::Record::RecordWriter
private alias WriterProperties = Parquet::File::Properties::WriterProperties
private alias SerializedFileWriter = Parquet::File::SerializedFileWriter
private alias SerializedFileReader = Parquet::File::SerializedFileReader
private alias BytesChunkReader = Parquet::File::BytesChunkReader
private alias Schema = Parquet::Schema

# Covers every field type the derive supports.
private class AllTypesRecord
  include Parquet::Derive::Record
  getter a_bool : Bool
  getter a_string : String
  getter maybe_a_string : String?
  getter i16 : Int16
  getter i32 : Int32
  getter u64 : UInt64
  getter maybe_u8 : UInt8?
  getter maybe_i16 : Int16?
  getter maybe_u32 : UInt32?
  getter isize : Int64
  getter float : Float32
  getter double : Float64
  getter maybe_float : Float32?
  getter maybe_double : Float64?
  getter now : Time
  getter date : Parquet::Derive::Date
  getter uuid : UUID
  getter byte_vec : Bytes
  getter maybe_byte_vec : Bytes?

  def initialize(
    @a_bool, @a_string, @maybe_a_string, @i16, @i32, @u64, @maybe_u8, @maybe_i16,
    @maybe_u32, @isize, @float, @double, @maybe_float, @maybe_double, @now, @date,
    @uuid, @byte_vec, @maybe_byte_vec,
  )
  end
end

private class PartiallyCompleteRecord
  include Parquet::Derive::Record
  getter bool : Bool
  getter string : String
  getter i16 : Int16
  getter i32 : Int32
  getter u64 : UInt64
  getter isize : Int64
  getter float : Float32
  getter double : Float64
  getter now : Time
  getter date : Parquet::Derive::Date
  getter uuid : UUID
  getter byte_vec : Bytes

  def initialize(@bool, @string, @i16, @i32, @u64, @isize, @float, @double, @now, @date, @uuid, @byte_vec)
  end
end

# Same columns, but the integer fields are OPTIONAL.
private class PartiallyOptionalRecord
  include Parquet::Derive::Record
  getter bool : Bool
  getter string : String
  getter i16 : Int16?
  getter i32 : Int32?
  getter u64 : UInt64?
  getter isize : Int64
  getter float : Float32
  getter double : Float64
  getter now : Time
  getter date : Parquet::Derive::Date
  getter uuid : UUID
  getter byte_vec : Bytes

  def initialize(@bool, @string, @i16, @i32, @u64, @isize, @float, @double, @now, @date, @uuid, @byte_vec)
  end
end

# Removes several fields and shuffles them; still loadable by column name.
private class PrunedRecord
  include Parquet::Derive::Record
  getter bool : Bool
  getter string : String
  getter byte_vec : Bytes
  getter float : Float32
  getter double : Float64
  getter i16 : Int16
  getter i32 : Int32
  getter u64 : UInt64
  getter isize : Int64

  def initialize(@bool, @string, @byte_vec, @float, @double, @i16, @i32, @u64, @isize)
  end
end

private def write_records(writer_impl, io : IO) : Nil
  file_writer = SerializedFileWriter.new(io, writer_impl.schema, WriterProperties.default)
  row_group = file_writer.next_row_group
  writer_impl.write_to_row_group(row_group)
  row_group.close
  file_writer.close
end

private def sample_uuid : UUID
  UUID.new("67e55044-10b1-426f-9247-bb680e5fe0c8")
end

describe "Parquet::Derive" do
  # Upstream `test_parquet_derive_hello`.
  it "generates a schema matching a hand-written one" do
    schema_str = <<-SCHEMA
    message rust_schema {
        REQUIRED BOOLEAN              a_bool;
        REQUIRED BINARY               a_string (STRING);
        OPTIONAL BINARY               maybe_a_string (STRING);
        REQUIRED INT32                i16 (INTEGER(16,true));
        REQUIRED INT32                i32;
        REQUIRED INT64                u64 (INTEGER(64,false));
        OPTIONAL INT32                maybe_u8 (INTEGER(8,false));
        OPTIONAL INT32                maybe_i16 (INTEGER(16,true));
        OPTIONAL INT32                maybe_u32 (INTEGER(32,false));
        REQUIRED INT64                isize (INTEGER(64,true));
        REQUIRED FLOAT                float;
        REQUIRED DOUBLE               double;
        OPTIONAL FLOAT                maybe_float;
        OPTIONAL DOUBLE               maybe_double;
        REQUIRED INT64                now (TIMESTAMP_MILLIS);
        REQUIRED INT32                date (DATE);
        REQUIRED FIXED_LEN_BYTE_ARRAY(16) uuid (UUID);
        REQUIRED BINARY               byte_vec;
        OPTIONAL BINARY               maybe_byte_vec;
    }
    SCHEMA

    records = [] of AllTypesRecord
    generated = AllTypesRecord::Writer.new(records).schema
    expected = Schema.parse_message_type(schema_str)

    generated.should eq(expected)
  end

  # Upstream `test_parquet_derive_read_write_combined`.
  it "round-trips records" do
    record = PartiallyCompleteRecord.new(
      true, "a string", -45_i16, 456, 4563424_u64, -365_i64, 3.5_f32,
      Float64::NAN, Time.utc(2024, 3, 14, 15, 9, 26, nanosecond: 123_000_000),
      Parquet::Derive::Date.from_time(Time.utc(2015, 3, 14)), sample_uuid,
      Bytes[0x65, 0x66, 0x67],
    )

    io = IO::Memory.new
    write_records(PartiallyCompleteRecord::Writer.new([record]), io)

    reader = SerializedFileReader.new(BytesChunkReader.new(io.to_slice))
    reader_impl = PartiallyCompleteRecord::Reader.new
    reader_impl.read_from_row_group(reader.get_row_group(0), 1)

    out = reader_impl.records
    out.size.should eq(1)
    out[0].bool.should eq(true)
    out[0].string.should eq("a string")
    out[0].i16.should eq(-45)
    out[0].i32.should eq(456)
    out[0].u64.should eq(4563424_u64)
    out[0].isize.should eq(-365)
    out[0].float.should eq(3.5_f32)
    out[0].double.nan?.should be_true
    out[0].now.should eq(Time.utc(2024, 3, 14, 15, 9, 26, nanosecond: 123_000_000))
    out[0].date.days.should eq(Parquet::Derive::Date.from_time(Time.utc(2015, 3, 14)).days)
    out[0].uuid.should eq(sample_uuid)
    out[0].byte_vec.should eq(Bytes[0x65, 0x66, 0x67])
  end

  # Upstream `test_parquet_derive_read_optional_but_valid_column`.
  it "reads optional-but-valid columns into required fields" do
    record = PartiallyOptionalRecord.new(
      true, "a string", -45_i16, 456, 4563424_u64, -365_i64, 3.5_f32,
      Float64::NAN, Time.utc(2024, 3, 14, 15, 9, 26),
      Parquet::Derive::Date.from_time(Time.utc(2015, 3, 14)), sample_uuid,
      Bytes[0x65, 0x66, 0x67],
    )

    io = IO::Memory.new
    write_records(PartiallyOptionalRecord::Writer.new([record]), io)

    reader = SerializedFileReader.new(BytesChunkReader.new(io.to_slice))
    reader_impl = PartiallyCompleteRecord::Reader.new
    reader_impl.read_from_row_group(reader.get_row_group(0), 1)

    out = reader_impl.records
    out[0].i16.should eq(-45)
    out[0].i32.should eq(456)
    out[0].u64.should eq(4563424_u64)
  end

  # Upstream `test_parquet_derive_read_pruned_and_shuffled_columns`.
  it "reads a pruned and shuffled column subset" do
    record = PartiallyCompleteRecord.new(
      true, "a string", -45_i16, 456, 4563424_u64, -365_i64, 3.5_f32,
      Float64::NAN, Time.utc(2024, 3, 14, 15, 9, 26),
      Parquet::Derive::Date.from_time(Time.utc(2015, 3, 14)), sample_uuid,
      Bytes[0x65, 0x66, 0x67],
    )

    io = IO::Memory.new
    write_records(PartiallyCompleteRecord::Writer.new([record]), io)

    reader = SerializedFileReader.new(BytesChunkReader.new(io.to_slice))
    reader_impl = PrunedRecord::Reader.new
    reader_impl.read_from_row_group(reader.get_row_group(0), 1)

    out = reader_impl.records
    out[0].bool.should eq(true)
    out[0].string.should eq("a string")
    out[0].byte_vec.should eq(Bytes[0x65, 0x66, 0x67])
    out[0].float.should eq(3.5_f32)
    out[0].double.nan?.should be_true
    out[0].i16.should eq(-45)
    out[0].i32.should eq(456)
    out[0].u64.should eq(4563424_u64)
    out[0].isize.should eq(-365)
  end
end
