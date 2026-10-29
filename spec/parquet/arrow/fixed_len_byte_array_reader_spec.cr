require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/fixed_len_byte_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `make_fixed_len_byte_array_reader` and `FixedLenByteArrayReader`.
#
# Divergences (documented in `plans/parity.md`): a contiguous native byte buffer
# feeds the dedicated fixed-width decoder and converts to the target array at
# `consume_batch` time; decimals/intervals are represented as `PrimitiveArray(T)`
# with the matching decimal/interval data type (arrow-cr has no
# `Decimal*Array`/`Interval*Array` classes); Float16 uses arrow-cr's half-float
# primitive array, and Interval(MonthDayNano) remains NYI like upstream.
private alias PArrow = Parquet::Arrow
private alias PageUtil = ParquetTestPageUtil

private FLBA_16         = "message test_schema { REQUIRED FIXED_LEN_BYTE_ARRAY (16) leaf; }"
private FLBA_4          = "message test_schema { REQUIRED FIXED_LEN_BYTE_ARRAY (4) leaf; }"
private NULLABLE_FLBA_4 = "message test_schema { OPTIONAL FIXED_LEN_BYTE_ARRAY (4) leaf; }"
private FLBA_12         = "message test_schema { REQUIRED FIXED_LEN_BYTE_ARRAY (12) leaf; }"

# Big-endian bytes of `value` in `size` bytes (decimals are big-endian).
private def be_bytes(value : Int, size : Int32) : Bytes
  Bytes.new(size) { |i| ((value >> (8 * (size - 1 - i))) & 0xff).to_u8 }
end

# Little-endian bytes of `value` in `size` bytes (intervals are little-endian).
private def le_bytes(value : Int, size : Int32) : Bytes
  Bytes.new(size) { |i| ((value >> (8 * i)) & 0xff).to_u8 }
end

private def flba_reader(values : Array(Bytes), message_type : String,
                        arrow_type : Arrow::DataType,
                        def_levels : Array(Int16)? = nil,
                        padding_threshold : Int16? = nil) : PArrow::ArrayReader
  desc = PageUtil.column_descriptor(message_type)
  num_values = def_levels.try(&.size) || values.size
  builder = PageUtil::DataPageBuilderImpl.new(desc, num_values.to_u32, true)
  builder.add_def_levels(desc.max_def_level, def_levels.not_nil!) if def_levels
  builder.add_values(
    Parquet::Encoding::PLAIN,
    values.map { |value| Parquet::FixedLenByteArray.new(Parquet::ByteArray.new(value.to_a)) }
  )
  PArrow.make_fixed_len_byte_array_reader(
    PageUtil::InMemoryPageIterator.new([[builder.consume]]), desc, arrow_type, 1024,
    padding_threshold
  )
end

describe Parquet::Arrow::FixedLenByteArrayReader do
  it "reads decimal128 values (big-endian)" do
    values = [be_bytes(1_i128, 16), be_bytes(-2_i128, 16)]
    reader = flba_reader(values, FLBA_16, Arrow::DataType.decimal128(25, 2))

    reader.data_type.should eq(Arrow::DataType.decimal128(25, 2))
    reader.read_records(2).should eq(2)
    array = reader.consume_batch.as(Arrow::PrimitiveArray(Int128))
    array.data_type.should eq(Arrow::DataType.decimal128(25, 2))
    Array(Int128).new(2) { |idx| array.value(idx) }.should eq([1_i128, -2_i128])
  end

  it "reads decimal32 and decimal64 values" do
    reader = flba_reader([be_bytes(5_i32, 4)], FLBA_4, Arrow::DataType.decimal32(9, 2))
    array = reader.next_batch(1).as(Arrow::PrimitiveArray(Int32))
    array.value(0).should eq(5)

    message = "message test_schema { REQUIRED FIXED_LEN_BYTE_ARRAY (8) leaf; }"
    reader = flba_reader([be_bytes(-7_i64, 8)], message, Arrow::DataType.decimal64(18, 2))
    array = reader.next_batch(1).as(Arrow::PrimitiveArray(Int64))
    array.value(0).should eq(-7_i64)
  end

  it "reads decimal256 values" do
    message = "message test_schema { REQUIRED FIXED_LEN_BYTE_ARRAY (32) leaf; }"
    value = Bytes.new(32)
    value[31] = 7_u8
    reader = flba_reader([value], message, Arrow::DataType.decimal256(40, 5))

    array = reader.next_batch(1).as(Arrow::PrimitiveArray(Arrow::I256))
    array.value(0).should eq(Arrow::I256.from_i128(7_i128))
  end

  it "reads fixed size binary values" do
    reader = flba_reader(["abcd".to_slice, "wxyz".to_slice], FLBA_4,
      Arrow::DataType.fixed_size_binary(4))

    array = reader.next_batch(2).as(Arrow::FixedSizeBinaryArray)
    array.len.should eq(2)
    array.value(0).should eq("abcd".to_slice)
    array.value(1).should eq("wxyz".to_slice)
  end

  it "reads interval year-month and day-time values (little-endian)" do
    # months = 14 in the first 4 bytes.
    year_month = Bytes.new(12)
    le_bytes(14, 4).copy_to(year_month)
    reader = flba_reader([year_month], FLBA_12,
      Arrow::DataType.interval(Arrow::IntervalUnit::YearMonth))
    array = reader.next_batch(1).as(Arrow::PrimitiveArray(Int32))
    array.value(0).should eq(14)

    # days = 3 (bytes 4..8), milliseconds = 2500 (bytes 8..12).
    day_time = Bytes.new(12)
    le_bytes(3, 4).copy_to(day_time[4, 4])
    le_bytes(2500, 4).copy_to(day_time[8, 4])
    reader = flba_reader([day_time], FLBA_12,
      Arrow::DataType.interval(Arrow::IntervalUnit::DayTime))
    array = reader.next_batch(1).as(Arrow::PrimitiveArray(Int64))
    array.value(0).should eq(3_i64 | (2500_i64 << 32))
  end

  it "applies null padding" do
    reader = flba_reader(
      ["abcd".to_slice, "wxyz".to_slice],
      NULLABLE_FLBA_4,
      Arrow::DataType.fixed_size_binary(4),
      def_levels: [1_i16, 0_i16, 1_i16]
    )

    array = reader.next_batch(3).as(Arrow::FixedSizeBinaryArray)
    array.len.should eq(3)
    array.null_count.should eq(1)
    array.null?(1).should be_true
    array.value(0).should eq("abcd".to_slice)
  end

  it "rejects unsupported arrow types" do
    expect_raises(Parquet::ParquetError, /invalid data type for fixed length byte array reader/) do
      flba_reader(["abcd".to_slice], NULLABLE_FLBA_4, Arrow::DataType.int32)
    end
  end

  it "rejects intervals that are not 12 bytes" do
    expect_raises(Parquet::ParquetError, /interval type must consist of 12 bytes/) do
      flba_reader(["abcd".to_slice], NULLABLE_FLBA_4,
        Arrow::DataType.interval(Arrow::IntervalUnit::YearMonth))
    end
  end

  it "reads Float16 little-endian values with exact bit patterns" do
    message = "message test_schema { REQUIRED FIXED_LEN_BYTE_ARRAY (2) leaf; }"
    reader = flba_reader([Bytes[0x00, 0xBE], Bytes[0x80, 0x40]], message, Arrow::DataType.float16)
    array = reader.next_batch(2).as(Arrow::PrimitiveArray(Arrow::Float16))
    array.value(0).bits.should eq 0xBE00_u16
    array.value(1).bits.should eq 0x4080_u16
    array.value(0).to_f32.should eq(-1.5_f32)
    array.value(1).to_f32.should eq(2.25_f32)
  end
end
