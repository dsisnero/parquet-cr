require "../../spec_helper"

# Ported from `parquet/src/arrow/schema/primitive.rs`
# (`convert_primitive`, `apply_hint`, `from_parquet` and its test module)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# The upstream schema-level tests (`test_flat_primitives`, `test_decimal_fields`,
# ...) exercise this mapping indirectly; the table-driven cases below pin every
# branch of `from_parquet`/`apply_hint` directly. Like upstream's
# `with_type_length` helper, invalid file states (which the builder rejects) are
# constructed by pairing a valid `BasicTypeInfo` with a different physical type.
private alias PArrow = Parquet::Arrow
private alias PSchema = Parquet::Schema
private alias PType = Parquet::Type
private alias PLogical = Parquet::LogicalType
private alias PConverted = Parquet::ConvertedType
private alias PTimeUnit = Parquet::TimeUnit
private alias ArrowDataType = Arrow::DataType

private def primitive(physical : PType, logical : PLogical? = nil, converted : PConverted = PConverted::NONE,
                      length : Int32 = -1, precision : Int32 = -1, scale : Int32 = -1) : PSchema::Type
  builder = PSchema::Type.primitive_type_builder("c", physical)
    .with_repetition(Parquet::Repetition::REQUIRED)
  builder = builder.with_logical_type(logical) if logical
  builder = builder.with_converted_type(converted) unless converted.none?
  builder = builder.with_length(length) if length != -1
  builder = builder.with_precision(precision) if precision != -1
  builder = builder.with_scale(scale) if scale != -1
  builder.build
end

# Rebuilds `template`'s basic info with a different physical type/length/etc.,
# bypassing the builder's validation (upstream tests do the same).
private def raw_primitive(template : PSchema::Type, physical : PType, length : Int32 = -1,
                          scale : Int32 = -1, precision : Int32 = -1) : PSchema::Type
  PSchema::PrimitiveType.new(template.basic_info, physical, length, scale, precision)
end

private def convert(type : PSchema::Type, hint : ArrowDataType? = nil) : ArrowDataType
  PArrow.convert_primitive(type, hint)
end

private def conversion_error(type : PSchema::Type) : String
  error = expect_raises(Parquet::ParquetError) { convert(type) }
  error.message.to_s
end

describe "Parquet::Arrow convert_primitive" do
  it "maps flat physical types without logical annotations" do
    convert(primitive(PType::BOOLEAN)).should eq(ArrowDataType.boolean)
    convert(primitive(PType::INT32)).should eq(ArrowDataType.int32)
    convert(primitive(PType::INT64)).should eq(ArrowDataType.int64)
    convert(primitive(PType::FLOAT)).should eq(ArrowDataType.float32)
    convert(primitive(PType::DOUBLE)).should eq(ArrowDataType.float64)
    convert(primitive(PType::BYTE_ARRAY)).should eq(ArrowDataType.binary)
  end

  it "maps INT96 to a nanosecond timestamp" do
    convert(primitive(PType::INT96)).should eq(ArrowDataType.timestamp(Arrow::TimeUnit::Nanosecond, nil))
  end

  it "maps the UNKNOWN logical type to null" do
    convert(primitive(PType::INT32, PLogical::Unknown.new)).should eq(ArrowDataType.null)
    convert(primitive(PType::BYTE_ARRAY, PLogical::Unknown.new)).should eq(ArrowDataType.null)
  end

  it "maps INT32 integer annotations" do
    convert(primitive(PType::INT32, PLogical.integer(8, true))).should eq(ArrowDataType.int8)
    convert(primitive(PType::INT32, PLogical.integer(16, true))).should eq(ArrowDataType.int16)
    convert(primitive(PType::INT32, PLogical.integer(32, true))).should eq(ArrowDataType.int32)
    convert(primitive(PType::INT32, PLogical.integer(8, false))).should eq(ArrowDataType.uint8)
    convert(primitive(PType::INT32, PLogical.integer(16, false))).should eq(ArrowDataType.uint16)
    convert(primitive(PType::INT32, PLogical.integer(32, false))).should eq(ArrowDataType.uint32)
  end

  it "maps INT32 converted-only annotations" do
    convert(primitive(PType::INT32, converted: PConverted::INT_8)).should eq(ArrowDataType.int8)
    convert(primitive(PType::INT32, converted: PConverted::INT_16)).should eq(ArrowDataType.int16)
    convert(primitive(PType::INT32, converted: PConverted::INT_32)).should eq(ArrowDataType.int32)
    convert(primitive(PType::INT32, converted: PConverted::UINT_8)).should eq(ArrowDataType.uint8)
    convert(primitive(PType::INT32, converted: PConverted::UINT_16)).should eq(ArrowDataType.uint16)
    convert(primitive(PType::INT32, converted: PConverted::UINT_32)).should eq(ArrowDataType.uint32)
    convert(primitive(PType::INT32, converted: PConverted::DATE)).should eq(ArrowDataType.date32)
    convert(primitive(PType::INT32, converted: PConverted::TIME_MILLIS))
      .should eq(ArrowDataType.time32(Arrow::TimeUnit::Millisecond))
  end

  it "maps INT32 date and time logical annotations" do
    convert(primitive(PType::INT32, PLogical::Date.new)).should eq(ArrowDataType.date32)
    convert(primitive(PType::INT32, PLogical.time(false, PTimeUnit::MILLIS)))
      .should eq(ArrowDataType.time32(Arrow::TimeUnit::Millisecond))
  end

  it "rejects incompatible INT32 annotations" do
    time_micros = raw_primitive(primitive(PType::INT64, PLogical.time(false, PTimeUnit::MICROS)), PType::INT32)
    conversion_error(time_micros).should contain("Cannot create INT32 physical type")

    int64_annotated = raw_primitive(primitive(PType::INT64, PLogical.integer(64, true)), PType::INT32)
    conversion_error(int64_annotated).should contain("Cannot create INT32 physical type")

    utf8_converted = raw_primitive(primitive(PType::BYTE_ARRAY, converted: PConverted::UTF8), PType::INT32)
    conversion_error(utf8_converted).should contain("Unable to convert parquet INT32 logical type")
  end

  it "maps INT64 annotations" do
    convert(primitive(PType::INT64, converted: PConverted::INT_64)).should eq(ArrowDataType.int64)
    convert(primitive(PType::INT64, converted: PConverted::UINT_64)).should eq(ArrowDataType.uint64)
    convert(primitive(PType::INT64, PLogical.integer(64, true))).should eq(ArrowDataType.int64)
    convert(primitive(PType::INT64, PLogical.integer(64, false))).should eq(ArrowDataType.uint64)
    convert(primitive(PType::INT64, PLogical.time(false, PTimeUnit::MICROS)))
      .should eq(ArrowDataType.time64(Arrow::TimeUnit::Microsecond))
    convert(primitive(PType::INT64, PLogical.time(false, PTimeUnit::NANOS)))
      .should eq(ArrowDataType.time64(Arrow::TimeUnit::Nanosecond))
    convert(primitive(PType::INT64, converted: PConverted::TIME_MICROS))
      .should eq(ArrowDataType.time64(Arrow::TimeUnit::Microsecond))
  end

  it "maps INT64 timestamps" do
    convert(primitive(PType::INT64, PLogical.timestamp(true, PTimeUnit::MILLIS)))
      .should eq(ArrowDataType.timestamp(Arrow::TimeUnit::Millisecond, "UTC"))
    convert(primitive(PType::INT64, PLogical.timestamp(false, PTimeUnit::MICROS)))
      .should eq(ArrowDataType.timestamp(Arrow::TimeUnit::Microsecond, nil))
    convert(primitive(PType::INT64, PLogical.timestamp(false, PTimeUnit::NANOS)))
      .should eq(ArrowDataType.timestamp(Arrow::TimeUnit::Nanosecond, nil))
    convert(primitive(PType::INT64, converted: PConverted::TIMESTAMP_MILLIS))
      .should eq(ArrowDataType.timestamp(Arrow::TimeUnit::Millisecond, "UTC"))
    convert(primitive(PType::INT64, converted: PConverted::TIMESTAMP_MICROS))
      .should eq(ArrowDataType.timestamp(Arrow::TimeUnit::Microsecond, "UTC"))
  end

  it "rejects MILLIS time on INT64" do
    millis = raw_primitive(primitive(PType::INT32, PLogical.time(false, PTimeUnit::MILLIS)), PType::INT64)
    conversion_error(millis).should contain("Cannot create INT64 from MILLIS time unit")
  end

  it "maps BYTE_ARRAY annotations" do
    convert(primitive(PType::BYTE_ARRAY, PLogical::String.new)).should eq(ArrowDataType.utf8)
    convert(primitive(PType::BYTE_ARRAY, PLogical::Json.new)).should eq(ArrowDataType.utf8)
    convert(primitive(PType::BYTE_ARRAY, PLogical::Bson.new)).should eq(ArrowDataType.binary)
    convert(primitive(PType::BYTE_ARRAY, PLogical::Enum.new)).should eq(ArrowDataType.binary)
    convert(primitive(PType::BYTE_ARRAY, converted: PConverted::UTF8)).should eq(ArrowDataType.utf8)
    convert(primitive(PType::BYTE_ARRAY, converted: PConverted::JSON)).should eq(ArrowDataType.utf8)
    convert(primitive(PType::BYTE_ARRAY, converted: PConverted::BSON)).should eq(ArrowDataType.binary)
    convert(primitive(PType::BYTE_ARRAY, converted: PConverted::ENUM)).should eq(ArrowDataType.binary)
  end

  it "maps decimals by precision" do
    convert(primitive(PType::INT32, PLogical.decimal(2, 4), precision: 4, scale: 2))
      .should eq(ArrowDataType.decimal128(4, 2))
    convert(primitive(PType::INT64, PLogical.decimal(2, 12), precision: 12, scale: 2))
      .should eq(ArrowDataType.decimal128(12, 2))
    convert(primitive(PType::BYTE_ARRAY, PLogical.decimal(2, 30), precision: 30, scale: 2))
      .should eq(ArrowDataType.decimal128(30, 2))
    convert(primitive(PType::BYTE_ARRAY, PLogical.decimal(2, 39), precision: 39, scale: 2))
      .should eq(ArrowDataType.decimal256(39, 2))
    convert(primitive(PType::INT32, converted: PConverted::DECIMAL, precision: 4, scale: 2))
      .should eq(ArrowDataType.decimal128(4, 2))
  end

  it "maps FIXED_LEN_BYTE_ARRAY decimals by length" do
    convert(primitive(PType::FIXED_LEN_BYTE_ARRAY, PLogical.decimal(2, 5),
      length: 16, precision: 5, scale: 2)).should eq(ArrowDataType.decimal128(5, 2))
    convert(primitive(PType::FIXED_LEN_BYTE_ARRAY, PLogical.decimal(2, 5),
      length: 20, precision: 5, scale: 2)).should eq(ArrowDataType.decimal256(5, 2))
    convert(primitive(PType::FIXED_LEN_BYTE_ARRAY, converted: PConverted::DECIMAL,
      length: 16, precision: 5, scale: 2)).should eq(ArrowDataType.decimal128(5, 2))
  end

  it "maps FIXED_LEN_BYTE_ARRAY interval and float16" do
    convert(primitive(PType::FIXED_LEN_BYTE_ARRAY, converted: PConverted::INTERVAL, length: 12))
      .should eq(ArrowDataType.interval(Arrow::IntervalUnit::DayTime))
    convert(primitive(PType::FIXED_LEN_BYTE_ARRAY, PLogical::Float16.new, length: 2))
      .should eq(ArrowDataType.float16)
    convert(primitive(PType::FIXED_LEN_BYTE_ARRAY, length: 20))
      .should eq(ArrowDataType.fixed_size_binary(20))
  end

  it "rejects FLOAT16 with the wrong length" do
    float16_valid = primitive(PType::FIXED_LEN_BYTE_ARRAY, PLogical::Float16.new, length: 2)
    conversion_error(raw_primitive(float16_valid, PType::FIXED_LEN_BYTE_ARRAY, length: 4))
      .should contain("FLOAT16 logical type must be Fixed Length Byte Array with length 2")
  end

  # Upstream `decimal_logical_rejects_invalid_length` /
  # `decimal_converted_rejects_invalid_length`.
  it "rejects decimals with an invalid fixed length" do
    decimal_logical = primitive(PType::FIXED_LEN_BYTE_ARRAY, PLogical.decimal(2, 5),
      length: 16, precision: 5, scale: 2)
    decimal_converted = primitive(PType::FIXED_LEN_BYTE_ARRAY, converted: PConverted::DECIMAL,
      length: 16, precision: 5, scale: 2)

    [-1, 0, 33].each do |bad_length|
      conversion_error(raw_primitive(decimal_logical, PType::FIXED_LEN_BYTE_ARRAY, length: bad_length))
        .should contain("DECIMAL")
      conversion_error(raw_primitive(decimal_converted, PType::FIXED_LEN_BYTE_ARRAY, length: bad_length))
        .should contain("DECIMAL")
    end
  end

  # Upstream `decimal_accepts_valid_lengths`.
  it "accepts decimal fixed lengths 16 and 32" do
    decimal_logical = primitive(PType::FIXED_LEN_BYTE_ARRAY, PLogical.decimal(2, 5),
      length: 16, precision: 5, scale: 2)

    convert(raw_primitive(decimal_logical, PType::FIXED_LEN_BYTE_ARRAY, length: 16))
      .should eq(ArrowDataType.decimal128(5, 2))
    convert(raw_primitive(decimal_logical, PType::FIXED_LEN_BYTE_ARRAY, length: 32))
      .should eq(ArrowDataType.decimal256(5, 2))
  end

  # Upstream `interval_rejects_wrong_length` / `interval_accepts_length_12`.
  it "checks the interval fixed length" do
    interval = primitive(PType::FIXED_LEN_BYTE_ARRAY, converted: PConverted::INTERVAL, length: 12)

    [0, 11, 13].each do |bad_length|
      conversion_error(raw_primitive(interval, PType::FIXED_LEN_BYTE_ARRAY, length: bad_length))
        .should contain("INTERVAL")
    end

    convert(interval).should eq(ArrowDataType.interval(Arrow::IntervalUnit::DayTime))
  end

  # `apply_hint`: types that Parquet cannot represent are recovered from the
  # embedded arrow schema.
  it "applies arrow type hints" do
    convert(primitive(PType::INT64), ArrowDataType.timestamp(Arrow::TimeUnit::Second, "UTC"))
      .should eq(ArrowDataType.timestamp(Arrow::TimeUnit::Second, "UTC"))
    convert(primitive(PType::INT32), ArrowDataType.time32(Arrow::TimeUnit::Millisecond))
      .should eq(ArrowDataType.time32(Arrow::TimeUnit::Millisecond))
    convert(primitive(PType::INT64), ArrowDataType.time64(Arrow::TimeUnit::Nanosecond))
      .should eq(ArrowDataType.time64(Arrow::TimeUnit::Nanosecond))
    convert(primitive(PType::INT64), ArrowDataType.duration(Arrow::TimeUnit::Microsecond))
      .should eq(ArrowDataType.duration(Arrow::TimeUnit::Microsecond))
    convert(primitive(PType::INT64), ArrowDataType.date64).should eq(ArrowDataType.date64)
    convert(primitive(PType::INT32, PLogical::Date.new), ArrowDataType.date64).should eq(ArrowDataType.date64)
    convert(primitive(PType::BYTE_ARRAY, PLogical::String.new), ArrowDataType.large_utf8)
      .should eq(ArrowDataType.large_utf8)
    convert(primitive(PType::BYTE_ARRAY), ArrowDataType.utf8).should eq(ArrowDataType.utf8)
    convert(primitive(PType::BYTE_ARRAY), ArrowDataType.binary_view).should eq(ArrowDataType.binary_view)
    convert(primitive(PType::BYTE_ARRAY, PLogical::String.new), ArrowDataType.utf8_view)
      .should eq(ArrowDataType.utf8_view)
    convert(primitive(PType::FIXED_LEN_BYTE_ARRAY, converted: PConverted::INTERVAL, length: 12),
      ArrowDataType.interval(Arrow::IntervalUnit::MonthDayNano))
      .should eq(ArrowDataType.interval(Arrow::IntervalUnit::MonthDayNano))
    convert(primitive(PType::BYTE_ARRAY, PLogical.decimal(2, 5), precision: 5, scale: 2),
      ArrowDataType.decimal256(5, 2)).should eq(ArrowDataType.decimal256(5, 2))
  end

  it "preserves dictionary hints whose value type matches" do
    dictionary_type = ArrowDataType.dictionary(ArrowDataType.int32, ArrowDataType.utf8)
    convert(primitive(PType::BYTE_ARRAY, PLogical::String.new), dictionary_type).should eq(dictionary_type)
  end

  it "drops dictionary hints whose value type does not survive the hint" do
    dictionary_type = ArrowDataType.dictionary(ArrowDataType.int8, ArrowDataType.int64)
    convert(primitive(PType::INT32), dictionary_type).should eq(ArrowDataType.int32)
  end

  it "keeps the inferred type when no hint applies" do
    convert(primitive(PType::INT32), ArrowDataType.int32).should eq(ArrowDataType.int32)
    convert(primitive(PType::INT64), ArrowDataType.float64).should eq(ArrowDataType.int64)
  end
end
