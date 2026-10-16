require "../../spec_helper"

# Ported from `parquet/src/record/api.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: Rust enum construction `Field::Int(3)` maps to
# `Field::Int.new(3)`; `f16` is the decoded `Float64` value; `serde_json::Value`
# is `JSON::Any`.

private alias R = Parquet::Record
private alias Field = Parquet::Record::Field
private alias Row = Parquet::Record::Row

private def bytes_of(values : Array(Int32)) : Bytes
  Bytes.new(values.size) { |i| values[i].to_u8 }
end

private def f(name : String, value : Field) : Tuple(String, Field)
  pair : Tuple(String, Field) = {name, value}
  pair
end

private def fields(*values : Field) : Array(Field)
  result = [] of Field
  values.each { |value| result << value }
  result
end

private def entry(key : Field, value : Field) : Tuple(Field, Field)
  pair : Tuple(Field, Field) = {key, value}
  pair
end

private def make_column_descr(
  physical : Parquet::Type,
  converted : Parquet::ConvertedType,
  length : Int32 = -1,
  precision : Int32 = -1,
  scale : Int32 = -1,
) : Parquet::Schema::ColumnDescriptor
  builder = Parquet::Schema::Type.primitive_type_builder("col", physical)
    .with_converted_type(converted)
  builder = builder.with_length(length)
  builder = builder.with_precision(precision)
  builder = builder.with_scale(scale)
  Parquet::Schema::ColumnDescriptor.new(
    builder.build, 0_i16, 0_i16, Parquet::Schema::ColumnPath.new("col")
  )
end

private F16_PI       = Parquet::Record.half_to_f64(0x4248_u16)
private F16_E        = Parquet::Record.half_to_f64(0x4170_u16)
private F16_ONE      = Parquet::Record.half_to_f64(0x3C00_u16)
private F16_MAX      = Parquet::Record.half_to_f64(0x7BFF_u16)
private F16_NEG_ZERO = Parquet::Record.half_to_f64(0x8000_u16)

describe Parquet::Record::Field do
  # Upstream `test_row_convert_bool`.
  it "converts bool" do
    descr = make_column_descr(Parquet::Type::BOOLEAN, Parquet::ConvertedType::NONE)
    Field.convert_bool(descr, true).should eq(Field::Bool.new(true))
    Field.convert_bool(descr, false).should eq(Field::Bool.new(false))
  end

  # Upstream `test_row_convert_int32`.
  it "converts int32 by converted type" do
    Field.convert_int32(make_column_descr(Parquet::Type::INT32, PCT::INT_8), 111).should eq(Field::Byte.new(111_i8))
    Field.convert_int32(make_column_descr(Parquet::Type::INT32, PCT::INT_16), 222).should eq(Field::Short.new(222_i16))
    Field.convert_int32(make_column_descr(Parquet::Type::INT32, PCT::INT_32), 333).should eq(Field::Int.new(333))
    Field.convert_int32(make_column_descr(Parquet::Type::INT32, PCT::UINT_8), -1).should eq(Field::UByte.new(255_u8))
    Field.convert_int32(make_column_descr(Parquet::Type::INT32, PCT::UINT_16), 256).should eq(Field::UShort.new(256_u16))
    Field.convert_int32(make_column_descr(Parquet::Type::INT32, PCT::UINT_32), 1234).should eq(Field::UInt.new(1234_u32))
    Field.convert_int32(make_column_descr(Parquet::Type::INT32, PCT::NONE), 444).should eq(Field::Int.new(444))
    Field.convert_int32(make_column_descr(Parquet::Type::INT32, PCT::DATE), 14611).should eq(Field::Date.new(14611))
    Field.convert_int32(make_column_descr(Parquet::Type::INT32, PCT::TIME_MILLIS), 14611).should eq(Field::TimeMillis.new(14611))
    Field.convert_int32(make_column_descr(Parquet::Type::INT32, PCT::DECIMAL, 0, 8, 2), 444)
      .should eq(Field::Decimal.new(Parquet::Decimal.from_i32(444, 8, 2)))
  end

  # Upstream `test_row_convert_int64`.
  it "converts int64 by converted type" do
    Field.convert_int64(make_column_descr(Parquet::Type::INT64, PCT::INT_64), 1111).should eq(Field::Long.new(1111))
    Field.convert_int64(make_column_descr(Parquet::Type::INT64, PCT::UINT_64), 78239823).should eq(Field::ULong.new(78239823_u64))
    Field.convert_int64(make_column_descr(Parquet::Type::INT64, PCT::TIMESTAMP_MILLIS), 1541186529153)
      .should eq(Field::TimestampMillis.new(1541186529153))
    Field.convert_int64(make_column_descr(Parquet::Type::INT64, PCT::TIMESTAMP_MICROS), 1541186529153123)
      .should eq(Field::TimestampMicros.new(1541186529153123))
    Field.convert_int64(make_column_descr(Parquet::Type::INT64, PCT::TIME_MICROS), 47445123456)
      .should eq(Field::TimeMicros.new(47445123456))
    Field.convert_int64(make_column_descr(Parquet::Type::INT64, PCT::NONE), 2222).should eq(Field::Long.new(2222))
    Field.convert_int64(make_column_descr(Parquet::Type::INT64, PCT::DECIMAL, 0, 8, 2), 3333)
      .should eq(Field::Decimal.new(Parquet::Decimal.from_i64(3333, 8, 2)))
  end

  # Upstream `test_row_convert_int96`.
  it "converts int96 to timestamp millis" do
    descr = make_column_descr(Parquet::Type::INT96, Parquet::ConvertedType::NONE)
    value = Parquet::Int96.new(StaticArray[0_u32, 0_u32, 2454923_u32])
    Field.convert_int96(descr, value).should eq(Field::TimestampMillis.new(1238544000000))
    value = Parquet::Int96.new(StaticArray[4165425152_u32, 13_u32, 2454923_u32])
    Field.convert_int96(descr, value).should eq(Field::TimestampMillis.new(1238544060000))
  end

  # Upstream `test_row_convert_float` / `test_row_convert_double`.
  it "converts float and double" do
    Field.convert_float(make_column_descr(Parquet::Type::FLOAT, Parquet::ConvertedType::NONE), 2.31_f32)
      .should eq(Field::Float.new(2.31_f32))
    Field.convert_double(make_column_descr(Parquet::Type::DOUBLE, Parquet::ConvertedType::NONE), 1.56)
      .should eq(Field::Double.new(1.56))
  end

  # Upstream `test_row_convert_byte_array`.
  it "converts byte arrays by converted type" do
    byte_array = ->(values : Array(Int32)) { Parquet::ByteArray.new(bytes_of(values)) }

    Field.convert_byte_array(make_column_descr(Parquet::Type::BYTE_ARRAY, PCT::UTF8), byte_array.call([65, 66, 67, 68]))
      .should eq(Field::Str.new("ABCD"))
    Field.convert_byte_array(make_column_descr(Parquet::Type::BYTE_ARRAY, PCT::ENUM), byte_array.call([49, 50, 51]))
      .should eq(Field::Str.new("123"))
    Field.convert_byte_array(make_column_descr(Parquet::Type::BYTE_ARRAY, PCT::JSON), byte_array.call([123, 34, 97, 34, 58, 49, 125]))
      .should eq(Field::Str.new(%({"a":1})))
    Field.convert_byte_array(make_column_descr(Parquet::Type::BYTE_ARRAY, PCT::NONE), byte_array.call([1, 2, 3, 4, 5]))
      .should eq(Field::Bytes.new(byte_array.call([1, 2, 3, 4, 5])))
    Field.convert_byte_array(make_column_descr(Parquet::Type::BYTE_ARRAY, PCT::BSON), byte_array.call([1, 2, 3, 4, 5]))
      .should eq(Field::Bytes.new(byte_array.call([1, 2, 3, 4, 5])))
    Field.convert_byte_array(make_column_descr(Parquet::Type::BYTE_ARRAY, PCT::DECIMAL, 0, 8, 2), byte_array.call([207, 200]))
      .should eq(Field::Decimal.new(Parquet::Decimal.from_bytes(byte_array.call([207, 200]), 8, 2)))
    Field.convert_byte_array(make_column_descr(Parquet::Type::FIXED_LEN_BYTE_ARRAY, PCT::DECIMAL, 8, 17, 5), byte_array.call([0, 0, 0, 0, 0, 4, 147, 224]))
      .should eq(Field::Decimal.new(Parquet::Decimal.from_bytes(byte_array.call([0, 0, 0, 0, 0, 4, 147, 224]), 17, 5)))
    Field.convert_byte_array(make_column_descr(Parquet::Type::FIXED_LEN_BYTE_ARRAY, PCT::NONE, 6), byte_array.call([1, 2, 3, 4, 5, 6]))
      .should eq(Field::Bytes.new(byte_array.call([1, 2, 3, 4, 5, 6])))

    f16_descr = Parquet::Schema::ColumnDescriptor.new(
      Parquet::Schema::Type.primitive_type_builder("col", Parquet::Type::FIXED_LEN_BYTE_ARRAY)
        .with_logical_type(Parquet::LogicalType::Float16.new)
        .with_length(2)
        .build,
      0_i16, 0_i16, Parquet::Schema::ColumnPath.new("col")
    )
    Field.convert_byte_array(f16_descr, Parquet::ByteArray.new(Bytes[0x48_u8, 0x42_u8]))
      .should eq(Field::Float16.new(F16_PI))
  end
end

describe Parquet::Record do
  # Upstream `test_convert_date_to_string`.
  it "converts dates to strings" do
    check = ->(y : Int32, m : Int32, d : Int32) do
      dt = Time.utc(y, m, d)
      res = R.convert_date_to_string((dt.to_unix // 86400).to_i32)
      res.should eq(dt.to_s("%Y-%m-%d"))
    end
    check.call(1969, 12, 31)
    check.call(2010, 1, 2)
    check.call(2014, 5, 1)
    check.call(2016, 2, 29)
    check.call(2017, 9, 12)
    check.call(2018, 3, 31)
  end

  # Upstream `test_convert_timestamp_millis_to_string`.
  it "converts timestamp millis to strings" do
    check = ->(y : Int32, m : Int32, d : Int32, h : Int32, mi : Int32, s : Int32, ms : Int32, exp : String) do
      dt = Time.utc(y, m, d, h, mi, s, nanosecond: ms * 1_000_000)
      R.convert_timestamp_millis_to_string(dt.to_unix_ms).should eq(exp)
    end
    check.call(1969, 9, 10, 1, 2, 3, 4, "1969-09-10 01:02:03.004 +00:00")
    check.call(2010, 1, 2, 13, 12, 54, 42, "2010-01-02 13:12:54.042 +00:00")
    check.call(2011, 1, 3, 8, 23, 1, 27, "2011-01-03 08:23:01.027 +00:00")
    check.call(2012, 4, 5, 11, 6, 32, 0, "2012-04-05 11:06:32.000 +00:00")
    check.call(2013, 5, 12, 16, 38, 0, 15, "2013-05-12 16:38:00.015 +00:00")
    check.call(2014, 11, 28, 21, 15, 12, 59, "2014-11-28 21:15:12.059 +00:00")
  end

  # Upstream `test_convert_timestamp_micros_to_string`.
  it "converts timestamp micros to strings" do
    check = ->(y : Int32, m : Int32, d : Int32, h : Int32, mi : Int32, s : Int32, us : Int32, exp : String) do
      dt = Time.utc(y, m, d, h, mi, s, nanosecond: us * 1000)
      R.convert_timestamp_micros_to_string((dt.to_unix_ns // 1000).to_i64).should eq(exp)
    end
    check.call(1969, 9, 10, 1, 2, 3, 4, "1969-09-10 01:02:03.000004 +00:00")
    check.call(2010, 1, 2, 13, 12, 54, 42, "2010-01-02 13:12:54.000042 +00:00")
    check.call(2011, 1, 3, 8, 23, 1, 27, "2011-01-03 08:23:01.000027 +00:00")
    check.call(2012, 4, 5, 11, 6, 32, 0, "2012-04-05 11:06:32.000000 +00:00")
    check.call(2013, 5, 12, 16, 38, 0, 15, "2013-05-12 16:38:00.000015 +00:00")
    check.call(2014, 11, 28, 21, 15, 12, 59, "2014-11-28 21:15:12.000059 +00:00")
  end

  # Upstream `test_convert_float16_to_string`.
  it "converts float16 to strings" do
    Field::Float16.new(F16_ONE).to_s.should eq("1.0")
    Field::Float16.new(F16_PI).to_s.should eq("3.140625")
    Field::Float16.new(F16_MAX).to_s.should eq("65504.0")
    Field::Float16.new(Float64::NAN).to_s.should eq("NaN")
    Field::Float16.new(Float64::INFINITY).to_s.should eq("inf")
    Field::Float16.new(-Float64::INFINITY).to_s.should eq("-inf")
    Field::Float16.new(0.0).to_s.should eq("0.0")
    Field::Float16.new(F16_NEG_ZERO).to_s.should eq("-0.0")
  end

  # Upstream `test_convert_float_to_string`.
  it "converts float to strings" do
    Field::Float.new(1.0_f32).to_s.should eq("1.0")
    Field::Float.new(9.63_f32).to_s.should eq("9.63")
    Field::Float.new(1e-15_f32).to_s.should eq("0.000000000000001")
    Field::Float.new(1e-16_f32).to_s.should eq("1E-16")
    Field::Float.new(1e19_f32).to_s.should eq("10000000000000000000.0")
    Field::Float.new(1e20_f32).to_s.should eq("1E20")
    Field::Float.new(1.7976931e30_f32).to_s.should eq("1.7976931E30")
    Field::Float.new(-1.7976931e30_f32).to_s.should eq("-1.7976931E30")
  end

  # Upstream `test_convert_double_to_string`.
  it "converts double to strings" do
    Field::Double.new(1.0).to_s.should eq("1.0")
    Field::Double.new(9.63).to_s.should eq("9.63")
    Field::Double.new(1e-15).to_s.should eq("0.000000000000001")
    Field::Double.new(1e-16).to_s.should eq("1E-16")
    Field::Double.new(1e19).to_s.should eq("10000000000000000000.0")
    Field::Double.new(1e20).to_s.should eq("1E20")
    Field::Double.new(1.79769313486e308).to_s.should eq("1.79769313486E308")
    Field::Double.new(-1.79769313486e308).to_s.should eq("-1.79769313486E308")
  end

  # Upstream `test_convert_decimal_to_string`.
  it "converts decimals to strings" do
    check = ->(bytes : Array(Int32), precision : Int32, scale : Int32, res : String) do
      decimal = Parquet::Decimal.from_bytes(
        Parquet::ByteArray.new(bytes_of(bytes)), precision, scale
      )
      R.convert_decimal_to_string(decimal).should eq(res)
    end

    check.call([0, 0, 0, 0, 0, 0, 0, 0, 13, 224, 182, 179, 167, 100, 0, 0], 38, 18, "1.000000000000000000")
    check.call([249, 233, 247, 16, 185, 192, 202, 223, 215, 165, 192, 166, 67, 72], 36, 28, "-12344.0242342304923409234234293432")
    check.call([0, 0, 0, 0, 0, 4, 147, 224], 17, 5, "3.00000")
    check.call([0, 0, 0, 0, 1, 201, 195, 140], 18, 2, "300000.12")
    check.call([207, 200], 10, 2, "-123.44")
    check.call([207, 200], 10, 8, "-0.00012344")
    check.call([48, 57], 5, 5, "0.12345")
    check.call([207, 199], 5, 5, "-0.12345")
    check.call([45], 5, 5, "0.00045")
  end
end

describe Parquet::Record::Row do
  # Upstream `test_row_display`.
  it "displays primitive and complex fields" do
    Field::Null.new.to_s.should eq("null")
    Field::Bool.new(true).to_s.should eq("true")
    Field::Byte.new(1_i8).to_s.should eq("1")
    Field::Short.new(2_i16).to_s.should eq("2")
    Field::Int.new(3).to_s.should eq("3")
    Field::Long.new(4_i64).to_s.should eq("4")
    Field::UByte.new(1_u8).to_s.should eq("1")
    Field::UShort.new(2_u16).to_s.should eq("2")
    Field::UInt.new(3_u32).to_s.should eq("3")
    Field::ULong.new(4_u64).to_s.should eq("4")
    Field::Float16.new(F16_E).to_s.should eq("2.71875")
    Field::Float.new(5.0_f32).to_s.should eq("5.0")
    Field::Float.new(5.1234_f32).to_s.should eq("5.1234")
    Field::Double.new(6.0).to_s.should eq("6.0")
    Field::Double.new(6.1234).to_s.should eq("6.1234")
    Field::Str.new("abc").to_s.should eq(%("abc"))
    Field::Bytes.new(Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8])).to_s.should eq("[1, 2, 3]")

    Row.new([f("x", Field::Null.new), f("Y", Field::Int.new(2)), f("z", Field::Float.new(3.1_f32)), f("a", Field::Str.new("abc"))])
      .to_s.should eq(%({x: null, Y: 2, z: 3.1, a: "abc"}))
    Field::ListInternal.new(R.make_list(fields(Field::Int.new(2), Field::Int.new(1), Field::Null.new, Field::Int.new(12))))
      .to_s.should eq("[2, 1, null, 12]")
    Field::MapInternal.new(R.make_map([entry(Field::Int.new(1), Field::Float.new(1.2_f32)), entry(Field::Int.new(2), Field::Float.new(4.5_f32)), entry(Field::Int.new(3), Field::Float.new(2.3_f32))] of Tuple(Field, Field)))
      .to_s.should eq("{1 -> 1.2, 2 -> 4.5, 3 -> 2.3}")
  end

  # Upstream `test_is_primitive`.
  it "reports primitives" do
    Field::Null.new.primitive?.should be_true
    Field::Bool.new(true).primitive?.should be_true
    Field::Int.new(3).primitive?.should be_true
    Field::Long.new(4_i64).primitive?.should be_true
    Field::Str.new("abc").primitive?.should be_true
    Field::Bytes.new(Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8])).primitive?.should be_true
    Field::TimestampMillis.new(12345678).primitive?.should be_true
    Field::Decimal.new(Parquet::Decimal.from_i32(4, 8, 2)).primitive?.should be_true
    Field::Group.new(Row.new([f("x", Field::Null.new)])).primitive?.should be_false
    Field::ListInternal.new(R.make_list(fields(Field::Int.new(2)))).primitive?.should be_false
    Field::MapInternal.new(R.make_map([entry(Field::Int.new(1), Field::Float.new(1.2_f32))] of Tuple(Field, Field))).primitive?.should be_false
  end

  # Upstream `test_row_primitive_accessors`.
  it "accesses primitive fields" do
    row = Row.new([
      f("a", Field::Null.new),
      f("b", Field::Bool.new(false)),
      f("c", Field::Byte.new(3_i8)),
      f("d", Field::Short.new(4_i16)),
      f("e", Field::Int.new(5)),
      f("f", Field::Long.new(6_i64)),
      f("g", Field::UByte.new(3_u8)),
      f("h", Field::UShort.new(4_u16)),
      f("i", Field::UInt.new(5_u32)),
      f("j", Field::ULong.new(6_u64)),
      f("k", Field::Float.new(7.1_f32)),
      f("l", Field::Double.new(8.1)),
      f("m", Field::Str.new("abc")),
      f("n", Field::Bytes.new(Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8]))),
      f("o", Field::Decimal.new(Parquet::Decimal.from_i32(4, 7, 2))),
      f("p", Field::Float16.new(9.1015625)),
    ])

    row.is_null(0).should be_true
    row.is_null(1).should be_false
    row.get_bool(1).should be_false
    row.get_byte(2).should eq(3)
    row.get_short(3).should eq(4)
    row.get_int(4).should eq(5)
    row.get_long(5).should eq(6)
    row.get_ubyte(6).should eq(3)
    row.get_ushort(7).should eq(4)
    row.get_uint(8).should eq(5)
    row.get_ulong(9).should eq(6)
    (row.get_float(10) - 7.1_f32).abs.should be < 1e-6
    (row.get_double(11) - 8.1).abs.should be < 1e-12
    row.get_string(12).should eq("abc")
    row.get_bytes(13).len.should eq(5)
    row.get_decimal(14).precision.should eq(7)
    (row.get_float16(15) - 9.1).abs.should be < 0.0078125

    error = expect_raises(Parquet::IndexOutOfBoundError) { row.is_null(16) }
    error.index.should eq(16)
    error.bound.should eq(16)
    expect_raises(Parquet::IndexOutOfBoundError) { row.get_bool(16) }
    expect_raises(Parquet::IndexOutOfBoundError) { row.get_int(16) }
    expect_raises(Parquet::IndexOutOfBoundError) { row.get_string(16) }
    expect_raises(Parquet::IndexOutOfBoundError) { row.get_decimal(16) }
  end

  # Upstream `test_row_primitive_invalid_accessors`.
  it "errors when accessing primitives as groups" do
    row = Row.new(fields_pair_helper)
    row.len.times do |i|
      expect_raises(Parquet::GeneralError) { row.get_group(i) }
    end
  end

  # Upstream `test_row_complex_accessors` / `test_row_complex_invalid_accessors`.
  it "accesses complex fields" do
    group = Field::Group.new(Row.new([f("x", Field::Null.new), f("Y", Field::Int.new(2))]))
    list = Field::ListInternal.new(R.make_list(fields(Field::Int.new(2), Field::Int.new(1), Field::Null.new, Field::Int.new(12))))
    map = Field::MapInternal.new(R.make_map([entry(Field::Int.new(1), Field::Float.new(1.2_f32)), entry(Field::Int.new(2), Field::Float.new(4.5_f32)), entry(Field::Int.new(3), Field::Float.new(2.3_f32))] of Tuple(Field, Field)))
    row = Row.new([f("a", group), f("b", list), f("c", map)])

    row.get_group(0).len.should eq(2)
    row.get_list(1).len.should eq(4)
    row.get_map(2).len.should eq(3)

    expect_raises(Parquet::GeneralError, "Cannot access Group as Float") { row.get_float(0) }
    expect_raises(Parquet::GeneralError, "Cannot access ListInternal as Float") { row.get_float(1) }
    expect_raises(Parquet::GeneralError, "Cannot access MapInternal as Float") { row.get_float(2) }
  end

  # Upstream `test_row_primitive_field_fmt` / `test_row_complex_field_fmt`.
  it "formats fields by index" do
    row = Row.new([
      f("00", Field::Null.new),
      f("01", Field::Bool.new(false)),
      f("02", Field::Int.new(3)),
      f("03", Field::Float.new(11.1_f32)),
      f("04", Field::Double.new(12.1)),
      f("05", Field::Str.new("abc")),
      f("06", Field::Decimal.new(Parquet::Decimal.from_i32(4, 7, 2))),
      f("07", Field::Float16.new(F16_PI)),
    ])
    row.fmt(0).should eq("null")
    row.fmt(1).should eq("false")
    row.fmt(2).should eq("3")
    row.fmt(3).should eq("11.1")
    row.fmt(4).should eq("12.1")
    row.fmt(5).should eq(%("abc"))
    row.fmt(6).should eq("0.04")
    row.fmt(7).should eq("3.140625")
  end

  # Upstream `test_field_visibility`.
  it "exposes column iterator" do
    row = Row.new([f("a", Field::Group.new(Row.new([f("x", Field::Null.new), f("Y", Field::Int.new(2))])))])
    column = row.get_column_iter.first
    column[0].should eq("a")
    column[1].as(Field::Group).value.should eq(Row.new([f("x", Field::Null.new), f("Y", Field::Int.new(2))]))
  end

  # Upstream `test_to_json_value`.
  it "converts to json" do
    Field::Null.new.to_json_value.should eq(JSON::Any.new(nil))
    Field::Bool.new(true).to_json_value.should eq(JSON::Any.new(true))
    Field::Byte.new(1_i8).to_json_value.should eq(JSON::Any.new(1_i64))
    Field::Int.new(3).to_json_value.should eq(JSON::Any.new(3_i64))
    Field::ULong.new(4_u64).to_json_value.should eq(JSON::Any.new(4_i64))
    Field::Float.new(5.0_f32).to_json_value.should eq(JSON::Any.new(5.0))
    Field::Float.new(5.1234_f32).to_json_value.should eq(JSON::Any.new(5.1234_f32.to_f64))
    Field::Double.new(6.1234).to_json_value.should eq(JSON::Any.new(6.1234))
    Field::Str.new("abc").to_json_value.should eq(JSON::Any.new("abc"))
    Field::Decimal.new(Parquet::Decimal.from_i32(4, 8, 2)).to_json_value.should eq(JSON::Any.new("0.04"))
    Field::Bytes.new(Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8])).to_json_value.should eq(JSON::Any.new("AQID"))
    Field::TimestampMillis.new(12345678).to_json_value.should eq(JSON::Any.new("1970-01-01 03:25:45.678 +00:00"))
    Field::TimeMillis.new(47445123).to_json_value.should eq(JSON::Any.new("13:10:45.123"))
    Field::TimeMicros.new(47445123456).to_json_value.should eq(JSON::Any.new("13:10:45.123456"))

    group = Field::Group.new(Row.new([f("X", Field::Int.new(1)), f("Y", Field::Double.new(2.2)), f("Z", Field::Str.new("abc"))]))
    group.to_json_value.should eq(JSON::Any.new({
      "X" => JSON::Any.new(1_i64), "Y" => JSON::Any.new(2.2), "Z" => JSON::Any.new("abc"),
    }))

    list = Field::ListInternal.new(R.make_list(fields(Field::Int.new(1), Field::Int.new(12), Field::Null.new)))
    list.to_json_value.should eq(JSON::Any.new([JSON::Any.new(1_i64), JSON::Any.new(12_i64), JSON::Any.new(nil)]))

    map = Field::MapInternal.new(R.make_map([
      entry(Field::Str.new("k1"), Field::Double.new(1.2)),
      entry(Field::Str.new("k2"), Field::Double.new(3.4)),
    ] of Tuple(Field, Field)))
    map.to_json_value.should eq(JSON::Any.new({"k1" => JSON::Any.new(1.2), "k2" => JSON::Any.new(3.4)}))
  end
end

describe Parquet::Record::List do
  # Upstream `test_list_primitive_accessors` / `_invalid_accessors`.
  it "accesses list primitives" do
    R.make_list(fields(Field::Byte.new(3_i8), Field::Byte.new(4_i8))).get_byte(1).should eq(4)
    R.make_list(fields(Field::Long.new(6_i64), Field::Long.new(7_i64))).get_long(1).should eq(7)
    R.make_list(fields(Field::Float16.new(F16_PI))).get_float16(0).should be_close(F16_PI, 1e-9)
    list = R.make_list(fields(Field::Float.new(8.1_f32), Field::Float.new(9.2_f32), Field::Float.new(10.3_f32)))
    list.get_float(2).should be_close(10.3_f32, 1e-6)
    expect_raises(Parquet::GeneralError) { R.make_list(fields(Field::Bool.new(false))).get_byte(0) }
    expect_raises(Parquet::GeneralError) { R.make_list(fields(Field::Str.new("abc"))).get_bytes(0) }
  end

  # Upstream `test_list_complex_accessors`.
  it "accesses list complex fields" do
    group = R.make_list(fields(Field::Group.new(Row.new([f("x", Field::Null.new), f("Y", Field::Int.new(2))]))))
    group.get_group(0).len.should eq(2)
    list = R.make_list(fields(Field::ListInternal.new(R.make_list(fields(Field::Int.new(2), Field::Null.new)))))
    list.get_list(0).len.should eq(2)
    expect_raises(Parquet::GeneralError, "Cannot access Group as Float") do
      R.make_list(fields(Field::Group.new(Row.new([f("x", Field::Null.new)])))).get_float(0)
    end
  end

  # Upstream `test_list_element_access`.
  it "exposes elements" do
    expected = fields(Field::Int.new(1), Field::Group.new(Row.new([f("x", Field::Null.new), f("Y", Field::Int.new(2))])))
    R.make_list(expected).elements.should eq(expected)
  end
end

describe Parquet::Record::Map do
  # Upstream `test_map_accessors`.
  it "accesses keys and values" do
    map = R.make_map([
      entry(Field::Int.new(1), Field::Str.new("a")),
      entry(Field::Int.new(2), Field::Str.new("b")),
      entry(Field::Int.new(3), Field::Str.new("c")),
      entry(Field::Int.new(4), Field::Str.new("d")),
      entry(Field::Int.new(5), Field::Str.new("e")),
    ] of Tuple(Field, Field))
    map.len.should eq(5)
    5.times do |i|
      map.get_keys.get_int(i).should eq(i + 1)
      map.get_values.get_string(i).should eq((i + 'a'.ord).chr.to_s)
    end
  end

  # Upstream `test_map_entry_access`.
  it "exposes entries" do
    expected = [entry(Field::Str.new("one"), Field::Int.new(1)), entry(Field::Str.new("two"), Field::Int.new(2))]
    R.make_map(expected).entries.should eq(expected)
  end
end

private def fields_pair_helper : Array(Tuple(String, Field))
  [
    f("a", Field::Null.new),
    f("b", Field::Bool.new(false)),
    f("c", Field::Byte.new(3_i8)),
    f("d", Field::Short.new(4_i16)),
    f("e", Field::Int.new(5)),
    f("f", Field::Long.new(6_i64)),
    f("g", Field::UByte.new(3_u8)),
    f("h", Field::UShort.new(4_u16)),
    f("i", Field::UInt.new(5_u32)),
    f("j", Field::ULong.new(6_u64)),
    f("k", Field::Float.new(7.1_f32)),
    f("l", Field::Double.new(8.1)),
    f("m", Field::Str.new("abc")),
    f("n", Field::Bytes.new(Parquet::ByteArray.new(Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8]))),
    f("o", Field::Decimal.new(Parquet::Decimal.from_i32(4, 7, 2))),
    f("p", Field::Float16.new(9.1015625)),
  ]
end
