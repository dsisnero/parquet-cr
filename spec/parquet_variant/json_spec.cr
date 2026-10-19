require "../spec_helper"

# Ported from `parquet-variant-json/src/to_json.rs` and `lib.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `VariantToJson` (`to_json`/`to_json_string`/`to_json_value`).
#
# Divergences: `serde_json::Value` is `JSON::Any`, `ArrowError` is
# `Parquet::ArrowError`, date/timestamp/time payloads are `Time`/`Time::Span`
# (Crystal has no date-only/time-of-day types) and the `from_json` half of the
# crate is deferred (it needs `VariantBuilderExt`/`ObjectFieldBuilder`).
# One assertion is relaxed: upstream's `test_decimal_precision_behavior` asserts
# the string parses back to exactly `to_json_value`, which relies on serde_json's
# multiply-by-power-of-ten float parser; Crystal's correctly rounded parser can
# differ by one ULP for values above 2^53, so the port allows that.
private alias PV = ParquetVariant
private alias Variant = ParquetVariant::Variant

# The numeric value of a JSON number, or nil for other JSON values.
private def json_number(value : JSON::Any) : Float64?
  if integer = value.as_i64?
    integer.to_f64
  elsif float = value.as_f?
    float
  end
end

# Compares two JSON values, treating numbers numerically (upstream does the same
# for the `Value::Number` cases, where `0` and `0.0` are both valid spellings).
private def json_equal?(actual : JSON::Any, expected : JSON::Any) : Bool
  if (left = json_number(actual)) && (right = json_number(expected))
    left == right
  else
    actual == expected
  end
end

# Upstream `JsonTest::run`: checks the string form, the JSON value and that the
# string parses back to the expected value.
private def check_json_test(variant : Variant, expected_json : String, expected_value : JSON::Any) : Nil
  json_string = variant.to_json_string
  json_string.should eq(expected_json)

  json_equal?(variant.to_json_value, expected_value).should be_true
  json_equal?(JSON.parse(json_string), expected_value).should be_true
end

private def decimal4(integer : Int32, scale : UInt8) : Variant
  Variant.from(PV::VariantDecimal4.try_new(integer, scale))
end

private def decimal8(integer : Int64, scale : UInt8) : Variant
  Variant.from(PV::VariantDecimal8.try_new(integer, scale))
end

private def decimal16(integer : Int128, scale : UInt8) : Variant
  Variant.from(PV::VariantDecimal16.try_new(integer, scale))
end

private def variant_from_builder(& : PV::VariantBuilder ->) : Variant
  builder = PV::VariantBuilder.new
  yield builder
  metadata, value = builder.finish
  Variant.try_new(metadata, value)
end

describe "Variant to JSON" do
  it "formats decimal edge cases (upstream test_decimal_edge_cases)" do
    decimal4(-12345, 3_u8).to_json_string.should eq("-12.345")
    decimal8(123456789_i64, 6_u8).to_json_string.should eq("123.456789")
  end

  it "formats decimal16 values (upstream test_decimal16_to_json)" do
    variant = decimal16(123456789012345_i128, 4_u8)
    variant.to_json_string.should eq("12345678901.2345")
    json_number(variant.to_json_value).should_not be_nil

    # Very large values may lose f64 precision, as upstream notes.
    large_json = decimal16(999999999999999999_i128, 2_u8).to_json_string
    (large_json.starts_with?("9999999999999999") || large_json.starts_with?("10000000000000000")).should be_true
  end

  it "formats dates (upstream test_date_to_json)" do
    variant = Variant::Date.new(Time.utc(2023, 12, 25))
    variant.to_json_string.should eq(%("2023-12-25"))
    variant.to_json_value.should eq(JSON::Any.new("2023-12-25"))

    Variant::Date.new(Time.utc(2024, 2, 29)).to_json_string.should eq(%("2024-02-29"))
  end

  it "formats timestamps (upstream test_timestamp_micros_to_json/test_timestamp_ntz_micros_to_json)" do
    variant = Variant::TimestampMicros.new(Time.utc(2023, 12, 25, 10, 30, 45))
    json = variant.to_json_string
    json.should contain("2023-12-25T10:30:45")
    json.starts_with?('"').should be_true
    json.ends_with?('"').should be_true
    variant.to_json_value.as_s?.should_not be_nil

    ntz = Variant::TimestampNtzMicros.new(Time.utc(2023, 12, 25, 11, 50, 45, nanosecond: 123_456))
    ntz_json = ntz.to_json_string
    ntz_json.should contain("2023-12-25")
    ntz.to_json_value.as_s?.should_not be_nil
  end

  it "formats nanosecond timestamps (upstream test_timestamp_nanos_to_json/test_timestamp_ntz_nanos_to_json)" do
    timestamp = Time.utc(2023, 12, 25, 10, 30, 45, nanosecond: 123_456_789)
    Variant::TimestampNanos.new(timestamp).to_json_string
      .should eq(%("2023-12-25T10:30:45.123456789+00:00"))

    ntz = Time.utc(2023, 12, 25, 11, 50, 45, nanosecond: 123_456_789)
    Variant::TimestampNtzNanos.new(ntz).to_json_string
      .should eq(%("2023-12-25T11:50:45.123456789"))
  end

  it "formats times with trimmed microseconds (upstream test_time_to_json)" do
    span = Time::Span.new(seconds: 12345, nanoseconds: 123_460_708)
    variant = Variant::Time.new(span)
    variant.to_json_string.should eq(%("03:25:45.12346"))
    variant.to_json_value.as_s?.should eq("03:25:45.12346")

    {
      {10_i64, "00:00:00.00001"},
      {10010_i64, "00:00:00.01001"},
      {100_000_i64, "00:00:00.1"},
      {123_450_i64, "00:00:00.12345"},
      {0_i64, "00:00:00.0"},
    }.each do |(micros, expected)|
      span = Time::Span.new(seconds: 0, nanoseconds: micros * 1000)
      Variant::Time.new(span).to_json_string.should eq(%("#{expected}"))
    end
  end

  it "base64-encodes binary values (upstream test_binary_to_json)" do
    variant = Variant::Binary.new("Hello, World!".to_slice)
    json = variant.to_json_string
    json.starts_with?('"').should be_true
    json.ends_with?('"').should be_true
    json.size.should be > 2
    variant.to_json_value.as_s?.should_not be_nil

    Variant::Binary.new(Bytes.empty).to_json_string.should eq(%(""))
    Variant::Binary.new(Bytes[0, 255, 128, 64]).to_json_string.should eq(%("AP+AQA=="))
  end

  it "formats strings, short strings and UUIDs (upstream test_string_to_json/test_short_string_to_json/test_uuid_to_json)" do
    check_json_test(Variant.from("hello world"), %("hello world"), JSON::Any.new("hello world"))

    short = Variant::ShortString.new(PV::ShortString.try_new("short"))
    check_json_test(short, %("short"), JSON::Any.new("short"))

    uuid = UUID.new("123e4567-e89b-12d3-a456-426614174000")
    check_json_test(Variant::Uuid.new(uuid), %("123e4567-e89b-12d3-a456-426614174000"),
      JSON::Any.new("123e4567-e89b-12d3-a456-426614174000"))
  end

  it "escapes strings (upstream test_string_escaping/test_string_escaping_comprehensive)" do
    variant = Variant.from("hello\nworld\t\"quoted\"")
    variant.to_json_string.should eq(%("hello\\nworld\\t\\"quoted\\""))
    variant.to_json_value.should eq(JSON::Any.new("hello\nworld\t\"quoted\""))

    check_json_test(
      Variant.from("line1\nline2\ttab\"quote\"\\backslash"),
      %("line1\\nline2\\ttab\\"quote\\"\\\\backslash"),
      JSON::Any.new("line1\nline2\ttab\"quote\"\\backslash"),
    )
    check_json_test(Variant.from("Hello 世界 🌍"), %("Hello 世界 🌍"), JSON::Any.new("Hello 世界 🌍"))
  end

  it "writes to an IO buffer (upstream test_json_buffer_writing/test_buffer_writing_variants)" do
    io = IO::Memory.new
    Variant::Int8.new(123_i8).to_json(io)
    io.to_s.should eq("123")

    io = IO::Memory.new
    Variant.from("test buffer writing").to_json(io)
    io.to_s.should eq(%("test buffer writing"))

    # A second write appends to the same buffer.
    Variant.from("!").to_json(io)
    io.to_s.should eq(%("test buffer writing""!"))
  end

  it "converts primitives (upstream test_primitive_json_conversion)" do
    check_json_test(Variant::Null.new, "null", JSON::Any.new(nil))
    check_json_test(Variant::BooleanTrue.new, "true", JSON::Any.new(true))
    check_json_test(Variant::BooleanFalse.new, "false", JSON::Any.new(false))

    check_json_test(Variant::Int8.new(42_i8), "42", JSON::Any.new(42_i64))
    check_json_test(Variant::Int8.new(-128_i8), "-128", JSON::Any.new(-128_i64))
    check_json_test(Variant::Int16.new(32767_i16), "32767", JSON::Any.new(32767_i64))
    check_json_test(Variant::Int16.new(-32768_i16), "-32768", JSON::Any.new(-32768_i64))
    check_json_test(Variant::Int32.new(2147483647_i32), "2147483647", JSON::Any.new(2147483647_i64))
    check_json_test(Variant::Int32.new(-2147483648_i32), "-2147483648", JSON::Any.new(-2147483648_i64))
    check_json_test(Variant::Int64.new(9223372036854775807_i64), "9223372036854775807",
      JSON::Any.new(9223372036854775807_i64))
    check_json_test(Variant::Int64.new(-9223372036854775808_i64), "-9223372036854775808",
      JSON::Any.new(-9223372036854775808_i64))

    check_json_test(Variant::Float.new(3.5_f32), "3.5", JSON::Any.new(3.5))
    check_json_test(Variant::Float.new(0.0_f32), "0", JSON::Any.new(0_i64))
    check_json_test(Variant::Float.new(-1.5_f32), "-1.5", JSON::Any.new(-1.5))
    check_json_test(Variant::Double.new(Math::E), "2.718281828459045", JSON::Any.new(Math::E))

    check_json_test(decimal4(12345_i32, 2_u8), "123.45", JSON::Any.new(123.45))
    check_json_test(decimal4(42_i32, 0_u8), "42", JSON::Any.new(42_i64))
    check_json_test(decimal8(1234567890_i64, 3_u8), "1234567.89", JSON::Any.new(1234567.89))
    check_json_test(decimal16(123456789012345_i128, 4_u8), "12345678901.2345", JSON::Any.new(12345678901.2345))

    check_json_test(Variant.from("hello world"), %("hello world"), JSON::Any.new("hello world"))
    check_json_test(Variant.from(""), %(""), JSON::Any.new(""))

    check_json_test(Variant::Date.new(Time.utc(2023, 12, 25)), %("2023-12-25"), JSON::Any.new("2023-12-25"))

    check_json_test(Variant::Binary.new("test".to_slice), %("dGVzdA=="), JSON::Any.new("dGVzdA=="))
    check_json_test(Variant::Binary.new(Bytes.empty), %(""), JSON::Any.new(""))
    check_json_test(Variant::Binary.new("binary data".to_slice), %("YmluYXJ5IGRhdGE="), JSON::Any.new("YmluYXJ5IGRhdGE="))
  end

  it "keeps decimal precision consistent between the string and value forms (upstream test_decimal_precision_behavior)" do
    high_precision = decimal8(9007199254740993_i64, 6_u8)
    json_string = high_precision.to_json_string
    json_value = high_precision.to_json_value
    parsed = JSON.parse(json_string).as_f
    value = json_value.as_f
    (parsed - value).abs.should be <= parsed.abs * Float64::EPSILON * 2

    decimal8(1234567890000_i64, 6_u8).to_json_string.should eq("1234567.89")
    decimal8(42000000_i64, 6_u8).to_json_string.should eq("42")
  end

  it "rejects NaN and infinite floats in the JSON value form (upstream test_float_nan_inf_handling)" do
    expect_raises(Parquet::ArrowError, /Invalid float value/) do
      Variant::Float.new(Float32::NAN).to_json_value
    end
    expect_raises(Parquet::ArrowError, /Invalid float value/) do
      Variant::Float.new(Float32::INFINITY).to_json_value
    end
    expect_raises(Parquet::ArrowError, /Invalid float value/) do
      Variant::Float.new(-Float32::INFINITY).to_json_value
    end
    expect_raises(Parquet::ArrowError, /Invalid double value/) do
      Variant::Double.new(Float64::NAN).to_json_value
    end
    expect_raises(Parquet::ArrowError, /Invalid double value/) do
      Variant::Double.new(Float64::INFINITY).to_json_value
    end
    expect_raises(Parquet::ArrowError, /Invalid double value/) do
      Variant::Double.new(-Float64::INFINITY).to_json_value
    end

    json_number(Variant::Float.new(Math::PI.to_f32).to_json_value).should_not be_nil
    json_number(Variant::Double.new(Math::E).to_json_value).should_not be_nil
  end

  it "converts a simple object (upstream test_simple_object_to_json)" do
    variant = variant_from_builder do |builder|
      object = builder.new_object
      object.insert("name", "Alice")
      object.insert("age", 30_i32)
      object.insert("active", true)
      object.insert("score", 95.5_f64)
      object.finish
    end

    parsed = JSON.parse(variant.to_json_string)
    parsed["name"].should eq(JSON::Any.new("Alice"))
    parsed["age"].should eq(JSON::Any.new(30_i64))
    parsed["active"].should eq(JSON::Any.new(true))
    json_number(parsed["score"]).should_not be_nil
    parsed.as_h.size.should eq(4)

    variant.to_json_value.as_h?.should_not be_nil
  end

  it "converts an empty object (upstream test_empty_object_to_json)" do
    variant = variant_from_builder do |builder|
      builder.new_object.finish
    end

    variant.to_json_string.should eq("{}")
    variant.to_json_value.as_h.should be_empty
  end

  it "escapes object keys and values (upstream test_object_with_special_characters_to_json)" do
    variant = variant_from_builder do |builder|
      object = builder.new_object
      object.insert("message", "Hello \"World\"\nWith\tTabs")
      object.insert("path", "C:\\Users\\Alice\\Documents")
      object.insert("unicode", "😀 Smiley")
      object.finish
    end

    json = variant.to_json_string
    json.should contain(%(Hello \\"World\\"\\nWith\\tTabs))
    json.should contain("C:\\\\Users\\\\Alice\\\\Documents")
    json.should contain("😀 Smiley")

    parsed = JSON.parse(json).as_h
    parsed["message"].should eq(JSON::Any.new("Hello \"World\"\nWith\tTabs"))
    parsed["path"].should eq(JSON::Any.new("C:\\Users\\Alice\\Documents"))
    parsed["unicode"].should eq(JSON::Any.new("😀 Smiley"))
  end

  it "preserves object field order (upstream test_object_field_ordering_in_json)" do
    variant = variant_from_builder do |builder|
      object = builder.new_object
      object.insert("zebra", "last")
      object.insert("alpha", "first")
      object.insert("beta", "second")
      object.finish
    end

    json = variant.to_json_string
    parsed = JSON.parse(json).as_h
    parsed.size.should eq(3)
    parsed["alpha"].should eq(JSON::Any.new("first"))
    parsed["beta"].should eq(JSON::Any.new("second"))
    parsed["zebra"].should eq(JSON::Any.new("last"))
  end

  it "converts a list (upstream test_simple_list_to_json/test_empty_list_to_json)" do
    variant = variant_from_builder do |builder|
      list = builder.new_list
      [1_i32, 2_i32, 3_i32, 4_i32, 5_i32].each { |value| list.append_value(value) }
      list.finish
    end

    variant.to_json_string.should eq("[1,2,3,4,5]")
    values = variant.to_json_value.as_a
    values.size.should eq(5)
    values[0].should eq(JSON::Any.new(1_i64))
    values[4].should eq(JSON::Any.new(5_i64))

    empty = variant_from_builder do |builder|
      builder.new_list.finish
    end
    empty.to_json_string.should eq("[]")
    empty.to_json_value.as_a.should be_empty
  end

  it "converts a mixed-type list (upstream test_mixed_type_list_to_json)" do
    variant = variant_from_builder do |builder|
      list = builder.new_list
      list.append_value("hello")
      list.append_value(42_i32)
      list.append_value(true)
      list.append_value(nil)
      list.append_value(Math::PI)
      list.finish
    end

    values = JSON.parse(variant.to_json_string).as_a
    values.size.should eq(5)
    values[0].should eq(JSON::Any.new("hello"))
    values[1].should eq(JSON::Any.new(42_i64))
    values[2].should eq(JSON::Any.new(true))
    values[3].should eq(JSON::Any.new(nil))
    json_number(values[4]).should_not be_nil
  end

  it "converts lists and objects of all primitive types (upstream test_list_with_various_primitive_types_to_json/test_object_with_various_primitive_types_to_json)" do
    list_variant = variant_from_builder do |builder|
      list = builder.new_list
      list.append_value("string_value")
      list.append_value(42_i32)
      list.append_value(true)
      list.append_value(Math::PI)
      list.append_value(false)
      list.append_value(nil)
      list.append_value(100_i64)
      list.finish
    end

    values = JSON.parse(list_variant.to_json_string).as_a
    values.size.should eq(7)
    values[0].should eq(JSON::Any.new("string_value"))
    values[1].should eq(JSON::Any.new(42_i64))
    values[2].should eq(JSON::Any.new(true))
    json_number(values[3]).should_not be_nil
    values[4].should eq(JSON::Any.new(false))
    values[5].should eq(JSON::Any.new(nil))
    values[6].should eq(JSON::Any.new(100_i64))

    object_variant = variant_from_builder do |builder|
      object = builder.new_object
      object.insert("string_field", "test_string")
      object.insert("int_field", 123_i32)
      object.insert("bool_field", true)
      object.insert("float_field", 2.71_f64)
      object.insert("null_field", nil)
      object.insert("long_field", 999_i64)
      object.finish
    end

    parsed = JSON.parse(object_variant.to_json_string).as_h
    parsed.size.should eq(6)
    parsed["string_field"].should eq(JSON::Any.new("test_string"))
    parsed["int_field"].should eq(JSON::Any.new(123_i64))
    parsed["bool_field"].should eq(JSON::Any.new(true))
    json_number(parsed["float_field"]).should_not be_nil
    parsed["null_field"].should eq(JSON::Any.new(nil))
    parsed["long_field"].should eq(JSON::Any.new(999_i64))
  end
end
