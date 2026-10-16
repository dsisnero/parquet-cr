require "../spec_helper"

# Ported from `parquet-variant/src/variant.rs` test module and the `Variant`
# accessor/factory docs (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: `Variant::Builder`-dependent tests (`test_variant_all_subtypes_debug`)
# are deferred until the value builder is ported (Phase 3.1b). Rust `From`
# impls are exercised through the `Variant.from_*` factories; date/timestamp
# values use `Time` and time-of-day uses `Time::Span`.

private alias V = ParquetVariant::Variant
private alias MD = ParquetVariant::VariantMetadata
private alias D4 = ParquetVariant::VariantDecimal4
private alias D8 = ParquetVariant::VariantDecimal8
private alias D16 = ParquetVariant::VariantDecimal16
private alias Err = ParquetVariant::InvalidArgumentError

describe ParquetVariant::Variant do
  # Upstream `test_empty_variant_will_fail`.
  it "rejects an empty value buffer" do
    metadata = MD.try_new(Bytes[1, 0, 0])
    expect_raises(Err, /Received empty bytes/) do
      V.try_new_with_metadata(metadata, Bytes.empty)
    end
  end

  # Upstream `test_construct_short_string`.
  it "constructs short strings" do
    short_string = ParquetVariant::ShortString.try_new("norm")
    short_string.as_str.should eq("norm")

    long_string = "a" * (ParquetVariant::MAX_SHORT_STRING_BYTES + 1)
    expect_raises(Err) { ParquetVariant::ShortString.try_new(long_string) }
  end

  it "constructs primitive variants from native values" do
    V.from_nil.should be_a(V::Null)
    V.from(true).should be_a(V::BooleanTrue)
    V.from(false).should be_a(V::BooleanFalse)
    V.from(42i8).should be_a(V::Int8)
    V.from(1234i16).should be_a(V::Int16)
    V.from(123456i32).should be_a(V::Int32)
    V.from(123456789i64).should be_a(V::Int64)
    V.from(1.5f32).should be_a(V::Float)
    V.from(2.5f64).should be_a(V::Double)
    V.from("hello").should be_a(V::ShortString)
    V.from("This is a very long string that exceeds the short string limit of 63 bytes")
      .should be_a(V::String)
  end

  it "widens unsigned values like the upstream From impls" do
    V.from(100_u8).should be_a(V::Int8)
    V.from(200_u8).should be_a(V::Int16)
    V.from(40_000_u16).should be_a(V::Int32)
    V.from(3_000_000_000_u32).should be_a(V::Int64)
    V.from(18_446_744_073_709_551_615_u64).should be_a(V::Decimal16)
  end

  it "converts integers" do
    V.from(123i64).as_int8.should eq(123)
    V.from(123i64).as_int16.should eq(123)
    V.from(123i64).as_int32.should eq(123)
    V.from(123i64).as_int64.should eq(123)
    V.from(123i64).as_u8.should eq(123)
    V.from(123i64).as_u16.should eq(123)
    V.from(123i64).as_u32.should eq(123)
    V.from(123i64).as_u64.should eq(123)

    V.from(150i16).as_int8.should be_nil
    V.from(-1i16).as_u8.should be_nil
    V.from("hello").as_int8.should be_nil
  end

  it "converts decimals" do
    V.from(D4.try_new(123, 0_u8)).as_int8.should eq(123)
    V.from(D4.try_new(100, 2_u8)).as_int8.should eq(1)
    V.from(D4.try_new(1234, 0_u8)).as_int8.should be_nil
    V.from("hello").as_decimal4.should be_nil

    V.from(D8.try_new(1234_i64, 2_u8)).as_decimal4.should eq(D4.try_new(1234, 2_u8))
    V.from(D8.try_new(12_345_678_901_i64, 2_u8)).as_decimal4.should be_nil
    V.from(D16.try_new(1234_i128, 2_u8)).as_decimal8.should eq(D8.try_new(1234_i64, 2_u8))

    V.from(123).as_decimal16.should eq(D16.try_new(123_i128, 0_u8))
  end

  it "converts booleans, strings, binary, and uuids" do
    V.from(true).as_boolean.should eq(true)
    V.from(false).as_boolean.should eq(false)
    V.from("hi").as_boolean.should be_nil

    V.from_nil.as_null.should be_true
    V.from(1).as_null.should be_false

    long = "a" * 64
    V.from(long).as_string.should eq(long)
    V.from("hello".to_slice).as_u8_slice.should eq("hello".to_slice)

    uuid = UUID.new("67e55044-10b1-426f-9247-bb680e5fe0c8")
    V.from(uuid).as_uuid.should eq(uuid)
    V.from(1234).as_uuid.should be_nil
  end

  it "converts dates and timestamps" do
    date = Time.utc(2025, 4, 12)
    V.from_date(date).as_naive_date.should eq(date)
    V.from("hello").as_naive_date.should be_nil

    datetime = Time.utc(2025, 4, 16, 12, 34, 56, nanosecond: 780_000_000)
    micros = V.from_datetime_utc(datetime)
    micros.should be_a(V::TimestampMicros)
    micros.as_timestamp_micros.should eq(datetime)
    micros.as_timestamp_nanos.should eq(datetime)

    datetime_nanos = Time.utc(2025, 8, 14, 12, 33, 54, nanosecond: 123_456_789)
    nanos = V.from_datetime_utc(datetime_nanos)
    nanos.should be_a(V::TimestampNanos)
    nanos.as_timestamp_micros.should be_nil
    nanos.as_timestamp_nanos.should eq(datetime_nanos)

    ntz = Time.utc(2025, 4, 16, 12, 34, 56, nanosecond: 780_000_000)
    V.from_naive_datetime(ntz).as_timestamp_ntz_micros.should eq(ntz)
    V.from("hello").as_timestamp_ntz_micros.should be_nil

    ntz_nanos = Time.utc(2025, 8, 14, 12, 33, 54, nanosecond: 123_456_789)
    V.from_naive_datetime(ntz_nanos).as_timestamp_ntz_nanos.should eq(ntz_nanos)

    span = Time::Span.new(hours: 1, minutes: 2, seconds: 3, nanoseconds: 4_000)
    V.from_time(span).as_time_utc.should eq(span)
    V.from("Hello").as_time_utc.should be_nil
  end

  it "converts floats" do
    V.from(1.5f32).as_f32.should eq(1.5f32)
    V.from(1.5f32).as_f64.should be_nil
    V.from(2.5f64).as_f64.should eq(2.5f64)
    V.from(2.5f64).as_f32.should be_nil
  end

  it "follows object and list paths" do
    # metadata dictionary: ["foo"]
    metadata = Bytes[0x11, 1, 0, 3] + "foo".to_slice
    # {"foo": ["bar", "baz"]}
    list_value = Bytes[0x03, 2, 0, 4, 8, 0x0D] + "bar".to_slice + Bytes[0x0D] + "baz".to_slice
    object_value = Bytes[0x02, 1, 0, 0, list_value.size.to_u8] + list_value

    variant = V.try_new(metadata, object_value)

    variant.get_path(ParquetVariant::VariantPath.parse("non_existent")).should be_nil

    path = ParquetVariant::VariantPath.parse("foo")
    foo = variant.get_path(path)
    foo.should_not be_nil
    foo.not_nil!.as_list.should_not be_nil

    bar = foo.not_nil!.get_path(ParquetVariant::VariantPath.from(0))
    bar.not_nil!.as_string.should eq("bar")

    nested = ParquetVariant::VariantPath.parse("foo[0]")
    variant.get_path(nested).not_nil!.should eq(bar.not_nil!)
  end
end

private alias VBD = ParquetVariant::VariantBuilder

# Upstream `test_variant_all_subtypes_debug`.
describe "ParquetVariant::Variant debug formatting" do
  it "formats every variant subtype like the upstream Debug impl" do
    builder = VBD.new
    root_obj = builder.new_object

    root_obj.insert("null", nil)
    root_obj.insert("boolean_true", true)
    root_obj.insert("boolean_false", false)
    root_obj.insert("int8", 42i8)
    root_obj.insert("int16", 1234i16)
    root_obj.insert("int32", 123456i32)
    root_obj.insert("int64", 1234567890123456789i64)
    root_obj.insert("float", 1.234f32)
    root_obj.insert("double", 1.23456789f64)

    root_obj.insert("date", V.from_date(Time.utc(2024, 12, 25)))
    root_obj.insert(
      "timestamp_micros",
      V::TimestampMicros.new(Time.utc(2024, 12, 25, 15, 30, 45, nanosecond: 123_000_000)),
    )
    root_obj.insert(
      "timestamp_ntz_micros",
      V::TimestampNtzMicros.new(Time.utc(2024, 12, 25, 15, 30, 45, nanosecond: 123_000_000)),
    )
    root_obj.insert(
      "timestamp_nanos",
      V::TimestampNanos.new(Time.utc(2025, 8, 15, 12, 3, 4, nanosecond: 123_456_789)),
    )
    root_obj.insert(
      "timestamp_ntz_nanos",
      V::TimestampNtzNanos.new(Time.utc(2025, 8, 15, 12, 3, 4, nanosecond: 123_456_789)),
    )

    root_obj.insert("decimal4", D4.try_new(1234, 2_u8))
    root_obj.insert("decimal8", D8.try_new(123456789_i64, 3_u8))
    root_obj.insert("decimal16", D16.try_new(123456789012345678901234567890_i128, 4_u8))

    root_obj.insert("binary", Bytes[0x01, 0x02, 0x03, 0x04, 0xde, 0xad, 0xbe, 0xef])
    root_obj.insert(
      "string",
      "This is a long string that exceeds the short string limit and contains emoji 🦀",
    )
    root_obj.insert("short_string", "Short string with emoji 🎉")
    root_obj.insert("time", V.from_time(Time::Span.new(hours: 1, minutes: 2, seconds: 3, nanoseconds: 4_000)))
    root_obj.insert("uuid", V.from(UUID.new("67e55044-10b1-426f-9247-bb680e5fe0c8")))

    nested_obj = root_obj.new_object("nested_object")
    nested_obj.insert("inner_key1", "inner_value1")
    nested_obj.insert("inner_key2", 999i32)
    nested_obj.finish

    mixed_list = root_obj.new_list("mixed_list")
    mixed_list.append_value(1i32)
    mixed_list.append_value("two")
    mixed_list.append_value(true)
    mixed_list.append_value(4.0f32)
    mixed_list.append_value(nil)
    nested_list = mixed_list.new_list
    nested_list.append_value("nested")
    nested_list.append_value(10i8)
    nested_list.finish
    mixed_list.finish

    root_obj.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)

    debug_output = variant.debug_string(false)
    debug_output.should contain("\"null\": Null")
    debug_output.should contain("\"boolean_true\": BooleanTrue")
    debug_output.should contain("\"int8\": Int8(42)")
    debug_output.should contain("\"binary\": Binary(01 02 03 04 de ad be ef)")
    debug_output.should contain("\"uuid\": Uuid(67e55044-10b1-426f-9247-bb680e5fe0c8)")
    debug_output.should contain("\"time\": Time(01:02:03.000004)")

    expected_compact = %({"binary": Binary(01 02 03 04 de ad be ef), "boolean_false": BooleanFalse, "boolean_true": BooleanTrue, "date": Date(2024-12-25), "decimal16": Decimal16(VariantDecimal16 { integer: 123456789012345678901234567890, scale: 4 }), "decimal4": Decimal4(VariantDecimal4 { integer: 1234, scale: 2 }), "decimal8": Decimal8(VariantDecimal8 { integer: 123456789, scale: 3 }), "double": Double(1.23456789), "float": Float(1.234), "int16": Int16(1234), "int32": Int32(123456), "int64": Int64(1234567890123456789), "int8": Int8(42), "mixed_list": [Int32(1), ShortString(ShortString("two")), BooleanTrue, Float(4.0), Null, [ShortString(ShortString("nested")), Int8(10)]], "nested_object": {"inner_key1": ShortString(ShortString("inner_value1")), "inner_key2": Int32(999)}, "null": Null, "short_string": ShortString(ShortString("Short string with emoji 🎉")), "string": String("This is a long string that exceeds the short string limit and contains emoji 🦀"), "time": Time(01:02:03.000004), "timestamp_micros": TimestampMicros(2024-12-25T15:30:45.123Z), "timestamp_nanos": TimestampNanos(2025-08-15T12:03:04.123456789Z), "timestamp_ntz_micros": TimestampNtzMicros(2024-12-25T15:30:45.123), "timestamp_ntz_nanos": TimestampNtzNanos(2025-08-15T12:03:04.123456789), "uuid": Uuid(67e55044-10b1-426f-9247-bb680e5fe0c8)})
    debug_output.should eq(expected_compact)

    expected_pretty = <<-PRETTY
{
    "binary": Binary(01 02 03 04 de ad be ef),
    "boolean_false": BooleanFalse,
    "boolean_true": BooleanTrue,
    "date": Date(
        2024-12-25,
    ),
    "decimal16": Decimal16(
        VariantDecimal16 {
            integer: 123456789012345678901234567890,
            scale: 4,
        },
    ),
    "decimal4": Decimal4(
        VariantDecimal4 {
            integer: 1234,
            scale: 2,
        },
    ),
    "decimal8": Decimal8(
        VariantDecimal8 {
            integer: 123456789,
            scale: 3,
        },
    ),
    "double": Double(
        1.23456789,
    ),
    "float": Float(
        1.234,
    ),
    "int16": Int16(
        1234,
    ),
    "int32": Int32(
        123456,
    ),
    "int64": Int64(
        1234567890123456789,
    ),
    "int8": Int8(
        42,
    ),
    "mixed_list": [
        Int32(
            1,
        ),
        ShortString(
            ShortString(
                "two",
            ),
        ),
        BooleanTrue,
        Float(
            4.0,
        ),
        Null,
        [
            ShortString(
                ShortString(
                    "nested",
                ),
            ),
            Int8(
                10,
            ),
        ],
    ],
    "nested_object": {
        "inner_key1": ShortString(
            ShortString(
                "inner_value1",
            ),
        ),
        "inner_key2": Int32(
            999,
        ),
    },
    "null": Null,
    "short_string": ShortString(
        ShortString(
            "Short string with emoji 🎉",
        ),
    ),
    "string": String(
        "This is a long string that exceeds the short string limit and contains emoji 🦀",
    ),
    "time": Time(
        01:02:03.000004,
    ),
    "timestamp_micros": TimestampMicros(
        2024-12-25T15:30:45.123Z,
    ),
    "timestamp_nanos": TimestampNanos(
        2025-08-15T12:03:04.123456789Z,
    ),
    "timestamp_ntz_micros": TimestampNtzMicros(
        2024-12-25T15:30:45.123,
    ),
    "timestamp_ntz_nanos": TimestampNtzNanos(
        2025-08-15T12:03:04.123456789,
    ),
    "uuid": Uuid(
        67e55044-10b1-426f-9247-bb680e5fe0c8,
    ),
}
PRETTY
    variant.debug_string(true).should eq(expected_pretty.chomp)
  end
end
