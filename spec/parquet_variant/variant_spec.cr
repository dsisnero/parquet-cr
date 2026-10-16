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
