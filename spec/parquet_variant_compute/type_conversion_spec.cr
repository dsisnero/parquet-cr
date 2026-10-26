require "../spec_helper"

# Ported from `parquet-variant-compute/src/type_conversion.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): casting a single `Variant`
# value to a strongly typed Arrow value.
#
# Upstream has no unit tests for this module (it is covered through
# `variant_get`/`shred_variant` integration tests), so the port characterizes
# each conversion family: safe/strict casting, the primitive/temporal
# conversions (including the precision-loss rejections), booleans and the
# decimal unscaled conversions.
private alias PVC = ParquetVariantCompute
private alias TC = ParquetVariantCompute::TypeConversion
private alias Variant = ParquetVariant::Variant

private def safe : Arrow::CastOptions
  Arrow::CastOptions.default
end

private def strict : Arrow::CastOptions
  Arrow::CastOptions.new(safe: false)
end

describe ParquetVariantCompute::TypeConversion do
  describe "variant_cast_with_options" do
    it "returns nil for a failed safe cast and for Variant::Null" do
      TC.cast_with_options(Variant.from("not a number"), safe) { |v| TC.int32(v) }.should be_nil
      TC.cast_with_options(Variant::Null.new, strict) { |v| TC.int32(v) }.should be_nil
      TC.cast_with_options(Variant.from(5_i32), strict) { |v| TC.int32(v) }.should eq 5
    end

    it "preserves false as a successful cast in safe and strict modes" do
      [safe, strict].each do |options|
        TC.cast_with_options(Variant.from(false), options) { |value| TC.variant_to_boolean(value) }.should be_false
      end
    end

    it "raises a CastError for a failed strict cast" do
      error = expect_raises(ParquetVariant::CastError) do
        TC.cast_with_options(Variant.from("not a number"), strict) { |v| TC.int32(v) }
      end
      error.message.not_nil!.should contain "Failed to cast variant value"
    end
  end

  describe "primitive conversions" do
    it "converts exact primitive variants" do
      TC.int8(Variant.from(5_i8)).should eq 5
      TC.int16(Variant.from(5_i16)).should eq 5
      TC.int32(Variant.from(5_i32)).should eq 5
      TC.int64(Variant.from(5_i64)).should eq 5
      TC.float32(Variant.from(1.5_f32)).should eq 1.5_f32
      TC.float64(Variant.from(2.5_f64)).should eq 2.5_f64
    end

    it "casts numbers and booleans between widths" do
      TC.int64(Variant.from(5_i8)).should eq 5
      TC.int32(Variant.from(true)).should eq 1
      TC.int32(Variant.from(false)).should eq 0
      TC.int8(Variant.from(5_i32)).should eq 5
      TC.int8(Variant.from(300_i32)).should be_nil
      TC.float32(Variant.from(5_i32)).should eq 5.0_f32
      TC.int32(Variant.from(2.9_f64)).should eq 2
    end

    it "casts decimals to numbers by truncation or lossy float" do
      decimal = ParquetVariant::VariantDecimal4.try_new(1234, 2)
      TC.int32(Variant.from(decimal)).should eq 12
      TC.float64(Variant.from(decimal)).should eq 12.34
    end

    it "restricts shredded values to numeric variants of the target type" do
      # Shredded ints accept any integer decimal variant (`Variant#as_int8`
      # narrows), but never booleans or floats.
      TC.int8(Variant.from(5_i16), shred: true).should eq 5
      TC.int8(Variant.from(true), shred: true).should be_nil
      TC.int8(Variant.from(1.5_f64), shred: true).should be_nil
      TC.float32(Variant.from(1.5_f64), shred: true).should be_nil
      TC.float32(Variant.from(1.5_f32), shred: true).should eq 1.5_f32
    end

    it "converts dates" do
      date = Variant.from_date(Time.utc(2022, 1, 1))
      TC.date32(date).should eq 18_993
      TC.date64(date).should eq 18_993_i64 * 86_400_000
      TC.date32(Variant.from(5_i32)).should be_nil
    end

    it "converts times of day and rejects dropped precision" do
      time = Variant.from_time(Time::Span.new(hours: 1, minutes: 2, seconds: 3))
      TC.time64_microsecond(time).should eq 3_723_000_000_i64
      TC.time32_second(time).should eq 3_723_i32
      TC.time32_millisecond(time).should eq 3_723_000_i32
      TC.time64_nanosecond(time).should eq 3_723_000_000_000_i64

      precise = Variant.from_time(Time::Span.new(nanoseconds: 1_500))
      TC.time32_second(precise).should be_nil
      TC.time32_millisecond(precise).should be_nil
      TC.time64_microsecond(precise).should eq 1_i64
    end

    it "converts timestamps with and without timezone" do
      utc = Variant.from_datetime_utc(Time.unix(1_600_000_000) + Time::Span.new(nanoseconds: 123_456_000))
      utc_ms = Variant.from_datetime_utc(Time.unix(1_600_000_000) + Time::Span.new(nanoseconds: 123_000_000))
      TC.timestamp_microsecond(utc, ntz: false).should eq 1_600_000_000_123_456_i64
      TC.timestamp_nanosecond(utc, ntz: false).should eq 1_600_000_000_123_456_000_i64
      # Milliseconds reject dropped sub-millisecond precision.
      TC.timestamp_millisecond(utc, ntz: false).should be_nil
      TC.timestamp_millisecond(utc_ms, ntz: false).should eq 1_600_000_000_123_i64
      # A UTC (non-ntz) variant is not a naive timestamp.
      TC.timestamp_microsecond(utc, ntz: true).should be_nil

      ntz = Variant.from_naive_datetime(Time.unix(1_600_000_000))
      TC.timestamp_microsecond(ntz, ntz: true).should eq 1_600_000_000_000_000_i64
      TC.timestamp_microsecond(ntz, ntz: false).should be_nil

      # Seconds and millis reject dropped precision.
      TC.timestamp_second(utc, ntz: false).should be_nil
      # Milliseconds reject dropped sub-millisecond precision.
      TC.timestamp_millisecond(utc, ntz: false).should be_nil
      TC.timestamp_millisecond(utc_ms, ntz: false).should eq 1_600_000_000_123_i64
      whole = Variant.from_datetime_utc(Time.unix(1_600_000_000))
      TC.timestamp_second(whole, ntz: false).should eq 1_600_000_000_i64
      # Shredded timestamps only accept the matching variant width.
      TC.timestamp_second(whole, ntz: false, shred: true).should be_nil
      TC.timestamp_microsecond(utc, ntz: false, shred: true).should eq 1_600_000_000_123_456_i64
    end

    it "keeps the Float16 target reachable" do
      TC.float16(Variant.from(1.5_f32)).should eq Arrow::Float16.new(0x3e00_u16)
      TC.float16(Variant.from(1.5_f32), shred: true).should be_nil
    end
  end

  describe "variant_to_boolean" do
    it "casts booleans, numbers and boolean strings" do
      TC.variant_to_boolean(Variant.from(true)).should be_true
      TC.variant_to_boolean(Variant.from(0_i32)).should be_false
      TC.variant_to_boolean(Variant.from(2.5_f64)).should be_true
      TC.variant_to_boolean(Variant.from("true")).should be_true
      TC.variant_to_boolean(Variant.from("FALSE")).should be_false
      TC.variant_to_boolean(Variant.from(1_i32), shred: true).should be_nil
      TC.variant_to_boolean(Variant.from(true), shred: true).should be_true
    end
  end

  describe "decimal unscaled conversions" do
    it "rescales integers, floats, strings and decimals to the target scale" do
      TC.unscaled_decimal32(Variant.from(12_i32), 9, 0).should eq 12
      TC.unscaled_decimal32(Variant.from(12_i32), 9, 2).should eq 1200
      TC.unscaled_decimal32(Variant.from(12_i64), 18, 2).should eq 1200_i64
      TC.unscaled_decimal128(Variant.from(12_i64), 38, 2).should eq 1200_i128

      TC.unscaled_decimal32(Variant.from(1.5_f64), 9, 2).should eq 150
      TC.unscaled_decimal32(Variant.from("12.34"), 9, 2).should eq 1234
      TC.unscaled_decimal32(Variant.from("not a number"), 9, 2).should be_nil
      TC.unscaled_decimal32(Variant.from("12.345"), 9, 2).should eq 1235

      decimal = Variant.from(ParquetVariant::VariantDecimal4.try_new(1234, 2))
      TC.unscaled_decimal32(decimal, 9, 2).should eq 1234
      TC.unscaled_decimal32(decimal, 9, 0).should eq 12
      TC.unscaled_decimal64(decimal, 18, 4).should eq 123_400_i64
      TC.unscaled_decimal128(decimal, 38, 2).should eq 1234_i128

      # Values that do not fit the target precision are rejected.
      TC.unscaled_decimal32(Variant.from(1_000_000_000_i32), 9, 0).should be_nil
      TC.unscaled_decimal32(Variant.from(5_i32), 9, 0).should eq 5
      TC.unscaled_decimal32(Variant.from(5_i32), 0, 0).should be_nil
    end

    it "only accepts exact rescalings when shredding" do
      exact = Variant.from(ParquetVariant::VariantDecimal4.try_new(150, 2))
      TC.shred_unscaled_decimal32(exact, 9, 1).should eq 15
      lossy = Variant.from(ParquetVariant::VariantDecimal4.try_new(155, 2))
      TC.shred_unscaled_decimal32(lossy, 9, 1).should be_nil

      TC.shred_unscaled_decimal32(Variant.from(12_i32), 9, 0).should eq 12
      TC.shred_unscaled_decimal32(Variant.from(12_i32), 9, 2).should eq 1200
      # Floats and strings are not valid shredded decimals.
      TC.shred_unscaled_decimal32(Variant.from(1.5_f64), 9, 2).should be_nil
      TC.shred_unscaled_decimal32(Variant.from("12.34"), 9, 2).should be_nil
      TC.shred_unscaled_decimal64(exact, 18, 1).should eq 15_i64
      TC.shred_unscaled_decimal128(exact, 38, 1).should eq 15_i128
    end
  end
end
