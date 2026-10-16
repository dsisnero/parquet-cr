require "../spec_helper"

# Ported from `parquet-variant/src/variant/decimal.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: `Result` errors are raised `InvalidArgumentError`s;
# `is_ok`/`is_err` map to `expect_raises`. The `Into<Variant>` conversions are
# deferred until `Variant` is ported, so `VariantDecimalType` is a marker module.

private alias D4 = ParquetVariant::VariantDecimal4
private alias D8 = ParquetVariant::VariantDecimal8
private alias D16 = ParquetVariant::VariantDecimal16
private alias Err = ParquetVariant::InvalidArgumentError

describe ParquetVariant::VariantDecimal4 do
  # Upstream `test_variant_decimal_invalid_precision` (Decimal4 part).
  it "validates precision" do
    expect_raises(Err, /wider than max precision/) { D4.try_new(1_000_000_000_i32, 2_u8) }
    expect_raises(Err, /wider than max precision/) { D4.try_new(-1_000_000_000_i32, 2_u8) }
    D4.try_new(999_999_999_i32, 2_u8).integer.should eq(999_999_999)
    D4.try_new(-999_999_999_i32, 2_u8).integer.should eq(-999_999_999)
  end

  # Upstream `test_variant_decimal_invalid_scale` (Decimal4 part).
  it "validates scale" do
    expect_raises(Err, /larger than max precision/) { D4.try_new(123_i32, 10_u8) }
    expect_raises(Err) { D4.try_new(123_i32, 20_u8) }
    D4.try_new(123_i32, 9_u8).scale.should eq(9)
  end

  # Upstream `test_variant_decimal4_display`.
  it "displays" do
    D4.try_new(42_i32, 0_u8).to_s.should eq("42")
    D4.try_new(-42_i32, 0_u8).to_s.should eq("-42")
    D4.try_new(12345_i32, 2_u8).to_s.should eq("123.45")
    D4.try_new(-12345_i32, 2_u8).to_s.should eq("-123.45")
    D4.try_new(12300_i32, 2_u8).to_s.should eq("123")
    D4.try_new(-12300_i32, 2_u8).to_s.should eq("-123")
    D4.try_new(1005_i32, 3_u8).to_s.should eq("1.005")
    D4.try_new(-1005_i32, 3_u8).to_s.should eq("-1.005")
    D4.try_new(123_i32, 4_u8).to_s.should eq("0.0123")
    D4.try_new(-123_i32, 4_u8).to_s.should eq("-0.0123")
    D4.try_new(0_i32, 0_u8).to_s.should eq("0")
    D4.try_new(0_i32, 3_u8).to_s.should eq("0")
    D4.try_new(123456789_i32, 9_u8).to_s.should eq("0.123456789")
    D4.try_new(-123456789_i32, 9_u8).to_s.should eq("-0.123456789")
    D4.try_new(999999999_i32, 0_u8).to_s.should eq("999999999")
    D4.try_new(-999999999_i32, 0_u8).to_s.should eq("-999999999")
    D4.try_new(120050_i32, 4_u8).to_s.should eq("12.005")
    D4.try_new(-120050_i32, 4_u8).to_s.should eq("-12.005")
  end

  # Upstream doc examples: `as_integer` and `is_valid_precision_and_scale`.
  it "converts to an integer and validates precision/scale" do
    D4.try_new(123_i32, 0_u8).as_integer.should eq(123)
    D4.try_new(100_i32, 2_u8).as_integer.should eq(1)
    D4.try_new(-100_i32, 2_u8).as_integer.should eq(-1)
    D4.try_new(0_i32, 4_u8).as_integer.should eq(0)
    D4.try_new(123_i32, 2_u8).as_integer.should be_nil
    D4.try_new(-123_i32, 2_u8).as_integer.should be_nil

    D4.is_valid_precision_and_scale(5_u8, 2_i8).should be_true
    D4.is_valid_precision_and_scale(10_u8, 2_i8).should be_false
    D4.is_valid_precision_and_scale(5_u8, -1_i8).should be_false
    D4.is_valid_precision_and_scale(5_u8, 7_i8).should be_false
  end

  it "rescales signed scales" do
    D4.try_new_with_signed_scale(123_i32, -2_i8).to_s.should eq("12300")
    D4.try_new_with_signed_scale(12345_i32, 2_i8).to_s.should eq("123.45")
  end
end

describe ParquetVariant::VariantDecimal8 do
  # Upstream `test_variant_decimal_invalid_precision` (Decimal8 part).
  it "validates precision" do
    expect_raises(Err, /wider than max precision/) { D8.try_new(1_000_000_000_000_000_000_i64, 2_u8) }
    expect_raises(Err, /wider than max precision/) { D8.try_new(-1_000_000_000_000_000_000_i64, 2_u8) }
    D8.try_new(999_999_999_999_999_999_i64, 2_u8).integer.should eq(999_999_999_999_999_999)
    D8.try_new(-999_999_999_999_999_999_i64, 2_u8).integer.should eq(-999_999_999_999_999_999)
  end

  # Upstream `test_variant_decimal_invalid_scale` (Decimal8 part).
  it "validates scale" do
    expect_raises(Err, /larger than max precision/) { D8.try_new(123_i64, 19_u8) }
    expect_raises(Err) { D8.try_new(123_i64, 25_u8) }
    D8.try_new(123_i64, 18_u8).scale.should eq(18)
  end

  # Upstream `test_variant_decimal8_display`.
  it "displays" do
    D8.try_new(42_i64, 0_u8).to_s.should eq("42")
    D8.try_new(-42_i64, 0_u8).to_s.should eq("-42")
    D8.try_new(1234567890_i64, 3_u8).to_s.should eq("1234567.89")
    D8.try_new(-1234567890_i64, 3_u8).to_s.should eq("-1234567.89")
    D8.try_new(123000000_i64, 6_u8).to_s.should eq("123")
    D8.try_new(-123000000_i64, 6_u8).to_s.should eq("-123")
    D8.try_new(100005_i64, 6_u8).to_s.should eq("0.100005")
    D8.try_new(-100005_i64, 6_u8).to_s.should eq("-0.100005")
    D8.try_new(123_i64, 10_u8).to_s.should eq("0.0000000123")
    D8.try_new(-123_i64, 10_u8).to_s.should eq("-0.0000000123")
    D8.try_new(0_i64, 0_u8).to_s.should eq("0")
    D8.try_new(0_i64, 10_u8).to_s.should eq("0")
    D8.try_new(123456789012345678_i64, 18_u8).to_s.should eq("0.123456789012345678")
    D8.try_new(-123456789012345678_i64, 18_u8).to_s.should eq("-0.123456789012345678")
    D8.try_new(999999999999999999_i64, 0_u8).to_s.should eq("999999999999999999")
    D8.try_new(-999999999999999999_i64, 0_u8).to_s.should eq("-999999999999999999")
    D8.try_new(1200000050000_i64, 10_u8).to_s.should eq("120.000005")
    D8.try_new(-1200000050000_i64, 10_u8).to_s.should eq("-120.000005")
  end
end

describe ParquetVariant::VariantDecimal16 do
  # Upstream `test_variant_decimal_invalid_precision` (Decimal16 part).
  it "validates precision" do
    too_large = 100000000000000000000000000000000000000_i128
    expect_raises(Err, /wider than max precision/) { D16.try_new(too_large, 2_u8) }
    expect_raises(Err, /wider than max precision/) { D16.try_new(-too_large, 2_u8) }
    max = 99999999999999999999999999999999999999_i128
    D16.try_new(max, 2_u8).integer.should eq(max)
    D16.try_new(-max, 2_u8).integer.should eq(-max)
  end

  # Upstream `test_variant_decimal_invalid_scale` (Decimal16 part).
  it "validates scale" do
    expect_raises(Err, /larger than max precision/) { D16.try_new(123_i128, 39_u8) }
    expect_raises(Err) { D16.try_new(123_i128, 50_u8) }
    D16.try_new(123_i128, 38_u8).scale.should eq(38)
  end

  # Upstream `test_variant_decimal16_display`.
  it "displays" do
    D16.try_new(42_i128, 0_u8).to_s.should eq("42")
    D16.try_new(-42_i128, 0_u8).to_s.should eq("-42")
    D16.try_new(123456789012345_i128, 4_u8).to_s.should eq("12345678901.2345")
    D16.try_new(-123456789012345_i128, 4_u8).to_s.should eq("-12345678901.2345")
    D16.try_new(12300000000_i128, 8_u8).to_s.should eq("123")
    D16.try_new(-12300000000_i128, 8_u8).to_s.should eq("-123")
    D16.try_new(10000005_i128, 8_u8).to_s.should eq("0.10000005")
    D16.try_new(-10000005_i128, 8_u8).to_s.should eq("-0.10000005")
    D16.try_new(123_i128, 20_u8).to_s.should eq("0.00000000000000000123")
    D16.try_new(-123_i128, 20_u8).to_s.should eq("-0.00000000000000000123")
    D16.try_new(0_i128, 0_u8).to_s.should eq("0")
    D16.try_new(0_i128, 20_u8).to_s.should eq("0")
    D16.try_new(12345678901234567890123456789012345678_i128, 38_u8).to_s.should eq("0.12345678901234567890123456789012345678")
    D16.try_new(-12345678901234567890123456789012345678_i128, 38_u8).to_s.should eq("-0.12345678901234567890123456789012345678")
    D16.try_new(99999999999999999999999999999999999999_i128, 0_u8).to_s.should eq("99999999999999999999999999999999999999")
    D16.try_new(-99999999999999999999999999999999999999_i128, 0_u8).to_s.should eq("-99999999999999999999999999999999999999")
    D16.try_new(12000000000000050000000000000_i128, 25_u8).to_s.should eq("1200.000000000005")
    D16.try_new(-12000000000000050000000000000_i128, 25_u8).to_s.should eq("-1200.000000000005")
    D16.try_new(12345678901234567890123456789_i128, 0_u8).to_s.should eq("12345678901234567890123456789")
    D16.try_new(-12345678901234567890123456789_i128, 0_u8).to_s.should eq("-12345678901234567890123456789")
  end
end
