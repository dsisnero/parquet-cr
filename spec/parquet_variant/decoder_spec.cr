require "../spec_helper"

# Ported from `parquet-variant/src/decoder.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: chrono `NaiveDate`/`DateTime`/`NaiveTime` are `Time`/`Time::Span`;
# `Uuid` is `UUID`; `ArrowError` variants are `ParquetVariant::*Error`s.
# `decode_date`'s out-of-range check relies on Crystal's `Time` range.

private alias Dec = ParquetVariant::Decoder
private alias VPErr = ParquetVariant::InvalidArgumentError

private def bytes_of(values : Array(Int32)) : Bytes
  Bytes.new(values.size) { |i| values[i].to_u8 }
end

private def le_bytes(value : Int64, size : Int32) : Bytes
  Bytes.new(size) { |i| (value >> (8 * i) & 0xFF).to_u8 }
end

private def check_bounds(data : Array(Int32), decode_fn : Proc(Bytes, T), expected : T) : Nil forall T
  bytes = bytes_of(data)
  decode_fn.call(bytes).should eq(expected)
  truncated = bytes[0, bytes.size - 1]
  expect_raises(VPErr) { decode_fn.call(truncated) }
end

describe ParquetVariant::Decoder do
  # Upstream `integer` module.
  it "decodes integers" do
    check_bounds([0x2a], ->(b : Bytes) { Dec.decode_int8(b) }, 42_i8)
    check_bounds([0xd2, 0x04], ->(b : Bytes) { Dec.decode_int16(b) }, 1234_i16)
    check_bounds([0x40, 0xe2, 0x01, 0x00], ->(b : Bytes) { Dec.decode_int32(b) }, 123456_i32)
    check_bounds(
      [0x15, 0x81, 0xe9, 0x7d, 0xf4, 0x10, 0x22, 0x11],
      ->(b : Bytes) { Dec.decode_int64(b) },
      1234567890123456789_i64
    )
  end

  # Upstream `decimal` module.
  it "decodes decimals" do
    check_bounds(
      [0x02, 0xd2, 0x04, 0x00, 0x00],
      ->(b : Bytes) { Dec.decode_decimal4(b) },
      {1234_i32, 2_u8}
    )
    check_bounds(
      [0x02, 0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00],
      ->(b : Bytes) { Dec.decode_decimal8(b) },
      {1234567890_i64, 2_u8}
    )
    check_bounds(
      [0x02, 0xd2, 0xb6, 0x23, 0xc0, 0xf4, 0x10, 0x22, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
      ->(b : Bytes) { Dec.decode_decimal16(b) },
      {1234567891234567890_i128, 2_u8}
    )
  end

  # Upstream `float` module.
  it "decodes floats" do
    check_bounds([0x06, 0x2c, 0x93, 0x4e], ->(b : Bytes) { Dec.decode_float(b) }, 1_234_568_000.0_f32)
    check_bounds(
      [0xc9, 0xe5, 0x87, 0xb4, 0x80, 0x65, 0xd2, 0x41],
      ->(b : Bytes) { Dec.decode_double(b) },
      1234567890.1234_f64
    )
  end

  # Upstream `datetime` module.
  it "decodes dates" do
    check_bounds([0xe2, 0x4e, 0x0, 0x0], ->(b : Bytes) { Dec.decode_date(b) }, Time.utc(2025, 4, 16))
  end

  it "rejects an out-of-range date" do
    expect_raises(ParquetVariant::CastError) { Dec.decode_date(bytes_of([0xFF, 0xFF, 0xFF, 0x7F])) }
    expect_raises(ParquetVariant::CastError) { Dec.decode_date(bytes_of([0x00, 0x00, 0x00, 0x80])) }
  end

  it "decodes timestamps" do
    expected = Time.utc(2025, 4, 16, 16, 34, 56, nanosecond: 780_000_000)
    check_bounds(
      [0xe0, 0x52, 0x97, 0xdd, 0xe7, 0x32, 0x06, 0x00],
      ->(b : Bytes) { Dec.decode_timestamp_micros(b) },
      expected
    )
    check_bounds(
      [0xe0, 0x52, 0x97, 0xdd, 0xe7, 0x32, 0x06, 0x00],
      ->(b : Bytes) { Dec.decode_timestampntz_micros(b) },
      expected
    )
    check_bounds(
      [0x15, 0x41, 0xa2, 0x5a, 0x36, 0xa2, 0x5b, 0x18],
      ->(b : Bytes) { Dec.decode_timestamp_nanos(b) },
      Time.utc(2025, 8, 14, 12, 33, 54, nanosecond: 123456789)
    )
    check_bounds(
      [0x15, 0x41, 0x52, 0xd4, 0x94, 0xe5, 0xad, 0xfa],
      ->(b : Bytes) { Dec.decode_timestamp_nanos(b) },
      Time.utc(1957, 11, 7, 12, 33, 54, nanosecond: 123456789)
    )
    check_bounds(
      [0x15, 0x41, 0xa2, 0x5a, 0x36, 0xa2, 0x5b, 0x18],
      ->(b : Bytes) { Dec.decode_timestampntz_nanos(b) },
      Time.utc(2025, 8, 14, 12, 33, 54, nanosecond: 123456789)
    )
    check_bounds(
      [0x15, 0x41, 0x52, 0xd4, 0x94, 0xe5, 0xad, 0xfa],
      ->(b : Bytes) { Dec.decode_timestampntz_nanos(b) },
      Time.utc(1957, 11, 7, 12, 33, 54, nanosecond: 123456789)
    )
  end

  # Upstream `test_uuid`.
  it "decodes uuids" do
    check_bounds(
      [0xf2, 0x4f, 0x9b, 0x64, 0x81, 0xfa, 0x49, 0xd1, 0xb7, 0x4e, 0x8c, 0x09, 0xa6, 0xe3, 0x1c, 0x56],
      ->(b : Bytes) { Dec.decode_uuid(b) },
      UUID.new("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")
    )
  end

  # Upstream `time` module.
  it "decodes times" do
    check_bounds(
      [0x53, 0x1f, 0x8e, 0xdf, 0x2, 0, 0, 0],
      ->(b : Bytes) { Dec.decode_time_ntz(b) },
      Time::Span.new(nanoseconds: 12340_i64 * 1_000_000_000 + 567_891_000)
    )
  end

  it "rejects an invalid time" do
    expect_raises(ParquetVariant::CastError) { Dec.decode_time_ntz(le_bytes(-1_i64, 8)) }
  end

  # Upstream `test_binary_exact_length` / `test_binary_truncated_length`.
  it "decodes binary" do
    data = bytes_of([0x09, 0, 0, 0, 0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe])
    Dec.decode_binary(data).should eq(bytes_of([0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]))

    truncated = data[0, data.size - 1]
    expect_raises(VPErr) { Dec.decode_binary(truncated) }
  end

  # Upstream `test_short_string_exact_length` / `test_short_string_truncated_length`.
  it "decodes short strings" do
    Dec.decode_short_string((1_u8 | (5_u8 << 2)), "Helloo".to_slice).value.should eq("Hello")
    expect_raises(VPErr) { Dec.decode_short_string((1_u8 | (5_u8 << 2)), "Abc".to_slice) }
  end

  # Upstream `test_string_exact_length` / `test_string_truncated_length`.
  it "decodes long strings" do
    data = bytes_of([0x05, 0, 0, 0, 'H'.ord, 'e'.ord, 'l'.ord, 'l'.ord, 'o'.ord, 'o'.ord])
    Dec.decode_long_string(data).should eq("Hello")

    truncated = bytes_of([0x05, 0, 0, 0, 'H'.ord, 'e'.ord, 'l'.ord])
    expect_raises(VPErr) { Dec.decode_long_string(truncated) }
  end

  # Upstream `test_offset`.
  it "builds OffsetSizeBytes" do
    ParquetVariant::OffsetSizeBytes.try_new(0_u8).should eq(ParquetVariant::OffsetSizeBytes::One)
    ParquetVariant::OffsetSizeBytes.try_new(1_u8).should eq(ParquetVariant::OffsetSizeBytes::Two)
    ParquetVariant::OffsetSizeBytes.try_new(2_u8).should eq(ParquetVariant::OffsetSizeBytes::Three)
    ParquetVariant::OffsetSizeBytes.try_new(3_u8).should eq(ParquetVariant::OffsetSizeBytes::Four)
    expect_raises(VPErr) { ParquetVariant::OffsetSizeBytes.try_new(4_u8) }
    expect_raises(VPErr) { ParquetVariant::OffsetSizeBytes.try_new(255_u8) }
  end

  # Upstream `unpack_u32_all_widths` / `unpack_u32_out_of_bounds` / `unpack_simple`.
  it "unpacks u32 offsets" do
    one = ParquetVariant::OffsetSizeBytes::One
    one.unpack_u32(bytes_of([0x01, 0xAB, 0xCD]), 0).should eq(0x01)
    one.unpack_u32(bytes_of([0x01, 0xAB, 0xCD]), 2).should eq(0xCD)

    two = ParquetVariant::OffsetSizeBytes::Two
    buf_two = bytes_of([0x34, 0x12, 0x78, 0x56])
    two.unpack_u32(buf_two, 0).should eq(0x1234)
    two.unpack_u32(buf_two, 1).should eq(0x5678)

    three = ParquetVariant::OffsetSizeBytes::Three
    buf_three = bytes_of([0x01, 0x02, 0x03, 0xFF, 0x00, 0x00])
    three.unpack_u32(buf_three, 0).should eq(0x030201)
    three.unpack_u32(buf_three, 1).should eq(0x0000FF)

    four = ParquetVariant::OffsetSizeBytes::Four
    buf_four = bytes_of([0x78, 0x56, 0x34, 0x12, 0xEF, 0xCD, 0xAB, 0x90])
    four.unpack_u32(buf_four, 0).should eq(0x1234_5678)
    four.unpack_u32(buf_four, 1).should eq(0x90AB_CDEF)

    tiny = bytes_of([0x00])
    expect_raises(VPErr) { two.unpack_u32(tiny, 0) }
    expect_raises(VPErr) { three.unpack_u32(tiny, 0) }

    buf = bytes_of([0x41, 0x02, 0x00, 0x00, 0x00, 0x05, 0x00, 0x09, 0x00])
    width = ParquetVariant::OffsetSizeBytes::Two
    width.unpack_u32_at_offset(buf, 1, 0).should eq(2)
    width.unpack_u32_at_offset(buf, 1, 1).should eq(0)
    width.unpack_u32_at_offset(buf, 1, 2).should eq(5)
    width.unpack_u32_at_offset(buf, 1, 3).should eq(9)
    expect_raises(VPErr) { width.unpack_u32_at_offset(buf, 1, 4) }
  end
end
