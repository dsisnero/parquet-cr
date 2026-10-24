require "../spec_helper"

# Ported from parquet-variant-compute/{cast_to_variant,arrow_to_variant}.rs
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
private alias PVC = ParquetVariantCompute
private alias Variant = ParquetVariant::Variant
private alias DT = Arrow::DataType

private def cast_required(value : T?) : T forall T
  raise "Expected a value" if value.nil?
  value
end

private def assert_variant_cast(input : Arrow::ArrayBase, expected : Array(Variant?)) : Nil
  [true, false].each do |safe|
    output = PVC.cast_to_variant_with_options(input, Arrow::CastOptions.new(safe: safe))
    output.len.should eq expected.size
    expected.each_with_index do |value, index|
      output.is_null(index).should eq value.nil?
      output.value(index).should eq value if value
    end
  end
end

describe "cast_to_variant scalar values" do
  {% for type in [Int8, Int16, Int32, Int64] %}
    it "converts {{type}} without changing its variant width" do
      values = [{{type}}::MIN, nil, {{type}}.new(-1), {{type}}.new(1), {{type}}::MAX] of {{type}}?
      expected = values.map { |value| value.nil? ? nil : Variant.from(value).as(Variant) }
      assert_variant_cast(Arrow::PrimitiveArray({{type}}).from_nullable(values), expected)
    end
  {% end %}
  {% for type in [UInt8, UInt16, UInt32, UInt64] %}
    it "widens {{type}} at signed boundaries" do
      values = [{{type}}.new(0), nil, {{type}}.new(1), {{type}}::MAX] of {{type}}?
      expected = values.map { |value| value.nil? ? nil : Variant.from(value).as(Variant) }
      assert_variant_cast(Arrow::PrimitiveArray({{type}}).from_nullable(values), expected)
    end
  {% end %}
  it "preserves Boolean false and SQL nulls" do
    assert_variant_cast(Arrow::BooleanArray.from_nullable([true, nil, false]), [Variant.from(true), nil, Variant.from(false)] of Variant?)
  end
  {% for type in [Float32, Float64] %}
    it "converts {{type}} extremes and signed values" do
      values = [{{type}}::MIN, nil, {{type}}.new(-1.5), {{type}}.new(0), {{type}}.new(1.5), {{type}}::MAX] of {{type}}?
      expected = values.map { |value| value.nil? ? nil : Variant.from(value).as(Variant) }
      assert_variant_cast(Arrow::PrimitiveArray({{type}}).from_nullable(values), expected)
    end
  {% end %}
  it "widens half floats to Variant::Float" do
    values = [-65504_f32, nil, -1.5_f32, 0_f32, 1.5_f32, 65504_f32] of Float32?
    input = Arrow::PrimitiveArray(Arrow::Float16).from_nullable(values.map { |value| value.nil? ? nil : Arrow::Float16.from_f32(value) })
    assert_variant_cast(input, values.map { |value| value.nil? ? nil : Variant.from(value).as(Variant) })
  end
  it "fast-paths empty and all-null inputs, including unsupported types" do
    [Arrow::NullArray.new(2), Arrow.new_null_array(DT.duration(Arrow::TimeUnit::Second), 3), Arrow.new_null_array(DT.interval(Arrow::IntervalUnit::DayTime), 0)].each do |input|
      output = PVC.cast_to_variant(input)
      output.len.should eq input.len
      output.nulls.try(&.null_count).should eq input.len unless input.len == 0
    end
  end
end

private def assert_safe_variant_cast(input : Arrow::ArrayBase, expected : Array(Variant?)) : Nil
  output = PVC.cast_to_variant_with_options(input, Arrow::CastOptions.default)
  output.len.should eq expected.size
  expected.each_with_index do |value, index|
    output.is_null(index).should eq value.nil?
    output.value(index).should eq value if value
  end
end

describe "cast_to_variant fallible scalar conversions" do
  {% for config in [{32, 4, 9}, {64, 8, 18}, {128, 16, 38}] %}
    {% bits, bytes, precision = config[0], config[1], config[2] %}
    it "converts Decimal{{bits}} boundaries, signed scales and overflow" do
      max = ParquetVariant::VariantDecimal{{bytes.id}}::MAX_UNSCALED_VALUE
      values = [Int{{bits.id}}::MIN, -max - 1, -max, nil, Int{{bits.id}}.new(-123), Int{{bits.id}}.new(0), Int{{bits.id}}.new(123), max, max + 1, Int{{bits.id}}::MAX] of Int{{bits.id}}?
      input = Arrow::PrimitiveArray(Int{{bits.id}}).from_nullable(values).with_data_type(DT.decimal{{bits.id}}({{precision}}, 3))
      expected = values.map do |value|
        if value.nil?
          nil
        elsif -max <= value <= max
          Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(value, 3)).as(Variant)
        else
          Variant::Null.new.as(Variant)
        end
      end
      assert_safe_variant_cast(input, expected)
      error = expect_raises(Arrow::ArrowError) { PVC.cast_to_variant(input) }
      error.message.should eq "Compute error: Failed to convert value at index 0: conversion failed"
      bound = Int{{bits.id}}.new(10) ** ({{precision}} - 3) - 1
      values = [-bound - 1, -bound, nil, Int{{bits.id}}.new(-123), Int{{bits.id}}.new(0), Int{{bits.id}}.new(123), bound, bound + 1] of Int{{bits.id}}?
      input = Arrow::PrimitiveArray(Int{{bits.id}}).from_nullable(values).with_data_type(DT.decimal{{bits.id}}({{precision}}, -3))
      expected = values.map do |value|
        if value.nil?
          nil
        elsif -bound <= value <= bound
          Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(value * 1000, 0)).as(Variant)
        else
          Variant::Null.new.as(Variant)
        end
      end
      assert_safe_variant_cast(input, expected)
    end
  {% end %}

  it "converts Decimal256 through Int128 with overflow checks" do
    max = ParquetVariant::VariantDecimal16::MAX_UNSCALED_VALUE
    values = [Arrow::I256::MIN, Arrow::I256.new(-max - 1), Arrow::I256.new(-max), nil, Arrow::I256.new(123), Arrow::I256.new(max), Arrow::I256.new(max + 1), Arrow::I256::MAX] of Arrow::I256?
    input = Arrow::PrimitiveArray(Arrow::I256).from_nullable(values).with_data_type(DT.decimal256(38, 3))
    assert_safe_variant_cast(input, [Variant::Null.new, Variant::Null.new,
                                     Variant.from(ParquetVariant::VariantDecimal16.try_new(-max, 3)), nil,
                                     Variant.from(ParquetVariant::VariantDecimal16.try_new(123_i128, 3)),
                                     Variant.from(ParquetVariant::VariantDecimal16.try_new(max, 3)), Variant::Null.new, Variant::Null.new] of Variant?)
    expect_raises(Arrow::ArrowError, "Failed to convert value at index 0") { PVC.cast_to_variant(input) }
    input = Arrow::PrimitiveArray(Arrow::I256).from_values([Arrow::I256.new(123)]).with_data_type(DT.decimal256(38, -3))
    assert_variant_cast(input, [Variant.from(ParquetVariant::VariantDecimal16.try_new(123000_i128, 0))] of Variant?)
  end

  [Arrow::TimeUnit::Second, Arrow::TimeUnit::Millisecond, Arrow::TimeUnit::Microsecond, Arrow::TimeUnit::Nanosecond].each do |unit|
    it "preserves #{unit} timestamps and timezone presence" do
      value = case unit
              when .second?      then 1_i64
              when .millisecond? then 1234_i64
              when .microsecond? then 1234567_i64
              else                    1234567890_i64
              end
      nanos = case unit
              when .second?      then 1_000_000_000_i64
              when .millisecond? then 1_234_000_000_i64
              when .microsecond? then 1_234_567_000_i64
              else                    1_234_567_890_i64
              end
      [nil, "+01:00"].each do |timezone|
        input = Arrow::PrimitiveArray(Int64).from_nullable([value, nil]).with_data_type(DT.timestamp(unit, timezone))
        time = Time.unix_ns(nanos)
        expected = timezone ? Variant.from_datetime_utc(time) : Variant.from_naive_datetime(time)
        assert_variant_cast(input, [expected, nil] of Variant?)
      end
    end
  end

  it "distinguishes failed timestamps and dates from SQL nulls" do
    [DT.timestamp(Arrow::TimeUnit::Second), DT.date64].each do |type|
      input = Arrow::PrimitiveArray(Int64).from_nullable([Int64::MAX, 0_i64, nil, Int64::MIN]).with_data_type(type)
      expected = type.kind.timestamp? ? Variant.from_naive_datetime(Time.unix(0)) : Variant.from_date(Time.unix(0))
      assert_safe_variant_cast(input, [Variant::Null.new, expected, nil, Variant::Null.new] of Variant?)
      expect_raises(Arrow::ArrowError, "Failed to convert value at index 0: conversion failed") { PVC.cast_to_variant(input) }
    end
    assert_variant_cast(Arrow::PrimitiveArray(Int32).from_nullable([0, nil, 20301]).with_data_type(DT.date32),
      [Variant.from_date(Time.utc(1970, 1, 1)), nil, Variant.from_date(Time.utc(2025, 8, 1))] of Variant?)
  end

  it "characterizes the Crystal calendar range for dates and timestamps" do
    first = Time.utc(1, 1, 1)
    last = Time.utc(9999, 12, 31)
    dates = Arrow::PrimitiveArray(Int32).from_values([first.to_unix // 86400, last.to_unix // 86400].map(&.to_i32)).with_data_type(DT.date32)
    assert_variant_cast(dates, [Variant.from_date(first), Variant.from_date(last)] of Variant?)
    timestamps = Arrow::PrimitiveArray(Int64).from_values([first.to_unix, last.to_unix]).with_data_type(DT.timestamp(Arrow::TimeUnit::Second))
    assert_variant_cast(timestamps, [Variant.from_naive_datetime(first), Variant.from_naive_datetime(last)] of Variant?)
    # Chrono can represent year zero; Crystal Time cannot.
    outside = Arrow::PrimitiveArray(Int64).from_values([first.to_unix - 1]).with_data_type(DT.timestamp(Arrow::TimeUnit::Second))
    assert_safe_variant_cast(outside, [Variant::Null.new] of Variant?)
    expect_raises(Arrow::ArrowError, "Failed to convert value at index 0") { PVC.cast_to_variant(outside) }
  end

  it "converts all time units, truncates nanoseconds to micros, and validates midnight bounds" do
    types = [DT.time32(Arrow::TimeUnit::Second), DT.time32(Arrow::TimeUnit::Millisecond), DT.time64(Arrow::TimeUnit::Microsecond), DT.time64(Arrow::TimeUnit::Nanosecond)]
    types.each do |type|
      if type.kind.time32?
        input = Arrow::PrimitiveArray(Int32).from_nullable([1, 3600, nil]).with_data_type(type)
        divisor = type.time_unit == Arrow::TimeUnit::Second ? 1 : 1000
        assert_variant_cast(input, [Variant.from_time(Time::Span.new(nanoseconds: 1_000_000_000_i64 // divisor)), Variant.from_time(Time::Span.new(nanoseconds: 3600_000_000_000_i64 // divisor)), nil] of Variant?)
      else
        input = Arrow::PrimitiveArray(Int64).from_nullable([1_i64, 1001_i64, nil]).with_data_type(type)
        multiplier = type.time_unit == Arrow::TimeUnit::Microsecond ? 1000_i64 : 1_i64
        assert_variant_cast(input, [Variant.from_time(Time::Span.new(nanoseconds: (multiplier // 1000) * 1000)), Variant.from_time(Time::Span.new(nanoseconds: (1001 * multiplier // 1000) * 1000)), nil] of Variant?)
      end
    end
    input = Arrow::PrimitiveArray(Int32).from_values([90000, 3600, -1]).with_data_type(DT.time32(Arrow::TimeUnit::Second))
    assert_safe_variant_cast(input, [Variant::Null.new, Variant.from_time(1.hour), Variant::Null.new] of Variant?)
    expect_raises(Arrow::ArrowError, "Failed to convert value at index 0") { PVC.cast_to_variant(input) }
  end

  it "rejects non-null durations, intervals and invalid time-unit combinations" do
    [DT.duration(Arrow::TimeUnit::Second), DT.interval(Arrow::IntervalUnit::YearMonth)].each do |type|
      input = type.kind.duration? ? Arrow::PrimitiveArray(Int64).from_values([1_i64]).with_data_type(type) : Arrow::PrimitiveArray(Int32).from_values([1]).with_data_type(type)
      expect_raises(ParquetVariant::InvalidArgumentError, "Casting duration/interval types to Variant is not supported.") { PVC.cast_to_variant(input) }
    end
  end
end

private def cast_json(value : String) : Variant
  builder = PVC::VariantArrayBuilder.new
  append_expected_cast_value(builder, JSON.parse(value))
  builder.build.value(0)
end

# The upstream nested fixtures store Int32 leaves, rather than JSON's narrowest
# integer width. Keep exact Variant equality (including integer widths).
private def append_expected_cast_value(builder : ParquetVariant::VariantBuilderExt, value : JSON::Any) : Nil
  case raw = value.raw
  when Int64
    builder.append_value(Variant.from(raw.to_i32))
  when String
    builder.append_value(Variant.from(raw))
  when Nil
    builder.append_value(Variant::Null.new)
  when Array(JSON::Any)
    list = builder.try_new_list
    raw.each { |item| append_expected_cast_value(list, item) }
    list.finish
  when Hash(String, JSON::Any)
    object = builder.try_new_object
    raw.each { |name, item| append_expected_cast_value(ParquetVariant::ObjectFieldBuilder.new(name, object), item) }
    object.finish
  else
    raise "Unsupported expected fixture"
  end
end

private def cast_strings(values : Array(String?)) : Arrow::ArrayBase
  builder = Arrow::GenericStringBuilder(Int32).new
  values.each { |value| value ? builder.append_value(value) : builder.append_null }
  builder.finish
end

describe "cast_to_variant byte and nested arrays" do
  {% for builder_type in [Arrow::GenericStringBuilder(Int32), Arrow::GenericStringBuilder(Int64), Arrow::StringViewBuilder] %}
    it "converts {{builder_type}} strings" do
      builder = {{builder_type}}.new
      values = ["hello", nil, "", "a string longer than twelve bytes"] of String?
      values.each { |value| value ? builder.append_value(value) : builder.append_null }
      assert_variant_cast(builder.finish, values.map { |value| value ? Variant.from(value).as(Variant) : nil })
    end
  {% end %}
  {% for builder_type in [Arrow::GenericBinaryBuilder(Int32), Arrow::GenericBinaryBuilder(Int64), Arrow::BinaryViewBuilder] %}
    it "converts {{builder_type}} binary data" do
      builder = {{builder_type}}.new
      values = [Bytes[0, 255, 1], nil, Bytes.empty] of Bytes?
      values.each { |value| value ? builder.append_value(value) : builder.append_null }
      assert_variant_cast(builder.finish, values.map { |value| value ? Variant.from(value).as(Variant) : nil })
    end
  {% end %}
  it "keeps fixed-size binary as binary, including sixteen-byte values" do
    builder = Arrow::FixedSizeBinaryBuilder.new(16)
    bytes = Bytes.new(16, 42)
    builder.append_value(bytes)
    builder.append_null
    assert_variant_cast(builder.finish, [Variant.from(bytes), nil] of Variant?)
  end

  it "converts and slices every list layout, preserving explicit null elements" do
    values = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 4, 5])
    field = Arrow::Field.new("item", DT.int32, true)
    nulls = Arrow::NullBuffer.from_slice([true, false, true])
    arrays = [] of Arrow::ArrayBase
    arrays << Arrow::GenericListArray(Int32).new(field, Arrow::OffsetBuffer(Int32).new(Arrow::ScalarBuffer(Int32).from([0, 3, 3, 5])), values, nulls)
    arrays << Arrow::GenericListArray(Int64).new(field, Arrow::OffsetBuffer(Int64).new(Arrow::ScalarBuffer(Int64).from([0_i64, 3_i64, 3_i64, 5_i64])), values, nulls)
    arrays << Arrow::GenericListViewArray(Int32).new(field, Arrow::ScalarBuffer(Int32).from([0, 3, 3]), Arrow::ScalarBuffer(Int32).from([3, 0, 2]), values, nulls)
    arrays << Arrow::GenericListViewArray(Int64).new(field, Arrow::ScalarBuffer(Int64).from([0_i64, 3_i64, 3_i64]), Arrow::ScalarBuffer(Int64).from([3_i64, 0_i64, 2_i64]), values, nulls)
    arrays.each do |input|
      assert_variant_cast(input, [cast_json("[1,null,3]"), nil, cast_json("[4,5]")] of Variant?)
      assert_variant_cast(input.slice(1, 2), [nil, cast_json("[4,5]")] of Variant?)
    end
    fixed_values = Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3, 4, nil, 5])
    fixed = Arrow::FixedSizeListArray.try_new(field, 2, fixed_values, nulls)
    assert_variant_cast(fixed, [cast_json("[1,null]"), nil, cast_json("[null,5]")] of Variant?)
    assert_variant_cast(fixed.slice(1, 2), [nil, cast_json("[null,5]")] of Variant?)
  end

  it "omits null struct fields, masks parent rows, and supports nested structs" do
    inner = PVC::StructArrayBuilder.new
      .with_field("id", Arrow::PrimitiveArray(Int32).from_nullable([1, nil, 3]), true)
      .with_field("name", cast_strings(["Alice", "Bob", nil] of String?), true).build
    assert_variant_cast(inner, [cast_json(%({"id":1,"name":"Alice"})), cast_json(%({"name":"Bob"})), cast_json(%({"id":3}))] of Variant?)
    outer = PVC::StructArrayBuilder.new.with_field("nested", inner, true)
      .with_nulls(Arrow::NullBuffer.from_slice([true, false, true])).build
    assert_variant_cast(outer, [cast_json(%({"nested":{"id":1,"name":"Alice"}})), nil, cast_json(%({"nested":{"id":3}}))] of Variant?)
  end

  it "converts maps, including empty/null maps, null values and non-string keys" do
    values = Arrow::PrimitiveArray(Int32).from_nullable([10, nil, 30])
    entries = PVC::StructArrayBuilder.new.with_field("keys", cast_strings(["a", "b", "c"] of String?), false).with_field("values", values, true).build
    map = Arrow::MapArray.try_new(Arrow::Field.new("entries", entries.data_type, false),
      Arrow::OffsetBuffer(Int32).new(Arrow::ScalarBuffer(Int32).from([0, 2, 2, 2, 3])), entries, Arrow::NullBuffer.from_slice([true, false, true, true]), false)
    assert_variant_cast(map, [cast_json(%({"a":10})), nil, cast_json("{}"), cast_json(%({"c":30}))] of Variant?)
    entries = PVC::StructArrayBuilder.new.with_field("keys", Arrow::PrimitiveArray(Int32).from_values([1, 2, 3]), false).with_field("values", values, true).build
    map = Arrow::MapArray.try_new(Arrow::Field.new("entries", entries.data_type, false),
      Arrow::OffsetBuffer(Int32).new(Arrow::ScalarBuffer(Int32).from([0, 3])), entries, nil, false)
    assert_variant_cast(map, [cast_json(%({"1":10,"3":30}))] of Variant?)
  end
end

describe "cast_to_variant encoded and union arrays" do
  {% for key_type in [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64] %}
    it "converts {{key_type}} dictionary keys, null keys and random access" do
      keys = Arrow::PrimitiveArray({{key_type}}).from_nullable([{{key_type}}.new(0), nil, {{key_type}}.new(1), {{key_type}}.new(0), {{key_type}}.new(2)])
      input = Arrow::DictionaryArray({{key_type}}).try_new(keys, cast_strings(["apple", "banana", "cherry"] of String?))
      assert_variant_cast(input, [Variant.from("apple"), nil, Variant.from("banana"), Variant.from("apple"), Variant.from("cherry")] of Variant?)
      assert_variant_cast(input.slice(2, 3), [Variant.from("banana"), Variant.from("apple"), Variant.from("cherry")] of Variant?)
      rows = PVC::ArrowToVariant.make_row_builder(input.data_type, input, Arrow::CastOptions.new(safe: false))
      [4, 0, 3, 1, 2].each do |index|
        builder = PVC::VariantArrayBuilder.new
        rows.append_row(builder, index)
        output = builder.build
        if index == 1
          output.is_null(0).should be_true
        else
          output.value(0).should eq Variant.from(["apple", "", "banana", "apple", "cherry"][index])
        end
      end
      empty = Arrow::DictionaryArray({{key_type}}).try_new(Arrow::PrimitiveArray({{key_type}}).from_nullable([nil] of {{key_type}}?), cast_strings([] of String?))
      rows = PVC::ArrowToVariant.make_row_builder(empty.data_type, empty, Arrow::CastOptions.default)
      builder = PVC::VariantArrayBuilder.new
      rows.append_row(builder, 0)
      builder.build.is_null(0).should be_true
    end
  {% end %}

  {% for run_type in [Int16, Int32, Int64] %}
    it "converts {{run_type}} run ends with null runs and random access" do
      ends = Arrow::PrimitiveArray({{run_type}}).from_values([{{run_type}}.new(2), {{run_type}}.new(4), {{run_type}}.new(5)])
      input = Arrow::RunArray({{run_type}}).try_new(ends, cast_strings(["A", nil, "B"] of String?))
      assert_variant_cast(input, [Variant.from("A"), Variant.from("A"), nil, nil, Variant.from("B")] of Variant?)
      rows = PVC::ArrowToVariant.make_row_builder(input.data_type, input, Arrow::CastOptions.new(safe: false))
      [4, 0, 3, 1, 2].each do |index|
        builder = PVC::VariantArrayBuilder.new
        rows.append_row(builder, index)
        output = builder.build
        if index == 2 || index == 3
          output.is_null(0).should be_true
        else
          output.value(0).should eq Variant.from(index == 4 ? "B" : "A")
        end
      end
    end
  {% end %}

  [Arrow::UnionMode::Dense, Arrow::UnionMode::Sparse].each do |mode|
    it "converts #{mode} unions with non-dense type IDs and child nulls" do
      fields = Arrow::UnionFields.new([{3_i8, Arrow::Field.new("int", DT.int32, true)}, {9_i8, Arrow::Field.new("str", DT.utf8, true)}])
      ids = Arrow::ScalarBuffer(Int8).from([3_i8, 9_i8, 3_i8, 9_i8])
      if mode.dense?
        arrays = [Arrow::PrimitiveArray(Int32).from_nullable([42, nil]).as(Arrow::ArrayBase), cast_strings(["hello", "world"] of String?)]
        offsets = Arrow::ScalarBuffer(Int32).from([0, 0, 1, 1])
      else
        arrays = [Arrow::PrimitiveArray(Int32).from_nullable([42, nil, nil, nil]).as(Arrow::ArrayBase), cast_strings([nil, "hello", nil, "world"] of String?)]
        offsets = nil
      end
      input = Arrow::UnionArray.try_new(fields, ids, offsets, arrays)
      assert_variant_cast(input, [Variant.from(42_i32), Variant.from("hello"), nil, Variant.from("world")] of Variant?)
    end
  end

  it "ignores unused union children and converts dictionaries with nested values" do
    fields = Arrow::UnionFields.new([{1_i8, Arrow::Field.new("used", DT.int32, true)}, {7_i8, Arrow::Field.new("unused", DT.duration(Arrow::TimeUnit::Second), true)}])
    input = Arrow::UnionArray.try_new(fields, Arrow::ScalarBuffer(Int8).from([1_i8]), Arrow::ScalarBuffer(Int32).from([0]),
      [Arrow::PrimitiveArray(Int32).from_values([42]).as(Arrow::ArrayBase), Arrow::PrimitiveArray(Int64).from_values([1_i64]).with_data_type(DT.duration(Arrow::TimeUnit::Second))])
    assert_variant_cast(input, [Variant.from(42_i32)] of Variant?)
    objects = PVC::StructArrayBuilder.new.with_field("id", Arrow::PrimitiveArray(Int32).from_values([1, 2]), false).with_field("name", cast_strings(["Alice", "Bob"] of String?), false).build
    input = Arrow::DictionaryArray(Int32).try_new(Arrow::PrimitiveArray(Int32).from_values([1, 0, 1]), objects)
    assert_variant_cast(input, [cast_json(%({"id":2,"name":"Bob"})), cast_json(%({"id":1,"name":"Alice"})), cast_json(%({"id":2,"name":"Bob"}))] of Variant?)
  end
end

describe "cast_to_variant validation and nested failures" do
  it "retains failed safe conversions as explicit null object fields while omitting SQL null fields" do
    decimals = Arrow::PrimitiveArray(Int32).from_nullable([Int32::MIN, nil]).with_data_type(DT.decimal32(9, 3))
    input = PVC::StructArrayBuilder.new.with_field("decimal", decimals, true).build
    assert_safe_variant_cast(input, [cast_json(%({"decimal":null})), cast_json("{}")] of Variant?)
    expect_raises(Arrow::ArrowError, "Failed to convert value at index 0") { PVC.cast_to_variant(input) }
    unsupported = PVC::StructArrayBuilder.new.with_field("duration", Arrow.new_null_array(DT.duration(Arrow::TimeUnit::Second), 1), true).build
    expect_raises(ParquetVariant::InvalidArgumentError) { PVC.cast_to_variant(unsupported) }
  end
  it "rejects invalid time units before reading rows, in either cast mode" do
    [true, false].each do |safe|
      invalid32 = Arrow::PrimitiveArray(Int32).from_values([1]).with_data_type(DT.time32(Arrow::TimeUnit::Microsecond))
      invalid64 = Arrow::PrimitiveArray(Int64).from_values([1_i64]).with_data_type(DT.time64(Arrow::TimeUnit::Second))
      expect_raises(ParquetVariant::CastError, "Unsupported Time32 unit: Microsecond") { PVC.cast_to_variant_with_options(invalid32, Arrow::CastOptions.new(safe: safe)) }
      expect_raises(ParquetVariant::CastError, "Unsupported Time64 unit: Second") { PVC.cast_to_variant_with_options(invalid64, Arrow::CastOptions.new(safe: safe)) }
    end
  end
end
