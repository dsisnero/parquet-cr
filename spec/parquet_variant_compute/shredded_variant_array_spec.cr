require "../spec_helper"

# Ported from `parquet-variant-compute/src/variant_array.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): shredded variant arrays —
# `typed_value` interpretation, the lenient read of a missing `value` column,
# the shredding-type canonicalization (decimal narrowing and UUID tagging) and
# `ShreddedVariantFieldArray`.
#
# The only upstream test left with the shredding kernels is
# `try_value_errors_on_unimplemented_typed_value_type`, which needs
# `shred_variant` (see `plans/parity.md`).
private alias PVC = ParquetVariantCompute
private alias Variant = ParquetVariant::Variant

private def empty_metadata_array(len : Int32) : Arrow::ArrayBase
  builder = Arrow::BinaryViewBuilder.new
  len.times { builder.append_value(ParquetVariant::EMPTY_VARIANT_METADATA_BYTES) }
  builder.finish
end

# Upstream `make_variant_struct_with_typed_value`: a shredded variant struct
# with an all-null `value` column.
private def variant_struct_with_typed_value(typed_value : Arrow::ArrayBase) : Arrow::StructArray
  fields = Arrow::Fields.new([
    Arrow::Field.new("metadata", Arrow::DataType.binary_view, false),
    Arrow::Field.new("value", Arrow::DataType.binary_view, true),
    Arrow::Field.new("typed_value", typed_value.data_type, true),
  ])
  value = Arrow.new_null_array(Arrow::DataType.binary_view, typed_value.len)
  Arrow::StructArray.try_new(
    fields,
    [empty_metadata_array(typed_value.len), value, typed_value] of Arrow::ArrayBase,
  )
end

# Builds a struct array from `(field name, column)` pairs.
private def struct_array(columns : Array(Tuple(String, Arrow::ArrayBase)),
                         nullable : Bool = true) : Arrow::StructArray
  fields = Arrow::Fields.new(
    columns.map { |(name, array)| Arrow::Field.new(name, array.data_type, nullable) }
  )
  Arrow::StructArray.try_new(fields, columns.map { |(_, array)| array })
end

private def int32_offsets(values : Array(Int32)) : Arrow::OffsetBuffer(Int32)
  Arrow::OffsetBuffer.new(Arrow::ScalarBuffer(Int32).from(values))
end

private def int64_offsets(values : Array(Int64)) : Arrow::OffsetBuffer(Int64)
  Arrow::OffsetBuffer.new(Arrow::ScalarBuffer(Int64).from(values))
end

# Reads the value at row 0 of a shredded single-row array.
private def shredded_value(typed_value : Arrow::ArrayBase) : Variant
  PVC::VariantArray.try_new(variant_struct_with_typed_value(typed_value)).value(0)
end

describe ParquetVariantCompute::VariantArray do
  it "synthesizes an all-null value column for a shredded array (upstream read_missing_value_column)" do
    typed_value = Arrow::PrimitiveArray(Int64).from_nullable([1_i64, nil, 3_i64])
    fields = Arrow::Fields.new([
      Arrow::Field.new("metadata", Arrow::DataType.binary_view, false),
      Arrow::Field.new("typed_value", Arrow::DataType.int64, true),
    ])
    input = Arrow::StructArray.try_new(
      fields, [empty_metadata_array(3), typed_value] of Arrow::ArrayBase
    )
    input.column_by_name("value").should be_nil

    variant_array = PVC::VariantArray.try_new(input)
    variant_array.value_column.len.should eq 3
    variant_array.value_column.nulls.not_nil!.null_count.should eq 3
    variant_array.inner.column_by_name("value").should_not be_nil
    variant_array.typed_value_column.should_not be_nil

    variant_array.value(0).should eq(Variant.from(1_i64))
    variant_array.value(1).should eq(Variant::Null.new)
    variant_array.value(2).should eq(Variant.from(3_i64))

    # With no typed_value either, there is nothing to read, so `value` stays
    # required.
    metadata_only = Arrow::StructArray.try_new(
      Arrow::Fields.new([Arrow::Field.new("metadata", Arrow::DataType.binary_view, false)]),
      [empty_metadata_array(1)] of Arrow::ArrayBase,
    )
    error = expect_raises(Parquet::ArrowError) { PVC::VariantArray.try_new(metadata_only) }
    error.message.not_nil!.should contain "StructArray must contain a 'value' field"
  end

  it "tags an untagged UUID typed_value on read (upstream try_new_tags_untagged_uuid_on_read)" do
    typed_value = Arrow::FixedSizeBinaryArray.try_from_iter([Bytes.new(16, 0_u8), Bytes.new(16, 0_u8)])
    variant_array = PVC::VariantArray.try_new(variant_struct_with_typed_value(typed_value))

    field = variant_array.inner.field_by_name("typed_value").not_nil!
    field.data_type.should eq Arrow::DataType.fixed_size_binary(16)
    field.has_valid_extension_type?(Arrow::Extension::Canonical::Uuid).should be_true
  end

  it "tags a nested untagged UUID typed_value on read (upstream try_new_tags_untagged_nested_uuid_on_read)" do
    leaf = Arrow::FixedSizeBinaryArray.try_from_iter([Bytes.new(16, 0_u8)])
    inner = struct_array([{"typed_value", leaf.as(Arrow::ArrayBase)}] of Tuple(String, Arrow::ArrayBase), nullable: true)
    object = struct_array([{"id", inner.as(Arrow::ArrayBase)}] of Tuple(String, Arrow::ArrayBase), nullable: false)
    variant_array = PVC::VariantArray.try_new(variant_struct_with_typed_value(object))

    typed_value = variant_array.typed_value_column.not_nil!.as(Arrow::StructArray)
    id = typed_value.column_by_name("id").not_nil!.as(Arrow::StructArray)
    uuid_leaf = id.field_by_name("typed_value").not_nil!
    uuid_leaf.has_valid_extension_type?(Arrow::Extension::Canonical::Uuid).should be_true
  end

  it "accepts list-like shredding types unchanged (upstream canonicalize_and_verify_list_like_data_types)" do
    item_binary = Arrow::Field.new("item", Arrow::DataType.binary, true)
    item_large_binary = Arrow::Field.new("item", Arrow::DataType.large_binary, true)
    item_binary_view = Arrow::Field.new("item", Arrow::DataType.binary_view, true)

    [
      Arrow::DataType.large_list(item_binary),
      Arrow::DataType.list_view(item_binary),
      Arrow::DataType.large_list_view(item_binary),
      Arrow::DataType.large_list(item_large_binary),
      Arrow::DataType.list_view(item_large_binary),
      Arrow::DataType.large_list_view(item_large_binary),
      Arrow::DataType.large_list(item_binary_view),
      Arrow::DataType.list_view(item_binary_view),
      Arrow::DataType.large_list_view(item_binary_view),
    ].each do |input|
      PVC::VariantArray.canonicalize_and_verify_data_type(input).should eq input
    end
  end

  it "supports list-like typed_value columns (upstream variant_array_try_new_supports_list_like_typed_value)" do
    item_field = Arrow::Field.new("item", Arrow::DataType.int64, true)
    values = Arrow::PrimitiveArray(Int64).from_nullable([1_i64, nil, 3_i64])

    typed_values = [
      Arrow::ListArray.new(item_field, int32_offsets([0, 2, 3]), values, nil).as(Arrow::ArrayBase),
      Arrow::LargeListArray.new(item_field, int64_offsets([0_i64, 2_i64, 3_i64]), values, nil).as(Arrow::ArrayBase),
      Arrow::ListViewArray.new(
        item_field, Arrow::ScalarBuffer(Int32).from([0, 2]),
        Arrow::ScalarBuffer(Int32).from([2, 1]), values, nil
      ).as(Arrow::ArrayBase),
      Arrow::LargeListViewArray.new(
        item_field, Arrow::ScalarBuffer(Int64).from([0_i64, 2_i64]),
        Arrow::ScalarBuffer(Int64).from([2_i64, 1_i64]), values, nil
      ).as(Arrow::ArrayBase),
    ]

    typed_values.each do |typed_value|
      input = variant_struct_with_typed_value(typed_value)
      variant_array = PVC::VariantArray.try_new(input)
      variant_array.typed_value_column.not_nil!.data_type.should eq typed_value.data_type
    end
  end

  it "reads a Binary typed_value (upstream binary_typed_value_roundtrips)" do
    builder = Arrow::BinaryBuilder.new
    builder.append_value("hello".to_slice)
    shredded_value(builder.finish).should eq(Variant.from("hello".to_slice))
  end

  it "reads a LargeBinary typed_value (upstream large_binary_typed_value_roundtrips)" do
    builder = Arrow::GenericBinaryBuilder(Int64).new
    builder.append_value("world".to_slice)
    shredded_value(builder.finish).should eq(Variant.from("world".to_slice))
  end

  it "reports an invalid Time64 typed_value (upstream test_variant_array_invalid_time)" do
    time = Arrow::PrimitiveArray(Int64)
      .from_values([86_401_000_000_i64])
      .with_data_type(Arrow::DataType.time64(Arrow::TimeUnit::Microsecond))

    variant_array = PVC::VariantArray.try_new(variant_struct_with_typed_value(time))
    error = expect_raises(ParquetVariant::CastError) { variant_array.try_value(0) }
    error.message.not_nil!.should contain(
      "Cast error: Cast failed at index 0 (array type: Time64(µs)): " \
      "Invalid microsecond from midnight: 86401000000"
    )
  end

  it "reports an invalid Decimal32 typed_value (upstream test_variant_array_invalid_decimal32)" do
    typed_value = Arrow::PrimitiveArray(Int32)
      .from_values([1_234_567_890])
      .with_data_type(Arrow::DataType.decimal32(9_u8, 2_i8))

    variant_array = PVC::VariantArray.try_new(variant_struct_with_typed_value(typed_value))
    error = expect_raises(ParquetVariant::CastError) { variant_array.try_value(0) }
    error.message.not_nil!.should contain(
      "Cast error: Cast failed at index 0 (array type: Decimal32(9, 2)): " \
      "Invalid argument error: 1234567890 is wider than max precision 9"
    )
  end

  it "reports an invalid Decimal64 typed_value (upstream test_variant_array_invalid_decimal64)" do
    typed_value = Arrow::PrimitiveArray(Int64)
      .from_values([1_234_567_890_123_456_789_i64])
      .with_data_type(Arrow::DataType.decimal64(18_u8, 6_i8))

    variant_array = PVC::VariantArray.try_new(variant_struct_with_typed_value(typed_value))
    error = expect_raises(ParquetVariant::CastError) { variant_array.try_value(0) }
    error.message.not_nil!.should contain(
      "Cast error: Cast failed at index 0 (array type: Decimal64(18, 6)): " \
      "Invalid argument error: 1234567890123456789 is wider than max precision 18"
    )
  end

  it "reports an invalid Decimal128 typed_value (upstream test_variant_array_invalid_decimal128)" do
    typed_value = Arrow::PrimitiveArray(Int128)
      .from_values([Int128.new("123456789012345678901234567890123456789")])
      .with_data_type(Arrow::DataType.decimal128(38_u8, 10_i8))

    variant_array = PVC::VariantArray.try_new(variant_struct_with_typed_value(typed_value))
    error = expect_raises(ParquetVariant::CastError) { variant_array.try_value(0) }
    error.message.not_nil!.should contain(
      "Cast error: Cast failed at index 0 (array type: Decimal128(38, 10)): " \
      "Invalid argument error: 123456789012345678901234567890123456789 is wider than max " \
      "precision 38"
    )
  end

  # Characterization: the typed_value types upstream supports but does not unit
  # test individually.
  it "reads primitive, string and binary typed_values" do
    shredded_value(Arrow::BooleanArray.new(Arrow::BooleanBuffer.from_slice([true])))
      .should eq(Variant.from(true))
    shredded_value(Arrow::PrimitiveArray(Int8).from_values([1_i8])).should eq(Variant.from(1_i8))
    shredded_value(Arrow::PrimitiveArray(Int16).from_values([2_i16])).should eq(Variant.from(2_i16))
    shredded_value(Arrow::PrimitiveArray(Int32).from_values([3_i32])).should eq(Variant.from(3_i32))
    shredded_value(Arrow::PrimitiveArray(Int64).from_values([4_i64])).should eq(Variant.from(4_i64))
    shredded_value(Arrow::PrimitiveArray(Float32).from_values([1.5_f32])).should eq(Variant.from(1.5_f32))
    shredded_value(Arrow::PrimitiveArray(Float64).from_values([2.5_f64])).should eq(Variant.from(2.5_f64))

    strings = Arrow::StringBuilder.new
    strings.append_value("utf8")
    shredded_value(strings.finish).should eq(Variant.from("utf8"))

    large = Arrow::GenericStringBuilder(Int64).new
    large.append_value("large utf8")
    shredded_value(large.finish).should eq(Variant.from("large utf8"))

    views = Arrow::StringViewBuilder.new
    views.append_value("utf8 view")
    shredded_value(views.finish).should eq(Variant.from("utf8 view"))

    binary_views = Arrow::BinaryViewBuilder.new
    binary_views.append_value("bin view".to_slice)
    shredded_value(binary_views.finish).should eq(Variant.from("bin view".to_slice))

    shredded_value(Arrow.new_null_array(Arrow::DataType.null, 1)).should eq(Variant::Null.new)
  end

  it "reads a UUID typed_value" do
    uuid = UUID.new(Bytes.new(16, 0_u8))
    typed_value = Arrow::FixedSizeBinaryArray.try_from_iter([uuid.bytes.to_slice])
    shredded_value(typed_value).should eq(Variant.from(uuid))
  end

  it "reads decimal typed_values, narrowing wide decimals first" do
    decimal32 = Arrow::PrimitiveArray(Int32)
      .from_values([123_45])
      .with_data_type(Arrow::DataType.decimal32(5_u8, 2_i8))
    shredded_value(decimal32)
      .should eq(Variant.from(ParquetVariant::VariantDecimal4.try_new(123_45, 2_u8)))

    # A Decimal128 with a 32-bit precision is canonicalized to Decimal32 on read.
    wide = Arrow::PrimitiveArray(Int128)
      .from_values([123_45_i128])
      .with_data_type(Arrow::DataType.decimal128(5_u8, 2_i8))
    variant_array = PVC::VariantArray.try_new(variant_struct_with_typed_value(wide))
    variant_array.typed_value_column.not_nil!.data_type
      .should eq Arrow::DataType.decimal32(5_u8, 2_i8)
    variant_array.value(0)
      .should eq(Variant.from(ParquetVariant::VariantDecimal4.try_new(123_45, 2_u8)))
  end

  it "reads temporal typed_values" do
    date = Arrow::PrimitiveArray(Int32)
      .from_values([19_000])
      .with_data_type(Arrow::DataType.date32)
    shredded_value(date).should eq(Variant.from_date(Time.utc(1970, 1, 1) + 19_000.days))

    time = Arrow::PrimitiveArray(Int64)
      .from_values([3_600_000_000_i64])
      .with_data_type(Arrow::DataType.time64(Arrow::TimeUnit::Microsecond))
    shredded_value(time).should eq(Variant.from_time(Time::Span.new(hours: 1)))

    timestamp_utc = Arrow::PrimitiveArray(Int64)
      .from_values([1_600_000_000_000_000_i64])
      .with_data_type(Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond, "UTC"))
    shredded_value(timestamp_utc)
      .should eq(Variant.from_datetime_utc(Time.unix(1_600_000_000)))

    timestamp_naive = Arrow::PrimitiveArray(Int64)
      .from_values([1_600_000_000_000_000_i64])
      .with_data_type(Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond, nil))
    shredded_value(timestamp_naive)
      .should eq(Variant.from_naive_datetime(Time.unix(1_600_000_000)))

    timestamp_nanos = Arrow::PrimitiveArray(Int64)
      .from_values([1_600_000_000_123_456_789_i64])
      .with_data_type(Arrow::DataType.timestamp(Arrow::TimeUnit::Nanosecond, "UTC"))
    shredded_value(timestamp_nanos)
      .should eq(Variant.from_datetime_utc(Time.unix_ns(1_600_000_000_123_456_789_i64)))
  end

  it "rejects composite typed_values that would need unshredding" do
    object = struct_array([{"a", Arrow::PrimitiveArray(Int32).from_values([1]).as(Arrow::ArrayBase)}] of Tuple(String, Arrow::ArrayBase))
    variant_array = PVC::VariantArray.try_new(variant_struct_with_typed_value(object))

    error = expect_raises(Parquet::NYIError) { variant_array.try_value(0) }
    error.message.not_nil!.should contain(
      "VariantArray::try_value cannot materialize typed_value of type Struct"
    )
  end
end

describe ParquetVariantCompute::ShreddedVariantFieldArray do
  it "wraps a shredded field struct" do
    typed_value = Arrow::PrimitiveArray(Int64).from_values([7_i64])
    fields = Arrow::Fields.new([
      Arrow::Field.new("value", Arrow::DataType.binary_view, true),
      Arrow::Field.new("typed_value", Arrow::DataType.int64, true),
    ])
    input = Arrow::StructArray.try_new(
      fields,
      [Arrow.new_null_array(Arrow::DataType.binary_view, 1), typed_value] of Arrow::ArrayBase,
    )

    field_array = PVC::ShreddedVariantFieldArray.try_new(input)
    field_array.len.should eq 1
    field_array.is_empty?.should be_false
    field_array.is_valid(0).should be_true
    # Shredded variant fields are physically non-nullable.
    field_array.nulls.should be_nil
    field_array.value_column.len.should eq 1
    field_array.typed_value_column.not_nil!.data_type.should eq Arrow::DataType.int64
    field_array.data_type.kind.struct?.should be_true
    field_array.inner.len.should eq 1
    field_array.into_inner.len.should eq 1
  end
end
