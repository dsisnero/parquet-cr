require "../spec_helper"

# Ported from `parquet-variant-compute/src/unshred_variant.rs` and
# `VariantValueArrayBuilder` in `variant_array_builder.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Shred/unshred integration roundtrips also live in shred_variant_spec.cr.
private alias PVC = ParquetVariantCompute
private alias Variant = ParquetVariant::Variant

private EMPTY_METADATA = ParquetVariant::EMPTY_VARIANT_METADATA_BYTES

private def metadata_array(values : Array(Bytes)) : Arrow::ArrayBase
  builder = Arrow::BinaryViewBuilder.new
  values.each { |value| builder.append_value(value) }
  builder.finish
end

private def empty_metadata_array(len : Int32) : Arrow::ArrayBase
  metadata_array(Array(Bytes).new(len, EMPTY_METADATA))
end

# Upstream `VariantArray.perfectly_shredded`: every value lives in
# `typed_value` and the required `value` column is all-null.
private def perfectly_shredded(
  typed_value : Arrow::ArrayBase,
  nulls : Arrow::NullBuffer? = nil,
) : PVC::VariantArray
  PVC::VariantArray.from_parts(
    empty_metadata_array(typed_value.len),
    PVC::VariantArray.all_null_value_column(typed_value.len),
    typed_value,
    nulls,
  )
end

# Whether the `value` field is annotated nullable.
private def value_field_nullable?(array : PVC::VariantArray) : Bool
  array.inner.field_by_name("value").not_nil!.nullable?
end

# A shredded variant field struct (`STRUCT<value, typed_value>`).
private def shredded_field(
  typed_value : Arrow::ArrayBase,
  value : Arrow::ArrayBase? = nil,
) : Arrow::StructArray
  value ||= Arrow.new_null_array(Arrow::DataType.binary_view, typed_value.len)
  fields = [
    Arrow::Field.new("value", Arrow::DataType.binary_view, true),
    Arrow::Field.new("typed_value", typed_value.data_type, true),
  ]
  Arrow::StructArray.try_new(
    Arrow::Fields.new(fields), [value, typed_value] of Arrow::ArrayBase
  )
end

private def strings_view(values : Array(String)) : Arrow::ArrayBase
  builder = Arrow::StringViewBuilder.new
  values.each { |value| builder.append_value(value) }
  builder.finish
end

describe ParquetVariantCompute do
  it "unshreds a Utf8View typed_value (upstream test_unshred_utf8view_typed_value)" do
    array = perfectly_shredded(strings_view(["hello", "middle", "world"]))

    result = PVC.unshred_variant(array)

    result.len.should eq 3
    result.value(0).should eq Variant.from("hello")
    result.value(1).should eq Variant.from("middle")
    result.value(2).should eq Variant.from("world")
    result.typed_value_column.should be_nil
    value_field_nullable?(result).should be_false
  end

  it "unshreds a LargeUtf8 typed_value (upstream test_unshred_largeutf8_typed_value)" do
    builder = Arrow::GenericStringBuilder(Int64).new
    ["hello", "middle", "world"].each { |value| builder.append_value(value) }

    result = PVC.unshred_variant(perfectly_shredded(builder.finish))

    result.len.should eq 3
    result.value(1).should eq Variant.from("middle")
  end

  it "unshreds a Binary typed_value (upstream test_unshred_binary_typed_value)" do
    builder = Arrow::BinaryBuilder.new
    ["\x00\x01\x02", "\xff\xaa"].each { |value| builder.append_value(value.to_slice) }

    result = PVC.unshred_variant(perfectly_shredded(builder.finish))

    result.len.should eq 2
    result.value(0).should eq Variant.from("\x00\x01\x02".to_slice)
    result.value(1).should eq Variant.from("\xff\xaa".to_slice)
  end

  it "unshreds a LargeBinary typed_value (upstream test_unshred_largebinary_typed_value)" do
    builder = Arrow::GenericBinaryBuilder(Int64).new
    ["\xde\xad\xbe\xef"].each { |value| builder.append_value(value.to_slice) }

    result = PVC.unshred_variant(perfectly_shredded(builder.finish))

    result.len.should eq 1
    result.value(0).should eq Variant.from("\xde\xad\xbe\xef".to_slice)
  end

  it "re-annotates an already unshredded nullable value column (upstream test_unshred_already_unshredded_reannotates_nullable_value)" do
    builder = PVC::VariantArrayBuilder.new(2)
    builder.append_variant(Variant.from(42_i64))
    builder.append_null
    original = builder.build

    # Same data, but with the out-of-spec nullable `value` annotation.
    nullable_input = PVC::VariantArray.from_parts(
      original.metadata_column,
      original.value_column,
      nil,
      original.nulls,
    )
    value_field_nullable?(nullable_input).should be_true

    unshredded = PVC.unshred_variant(nullable_input)
    value_field_nullable?(unshredded).should be_false
    unshredded.should eq original
  end

  it "materializes Variant::Null for a missing top-level value (upstream test_unshred_missing_top_level_value_becomes_variant_null)" do
    typed_value = Arrow::PrimitiveArray(Int64).from_nullable([1_i64, nil, 3_i64])

    result = PVC.unshred_variant(perfectly_shredded(typed_value))

    value_field_nullable?(result).should be_false
    result.inner.null_count.should eq 0
    result.value(0).should eq Variant.from(1_i64)
    result.value(1).should eq Variant::Null.new
    result.value(2).should eq Variant.from(3_i64)
  end

  it "materializes Variant::Null for unmasked value nulls (upstream test_unshred_value_only_with_unmasked_nulls_materializes_variant_null)" do
    builder = PVC::VariantArrayBuilder.new(1)
    builder.append_variant(Variant.from(42_i64))
    single = builder.build
    metadata_bytes = single.metadata_column.as(Arrow::BinaryViewArray).value(0)
    value_bytes = single.value_column.as(Arrow::BinaryViewArray).value(0)

    metadata = metadata_array([metadata_bytes, metadata_bytes])
    value_builder = Arrow::BinaryViewBuilder.new
    value_builder.append_value(value_bytes)
    value_builder.append_null
    input = PVC::VariantArray.from_parts(metadata, value_builder.finish, nil, nil)

    result = PVC.unshred_variant(input)

    value_field_nullable?(result).should be_false
    result.inner.null_count.should eq 0
    result.value(0).should eq Variant.from(42_i64)
    result.value(1).should eq Variant::Null.new
  end

  it "raises on malformed metadata (upstream test_unshred_returns_err_on_malformed_metadata)" do
    array = PVC::VariantArray.from_parts(
      metadata_array([Bytes.empty]),
      PVC::VariantArray.all_null_value_column(1),
      strings_view(["hello"]),
      nil,
    )

    # Upstream only asserts that an error (not a panic) is returned.
    expect_raises(Parquet::ParquetError) { PVC.unshred_variant(array) }
  end

  it "unshreds a shredded object" do
    metadata_builder = ParquetVariant::VariantBuilder.new.with_field_names(["a"])
    metadata_object = metadata_builder.new_object
    metadata_object.insert("a", Variant.from(0_i64))
    metadata_object.finish
    object_metadata, _ = metadata_builder.finish
    typed_value = shredded_field(Arrow::PrimitiveArray(Int64).from_values([7_i64]))
    object = Arrow::StructArray.try_new(
      Arrow::Fields.new([Arrow::Field.new("a", typed_value.data_type, false)]),
      [typed_value] of Arrow::ArrayBase,
    )

    array = PVC::VariantArray.from_parts(
      metadata_array([object_metadata]),
      PVC::VariantArray.all_null_value_column(1),
      object,
      nil,
    )
    result = PVC.unshred_variant(array)

    result.len.should eq 1
    object_value = result.value(0).as_object.not_nil!
    object_value.get("a").should eq Variant.from(7_i64)
  end

  it "unshreds a partially shredded object" do
    # The row metadata must name both fields: "a" (shredded) and "b" (in value).
    variant_builder = ParquetVariant::VariantBuilder.new.with_field_names(["a", "b"])
    object_builder = variant_builder.new_object
    object_builder.insert("b", Variant.from(2_i64))
    object_builder.finish
    object_metadata, object_value = variant_builder.finish

    typed_value = shredded_field(Arrow::PrimitiveArray(Int64).from_values([1_i64]))
    object = Arrow::StructArray.try_new(
      Arrow::Fields.new([Arrow::Field.new("a", typed_value.data_type, false)]),
      [typed_value] of Arrow::ArrayBase,
    )

    array = PVC::VariantArray.from_parts(
      metadata_array([object_metadata]),
      metadata_array([object_value]),
      object,
      nil,
    )
    result = PVC.unshred_variant(array)

    object_result = result.value(0).as_object.not_nil!
    object_result.get("a").should eq Variant.from(1_i64)
    object_result.get("b").should eq Variant.from(2_i64)
  end

  it "unshreds a shredded list" do
    elements = shredded_field(Arrow::PrimitiveArray(Int64).from_values([1_i64, 2_i64]))
    item_field = Arrow::Field.new("item", elements.data_type, true)
    list = Arrow::ListArray.new(
      item_field,
      Arrow::OffsetBuffer.new(Arrow::ScalarBuffer(Int32).from([0, 2])),
      elements,
      nil,
    )

    array = PVC::VariantArray.from_parts(
      metadata_array(Array(Bytes).new(1, EMPTY_METADATA)),
      PVC::VariantArray.all_null_value_column(1),
      list,
      nil,
    )
    result = PVC.unshred_variant(array)

    list_result = result.value(0).as_list.not_nil!
    list_result.len.should eq 2
    list_result.get(0).should eq Variant.from(1_i64)
    list_result.get(1).should eq Variant.from(2_i64)
  end

  it "unshreds the remaining primitive typed_value types" do
    {
      Arrow::PrimitiveArray(Int8).from_values([1_i8])                  => Variant.from(1_i8),
      Arrow::PrimitiveArray(Int32).from_values([3_i32])                => Variant.from(3_i32),
      Arrow::PrimitiveArray(Float64).from_values([2.5_f64])            => Variant.from(2.5_f64),
      Arrow::BooleanArray.new(Arrow::BooleanBuffer.from_slice([true])) => Variant.from(true),
    }.each do |typed_value, expected|
      PVC.unshred_variant(perfectly_shredded(typed_value)).value(0).should eq expected
    end

    uuid = UUID.new(Bytes.new(16, 0_u8))
    uuid_typed = Arrow::FixedSizeBinaryArray.try_from_iter([uuid.bytes.to_slice])
    PVC.unshred_variant(perfectly_shredded(uuid_typed)).value(0).should eq Variant.from(uuid)

    date = Arrow::PrimitiveArray(Int32)
      .from_values([19_000])
      .with_data_type(Arrow::DataType.date32)
    PVC.unshred_variant(perfectly_shredded(date)).value(0)
      .should eq Variant.from_date(Time.utc(1970, 1, 1) + 19_000.days)

    time = Arrow::PrimitiveArray(Int64)
      .from_values([3_600_000_000_i64])
      .with_data_type(Arrow::DataType.time64(Arrow::TimeUnit::Microsecond))
    PVC.unshred_variant(perfectly_shredded(time)).value(0)
      .should eq Variant.from_time(Time::Span.new(hours: 1))

    timestamp = Arrow::PrimitiveArray(Int64)
      .from_values([1_600_000_000_000_000_i64])
      .with_data_type(Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond, "UTC"))
    PVC.unshred_variant(perfectly_shredded(timestamp)).value(0)
      .should eq Variant.from_datetime_utc(Time.unix(1_600_000_000))

    decimal = Arrow::PrimitiveArray(Int64)
      .from_values([123_45_i64])
      .with_data_type(Arrow::DataType.decimal64(18_u8, 2_i8))
    PVC.unshred_variant(perfectly_shredded(decimal)).value(0)
      .should eq Variant.from(ParquetVariant::VariantDecimal8.try_new(123_45, 2_u8))
  end

  it "rejects a shredded row with both value and typed_value set" do
    builder = PVC::VariantArrayBuilder.new(1)
    builder.append_variant(Variant.from(42_i64))
    single = builder.build
    metadata = single.metadata_column
    value = single.value_column

    input = PVC::VariantArray.from_parts(
      metadata, value, Arrow::PrimitiveArray(Int64).from_values([7_i64]), nil
    )

    error = expect_raises(Parquet::ArrowError) { PVC.unshred_variant(input) }
    error.message.not_nil!.should contain(
      "Invalid shredded variant: both value and typed_value are non-null"
    )
  end

  it "rejects a field that is both shredded and stored in value" do
    variant_builder = ParquetVariant::VariantBuilder.new.with_field_names(["a"])
    object_builder = variant_builder.new_object
    object_builder.insert("a", Variant.from(2_i64))
    object_builder.finish
    object_metadata, object_value = variant_builder.finish

    typed_value = shredded_field(Arrow::PrimitiveArray(Int64).from_values([1_i64]))
    object = Arrow::StructArray.try_new(
      Arrow::Fields.new([Arrow::Field.new("a", typed_value.data_type, false)]),
      [typed_value] of Arrow::ArrayBase,
    )

    input = PVC::VariantArray.from_parts(
      metadata_array([object_metadata]),
      metadata_array([object_value]),
      object,
      nil,
    )

    error = expect_raises(Parquet::ArrowError) { PVC.unshred_variant(input) }
    error.message.not_nil!.should contain "Field 'a' appears in both typed_value and value"
  end
end

describe ParquetVariantCompute::VariantValueArrayBuilder do
  it "builds a value-only array (upstream test_variant_value_array_builder_basic)" do
    builder = PVC::VariantValueArrayBuilder.new(10)
    builder.append_value(Variant.from(42_i32))
    builder.append_null
    builder.append_value(Variant.from("hello"))

    value_array = builder.build
    value_array.len.should eq 3
    value_array.nulls.not_nil!.null_count.should eq 1
  end

  it "copies and reshapes objects (upstream test_variant_value_array_builder_with_objects)" do
    builder = PVC::VariantArrayBuilder.new(3)
    builder.new_object.with_field("name", "Alice").with_field("age", 30_i32).finish
    builder.new_object
      .with_field("name", "Bob").with_field("age", 42_i32).with_field("city", "Wonderland")
      .finish
    builder.new_object.with_field("name", "Charlie").with_field("age", 1_i32).finish
    array = builder.build

    value_builder = PVC::VariantValueArrayBuilder.new(3)
    # Straight copy.
    value_builder.append_value(array.value(0))

    value = array.value(1)
    ext = value_builder.builder_ext(value.metadata)
    ext.new_object
      .with_field("name", value.as_object.not_nil!.get("name").not_nil!)
      .with_field("age", value.as_object.not_nil!.get("age").not_nil!)
      .finish

    value = array.value(2)
    ext = value_builder.builder_ext(value.metadata)
    ext.new_list.with_value(value).with_value(value).finish

    array2 = PVC::VariantArray.from_parts(
      array.metadata_column, value_builder.build, nil, nil
    )

    array2.len.should eq 3
    array.value(0).should eq array2.value(0)
    array2.value(1).as_object.not_nil!.get("name").should eq Variant.from("Bob")
    array2.value(1).as_object.not_nil!.get("age").should eq Variant.from(42_i32)

    list = array2.value(2).as_list.not_nil!
    array.value(2).should eq list.get(0)
    array.value(2).should eq list.get(1)
  end
end
