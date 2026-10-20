require "../spec_helper"

# Ported from `parquet-variant-compute/src/variant_to_arrow.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): the variant → Arrow row
# builders, primitive slice.
#
# Deferred (documented in `plans/parity.md`): the list/struct/map/union,
# dictionary/run-end-encoded, variant-path and binary-variant row builders
# (upstream's `strict_cast_allows_variant_null_for_list_and_struct_builders`
# test goes with them).
private alias PVC = ParquetVariantCompute
private alias V2A = ParquetVariantCompute::VariantToArrow
private alias Variant = ParquetVariant::Variant

private def safe : Arrow::CastOptions
  Arrow::CastOptions.default
end

private def strict : Arrow::CastOptions
  Arrow::CastOptions.new(safe: false)
end

# Appends `values` to `builder` and returns the finished array.
private def build_array(
  builder : PVC::VariantToArrow::RowBuilder,
  values : Array(Variant),
) : Arrow::ArrayBase
  values.each { |value| builder.append_value(value) }
  builder.finish
end

describe ParquetVariantCompute::VariantToArrow do
  it "rejects non-primitive target types (upstream make_primitive_builder_rejects_non_primitive_types)" do
    item_field = Arrow::Field.new("item", Arrow::DataType.int32, true)
    struct_fields = Arrow::Fields.new([Arrow::Field.new("child", Arrow::DataType.int32, true)])
    map_entries = Arrow::Field.new(
      Arrow::Field::MAP_ENTRIES_FIELD_DEFAULT_NAME,
      Arrow::DataType.struct(Arrow::Fields.new([
        Arrow::Field.new(Arrow::Field::MAP_KEY_FIELD_DEFAULT_NAME, Arrow::DataType.utf8, false),
        Arrow::Field.new(Arrow::Field::MAP_VALUE_FIELD_DEFAULT_NAME, Arrow::DataType.float64, true),
      ])),
      true,
    )
    union_fields = Arrow::UnionFields.new([
      {1_i8, Arrow::Field.new("child", Arrow::DataType.int32, true)},
    ] of Tuple(Int8, Arrow::Field))
    run_ends = Arrow::Field.new("run_ends", Arrow::DataType.int32, false)
    ree_values = Arrow::Field.new("values", Arrow::DataType.utf8, true)

    [
      Arrow::DataType.list(item_field),
      Arrow::DataType.large_list(item_field),
      Arrow::DataType.list_view(item_field),
      Arrow::DataType.large_list_view(item_field),
      Arrow::DataType.fixed_size_list(item_field, 2),
      Arrow::DataType.struct(struct_fields),
      Arrow::DataType.map(map_entries, false),
      Arrow::DataType.union(union_fields, Arrow::UnionMode::Dense),
      Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8),
      Arrow::DataType.run_end_encoded(run_ends, ree_values),
    ].each do |data_type|
      error = expect_raises(ParquetVariant::InvalidArgumentError) do
        V2A.make_primitive_row_builder(data_type, safe, 1, false)
      end
      error.message.not_nil!.should contain data_type.to_s
    end
  end

  it "returns a null for Variant::Null in strict mode (upstream strict_cast_allows_variant_null_for_primitive_builder)" do
    builder = V2A.make_primitive_row_builder(Arrow::DataType.int32, strict, 2, false)

    builder.append_value(Variant::Null.new).should be_false
    builder.append_value(Variant.from(42_i32)).should be_true

    array = builder.finish.as(Arrow::PrimitiveArray(Int32))
    array.len.should eq 2
    array.nulls.not_nil!.null?(0).should be_true
    array.value(1).should eq 42
  end

  it "returns a null for Variant::Null in strict mode for decimals (upstream strict_cast_allows_variant_null_for_decimal_builder)" do
    builder = V2A.make_primitive_row_builder(
      Arrow::DataType.decimal32(9, 2), strict, 2, false
    )
    decimal = Variant.from(ParquetVariant::VariantDecimal4.try_new(1234, 2))

    builder.append_value(Variant::Null.new).should be_false
    builder.append_value(decimal).should be_true

    array = builder.finish.as(Arrow::PrimitiveArray(Int32))
    array.data_type.should eq Arrow::DataType.decimal32(9, 2)
    array.nulls.not_nil!.null?(0).should be_true
    array.value(1).should eq 1234
  end

  it "returns a null for Variant::Null in strict mode for UUIDs (upstream strict_cast_allows_variant_null_for_uuid_builder)" do
    builder = V2A.make_primitive_row_builder(
      Arrow::DataType.fixed_size_binary(16), strict, 2, false
    )
    uuid = UUID.new(Bytes.new(16, 0_u8))

    builder.append_value(Variant::Null.new).should be_false
    builder.append_value(Variant.from(uuid)).should be_true

    array = builder.finish.as(Arrow::FixedSizeBinaryArray)
    array.nulls.not_nil!.null?(0).should be_true
    array.value(1).should eq uuid.bytes.to_slice
  end

  it "builds numeric, boolean, string and binary arrays" do
    ints = build_array(
      V2A.make_primitive_row_builder(Arrow::DataType.int64, safe, 3, false),
      [Variant.from(1_i64), Variant.from(2_i8), Variant::Null.new],
    ).as(Arrow::PrimitiveArray(Int64))
    ints.len.should eq 3
    ints.value(0).should eq 1
    ints.value(1).should eq 2
    ints.nulls.not_nil!.null?(2).should be_true

    bools = build_array(
      V2A.make_primitive_row_builder(Arrow::DataType.boolean, safe, 2, false),
      [Variant.from(true), Variant.from(0_i32)],
    ).as(Arrow::BooleanArray)
    bools.value(0).should be_true
    bools.value(1).should be_false

    strings = build_array(
      V2A.make_primitive_row_builder(Arrow::DataType.utf8, safe, 2, false),
      [Variant.from("hello"), Variant.from("world")],
    ).as(Arrow::StringArray)
    strings.value(0).should eq "hello"
    strings.value(1).should eq "world"

    views = build_array(
      V2A.make_primitive_row_builder(Arrow::DataType.utf8_view, safe, 1, false),
      [Variant.from("view")],
    ).as(Arrow::StringViewArray)
    views.value(0).should eq "view"

    binary = build_array(
      V2A.make_primitive_row_builder(Arrow::DataType.binary, safe, 1, false),
      [Variant.from(Bytes[1_u8, 2_u8])],
    ).as(Arrow::BinaryArray)
    binary.value(0).should eq Bytes[1_u8, 2_u8]

    large_binary = build_array(
      V2A.make_primitive_row_builder(Arrow::DataType.large_binary, safe, 1, false),
      [Variant.from(Bytes[3_u8])],
    ).as(Arrow::GenericBinaryArray(Int64))
    large_binary.value(0).should eq Bytes[3_u8]
  end

  it "builds temporal arrays" do
    date = build_array(
      V2A.make_primitive_row_builder(Arrow::DataType.date32, safe, 1, false),
      [Variant.from_date(Time.utc(2022, 1, 1))],
    ).as(Arrow::PrimitiveArray(Int32))
    date.value(0).should eq 18_993

    time = build_array(
      V2A.make_primitive_row_builder(
        Arrow::DataType.time64(Arrow::TimeUnit::Microsecond), safe, 1, false
      ),
      [Variant.from_time(Time::Span.new(hours: 1))],
    ).as(Arrow::PrimitiveArray(Int64))
    time.value(0).should eq 3_600_000_000_i64

    timestamp = build_array(
      V2A.make_primitive_row_builder(
        Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond, "UTC"), safe, 1, false
      ),
      [Variant.from_datetime_utc(Time.unix(1_600_000_000))],
    ).as(Arrow::PrimitiveArray(Int64))
    timestamp.data_type.should eq Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond, "UTC")
    timestamp.value(0).should eq 1_600_000_000_000_000_i64

    naive = build_array(
      V2A.make_primitive_row_builder(
        Arrow::DataType.timestamp(Arrow::TimeUnit::Microsecond), safe, 1, false
      ),
      [Variant.from_naive_datetime(Time.unix(1_600_000_000))],
    ).as(Arrow::PrimitiveArray(Int64))
    naive.value(0).should eq 1_600_000_000_000_000_i64
  end

  it "nulls failed casts in safe mode and raises in strict mode" do
    safe_builder = V2A.make_primitive_row_builder(Arrow::DataType.int32, safe, 1, false)
    safe_builder.append_value(Variant.from("not a number")).should be_false
    safe_builder.finish.as(Arrow::PrimitiveArray(Int32)).nulls.not_nil!.null?(0).should be_true

    strict_builder = V2A.make_primitive_row_builder(Arrow::DataType.int32, strict, 1, false)
    error = expect_raises(ParquetVariant::CastError) do
      strict_builder.append_value(Variant.from("not a number"))
    end
    # Short strings are stored as a `ShortString` variant.
    error.message.not_nil!.should contain(
      "Failed to extract primitive of type Int32 from variant ShortString("
    )
    error.message.not_nil!.should contain "not a number"
  end

  it "builds a NullArray for the Null target type" do
    builder = V2A.make_primitive_row_builder(Arrow::DataType.null, safe, 2, false)
    builder.append_value(Variant::Null.new).should be_true
    builder.append_value(Variant.from(5_i32)).should be_false

    array = builder.finish
    array.as(Arrow::NullArray).len.should eq 2
  end

  it "reports invalid time units and unsupported targets" do
    [
      Arrow::DataType.time32(Arrow::TimeUnit::Microsecond),
      Arrow::DataType.time64(Arrow::TimeUnit::Second),
      Arrow::DataType.duration(Arrow::TimeUnit::Second),
      Arrow::DataType.interval(Arrow::IntervalUnit::YearMonth),
    ].each do |data_type|
      expect_raises(ParquetVariant::InvalidArgumentError) do
        V2A.make_primitive_row_builder(data_type, safe, 1, false)
      end
    end

    # A non-16-byte FixedSizeBinary target is not yet implemented upstream.
    expect_raises(Parquet::NYIError) do
      V2A.make_primitive_row_builder(Arrow::DataType.fixed_size_binary(8), safe, 1, false)
    end
  end

  it "dispatches shared primitive types through make_typed_row_builder" do
    builder = V2A.make_typed_row_builder(Arrow::DataType.int32, safe, 2, false)
    builder.append_value(Variant.from(7_i32)).should be_true
    builder.append_null
    array = builder.finish.as(Arrow::PrimitiveArray(Int32))
    array.len.should eq 2
    array.value(0).should eq 7
    array.nulls.not_nil!.null?(1).should be_true
  end
end

# Composite (list and struct) row builders. Upstream
# `strict_cast_allows_variant_null_for_list_and_struct_builders` and the
# equivalent characterization of the list/struct shapes.
private def list_variant(values : Array(Variant)) : Variant
  builder = ParquetVariant::VariantBuilder.new
  list = builder.new_list
  values.each { |value| list.append_value(value) }
  list.finish
  metadata, value = builder.finish
  Variant.new(metadata, value)
end

private def object_variant(fields : Array(Tuple(String, Variant))) : Variant
  builder = ParquetVariant::VariantBuilder.new
  object = builder.new_object
  fields.each { |(name, value)| object.insert(name, value) }
  object.finish
  metadata, value = builder.finish
  Variant.new(metadata, value)
end

private def int64_item : Arrow::Field
  Arrow::Field.new("item", Arrow::DataType.int64, true)
end

describe ParquetVariantCompute::VariantToArrow do
  it "returns a null for Variant::Null in strict mode for list and struct builders (upstream strict_cast_allows_variant_null_for_list_and_struct_builders)" do
    list_builder = V2A.make_typed_row_builder(
      Arrow::DataType.list(int64_item), strict, 1, false
    )
    list_builder.append_value(Variant::Null.new).should be_false
    list_array = list_builder.finish.as(Arrow::ListArray)
    list_array.nulls.not_nil!.null?(0).should be_true

    struct_builder = V2A.make_typed_row_builder(
      Arrow::DataType.struct(Arrow::Fields.new([
        Arrow::Field.new("a", Arrow::DataType.int32, true),
      ])), strict, 1, false
    )
    struct_builder.append_value(Variant::Null.new).should be_false
    struct_array = struct_builder.finish.as(Arrow::StructArray)
    struct_array.nulls.not_nil!.null?(0).should be_true
  end

  it "builds list arrays" do
    builder = V2A.make_typed_row_builder(Arrow::DataType.list(int64_item), safe, 3, false)
    builder.append_value(list_variant([Variant.from(1_i64), Variant.from(2_i64)])).should be_true
    builder.append_value(Variant::Null.new).should be_false
    builder.append_value(list_variant([] of Variant)).should be_true

    array = builder.finish.as(Arrow::ListArray)
    array.len.should eq 3
    array.value_length(0).should eq 2
    array.value(0).as(Arrow::PrimitiveArray(Int64)).value(1).should eq 2
    array.nulls.not_nil!.null?(1).should be_true
    array.value_length(2).should eq 0
  end

  it "builds large list, list view and large list view arrays" do
    values = [Variant.from(1_i64), Variant.from(2_i64)]

    large = V2A.make_typed_row_builder(Arrow::DataType.large_list(int64_item), safe, 1, false)
    large.append_value(list_variant(values))
    large_array = large.finish.as(Arrow::LargeListArray)
    large_array.value_length(0).should eq 2
    large_array.data_type.kind.large_list?.should be_true

    list_view = V2A.make_typed_row_builder(Arrow::DataType.list_view(int64_item), safe, 1, false)
    list_view.append_value(list_variant(values))
    view_array = list_view.finish.as(Arrow::ListViewArray)
    view_array.offsets[0].should eq 0
    view_array.sizes[0].should eq 2

    large_view = V2A.make_typed_row_builder(
      Arrow::DataType.large_list_view(int64_item), safe, 1, false
    )
    large_view.append_value(list_variant(values))
    large_view_array = large_view.finish.as(Arrow::LargeListViewArray)
    large_view_array.sizes[0].should eq 2
  end

  it "builds fixed size list arrays and rejects the wrong size" do
    data_type = Arrow::DataType.fixed_size_list(int64_item, 2)
    builder = V2A.make_typed_row_builder(data_type, safe, 2, false)
    builder.append_value(list_variant([Variant.from(1_i64), Variant.from(2_i64)]))
    builder.append_null

    array = builder.finish.as(Arrow::FixedSizeListArray)
    array.len.should eq 2
    array.value_length.should eq 2
    array.value(0).as(Arrow::PrimitiveArray(Int64)).value(1).should eq 2
    array.nulls.not_nil!.null?(1).should be_true

    # A wrong size is a null in safe mode and an error in strict mode.
    safe_builder = V2A.make_typed_row_builder(data_type, safe, 1, false)
    safe_builder.append_value(list_variant([Variant.from(1_i64)])).should be_false
    safe_builder.finish.as(Arrow::FixedSizeListArray).nulls.not_nil!.null?(0).should be_true

    strict_builder = V2A.make_typed_row_builder(data_type, strict, 1, false)
    error = expect_raises(ParquetVariant::CastError) do
      strict_builder.append_value(list_variant([Variant.from(1_i64)]))
    end
    error.message.not_nil!.should contain "Expected fixed size list of size 2, got size 1"
  end

  it "builds struct arrays and nulls missing fields" do
    fields = Arrow::Fields.new([
      Arrow::Field.new("a", Arrow::DataType.int32, true),
      Arrow::Field.new("b", Arrow::DataType.utf8, true),
    ])
    builder = V2A.make_typed_row_builder(Arrow::DataType.struct(fields), safe, 3, false)
    builder.append_value(
      object_variant([{"a", Variant.from(1_i32)}, {"b", Variant.from("x")}])
    ).should be_true
    builder.append_value(object_variant([{"a", Variant.from(2_i32)}])).should be_true
    builder.append_null

    array = builder.finish.as(Arrow::StructArray)
    array.len.should eq 3
    array.column_by_name("a").not_nil!.as(Arrow::PrimitiveArray(Int32)).value(0).should eq 1
    array.column_by_name("b").not_nil!.as(Arrow::StringArray).value(0).should eq "x"
    # The missing field is null.
    array.column_by_name("b").not_nil!.nulls.not_nil!.null?(1).should be_true
    array.nulls.not_nil!.null?(2).should be_true
  end

  it "nulls non-list and non-struct values in safe mode and raises in strict mode" do
    safe_list = V2A.make_typed_row_builder(Arrow::DataType.list(int64_item), safe, 1, false)
    safe_list.append_value(Variant.from(5_i64)).should be_false
    safe_list.finish.as(Arrow::ListArray).nulls.not_nil!.null?(0).should be_true

    strict_list = V2A.make_typed_row_builder(Arrow::DataType.list(int64_item), strict, 1, false)
    error = expect_raises(ParquetVariant::CastError) do
      strict_list.append_value(Variant.from(5_i64))
    end
    error.message.not_nil!.should contain "Failed to extract list from variant"

    struct_type = Arrow::DataType.struct(
      Arrow::Fields.new([Arrow::Field.new("a", Arrow::DataType.int32, true)])
    )
    strict_struct = V2A.make_typed_row_builder(struct_type, strict, 1, false)
    error = expect_raises(ParquetVariant::CastError) do
      strict_struct.append_value(Variant.from(5_i64))
    end
    error.message.not_nil!.should contain "Failed to extract struct from variant"
  end

  it "keeps map, union and encoded targets deferred" do
    union_fields = Arrow::UnionFields.new([
      {1_i8, Arrow::Field.new("child", Arrow::DataType.int32, true)},
    ] of Tuple(Int8, Arrow::Field))
    [
      Arrow::DataType.dictionary(Arrow::DataType.int32, Arrow::DataType.utf8),
      Arrow::DataType.union(union_fields, Arrow::UnionMode::Dense),
    ].each do |data_type|
      expect_raises(Parquet::NYIError) do
        V2A.make_typed_row_builder(data_type, safe, 1, false)
      end
    end
  end
end
