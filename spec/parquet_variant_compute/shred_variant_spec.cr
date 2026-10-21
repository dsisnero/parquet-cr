require "../spec_helper"

# Ported from parquet-variant-compute/src/shred_variant.rs, arrow-rs 60.0.0.
private alias PVC = ParquetVariantCompute
private alias Variant = ParquetVariant::Variant
private alias DT = Arrow::DataType

private def shred_input(rows : Array(String?)) : PVC::VariantArray
  builder = PVC::VariantArrayBuilder.new
  rows.each do |row|
    row ? builder.append_json(row) : builder.append_null
  end
  builder.build
end

private def assert_shred_roundtrip(input : PVC::VariantArray, result : PVC::VariantArray) : Nil
  unshredded = PVC.unshred_variant(result)
  unshredded.inner.field_by_name("value").not_nil!.nullable?.should be_false
  unshredded.len.should eq input.len
  input.len.times do |index|
    unshredded.is_null(index).should eq input.is_null(index)
    unless input.is_null(index)
      unshredded.value(index).to_json_value.should eq input.value(index).to_json_value
    end
  end
end

private def shredded_child(array : Arrow::ArrayBase, name : String) : PVC::ShreddedVariantFieldArray
  PVC::ShreddedVariantFieldArray.try_new(array.as(Arrow::StructArray).column_by_name(name).not_nil!)
end

private def fallback_value(metadata : Arrow::ArrayBase, value : Arrow::ArrayBase, index : Int32) : Variant?
  bytes = PVC::VariantArray.binary_array_value(value, index)
  return nil unless bytes
  Variant.new(PVC::VariantArray.binary_array_value(metadata, index).not_nil!, bytes)
end

private def array_null?(array : Arrow::ArrayBase, index : Int32) : Bool
  array.nulls.try(&.null?(index)) || false
end

describe ParquetVariantCompute do
  it "shreds primitives while preserving fallbacks, SQL nulls and variant nulls" do
    input = PVC::VariantArray.from_iter([
      Variant.from(42_i64), Variant.from("hello"), Variant.from(100_i64),
      nil, Variant::Null.new, Variant.from(3_i8),
    ] of Variant?)
    result = PVC.shred_variant(input, DT.int64)
    typed = result.typed_value_column.not_nil!.as(Arrow::PrimitiveArray(Int64))
    result.metadata_column.same?(input.metadata_column).should be_true
    result.len.should eq 6
    typed.value(0).should eq 42
    typed.value(2).should eq 100
    typed.value(5).should eq 3
    [0, 2, 5].each { |index| array_null?(result.value_column, index).should be_true }
    fallback_value(result.metadata_column, result.value_column, 1).should eq Variant.from("hello")
    result.is_null(3).should be_true
    result.is_null(4).should be_false
    fallback_value(result.metadata_column, result.value_column, 4).should eq Variant::Null.new
    [1, 3, 4].each { |index| array_null?(typed, index).should be_true }
    expect_raises(ParquetVariant::InvalidArgumentError, "Input is already shredded") do
      PVC.shred_variant(result, DT.int64)
    end
  end

  it "honors strict cast options while preserving both null kinds" do
    strict = Arrow::CastOptions.new(safe: false)
    input = PVC::VariantArray.from_iter([nil, Variant::Null.new, Variant.from(false)] of Variant?)
    result = PVC.shred_variant_with_options(input, DT.boolean, strict)
    result.is_null(0).should be_true
    result.is_null(1).should be_false
    result.typed_value_column.not_nil!.as(Arrow::BooleanArray).value(2).should be_false
    assert_shred_roundtrip(input, result)
    expect_raises(ParquetVariant::CastError) do
      PVC.shred_variant_with_options(PVC::VariantArray.from_iter([Variant.from("bad")]), DT.int64, strict)
    end
  end

  it "builds sorted nested schemas with nullability and replacement paths" do
    schema = PVC::ShreddedSchemaBuilder.new
      .with_path("z", DT.float64)
      .with_path("a.b", {DT.utf8, false})
      .with_path("a.c", Arrow::Field.new("renamed", DT.int64, false))
      .build
    schema.fields.not_nil!.map(&.name).should eq ["a", "z"]
    nested = schema.fields.not_nil![0].data_type.fields.not_nil!
    nested.map(&.name).should eq ["b", "c"]
    nested.each { |field| field.nullable?.should be_false }
    PVC::ShreddedSchemaBuilder.new.build.should eq DT.null
    PVC::ShreddedSchemaBuilder.new.with_path("", DT.int64).build.should eq DT.int64
    PVC::ShreddedSchemaBuilder.new.with_path("a", DT.int64).with_path("a", DT.float64)
      .build.fields.not_nil![0].data_type.should eq DT.float64
    path = ParquetVariant::VariantPath.from_iter([ParquetVariant::VariantPathElement.field("a.b")])
    PVC::ShreddedSchemaBuilder.new.with_path(path, DT.int64).build.fields.not_nil![0].name.should eq "a.b"
  end
end

describe ParquetVariantCompute::ShredVariant do
  [DT.int64, DT.list(Arrow::Field.new("item", DT.int64, true)),
   DT.struct(Arrow::Fields.new([Arrow::Field.new("id", DT.int64, true), Arrow::Field.new("name", DT.utf8, true)]))].each do |target|
    PVC::ShredVariant::NullValue.each do |mode|
      it "encodes missing rows for #{target} in #{mode} mode" do
        builder = PVC::ShredVariant.make_row_builder(target, Arrow::CastOptions.default, 1, mode)
        builder.append_null
        value, typed, nulls = builder.finish
        array_null?(typed, 0).should be_true
        (nulls.try(&.null?(0)) || false).should eq mode.top_level_variant?
        array_null?(value, 0).should eq !mode.array_element?
        if mode.array_element?
          metadata = Arrow::BinaryViewBuilder.new
          metadata.append_value(ParquetVariant::EMPTY_VARIANT_METADATA_BYTES)
          fallback_value(metadata.finish, value, 0).should eq Variant::Null.new
        end
        if target.kind.struct?
          ["id", "name"].each do |name|
            field = shredded_child(typed, name)
            field.is_null(0).should be_false
            array_null?(field.value_column, 0).should be_true
            array_null?(field.typed_value_column.not_nil!, 0).should be_true
          end
        end
      end
    end
  end

  [DT.list(Arrow::Field.new("item", DT.int64, true)),
   DT.large_list(Arrow::Field.new("item", DT.int64, true)),
   DT.list_view(Arrow::Field.new("item", DT.int64, true)),
   DT.large_list_view(Arrow::Field.new("item", DT.int64, true))].each do |target|
    it "shreds #{target} with element fallbacks and empty lists" do
      input = shred_input(["[1,2,3]", "[1,\"two\",null]", "\"not a list\"", nil, "[]"] of String?)
      result = PVC.shred_variant(input, target)
      typed = result.typed_value_column.not_nil!
      result.len.should eq 5
      array_null?(typed, 2).should be_true
      array_null?(typed, 3).should be_true
      array_null?(typed, 4).should be_false
      elements = case typed
                 when Arrow::GenericListArray(Int32), Arrow::GenericListArray(Int64),
                      Arrow::GenericListViewArray(Int32), Arrow::GenericListViewArray(Int64)
                   typed.values
                 else
                   raise "Expected a list"
                 end
      field = PVC::ShreddedVariantFieldArray.try_new(elements)
      field.len.should eq 6
      field.nulls.should be_nil
      ints = field.typed_value_column.not_nil!.as(Arrow::PrimitiveArray(Int64))
      [1_i64, 2_i64, 3_i64, 1_i64].each_with_index { |value, index| ints.value(index).should eq value }
      [4, 5].each do |index|
        array_null?(ints, index).should be_true
        array_null?(field.value_column, index).should be_false
      end
      case typed
      when Arrow::GenericListArray(Int32), Arrow::GenericListArray(Int64)
        typed.value_offsets.to_a.should eq [0, 3, 6, 6, 6, 6]
      when Arrow::GenericListViewArray(Int32), Arrow::GenericListViewArray(Int64)
        typed.value_offsets.to_a.should eq [0, 3, 6, 6, 6]
        typed.value_sizes.to_a.should eq [3, 3, 0, 0, 0]
      end
      assert_shred_roundtrip(input, result)
    end
  end

  it "shreds fixed-size lists, including null element padding, and rejects wrong sizes" do
    target = DT.fixed_size_list(Arrow::Field.new("item", DT.int64, true), 2)
    input = shred_input(["[1,2]", "\"fallback\"", "[3,4]", nil] of String?)
    result = PVC.shred_variant(input, target)
    typed = result.typed_value_column.not_nil!.as(Arrow::FixedSizeListArray)
    field = PVC::ShreddedVariantFieldArray.try_new(typed.values)
    field.len.should eq 8
    array_null?(field.value_column, 2).should be_false
    array_null?(field.typed_value_column.not_nil!, 2).should be_true
    assert_shred_roundtrip(input, result)
    expect_raises(ParquetVariant::CastError, "Expected fixed size list of size 2, got size 3") do
      PVC.shred_variant(shred_input(["[1,2,3]"] of String?), target)
    end
  end

  it "shreds objects with missing, mismatched and residual fields" do
    input = shred_input([
      %({"score":95.5,"age":30}),
      %({"score":87.2,"age":25,"email":"bob@example.com"}),
      %({"age":35}), %({"score":"ninety-five","age":"thirty"}),
      %("not an object"), "{}", nil, %({"foo":10}),
      %({"score":66.67,"foo":10}), %({"score":null}),
    ] of String?)
    target = PVC::ShreddedSchemaBuilder.new.with_path("score", DT.float64).with_path("age", DT.int64).build
    result = PVC.shred_variant(input, target)
    typed = result.typed_value_column.not_nil!
    age = shredded_child(typed, "age")
    score = shredded_child(typed, "score")
    age_typed = age.typed_value_column.not_nil!.as(Arrow::PrimitiveArray(Int64))
    score_typed = score.typed_value_column.not_nil!.as(Arrow::PrimitiveArray(Float64))
    age_typed.value(0).should eq 30
    score_typed.value(0).should eq 95.5
    array_null?(result.value_column, 0).should be_true
    fallback_value(result.metadata_column, result.value_column, 1).not_nil!.as_object.not_nil!.get("email").should eq Variant.from("bob@example.com")
    age_typed.value(2).should eq 35
    array_null?(score.value_column, 2).should be_true
    array_null?(score_typed, 2).should be_true
    score.is_null(2).should be_false
    fallback_value(result.metadata_column, score.value_column, 3).should eq Variant.from("ninety-five")
    array_null?(typed, 4).should be_true
    array_null?(typed, 5).should be_false
    result.is_null(6).should be_true
    array_null?(result.value_column, 7).should be_false
    fallback_value(result.metadata_column, score.value_column, 9).should eq Variant::Null.new
    assert_shred_roundtrip(input, result)
  end

  it "roundtrips nested lists, object elements, list fields and residual objects" do
    targets = [
      DT.list(Arrow::Field.new("item", DT.list(Arrow::Field.new("item", DT.int64, true)), true)),
      DT.list(Arrow::Field.new("item", PVC::ShreddedSchemaBuilder.new.with_path("id", DT.int64).with_path("name", DT.utf8).build, true)),
      PVC::ShreddedSchemaBuilder.new.with_path("items", DT.list(Arrow::Field.new("item", DT.int64, true))).with_path("nested.id", DT.int64).build,
    ]
    inputs = [
      ["[[1,2],[3,4],[]]", "[[5,\"bad\",null],\"not a list inner\",null]", "\"not a list\"", nil] of String?,
      [%([{"id":1,"name":"a","extra":42},{"id":"bad"},null,3,{}]), "[]", nil] of String?,
      [%({"items":[1,"bad",null],"nested":{"id":2,"extra":{"x":[1,2]}},"other":{"k":3}}), %({"nested":null}), "{}", nil] of String?,
    ]
    targets.each_with_index do |target, index|
      input = shred_input(inputs[index])
      assert_shred_roundtrip(input, PVC.shred_variant(input, target))
    end
  end
end

describe "shredding primitive type fidelity" do
  values = [
    Variant::Null.new, Variant.from(1_i8), Variant.from(2_i16), Variant.from(3_i32), Variant.from(4_i64),
    Variant.from_date(Time.utc(2003, 10, 20)),
    Variant::TimestampMicros.new(Time.unix_ns(123_456_789_000_i64)),
    Variant::TimestampNtzMicros.new(Time.unix_ns(123_456_789_000_i64)),
    Variant::TimestampNanos.new(Time.unix_ns(1_234_567_890_000_i64)),
    Variant::TimestampNtzNanos.new(Time.unix_ns(1_234_567_890_000_i64)),
    Variant.from(ParquetVariant::VariantDecimal4.try_new(123, 0)),
    Variant.from(ParquetVariant::VariantDecimal8.try_new(123_i64, 0)),
    Variant.from(ParquetVariant::VariantDecimal16.try_new(123_i128, 0)),
    Variant.from(5_f32), Variant.from(6_f64), Variant.from(true), Variant.from(false),
    Variant.from("hello".to_slice), Variant.from("hello"),
    Variant.from_time(Time::Span.new(seconds: 12345, nanoseconds: 0)),
    Variant.from(UUID.new("12345678-1234-5678-1234-567812345678")),
  ] of Variant
  types = [
    DT.boolean, DT.int8, DT.int16, DT.int32, DT.int64, DT.float32, DT.float64,
    DT.timestamp(Arrow::TimeUnit::Microsecond, "+00:00"), DT.timestamp(Arrow::TimeUnit::Microsecond),
    DT.timestamp(Arrow::TimeUnit::Nanosecond, "+00:00"), DT.timestamp(Arrow::TimeUnit::Nanosecond),
    DT.date32, DT.time64(Arrow::TimeUnit::Microsecond),
    DT.binary, DT.binary_view, DT.large_binary, DT.utf8, DT.utf8_view, DT.large_utf8,
    DT.decimal32(7, 4), DT.decimal64(7, 4), DT.decimal128(7, 4), DT.fixed_size_binary(16),
  ]
  types.each do |target|
    it "preserves every variant type while shredding to #{target}" do
      input = PVC::VariantArray.from_iter(values)
      result = PVC.shred_variant(input, target)
      typed = result.typed_value_column.not_nil!
      values.each_with_index do |value, index|
        expected = case value
                   when Variant::Int8, Variant::Int16, Variant::Int32, Variant::Int64,
                        Variant::Decimal4, Variant::Decimal8, Variant::Decimal16
                     target.kind.int8? || target.kind.int16? || target.kind.int32? || target.kind.int64? ||
                       target.kind.decimal32? || target.kind.decimal64? || target.kind.decimal128?
                   when Variant::Date
                     target.kind.date32?
                   when Variant::TimestampMicros, Variant::TimestampNanos
                     target.kind.timestamp? && !target.timezone.nil?
                   when Variant::TimestampNtzMicros, Variant::TimestampNtzNanos
                     target.kind.timestamp? && target.timezone.nil?
                   when Variant::Float
                     target.kind.float32?
                   when Variant::Double
                     target.kind.float64?
                   when Variant::BooleanTrue, Variant::BooleanFalse
                     target.kind.boolean?
                   when Variant::Binary
                     target.kind.binary? || target.kind.binary_view? || target.kind.large_binary?
                   when Variant::String, Variant::ShortString
                     target.kind.utf8? || target.kind.utf8_view? || target.kind.large_utf8?
                   when Variant::Time
                     target.kind.time64?
                   when Variant::Uuid
                     target.kind.fixed_size_binary?
                   else
                     false
                   end
        array_null?(result.value_column, index).should eq expected
        array_null?(typed, index).should eq !expected
        unless expected
          fallback_value(result.metadata_column, result.value_column, index).should eq value
        end
      end
    end
  end

  it "keeps non-integral nanoseconds in the fallback when targeting microseconds" do
    value = Variant::TimestampNanos.new(Time.unix_ns(1_234_567_890_001_i64))
    input = PVC::VariantArray.from_iter([value.as(Variant)])
    result = PVC.shred_variant(input, DT.timestamp(Arrow::TimeUnit::Microsecond, "UTC"))
    array_null?(result.typed_value_column.not_nil!, 0).should be_true
    fallback_value(result.metadata_column, result.value_column, 0).should eq value
    assert_shred_roundtrip(input, result)
  end

  it "rejects every unsupported shredding type" do
    item = Arrow::Field.new("item", DT.int64, true)
    invalid = [
      DT.null, DT.uint8, DT.uint16, DT.uint32, DT.uint64, DT.float16, DT.decimal256(38, 10),
      DT.date64, DT.time32(Arrow::TimeUnit::Second), DT.time64(Arrow::TimeUnit::Nanosecond),
      DT.timestamp(Arrow::TimeUnit::Second), DT.timestamp(Arrow::TimeUnit::Millisecond, "UTC"),
      DT.duration(Arrow::TimeUnit::Nanosecond), DT.interval(Arrow::IntervalUnit::DayTime),
      DT.fixed_size_binary(17), DT.dictionary(DT.int32, DT.utf8),
      DT.union(Arrow::UnionFields.new([{1_i8, item}] of Tuple(Int8, Arrow::Field)), Arrow::UnionMode::Dense),
      DT.run_end_encoded(Arrow::Field.new("run_ends", DT.int32, false), Arrow::Field.new("values", DT.utf8, true)),
      DT.map(Arrow::Field.new("entries", DT.struct(Arrow::Fields.new([
        Arrow::Field.new("key", DT.utf8, false), Arrow::Field.new("value", DT.int64, true),
      ])), false), false),
    ]
    input = PVC::VariantArray.from_iter([Variant.from(42_i32)])
    invalid.each do |target|
      expect_raises(ParquetVariant::InvalidArgumentError, "is not a valid variant shredding type") do
        PVC.shred_variant(input, target)
      end
    end
    expect_raises(ParquetVariant::InvalidArgumentError, "Only FixedSizeBinary(16) for UUID is supported.") do
      PVC.shred_variant(input, DT.fixed_size_binary(17))
    end
  end

  it "retains explicit Variant::Null for a valid missing top-level value" do
    metadata = Arrow::BinaryViewBuilder.new
    metadata.append_value(ParquetVariant::EMPTY_VARIANT_METADATA_BYTES)
    input = PVC::VariantArray.from_parts(metadata.finish, PVC::VariantArray.all_null_value_column(1), nil, nil)
    result = PVC.shred_variant(input, DT.int64)
    result.is_null(0).should be_false
    array_null?(result.typed_value_column.not_nil!, 0).should be_true
    result.value(0).should eq Variant::Null.new
    array_null?(result.value_column, 0).should be_false
  end

  it "tags UUID leaves at the top level and inside objects" do
    uuid = UUID.new("12345678-1234-5678-1234-567812345678")
    input = PVC::VariantArray.from_iter([Variant.from(uuid), nil, Variant.from(false)] of Variant?)
    result = PVC.shred_variant(input, DT.fixed_size_binary(16))
    result.inner.field_by_name("typed_value").not_nil!.extension_type_name.should eq "arrow.uuid"
    result.typed_value_column.not_nil!.as(Arrow::FixedSizeBinaryArray).value(0).should eq uuid.bytes.to_slice
    assert_shred_roundtrip(input, result)
    builder = PVC::VariantArrayBuilder.new
    object = builder.new_object
    object.insert("id", uuid)
    object.finish
    nested = PVC.shred_variant(builder.build, PVC::ShreddedSchemaBuilder.new.with_path("id", DT.fixed_size_binary(16)).build)
    field = shredded_child(nested.typed_value_column.not_nil!, "id")
    field.inner.field_by_name("typed_value").not_nil!.extension_type_name.should eq "arrow.uuid"
  end

  {% for bits in [32, 64, 128] %}
    {% for config in [{4, 2}, {6, 3}, {6, 0}] %}
      it "shreds Decimal{{bits.id}} with precision {{config[0]}} and scale {{config[1]}} without rounding" do
        input = PVC::VariantArray.from_iter([
          Variant.from(12_i8), Variant.from(234_i16), Variant.from(456_i32), Variant.from(456_i64),
          Variant.from(ParquetVariant::VariantDecimal4.try_new(1200, 2)),
          Variant.from(ParquetVariant::VariantDecimal8.try_new(1230_i64, 2)),
          Variant.from(ParquetVariant::VariantDecimal16.try_new(1234_i128, 2)),
        ] of Variant)
        result = PVC.shred_variant(input, DT.decimal{{bits.id}}({{config[0]}}, {{config[1]}}))
        typed = result.typed_value_column.not_nil!.as(Arrow::PrimitiveArray(Int{{bits.id}}))
        typed.data_type.precision.should eq {{config[0]}}
        typed.data_type.scale.should eq {{config[1]}}
        expected = {% if config[1] == 2 %}
                     [1200_i64, nil, nil, nil, 1200_i64, 1230_i64, 1234_i64]
                   {% elsif config[1] == 3 %}
                     [12000_i64, 234000_i64, 456000_i64, 456000_i64, 12000_i64, 12300_i64, 12340_i64]
                   {% else %}
                     [12_i64, 234_i64, 456_i64, 456_i64, 12_i64, nil, nil]
                   {% end %}
        expected.each_with_index do |value, index|
          array_null?(typed, index).should eq value.nil?
          array_null?(result.value_column, index).should eq !value.nil?
          typed.value(index).should eq value if value
        end
        assert_shred_roundtrip(input, result)
      end
    {% end %}
  {% end %}

  it "shreds integral decimals to integers without rounding" do
    input = PVC::VariantArray.from_iter([
      Variant.from(ParquetVariant::VariantDecimal16.try_new(1200_i128, 2)),
      Variant.from(ParquetVariant::VariantDecimal16.try_new(1230_i128, 2)),
    ])
    result = PVC.shred_variant(input, DT.int64)
    typed = result.typed_value_column.not_nil!.as(Arrow::PrimitiveArray(Int64))
    typed.value(0).should eq 12
    array_null?(typed, 1).should be_true
    assert_shred_roundtrip(input, result)
  end

  it "handles empty inputs and rejects malformed and list schema paths" do
    input = PVC::VariantArray.from_iter([] of Variant)
    PVC.shred_variant(input, DT.int64).len.should eq 0
    expect_raises(ParquetVariant::InvalidArgumentError) do
      PVC::ShreddedSchemaBuilder.new.with_path("a[", DT.int64)
    end
    expect_raises(ParquetVariant::InvalidArgumentError, "List paths are not supported yet") do
      PVC::ShreddedSchemaBuilder.new.with_path("a[0]", DT.int64)
    end
    builder = PVC::ShreddedSchemaBuilder.new.with_path("a", DT.int64).with_path("a.b", DT.utf8)
    builder.build.fields.not_nil![0].data_type.kind.struct?.should be_true
    builder.with_path("a", DT.float64).build.fields.not_nil![0].data_type.should eq DT.float64
  end
end
