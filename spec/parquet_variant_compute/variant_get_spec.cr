require "../spec_helper"

# Ported from parquet-variant-compute/src/variant_get.rs, apache/arrow-rs
# 60.0.0 (ef1fa157977633f0ba21aa921f9ef3d5c669235b).
private alias PVC = ParquetVariantCompute
private alias Variant = ParquetVariant::Variant
private alias DT = Arrow::DataType

private def get_required(value : T?) : T forall T
  raise "Expected a value" if value.nil?
  value
end

private def get_input(rows : Array(String?)) : PVC::VariantArray
  builder = PVC::VariantArrayBuilder.new
  rows.each { |row| row ? builder.append_json(row) : builder.append_null }
  builder.build
end

private def get_options(data_type : Arrow::DataType, path : String = "", safe : Bool = true) : PVC::GetOptions
  PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.parse(path))
    .with_as_type(Arrow::Field.new("result", data_type, true))
    .with_cast_options(Arrow::CastOptions.new(safe: safe))
end

private def get_null?(array : Arrow::ArrayBase, index : Int32) : Bool
  array.nulls.try(&.null?(index)) || array.data_type.kind.null?
end

private def get_ints(array : Arrow::ArrayBase) : Array(Int32?)
  typed = array.as(Arrow::PrimitiveArray(Int32))
  Array(Int32?).new(typed.len) { |index| get_null?(typed, index) ? nil : typed.value(index) }
end

describe "variant_get unshredded extraction" do
  [
    { %({"some_field":1234}), "some_field", "1234" },
    {"[1234,5678]", "[0]", "1234"},
    { %({"top_level_field":{"inner_field":1234}}), "top_level_field.inner_field", "1234" },
    { %([{"some_field":1234}]), "[0].some_field", "1234" },
    { %({"some_field":[1234]}), "some_field[0]", "1234" },
    { %({"top_level_field":{"inner_field":1234}}), "top_level_field", %({"inner_field":1234}) },
  ].each do |(json, path, expected)|
    it "extracts #{path} as a variant (upstream primitive/complex path tests)" do
      input = get_input([json] of String?)
      options = PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.parse(path))
      result = PVC::VariantArray.try_new(PVC.variant_get(input.inner, options))
      result.len.should eq 1
      result.nulls.should be_nil
      result.value(0).should eq get_input([expected] of String?).value(0)
    end
  end

  it "provides default options and casts with safe/strict behavior" do
    options = PVC::GetOptions.new
    options.path.empty?.should be_true
    options.as_type.should be_nil
    options.cast_options.safe?.should be_true
    input = PVC::VariantArray.from_iter([Variant.from(34_i32), nil, Variant.from("n/a"), Variant.from(100_i32)] of Variant?)
    result = PVC.variant_get(input.inner, get_options(DT.int32))
    get_ints(result).should eq [34, nil, nil, 100]
    error = expect_raises(ParquetVariant::CastError) do
      PVC.variant_get(input.inner, get_options(DT.int32, safe: false))
    end
    error.message.should eq "Cast error: Failed to extract primitive of type Int32 from variant ShortString(ShortString(\"n/a\")) at path VariantPath([])"
  end

  it "preserves SQL nulls and returns null for missing paths in either cast mode" do
    input = get_input([%({"x":42}), "{}", nil, %({"x":null})] of String?)
    [true, false].each do |safe|
      get_ints(PVC.variant_get(input.inner, get_options(DT.int32, "x", safe))).should eq [42, nil, nil, nil]
    end
  end
end

private def get_metadata(len : Int32) : Arrow::ArrayBase
  builder = Arrow::BinaryViewBuilder.new
  len.times { builder.append_value(ParquetVariant::EMPTY_VARIANT_METADATA_BYTES) }
  builder.finish
end

private def get_perfect(typed : Arrow::ArrayBase, nulls : Arrow::NullBuffer? = nil) : PVC::VariantArray
  PVC::VariantArray.from_parts(get_metadata(typed.len), PVC::VariantArray.all_null_value_column(typed.len), typed, nulls)
end

private def get_shredded_field(typed : Arrow::ArrayBase) : Arrow::StructArray
  PVC::ShreddedVariantFieldArray.from_parts(PVC::VariantArray.all_null_value_column(typed.len), typed, nil).inner
end

private def get_struct(fields : Array(Tuple(String, Arrow::ArrayBase)), nulls : Arrow::NullBuffer? = nil) : Arrow::StructArray
  builder = PVC::StructArrayBuilder.new
  fields.each { |(name, array)| builder.with_field(name, array, true) }
  builder.with_nulls(nulls) if nulls
  builder.build
end

describe "variant_get shredded paths" do
  it "reuses perfectly shredded primitive arrays directly" do
    typed = Arrow::PrimitiveArray(Int32).from_nullable([1, 2, 3])
    result = PVC.variant_get(get_perfect(typed).inner, get_options(DT.int32))
    result.same?(typed).should be_true
  end

  it "merges parent and child nulls without rebuilding scalar values" do
    typed = Arrow::PrimitiveArray(Int32).from_nullable([nil, 1, 2] of Int32?)
    parent = Arrow::NullBuffer.from_slice([true, false, true])
    get_ints(PVC.variant_get(get_perfect(typed, parent).inner, get_options(DT.int32))).should eq [nil, nil, 2]
  end

  it "extracts shredded fields and falls back for unshredded fields" do
    input = get_input([%({"x":1,"y":"foo"}), %({"x":42})] of String?)
    target = PVC::ShreddedSchemaBuilder.new.with_path("x", DT.int32).build
    shredded = PVC.shred_variant(input, target)
    get_ints(PVC.variant_get(shredded.inner, get_options(DT.int32, "x"))).should eq [1, 42]
    result = PVC::VariantArray.try_new(PVC.variant_get(shredded.inner, PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.parse("y"))))
    result.value(0).should eq Variant.from("foo")
    result.is_null(1).should be_true
  end

  it "distinguishes no requested type from explicit unshredded Variant output" do
    input = get_input([%({"field_name":{"k":100000}}), %({"field_name":{"k":"s"}})] of String?)
    schema = PVC::ShreddedSchemaBuilder.new.with_path("field_name.k", DT.int32).build
    shredded = PVC.shred_variant(input, schema)
    path = ParquetVariant::VariantPath.parse("field_name")
    default = PVC::VariantArray.try_new(PVC.variant_get(shredded.inner, PVC::GetOptions.new_with_path(path)))
    default.typed_value_column.should_not be_nil
    [input, shredded].each do |array|
      options = PVC::GetOptions.new_with_path(path).with_as_type(input.field("result"))
      output = PVC::VariantArray.try_new(PVC.variant_get(array.inner, options))
      output.typed_value_column.should be_nil
      get_required(output.inner.field_by_name("value")).nullable?.should be_false
      output.value(0).should eq get_input([%({"k":100000})] of String?).value(0)
      output.value(1).should eq get_input([%({"k":"s"})] of String?).value(0)
    end
    expect_raises(Parquet::NYIError, "variant_get with shredded `Variant` output is not yet supported") do
      PVC.variant_get(shredded.inner, PVC::GetOptions.new_with_path(path).with_as_type(shredded.field("result")))
    end
  end

  it "returns typed nulls for missing paths even with strict casts" do
    input = get_perfect(Arrow::PrimitiveArray(Int32).from_nullable([1, 2, 3]))
    ["missing", "[0]"].each do |path|
      [true, false].each do |safe|
        get_ints(PVC.variant_get(input.inner, get_options(DT.int32, path, safe))).should eq [nil, nil, nil]
      end
    end
    default = PVC.variant_get(input.inner, PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.parse("missing")))
    default.should be_a(Arrow::NullArray)
    field = PVC::VariantArray.from_iter([Variant.from(1_i32)]).field("result")
    output = PVC::VariantArray.try_new(PVC.variant_get(input.inner, PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.parse("missing")).with_as_type(field)))
    output.nulls.try(&.null_count).should eq 3
    get_required(output.inner.field_by_name("value")).nullable?.should be_false
  end

  it "reports malformed shredded object fields with their name and type" do
    typed = get_struct([{"x", Arrow::PrimitiveArray(Int32).from_nullable([1, 42]).as(Arrow::ArrayBase)}])
    error = expect_raises(ParquetVariant::InvalidArgumentError) do
      PVC.variant_get(get_perfect(typed).inner, PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.parse("x")))
    end
    error.message.should eq "Invalid argument error: Shredded object field 'x' must be a Struct containing 'value' and/or 'typed_value', got Int32"
  end

  it "accumulates masks across nested objects and preserves requested struct fields" do
    leaf = get_shredded_field(Arrow::PrimitiveArray(Int32).from_nullable([42, 43, 44, 45]))
    inner = get_struct([{"inner", leaf.as(Arrow::ArrayBase)}], Arrow::NullBuffer.from_slice([true, false, true, true]))
    outer = get_struct([{"outer", get_shredded_field(inner).as(Arrow::ArrayBase)}], Arrow::NullBuffer.from_slice([true, true, false, true]))
    input = get_perfect(outer, Arrow::NullBuffer.from_slice([true, true, true, false]))
    get_ints(PVC.variant_get(input.inner, get_options(DT.int32, "outer.inner"))).should eq [42, nil, nil, nil]
    type = DT.struct(Arrow::Fields.new([Arrow::Field.new("inner", DT.int32, true)]))
    result = PVC.variant_get(input.inner, get_options(type, "outer")).as(Arrow::StructArray)
    result.data_type.should eq type
    get_ints(get_required(result.column_by_name("inner"))).should eq [42, nil, nil, nil]
    [2, 3].each { |index| get_null?(result, index).should be_true }
  end
end

describe "variant_get shredded list traversal" do
  [DT.list(Arrow::Field.new("item", DT.utf8, true)),
   DT.large_list(Arrow::Field.new("item", DT.utf8, true)),
   DT.list_view(Arrow::Field.new("item", DT.utf8, true)),
   DT.large_list_view(Arrow::Field.new("item", DT.utf8, true))].each do |type|
    it "extracts typed and fallback elements from #{type}" do
      input = get_input([%(["comedy","drama"]), %(["horror",123])] of String?)
      shredded = PVC.shred_variant(input, type)
      options = PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.from(1))
      variants = PVC::VariantArray.try_new(PVC.variant_get(shredded.inner, options))
      variants.value(0).should eq Variant.from("drama")
      variants.value(1).as_int64.should eq 123
      strings = PVC.variant_get(shredded.inner, get_options(DT.utf8, "[0]")).as(Arrow::GenericStringArray(Int32))
      strings.value(0).should eq "comedy"
      strings.value(1).should eq "horror"
      get_ints(PVC.variant_get(shredded.inner, get_options(DT.int32, "[1]"))).should eq [nil, 123]
      expect_raises(ParquetVariant::CastError) do
        PVC.variant_get(shredded.inner, get_options(DT.int32, "[1]", false))
      end
      out_of_bounds = PVC::VariantArray.try_new(PVC.variant_get(shredded.inner,
        PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.from(10)).with_cast_options(Arrow::CastOptions.new(safe: false))))
      out_of_bounds.len.should eq 2
      2.times { |index| out_of_bounds.value(index).should eq Variant::Null.new }
    end

    it "propagates SQL nulls, empty lists and explicit null elements through #{type}" do
      input = get_input([%(["a",null]), nil, "[]", %(["b"])] of String?)
      shredded = PVC.shred_variant(input, type)
      result = PVC.variant_get(shredded.inner, get_options(DT.utf8, "[0]"))
      result.len.should eq 4
      get_null?(result, 0).should be_false
      [1, 2].each { |index| get_null?(result, index).should be_true }
      variants = PVC::VariantArray.try_new(PVC.variant_get(shredded.inner,
        PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.from(1))))
      variants.value(0).should eq Variant::Null.new
      variants.is_null(0).should be_false
      variants.is_null(1).should be_true
      # Out-of-range elements have missing value/typed_value, interpreted as
      # Variant::Null; only the list's parent validity supplies SQL nulls.
      variants.is_null(2).should be_false
      variants.is_null(3).should be_false
    end
  end

  it "traverses shredded lists inside objects, objects inside lists and lists of lists" do
    input = get_input([%({"a":["comedy","drama"]}), %({"a":["horror",123]})] of String?)
    type = PVC::ShreddedSchemaBuilder.new.with_path("a", DT.list(Arrow::Field.new("item", DT.utf8, true))).build
    shredded = PVC.shred_variant(input, type)
    result = PVC::VariantArray.try_new(PVC.variant_get(shredded.inner,
      PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.parse("a[1]"))))
    result.value(0).should eq Variant.from("drama")
    result.value(1).as_int64.should eq 123

    input = get_input([%([{"x":1},{"x":2}]), %([{"x":3},{"y":4}])] of String?)
    type = DT.list(Arrow::Field.new("item", PVC::ShreddedSchemaBuilder.new.with_path("x", DT.int32).build, true))
    shredded = PVC.shred_variant(input, type)
    get_ints(PVC.variant_get(shredded.inner, get_options(DT.int32, "[0].x"))).should eq [1, 3]
    get_ints(PVC.variant_get(shredded.inner, get_options(DT.int32, "[1].x"))).should eq [2, nil]
    get_ints(PVC.variant_get(shredded.inner, get_options(DT.int32, "[1].y"))).should eq [nil, 4]

    input = get_input([%([["a","b"],["c","d"]]), %([["x",123],["y","z"]])] of String?)
    inner = DT.list(Arrow::Field.new("item", DT.utf8, true))
    shredded = PVC.shred_variant(input, DT.list(Arrow::Field.new("item", inner, true)))
    result = PVC::VariantArray.try_new(PVC.variant_get(shredded.inner,
      PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.parse("[0][1]"))))
    result.value(0).should eq Variant.from("b")
    result.value(1).as_int64.should eq 123
  end
end

describe "variant_get scalar and mask parity" do
  {% for type, target in {Int8: "int8", Int16: "int16", Int32: "int32", Int64: "int64", Float32: "float32", Float64: "float64"} %}
    it "reads partial and perfect {{type.id}} shredding as variants and typed arrays" do
      first = Variant.from({{type.id}}.new(34))
      last = Variant.from({{type.id}}.new(100))
      input = PVC::VariantArray.from_iter([first, nil, Variant.from("n/a"), last] of Variant?)
      shredded = PVC.shred_variant(input, DT.{{target.id}})
      result = PVC::VariantArray.try_new(PVC.variant_get(shredded.inner))
      result.value(0).should eq first
      result.is_null(1).should be_true
      result.value(2).should eq Variant.from("n/a")
      result.value(3).should eq last
      typed = PVC.variant_get(shredded.inner, get_options(DT.{{target.id}})).as(Arrow::PrimitiveArray({{type.id}}))
      typed.value(0).should eq {{type.id}}.new(34)
      typed.value(3).should eq {{type.id}}.new(100)
      [1, 2].each { |index| get_null?(typed, index).should be_true }
      expect_raises(ParquetVariant::CastError) do
        PVC.variant_get(shredded.inner, get_options(DT.{{target.id}}, safe: false))
      end
      source = Arrow::PrimitiveArray({{type.id}}).from_values([{{type.id}}.new(1), {{type.id}}.new(2), {{type.id}}.new(3)])
      perfect = get_perfect(source)
      PVC.variant_get(perfect.inner, get_options(DT.{{target.id}})).same?(source).should be_true
      result = PVC::VariantArray.try_new(PVC.variant_get(perfect.inner))
      3.times { |index| result.value(index).should eq Variant.from({{type.id}}.new(index + 1)) }
      masked = get_perfect(source, Arrow::NullBuffer.from_slice([true, false, true]))
      output = PVC.variant_get(masked.inner, get_options(DT.{{target.id}}))
      output.data_type.should eq source.data_type
      get_null?(output, 1).should be_true
      output.as(Arrow::PrimitiveArray({{type.id}})).value(2).should eq {{type.id}}.new(3)
    end
  {% end %}

  it "preserves all-null inputs and Null typed arrays" do
    input = PVC::VariantArray.from_iter([nil, nil, nil] of Variant?)
    result = PVC::VariantArray.try_new(PVC.variant_get(input.inner))
    result.len.should eq 3
    result.nulls.try(&.null_count).should eq 3
    get_ints(PVC.variant_get(input.inner, get_options(DT.int32))).should eq [nil, nil, nil]
    nulls = Arrow::NullArray.new(3)
    output = PVC.variant_get(get_perfect(nulls, Arrow::NullBuffer.new_null(3)).inner, get_options(DT.null))
    output.same?(nulls).should be_true
    mismatched = PVC.variant_get(get_perfect(nulls).inner, get_options(DT.int32, safe: false))
    get_ints(mismatched).should eq [nil, nil, nil]
  end

  it "reuses and masks boolean, string, binary, temporal and decimal arrays" do
    arrays = [Arrow::BooleanArray.from_values([true, false, true]).as(Arrow::ArrayBase)]
    {% for builder_type in [Arrow::GenericStringBuilder(Int32), Arrow::GenericStringBuilder(Int64), Arrow::StringViewBuilder] %}
      builder = {{builder_type}}.new
      ["foo", "bar", "baz"].each { |value| builder.append_value(value) }
      arrays << builder.finish
    {% end %}
    {% for builder_type in [Arrow::GenericBinaryBuilder(Int32), Arrow::GenericBinaryBuilder(Int64), Arrow::BinaryViewBuilder] %}
      builder = {{builder_type}}.new
      ["Apache", "masked-null", "Parquet-variant"].each { |value| builder.append_value(value.to_slice) }
      arrays << builder.finish
    {% end %}
    arrays << Arrow::PrimitiveArray(Int32).from_values([20348, 20349, 20340]).with_data_type(DT.date32)
    arrays << Arrow::PrimitiveArray(Int64).from_values([1_i64, 2_i64, 3_i64]).with_data_type(DT.time64(Arrow::TimeUnit::Microsecond))
    [Arrow::TimeUnit::Microsecond, Arrow::TimeUnit::Nanosecond].each do |unit|
      [nil, "UTC"].each do |timezone|
        arrays << Arrow::PrimitiveArray(Int64).from_values([1_i64, 2_i64, 3_i64]).with_data_type(DT.timestamp(unit, timezone))
      end
    end
    arrays << Arrow::PrimitiveArray(Int32).from_values([12345, 23400, -12342]).with_data_type(DT.decimal32(5, 2))
    arrays << Arrow::PrimitiveArray(Int64).from_values([1234567809_i64, 1456787000_i64, -1234561203_i64]).with_data_type(DT.decimal64(10, 1))
    arrays << Arrow::PrimitiveArray(Int128).from_values([12345678901234567899_i128, 23445677483748324300_i128, -12345678901234567899_i128]).with_data_type(DT.decimal128(20, 3))
    arrays.each do |source|
      PVC.variant_get(get_perfect(source).inner, get_options(source.data_type)).same?(source).should be_true
      parent = Arrow::NullBuffer.from_slice([true, false, true])
      output = PVC.variant_get(get_perfect(source, parent).inner, get_options(source.data_type))
      output.data_type.should eq source.data_type
      get_null?(output, 1).should be_true
      output.slice(0, 1).should eq source.slice(0, 1)
      output.slice(2, 1).should eq source.slice(2, 1)
    end
  end

  it "preserves safe casting regardless of requested field nullability" do
    input = get_input([
      %({"x":42}), %({"x":"not_a_number"}), %({"x":null}), %({"x":"hello"}), %({"y":100}),
      %({"x":127}), %({"x":32767}), %({"x":2147483647}), %({"x":9223372036854775807}),
    ] of String?)
    [true, false].each do |nullable|
      options = PVC::GetOptions.new_with_path(ParquetVariant::VariantPath.parse("x"))
        .with_as_type(Arrow::Field.new("result", DT.int32, nullable))
      get_ints(PVC.variant_get(input.inner, options)).should eq [42, nil, nil, nil, nil, 127, 32767, 2147483647, nil]
    end
  end

  it "returns typed nulls for invalid time values and raises in strict mode" do
    input = PVC::VariantArray.from_iter(Array(Variant).new(3) { Variant.from(86401000000_i64) })
    type = DT.time64(Arrow::TimeUnit::Microsecond)
    result = PVC.variant_get(input.inner, get_options(type))
    result.len.should eq 3
    3.times { |index| get_null?(result, index).should be_true }
    error = expect_raises(ParquetVariant::CastError) do
      PVC.variant_get(input.inner, get_options(type, safe: false))
    end
    get_required(error.message).should contain "Failed to extract primitive of type Time64(µs) from variant Int64(86401000000)"
    [DT.duration(Arrow::TimeUnit::Microsecond), DT.interval(Arrow::IntervalUnit::YearMonth)].each do |unsupported|
      error = expect_raises(Parquet::ParquetError) { PVC.variant_get(input.inner, get_options(unsupported)) }
      get_required(error.message).should contain "Casting Variant to duration/interval types is not supported"
    end
  end

  it "preserves error messages for boolean, numeric and temporal casts" do
    [DT.boolean, DT.float32, DT.timestamp(Arrow::TimeUnit::Nanosecond)].each do |type|
      value = type.kind.timestamp? ? Variant.from(false) : Variant.from("abcd")
      error = expect_raises(ParquetVariant::CastError) do
        PVC.variant_get(PVC::VariantArray.from_iter([value]).inner, get_options(type, safe: false))
      end
      get_required(error.message).should contain "Failed to extract primitive of type #{type}"
    end
  end
end

describe "variant_get struct extraction" do
  it "extracts subset/superset schemas from perfect and partial shredding" do
    input = get_input([%({"a":1,"b":2,"c":3}), nil, %({"b":2}), %({"a":1}), "{}"] of String?)
    target = DT.struct(Arrow::Fields.new([Arrow::Field.new("a", DT.int32, true), Arrow::Field.new("b", DT.int32, true), Arrow::Field.new("d", DT.int32, true)]))
    schemas = [PVC::ShreddedSchemaBuilder.new.with_path("a", DT.int32).with_path("b", DT.int32).with_path("c", DT.int32).build,
               PVC::ShreddedSchemaBuilder.new.with_path("a", DT.int32).build]
    [input, PVC.shred_variant(input, schemas[0]), PVC.shred_variant(input, schemas[1])].each do |array|
      result = PVC.variant_get(array.inner, get_options(target)).as(Arrow::StructArray)
      result.data_type.should eq target
      get_ints(get_required(result.column_by_name("a"))).should eq [1, nil, nil, 1, nil]
      get_ints(get_required(result.column_by_name("b"))).should eq [2, nil, 2, nil, nil]
      get_ints(get_required(result.column_by_name("d"))).should eq [nil, nil, nil, nil, nil]
      get_null?(result, 1).should be_true
      [0, 2, 3, 4].each { |index| get_null?(result, index).should be_false }
    end
  end

  it "preserves non-object cast semantics and validates non-nullable child fields" do
    input = get_input([%({"a":1,"b":2,"extra":3}), "123", "{}"] of String?)
    type = DT.struct(Arrow::Fields.new([Arrow::Field.new("a", DT.int32, true), Arrow::Field.new("b", DT.int32, true)]))
    result = PVC.variant_get(input.inner, get_options(type)).as(Arrow::StructArray)
    get_ints(get_required(result.column_by_name("a"))).should eq [1, nil, nil]
    get_ints(get_required(result.column_by_name("b"))).should eq [2, nil, nil]
    get_null?(result, 0).should be_false
    get_null?(result, 1).should be_true
    get_null?(result, 2).should be_false
    expect_raises(ParquetVariant::CastError, "Failed to extract struct from variant") do
      PVC.variant_get(input.inner, get_options(type, safe: false))
    end
    type = DT.struct(Arrow::Fields.new([Arrow::Field.new("a", DT.int32, false), Arrow::Field.new("missing", DT.int32, false)]))
    error = expect_raises(Arrow::ArrowError) do
      PVC.variant_get(get_input([%({"a":1})] of String?).inner, get_options(type))
    end
    get_required(error.message).should contain "unmasked nulls for non-nullable StructArray field \"missing\""
  end

  it "keeps struct rows valid when individual fields fail casting" do
    input = get_input([%({"a":42,"b":"hello"}), %({"a":"world","b":100}), %({"a":55,"b":77})] of String?)
    type = DT.struct(Arrow::Fields.new([Arrow::Field.new("a", DT.int32, true), Arrow::Field.new("b", DT.int32, true)]))
    result = PVC.variant_get(input.inner, get_options(type)).as(Arrow::StructArray)
    result.null_count.should eq 0
    get_ints(get_required(result.column_by_name("a"))).should eq [42, nil, 55]
    get_ints(get_required(result.column_by_name("b"))).should eq [nil, 100, 77]
  end
end

describe "variant_get decimal conversions" do
  {% for config in [{32, 4, 9}, {64, 8, 18}, {128, 16, 38}] %}
    {% bits, bytes, precision = config[0], config[1], config[2] %}
    it "rescales Decimal{{bits}} with half-away-from-zero rounding" do
      input = PVC::VariantArray.from_iter([
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(Int{{bits.id}}.new(1234), 2)),
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(Int{{bits.id}}.new(1234), 3)),
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(Int{{bits.id}}.new(1234), 0)), nil,
      ] of Variant?)
      output = PVC.variant_get(input.inner, get_options(DT.decimal{{bits.id}}({{precision}}, 2))).as(Arrow::PrimitiveArray(Int{{bits.id}}))
      output.data_type.should eq DT.decimal{{bits.id}}({{precision}}, 2)
      [1234, 123, 123400].each_with_index { |value, index| output.value(index).should eq value }
      get_null?(output, 3).should be_true
      values = [
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(Int{{bits.id}}.new(1235), 0)),
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(Int{{bits.id}}.new(1245), 0)),
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(Int{{bits.id}}.new(-1235), 0)),
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(Int{{bits.id}}.new(-1245), 0)),
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(Int{{bits.id}}.new(1235), 2)),
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(Int{{bits.id}}.new(1235), 3)),
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(Int{{bits.id}}.new(5235), 3)),
      ] of Variant
      output = PVC.variant_get(PVC::VariantArray.from_iter(values).inner, get_options(DT.decimal{{bits.id}}({{precision}}, -1))).as(Arrow::PrimitiveArray(Int{{bits.id}}))
      output.null_count.should eq 0
      [124, 125, -124, -125, 1, 0, 1].each_with_index { |value, index| output.value(index).should eq value }
    end

    it "handles Decimal{{bits}} overflow safely or raises for strict casts" do
      max = ParquetVariant::VariantDecimal{{bytes.id}}::MAX_UNSCALED_VALUE
      values = [
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(max, 0)),
        Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(max, {{precision}})),
      ] of Variant
      input = PVC::VariantArray.from_iter(values)
      output = PVC.variant_get(input.inner, get_options(DT.decimal{{bits.id}}(2, 2)))
      output.null_count.should eq 2
      error = expect_raises(ParquetVariant::CastError) do
        PVC.variant_get(input.inner, get_options(DT.decimal{{bits.id}}({{precision}}, 2), safe: false))
      end
      get_required(error.message).should contain "Failed to cast to Decimal{{bits}}(precision={{precision}}, scale=2) from variant Decimal{{bytes}}"
    end
  {% end %}

  {% for config in [{32, 4, 9}, {64, 8, 18}] %}
    {% bits, bytes, precision = config[0], config[1], config[2] %}
    it "handles large scale reductions and narrowing from a wider Decimal{{bits}} source" do
      max = ParquetVariant::VariantDecimal{{bytes.id}}::MAX_UNSCALED_VALUE
      values = [-max, max].map { |value| Variant.from(ParquetVariant::VariantDecimal{{bytes.id}}.try_new(value, 0)) }
      input = PVC::VariantArray.from_iter(values)
      output = PVC.variant_get(input.inner, get_options(DT.decimal{{bits.id}}({{precision}}, -{{precision}}))).as(Arrow::PrimitiveArray(Int{{bits.id}}))
      output.value(0).should eq -1
      output.value(1).should eq 1
      output = PVC.variant_get(input.inner, get_options(DT.decimal{{bits.id}}({{precision}}, -{{precision + 1}}))).as(Arrow::PrimitiveArray(Int{{bits.id}}))
      output.null_count.should eq 0
      output.value(0).should eq 0
      output.value(1).should eq 0
      wider = Variant.from(ParquetVariant::VariantDecimal{{(bytes * 2).id}}.try_new(Int{{(bits * 2).id}}.new(max) + 1, 3))
      output = PVC.variant_get(PVC::VariantArray.from_iter([wider]).inner,
        get_options(DT.decimal{{bits.id}}({{precision}}, 2))).as(Arrow::PrimitiveArray(Int{{bits.id}}))
      output.value(0).should eq max // 10 + 1
    end
  {% end %}
end

private def get_map_type(value : Arrow::DataType, key : Arrow::DataType = DT.utf8) : Arrow::DataType
  DT.map(Arrow::Field.new("entries", DT.struct(Arrow::Fields.new([
    Arrow::Field.new("keys", key, false), Arrow::Field.new("values", value, true),
  ])), false), false)
end

describe "variant_get encoded and composite output" do
  it "builds string and numeric dictionaries and run-end-encoded arrays" do
    input = get_input([%("apple"), %("banana"), nil, %("apple")] of String?)
    types = [DT.dictionary(DT.int32, DT.utf8), DT.run_end_encoded(Arrow::Field.new("run_ends", DT.int32, false), Arrow::Field.new("values", DT.utf8, true))]
    expected = Arrow::GenericStringBuilder(Int32).new
    ["apple", "banana", nil, "apple"].each { |value| value ? expected.append_value(value) : expected.append_null }
    expected = expected.finish
    types.each do |type|
      output = PVC.variant_get(input.inner, get_options(type))
      output.data_type.should eq type
      Arrow.cast(output, DT.utf8).should eq expected
    end
    input = get_input(["42", "7", nil, "42"] of String?)
    type = DT.dictionary(DT.int16, DT.int32)
    output = PVC.variant_get(input.inner, get_options(type))
    output.data_type.should eq type
    get_ints(Arrow.cast(output, DT.int32)).should eq [42, 7, nil, 42]
  end

  it "builds maps with null rows, empty objects and null values" do
    input = get_input([%({"b":2,"a":1}), %({"c":3}), nil, "{}", %({"d":null})] of String?)
    type = get_map_type(DT.int32)
    output = PVC.variant_get(input.inner, get_options(type)).as(Arrow::MapArray)
    output.data_type.should eq type
    output.value_offsets.to_a.should eq [0, 2, 3, 3, 3, 4]
    keys = output.keys.as(Arrow::GenericStringArray(Int32))
    Array(String).new(keys.len) { |index| keys.value(index) }.should eq ["a", "b", "c", "d"]
    get_ints(output.values).should eq [1, 2, 3, nil]
    get_null?(output, 2).should be_true
    get_null?(output, 3).should be_false
    input = get_input([%({"a":[1,2],"b":[]}), %({"c":[3]})] of String?)
    type = get_map_type(DT.list(Arrow::Field.new("item", DT.int32, true)))
    output = PVC.variant_get(input.inner, get_options(type)).as(Arrow::MapArray)
    list = output.values.as(Arrow::GenericListArray(Int32))
    list.value_offsets.to_a.should eq [0, 2, 2, 3]
    get_ints(list.values).should eq [1, 2, 3]
  end

  it "validates map layouts and safe/strict handling of non-objects and invalid keys" do
    input = get_input([%({"a":1}), "42"] of String?)
    output = PVC.variant_get(input.inner, get_options(get_map_type(DT.int64)))
    get_null?(output, 1).should be_true
    expect_raises(ParquetVariant::CastError, "Failed to extract object") do
      PVC.variant_get(input.inner, get_options(get_map_type(DT.int64), safe: false))
    end
    expect_raises(ParquetVariant::CastError, "Failed to cast map key") do
      PVC.variant_get(input.inner, get_options(get_map_type(DT.int64, DT.int32)))
    end
    [DT.map(Arrow::Field.new("entries", DT.int32, false), false),
     DT.map(Arrow::Field.new("entries", DT.struct(Arrow::Fields.new([Arrow::Field.new("keys", DT.utf8, false)])), false), false)].each do |type|
      expect_raises(ParquetVariant::InvalidArgumentError) { PVC.variant_get(input.inner, get_options(type)) }
    end
  end

  [DT.list(Arrow::Field.new("item", DT.int64, true)), DT.large_list(Arrow::Field.new("item", DT.int64, true)),
   DT.list_view(Arrow::Field.new("item", DT.int64, true)), DT.large_list_view(Arrow::Field.new("item", DT.int64, true)),
   DT.fixed_size_list(Arrow::Field.new("item", DT.int64, true), 3)].each do |type|
    it "casts lists as #{type} with the upstream safe/strict behavior" do
      input = get_input([%({"outer":{"list":[1,"two",3]}}), %({"outer":{"list":"not a list"}})] of String?)
      output = PVC.variant_get(input.inner, get_options(type, "outer.list"))
      output.data_type.should eq type
      get_null?(output, 0).should be_false
      get_null?(output, 1).should be_true
      elements = case output
                 when Arrow::GenericListArray(Int32), Arrow::GenericListArray(Int64), Arrow::GenericListViewArray(Int32), Arrow::GenericListViewArray(Int64), Arrow::FixedSizeListArray
                   output.values.as(Arrow::PrimitiveArray(Int64))
                 else
                   raise "Expected list"
                 end
      elements.value(0).should eq 1
      get_null?(elements, 1).should be_true
      elements.value(2).should eq 3
      expect_raises(ParquetVariant::CastError, "Failed to extract primitive of type Int64") do
        PVC.variant_get(input.inner, get_options(type, "outer.list", false))
      end
      input = get_input([%("not a list")] of String?)
      expect_raises(ParquetVariant::CastError, "Failed to extract list from variant") do
        PVC.variant_get(input.inner, get_options(type, safe: false))
      end
    end
  end

  it "preserves explicit null list elements in strict mode and rejects fixed-size mismatch" do
    input = get_input(["[1,null,3]"] of String?)
    type = DT.list(Arrow::Field.new("item", DT.int32, true))
    result = PVC.variant_get(input.inner, get_options(type, safe: false)).as(Arrow::GenericListArray(Int32))
    get_ints(result.values).should eq [1, nil, 3]
    type = DT.fixed_size_list(Arrow::Field.new("item", DT.int32, true), 2)
    get_null?(PVC.variant_get(input.inner, get_options(type)), 0).should be_true
    expect_raises(ParquetVariant::CastError, "Expected fixed size list of size 2, got size 3") do
      PVC.variant_get(input.inner, get_options(type, safe: false))
    end
  end
end

private def get_union_type(fields : Array(Tuple(Int8, Arrow::Field)), mode : Arrow::UnionMode = Arrow::UnionMode::Dense) : Arrow::DataType
  DT.union(Arrow::UnionFields.new(fields), mode)
end

describe "variant_get union output" do
  [Arrow::UnionMode::Dense, Arrow::UnionMode::Sparse].each do |mode|
    it "builds #{mode} unions with null and unmatched rows in the first child" do
      type = get_union_type([
        {0_i8, Arrow::Field.new("int", DT.int64, true)},
        {1_i8, Arrow::Field.new("str", DT.utf8, true)},
        {2_i8, Arrow::Field.new("bool", DT.boolean, true)},
      ], mode)
      input = PVC::VariantArray.from_iter([
        Variant.from(1_i8), Variant.from("hello"), Variant.from(true), nil,
        Variant::Null.new, Variant.from(2.5_f64), Variant.from(5_000_000_000_i64),
      ] of Variant?)
      output = PVC.variant_get(input.inner, get_options(type)).as(Arrow::UnionArray)
      output.data_type.should eq type
      output.type_ids.to_a.should eq [0_i8, 1_i8, 2_i8, 0_i8, 0_i8, 0_i8, 0_i8]
      ints = output.child(0_i8).as(Arrow::PrimitiveArray(Int64))
      strings = output.child(1_i8).as(Arrow::GenericStringArray(Int32))
      bools = output.child(2_i8).as(Arrow::BooleanArray)
      if mode.dense?
        get_required(output.offsets).to_a.should eq [0, 0, 0, 1, 2, 3, 4]
        ints.len.should eq 5
        ints.value(0).should eq 1
        [1, 2, 3].each { |index| get_null?(ints, index).should be_true }
        ints.value(4).should eq 5_000_000_000_i64
        strings.value(0).should eq "hello"
        bools.value(0).should be_true
      else
        output.offsets.should be_nil
        ints.len.should eq 7
        ints.value(0).should eq 1
        ints.value(6).should eq 5_000_000_000_i64
        (1..5).each { |index| get_null?(ints, index).should be_true }
        strings.len.should eq 7
        strings.value(1).should eq "hello"
        bools.len.should eq 7
        bools.value(2).should be_true
      end
    end
  end

  it "prefers exact children, skips decimals that cannot fit, and prefers a Null child" do
    type = get_union_type([{0_i8, Arrow::Field.new("big", DT.int64, true)}, {1_i8, Arrow::Field.new("small", DT.int32, true)}])
    input = PVC::VariantArray.from_iter([Variant.from(1_i8), Variant.from(2_i32), Variant.from(3_i64)] of Variant)
    output = PVC.variant_get(input.inner, get_options(type)).as(Arrow::UnionArray)
    output.type_ids.to_a.should eq [1_i8, 1_i8, 0_i8]
    get_required(output.offsets).to_a.should eq [0, 1, 0]
    get_ints(output.child(1_i8)).should eq [1, 2]
    type = get_union_type([{0_i8, Arrow::Field.new("too_narrow", DT.decimal32(3, 2), true)}, {1_i8, Arrow::Field.new("fits", DT.decimal32(5, 2), true)}])
    input = PVC::VariantArray.from_iter([Variant.from(ParquetVariant::VariantDecimal4.try_new(12345, 2))])
    [true, false].each do |safe|
      output = PVC.variant_get(input.inner, get_options(type, safe: safe)).as(Arrow::UnionArray)
      output.type_ids.to_a.should eq [1_i8]
      output.child(1_i8).as(Arrow::PrimitiveArray(Int32)).value(0).should eq 12345
    end
    type = get_union_type([{0_i8, Arrow::Field.new("int", DT.int64, true)}, {1_i8, Arrow::Field.new("null", DT.null, true)}])
    input = PVC::VariantArray.from_iter([Variant.from(1_i8), nil, Variant::Null.new, Variant.from("no matching field")] of Variant?)
    output = PVC.variant_get(input.inner, get_options(type)).as(Arrow::UnionArray)
    output.type_ids.to_a.should eq [0_i8, 1_i8, 1_i8, 1_i8]
    output.child(1_i8).len.should eq 3
  end

  it "supports encoded union children and fixed-size lists" do
    [DT.dictionary(DT.int32, DT.utf8), DT.run_end_encoded(Arrow::Field.new("run_ends", DT.int32, false), Arrow::Field.new("values", DT.utf8, true))].each do |child_type|
      type = get_union_type([{0_i8, Arrow::Field.new("encoded", child_type, true)}])
      input = get_input([%("apple"), %("banana")] of String?)
      result = PVC.variant_get(input.inner, get_options(type, safe: false)).as(Arrow::UnionArray)
      result.type_ids.to_a.should eq [0_i8, 0_i8]
      result.child(0_i8).data_type.should eq child_type
      decoded = Arrow.cast(result.child(0_i8), DT.utf8).as(Arrow::GenericStringArray(Int32))
      decoded.value(0).should eq "apple"
      decoded.value(1).should eq "banana"
    end
    type = get_union_type([{0_i8, Arrow::Field.new("fixed", DT.fixed_size_list(Arrow::Field.new("item", DT.int32, true), 2), true)}])
    [true, false].each do |safe|
      output = PVC.variant_get(get_input(["[1,2]"] of String?).inner, get_options(type, safe: safe)).as(Arrow::UnionArray)
      get_ints(output.child(0_i8).as(Arrow::FixedSizeListArray).value(0)).should eq [1, 2]
    end
  end

  it "routes objects and lists into nested children and prefers Struct over Map" do
    object_type = DT.struct(Arrow::Fields.new([Arrow::Field.new("a", DT.int32, true)]))
    list_type = DT.list(Arrow::Field.new("item", DT.int32, true))
    type = get_union_type([{0_i8, Arrow::Field.new("struct", object_type, true)}, {1_i8, Arrow::Field.new("list", list_type, true)}, {2_i8, Arrow::Field.new("str", DT.utf8, true)}])
    input = get_input([%({"a":1}), "[1,2,3]", %("s")] of String?)
    output = PVC.variant_get(input.inner, get_options(type)).as(Arrow::UnionArray)
    output.type_ids.to_a.should eq [0_i8, 1_i8, 2_i8]
    get_ints(get_required(output.child(0_i8).as(Arrow::StructArray).column_by_name("a"))).should eq [1]
    get_ints(output.child(1_i8).as(Arrow::GenericListArray(Int32)).values).should eq [1, 2, 3]
    type = get_union_type([{0_i8, Arrow::Field.new("map", get_map_type(DT.int32), true)}, {1_i8, Arrow::Field.new("struct", object_type, true)}])
    input = get_input([%({"a":1})] of String?)
    output = PVC.variant_get(input.inner, get_options(type)).as(Arrow::UnionArray)
    output.type_ids.to_a.should eq [1_i8]
    type = get_union_type([{0_i8, Arrow::Field.new("map", get_map_type(DT.int32), true)}])
    output = PVC.variant_get(input.inner, get_options(type)).as(Arrow::UnionArray)
    get_ints(output.child(0_i8).as(Arrow::MapArray).values).should eq [1]
  end

  it "handles unmatched and empty unions with the upstream safe/strict errors" do
    type = get_union_type([{0_i8, Arrow::Field.new("str", DT.utf8, false)}])
    input = PVC::VariantArray.from_iter([Variant.from("kept"), Variant.from(1_i8)] of Variant)
    output = PVC.variant_get(input.inner, get_options(type)).as(Arrow::UnionArray)
    output.type_ids.to_a.should eq [0_i8, 0_i8]
    get_null?(output.child(0_i8), 1).should be_true
    expect_raises(ParquetVariant::CastError, "no field can represent it") do
      PVC.variant_get(input.inner, get_options(type, safe: false))
    end
    expect_raises(ParquetVariant::InvalidArgumentError, "at least one union field") do
      PVC.variant_get(input.inner, get_options(DT.union(Arrow::UnionFields.empty, Arrow::UnionMode::Dense)))
    end
  end
end

describe "variant_get materialization errors" do
  it "nulls invalid value/typed_value conflicts in safe mode and propagates them in strict mode" do
    input = PVC::VariantArray.from_iter([Variant.from(1_i32)])
    conflicting = PVC::VariantArray.from_parts(input.metadata_column, input.value_column,
      Arrow::PrimitiveArray(Int32).from_values([2]), nil)
    output = PVC.variant_get(conflicting.inner, get_options(DT.int64))
    output.null_count.should eq 1
    expect_raises(Parquet::ArrowError, "Invalid variant, conflicting value and typed_value") do
      PVC.variant_get(conflicting.inner, get_options(DT.int64, safe: false))
    end
  end
end

describe "variant_get extension validation" do
  it "treats an invalid Variant extension tag as a concrete type request (Field::has_valid_extension_type)" do
    field = Arrow::Field.new("result", DT.int32, true)
      .with_metadata({"ARROW:extension:name" => "arrow.parquet.variant"})
    input = PVC::VariantArray.from_iter([Variant.from(42_i32)])
    get_ints(PVC.variant_get(input.inner, PVC::GetOptions.new.with_as_type(field))).should eq [42]
  end
end
