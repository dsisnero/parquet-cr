require "../spec_helper"

# Upstream parquet-variant-compute/src/variant_get.rs Decimal256 tests,
# apache/arrow-rs 60.0.0 (ef1fa157977633f0ba21aa921f9ef3d5c669235b).
private alias NC = ParquetVariantCompute
private alias NV = ParquetVariant::Variant
private alias ND = Arrow::DataType
private alias Wide = Arrow::I256

private def numeric_decimal(value : Int128, scale : UInt8) : NV
  NV.from(ParquetVariant::VariantDecimal16.try_new(value, scale))
end

private def numeric_get(values : Array(NV?), type : Arrow::DataType, safe : Bool = true) : Arrow::ArrayBase
  input = NC::VariantArray.from_iter(values)
  options = NC::GetOptions.new.with_as_type(Arrow::Field.new("result", type, true)).with_cast_options(Arrow::CastOptions.new(safe: safe))
  NC.variant_get(input.inner, options)
end

private def wide_values(array : Arrow::ArrayBase) : Array(Wide?)
  typed = array.as(Arrow::PrimitiveArray(Wide))
  Array(Wide?).new(typed.len) { |index| typed.nulls.try(&.null?(index)) ? nil : typed.value(index) }
end

describe "Variant Decimal256 extraction" do
  it "rescales source Decimal16 values to scale 2" do
    input = [numeric_decimal(1234_i128, 2), numeric_decimal(1234_i128, 3), numeric_decimal(1234_i128, 0), nil] of NV?
    result = numeric_get(input, ND.decimal256(76, 2))
    result.data_type.should eq ND.decimal256(76, 2)
    wide_values(result).should eq [Wide.new(1234), Wide.new(123), Wide.new(123400), nil]
  end

  it "rounds negative scales half away from zero" do
    input = [numeric_decimal(1235_i128, 0), numeric_decimal(1245_i128, 0), numeric_decimal(-1235_i128, 0), numeric_decimal(-1245_i128, 0), numeric_decimal(1235_i128, 2), numeric_decimal(1235_i128, 3), numeric_decimal(5235_i128, 3)] of NV?
    result = numeric_get(input, ND.decimal256(76, -1))
    result.data_type.should eq ND.decimal256(76, -1)
    wide_values(result).should eq [124, 125, -124, -125, 1, 0, 1].map { |value| Wide.new(value) }
  end

  it "returns null only for precision overflow in safe mode" do
    max = ParquetVariant::VariantDecimal16::MAX_UNSCALED_VALUE
    input = [numeric_decimal(max, 1), numeric_decimal(max, 0)] of NV?
    result = numeric_get(input, ND.decimal256(76, 39))
    factor = Wide.new(10).checked_pow(38) || raise "Invalid test factor"
    wide_values(result).should eq [Wide.new(max) * factor, nil]
  end

  it "raises the upstream Decimal256 error in strict mode" do
    max = ParquetVariant::VariantDecimal16::MAX_UNSCALED_VALUE
    error = expect_raises(ParquetVariant::CastError) do
      numeric_get([numeric_decimal(max, 1), numeric_decimal(max, 0)] of NV?, ND.decimal256(76, 39), false)
    end
    error.message.to_s.should contain "Failed to cast to Decimal256(precision=76, scale=39) from variant Decimal16"
  end
end

# arrow-cast/src/parse.rs: exponent, rounding, negative scale, whitespace and
# long-input fixtures, exercised through Variant conversions for every width.
describe "Variant decimal string parsing" do
  {% for config in [{32, 9}, {64, 18}, {128, 38}, {256, 76}] %}
    it "parses exponents and rounds Decimal{{config[0]}} half away from zero" do
      cases = [
        {"1e2", 0, 100}, {"1E2", 0, 100}, {"1e+02", 0, 100},
        {"1.5e2", 2, 15000}, {"1.5e-1", 1, 2}, {"15e-1", 0, 2},
        {"1e-2", 1, 0}, {"0e99999", 2, 0}, {"+00.0E+41", 12, 0},
        {"1.25e1", 0, 13}, {"1e-99999", 2, 0}, {"1.5e-400", 2, 0},
        {"123456789e-9", 9, 123456789}, {"0.000000001e9", 0, 1},
        {"5e-1", 0, 1}, {"4e-1", 0, 0}, {"-5e-1", 0, -1},
        {"1.234", 2, 123}, {"1.235", 2, 124}, {"1.2349999", 2, 123},
        {"-1.235", 2, -124}, {"-0.005", 2, -1}, {".5", 0, 1},
        {"-.5", 0, -1}, {"1.99", 1, 20}, {"1234.5", -2, 12},
        {"150", -2, 2}, {"-150", -2, -2}, {"149", -2, 1},
        {"49", -2, 0}, {"5.9", -1, 1}, {"1.5e5", -2, 1500},
        {".5e3", -2, 5}, {"000123456", -3, 123},
      ]
      cases.each do |text, scale, expected|
        result = NC::TypeConversion.unscaled_decimal{{config[0].id}}(NV.from(text), {{config[1]}}, scale)
        {% if config[0] == 256 %}
          result.should(eq(Wide.new(expected)), "#{text} at scale #{scale}")
        {% else %}
          result.should(eq(expected), "#{text} at scale #{scale}")
        {% end %}
      end
    end

    it "handles Decimal{{config[0]}} precision boundaries and malformed strings" do
      max = "9" * {{config[1]}}
      expected = {% if config[0] == 256 %} (Wide.from_string(max) || raise "Invalid test maximum") {% else %} max.to_i128 {% end %}
      NC::TypeConversion.unscaled_decimal{{config[0].id}}(NV.from(max), {{config[1]}}, 0).should eq expected
      NC::TypeConversion.unscaled_decimal{{config[0].id}}(NV.from("-#{max}"), {{config[1]}}, 0).should eq -expected
      ["1#{"0" * {{config[1]}}}", "#{max}.5", "1" * 300, "1e#{"9" * 300}"].each do |text|
        NC::TypeConversion.unscaled_decimal{{config[0].id}}(NV.from(text), {{config[1]}}, 0).should be_nil
      end
      ["", " ", ".", "+", "-", "e", "1e", "1e+", "1e2e3", "1.2.3", "NaN", "Infinity", "1 2", "\u{a0}1.5", "1.5\u{2003}"].each do |text|
        NC::TypeConversion.unscaled_decimal{{config[0].id}}(NV.from(text), {{config[1]}}, 1).should(be_nil, text)
      end
    end

    it "preserves Decimal{{config[0]}} rounding with long mantissas and huge exponents" do
      [" 1.5", "1.5 ", "\t1.5\n", "\r\n1.5\f"].each do |text|
        actual = NC::TypeConversion.unscaled_decimal{{config[0].id}}(NV.from(text), {{config[1]}}, 1)
        actual.should eq {% if config[0] == 256 %} Wide.new(15) {% else %} 15 {% end %}
      end
      tests = [
        {("1." + "9" * 300), 2, 200},
        {"4825037936439135476.2609835314269495255615E-14", 4, 482503794},
        {"+18232335063972188138031550982650807591758238.0724251287782783777442440E-58", 0, 0},
        {("0." + "0" * 200 + "1"), 10, 0}, {("1e-" + "9" * 300), 0, 0}, {("0e" + "9" * 300), 0, 0},
      ]
      tests.each do |text, scale, expected|
        actual = NC::TypeConversion.unscaled_decimal{{config[0].id}}(NV.from(text), {{config[1]}}, scale)
        actual.should eq {% if config[0] == 256 %} Wide.new(expected) {% else %} expected {% end %}
      end
    end
  {% end %}
end

describe "Variant decimal floating conversions" do
  it "accepts native-representable floats beyond 2^53 as upstream" do
    NC::TypeConversion.unscaled_decimal128(NV.from(1.0e20), 38, 0).should eq 100_000_000_000_000_000_000_i128
    NC::TypeConversion.unscaled_decimal64(NV.from(-1.0e16), 18, 0).should eq(-10_000_000_000_000_000_i64)
    NC::TypeConversion.unscaled_decimal256(NV.from(1.0e40), 76, 0).should eq Wide.from_f64(1.0e40)
    # single_float_to_decimal checks the native range, not the declared precision.
    NC::TypeConversion.unscaled_decimal32(NV.from(123.0), 2, 0).should eq 123
  end

  it "rejects non-finite floats and native overflow" do
    [Float64::NAN, Float64::INFINITY, -Float64::INFINITY, Float64::MAX].each do |value|
      NC::TypeConversion.unscaled_decimal32(NV.from(value), 9, 0).should be_nil
      NC::TypeConversion.unscaled_decimal128(NV.from(value), 38, 0).should be_nil
      NC::TypeConversion.unscaled_decimal256(NV.from(value), 76, 0).should be_nil
    end
    NC::TypeConversion.unscaled_decimal32(NV.from(-2.5), 9, 0).should eq(-3)
  end
end

describe "Variant numeric kernel integration" do
  [Arrow::UnionMode::Dense, Arrow::UnionMode::Sparse].each do |mode|
    it "selects Decimal256 union children with precision-aware ranking in #{mode}" do
      value = numeric_decimal(1234_i128, 2)
      NC::VariantToArrow.union_child_rank(value, ND.decimal256(76, 2)).should eq 1
      NC::VariantToArrow.union_child_rank(value, ND.decimal256(2, 2)).should be_nil
      fields = Arrow::UnionFields.new([
        {5_i8, Arrow::Field.new("small", ND.decimal128(2, 2), true)},
        {9_i8, Arrow::Field.new("wide", ND.decimal256(76, 2), true)},
        {12_i8, Arrow::Field.new("null", ND.null, true)},
      ])
      output = numeric_get([value, nil, NV::Null.new] of NV?, ND.union(fields, mode)).as(Arrow::UnionArray)
      output.type_id(0).should eq 9_i8
      output.child(9_i8).as(Arrow::PrimitiveArray(Wide)).value(output.value_offset(0)).should eq Wide.new(1234)
      output.type_id(1).should eq 12_i8
      output.type_id(2).should eq 12_i8
    end
  end

  it "keeps natural decimal widths ahead of wider union children" do
    value = NV.from(ParquetVariant::VariantDecimal4.try_new(1234, 2))
    [ND.decimal32(9, 2), ND.decimal64(18, 2), ND.decimal128(38, 2), ND.decimal256(76, 2)].each_with_index do |type, rank|
      NC::VariantToArrow.union_child_rank(value, type).should eq rank
    end
    smaller = NV.from(ParquetVariant::VariantDecimal8.try_new(1234_i64, 2))
    NC::VariantToArrow.union_child_rank(smaller, ND.decimal256(76, 2)).should eq 2
  end

  it "extracts exponent strings at nested paths with exact wide digits" do
    input = NC::VariantArrayBuilder.new
    input.append_json(%({"object":{"value":"1.23456789012345678901234567890123456789e39"}}))
    input.append_json(%({"object":{"value":"-1.235e-1"}}))
    input.append_json(%({"object":{"value":"bad"}}))
    input.append_json(%({"object":{}}))
    input.append_null
    options = NC::GetOptions.new_with_path(ParquetVariant::VariantPath.parse("object.value"))
      .with_as_type(Arrow::Field.new("result", ND.decimal256(76, 2), true))
    output = NC.variant_get(input.build.inner, options)
    expected = Wide.from_string("123456789012345678901234567890123456789000") || raise "Invalid test value"
    wide_values(output).should eq [expected, Wide.new(-12), nil, nil, nil]
  end

  it "builds nested lists of Decimal256 and distinguishes SQL/Variant nulls from strict errors" do
    input = NC::VariantArrayBuilder.new
    input.append_json(%(["1.25e1",null,"-1.25e1"]))
    input.append_null
    input.append_json("null")
    input.append_json("[]")
    options = NC::GetOptions.new.with_as_type(Arrow::Field.new("result", ND.list(Arrow::Field.new("item", ND.decimal256(76, 1), true)), true))
    output = NC.variant_get(input.build.inner, options).as(Arrow::GenericListArray(Int32))
    wide_values(output.values).should eq [Wide.new(125), nil, Wide.new(-125)]
    output.value_offsets.to_a.should eq [0, 3, 3, 3, 3]
    output.nulls.try(&.null?(1)).should be_true
    output.nulls.try(&.null?(2)).should be_true
    output.nulls.try(&.null?(3)).should_not be_true
    values = [NV.from("1e99"), nil, NV::Null.new] of NV?
    wide_values(numeric_get(values, ND.decimal256(76, 2))).should eq [nil, nil, nil]
    expect_raises(ParquetVariant::CastError, "Failed to cast to Decimal256") { numeric_get(values, ND.decimal256(76, 2), false) }
    wide_values(numeric_get([nil, NV::Null.new] of NV?, ND.decimal256(76, 2), false)).should eq [nil, nil]
  end

  it "encodes Decimal256 dictionaries through the shared row builder" do
    values = [numeric_decimal(1234_i128, 2), numeric_decimal(1234_i128, 2), nil, numeric_decimal(5678_i128, 2)] of NV?
    decimal = ND.decimal256(76, 2)
    dictionary = numeric_get(values, ND.dictionary(ND.int32, decimal)).as(Arrow::DictionaryArray(Int32))
    wide_values(dictionary.values).should eq [Wide.new(1234), Wide.new(5678)]
    dictionary.keys.to_a.should eq [0, 0, nil, 1]
    dictionary.keys.nulls.try(&.null?(2)).should be_true
  end

  it "encodes Decimal256 run-end arrays through the shared row builder" do
    values = [numeric_decimal(1234_i128, 2), numeric_decimal(1234_i128, 2), nil, numeric_decimal(5678_i128, 2)] of NV?
    decimal = ND.decimal256(76, 2)
    type = ND.run_end_encoded(Arrow::Field.new("run_ends", ND.int32, false), Arrow::Field.new("values", decimal, true))
    run = numeric_get(values, type).as(Arrow::RunArray(Int32))
    run.run_ends.values.to_a.should eq [2, 3, 4]
    actual = wide_values(run.values)
    actual.should eq [Wide.new(1234), nil, Wide.new(5678)]
    (0...run.len).map { |index| actual[run.get_physical_index(index)] }.should eq [Wide.new(1234), Wide.new(1234), nil, Wide.new(5678)]
  end

  it "round trips representable Decimal256 values through the public Arrow-to-Variant cast" do
    input = numeric_get([numeric_decimal(-1234_i128, 2), nil, numeric_decimal(5678_i128, 2)] of NV?, ND.decimal256(38, 2))
    output = NC.cast_to_variant(input)
    output.is_null(1).should be_true
    output.value(0).should eq numeric_decimal(-1234_i128, 2)
    output.value(2).should eq numeric_decimal(5678_i128, 2)
  end

  it "never shreds into Decimal256, matching the explicit upstream implementation" do
    value = numeric_decimal(1234_i128, 2)
    NC::TypeConversion.shred_unscaled_decimal256(value, 76, 2).should be_nil
    NC::TypeConversion.shred_unscaled_decimal256(NV.from(123_i64), 76, 0).should be_nil
    expect_raises(ParquetVariant::InvalidArgumentError, "is not a valid variant shredding type") do
      NC.shred_variant(NC::VariantArray.from_iter([value]), ND.decimal256(38, 2))
    end
    # Ordinary shredding still requires exact decimal rescaling.
    input = NC::VariantArray.from_iter([numeric_decimal(150_i128, 2), numeric_decimal(155_i128, 2)])
    shredded = NC.shred_variant(input, ND.decimal32(9, 1))
    typed = shredded.typed_value_column || raise "Missing typed column"
    typed = typed.as(Arrow::PrimitiveArray(Int32))
    typed.value(0).should eq 15
    typed.nulls.try(&.null?(1)).should be_true
    restored = NC.unshred_variant(shredded)
    restored.value(0).should eq NV::Decimal4.new(ParquetVariant::VariantDecimal4.new(15, 1))
    restored.value(1).should eq numeric_decimal(155_i128, 2)
  end

  it "validates decimal target precision/scale before appending values" do
    [ND.decimal32(10, 0), ND.decimal64(19, 0), ND.decimal128(39, 0), ND.decimal256(77, 0), ND.decimal256(0, 0), ND.decimal256(76, 77)].each do |target|
      expect_raises(Arrow::ArrowError) { numeric_get([NV.from(1_i32)] of NV?, target) }
    end
  end
end
