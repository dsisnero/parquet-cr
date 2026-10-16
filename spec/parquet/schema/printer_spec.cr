require "../../spec_helper"

# Ported from `parquet/src/schema/printer.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: `Printer::new(&mut String)` maps to `Printer.new(String::Builder)`;
# Rust unit logical types (`LogicalType::String`) become instances
# (`LogicalType::String.new`); `fmt::Write`/`io::Write` are `IO`.

private alias ST = Parquet::Schema::Type
private alias PType = Parquet::Type
private alias Rep = Parquet::Repetition
private alias Conv = Parquet::ConvertedType
private alias LT = Parquet::LogicalType
private alias TU = Parquet::TimeUnit
private alias Printer = Parquet::Schema::Printer

private def print_type(tp : Parquet::Schema::Type) : String
  builder = String::Builder.new
  Printer.new(builder).print(tp)
  builder.to_s
end

private def build_primitive_type(
  name : String,
  id : Int32?,
  physical_type : PType,
  logical_type : LT?,
  converted_type : Conv,
  repetition : Rep,
) : Parquet::Schema::Type
  ST.primitive_type_builder(name, physical_type)
    .with_id(id)
    .with_repetition(repetition)
    .with_logical_type(logical_type)
    .with_converted_type(converted_type)
    .build
end

private def decimal_length_from_precision(precision : Int32) : Int32
  max_val = 10.0 ** precision - 1.0
  bits_unsigned = Math.log2(max_val).ceil
  bits_signed = bits_unsigned + 1.0
  (bits_signed / 8.0).ceil.to_i32
end

private def assert_print_parse_message(message : Parquet::Schema::Type) : Nil
  parsed = Parquet::Schema.parse_message_type(print_type(message))
  parsed.should eq(message)
end

describe Parquet::Schema::Printer do
  # Upstream `test_print_primitive_type`.
  it "prints primitive types" do
    cases = {
      ST.primitive_type_builder("field", PType::INT32)
        .with_repetition(Rep::REQUIRED).with_converted_type(Conv::INT_32).build => "REQUIRED INT32 field (INT_32);",
      ST.primitive_type_builder("field", PType::INT32)
        .with_repetition(Rep::REQUIRED).with_converted_type(Conv::INT_32).with_id(42).build => "REQUIRED INT32 field [42] (INT_32);",
      ST.primitive_type_builder("field", PType::INT32)
        .with_repetition(Rep::REQUIRED).build => "REQUIRED INT32 field;",
      ST.primitive_type_builder("field", PType::INT32)
        .with_repetition(Rep::REQUIRED).with_id(42).build => "REQUIRED INT32 field [42];",
    }
    cases.each do |field, expected|
      print_type(field).should eq(expected)
    end
  end

  # Upstream `test_print_logical_types`.
  it "prints logical types" do
    cases = {
      build_primitive_type("field", nil, PType::INT32, LT.integer(32, true), Conv::NONE, Rep::REQUIRED)                                                                 => "REQUIRED INT32 field (INTEGER(32,true));",
      build_primitive_type("field", nil, PType::INT32, LT.integer(8, false), Conv::NONE, Rep::OPTIONAL)                                                                 => "OPTIONAL INT32 field (INTEGER(8,false));",
      build_primitive_type("field", nil, PType::INT32, LT.integer(16, true), Conv::INT_16, Rep::REPEATED)                                                               => "REPEATED INT32 field (INTEGER(16,true));",
      build_primitive_type("field", 42, PType::INT32, LT.integer(16, true), Conv::INT_16, Rep::REPEATED)                                                                => "REPEATED INT32 field [42] (INTEGER(16,true));",
      build_primitive_type("field", nil, PType::INT64, nil, Conv::NONE, Rep::REPEATED)                                                                                  => "REPEATED INT64 field;",
      build_primitive_type("field", nil, PType::FLOAT, nil, Conv::NONE, Rep::REQUIRED)                                                                                  => "REQUIRED FLOAT field;",
      build_primitive_type("booleans", nil, PType::BOOLEAN, nil, Conv::NONE, Rep::OPTIONAL)                                                                             => "OPTIONAL BOOLEAN booleans;",
      build_primitive_type("booleans", 42, PType::BOOLEAN, nil, Conv::NONE, Rep::OPTIONAL)                                                                              => "OPTIONAL BOOLEAN booleans [42];",
      build_primitive_type("field", nil, PType::INT64, LT.timestamp(true, TU::MILLIS), Conv::NONE, Rep::REQUIRED)                                                       => "REQUIRED INT64 field (TIMESTAMP(MILLIS,true));",
      build_primitive_type("field", nil, PType::INT32, LT::Date.new, Conv::NONE, Rep::OPTIONAL)                                                                         => "OPTIONAL INT32 field (DATE);",
      build_primitive_type("field", nil, PType::INT32, LT.time(false, TU::MILLIS), Conv::TIME_MILLIS, Rep::REQUIRED)                                                    => "REQUIRED INT32 field (TIME(MILLIS,false));",
      build_primitive_type("field", 42, PType::INT32, LT.time(false, TU::MILLIS), Conv::TIME_MILLIS, Rep::REQUIRED)                                                     => "REQUIRED INT32 field [42] (TIME(MILLIS,false));",
      build_primitive_type("field", nil, PType::BYTE_ARRAY, nil, Conv::NONE, Rep::REQUIRED)                                                                             => "REQUIRED BYTE_ARRAY field;",
      build_primitive_type("field", 42, PType::BYTE_ARRAY, nil, Conv::NONE, Rep::REQUIRED)                                                                              => "REQUIRED BYTE_ARRAY field [42];",
      build_primitive_type("field", nil, PType::BYTE_ARRAY, nil, Conv::UTF8, Rep::REQUIRED)                                                                             => "REQUIRED BYTE_ARRAY field (UTF8);",
      build_primitive_type("field", nil, PType::BYTE_ARRAY, LT::Json.new, Conv::JSON, Rep::REQUIRED)                                                                    => "REQUIRED BYTE_ARRAY field (JSON);",
      build_primitive_type("field", nil, PType::BYTE_ARRAY, LT::Bson.new, Conv::BSON, Rep::REQUIRED)                                                                    => "REQUIRED BYTE_ARRAY field (BSON);",
      build_primitive_type("field", nil, PType::BYTE_ARRAY, LT::String.new, Conv::NONE, Rep::REQUIRED)                                                                  => "REQUIRED BYTE_ARRAY field (STRING);",
      build_primitive_type("field", 42, PType::BYTE_ARRAY, LT::String.new, Conv::NONE, Rep::REQUIRED)                                                                   => "REQUIRED BYTE_ARRAY field [42] (STRING);",
      build_primitive_type("field", nil, PType::BYTE_ARRAY, LT.geometry(nil), Conv::NONE, Rep::REQUIRED)                                                                => "REQUIRED BYTE_ARRAY field (GEOMETRY);",
      build_primitive_type("field", nil, PType::BYTE_ARRAY, LT.geometry("non-missing CRS"), Conv::NONE, Rep::REQUIRED)                                                  => "REQUIRED BYTE_ARRAY field (GEOMETRY(non-missing CRS));",
      build_primitive_type("field", nil, PType::BYTE_ARRAY, LT.geography(nil, Parquet::EdgeInterpolationAlgorithm::SPHERICAL), Conv::NONE, Rep::REQUIRED)               => "REQUIRED BYTE_ARRAY field (GEOGRAPHY(SPHERICAL));",
      build_primitive_type("field", nil, PType::BYTE_ARRAY, LT.geography("non-missing CRS", Parquet::EdgeInterpolationAlgorithm::SPHERICAL), Conv::NONE, Rep::REQUIRED) => "REQUIRED BYTE_ARRAY field (GEOGRAPHY(SPHERICAL, non-missing CRS));",
    }
    cases.each do |field, expected|
      print_type(field).should eq(expected)
    end
  end

  # Upstream `test_print_flba_logical_types`.
  it "prints fixed-length byte array logical types" do
    cases = {
      ST.primitive_type_builder("field", PType::FIXED_LEN_BYTE_ARRAY)
        .with_logical_type(nil).with_converted_type(Conv::INTERVAL).with_length(12).with_repetition(Rep::REQUIRED).build => "REQUIRED FIXED_LEN_BYTE_ARRAY (12) field (INTERVAL);",
      ST.primitive_type_builder("field", PType::FIXED_LEN_BYTE_ARRAY)
        .with_logical_type(LT::Uuid.new).with_length(16).with_repetition(Rep::REQUIRED).build => "REQUIRED FIXED_LEN_BYTE_ARRAY (16) field (UUID);",
      ST.primitive_type_builder("decimal", PType::FIXED_LEN_BYTE_ARRAY)
        .with_logical_type(LT.decimal(20, 32)).with_precision(32).with_scale(20)
        .with_length(decimal_length_from_precision(32)).with_repetition(Rep::REPEATED).build => "REPEATED FIXED_LEN_BYTE_ARRAY (14) decimal (DECIMAL(32,20));",
      ST.primitive_type_builder("decimal", PType::FIXED_LEN_BYTE_ARRAY)
        .with_converted_type(Conv::DECIMAL).with_precision(19).with_scale(4)
        .with_length(decimal_length_from_precision(19)).with_repetition(Rep::OPTIONAL).build => "OPTIONAL FIXED_LEN_BYTE_ARRAY (9) decimal (DECIMAL(19,4));",
      ST.primitive_type_builder("float16", PType::FIXED_LEN_BYTE_ARRAY)
        .with_logical_type(LT::Float16.new).with_length(2).with_repetition(Rep::REQUIRED).build => "REQUIRED FIXED_LEN_BYTE_ARRAY (2) float16 (FLOAT16);",
    }
    cases.each do |field, expected|
      print_type(field).should eq(expected)
    end
  end

  # Upstream `test_print_schema_documentation`.
  it "prints a schema" do
    field_a = ST.primitive_type_builder("a", PType::BYTE_ARRAY)
      .with_id(42).with_converted_type(Conv::UTF8).build
    field_b = ST.primitive_type_builder("b", PType::INT32).with_repetition(Rep::REQUIRED).build
    field_d = ST.primitive_type_builder("d", PType::INT64).with_id(99).build
    field_c = ST.group_type_builder("c").with_id(43).with_fields([field_d] of Parquet::Schema::Type).build
    schema = ST.group_type_builder("schema")
      .with_fields([field_a, field_b, field_c] of Parquet::Schema::Type).build

    expected = "message schema {\n" \
               "  OPTIONAL BYTE_ARRAY a [42] (UTF8);\n" \
               "  REQUIRED INT32 b;\n" \
               "  message c [43] {\n" \
               "    OPTIONAL INT64 d [99];\n" \
               "  }\n" \
               "}"
    print_type(schema).should eq(expected)
  end

  # Upstream `test_print_group_type`.
  it "prints a group type" do
    f1 = ST.primitive_type_builder("f1", PType::INT32).with_repetition(Rep::REQUIRED).with_converted_type(Conv::INT_32).build
    f2 = ST.primitive_type_builder("f2", PType::BYTE_ARRAY).with_converted_type(Conv::UTF8).build
    f3 = ST.primitive_type_builder("f3", PType::BYTE_ARRAY).with_logical_type(LT::String.new).build
    f4 = ST.primitive_type_builder("f4", PType::FIXED_LEN_BYTE_ARRAY).with_repetition(Rep::REPEATED).with_converted_type(Conv::INTERVAL).with_length(12).build
    field = ST.group_type_builder("field").with_repetition(Rep::OPTIONAL).with_fields([f1, f2, f3] of Parquet::Schema::Type).build
    message = ST.group_type_builder("schema").with_fields([field, f4] of Parquet::Schema::Type).build

    expected = "message schema {\n" \
               "  OPTIONAL group field {\n" \
               "    REQUIRED INT32 f1 (INT_32);\n" \
               "    OPTIONAL BYTE_ARRAY f2 (UTF8);\n" \
               "    OPTIONAL BYTE_ARRAY f3 (STRING);\n" \
               "  }\n" \
               "  REPEATED FIXED_LEN_BYTE_ARRAY (12) f4 (INTERVAL);\n" \
               "}"
    print_type(message).should eq(expected)
  end

  # Upstream `test_print_group_type_with_ids`.
  it "prints a group type with field ids" do
    f1 = ST.primitive_type_builder("f1", PType::INT32).with_repetition(Rep::REQUIRED).with_converted_type(Conv::INT_32).with_id(0).build
    f2 = ST.primitive_type_builder("f2", PType::BYTE_ARRAY).with_converted_type(Conv::UTF8).with_id(1).build
    f3 = ST.primitive_type_builder("f3", PType::BYTE_ARRAY).with_logical_type(LT::String.new).with_id(1).build
    f4 = ST.primitive_type_builder("f4", PType::FIXED_LEN_BYTE_ARRAY).with_repetition(Rep::REPEATED).with_converted_type(Conv::INTERVAL).with_length(12).with_id(2).build
    field = ST.group_type_builder("field").with_repetition(Rep::OPTIONAL).with_fields([f1, f2, f3] of Parquet::Schema::Type).with_id(1).build
    message = ST.group_type_builder("schema").with_fields([field, f4] of Parquet::Schema::Type).with_id(2).build

    expected = "message schema [2] {\n" \
               "  OPTIONAL group field [1] {\n" \
               "    REQUIRED INT32 f1 [0] (INT_32);\n" \
               "    OPTIONAL BYTE_ARRAY f2 [1] (UTF8);\n" \
               "    OPTIONAL BYTE_ARRAY f3 [1] (STRING);\n" \
               "  }\n" \
               "  REPEATED FIXED_LEN_BYTE_ARRAY (12) f4 [2] (INTERVAL);\n" \
               "}"
    print_type(message).should eq(expected)
  end

  # Upstream `test_print_and_parse_primitive`.
  it "round-trips a primitive schema through parse" do
    a2 = ST.primitive_type_builder("a2", PType::BYTE_ARRAY).with_repetition(Rep::REPEATED).with_converted_type(Conv::UTF8).build
    a1 = ST.group_type_builder("a1").with_repetition(Rep::OPTIONAL).with_logical_type(LT::List.new).with_converted_type(Conv::LIST).with_fields([a2] of Parquet::Schema::Type).build
    b3 = ST.primitive_type_builder("b3", PType::INT32).with_repetition(Rep::OPTIONAL).build
    b4 = ST.primitive_type_builder("b4", PType::DOUBLE).with_repetition(Rep::OPTIONAL).build
    b2 = ST.group_type_builder("b2").with_repetition(Rep::REPEATED).with_converted_type(Conv::NONE).with_fields([b3, b4] of Parquet::Schema::Type).build
    b1 = ST.group_type_builder("b1").with_repetition(Rep::OPTIONAL).with_logical_type(LT::List.new).with_converted_type(Conv::LIST).with_fields([b2] of Parquet::Schema::Type).build
    a0 = ST.group_type_builder("a0").with_repetition(Rep::REQUIRED).with_fields([a1, b1] of Parquet::Schema::Type).build
    message = ST.group_type_builder("root").with_fields([a0] of Parquet::Schema::Type).build

    assert_print_parse_message(message)
  end

  # Upstream `test_print_and_parse_nested`.
  it "round-trips a nested schema through parse" do
    f1 = ST.primitive_type_builder("f1", PType::INT32).with_repetition(Rep::REQUIRED).with_converted_type(Conv::INT_32).build
    f2 = ST.primitive_type_builder("f2", PType::BYTE_ARRAY).with_repetition(Rep::OPTIONAL).with_converted_type(Conv::UTF8).build
    field = ST.group_type_builder("field").with_repetition(Rep::OPTIONAL).with_fields([f1, f2] of Parquet::Schema::Type).build
    f3 = ST.primitive_type_builder("f3", PType::FIXED_LEN_BYTE_ARRAY).with_repetition(Rep::REPEATED).with_converted_type(Conv::INTERVAL).with_length(12).build
    message = ST.group_type_builder("schema").with_fields([field, f3] of Parquet::Schema::Type).build

    assert_print_parse_message(message)
  end

  # Upstream `test_print_and_parse_decimal`.
  it "round-trips a decimal schema through parse" do
    f1 = ST.primitive_type_builder("f1", PType::INT32).with_repetition(Rep::OPTIONAL).with_logical_type(LT.decimal(2, 9)).with_converted_type(Conv::DECIMAL).with_precision(9).with_scale(2).build
    f2 = ST.primitive_type_builder("f2", PType::INT32).with_repetition(Rep::OPTIONAL).with_logical_type(LT.decimal(0, 9)).with_converted_type(Conv::DECIMAL).with_precision(9).with_scale(0).build
    message = ST.group_type_builder("schema").with_fields([f1, f2] of Parquet::Schema::Type).build

    assert_print_parse_message(message)
  end

  # Upstream `test_print_file_type`.
  it "prints a file logical type" do
    uri_field = ST.primitive_type_builder("uri", PType::BYTE_ARRAY).with_repetition(Rep::OPTIONAL).with_logical_type(LT::String.new).build
    size_field = ST.primitive_type_builder("size", PType::INT64).with_repetition(Rep::OPTIONAL).build
    file_field = ST.group_type_builder("f").with_repetition(Rep::REQUIRED).with_logical_type(LT::File.new).with_fields([uri_field, size_field] of Parquet::Schema::Type).build
    message = ST.group_type_builder("schema").with_fields([file_field] of Parquet::Schema::Type).build

    expected = "message schema {\n" \
               "  REQUIRED group f (FILE) {\n" \
               "    OPTIONAL BYTE_ARRAY uri (STRING);\n" \
               "    OPTIONAL INT64 size;\n" \
               "  }\n" \
               "}"
    print_type(message).should eq(expected)
  end
end
