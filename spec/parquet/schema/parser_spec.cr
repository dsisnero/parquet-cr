require "../../spec_helper"

# Ported from `parquet/src/schema/parser.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Reuses the schema aliases (ST, PPhys, PLT, PCT, PRep, PTU) defined in
# `types_spec.cr`.

private def parse_message(schema : String) : ST
  Parquet::Schema.parse_message_type(schema)
end

describe Parquet::Schema::Parser do
  # Upstream `test_tokenize_*`.
  it "tokenizes schema strings" do
    STokenizer.from_str("").next.should be_nil

    iter = STokenizer.from_str(",;{}()=")
    [",", ";", "{", "}", "(", ")", "="].each { |t| iter.next.should eq(t) }
    iter.next.should be_nil

    iter = STokenizer.from_str(" , ; { } ( ) = ")
    [",", ";", "{", "}", "(", ")", "="].each { |t| iter.next.should eq(t) }
    iter.next.should be_nil

    iter = STokenizer.from_str("abc def ghi jkl mno")
    %w[abc def ghi jkl mno].each { |t| iter.next.should eq(t) }
    iter.next.should be_nil

    iter = STokenizer.from_str("abc;")
    iter.next.should eq("abc")
    iter.next.should eq(";")
    iter.backtrack
    iter.next.should eq(";")
    iter.next.should be_nil
  end

  # Upstream `test_tokenize_message_type`.
  it "tokenizes a full message type" do
    schema = "
      message schema {
        required int32 a;
        optional binary c (UTF8);
        required group d {
          required int32 a;
          optional binary c (UTF8);
        }
        required group e (LIST) {
          repeated group list {
            required int32 element;
          }
        }
      }
    "
    iter = STokenizer.from_str(schema)
    tokens = [] of String
    while t = iter.next
      tokens << t
    end
    tokens.should eq([
      "message", "schema", "{", "required", "int32", "a", ";", "optional", "binary", "c",
      "(", "UTF8", ")", ";", "required", "group", "d", "{", "required", "int32", "a",
      ";", "optional", "binary", "c", "(", "UTF8", ")", ";", "}", "required", "group",
      "e", "(", "LIST", ")", "{", "repeated", "group", "list", "{", "required", "int32",
      "element", ";", "}", "}", "}",
    ])
  end

  # Upstream `test_assert_token`.
  it "asserts tokens" do
    Parquet::Schema.assert_token("a", "a")
    expect_raises(Parquet::ParquetError) { Parquet::Schema.assert_token("a", "b") }
    expect_raises(Parquet::ParquetError) { Parquet::Schema.assert_token(nil, "b") }
  end

  # Upstream `test_parse_message_type_invalid`.
  it "rejects a message type not starting with 'message'" do
    expect_raises(Parquet::ParquetError, "Message type does not start with 'message'") do
      parse_message("test")
    end
  end

  # Upstream `test_parse_message_type_no_name`.
  it "rejects a message type without a name" do
    expect_raises(Parquet::ParquetError, "Expected name, found None") do
      parse_message("message")
    end
  end

  # Upstream `test_parse_message_type_fixed_byte_array`.
  it "parses FIXED_LEN_BYTE_ARRAY with a length" do
    expect_raises(Parquet::ParquetError, "Expected '(', found token 'col'") do
      parse_message("message schema { REQUIRED FIXED_LEN_BYTE_ARRAY col; }")
    end
    parse_message("message schema { REQUIRED FIXED_LEN_BYTE_ARRAY(16) col; }")
      .should be_a(Parquet::Schema::GroupType)
  end

  # Upstream `test_parse_message_type_integer`.
  it "parses INTEGER logical types and rejects invalid ones" do
    expect_raises(Parquet::ParquetError, "Failed to parse bit_width for INTEGER type") do
      parse_message("message root { optional int64 f1 (INTEGER()); }")
    end
    expect_raises(Parquet::ParquetError, "Incorrect bit width 32 for INT64") do
      parse_message("message root { optional int64 f1 (INTEGER(32,)); }")
    end
    expect_raises(Parquet::ParquetError, "Failed to parse bit_width for INTEGER type") do
      parse_message("message root { optional int32 f1 (INTEGER(eight,true)); }")
    end
    parse_message(<<-SCHEMA).should be_a(Parquet::Schema::GroupType)
      message root {
        optional int32 f1 (INTEGER(8,false));
        optional int32 f2 (INTEGER(8,true));
        optional int32 f3 (INTEGER(16,false));
        optional int32 f4 (INTEGER(16,true));
        optional int32 f5 (INTEGER(32,false));
        optional int32 f6 (INTEGER(32,true));
        optional int64 f7 (INTEGER(64,false));
        optional int64 f8 (INTEGER(64,true));
      }
      SCHEMA
  end

  # Upstream `test_parse_message_type_temporal`.
  it "parses temporal logical types and rejects invalid ones" do
    expect_raises(Parquet::ParquetError, "Failed to parse timeunit for TIMESTAMP type") do
      parse_message("message root { optional int64 f1 (TIMESTAMP(); }")
    end
    expect_raises(Parquet::ParquetError, "Failed to parse timezone info for TIMESTAMP type") do
      parse_message("message root { optional int64 f1 (TIMESTAMP(MILLIS,)); }")
    end
    expect_raises(Parquet::ParquetError, "Failed to parse timeunit for TIMESTAMP type") do
      parse_message("message root { optional int64 f1 (TIMESTAMP(YOCTOS,)); }")
    end
    parse_message(<<-SCHEMA).should be_a(Parquet::Schema::GroupType)
      message root {
        optional int32 f1 (DATE);
        optional int32 f2 (TIME(MILLIS,true));
        optional int64 f3 (TIME(MICROS,false));
        optional int64 f4 (TIME(NANOS,true));
        optional int64 f5 (TIMESTAMP(MILLIS,true));
        optional int64 f6 (TIMESTAMP(MICROS,true));
        optional int64 f7 (TIMESTAMP(NANOS,false));
      }
      SCHEMA
  end

  # Upstream `test_parse_message_type_decimal`.
  it "parses DECIMAL logical types and rejects invalid ones" do
    expect_raises(Parquet::ParquetError, "Failed to parse precision for DECIMAL type") do
      parse_message("message root { optional int32 f1 (DECIMAL(); }")
    end
    expect_raises(Parquet::ParquetError, "Failed to parse precision for DECIMAL type") do
      parse_message("message root { optional int32 f1 (DECIMAL()); }")
    end
    expect_raises(Parquet::ParquetError, "Failed to parse scale for DECIMAL type") do
      parse_message("message root { optional int32 f1 (DECIMAL(8,)); }")
    end
    expect_raises(Parquet::ParquetError, "Expected ')', found token ';'") do
      parse_message("message root { optional int32 f3 (DECIMAL); }")
    end
    parse_message(<<-SCHEMA).should be_a(Parquet::Schema::GroupType)
      message root {
        optional int32 f1 (DECIMAL(8, 3));
        optional int32 f2 (DECIMAL(8));
      }
      SCHEMA
  end

  # Upstream `test_parse_message_type_compare_1`.
  it "parses decimals and float16 (compare 1)" do
    message = parse_message(<<-SCHEMA)
      message root {
        optional fixed_len_byte_array(5) f1 (DECIMAL(9, 3));
        optional fixed_len_byte_array (16) f2 (DECIMAL (38, 18));
        optional fixed_len_byte_array (2) f3 (FLOAT16);
      }
      SCHEMA

    expected = ST.group_type_builder("root")
      .with_fields([
        ST.primitive_type_builder("f1", PPhys::FIXED_LEN_BYTE_ARRAY)
          .with_logical_type(PLT.decimal(3, 9)).with_converted_type(PCT::DECIMAL)
          .with_length(5).with_precision(9).with_scale(3).build.as(ST),
        ST.primitive_type_builder("f2", PPhys::FIXED_LEN_BYTE_ARRAY)
          .with_logical_type(PLT.decimal(18, 38)).with_converted_type(PCT::DECIMAL)
          .with_length(16).with_precision(38).with_scale(18).build.as(ST),
        ST.primitive_type_builder("f3", PPhys::FIXED_LEN_BYTE_ARRAY)
          .with_logical_type(PLT::Float16.new).with_length(2).build.as(ST),
      ])
      .build.as(ST)

    message.should eq(expected)
  end

  # Upstream `test_parse_message_type_compare_2`.
  it "parses nested groups and lists (compare 2)" do
    message = parse_message(<<-SCHEMA)
      message root {
        required group a0 {
          optional group a1 (LIST) {
            repeated binary a2 (UTF8);
          }
          optional group b1 (LIST) {
            repeated group b2 {
              optional int32 b3;
              optional double b4;
            }
          }
        }
      }
      SCHEMA

    expected = ST.group_type_builder("root")
      .with_fields([
        ST.group_type_builder("a0")
          .with_repetition(PRep::REQUIRED)
          .with_fields([
            ST.group_type_builder("a1")
              .with_repetition(PRep::OPTIONAL)
              .with_logical_type(PLT::List.new)
              .with_converted_type(PCT::LIST)
              .with_fields([
                ST.primitive_type_builder("a2", PPhys::BYTE_ARRAY)
                  .with_repetition(PRep::REPEATED)
                  .with_converted_type(PCT::UTF8).build.as(ST),
              ]).build.as(ST),
            ST.group_type_builder("b1")
              .with_repetition(PRep::OPTIONAL)
              .with_logical_type(PLT::List.new)
              .with_converted_type(PCT::LIST)
              .with_fields([
                ST.group_type_builder("b2")
                  .with_repetition(PRep::REPEATED)
                  .with_fields([
                    ST.primitive_type_builder("b3", PPhys::INT32).build.as(ST),
                    ST.primitive_type_builder("b4", PPhys::DOUBLE).build.as(ST),
                  ]).build.as(ST),
              ]).build.as(ST),
          ]).build.as(ST),
      ])
      .build.as(ST)

    message.should eq(expected)
  end

  # Upstream `test_parse_message_type_compare_3`.
  it "parses primitive fields (compare 3)" do
    message = parse_message(<<-SCHEMA)
      message root {
        required int32 _1 (INT_8);
        required int32 _2 (INT_16);
        required float _3;
        required double _4;
        optional int32 _5 (DATE);
        optional binary _6 (UTF8);
      }
      SCHEMA

    fields = [
      ST.primitive_type_builder("_1", PPhys::INT32)
        .with_repetition(PRep::REQUIRED).with_converted_type(PCT::INT_8).build.as(ST),
      ST.primitive_type_builder("_2", PPhys::INT32)
        .with_repetition(PRep::REQUIRED).with_converted_type(PCT::INT_16).build.as(ST),
      ST.primitive_type_builder("_3", PPhys::FLOAT)
        .with_repetition(PRep::REQUIRED).build.as(ST),
      ST.primitive_type_builder("_4", PPhys::DOUBLE)
        .with_repetition(PRep::REQUIRED).build.as(ST),
      ST.primitive_type_builder("_5", PPhys::INT32)
        .with_logical_type(PLT::Date.new).with_converted_type(PCT::DATE).build.as(ST),
      ST.primitive_type_builder("_6", PPhys::BYTE_ARRAY)
        .with_converted_type(PCT::UTF8).build.as(ST),
    ]

    expected = ST.group_type_builder("root").with_fields(fields).build.as(ST)
    message.should eq(expected)
  end

  # Upstream `test_parse_message_type_compare_4`.
  it "parses integer/time/timestamp/string logical types (compare 4)" do
    message = parse_message(<<-SCHEMA)
      message root {
        required int32 _1 (INTEGER(8,true));
        required int32 _2 (INTEGER(16,false));
        required float _3;
        required double _4;
        optional int32 _5 (DATE);
        optional int32 _6 (TIME(MILLIS,false));
        optional int64 _7 (TIME(MICROS,true));
        optional int64 _8 (TIMESTAMP(MILLIS,true));
        optional int64 _9 (TIMESTAMP(NANOS,false));
        optional binary _10 (STRING);
      }
      SCHEMA

    fields = [
      ST.primitive_type_builder("_1", PPhys::INT32)
        .with_repetition(PRep::REQUIRED).with_logical_type(PLT.integer(8, true)).build.as(ST),
      ST.primitive_type_builder("_2", PPhys::INT32)
        .with_repetition(PRep::REQUIRED).with_logical_type(PLT.integer(16, false)).build.as(ST),
      ST.primitive_type_builder("_3", PPhys::FLOAT)
        .with_repetition(PRep::REQUIRED).build.as(ST),
      ST.primitive_type_builder("_4", PPhys::DOUBLE)
        .with_repetition(PRep::REQUIRED).build.as(ST),
      ST.primitive_type_builder("_5", PPhys::INT32)
        .with_logical_type(PLT::Date.new).build.as(ST),
      ST.primitive_type_builder("_6", PPhys::INT32)
        .with_logical_type(PLT.time(false, PTU::MILLIS)).build.as(ST),
      ST.primitive_type_builder("_7", PPhys::INT64)
        .with_logical_type(PLT.time(true, PTU::MICROS)).build.as(ST),
      ST.primitive_type_builder("_8", PPhys::INT64)
        .with_logical_type(PLT.timestamp(true, PTU::MILLIS)).build.as(ST),
      ST.primitive_type_builder("_9", PPhys::INT64)
        .with_logical_type(PLT.timestamp(false, PTU::NANOS)).build.as(ST),
      ST.primitive_type_builder("_10", PPhys::BYTE_ARRAY)
        .with_logical_type(PLT::String.new).build.as(ST),
    ]

    expected = ST.group_type_builder("root").with_fields(fields).build.as(ST)
    message.should eq(expected)
  end
end
