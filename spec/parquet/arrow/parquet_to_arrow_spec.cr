require "../../spec_helper"

# Ported from `parquet/src/arrow/schema/mod.rs` tests
# (`test_flat_primitives`, `test_decimal_fields`, `test_byte_array_fields`,
# `test_duplicate_fields`, `test_parquet_lists`, `test_parquet_maps`,
# `test_nested_schema`, `test_repeated_nested_schema`,
# `test_column_desc_to_field`, `test_metadata`, and a compact
# `test_arrow_schema_roundtrip`) (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `test_parquet_list_nullable` and `test_nested_schema_partial[_ordering]` are
# scheduled with the remaining projection work. Virtual-column schema and
# reader coverage is in virtual_type_spec.cr and virtual_reader_spec.cr.
private alias PArrow = Parquet::Arrow
private alias PSchema = Parquet::Schema
private alias PConv = Parquet::ConvertedType
private alias ADataType = Arrow::DataType
private alias AField = Arrow::Field
private alias AFields = Arrow::Fields
private alias ATimeUnit = Arrow::TimeUnit
private alias PSchemaDescriptor = PSchema::SchemaDescriptor

private def arrow_schema_from_parquet(message_type : String,
                                      metadata : Array(Parquet::File::Metadata::KeyValue)? = nil) : Arrow::Schema
  descriptor = PSchemaDescriptor.new(PSchema.parse_message_type(message_type))
  PArrow.parquet_to_arrow_schema(descriptor, metadata)
end

# Upstream schema/mod.rs::json_field varies with the canonical feature.
private def expected_json_field : Arrow::Field
  field = AField.new("json", ADataType.utf8, true)
  {% if flag?(:arrow_canonical_extension_types) %}
    field.with_extension_type(Arrow::Extension::Canonical::Json.new)
  {% else %}
    field
  {% end %}
end

describe "Parquet::Arrow parquet_to_arrow_schema" do
  # Upstream `test_flat_primitives`.
  it "maps flat primitives" do
    schema = arrow_schema_from_parquet(%(
      message test_schema {
        REQUIRED BOOLEAN boolean;
        REQUIRED INT32   int8  (INT_8);
        REQUIRED INT32   int16 (INT_16);
        REQUIRED INT32   uint8 (INTEGER(8,false));
        REQUIRED INT32   uint16 (INTEGER(16,false));
        REQUIRED INT32   int32;
        REQUIRED INT64   int64;
        OPTIONAL DOUBLE  double;
        OPTIONAL FLOAT   float;
        OPTIONAL FIXED_LEN_BYTE_ARRAY (2) float16 (FLOAT16);
        OPTIONAL BINARY  string (UTF8);
        OPTIONAL BINARY  string_2 (STRING);
        OPTIONAL BINARY  json (JSON);
      }
    ))

    schema.fields.should eq(AFields.from([
      AField.new("boolean", ADataType.boolean, false),
      AField.new("int8", ADataType.int8, false),
      AField.new("int16", ADataType.int16, false),
      AField.new("uint8", ADataType.uint8, false),
      AField.new("uint16", ADataType.uint16, false),
      AField.new("int32", ADataType.int32, false),
      AField.new("int64", ADataType.int64, false),
      AField.new("double", ADataType.float64, true),
      AField.new("float", ADataType.float32, true),
      AField.new("float16", ADataType.float16, true),
      AField.new("string", ADataType.utf8, true),
      AField.new("string_2", ADataType.utf8, true),
      expected_json_field,
    ]))
  end

  # Upstream `test_decimal_fields`.
  it "maps decimals by precision and length" do
    schema = arrow_schema_from_parquet(%(
      message test_schema {
        REQUIRED INT32 decimal1 (DECIMAL(4,2));
        REQUIRED INT64 decimal2 (DECIMAL(12,2));
        REQUIRED FIXED_LEN_BYTE_ARRAY (16) decimal3 (DECIMAL(30,2));
        REQUIRED BYTE_ARRAY decimal4 (DECIMAL(33,2));
        REQUIRED BYTE_ARRAY decimal5 (DECIMAL(38,2));
        REQUIRED FIXED_LEN_BYTE_ARRAY (17) decimal6 (DECIMAL(39,2));
        REQUIRED BYTE_ARRAY decimal7 (DECIMAL(39,2));
      }
    ))

    schema.fields.should eq(AFields.from([
      AField.new("decimal1", ADataType.decimal128(4, 2), false),
      AField.new("decimal2", ADataType.decimal128(12, 2), false),
      AField.new("decimal3", ADataType.decimal128(30, 2), false),
      AField.new("decimal4", ADataType.decimal128(33, 2), false),
      AField.new("decimal5", ADataType.decimal128(38, 2), false),
      AField.new("decimal6", ADataType.decimal256(39, 2), false),
      AField.new("decimal7", ADataType.decimal256(39, 2), false),
    ]))
  end

  # Upstream `test_byte_array_fields`.
  it "maps byte arrays" do
    schema = arrow_schema_from_parquet(%(
      message test_schema {
        REQUIRED BYTE_ARRAY binary;
        REQUIRED FIXED_LEN_BYTE_ARRAY (20) fixed_binary;
      }
    ))

    schema.fields.should eq(AFields.from([
      AField.new("binary", ADataType.binary, false),
      AField.new("fixed_binary", ADataType.fixed_size_binary(20), false),
    ]))
  end

  # Upstream `test_duplicate_fields` (including the by-columns variant).
  it "maps duplicate field names and by-columns projections" do
    descriptor = PSchemaDescriptor.new(PSchema.parse_message_type(%(
      message test_schema {
        REQUIRED BOOLEAN boolean;
        REQUIRED INT32 int8 (INT_8);
      }
    )))
    expected = AFields.from([
      AField.new("boolean", ADataType.boolean, false),
      AField.new("int8", ADataType.int8, false),
    ])

    PArrow.parquet_to_arrow_schema(descriptor, nil).fields.should eq(expected)
    PArrow.parquet_to_arrow_schema_by_columns(descriptor, PArrow::ProjectionMask.all, nil)
      .fields.should eq(expected)
  end

  # Upstream `test_parquet_lists`.
  it "maps every legacy and modern list encoding" do
    schema = arrow_schema_from_parquet(%(
      message test_schema {
        REQUIRED GROUP my_list (LIST) {
          REPEATED GROUP list {
            OPTIONAL BINARY element (UTF8);
          }
        }
        OPTIONAL GROUP my_list (LIST) {
          REPEATED GROUP list {
            REQUIRED BINARY element (UTF8);
          }
        }
        OPTIONAL GROUP array_of_arrays (LIST) {
          REPEATED GROUP list {
            REQUIRED GROUP element (LIST) {
              REPEATED GROUP list {
                REQUIRED INT32 element;
              }
            }
          }
        }
        OPTIONAL GROUP my_list (LIST) {
          REPEATED GROUP element {
            REQUIRED BINARY str (UTF8);
          }
        }
        OPTIONAL GROUP my_list (LIST) {
          REPEATED INT32 element;
        }
        OPTIONAL GROUP my_list (LIST) {
          REPEATED GROUP element {
            REQUIRED BINARY str (UTF8);
            REQUIRED INT32 num;
          }
        }
        OPTIONAL GROUP my_list (LIST) {
          REPEATED GROUP array {
            REQUIRED BINARY str (UTF8);
          }
        }
        OPTIONAL GROUP my_list (LIST) {
          REPEATED GROUP my_list_tuple {
            REQUIRED BINARY str (UTF8);
          }
        }
        REPEATED INT32 name;
      }
    ))

    expected = AFields.from([
      # List<String> (list non-null, elements nullable)
      AField.new_list("my_list", AField.new("element", ADataType.utf8, true), false),
      # List<String> (list nullable, elements non-null)
      AField.new_list("my_list", AField.new("element", ADataType.utf8, false), true),
      # List<List<Integer>>
      AField.new_list(
        "array_of_arrays",
        AField.new_list("element", AField.new("element", ADataType.int32, false), false),
        true
      ),
      # List<String> (list nullable, elements non-null)
      AField.new_list("my_list", AField.new("str", ADataType.utf8, false), true),
      # List<Integer> (nullable list, non-null elements)
      AField.new_list("my_list", AField.new("element", ADataType.int32, false), true),
      # List<Tuple<String, Integer>>
      AField.new_list(
        "my_list",
        AField.new_struct("element", AFields.from([
          AField.new("str", ADataType.utf8, false),
          AField.new("num", ADataType.int32, false),
        ]), false),
        true
      ),
      # List<OneTuple<String>> (group named array)
      AField.new_list(
        "my_list",
        AField.new_struct("array", AFields.from([AField.new("str", ADataType.utf8, false)]), false),
        true
      ),
      # List<OneTuple<String>> (group named <list>_tuple)
      AField.new_list(
        "my_list",
        AField.new_struct("my_list_tuple", AFields.from([AField.new("str", ADataType.utf8, false)]), false),
        true
      ),
      # One-level encoding: repeated value_type name
      AField.new_list("name", AField.new("name", ADataType.int32, false), false),
    ])

    schema.fields.size.should eq(expected.size)
    expected.each_with_index do |field, i|
      schema.fields[i].should eq(field)
    end
  end

  # Upstream `test_parquet_maps`.
  it "maps MAP and MAP_KEY_VALUE groups" do
    schema = arrow_schema_from_parquet(%(
      message test_schema {
        REQUIRED group my_map1 (MAP) {
          REPEATED group key_value {
            REQUIRED binary key (UTF8);
            OPTIONAL int32 value;
          }
        }
        OPTIONAL group my_map2 (MAP) {
          REPEATED group map {
            REQUIRED binary str (UTF8);
            REQUIRED int32 num;
          }
        }
        OPTIONAL group my_map3 (MAP_KEY_VALUE) {
          REPEATED group map {
            REQUIRED binary key (UTF8);
            OPTIONAL int32 value;
          }
        }
        REQUIRED group my_map4 (MAP) {
          REPEATED group map {
            OPTIONAL binary key (UTF8);
            REQUIRED int32 value;
          }
        }
      }
    ))

    expected = AFields.from([
      AField.new_map("my_map1", "key_value",
        AField.new("key", ADataType.utf8, false), AField.new("value", ADataType.int32, true), false, false),
      AField.new_map("my_map2", "map",
        AField.new("str", ADataType.utf8, false), AField.new("num", ADataType.int32, false), false, true),
      AField.new_map("my_map3", "map",
        AField.new("key", ADataType.utf8, false), AField.new("value", ADataType.int32, true), false, true),
      # The key is always non-nullable (#5630)
      AField.new_map("my_map4", "map",
        AField.new("key", ADataType.utf8, false), AField.new("value", ADataType.int32, false), false, false),
    ])

    schema.fields.size.should eq(expected.size)
    expected.each_with_index { |field, i| schema.fields[i].should eq(field) }
  end

  # Upstream `test_nested_schema`.
  it "maps nested structs" do
    schema = arrow_schema_from_parquet(%(
      message test_schema {
        REQUIRED GROUP group1 {
          REQUIRED BOOLEAN leaf1;
          REQUIRED INT32 leaf2;
        }
        REQUIRED INT64 leaf3;
      }
    ))

    schema.fields.should eq(AFields.from([
      AField.new("group1", ADataType.struct(AFields.from([
        AField.new("leaf1", ADataType.boolean, false),
        AField.new("leaf2", ADataType.int32, false),
      ])), false),
      AField.new("leaf3", ADataType.int64, false),
    ]))
  end

  # Upstream `test_repeated_nested_schema` (deprecated repeated groups become lists).
  it "maps repeated nested groups to lists" do
    schema = arrow_schema_from_parquet(%(
      message test_schema {
        OPTIONAL INT32 leaf1;
        REPEATED GROUP outerGroup {
          OPTIONAL INT32 leaf2;
          REPEATED GROUP innerGroup {
            OPTIONAL INT32 leaf3;
          }
        }
      }
    ))

    inner_group_list = AField.new_list(
      "innerGroup",
      AField.new_struct("innerGroup", AFields.from([AField.new("leaf3", ADataType.int32, true)]), false),
      false
    )
    outer_group_list = AField.new_list(
      "outerGroup",
      AField.new_struct("outerGroup", AFields.from([
        AField.new("leaf2", ADataType.int32, true),
        inner_group_list,
      ]), false),
      false
    )

    schema.fields.should eq(AFields.from([
      AField.new("leaf1", ADataType.int32, true),
      outer_group_list,
    ]))
  end

  # Upstream `test_metadata`: key/value metadata without values is dropped, and
  # `ARROW:schema` is not exposed as schema metadata.
  it "carries key/value metadata into the arrow schema" do
    metadata = [
      Parquet::File::Metadata::KeyValue.new("foo", "bar"),
      Parquet::File::Metadata::KeyValue.new("baz", nil),
    ]

    schema = arrow_schema_from_parquet(%(
      message test_schema {
        OPTIONAL BINARY string (STRING);
      }
    ), metadata)

    schema.metadata.get("foo").should eq("bar")
    schema.metadata.get("baz").should be_nil
    schema.metadata.get("ARROW:schema").should be_nil
  end
end

describe "Parquet::Arrow parquet_to_arrow_field" do
  # Upstream `test_column_desc_to_field`.
  it "converts column descriptors to arrow fields" do
    descriptor = PSchemaDescriptor.new(PSchema.parse_message_type(%(
      message test_schema {
        REQUIRED BOOLEAN boolean;
        REQUIRED INT32   int8  (INT_8);
        REQUIRED INT32   uint8 (INTEGER(8,false));
        REQUIRED INT32   int16 (INT_16);
        REQUIRED INT32   uint16 (INTEGER(16,false));
        REQUIRED INT32   int32;
        REQUIRED INT64   int64;
        OPTIONAL DOUBLE  double;
        OPTIONAL FLOAT   float;
        OPTIONAL FIXED_LEN_BYTE_ARRAY (2) float16 (FLOAT16);
        OPTIONAL BINARY  string (UTF8);
        REPEATED BOOLEAN bools;
        OPTIONAL INT32   date       (DATE);
        OPTIONAL INT32   time_milli (TIME_MILLIS);
        OPTIONAL INT64   time_micro (TIME_MICROS);
        OPTIONAL INT64   time_nano (TIME(NANOS,false));
        OPTIONAL INT64   ts_milli (TIMESTAMP_MILLIS);
        REQUIRED INT64   ts_micro (TIMESTAMP_MICROS);
        REQUIRED INT64   ts_nano (TIMESTAMP(NANOS,true));
        REPEATED INT32   int_list;
        REPEATED BINARY  byte_list;
        REPEATED BINARY  string_list (UTF8);
        REQUIRED INT32 decimal_int32 (DECIMAL(8,2));
        REQUIRED INT64 decimal_int64 (DECIMAL(16,2));
        REQUIRED FIXED_LEN_BYTE_ARRAY (13) decimal_fix_length (DECIMAL(30,2));
      }
    )))

    converted = descriptor.columns.map { |column| PArrow.parquet_to_arrow_field(column) }

    converted.should eq([
      AField.new("boolean", ADataType.boolean, false),
      AField.new("int8", ADataType.int8, false),
      AField.new("uint8", ADataType.uint8, false),
      AField.new("int16", ADataType.int16, false),
      AField.new("uint16", ADataType.uint16, false),
      AField.new("int32", ADataType.int32, false),
      AField.new("int64", ADataType.int64, false),
      AField.new("double", ADataType.float64, true),
      AField.new("float", ADataType.float32, true),
      AField.new("float16", ADataType.float16, true),
      AField.new("string", ADataType.utf8, true),
      AField.new_list("bools", AField.new("bools", ADataType.boolean, false), false),
      AField.new("date", ADataType.date32, true),
      AField.new("time_milli", ADataType.time32(ATimeUnit::Millisecond), true),
      AField.new("time_micro", ADataType.time64(ATimeUnit::Microsecond), true),
      AField.new("time_nano", ADataType.time64(ATimeUnit::Nanosecond), true),
      AField.new("ts_milli", ADataType.timestamp(ATimeUnit::Millisecond, "UTC"), true),
      AField.new("ts_micro", ADataType.timestamp(ATimeUnit::Microsecond, "UTC"), false),
      AField.new("ts_nano", ADataType.timestamp(ATimeUnit::Nanosecond, "UTC"), false),
      AField.new_list("int_list", AField.new("int_list", ADataType.int32, false), false),
      AField.new_list("byte_list", AField.new("byte_list", ADataType.binary, false), false),
      AField.new_list("string_list", AField.new("string_list", ADataType.utf8, false), false),
      AField.new("decimal_int32", ADataType.decimal128(8, 2), false),
      AField.new("decimal_int64", ADataType.decimal128(16, 2), false),
      AField.new("decimal_fix_length", ADataType.decimal128(30, 2), false),
    ])
  end
end

describe "Parquet::Arrow convert_schema" do
  # Upstream `test_roundtrip`: converting a schema and then re-converting it with
  # its own fields as the hint must be stable.
  it "is stable when its own output is used as the hint" do
    descriptor = PSchemaDescriptor.new(PSchema.parse_message_type(%(
      message schema {
        optional group my_list (LIST) {
          repeated int32 element;
        }
        optional group my_map (MAP) {
          repeated group key_value {
            required binary key (UTF8);
            optional int32 value;
          }
        }
        optional binary plain (UTF8);
      }
    )))

    converted = PArrow.convert_schema(descriptor, PArrow::ProjectionMask.all, nil).not_nil!
    again = PArrow.convert_schema(descriptor, PArrow::ProjectionMask.all, converted.arrow_type.fields).not_nil!

    again.arrow_type.should eq(converted.arrow_type)
  end
end

describe "Parquet::Arrow schema round-trip" do
  # Compact version of upstream `test_arrow_schema_roundtrip`: arrow -> parquet
  # -> arrow preserves types parquet cannot represent, field ids, and metadata.
  it "round-trips an arrow schema through parquet" do
    arrow_schema = Arrow::Schema.new(
      AFields.from([
        AField.new("c1", ADataType.utf8, false).with_metadata({
          "Key"                             => "Foo",
          PArrow::PARQUET_FIELD_ID_META_KEY => "2",
        }),
        AField.new("c2", ADataType.binary, false),
        AField.new("c3", ADataType.fixed_size_binary(3), false),
        AField.new("c5", ADataType.date32, false),
        AField.new("c6", ADataType.date64, false),
        AField.new("c7", ADataType.time32(ATimeUnit::Second), false),
        AField.new("c8", ADataType.time32(ATimeUnit::Millisecond), false),
        AField.new("c13", ADataType.time64(ATimeUnit::Microsecond), false),
        AField.new("c14", ADataType.time64(ATimeUnit::Nanosecond), false),
        AField.new("c15", ADataType.timestamp(ATimeUnit::Second, nil), false),
        AField.new("c16", ADataType.timestamp(ATimeUnit::Millisecond, "UTC"), false),
        AField.new("c17", ADataType.timestamp(ATimeUnit::Microsecond, "Africa/Johannesburg"), false),
        AField.new("c19", ADataType.interval(Arrow::IntervalUnit::DayTime), false),
        AField.new("c21", ADataType.decimal128(38, 2), false),
        AField.new_list(
          "c22",
          AField.new_list_field(ADataType.boolean, true).with_metadata({
            "Key"                             => "Bar",
            PArrow::PARQUET_FIELD_ID_META_KEY => "5",
          }),
          false
        ).with_metadata({PArrow::PARQUET_FIELD_ID_META_KEY => "4"}),
        AField.new_struct("c23", AFields.from([
          AField.new("a", ADataType.int16, true),
          AField.new("b", ADataType.float64, false),
        ]), false),
      ]),
      Arrow::Metadata.from({"created_by" => "parquet-cr"})
    )

    parquet_schema = PArrow::ArrowSchemaConverter.new.convert(arrow_schema)
    metadata = [
      Parquet::File::Metadata::KeyValue.new(PArrow::ARROW_SCHEMA_META_KEY, PArrow.encode_arrow_schema(arrow_schema)),
    ]
    round_tripped = PArrow.parquet_to_arrow_schema(parquet_schema, metadata)

    round_tripped.fields.size.should eq(arrow_schema.fields.size)
    round_tripped.fields.each_with_index do |field, i|
      field.name.should eq(arrow_schema.fields[i].name)
      field.data_type.should eq(arrow_schema.fields[i].data_type)
      field.metadata.should eq(arrow_schema.fields[i].metadata)
    end
    round_tripped.metadata.get("created_by").should eq("parquet-cr")
  end
end
