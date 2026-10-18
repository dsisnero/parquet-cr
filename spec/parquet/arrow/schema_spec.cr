require "../../spec_helper"

# Ported from `parquet/src/arrow/schema/mod.rs` tests
# (`test_field_to_column_desc`, `test_coerced_map_list`, `test_empty_struct_field`
# + the `ArrowSchemaConverter` doc example)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Only the Arrow -> Parquet direction is ported here; the Parquet -> Arrow
# direction (and the `ARROW:schema` IPC round-trip) needs arrow-cr's future
# `arrow-ipc` schema flatbuffers (see `shards_info/arrow_cr.1.md`).
private alias PSchema = Parquet::Schema
private alias PType = Parquet::Type
private alias PArrow = Parquet::Arrow
private alias PRep = Parquet::Repetition
private alias PLogical = Parquet::LogicalType
private alias ArrowField = Arrow::Field
private alias ArrowFields = Arrow::Fields
private alias ArrowSchema = Arrow::Schema
private alias ArrowDataType = Arrow::DataType
private alias ArrowTimeUnit = Arrow::TimeUnit

private def parse_parquet_descriptor(message_type : String) : PSchema::SchemaDescriptor
  PSchema::SchemaDescriptor.new(PSchema.parse_message_type(message_type))
end

private def convert_arrow_schema(fields : ArrowFields, coerce_types : Bool = false) : PSchema::SchemaDescriptor
  PArrow::ArrowSchemaConverter.new
    .with_coerce_types(coerce_types)
    .convert(ArrowSchema.new(fields))
end

describe Parquet::Arrow::ArrowSchemaConverter do
  # The doc example on `ArrowSchemaConverter`.
  it "converts a flat arrow schema" do
    converted = convert_arrow_schema(ArrowFields.new([
      ArrowField.new("a", ArrowDataType.int64, true),
      ArrowField.new("b", ArrowDataType.date32, true),
    ]))

    expected = parse_parquet_descriptor(%(
      message arrow_schema {
        OPTIONAL INT64 a;
        OPTIONAL INT32 b (DATE);
      }
    ))
    converted.should eq(expected)
  end

  # Upstream `test_field_to_column_desc`: every supported Arrow type maps to the
  # expected Parquet primitive/group type (Arrow always sets a logical type,
  # even where the Parquet side has none, so those columns are compared on
  # name/physical/converted type only, exactly like upstream).
  it "maps arrow fields to parquet column descriptors" do
    expected = parse_parquet_descriptor(%(
      message arrow_schema {
        REQUIRED BOOLEAN boolean;
        REQUIRED INT32   int8  (INT_8);
        REQUIRED INT32   int16 (INTEGER(16,true));
        REQUIRED INT32   int32;
        REQUIRED INT64   int64;
        OPTIONAL DOUBLE  double;
        OPTIONAL FLOAT   float;
        OPTIONAL FIXED_LEN_BYTE_ARRAY (2) float16 (FLOAT16);
        OPTIONAL BINARY  string (STRING);
        OPTIONAL GROUP   bools (LIST) {
          REPEATED GROUP list {
            OPTIONAL BOOLEAN element;
          }
        }
        REQUIRED GROUP   bools_non_null (LIST) {
          REPEATED GROUP list {
            REQUIRED BOOLEAN element;
          }
        }
        OPTIONAL INT32   date       (DATE);
        OPTIONAL INT32   time_milli (TIME(MILLIS,false));
        OPTIONAL INT32   time_milli_utc (TIME(MILLIS,true));
        OPTIONAL INT64   time_micro (TIME_MICROS);
        OPTIONAL INT64   time_micro_utc (TIME(MICROS, true));
        OPTIONAL INT64   ts_milli (TIMESTAMP_MILLIS);
        REQUIRED INT64   ts_micro (TIMESTAMP(MICROS,false));
        REQUIRED INT64   ts_seconds;
        REQUIRED INT64   ts_micro_utc (TIMESTAMP(MICROS, true));
        REQUIRED INT64   ts_millis_zero_offset (TIMESTAMP(MILLIS, true));
        REQUIRED INT64   ts_millis_zero_negative_offset (TIMESTAMP(MILLIS, true));
        REQUIRED INT64   ts_micro_non_utc (TIMESTAMP(MICROS, true));
        REQUIRED GROUP struct {
          REQUIRED BOOLEAN bools;
          REQUIRED INT32 uint32 (INTEGER(32,false));
          REQUIRED GROUP   int32 (LIST) {
            REPEATED GROUP list {
              OPTIONAL INT32 element;
            }
          }
        }
        REQUIRED BINARY  dictionary_strings (STRING);
        REQUIRED INT32 decimal_int32 (DECIMAL(8,2));
        REQUIRED INT64 decimal_int64 (DECIMAL(16,2));
        REQUIRED FIXED_LEN_BYTE_ARRAY (13) decimal_fix_length (DECIMAL(30,2));
        REQUIRED FIXED_LEN_BYTE_ARRAY (16) decimal128 (DECIMAL(38,2));
        REQUIRED FIXED_LEN_BYTE_ARRAY (17) decimal256 (DECIMAL(39,2));
      }
    ))

    adjusted = {"adjusted_to_utc" => ""}
    arrow_fields = ArrowFields.new([
      ArrowField.new("boolean", ArrowDataType.boolean, false),
      ArrowField.new("int8", ArrowDataType.int8, false),
      ArrowField.new("int16", ArrowDataType.int16, false),
      ArrowField.new("int32", ArrowDataType.int32, false),
      ArrowField.new("int64", ArrowDataType.int64, false),
      ArrowField.new("double", ArrowDataType.float64, true),
      ArrowField.new("float", ArrowDataType.float32, true),
      ArrowField.new("float16", ArrowDataType.float16, true),
      ArrowField.new("string", ArrowDataType.utf8, true),
      ArrowField.new_list("bools", ArrowField.new("element", ArrowDataType.boolean, true), true),
      ArrowField.new_list("bools_non_null", ArrowField.new("element", ArrowDataType.boolean, false), false),
      ArrowField.new("date", ArrowDataType.date32, true),
      ArrowField.new("time_milli", ArrowDataType.time32(ArrowTimeUnit::Millisecond), true),
      ArrowField.new("time_milli_utc", ArrowDataType.time32(ArrowTimeUnit::Millisecond), true).with_metadata(adjusted),
      ArrowField.new("time_micro", ArrowDataType.time64(ArrowTimeUnit::Microsecond), true),
      ArrowField.new("time_micro_utc", ArrowDataType.time64(ArrowTimeUnit::Microsecond), true).with_metadata(adjusted),
      ArrowField.new("ts_milli", ArrowDataType.timestamp(ArrowTimeUnit::Millisecond, nil), true),
      ArrowField.new("ts_micro", ArrowDataType.timestamp(ArrowTimeUnit::Microsecond, nil), false),
      ArrowField.new("ts_seconds", ArrowDataType.timestamp(ArrowTimeUnit::Second, "UTC"), false),
      ArrowField.new("ts_micro_utc", ArrowDataType.timestamp(ArrowTimeUnit::Microsecond, "UTC"), false),
      ArrowField.new("ts_millis_zero_offset", ArrowDataType.timestamp(ArrowTimeUnit::Millisecond, "+00:00"), false),
      ArrowField.new("ts_millis_zero_negative_offset", ArrowDataType.timestamp(ArrowTimeUnit::Millisecond, "-00:00"), false),
      ArrowField.new("ts_micro_non_utc", ArrowDataType.timestamp(ArrowTimeUnit::Microsecond, "+01:00"), false),
      ArrowField.new_struct("struct", ArrowFields.new([
        ArrowField.new("bools", ArrowDataType.boolean, false),
        ArrowField.new("uint32", ArrowDataType.uint32, false),
        ArrowField.new_list("int32", ArrowField.new("element", ArrowDataType.int32, true), false),
      ]), false),
      ArrowField.new_dictionary("dictionary_strings", ArrowDataType.int32, ArrowDataType.utf8, false),
      ArrowField.new("decimal_int32", ArrowDataType.decimal128(8, 2), false),
      ArrowField.new("decimal_int64", ArrowDataType.decimal128(16, 2), false),
      ArrowField.new("decimal_fix_length", ArrowDataType.decimal128(30, 2), false),
      ArrowField.new("decimal128", ArrowDataType.decimal128(38, 2), false),
      ArrowField.new("decimal256", ArrowDataType.decimal256(39, 2), false),
    ])

    converted = convert_arrow_schema(arrow_fields)
    converted.columns.size.should eq(expected.columns.size)
    expected.columns.zip(converted.columns).each do |(a, b)|
      if a.logical_type_ref
        a.should eq(b)
      else
        a.name.should eq(b.name)
        a.physical_type.should eq(b.physical_type)
        a.converted_type.should eq(b.converted_type)
      end
    end
  end

  # Upstream `test_coerced_map_list`: with coercion the list element and map
  # entries/key/value take the names prescribed by the Parquet spec.
  it "coerces list and map names" do
    arrow_fields = ArrowFields.new([
      ArrowField.new_list("my_list", ArrowField.new("item", ArrowDataType.boolean, true), false),
      ArrowField.new_map(
        "my_map",
        "my_entries",
        ArrowField.new("my_keys", ArrowDataType.utf8, false),
        ArrowField.new("my_values", ArrowDataType.int32, true),
        false,
        true
      ),
    ])

    coerced = convert_arrow_schema(arrow_fields, coerce_types: true)
    uncoerced = convert_arrow_schema(arrow_fields, coerce_types: false)

    coerced_spec = parse_parquet_descriptor(%(
      message parquet_schema {
        REQUIRED GROUP my_list (LIST) {
          REPEATED GROUP list {
            OPTIONAL BOOLEAN element;
          }
        }
        OPTIONAL GROUP my_map (MAP) {
          REPEATED GROUP key_value {
            REQUIRED BINARY key (STRING);
            OPTIONAL INT32 value;
          }
        }
      }
    ))
    uncoerced_spec = parse_parquet_descriptor(%(
      message parquet_schema {
        REQUIRED GROUP my_list (LIST) {
          REPEATED GROUP list {
            OPTIONAL BOOLEAN item;
          }
        }
        OPTIONAL GROUP my_map (MAP) {
          REPEATED GROUP my_entries {
            REQUIRED BINARY my_keys (STRING);
            OPTIONAL INT32 my_values;
          }
        }
      }
    ))

    coerced.columns.size.should eq(coerced_spec.columns.size)
    uncoerced.columns.size.should eq(uncoerced_spec.columns.size)
    coerced.columns.map(&.path.to_s).should eq(coerced_spec.columns.map(&.path.to_s))
    uncoerced.columns.map(&.path.to_s).should eq(uncoerced_spec.columns.map(&.path.to_s))
  end

  # Upstream `test_empty_struct_field`.
  it "rejects empty structs" do
    arrow_fields = ArrowFields.new([
      ArrowField.new("struct", ArrowDataType.struct(ArrowFields.empty), false),
    ])

    expect_raises(Parquet::ParquetError, "Parquet does not support writing empty structs") do
      convert_arrow_schema(arrow_fields, coerce_types: true)
    end
  end

  # Upstream `PARQUET:field_id` handling: `field_id` reads the metadata key and
  # silently ignores non-integer values.
  it "carries parquet field ids from field metadata" do
    fields = ArrowFields.new([
      ArrowField.new("a", ArrowDataType.int32, false).with_metadata({PArrow::PARQUET_FIELD_ID_META_KEY => "42"}),
      ArrowField.new("b", ArrowDataType.int32, false).with_metadata({PArrow::PARQUET_FIELD_ID_META_KEY => "not-a-number"}),
    ])

    converted = convert_arrow_schema(fields)
    converted.schema.get_fields[0].basic_info.id.should eq(42)
    converted.schema.get_fields[1].basic_info.has_id?.should be_false
  end

  # Arrow types with no Parquet logical type are stored as their physical type
  # (converted_type NONE, logical_type nil).
  it "maps types without a parquet logical type" do
    converted = convert_arrow_schema(ArrowFields.new([
      ArrowField.new("duration", ArrowDataType.duration(ArrowTimeUnit::Microsecond), true),
      ArrowField.new("interval", ArrowDataType.interval(Arrow::IntervalUnit::DayTime), true),
      ArrowField.new("date64", ArrowDataType.date64, true),
      ArrowField.new("null", ArrowDataType.null, true),
      ArrowField.new("binary", ArrowDataType.binary, true),
      ArrowField.new("fixed_binary", ArrowDataType.fixed_size_binary(20), true),
    ]))

    duration = converted.columns[0]
    duration.physical_type.should eq(PType::INT64)
    duration.logical_type_ref.should be_nil

    interval = converted.columns[1]
    interval.physical_type.should eq(PType::FIXED_LEN_BYTE_ARRAY)
    interval.type_length.should eq(12)
    interval.converted_type.should eq(Parquet::ConvertedType::INTERVAL)

    converted.columns[2].physical_type.should eq(PType::INT64)
    converted.columns[2].logical_type_ref.should be_nil

    null_column = converted.columns[3]
    null_column.physical_type.should eq(PType::INT32)
    null_column.logical_type_ref.should eq(PLogical::Unknown.new)

    converted.columns[4].physical_type.should eq(PType::BYTE_ARRAY)
    converted.columns[4].logical_type_ref.should be_nil

    fixed_binary = converted.columns[5]
    fixed_binary.physical_type.should eq(PType::FIXED_LEN_BYTE_ARRAY)
    fixed_binary.type_length.should eq(20)
  end

  # `with_coerce_types` maps `Date64` onto Parquet's 32-bit `DATE`.
  it "coerces date64 to a parquet date" do
    fields = ArrowFields.new([ArrowField.new("date64", ArrowDataType.date64, true)])

    convert_arrow_schema(fields).columns[0].physical_type.should eq(PType::INT64)
    convert_arrow_schema(fields, coerce_types: true).columns[0].physical_type.should eq(PType::INT32)
    convert_arrow_schema(fields, coerce_types: true).columns[0].logical_type_ref.should eq(PLogical::Date.new)
  end

  # `schema_root` and the default root name.
  it "uses the configured root schema name" do
    fields = ArrowFields.new([ArrowField.new("a", ArrowDataType.int32, true)])

    convert_arrow_schema(fields).schema.name.should eq("arrow_schema")
    PArrow::ArrowSchemaConverter.new.schema_root("custom").convert(ArrowSchema.new(fields))
      .schema.name.should eq("custom")
  end

  # `decimal_length_from_precision` matches the parquet-format rule.
  it "computes decimal byte lengths from precision" do
    PArrow.decimal_length_from_precision(9).should eq(4)
    PArrow.decimal_length_from_precision(10).should eq(5)
    PArrow.decimal_length_from_precision(18).should eq(8)
    PArrow.decimal_length_from_precision(19).should eq(9)
    PArrow.decimal_length_from_precision(38).should eq(16)
    PArrow.decimal_length_from_precision(39).should eq(17)
  end
end

# Ported from `parquet/src/arrow/schema/mod.rs`
# (`encode_arrow_schema`, `get_arrow_schema_from_metadata`,
# `add_encoded_arrow_schema_to_metadata`, `flatten_ree_field`).
#
# `arrow-ipc`'s schema `Message` flatbuffer lives in arrow-cr
# (`Arrow::Ipc.schema_message` / `Arrow::Ipc.read_message`), so the
# `ARROW:schema` metadata round-trip is portable.
private alias PWriterProperties = Parquet::File::Properties::WriterProperties

private def sample_arrow_schema : ArrowSchema
  ArrowSchema.new(
    ArrowFields.new([
      ArrowField.new("a", ArrowDataType.int64, false),
      ArrowField.new("b", ArrowDataType.utf8, true),
      ArrowField.new_list("c", ArrowField.new("element", ArrowDataType.float64, true), true),
      ArrowField.new("d", ArrowDataType.decimal128(30, 2), true),
      ArrowField.new("e", ArrowDataType.timestamp(ArrowTimeUnit::Microsecond, "UTC"), true),
      ArrowField.new_dictionary("f", ArrowDataType.int32, ArrowDataType.utf8, false),
    ]),
    Arrow::Metadata.from({"created_by" => "parquet-cr"})
  )
end

describe "Parquet::Arrow schema metadata" do
  it "round-trips an arrow schema through the ARROW:schema metadata" do
    schema = sample_arrow_schema
    encoded = PArrow.encode_arrow_schema(schema)
    encoded.should_not be_empty

    decoded = PArrow.get_arrow_schema_from_metadata(encoded)
    decoded.should eq(schema)
  end

  it "encodes the legacy 8-byte IPC length prefix" do
    encoded = PArrow.encode_arrow_schema(sample_arrow_schema)
    bytes = Base64.decode(encoded)
    bytes[0, 4].should eq(Bytes[0xFF_u8, 0xFF_u8, 0xFF_u8, 0xFF_u8])

    # The prefix length is the size of the remaining flatbuffer.
    length = IO::ByteFormat::LittleEndian.decode(Int32, bytes[4, 4])
    length.should eq(bytes.size - 8)
  end

  it "adds the encoded arrow schema to writer properties" do
    schema = sample_arrow_schema
    props = PWriterProperties.default
    PArrow.add_encoded_arrow_schema_to_metadata(schema, props)

    metadata = props.key_value_metadata.not_nil!
    entry = metadata.find { |kv| kv.key == PArrow::ARROW_SCHEMA_META_KEY }.not_nil!
    PArrow.get_arrow_schema_from_metadata(entry.value.not_nil!).should eq(schema)
  end

  it "replaces an existing encoded arrow schema" do
    props = PWriterProperties.default
    props.key_value_metadata = [
      Parquet::File::Metadata::KeyValue.new("foo", "bar"),
      Parquet::File::Metadata::KeyValue.new(PArrow::ARROW_SCHEMA_META_KEY, "stale"),
    ]

    PArrow.add_encoded_arrow_schema_to_metadata(sample_arrow_schema, props)

    metadata = props.key_value_metadata.not_nil!
    metadata.count { |kv| kv.key == PArrow::ARROW_SCHEMA_META_KEY }.should eq(1)
    metadata[0].key.should eq("foo")
    PArrow.get_arrow_schema_from_metadata(metadata[1].value.not_nil!).should eq(sample_arrow_schema)
  end

  # `flatten_ree_field`: run-end encoded fields are written as their value type.
  it "flattens run-end encoded fields in the stored schema" do
    values = ArrowField.new("values", ArrowDataType.utf8, true)
    run_ends = ArrowField.new("run_ends", ArrowDataType.int32, false)
    schema = ArrowSchema.new(ArrowFields.new([
      ArrowField.new("ree", ArrowDataType.run_end_encoded(run_ends, values), true),
    ]))

    props = PWriterProperties.default
    PArrow.add_encoded_arrow_schema_to_metadata(schema, props)
    entry = props.key_value_metadata.not_nil!.find { |kv| kv.key == PArrow::ARROW_SCHEMA_META_KEY }.not_nil!
    decoded = PArrow.get_arrow_schema_from_metadata(entry.value.not_nil!)

    decoded.fields[0].name.should eq("ree")
    decoded.fields[0].data_type.should eq(ArrowDataType.utf8)
  end

  it "rejects values that are not base64" do
    expect_raises(Parquet::ArrowError, "Unable to decode the encoded schema stored in ARROW:schema") do
      PArrow.get_arrow_schema_from_metadata("!!!not base64!!!")
    end
  end

  it "rejects base64 that is not an IPC message" do
    expect_raises(Parquet::ArrowError, "Unable to get root as message stored in ARROW:schema") do
      PArrow.get_arrow_schema_from_metadata(Base64.strict_encode(Bytes[1_u8, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
    end
  end

  # arrow-cr's flatbuffer reader is bounds-checked (it raises `ArrowError`, not
  # `IndexError`, on hostile metadata), so truncated payloads must surface as a
  # `Parquet::ArrowError` rather than a crash.
  it "rejects truncated encoded schemas without leaking slice errors" do
    encoded = Base64.decode(PArrow.encode_arrow_schema(sample_arrow_schema))
    [encoded.size // 2, encoded.size - 4, encoded.size - 8].each do |length|
      next if length <= 8

      truncated = encoded[0, length]
      expect_raises(Parquet::ArrowError) do
        PArrow.get_arrow_schema_from_metadata(Base64.strict_encode(truncated))
      end
    end
  end

  # A message whose header is not a schema (e.g. zeroed metadata: upstream's
  # "empty table" with `header = NONE`).
  it "rejects messages that are not arrow schemas" do
    payload = Bytes[0xFF_u8, 0xFF_u8, 0xFF_u8, 0xFF_u8] + Bytes[40_u8, 0_u8, 0_u8, 0_u8] + Bytes.new(40, 0_u8)
    expect_raises(Parquet::ArrowError, "the message is not Arrow Schema") do
      PArrow.get_arrow_schema_from_metadata(Base64.strict_encode(payload))
    end
  end

  # The decoder also accepts a bare IPC `Message` flatbuffer (no legacy prefix).
  it "decodes an unframed IPC message flatbuffer" do
    schema = sample_arrow_schema
    framed = Base64.decode(PArrow.encode_arrow_schema(schema))
    flatbuffer = framed[8, framed.size - 8]

    PArrow.get_arrow_schema_from_metadata(Base64.strict_encode(flatbuffer)).should eq(schema)
  end
end
