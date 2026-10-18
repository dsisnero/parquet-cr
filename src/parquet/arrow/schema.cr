require "base64"
require "arrow-cr"
require "../basic"
require "../errors"
require "../file/metadata"
require "../file/properties"
require "../schema/types"

# Ported from `parquet/src/arrow/schema/mod.rs` (the Arrow -> Parquet direction)
# and `parquet/src/arrow/schema/extension.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ArrowSchemaConverter`, `arrow_to_parquet_type`, `field_id`,
# `decimal_length_from_precision`, `encode_arrow_schema`,
# `get_arrow_schema_from_metadata`, and
# `add_encoded_arrow_schema_to_metadata` (the `ARROW:schema` IPC round-trip uses
# arrow-cr's `arrow-ipc` schema message).
#
# The other direction (`parquet_to_arrow_schema*`, `ParquetField`, `Visitor`,
# `FieldLevels`, `parquet_to_arrow_field`) is the next arrow feature.
#
# Divergences: upstream's `ArrowSchemaConverter::with_*` builders consume `self`
# and return a new converter; the port mutates the converter and returns `self`
# (like the other ported builders). `arrow_to_parquet_type`, `field_id`,
# `logical_type_for_*`, and `decimal_length_from_precision` are module methods
# (upstream: private functions in `schema` / `schema::extension`, plus one `pub
# fn`). Arrow *extension type* mapping (`ARROW:extension:name` → Variant/Uuid/
# Json/Geometry/Geography logical types) is compiled out upstream unless
# `arrow_canonical_extension_types`/`geospatial`/`variant_experimental` are
# enabled, and arrow-cr does not model extension types yet, so the port follows
# upstream's default build (only `Utf8` → `STRING`, no other logical types).
module Parquet
  # Arrow <-> Parquet interop (upstream `parquet::arrow`).
  module Arrow
    # Schema metadata key used to store the serialized Arrow schema (Arrow IPC
    # format, base64 encoded).
    ARROW_SCHEMA_META_KEY = "ARROW:schema"

    # When present on an Arrow field's metadata, populates `BasicTypeInfo#id`.
    PARQUET_FIELD_ID_META_KEY = "PARQUET:field_id"

    # Name of the Parquet list element / map struct / map key / map value
    # fields required by the Parquet spec when coercing types.
    PARQUET_LIST_ELEMENT_NAME = "element"
    PARQUET_MAP_STRUCT_NAME   = "key_value"
    PARQUET_KEY_FIELD_NAME    = "key"
    PARQUET_VALUE_FIELD_NAME  = "value"

    # Number of bytes needed to store a decimal with `precision` digits.
    #
    # Upstream: `pub fn decimal_length_from_precision(precision: u8) -> usize`
    # (kept in `parquet::arrow::schema` rather than re-exported).
    def self.decimal_length_from_precision(precision : Int32) : Int32
      ((Math.log2((10.0 ** precision) + 1.0) + 1.0) / 8.0).ceil.to_i32
    end

    # Reads the `PARQUET:field_id` metadata key, failing quietly on non-integers.
    def self.field_id(field : ::Arrow::Field) : Int32?
      value = field.metadata.get(PARQUET_FIELD_ID_META_KEY)
      value.try(&.to_i?)
    end

    # Encodes the Arrow schema into the IPC format and base64-encodes it
    # (upstream `encode_arrow_schema`).
    #
    # The payload is the legacy IPC framing: four `0xFF` continuation bytes, the
    # little-endian flatbuffer length, then the IPC `Message` flatbuffer whose
    # header carries the schema.
    def self.encode_arrow_schema(schema : ::Arrow::Schema) : String
      ipc_message = ::Arrow::Ipc.schema_message(schema)

      prefix = Bytes.new(8)
      prefix[0] = 0xFF_u8
      prefix[1] = 0xFF_u8
      prefix[2] = 0xFF_u8
      prefix[3] = 0xFF_u8
      IO::ByteFormat::LittleEndian.encode(ipc_message.size.to_i32, prefix[4, 4])

      Base64.strict_encode(Bytes.new(prefix.size + ipc_message.size) { |i| i < prefix.size ? prefix[i] : ipc_message[i - prefix.size] })
    end

    # Tries to convert the `ARROW:schema` metadata value into an Arrow schema
    # (upstream `get_arrow_schema_from_metadata`).
    def self.get_arrow_schema_from_metadata(encoded_meta : String) : ::Arrow::Schema
      bytes =
        begin
          Base64.decode(encoded_meta)
        rescue error : Base64::Error
          # The C++ implementation returns an error if the schema can't be parsed.
          raise arrow_err!("Unable to decode the encoded schema stored in #{ARROW_SCHEMA_META_KEY}, #{error.message}")
        end

      # Upstream strips the legacy prefix and calls `root_as_message`; the port
      # hands both shapes to the IPC message reader, which understands the
      # legacy `0xFF*4 + len` framing, so raw flatbuffers get framed first.
      slice = if bytes.size > 8 && bytes[0, 4] == Bytes[0xFF_u8, 0xFF_u8, 0xFF_u8, 0xFF_u8]
                bytes
              else
                framed = Bytes.new(bytes.size + 8)
                framed[0] = 0xFF_u8
                framed[1] = 0xFF_u8
                framed[2] = 0xFF_u8
                framed[3] = 0xFF_u8
                IO::ByteFormat::LittleEndian.encode(bytes.size.to_i32, framed[4, 4])
                bytes.copy_to(framed[8, bytes.size])
                framed
              end

      message =
        begin
          # The flatbuffers implementation returns an error on a verification
          # error. arrow-cr's flatbuffer reader can also raise `IndexError` on a
          # truncated/garbage buffer (see `shards_info/arrow_cr.1.md`), so wrap
          # any failure here.
          ::Arrow::Ipc.read_message(slice)[0]
        rescue error : ParquetError
          raise error
        rescue error
          raise arrow_err!("Unable to get root as message stored in #{ARROW_SCHEMA_META_KEY}, #{error.message}")
        end

      schema = message.try(&.schema)
      raise arrow_err!("the message is not Arrow Schema") unless schema
      schema
    end

    # Stores the encoded Arrow schema hint in `WriterProperties` under
    # `ARROW:schema`, replacing any existing value (upstream
    # `add_encoded_arrow_schema_to_metadata`).
    #
    # Run-end encoded fields are flattened to their value type (upstream
    # `flatten_ree_field`).
    def self.add_encoded_arrow_schema_to_metadata(
      schema : ::Arrow::Schema,
      props : File::Properties::WriterProperties,
    ) : Nil
      has_ree = schema.fields.any?(&.data_type.kind.run_end_encoded?)
      encoded = if has_ree
                  flat_fields = schema.fields.map { |field| flatten_ree_field(field) }
                  encode_arrow_schema(::Arrow::Schema.new(::Arrow::Fields.from(flat_fields), schema.metadata))
                else
                  encode_arrow_schema(schema)
                end

      schema_kv = File::Metadata::KeyValue.new(ARROW_SCHEMA_META_KEY, encoded)
      meta = props.key_value_metadata || [] of File::Metadata::KeyValue

      # `ARROW:schema` is overwritten if it already exists.
      if index = meta.index { |entry| entry.key == ARROW_SCHEMA_META_KEY }
        meta.delete_at(index)
      end
      meta << schema_kv

      props.key_value_metadata = meta
    end

    # Upstream `flatten_ree_field`.
    private def self.flatten_ree_field(field : ::Arrow::Field) : ::Arrow::Field
      if field.data_type.kind.run_end_encoded?
        values = field.data_type.field2 || raise general_err!("RunEndEncoded is missing its values field")
        retyped_field(field, values.data_type)
      else
        field
      end
    end

    # Returns a copy of `field` with a new name.
    #
    # Workaround: arrow-cr's `Field#with_name`/`#with_data_type` mutate the
    # receiver (upstream's consume `self` and return a copy), which would corrupt
    # the caller's schema during coercion. See `shards_info/arrow_cr.1.md`.
    private def self.renamed_field(field : ::Arrow::Field, name : String) : ::Arrow::Field
      ::Arrow::Field.new(name, field.data_type, field.nullable?, field.metadata)
    end

    # Returns a copy of `field` with a new data type (see `renamed_field`).
    private def self.retyped_field(field : ::Arrow::Field, data_type : ::Arrow::DataType) : ::Arrow::Field
      ::Arrow::Field.new(field.name, data_type, field.nullable?, field.metadata)
    end

    # Upstream `logical_type_for_string` with `arrow_canonical_extension_types`
    # disabled (the default build): always the `STRING` logical type.
    def self.logical_type_for_string(_field : ::Arrow::Field) : LogicalType?
      LogicalType::String.new
    end

    # Upstream `logical_type_for_binary` with the `geospatial` feature disabled
    # (the default build): no logical type.
    def self.logical_type_for_binary(_field : ::Arrow::Field) : LogicalType?
      nil
    end

    # Upstream `logical_type_for_fixed_size_binary` with
    # `arrow_canonical_extension_types` disabled (the default build).
    def self.logical_type_for_fixed_size_binary(_field : ::Arrow::Field) : LogicalType?
      nil
    end

    # Upstream `logical_type_for_binary_view` with the `geospatial` feature
    # disabled (the default build).
    def self.logical_type_for_binary_view(_field : ::Arrow::Field) : LogicalType?
      nil
    end

    # Upstream `logical_type_for_struct` with `variant_experimental` disabled
    # (the default build).
    def self.logical_type_for_struct(_field : ::Arrow::Field) : LogicalType?
      nil
    end

    # Maps an Arrow time unit to the Parquet one (seconds have no mapping).
    private def self.parquet_time_unit(unit : ::Arrow::TimeUnit) : TimeUnit
      case unit
      in .millisecond? then TimeUnit::MILLIS
      in .microsecond? then TimeUnit::MICROS
      in .nanosecond?  then TimeUnit::NANOS
      in .second?      then raise general_err!("Cannot represent seconds in LogicalType")
      end
    end

    # Whether the field is marked `adjusted_to_utc` (used by TIME).
    private def self.adjusted_to_utc?(field : ::Arrow::Field) : Bool
      field.metadata.contains_key?("adjusted_to_utc")
    end

    # Converts an Arrow field to a Parquet `Type` (upstream
    # `arrow_to_parquet_type`).
    #
    # ameba:disable Metrics/CyclomaticComplexity
    def self.arrow_to_parquet_type(field : ::Arrow::Field, coerce_types : Bool) : Schema::Type
      data_type = field.data_type
      name = field.name
      repetition = field.nullable? ? Repetition::OPTIONAL : Repetition::REQUIRED
      id = field_id(field)

      primitive = ->(physical_type : Parquet::Type) do
        Schema::Type.primitive_type_builder(name, physical_type).with_repetition(repetition).with_id(id)
      end

      case data_type.kind
      when ::Arrow::DataType::Kind::Null
        primitive.call(Parquet::Type::INT32).with_logical_type(LogicalType::Unknown.new).build
      when ::Arrow::DataType::Kind::Boolean
        primitive.call(Parquet::Type::BOOLEAN).build
      when ::Arrow::DataType::Kind::Int8
        primitive.call(Parquet::Type::INT32).with_logical_type(LogicalType.integer(8, true)).build
      when ::Arrow::DataType::Kind::Int16
        primitive.call(Parquet::Type::INT32).with_logical_type(LogicalType.integer(16, true)).build
      when ::Arrow::DataType::Kind::Int32
        primitive.call(Parquet::Type::INT32).build
      when ::Arrow::DataType::Kind::Int64
        primitive.call(Parquet::Type::INT64).build
      when ::Arrow::DataType::Kind::UInt8
        primitive.call(Parquet::Type::INT32).with_logical_type(LogicalType.integer(8, false)).build
      when ::Arrow::DataType::Kind::UInt16
        primitive.call(Parquet::Type::INT32).with_logical_type(LogicalType.integer(16, false)).build
      when ::Arrow::DataType::Kind::UInt32
        primitive.call(Parquet::Type::INT32).with_logical_type(LogicalType.integer(32, false)).build
      when ::Arrow::DataType::Kind::UInt64
        primitive.call(Parquet::Type::INT64).with_logical_type(LogicalType.integer(64, false)).build
      when ::Arrow::DataType::Kind::Float16
        primitive.call(Parquet::Type::FIXED_LEN_BYTE_ARRAY)
          .with_logical_type(LogicalType::Float16.new)
          .with_length(2)
          .build
      when ::Arrow::DataType::Kind::Float32
        primitive.call(Parquet::Type::FLOAT).build
      when ::Arrow::DataType::Kind::Float64
        primitive.call(Parquet::Type::DOUBLE).build
      when ::Arrow::DataType::Kind::Timestamp
        unit = data_type.time_unit || raise general_err!("Timestamp is missing its time unit")
        if unit.second?
          # Cannot represent seconds in LogicalType
          primitive.call(Parquet::Type::INT64).build
        else
          tz = data_type.timezone
          adjusted = !tz.nil? && !tz.empty?
          primitive.call(Parquet::Type::INT64)
            .with_logical_type(LogicalType.timestamp(adjusted, parquet_time_unit(unit)))
            .build
        end
      when ::Arrow::DataType::Kind::Date32
        primitive.call(Parquet::Type::INT32).with_logical_type(LogicalType::Date.new).build
      when ::Arrow::DataType::Kind::Date64
        if coerce_types
          primitive.call(Parquet::Type::INT32).with_logical_type(LogicalType::Date.new).build
        else
          primitive.call(Parquet::Type::INT64).build
        end
      when ::Arrow::DataType::Kind::Time32
        unit = data_type.time_unit || raise general_err!("Time32 is missing its time unit")
        if unit.second?
          # Cannot represent seconds in LogicalType
          primitive.call(Parquet::Type::INT32).build
        elsif unit.millisecond?
          primitive.call(Parquet::Type::INT32)
            .with_logical_type(LogicalType.time(adjusted_to_utc?(field), TimeUnit::MILLIS))
            .build
        else
          raise general_err!("Invalid unit for Time32: #{unit}")
        end
      when ::Arrow::DataType::Kind::Time64
        unit = data_type.time_unit || raise general_err!("Time64 is missing its time unit")
        unless unit.microsecond? || unit.nanosecond?
          raise general_err!("Invalid unit for Time64: #{unit}")
        end

        primitive.call(Parquet::Type::INT64)
          .with_logical_type(LogicalType.time(adjusted_to_utc?(field), parquet_time_unit(unit)))
          .build
      when ::Arrow::DataType::Kind::Duration
        primitive.call(Parquet::Type::INT64).build
      when ::Arrow::DataType::Kind::Interval
        primitive.call(Parquet::Type::FIXED_LEN_BYTE_ARRAY)
          .with_converted_type(ConvertedType::INTERVAL)
          .with_length(12)
          .build
      when ::Arrow::DataType::Kind::Binary, ::Arrow::DataType::Kind::LargeBinary
        primitive.call(Parquet::Type::BYTE_ARRAY)
          .with_logical_type(logical_type_for_binary(field))
          .build
      when ::Arrow::DataType::Kind::FixedSizeBinary
        width = data_type.byte_width || raise general_err!("FixedSizeBinary is missing its byte width")
        primitive.call(Parquet::Type::FIXED_LEN_BYTE_ARRAY)
          .with_length(width)
          .with_logical_type(logical_type_for_fixed_size_binary(field))
          .build
      when ::Arrow::DataType::Kind::BinaryView
        primitive.call(Parquet::Type::BYTE_ARRAY)
          .with_logical_type(logical_type_for_binary_view(field))
          .build
      when ::Arrow::DataType::Kind::Decimal32, ::Arrow::DataType::Kind::Decimal64,
           ::Arrow::DataType::Kind::Decimal128, ::Arrow::DataType::Kind::Decimal256
        precision = data_type.precision || raise general_err!("Decimal is missing its precision")
        scale = data_type.scale || raise general_err!("Decimal is missing its scale")

        # Decimal precision determines the Parquet physical type to use, per
        # https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
        physical_type, length =
          if precision > 1 && precision <= 9
            {Parquet::Type::INT32, -1}
          elsif precision <= 18
            {Parquet::Type::INT64, -1}
          else
            {Parquet::Type::FIXED_LEN_BYTE_ARRAY, decimal_length_from_precision(precision.to_i32)}
          end

        primitive.call(physical_type)
          .with_length(length)
          .with_logical_type(LogicalType.decimal(scale.to_i32, precision.to_i32))
          .with_precision(precision.to_i32)
          .with_scale(scale.to_i32)
          .build
      when ::Arrow::DataType::Kind::Utf8, ::Arrow::DataType::Kind::LargeUtf8,
           ::Arrow::DataType::Kind::Utf8View
        primitive.call(Parquet::Type::BYTE_ARRAY)
          .with_logical_type(logical_type_for_string(field))
          .build
      when ::Arrow::DataType::Kind::List, ::Arrow::DataType::Kind::FixedSizeList,
           ::Arrow::DataType::Kind::LargeList, ::Arrow::DataType::Kind::ListView,
           ::Arrow::DataType::Kind::LargeListView
        element = data_type.field || raise general_err!("List is missing its element field")
        element_type =
          if coerce_types && element.name != PARQUET_LIST_ELEMENT_NAME
            # Ensure proper naming per the Parquet specification
            arrow_to_parquet_type(renamed_field(element, PARQUET_LIST_ELEMENT_NAME), coerce_types)
          else
            arrow_to_parquet_type(element, coerce_types)
          end

        list_group = Schema::Type.group_type_builder("list")
          .with_fields([element_type])
          .with_repetition(Repetition::REPEATED)
          .build

        Schema::Type.group_type_builder(name)
          .with_fields([list_group.as(Schema::Type)])
          .with_logical_type(LogicalType::List.new)
          .with_repetition(repetition)
          .with_id(id)
          .build
      when ::Arrow::DataType::Kind::Struct
        children = data_type.fields
        if children.nil? || children.empty?
          raise arrow_err!("Parquet does not support writing empty structs")
        end

        fields = children.map { |child| arrow_to_parquet_type(child, coerce_types) }
        Schema::Type.group_type_builder(name)
          .with_fields(fields)
          .with_repetition(repetition)
          .with_id(id)
          .with_logical_type(logical_type_for_struct(field))
          .build
      when ::Arrow::DataType::Kind::Map
        entry = data_type.field
        unless entry && entry.data_type.kind.struct?
          raise arrow_err!("DataType::Map should contain a struct field child")
        end

        struct_fields = entry.data_type.fields || raise general_err!("Map entry is missing its fields")
        unless struct_fields.size == 2
          raise arrow_err!("DataType::Map should contain a struct field child")
        end

        # If coercing then set the inner struct name to "key_value" and ensure
        # the struct fields are named "key" and "value".
        map_struct_name = coerce_types ? PARQUET_MAP_STRUCT_NAME : entry.name
        fix_map_field = ->(wanted : String, child : ::Arrow::Field) do
          if coerce_types && child.name != wanted
            arrow_to_parquet_type(renamed_field(child, wanted), coerce_types)
          else
            arrow_to_parquet_type(child, coerce_types)
          end
        end

        key_field = fix_map_field.call(PARQUET_KEY_FIELD_NAME, struct_fields[0])
        val_field = fix_map_field.call(PARQUET_VALUE_FIELD_NAME, struct_fields[1])

        entries_group = Schema::Type.group_type_builder(map_struct_name)
          .with_fields([key_field, val_field])
          .with_repetition(Repetition::REPEATED)
          .build

        Schema::Type.group_type_builder(name)
          .with_fields([entries_group.as(Schema::Type)])
          .with_logical_type(LogicalType::Map.new)
          .with_repetition(repetition)
          .with_id(id)
          .build
      when ::Arrow::DataType::Kind::Union
        raise nyi_err!("See ARROW-8817.")
      when ::Arrow::DataType::Kind::Dictionary
        # Dictionary encoding is not handled at the schema level
        value = data_type.value || raise general_err!("Dictionary is missing its value type")
        arrow_to_parquet_type(retyped_field(field, value), coerce_types)
      when ::Arrow::DataType::Kind::RunEndEncoded
        values = data_type.field2 || raise general_err!("RunEndEncoded is missing its values field")
        arrow_to_parquet_type(retyped_field(field, values.data_type), coerce_types)
      else
        raise nyi_err!("Arrow to Parquet conversion for type #{data_type} is not implemented")
      end
    end

    # Converts an Arrow schema to a Parquet schema
    # (upstream `ArrowSchemaConverter`).
    class ArrowSchemaConverter
      def initialize(@schema_root : String = "arrow_schema", @coerce_types : Bool = false)
      end

      # Should Arrow types be coerced into Parquet native types (default `false`).
      #
      # Coercion produces Parquet files that other readers understand without
      # consulting the embedded Arrow schema, but loses information for types
      # with no native Parquet counterpart (`Date64`, `Timestamp`, `Interval`)
      # and replaces list/map field names with the ones the Parquet spec
      # prescribes.
      def with_coerce_types(coerce_types : Bool) : self
        @coerce_types = coerce_types
        self
      end

      # Sets the root schema element name (default `"arrow_schema"`).
      def schema_root(schema_root : String) : self
        @schema_root = schema_root
        self
      end

      # Converts the given Arrow `Schema` into a `SchemaDescriptor`.
      def convert(schema : ::Arrow::Schema) : Schema::SchemaDescriptor
        fields = schema.fields.map { |field| Parquet::Arrow.arrow_to_parquet_type(field, @coerce_types) }
        group = Schema::Type.group_type_builder(@schema_root).with_fields(fields).build
        Schema::SchemaDescriptor.new(group)
      end
    end
  end
end
