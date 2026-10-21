require "arrow-cr"
require "../parquet/errors"
require "../parquet_variant"

# Ported from `parquet-variant-compute/src/variant_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `VariantArray` wraps an Arrow `StructArray` holding the `metadata` and `value`
# columns of the [Parquet Variant] binary encoding and exposes the decoded
# `Variant` values, together with the canonical `arrow.parquet.variant`
# extension type and the shredding state of the array.
#
# Shredded arrays support primitive typed_value reads, schema canonicalization,
# and lenient missing-value reads. Composite typed values require
# unshred_variant before materializing Variant values, matching upstream.
#
# Divergences: `usize` counts are `Int32`; `arrow_schema::ArrowError` is
# `Parquet::ArrowError`; upstream's panicking `value()` raises instead;
# `try_new` is a class method that raises (`Result` is not modelled); the
# RunEndEncoded metadata encoding is not ported (only Binary, LargeBinary,
# BinaryView and Dictionary), and `Arrow::ArrayBase#==` provides the upstream
# `PartialEq` on the wrapped struct array.
#
# [Parquet Variant]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
module ParquetVariantCompute
  # Arrow Variant extension type (upstream `VariantType`).
  #
  # Represents the canonical Arrow extension type for storing variants. The
  # extension metadata is the empty string.
  class VariantType < ::Arrow::Extension::ExtensionType(String)
    # Upstream `ExtensionType::NAME`.
    NAME = "arrow.parquet.variant"

    def self.extension_name : String
      NAME
    end

    # Variants' extension metadata is an empty string.
    def self.deserialize_metadata(metadata : String?) : String
      ""
    end

    def self.try_new(data_type : ::Arrow::DataType, metadata : String) : VariantType
      validate(data_type, metadata)
      new
    end

    def self.validate(data_type : ::Arrow::DataType, metadata : String) : Nil
      supports_data_type(data_type)
    end

    # Only struct arrays are valid variant storage.
    def self.supports_data_type(data_type : ::Arrow::DataType) : Nil
      return if data_type.kind.struct?

      raise Parquet::ArrowError.new(
        "Invalid argument error: VariantType only supports StructArray, got #{data_type}"
      )
    end

    def metadata : String
      ""
    end

    def serialize_metadata : String?
      ""
    end

    def supports_data_type(data_type : ::Arrow::DataType) : Nil
      VariantType.supports_data_type(data_type)
    end
  end

  # The shredding state of a `VariantArray` (upstream `ShreddingState`).
  #
  # A variant array always has a `value` column and may have a `typed_value`
  # column when it is shredded. Shredded-value interpretation is deferred (see
  # the module comment), but the columns are tracked so the unshredded rows of a
  # shredded array are still readable.
  class ShreddingState
    # The `value` column.
    getter value : ::Arrow::ArrayBase
    # The `typed_value` column, if the array is shredded.
    getter typed_value : ::Arrow::ArrayBase?

    def initialize(@value : ::Arrow::ArrayBase, @typed_value : ::Arrow::ArrayBase?)
    end

    # Builds the state from the inner struct array (upstream
    # `TryFrom<&StructArray>`).
    def self.from_struct(inner : ::Arrow::StructArray) : ShreddingState
      typed_value = inner.column_by_name("typed_value")
      value = inner.column_by_name("value")

      if value
        VariantArray.validate_binary_array(value, "value")
        return new(value, typed_value)
      end

      # Lenient read: a shredded group may omit the spec-required `value`.
      # Synthesize an all-null one so the model always has a `value`.
      unless typed_value
        raise Parquet::ArrowError.new(
          "Invalid argument error: Invalid VariantArray: StructArray must contain a 'value' field"
        )
      end
      new(VariantArray.all_null_value_column(inner.len), typed_value)
    end

    # Slices both columns (upstream `ShreddingState::slice`).
    def slice(offset : Int32, length : Int32) : ShreddingState
      ShreddingState.new(
        @value.slice(offset, length),
        @typed_value.try(&.slice(offset, length)),
      )
    end
  end

  # Builds struct arrays from component fields (upstream `StructArrayBuilder`).
  #
  # Divergence: upstream keeps this `pub(crate)` (a TODO says it belongs in the
  # arrow crate); the port exposes it because `VariantArray.from_parts` and
  # `ShreddedVariantFieldArray` use it and Crystal has no crate-private
  # visibility.
  class StructArrayBuilder
    def initialize
      @fields = [] of ::Arrow::Field
      @arrays = [] of ::Arrow::ArrayBase
      @nulls = nil.as(::Arrow::NullBuffer?)
    end

    # Adds `array` as a field with the given name (nullability is explicit).
    def with_field(
      field_name : String,
      array : ::Arrow::ArrayBase,
      nullable : Bool,
    ) : self
      @fields << ::Arrow::Field.new(field_name, array.data_type, nullable)
      @arrays << array
      self
    end

    # Adds `array` using a caller-supplied field, keeping its metadata (e.g. an
    # extension type).
    def with_field_ref(field : ::Arrow::Field, array : ::Arrow::ArrayBase) : self
      @fields << field
      @arrays << array
      self
    end

    # Sets the struct's null buffer.
    def with_nulls(nulls : ::Arrow::NullBuffer) : self
      @nulls = nulls
      self
    end

    def build : ::Arrow::StructArray
      ::Arrow::StructArray.try_new(::Arrow::Fields.new(@fields), @arrays, @nulls)
    end
  end

  # An array of Parquet `Variant` values (upstream `VariantArray`).
  #
  # A `VariantArray` wraps an Arrow `StructArray` that stores the underlying
  # `metadata` and `value` fields, and adds convenience methods to access the
  # `Variant`s. Use `VariantArrayBuilder` to construct one row by row.
  class VariantArray
    # The underlying struct array.
    @inner : ::Arrow::StructArray
    # The metadata column of this variant (Binary, LargeBinary or BinaryView),
    # possibly dictionary-encoded.
    @metadata : ::Arrow::ArrayBase
    # How this variant array is shredded.
    @shredding_state : ShreddingState

    def initialize(
      @inner : ::Arrow::StructArray,
      @metadata : ::Arrow::ArrayBase,
      @shredding_state : ShreddingState,
    )
    end

    # Creates a new `VariantArray` from a struct array (upstream `try_new`).
    #
    # The struct must contain a required `metadata` field that is Binary,
    # LargeBinary or BinaryView (optionally dictionary-encoded) and a required
    # `value` field that is one of those binary types. An optional `typed_value`
    # field marks the array as shredded.
    def self.try_new(inner : ::Arrow::ArrayBase) : VariantArray
      # Canonicalize shredded typed_value fields (e.g. decimal narrowing and
      # UUID tagging) before inspecting the struct.
      canonical = canonicalize_shredded_types(inner)

      struct_array = canonical.as?(::Arrow::StructArray)
      unless struct_array
        raise Parquet::ArrowError.new(
          "Invalid argument error: Invalid VariantArray: requires StructArray as input"
        )
      end

      # The specification allows any field order, so fields are searched by name.
      metadata = struct_array.column_by_name("metadata")
      unless metadata
        raise Parquet::ArrowError.new(
          "Invalid argument error: Invalid VariantArray: StructArray must contain a 'metadata' field"
        )
      end
      validate_metadata_array(metadata)

      shredding_state = ShreddingState.from_struct(struct_array)

      # `from_struct` synthesizes an all-null `value` when the input omits it.
      # Rebuild the inner struct so `inner` and write-back carry the column too.
      if struct_array.column_by_name("value").nil?
        return from_parts(
          metadata,
          shredding_state.value,
          shredding_state.typed_value,
          struct_array.nulls,
        )
      end

      new(struct_array, metadata, shredding_state)
    end

    # Whether `data_type` is one of the binary types a variant column uses.
    def self.binary_data_type?(data_type : ::Arrow::DataType) : Bool
      case data_type.kind
      when .binary?, .large_binary?, .binary_view?
        true
      else
        false
      end
    end

    # Validates that a `value` column has a binary data type (upstream
    # `validate_binary_array`).
    def self.validate_binary_array(array : ::Arrow::ArrayBase, field_name : String) : Nil
      return if binary_data_type?(array.data_type)

      raise Parquet::ArrowError.new(
        "Invalid argument error: VariantArray '#{field_name}' field must be Binary, " \
        "LargeBinary, or BinaryView, got #{array.data_type}"
      )
    end

    # Validates that a `metadata` column has binary-like logical values
    # (upstream `validate_metadata_array`).
    def self.validate_metadata_array(array : ::Arrow::ArrayBase) : Nil
      data_type = array.data_type
      return if binary_data_type?(data_type)

      if data_type.kind.dictionary?
        value = data_type.value
        return if value && binary_data_type?(value)
      end

      raise Parquet::ArrowError.new(
        "Invalid argument error: VariantArray 'metadata' field must be Binary, LargeBinary, " \
        "BinaryView, or a Dictionary or RunEndEncoded array of one of those types, " \
        "got #{data_type}"
      )
    end

    # An all-null `value` column of the given length (upstream
    # `all_null_value_column`).
    #
    # The shredding spec requires the `value` column to always be present in the
    # schema, so producers with no unshredded values still emit an all-null one.
    def self.all_null_value_column(len : Int32) : ::Arrow::ArrayBase
      ::Arrow.new_null_array(::Arrow::DataType.binary_view, len)
    end

    # Creates a shredded variant array from its columns (upstream `from_parts`).
    #
    # Divergence: upstream keeps this `pub(crate)`; Crystal has no crate-private
    # visibility, so it is public (the lenient read of a missing `value` column
    # and `ShreddedVariantFieldArray` use it).
    def self.from_parts(
      metadata : ::Arrow::ArrayBase,
      value : ::Arrow::ArrayBase,
      typed_value : ::Arrow::ArrayBase?,
      nulls : ::Arrow::NullBuffer?,
    ) : VariantArray
      from_parts_with_nullable_value(metadata, value, typed_value, nulls, true)
    end

    # Creates an unshredded variant array, annotating `value` as non-nullable as
    # the spec requires when there is no `typed_value` column (upstream
    # `from_parts_unshredded`).
    def self.from_parts_unshredded(
      metadata : ::Arrow::ArrayBase,
      value : ::Arrow::ArrayBase,
      nulls : ::Arrow::NullBuffer?,
    ) : VariantArray
      from_parts_with_nullable_value(metadata, value, nil, nulls, false)
    end

    private def self.from_parts_with_nullable_value(
      metadata : ::Arrow::ArrayBase,
      value : ::Arrow::ArrayBase,
      typed_value : ::Arrow::ArrayBase?,
      nulls : ::Arrow::NullBuffer?,
      value_nullable : Bool,
    ) : VariantArray
      builder = StructArrayBuilder.new
        .with_field("metadata", metadata, false)
        .with_field("value", value, value_nullable)
      if typed_value
        builder = builder.with_field_ref(typed_value_field(typed_value), typed_value)
      end
      builder = builder.with_nulls(nulls) if nulls

      new(
        builder.build,
        metadata,
        ShreddingState.new(value, typed_value),
      )
    end

    # Builds the `typed_value` field for a shredded column (upstream
    # `typed_value_field`).
    #
    # The variant spec maps `FixedSizeBinary(16)` exclusively to UUID, so such a
    # column carries the canonical `arrow.uuid` extension metadata even when the
    # writer omitted it.
    def self.typed_value_field(array : ::Arrow::ArrayBase) : ::Arrow::Field
      field = ::Arrow::Field.new("typed_value", array.data_type, true)
      if uuid_storage?(array.data_type)
        field = copy_field_with_extension(
          field, array.data_type, ::Arrow::Extension::Canonical::Uuid.new
        )
      end
      field
    end

    # Canonicalizes shredded typed_value fields (e.g. decimal narrowing) and
    # verifies that every data type in the array is legal for a variant array
    # (upstream `canonicalize_shredded_types`).
    def self.canonicalize_shredded_types(array : ::Arrow::ArrayBase) : ::Arrow::ArrayBase
      new_type = canonicalize_and_verify_data_type_impl(array.data_type, true)
      return array if new_type == array.data_type

      ::Arrow.cast(array, new_type)
    end

    # Recursively verifies that `data_type` can legally appear in a (possibly
    # shredded) variant array, narrowing decimals to the smallest valid precision
    # (upstream `canonicalize_and_verify_data_type`).
    #
    # Divergence: upstream keeps this private (its unit test lives in the same
    # module); the port exposes it so the ported test can call it.
    def self.canonicalize_and_verify_data_type(data_type : ::Arrow::DataType) : ::Arrow::DataType
      canonicalize_and_verify_data_type_impl(data_type, false)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def self.canonicalize_and_verify_data_type_impl(
      data_type : ::Arrow::DataType,
      skip_top_level_metadata : Bool,
    ) : ::Arrow::DataType
      kind = data_type.kind

      # Primitive arrow types that have a direct variant counterpart are allowed.
      return data_type if kind.null? || kind.boolean? || kind.int8? || kind.int16? ||
                          kind.int32? || kind.int64? || kind.float32? || kind.float64? ||
                          kind.binary? || kind.large_binary? || kind.binary_view? ||
                          kind.utf8? || kind.large_utf8? || kind.utf8_view? || kind.date32?

      case kind
      when .decimal32?, .decimal64?, .decimal128?
        precision = data_type.precision || 0_u8
        scale = data_type.scale || 0_i8
        valid4 = ParquetVariant::VariantDecimal4.is_valid_precision_and_scale(precision, scale)
        valid8 = ParquetVariant::VariantDecimal8.is_valid_precision_and_scale(precision, scale)
        valid16 = ParquetVariant::VariantDecimal16.is_valid_precision_and_scale(precision, scale)

        # arrow-parquet widens 32- and 64-bit decimals to 128-bit, but the
        # variant spec requires the narrowest decimal type for a precision.
        return ::Arrow::DataType.decimal32(precision, scale) if kind.decimal64? && valid4
        if kind.decimal128?
          return ::Arrow::DataType.decimal32(precision, scale) if valid4
          return ::Arrow::DataType.decimal64(precision, scale) if valid8
        end

        return data_type if (kind.decimal32? && valid4) ||
                            (kind.decimal64? && valid8) ||
                            (kind.decimal128? && valid16)
      when .timestamp?
        # Only microsecond and nanosecond timestamps are allowed.
        unit = data_type.time_unit
        return data_type if unit == ::Arrow::TimeUnit::Microsecond || unit == ::Arrow::TimeUnit::Nanosecond
      when .time64?
        return data_type if data_type.time_unit.try(&.microsecond?)
      when .fixed_size_binary?
        # UUID maps to 16-byte fixed-size binary; no other width is allowed.
        return data_type if data_type.byte_width == 16
      when .list?, .large_list?, .list_view?, .large_list_view?
        field = data_type.field
        unless field
          raise ParquetVariant::InvalidArgumentError.new("Illegal shredded value type: #{data_type}")
        end
        new_field = canonicalize_and_verify_field(field)
        return data_type if new_field.same?(field)

        return case kind
        when .list?       then ::Arrow::DataType.list(new_field)
        when .large_list? then ::Arrow::DataType.large_list(new_field)
        when .list_view?  then ::Arrow::DataType.list_view(new_field)
        else                   ::Arrow::DataType.large_list_view(new_field)
        end
      when .struct?
        fields = data_type.fields
        unless fields
          raise ParquetVariant::InvalidArgumentError.new("Illegal shredded value type: #{data_type}")
        end
        new_fields = fields.map do |field|
          if skip_top_level_metadata && field.name == "metadata"
            field
          else
            canonicalize_and_verify_field(field)
          end
        end
        return data_type if new_fields.size == fields.size &&
                            new_fields.each_with_index.all? { |field, i| field.same?(fields[i]) }

        return ::Arrow::DataType.struct(::Arrow::Fields.new(new_fields))
      end

      # Unsigned integers, half-float, wide/fixed-size decimals, other temporal
      # types, maps, unions, dictionaries and run-end encoded arrays are illegal.
      raise ParquetVariant::InvalidArgumentError.new("Illegal shredded value type: #{data_type}")
    end

    # Canonicalizes one field of a shredded type and tags 16-byte fixed-size
    # binary leaves with the UUID extension (upstream
    # `canonicalize_and_verify_field`).
    private def self.canonicalize_and_verify_field(field : ::Arrow::Field) : ::Arrow::Field
      new_data_type = canonicalize_and_verify_data_type(field.data_type)

      # A shredded FixedSizeBinary(16) column is always a UUID. Tag it on read,
      # as a safety net against writers that omit the extension metadata.
      if uuid_storage?(new_data_type) &&
         !field.has_valid_extension_type?(::Arrow::Extension::Canonical::Uuid)
        return copy_field_with_extension(
          field, new_data_type, ::Arrow::Extension::Canonical::Uuid.new
        )
      end

      return field if new_data_type == field.data_type

      copy_field_with_extension(field, new_data_type, nil)
    end

    # Whether `data_type` is the UUID storage type (`FixedSizeBinary(16)`).
    private def self.uuid_storage?(data_type : ::Arrow::DataType) : Bool
      data_type.kind.fixed_size_binary? && data_type.byte_width == 16
    end

    # Copies `field` with a new data type and/or extension type.
    #
    # Divergence: the port's `Field#with_*` helpers mutate, so canonicalization
    # builds a fresh `Field` with a copied `Metadata` instead (upstream clones
    # the `Arc<Field>`, whose metadata is immutable).
    private def self.copy_field_with_extension(
      field : ::Arrow::Field,
      data_type : ::Arrow::DataType,
      extension_type : ::Arrow::Extension::ExtensionType?,
    ) : ::Arrow::Field
      metadata = ::Arrow::Metadata.from(field.metadata.to_h)
      copy = ::Arrow::Field.new(field.name, data_type, field.nullable?, metadata)
      copy.try_with_extension_type(extension_type) if extension_type
      copy
    end

    # Converts the non-null `typed_value` at `index` to a `Variant` (upstream
    # `typed_value_to_variant`).
    #
    # Composite shredded values may need to combine `value` and `typed_value` and
    # allocate new encoded bytes; upstream raises `NotYetImplemented` for them
    # (callers must unshred first), which the port mirrors with `NYIError`.
    # ameba:disable Metrics/CyclomaticComplexity
    def self.typed_value_to_variant(
      typed_value : ::Arrow::ArrayBase,
      index : Int32,
    ) : ParquetVariant::Variant
      data_type = typed_value.data_type

      case data_type.kind
      when .null?
        return ParquetVariant::Variant::Null.new
      when .boolean?
        return ParquetVariant::Variant.from(typed_value.as(::Arrow::BooleanArray).value(index))
      when .fixed_size_binary?
        # 16-byte FixedSizeBinary always corresponds to a UUID.
        bytes = typed_value.as(::Arrow::FixedSizeBinaryArray).value(index)
        return ParquetVariant::Variant.from(::UUID.new(bytes))
      when .binary?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::GenericBinaryArray(Int32)).value(index)
        )
      when .large_binary?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::GenericBinaryArray(Int64)).value(index)
        )
      when .binary_view?
        return ParquetVariant::Variant.from(typed_value.as(::Arrow::BinaryViewArray).value(index))
      when .utf8?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::GenericStringArray(Int32)).value(index)
        )
      when .large_utf8?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::GenericStringArray(Int64)).value(index)
        )
      when .utf8_view?
        return ParquetVariant::Variant.from(typed_value.as(::Arrow::StringViewArray).value(index))
      when .int8?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::PrimitiveArray(Int8)).value(index)
        )
      when .int16?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::PrimitiveArray(Int16)).value(index)
        )
      when .int32?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::PrimitiveArray(Int32)).value(index)
        )
      when .int64?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::PrimitiveArray(Int64)).value(index)
        )
      when .float16?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::PrimitiveArray(::Arrow::Float16)).value(index).to_f32
        )
      when .float32?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::PrimitiveArray(Float32)).value(index)
        )
      when .float64?
        return ParquetVariant::Variant.from(
          typed_value.as(::Arrow::PrimitiveArray(Float64)).value(index)
        )
      when .decimal32?
        value = typed_value.as(::Arrow::PrimitiveArray(Int32)).value(index)
        return cast_decimal(data_type, index) do
          ParquetVariant::Variant.from(
            ParquetVariant::VariantDecimal4.try_new(value, decimal_scale(data_type))
          )
        end
      when .decimal64?
        value = typed_value.as(::Arrow::PrimitiveArray(Int64)).value(index)
        return cast_decimal(data_type, index) do
          ParquetVariant::Variant.from(
            ParquetVariant::VariantDecimal8.try_new(value, decimal_scale(data_type))
          )
        end
      when .decimal128?
        value = typed_value.as(::Arrow::PrimitiveArray(Int128)).value(index)
        return cast_decimal(data_type, index) do
          ParquetVariant::Variant.from(
            ParquetVariant::VariantDecimal16.try_new(value, decimal_scale(data_type))
          )
        end
      when .date32?
        days = typed_value.as(::Arrow::PrimitiveArray(Int32)).value(index)
        return ParquetVariant::Variant.from_date(::Time.utc(1970, 1, 1) + days.days)
      when .time64?
        value = typed_value.as(::Arrow::PrimitiveArray(Int64)).value(index)
        if value < 0 || value >= 86_400_000_000_i64
          raise ParquetVariant::CastError.new(
            "Cast failed at index #{index} (array type: #{data_type}): " \
            "Invalid microsecond from midnight: #{value}"
          )
        end
        return ParquetVariant::Variant.from_time(::Time::Span.new(nanoseconds: value * 1000))
      when .timestamp?
        value = typed_value.as(::Arrow::PrimitiveArray(Int64)).value(index)
        time =
          if data_type.time_unit.try(&.nanosecond?)
            ::Time.unix_ns(value)
          else
            timestamp_micros_to_time(value)
          end
        if data_type.timezone
          return ParquetVariant::Variant.from_datetime_utc(time)
        end
        return ParquetVariant::Variant.from_naive_datetime(time)
      end

      raise nyi_err!(
        "Not yet implemented: VariantArray::try_value cannot materialize typed_value of " \
        "type #{data_type} as a borrowed Variant; call unshred_variant first"
      )
    end

    private def self.decimal_scale(data_type : ::Arrow::DataType) : UInt8
      (data_type.scale || 0_i8).to_u8
    end

    # Runs a decimal conversion, wrapping the decimal error as a cast error like
    # upstream's `generic_conversion_single_value_with_result!`.
    private def self.cast_decimal(
      data_type : ::Arrow::DataType,
      index : Int32,
      & : -> ParquetVariant::Variant
    ) : ParquetVariant::Variant
      yield
    rescue error : ParquetVariant::InvalidArgumentError
      raise ParquetVariant::CastError.new(
        "Cast failed at index #{index} (array type: #{data_type}): #{error.message}"
      )
    end

    # Converts microsecond-since-epoch to a `Time`.
    private def self.timestamp_micros_to_time(micros : Int64) : ::Time
      ::Time.unix(micros // 1_000_000) +
        ::Time::Span.new(nanoseconds: (micros % 1_000_000) * 1000)
    end

    # The bytes of a binary-like array at `index`, or `nil` when the value is
    # null or the type is not binary-like (upstream `binary_array_value`).
    def self.binary_array_value(array : ::Arrow::ArrayBase, index : Int32) : Bytes?
      nulls = array.nulls
      return nil if nulls && !nulls.valid?(index)

      case array
      when ::Arrow::GenericBinaryArray(Int32), ::Arrow::GenericBinaryArray(Int64), ::Arrow::BinaryViewArray
        array.value(index)
      when ::Arrow::DictionaryArray(Int8), ::Arrow::DictionaryArray(Int16),
           ::Arrow::DictionaryArray(Int32), ::Arrow::DictionaryArray(Int64)
        dictionary_array_value(array, index)
      else
        nil
      end
    end

    # Resolves a dictionary-encoded value through its keys.
    private def self.dictionary_array_value(array : ::Arrow::ArrayBase, index : Int32) : Bytes?
      case array
      when ::Arrow::DictionaryArray(Int8), ::Arrow::DictionaryArray(Int16),
           ::Arrow::DictionaryArray(Int32), ::Arrow::DictionaryArray(Int64)
        key = array.key(index)
        return nil unless key
        binary_array_value(array.values, key.to_i32)
      else
        nil
      end
    end

    # The decoded variant at `index` (upstream `value`).
    #
    # Upstream panics on an invalid row; the port raises `Parquet::ArrowError`.
    def value(index : Int32) : ParquetVariant::Variant
      try_value(index)
    end

    # The decoded variant at `index` (upstream `try_value`).
    #
    # Raises when the index is out of bounds, when the row's `metadata`/`value`
    # bytes are not valid, or when a composite typed_value requires unshredding.
    def try_value(index : Int32) : ParquetVariant::Variant
      if len <= index
        raise Parquet::ArrowError.new(
          "Invalid argument error: Index #{index} out of bounds for VariantArray of length #{len}"
        )
      end

      value = value_column
      typed_value = typed_value_column

      # Always prefer typed_value, if available.
      if typed_value && valid_at?(typed_value, index)
        if !typed_value.data_type.kind.struct? && valid_at?(value, index)
          # Only a partially shredded struct may have values for both columns.
          raise Parquet::ArrowError.new(
            "Invalid argument error: Invalid variant, conflicting value and typed_value"
          )
        end
        return VariantArray.typed_value_to_variant(typed_value, index)
      end

      if valid_at?(value, index)
        metadata = VariantArray.binary_array_value(@metadata, index)
        value_bytes = VariantArray.binary_array_value(value, index)
        if metadata && value_bytes
          return ParquetVariant::Variant.new(metadata, value_bytes)
        end

        raise Parquet::ArrowError.new(
          "Invalid argument error: metadata and value fields must be binary-like arrays, " \
          "instead got #{@metadata.data_type} and #{value.data_type}"
        )
      end

      # It is technically invalid for both value and typed_value to be null, but
      # the spec requires readers to return `Variant::Null` in this case.
      ParquetVariant::Variant::Null.new
    end

    # A reference to the `metadata` column of the struct array.
    def metadata_column : ::Arrow::ArrayBase
      @metadata
    end

    # A reference to the `value` column of the struct array.
    def value_column : ::Arrow::ArrayBase
      @shredding_state.value
    end

    # A reference to the `typed_value` column, if the array is shredded.
    def typed_value_column : ::Arrow::ArrayBase?
      @shredding_state.typed_value
    end

    # How this array is shredded.
    def shredding_state : ShreddingState
      @shredding_state
    end

    # A field representing this variant array in a schema (upstream `field`).
    #
    # The field carries the `arrow.parquet.variant` extension metadata.
    def field(name : String) : ::Arrow::Field
      ::Arrow::Field.new(name, data_type, @inner.nullable?)
        .with_extension_type(VariantType.new)
    end

    # The data type of the underlying struct array.
    def data_type : ::Arrow::DataType
      @inner.data_type
    end

    # Slices the array (upstream `slice`).
    def slice(offset : Int32, length : Int32) : VariantArray
      VariantArray.new(
        @inner.slice(offset, length).as(::Arrow::StructArray),
        @metadata.slice(offset, length),
        @shredding_state.slice(offset, length),
      )
    end

    def len : Int32
      @inner.len
    end

    # ameba:disable Naming/PredicateName
    def is_empty? : Bool
      @inner.len == 0
    end

    # The null buffer of the struct array, if it has one.
    def nulls : ::Arrow::NullBuffer?
      @inner.nulls
    end

    # Whether the element at `index` is null.
    # ameba:disable Naming/PredicateName
    def is_null(index : Int32) : Bool
      nulls.try(&.null?(index)) || false
    end

    # Whether the element at `index` is valid (not null).
    # ameba:disable Naming/PredicateName
    def is_valid(index : Int32) : Bool
      !is_null(index)
    end

    # The underlying struct array.
    def inner : ::Arrow::StructArray
      @inner
    end

    # The underlying struct array, consuming `self`.
    def into_inner : ::Arrow::StructArray
      @inner
    end

    # An iterator over the values in this array.
    def iter : VariantArrayIter
      VariantArrayIter.new(self)
    end

    # Logical equality of the wrapped struct arrays (upstream `PartialEq`).
    def ==(other : VariantArray) : Bool
      @inner == other.inner
    end

    # Builds an array from optional variants (upstream
    # `FromIterator<Option<Variant>>`).
    def self.from_iter(values : Enumerable(ParquetVariant::Variant?)) : VariantArray
      builder = VariantArrayBuilder.new(values.size)
      values.each { |value| value ? builder.append_variant(value) : builder.append_null }
      builder.build
    end

    # Builds an array from variants (upstream `FromIterator<Variant>`).
    def self.from_iter(values : Enumerable(ParquetVariant::Variant)) : VariantArray
      builder = VariantArrayBuilder.new(values.size)
      values.each { |value| builder.append_variant(value) }
      builder.build
    end

    # Whether the element at `index` is valid in `array`'s own null bitmap.
    protected def valid_at?(array : ::Arrow::ArrayBase, index : Int32) : Bool
      nulls = array.nulls
      nulls.nil? || nulls.valid?(index)
    end
  end

  # An iterator over a `VariantArray` (upstream `VariantArrayIter`).
  #
  # Yields `nil` for a null row and the decoded `Variant` otherwise.
  #
  # Divergence: Crystal iterators are not double-ended, so `next_back` is a
  # plain method and `size` is exposed so that `to_a`/`reverse_each` work (the
  # ported tests materialize and reverse instead of using `.rev()`).
  class VariantArrayIter
    include Iterator(ParquetVariant::Variant?)

    @head_i : Int32
    @tail_i : Int32

    def initialize(@array : VariantArray)
      @head_i = 0
      @tail_i = @array.len
    end

    def next : ParquetVariant::Variant? | Iterator::Stop
      return Iterator::Stop::INSTANCE if @head_i == @tail_i

      value = value_opt(@head_i)
      @head_i += 1
      value
    end

    # Yields the next value from the end of the array.
    def next_back : ParquetVariant::Variant? | Iterator::Stop
      return Iterator::Stop::INSTANCE if @head_i == @tail_i

      @tail_i -= 1
      value_opt(@tail_i)
    end

    # The number of values left in this iterator.
    def size : Int32
      @tail_i - @head_i
    end

    private def value_opt(index : Int32) : ParquetVariant::Variant?
      @array.is_valid(index) ? @array.value(index) : nil
    end
  end

  # One shredded field of a partially or perfectly shredded variant (upstream
  # `ShreddedVariantFieldArray`).
  #
  # A shredded object field is laid out as
  # `STRUCT<value: BINARY, typed_value: ...>`, where `value` is a variant value
  # when the field was not shredded, a variant object when it was partially
  # shredded, or `NULL` when it was perfectly shredded.
  #
  # Divergence: `is_null`/`is_valid`/`is_empty?` keep the upstream names (see the
  # `ameba:disable` notes); reading a composite `typed_value` still needs the
  # unshredding first, so `VariantArray#try_value` raises for those.
  class ShreddedVariantFieldArray
    @inner : ::Arrow::StructArray
    @shredding_state : ShreddingState

    def initialize(@inner : ::Arrow::StructArray, @shredding_state : ShreddingState)
    end

    # Creates a shredded field array from a struct array (upstream `try_new`).
    def self.try_new(inner : ::Arrow::ArrayBase) : ShreddedVariantFieldArray
      struct_array = inner.as?(::Arrow::StructArray)
      unless struct_array
        raise Parquet::ArrowError.new(
          "Invalid argument error: Invalid ShreddedVariantFieldArray: requires StructArray as input"
        )
      end

      shredding_state = ShreddingState.from_struct(struct_array)

      # `from_struct` synthesizes an all-null `value` when the input omits it.
      if struct_array.column_by_name("value").nil?
        return from_parts(shredding_state.value, shredding_state.typed_value, struct_array.nulls)
      end

      new(struct_array, shredding_state)
    end

    # Creates a shredded field array from its columns (upstream `from_parts`).
    def self.from_parts(
      value : ::Arrow::ArrayBase,
      typed_value : ::Arrow::ArrayBase?,
      nulls : ::Arrow::NullBuffer?,
    ) : ShreddedVariantFieldArray
      builder = StructArrayBuilder.new.with_field("value", value, true)
      if typed_value
        builder = builder.with_field_ref(VariantArray.typed_value_field(typed_value), typed_value)
      end
      builder = builder.with_nulls(nulls) if nulls

      new(builder.build, ShreddingState.new(value, typed_value))
    end

    # The shredding state of this field.
    def shredding_state : ShreddingState
      @shredding_state
    end

    # A reference to the `value` column.
    def value_column : ::Arrow::ArrayBase
      @shredding_state.value
    end

    # A reference to the `typed_value` column, if present.
    def typed_value_column : ::Arrow::ArrayBase?
      @shredding_state.typed_value
    end

    # The underlying struct array.
    def inner : ::Arrow::StructArray
      @inner
    end

    # The underlying struct array, consuming `self`.
    def into_inner : ::Arrow::StructArray
      @inner
    end

    def data_type : ::Arrow::DataType
      @inner.data_type
    end

    def len : Int32
      @inner.len
    end

    # ameba:disable Naming/PredicateName
    def is_empty? : Bool
      @inner.len == 0
    end

    def offset : Int32
      @inner.offset
    end

    # Shredded variant fields are physically non-nullable: a SQL `NULL` is
    # inferred by both `value` and `typed_value` being physically `NULL`.
    def nulls : ::Arrow::NullBuffer?
      nil
    end

    # Whether the element at `index` is null (always false; see `#nulls`).
    # ameba:disable Naming/PredicateName
    def is_null(index : Int32) : Bool
      nulls.try(&.null?(index)) || false
    end

    # Whether the element at `index` is valid (not null).
    # ameba:disable Naming/PredicateName
    def is_valid(index : Int32) : Bool
      !is_null(index)
    end
  end
end
