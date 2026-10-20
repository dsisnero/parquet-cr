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
# Deferred (see `plans/parity.md`): shredded variants — `typed_value`
# interpretation (`typed_value_to_variant`), `ShreddedVariantFieldArray`, the
# shredding-schema canonicalization (`canonicalize_shredded_types`) and the
# synthesized all-null `value` column for lenient reads. Reading a row whose
# `typed_value` is set raises `NYIError`; reading an unshredded array is
# unaffected.
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
      unless value
        raise Parquet::ArrowError.new(
          "Invalid argument error: Invalid VariantArray: StructArray must contain a 'value' field"
        )
      end

      VariantArray.validate_binary_array(value, "value")
      new(value, typed_value)
    end

    # Slices both columns (upstream `ShreddingState::slice`).
    def slice(offset : Int32, length : Int32) : ShreddingState
      ShreddingState.new(
        @value.slice(offset, length),
        @typed_value.try(&.slice(offset, length)),
      )
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
      struct_array = inner.as?(::Arrow::StructArray)
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

      new(struct_array, metadata, ShreddingState.from_struct(struct_array))
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

    # The bytes of a binary-like array at `index`, or `nil` when the value is
    # null or the type is not binary-like (upstream `binary_array_value`).
    def self.binary_array_value(array : ::Arrow::ArrayBase, index : Int32) : Bytes?
      nulls = array.nulls
      return nil if nulls && !nulls.valid?(index)

      case array.data_type.kind
      when .binary?
        array.as(::Arrow::GenericBinaryArray(Int32)).value(index)
      when .large_binary?
        array.as(::Arrow::GenericBinaryArray(Int64)).value(index)
      when .binary_view?
        array.as(::Arrow::BinaryViewArray).value(index)
      when .dictionary?
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
    # bytes are not valid, or when the row is shredded (deferred).
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
        raise nyi_err!("shredded variant values (typed_value) are not ported")
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
end
