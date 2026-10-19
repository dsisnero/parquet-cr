require "uuid"
require "../variant"
require "../decoder"
require "../short_string"
require "./metadata"

# Ported from `parquet-variant/src/builder.rs`, `builder/object.rs`, and
# `builder/list.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ValueBuilder`, `BuilderSpecificState`, `ParentState`, and the shared
# builder plumbing (`int_size`, packed little-endian writes).
#
# Divergences: Rust's affine borrows and `Drop`-based rollback cannot be
# modelled, so unfinished child builders must be rolled back explicitly via
# `abandon` (`drop(child)` upstream); the value buffer is an
# `Array(UInt8)` spliced by index; Rust `Into<Variant>` generics are covered by
# the `VariantValue` union + `ValueConversion.to_variant`; Crystal has no
# distinct date/time types, so bare `Time` (date/UTC/naive) is not part of
# `VariantValue` — wrap such values with `Variant.from_date`/
# `from_datetime_utc`/`from_naive_datetime` (only `Time::Span` maps directly,
# via `from_time`).
module ParquetVariant
  # Native Crystal values accepted by the variant builders (upstream
  # `Into<Variant>`).
  alias VariantValue = Variant | Nil | Bool |
                       Int8 | Int16 | Int32 | Int64 |
                       UInt8 | UInt16 | UInt32 | UInt64 |
                       Float32 | Float64 |
                       String | Bytes | UUID |
                       VariantDecimal4 | VariantDecimal8 | VariantDecimal16 |
                       VariantObject | VariantList | Time::Span

  # Converts a `VariantValue` to a `Variant`.
  module ValueConversion
    def self.to_variant(value : Variant) : Variant
      value
    end

    def self.to_variant(value : Nil) : Variant
      Variant.from_nil
    end

    def self.to_variant(value : Bool) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : Int8) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : Int16) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : Int32) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : Int64) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : UInt8) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : UInt16) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : UInt32) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : UInt64) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : Float32) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : Float64) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : String) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : Bytes) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : UUID) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : VariantDecimal4) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : VariantDecimal8) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : VariantDecimal16) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : VariantObject) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : VariantList) : Variant
      Variant.from(value)
    end

    def self.to_variant(value : Time::Span) : Variant
      Variant.from_time(value)
    end
  end

  # Appends the low `nbytes` little-endian bytes of `value` to `dest`.
  def self.append_packed(dest : Array(UInt8), value : Int, nbytes : Int32) : Nil
    nbytes.times { |i| dest << ((value >> (8 * i)) & 0xFF).to_u8 }
    nil
  end

  # A growable buffer of variant value bytes (upstream `ValueBuilder`).
  class ValueBuilder
    getter buffer : Array(UInt8)

    def initialize
      @buffer = [] of UInt8
    end

    def offset : Int32
      @buffer.size
    end

    def into_inner : Bytes
      Bytes.new(@buffer.size) { |i| @buffer[i] }
    end

    def truncate(n : Int32) : Nil
      @buffer = @buffer[0, n]
      nil
    end

    # Replaces `buffer[0, index]` + `bytes` + `buffer[index..]`.
    def splice(index : Int32, bytes : Array(UInt8)) : Nil
      # Insert in place with one tail `memmove` rather than rebuilding the
      # buffer: nested objects/lists finish by splicing their header in front of
      # the data they appended, so rebuilding made deep/large values quadratic
      # in both copying and allocation.
      size = bytes.size
      return nil if size == 0

      old_size = @buffer.size
      @buffer.concat(bytes)
      return nil if index >= old_size

      storage = Slice.new(@buffer.to_unsafe, @buffer.size)
      tail_size = old_size - index
      (storage.to_unsafe + index + size).move_from(storage.to_unsafe + index, tail_size)
      Slice.new(bytes.to_unsafe, size).copy_to(storage[index, size])
      nil
    end

    def append_slice(slice : Bytes) : Nil
      @buffer.concat(slice)
      nil
    end

    def append_byte(byte : UInt8) : Nil
      @buffer << byte
      nil
    end

    private def append_le(value : Int, nbytes : Int32) : Nil
      nbytes.times { |i| @buffer << ((value >> (8 * i)) & 0xFF).to_u8 }
      nil
    end

    private def append_float(value : Float32) : Nil
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      @buffer.concat(io.to_slice)
      nil
    end

    private def append_double(value : Float64) : Nil
      io = IO::Memory.new
      io.write_bytes(value, IO::ByteFormat::LittleEndian)
      @buffer.concat(io.to_slice)
      nil
    end

    private def append_primitive_header(primitive_type : VariantPrimitiveType) : Nil
      @buffer << (((primitive_type.value << 2) | VariantBasicType::Primitive.value)).to_u8
      nil
    end

    def append_null : Nil
      append_primitive_header(VariantPrimitiveType::Null)
    end

    def append_bool(value : Bool) : Nil
      append_primitive_header(
        value ? VariantPrimitiveType::BooleanTrue : VariantPrimitiveType::BooleanFalse
      )
    end

    def append_int8(value : Int8) : Nil
      append_primitive_header(VariantPrimitiveType::Int8)
      append_byte(value.to_u8!)
    end

    def append_int16(value : Int16) : Nil
      append_primitive_header(VariantPrimitiveType::Int16)
      append_le(value, 2)
    end

    def append_int32(value : Int32) : Nil
      append_primitive_header(VariantPrimitiveType::Int32)
      append_le(value, 4)
    end

    def append_int64(value : Int64) : Nil
      append_primitive_header(VariantPrimitiveType::Int64)
      append_le(value, 8)
    end

    def append_float32(value : Float32) : Nil
      append_primitive_header(VariantPrimitiveType::Float)
      append_float(value)
    end

    def append_float64(value : Float64) : Nil
      append_primitive_header(VariantPrimitiveType::Double)
      append_double(value)
    end

    def append_date(value : Time) : Nil
      append_primitive_header(VariantPrimitiveType::Date)
      append_le(value.to_unix // 86_400, 4)
    end

    def append_timestamp_micros(value : Time) : Nil
      append_primitive_header(VariantPrimitiveType::TimestampMicros)
      append_le(value.to_unix_ns // 1_000, 8)
    end

    def append_timestamp_ntz_micros(value : Time) : Nil
      append_primitive_header(VariantPrimitiveType::TimestampNtzMicros)
      append_le(value.to_unix_ns // 1_000, 8)
    end

    def append_timestamp_nanos(value : Time) : Nil
      append_primitive_header(VariantPrimitiveType::TimestampNanos)
      append_le(value.to_unix_ns, 8)
    end

    def append_timestamp_ntz_nanos(value : Time) : Nil
      append_primitive_header(VariantPrimitiveType::TimestampNtzNanos)
      append_le(value.to_unix_ns, 8)
    end

    def append_time_micros(value : Time::Span) : Nil
      append_primitive_header(VariantPrimitiveType::Time)
      append_le(value.total_microseconds.to_i64, 8)
    end

    def append_uuid(value : UUID) : Nil
      append_primitive_header(VariantPrimitiveType::Uuid)
      append_slice(value.bytes.to_slice)
    end

    def append_decimal4(value : VariantDecimal4) : Nil
      append_primitive_header(VariantPrimitiveType::Decimal4)
      append_byte(value.scale)
      append_le(value.integer, 4)
    end

    def append_decimal8(value : VariantDecimal8) : Nil
      append_primitive_header(VariantPrimitiveType::Decimal8)
      append_byte(value.scale)
      append_le(value.integer, 8)
    end

    def append_decimal16(value : VariantDecimal16) : Nil
      append_primitive_header(VariantPrimitiveType::Decimal16)
      append_byte(value.scale)
      append_le(value.integer, 16)
    end

    def append_binary(value : Bytes) : Nil
      append_primitive_header(VariantPrimitiveType::Binary)
      append_le(value.size, 4)
      append_slice(value)
    end

    def append_short_string(value : ShortString) : Nil
      inner = value.value
      @buffer << (((inner.bytesize << 2) | VariantBasicType::ShortString.value)).to_u8
      append_slice(inner.to_slice)
    end

    def append_string(value : String) : Nil
      append_primitive_header(VariantPrimitiveType::String)
      append_le(value.bytesize, 4)
      append_slice(value.to_slice)
    end

    # Appends `variant` to `state`, then commits the state (unless the variant is
    # an object/list, which is re-encoded via a nested builder that commits).
    def self.append_variant(state : ParentState, variant : Variant) : Nil
      v = variant
      vb = state.value_builder
      case v
      when Variant::Null               then vb.append_null
      when Variant::BooleanTrue        then vb.append_bool(true)
      when Variant::BooleanFalse       then vb.append_bool(false)
      when Variant::Int8               then vb.append_int8(v.value)
      when Variant::Int16              then vb.append_int16(v.value)
      when Variant::Int32              then vb.append_int32(v.value)
      when Variant::Int64              then vb.append_int64(v.value)
      when Variant::Date               then vb.append_date(v.value)
      when Variant::Time               then vb.append_time_micros(v.value)
      when Variant::TimestampMicros    then vb.append_timestamp_micros(v.value)
      when Variant::TimestampNtzMicros then vb.append_timestamp_ntz_micros(v.value)
      when Variant::TimestampNanos     then vb.append_timestamp_nanos(v.value)
      when Variant::TimestampNtzNanos  then vb.append_timestamp_ntz_nanos(v.value)
      when Variant::Decimal4           then vb.append_decimal4(v.value)
      when Variant::Decimal8           then vb.append_decimal8(v.value)
      when Variant::Decimal16          then vb.append_decimal16(v.value)
      when Variant::Float              then vb.append_float32(v.value)
      when Variant::Double             then vb.append_float64(v.value)
      when Variant::Binary             then vb.append_binary(v.value)
      when Variant::String             then vb.append_string(v.value)
      when Variant::ShortString        then vb.append_short_string(v.value)
      when Variant::Uuid               then vb.append_uuid(v.value)
      when Variant::Object
        self.append_object(state, v.value)
        return
      when Variant::List
        self.append_list(state, v.value)
        return
      end
      state.finish
    end

    # Same as `append_variant`, but re-encodes nested objects/lists with the
    # fallible nested builders.
    def self.try_append_variant(state : ParentState, variant : Variant) : Nil
      v = variant
      case v
      when Variant::Object
        self.try_append_object(state, v.value)
        return
      when Variant::List
        self.try_append_list(state, v.value)
        return
      end
      append_variant(state, variant)
    end

    # Appends `variant` by copying raw object/list bytes when possible.
    def self.append_variant_bytes(state : ParentState, variant : Variant) : Nil
      v = variant
      case v
      when Variant::Object
        state.value_builder.append_slice(v.value.value)
      when Variant::List
        state.value_builder.append_slice(v.value.value)
      else
        append_variant(state, variant)
        return
      end
      state.finish
    end

    private def self.append_object(state : ParentState, obj : VariantObject) : Nil
      object_builder = ObjectBuilder.new(state, false)
      obj.iter.each { |(name, value)| object_builder.insert(name, value) }
      object_builder.finish
    end

    private def self.try_append_object(state : ParentState, obj : VariantObject) : Nil
      object_builder = ObjectBuilder.new(state, false)
      obj.iter_try.each { |(name, value)| object_builder.try_insert(name, value) }
      object_builder.finish
    end

    private def self.append_list(state : ParentState, list : VariantList) : Nil
      list_builder = ListBuilder.new(state, false)
      list.iter.each { |value| list_builder.append_value(value) }
      list_builder.finish
    end

    private def self.try_append_list(state : ParentState, list : VariantList) : Nil
      list_builder = ListBuilder.new(state, false)
      list.iter_try.each { |value| list_builder.try_append_value(value) }
      list_builder.finish
    end
  end

  # The union of metadata builders usable by the value builders.
  alias VariantMetadataBuilder = WritableMetadataBuilder | ReadOnlyMetadataBuilder

  # Builder-specific state that participates in `ParentState` rollback.
  abstract class BuilderSpecificState
    def rollback : Nil
    end

    def finish(metadata_builder : VariantMetadataBuilder, value_builder : ValueBuilder) : Nil
    end
  end

  # No-op state used by top-level value construction.
  class UnitBuilderState < BuilderSpecificState
  end

  # Tracks information needed to finalize a nested builder and to roll it back
  # if it is abandoned.
  class ParentState
    getter value_builder : ValueBuilder
    getter metadata_builder : VariantMetadataBuilder
    getter saved_value_builder_offset : Int32
    getter saved_metadata_builder_dict_size : Int32
    getter builder_state : BuilderSpecificState
    getter? finished : Bool

    def initialize(
      @value_builder : ValueBuilder,
      @metadata_builder : VariantMetadataBuilder,
      @builder_state : BuilderSpecificState,
      @saved_value_builder_offset : Int32,
      @saved_metadata_builder_dict_size : Int32,
    )
      @finished = false
    end

    # Creates a top-level parent state.
    def self.variant(
      value_builder : ValueBuilder,
      metadata_builder : VariantMetadataBuilder,
    ) : ParentState
      new(
        value_builder, metadata_builder, UnitBuilderState.new,
        value_builder.offset, metadata_builder.num_field_names
      )
    end

    # Marks the insertion as having succeeded (rollback is disabled).
    def finish : Nil
      @builder_state.finish(@metadata_builder, @value_builder)
      @finished = true
      nil
    end

    # Reverts value/metadata/builder-specific state if not finished.
    def rollback : Nil
      return if @finished

      @value_builder.truncate(@saved_value_builder_offset)
      @metadata_builder.truncate_field_names(@saved_metadata_builder_dict_size)
      @builder_state.rollback
      nil
    end
  end
end
