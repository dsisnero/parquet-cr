require "arrow-cr"
require "../parquet_variant"
require "./type_conversion"

# Ported from `parquet-variant-compute/src/variant_to_arrow.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Row builders that convert variant values into strongly typed Arrow arrays.
# `variant_get` uses them to extract a (possibly path-selected) value with
# casting, and `shred_variant` uses the primitive builders for its
# `typed_value` columns.
#
# Ported so far: the primitive builders (`PrimitiveVariantToArrowRowBuilder`),
# i.e. every scalar Arrow type plus decimals and UUIDs.
#
# Deferred (see `plans/parity.md`): the array/list, struct, map, union,
# dictionary/run-end-encoded, variant-path and binary-variant row builders
# (`ArrayVariantToArrowRowBuilder`, `StructVariantToArrowRowBuilder`,
# `MapVariantToArrowRowBuilder`, `UnionVariantToArrowRowBuilder`,
# `EncodedVariantToArrowRowBuilder`, `VariantPathRowBuilder`,
# `VariantToBinaryVariantArrowRowBuilder`), and `Decimal256`.
#
# Divergences: upstream's builder enums are a `RowBuilder` class hierarchy with
# one generic `ValueRowBuilder` for the macro-generated builders; `usize`
# capacity is `Int32`; errors raise (`ParquetVariant::CastError` for failed
# casts in strict mode, `InvalidArgumentError` for unsupported target types) and
# the "path VariantPath([])" cast message is kept verbatim.
module ParquetVariantCompute
  # Converts variant values to a typed Arrow array (upstream
  # `variant_to_arrow.rs`).
  module VariantToArrow
    # Appends variant values and produces the target array (upstream
    # `VariantToArrowRowBuilder`).
    abstract class RowBuilder
      # Appends a null row.
      abstract def append_null : Nil

      # Appends `value`, returning whether the target array holds a value (as
      # opposed to a null).
      abstract def append_value(value : ParquetVariant::Variant) : Bool

      # Builds the array.
      abstract def finish : ::Arrow::ArrayBase
    end

    # Builds one typed array through a per-type extraction (upstream's
    # `define_variant_to_primitive_builder!` instantiations).
    class ValueRowBuilder(B, T) < RowBuilder
      @builder : B
      @cast_options : ::Arrow::CastOptions
      @shred : Bool
      @extract : Proc(ParquetVariant::Variant, Bool, T?)
      @cast_error : Proc(ParquetVariant::Variant, String)

      def initialize(
        @builder : B,
        @cast_options : ::Arrow::CastOptions,
        @shred : Bool,
        type_name : String,
        &extract : ParquetVariant::Variant, Bool -> T?
      )
        @extract = extract
        @cast_error = ->(variant : ParquetVariant::Variant) do
          "Failed to extract primitive of type #{type_name} " \
          "from variant #{variant.debug_string} at path VariantPath([])"
        end
      end

      # Overrides the message used for a failed strict cast.
      def with_cast_error(&block : ParquetVariant::Variant -> String) : self
        @cast_error = block
        self
      end

      def append_null : Nil
        @builder.append_null
      end

      def append_value(value : ParquetVariant::Variant) : Bool
        result = TypeConversion.cast_with_options(value, @cast_options) do |variant|
          @extract.call(variant, @shred)
        end
        if result.nil?
          @builder.append_null
          false
        else
          @builder.append_value(result)
          true
        end
      rescue ParquetVariant::CastError
        raise ParquetVariant::CastError.new(@cast_error.call(value))
      end

      def finish : ::Arrow::ArrayBase
        @builder.finish
      end
    end

    # Builds a `NullArray` (upstream `VariantToNullArrowRowBuilder` with its
    # `FakeNullBuilder`).
    class NullRowBuilder < RowBuilder
      def initialize
        @builder = ::Arrow::NullBuilder.new
      end

      def append_null : Nil
        @builder.append_null
      end

      def append_value(value : ParquetVariant::Variant) : Bool
        # Only `Variant::Null` maps to the Null storage type; the `true` result
        # marks that the row carries a value.
        @builder.append_null
        value.as_null
      end

      def finish : ::Arrow::ArrayBase
        @builder.finish
      end
    end

    # Creates the builder for a primitive target type (upstream
    # `make_primitive_variant_to_arrow_row_builder`).
    #
    # ameba:disable Metrics/CyclomaticComplexity
    def self.make_primitive_row_builder(
      data_type : ::Arrow::DataType,
      cast_options : ::Arrow::CastOptions,
      capacity : Int32,
      shred : Bool,
    ) : RowBuilder
      kind = data_type.kind

      # Unsigned integers have no `?` predicate on the port's `DataType::Kind`.
      if kind == ::Arrow::DataType::Kind::UInt8
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(UInt8).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.uint8(variant, shredded)
        end
      elsif kind == ::Arrow::DataType::Kind::UInt16
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(UInt16).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.uint16(variant, shredded)
        end
      elsif kind == ::Arrow::DataType::Kind::UInt32
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(UInt32).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.uint32(variant, shredded)
        end
      elsif kind == ::Arrow::DataType::Kind::UInt64
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(UInt64).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.uint64(variant, shredded)
        end
      end

      case kind
      when .null?
        return NullRowBuilder.new
      when .boolean?
        return numeric(cast_options, shred,
          ::Arrow::BooleanBuilder.with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.variant_to_boolean(variant, shredded)
        end
      when .int8?
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(Int8).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.int8(variant, shredded)
        end
      when .int16?
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(Int16).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.int16(variant, shredded)
        end
      when .int32?
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(Int32).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.int32(variant, shredded)
        end
      when .int64?
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(Int64).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.int64(variant, shredded)
        end
      when .float16?
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(::Arrow::Float16).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.float16(variant, shredded)
        end
      when .float32?
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(Float32).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.float32(variant, shredded)
        end
      when .float64?
        return numeric(cast_options, shred,
          ::Arrow::PrimitiveBuilder(Float64).with_capacity(capacity), data_type.to_s) do |variant, shredded|
          TypeConversion.float64(variant, shredded)
        end
      when .decimal32?
        return decimal(data_type, cast_options, capacity, shred,
          ::Arrow::PrimitiveBuilder(Int32), "Decimal32") do |variant, precision, scale, shredded|
          if shredded
            TypeConversion.shred_unscaled_decimal32(variant, precision, scale)
          else
            TypeConversion.unscaled_decimal32(variant, precision, scale)
          end
        end
      when .decimal64?
        return decimal(data_type, cast_options, capacity, shred,
          ::Arrow::PrimitiveBuilder(Int64), "Decimal64") do |variant, precision, scale, shredded|
          if shredded
            TypeConversion.shred_unscaled_decimal64(variant, precision, scale)
          else
            TypeConversion.unscaled_decimal64(variant, precision, scale)
          end
        end
      when .decimal128?
        return decimal(data_type, cast_options, capacity, shred,
          ::Arrow::PrimitiveBuilder(Int128), "Decimal128") do |variant, precision, scale, shredded|
          if shredded
            TypeConversion.shred_unscaled_decimal128(variant, precision, scale)
          else
            TypeConversion.unscaled_decimal128(variant, precision, scale)
          end
        end
      when .date32?
        return primitive_with_type(data_type, cast_options, capacity, shred,
          ::Arrow::PrimitiveBuilder(Int32), Int32) do |variant, shredded|
          TypeConversion.date32(variant, shredded)
        end
      when .date64?
        return primitive_with_type(data_type, cast_options, capacity, shred,
          ::Arrow::PrimitiveBuilder(Int64), Int64) do |variant, shredded|
          TypeConversion.date64(variant, shredded)
        end
      when .time32?
        unit = data_type.time_unit
        unless unit == ::Arrow::TimeUnit::Second || unit == ::Arrow::TimeUnit::Millisecond
          raise ParquetVariant::InvalidArgumentError.new(
            "The unit for Time32 must be second/millisecond, received #{unit}"
          )
        end
        return primitive_with_type(data_type, cast_options, capacity, shred,
          ::Arrow::PrimitiveBuilder(Int32), Int32) do |variant, shredded|
          if unit == ::Arrow::TimeUnit::Second
            TypeConversion.time32_second(variant, shredded)
          else
            TypeConversion.time32_millisecond(variant, shredded)
          end
        end
      when .time64?
        unit = data_type.time_unit
        unless unit == ::Arrow::TimeUnit::Microsecond || unit == ::Arrow::TimeUnit::Nanosecond
          raise ParquetVariant::InvalidArgumentError.new(
            "The unit for Time64 must be micro/nano seconds, received #{unit}"
          )
        end
        return primitive_with_type(data_type, cast_options, capacity, shred,
          ::Arrow::PrimitiveBuilder(Int64), Int64) do |variant, shredded|
          if unit == ::Arrow::TimeUnit::Microsecond
            TypeConversion.time64_microsecond(variant, shredded)
          else
            TypeConversion.time64_nanosecond(variant, shredded)
          end
        end
      when .timestamp?
        unit = data_type.time_unit
        ntz = data_type.timezone.nil?
        return timestamp(data_type, cast_options, capacity, shred, unit, ntz)
      when .duration?
        raise ParquetVariant::InvalidArgumentError.new(
          "Casting Variant to duration/interval types is not supported. " \
          "The Variant format does not define duration/interval types."
        )
      when .interval?
        raise ParquetVariant::InvalidArgumentError.new(
          "Casting Variant to duration/interval types is not supported. " \
          "The Variant format does not define duration/interval types."
        )
      when .binary?
        return numeric(cast_options, shred,
          ::Arrow::BinaryBuilder.with_capacity(capacity, capacity * 2), data_type.to_s) do |variant, _shredded|
          variant.as_u8_slice
        end
      when .large_binary?
        return numeric(cast_options, shred,
          ::Arrow::GenericBinaryBuilder(Int64).with_capacity(capacity, capacity * 2), data_type.to_s) do |variant, _shredded|
          variant.as_u8_slice
        end
      when .binary_view?
        return numeric(cast_options, shred,
          ::Arrow::BinaryViewBuilder.with_capacity(capacity), data_type.to_s) do |variant, _shredded|
          variant.as_u8_slice
        end
      when .fixed_size_binary?
        if data_type.byte_width == 16
          builder = numeric(cast_options, shred,
            ::Arrow::FixedSizeBinaryBuilder.with_capacity(capacity, 16), data_type.to_s) do |variant, _shredded|
            # `UUID#bytes` returns a `StaticArray` by value, so materialize the
            # bytes instead of taking a slice of the temporary.
            variant.as_uuid.try do |uuid|
              bytes = uuid.bytes
              Bytes.new(16) { |i| bytes[i] }
            end
          end
          return builder.with_cast_error do |variant|
            "Failed to extract UUID from variant #{variant.debug_string}"
          end
        end
        raise nyi_err!("DataType #{data_type} not yet implemented")
      when .utf8?
        return numeric(cast_options, shred,
          ::Arrow::StringBuilder.with_capacity(capacity, capacity * 2), data_type.to_s) do |variant, _shredded|
          variant.as_string
        end
      when .large_utf8?
        return numeric(cast_options, shred,
          ::Arrow::GenericStringBuilder(Int64).with_capacity(capacity, capacity * 2), data_type.to_s) do |variant, _shredded|
          variant.as_string
        end
      when .utf8_view?
        return numeric(cast_options, shred,
          ::Arrow::StringViewBuilder.with_capacity(capacity), data_type.to_s) do |variant, _shredded|
          variant.as_string
        end
      end

      # List-like, struct, map, union and encoded targets are not primitive.
      raise ParquetVariant::InvalidArgumentError.new(
        "Casting to #{data_type} is not applicable for primitive Variant types"
      )
    end

    # Creates the builder for a typed target (upstream
    # `make_typed_variant_to_arrow_row_builder`).
    #
    # Only primitive targets are ported so far; the composite and encoded
    # builders are deferred (see the module comment).
    def self.make_typed_row_builder(
      data_type : ::Arrow::DataType,
      cast_options : ::Arrow::CastOptions,
      capacity : Int32,
      shred : Bool,
    ) : RowBuilder
      case data_type.kind
      when .struct?, .list?, .large_list?, .list_view?, .large_list_view?,
           .fixed_size_list?, .map?, .union?, .dictionary?, .run_end_encoded?
        raise nyi_err!(
          "The #{data_type} variant-to-arrow row builder is not ported yet"
        )
      end

      make_primitive_row_builder(data_type, cast_options, capacity, shred)
    end

    # Builds a `ValueRowBuilder` over an already-created builder.
    private def self.numeric(
      cast_options : ::Arrow::CastOptions,
      shred : Bool,
      builder : B,
      type_name : String,
      &extract : ParquetVariant::Variant, Bool -> T?
    ) : ValueRowBuilder(B, T) forall B, T
      ValueRowBuilder(B, T).new(builder, cast_options, shred, type_name, &extract)
    end

    # Builds a `ValueRowBuilder` whose array carries `data_type` (temporal
    # targets).
    private def self.primitive_with_type(
      data_type : ::Arrow::DataType,
      cast_options : ::Arrow::CastOptions,
      capacity : Int32,
      shred : Bool,
      builder_class : B.class,
      type : T.class,
      &extract : ParquetVariant::Variant, Bool -> T?
    ) : ValueRowBuilder(B, T) forall B, T
      builder = builder_class.with_capacity(capacity).with_data_type(data_type)
      ValueRowBuilder(B, T).new(builder, cast_options, shred, data_type.to_s, &extract)
    end

    # Builds a decimal `ValueRowBuilder` (precision/scale are part of the array's
    # data type).
    private def self.decimal(
      data_type : ::Arrow::DataType,
      cast_options : ::Arrow::CastOptions,
      capacity : Int32,
      shred : Bool,
      builder_class : B.class,
      prefix : String,
      &extract : ParquetVariant::Variant, Int32, Int32, Bool -> T?
    ) : ValueRowBuilder(B, T) forall B, T
      precision = (data_type.precision || 0_u8).to_i32
      scale = (data_type.scale || 0_i8).to_i32
      builder = builder_class.with_capacity(capacity).with_data_type(data_type)
      row_builder = ValueRowBuilder(B, T).new(
        builder, cast_options, shred, data_type.to_s
      ) do |variant, shredded|
        extract.call(variant, precision, scale, shredded)
      end
      row_builder.with_cast_error do |variant|
        "Failed to cast to #{prefix}(precision=#{precision}, scale=#{scale}) " \
        "from variant #{variant.debug_string}"
      end
    end

    # Builds a timestamp `ValueRowBuilder`.
    private def self.timestamp(
      data_type : ::Arrow::DataType,
      cast_options : ::Arrow::CastOptions,
      capacity : Int32,
      shred : Bool,
      unit : ::Arrow::TimeUnit?,
      ntz : Bool,
    ) : RowBuilder
      builder = ::Arrow::PrimitiveBuilder(Int64).with_capacity(capacity)
        .with_data_type(data_type)

      case unit
      when ::Arrow::TimeUnit::Second
        ValueRowBuilder(::Arrow::PrimitiveBuilder(Int64), Int64).new(
          builder, cast_options, shred, data_type.to_s
        ) { |variant, shredded| TypeConversion.timestamp_second(variant, ntz, shredded) }
      when ::Arrow::TimeUnit::Millisecond
        ValueRowBuilder(::Arrow::PrimitiveBuilder(Int64), Int64).new(
          builder, cast_options, shred, data_type.to_s
        ) { |variant, shredded| TypeConversion.timestamp_millisecond(variant, ntz, shredded) }
      when ::Arrow::TimeUnit::Microsecond
        ValueRowBuilder(::Arrow::PrimitiveBuilder(Int64), Int64).new(
          builder, cast_options, shred, data_type.to_s
        ) { |variant, shredded| TypeConversion.timestamp_microsecond(variant, ntz, shredded) }
      when ::Arrow::TimeUnit::Nanosecond
        ValueRowBuilder(::Arrow::PrimitiveBuilder(Int64), Int64).new(
          builder, cast_options, shred, data_type.to_s
        ) { |variant, shredded| TypeConversion.timestamp_nanosecond(variant, ntz, shredded) }
      else
        raise ParquetVariant::InvalidArgumentError.new("Invalid timestamp unit #{unit}")
      end
    end
  end
end
