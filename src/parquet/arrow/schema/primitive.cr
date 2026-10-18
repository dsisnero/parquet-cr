require "arrow-cr"
require "../../basic"
require "../../errors"
require "../../schema/types"

# Ported from `parquet/src/arrow/schema/primitive.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `convert_primitive` (parquet primitive type -> arrow data type, with an
# optional hint from the embedded arrow schema), `apply_hint`, and
# `from_parquet`.
#
# Divergences: `apply_hint`/`from_parquet` are private module methods; the Rust
# pattern matches on `(Option<LogicalType>, ConvertedType)` + physical type,
# which becomes `case logical_type.kind`-style dispatch on the sealed
# `LogicalType` hierarchy plus the `ConvertedType` fallback; `unreachable!()`
# for group types raises `GeneralError`.
module Parquet
  module Arrow
    # Converts a parquet primitive `Type` to an arrow `DataType`, using
    # `arrow_type_hint` (from the embedded arrow schema) where parquet cannot
    # represent the type losslessly.
    def self.convert_primitive(
      parquet_type : Schema::Type,
      arrow_type_hint : ::Arrow::DataType?,
    ) : ::Arrow::DataType
      physical_type = from_parquet(parquet_type)
      return physical_type unless hint = arrow_type_hint
      apply_hint(physical_type, hint)
    end

    # Uses the arrow schema hint to faithfully reproduce the data as it was
    # written into parquet.
    private def self.apply_hint(parquet : ::Arrow::DataType, hint : ::Arrow::DataType) : ::Arrow::DataType
      parquet_kind = parquet.kind
      hint_kind = hint.kind

      case
      when (parquet_kind.int32? || parquet_kind.int64?) && hint_kind.timestamp?
        # Not all time units can be represented as LogicalType/ConvertedType
        hint
      when parquet_kind.int32? && hint_kind.time32?
        hint
      when parquet_kind.int64? && hint_kind.time64?
        hint
      when parquet_kind.int64? && hint_kind.duration?
        hint
      when parquet_kind.int64? && hint_kind.date64?
        # Date64 doesn't have a corresponding LogicalType/ConvertedType
        hint
      when parquet_kind.date32? && hint_kind.date64?
        # Coerce Date32 back to Date64 (#1666)
        hint
      when parquet_kind.timestamp? && hint_kind.timestamp? && parquet.time_unit == hint.time_unit && hint.timezone
        # Timestamps of the same resolution can use a different timezone
        hint
      when parquet_kind.timestamp? && parquet.time_unit.try(&.nanosecond?) && parquet.timezone.nil? &&
        hint_kind.timestamp? && hint.time_unit.try(&.second?)
        # INT96 defaults to Timestamp(ns); allow different resolutions
        hint
      when parquet_kind.timestamp? && parquet.time_unit.try(&.nanosecond?) && parquet.timezone.nil? &&
        hint_kind.timestamp? && hint.time_unit.try(&.millisecond?)
        hint
      when parquet_kind.timestamp? && parquet.time_unit.try(&.nanosecond?) && parquet.timezone.nil? &&
        hint_kind.timestamp? && hint.time_unit.try(&.microsecond?)
        hint
      when parquet_kind.utf8? && hint_kind.large_utf8?
        # Determine offset size
        hint
      when parquet_kind.binary? && hint_kind.large_binary?
        hint
      when parquet_kind.binary? && hint_kind.utf8?
        # Read as Utf8
        hint
      when parquet_kind.binary? && hint_kind.large_utf8?
        hint
      when parquet_kind.binary? && hint_kind.utf8_view?
        hint
      when parquet_kind.utf8? && hint_kind.utf8_view?
        # Determine view type
        hint
      when parquet_kind.binary? && hint_kind.binary_view?
        hint
      when parquet_kind.interval? && hint_kind.interval?
        # Determine interval time unit (#1666)
        hint
      when parquet_kind.decimal128? && (hint_kind.decimal32? || hint_kind.decimal64? || hint_kind.decimal256?)
        # Promote to Decimal256 or narrow to Decimal32/Decimal64
        hint
      when hint_kind.dictionary?
        # Potentially preserve dictionary encoding
        value = hint.value || return parquet
        hinted = apply_hint(parquet, value)
        hinted == value ? hint : hinted
      else
        parquet
      end
    end

    # Maps the parquet physical type (plus logical/converted annotations) to an
    # arrow data type.
    private def self.from_parquet(parquet_type : Schema::Type) : ::Arrow::DataType
      primitive = parquet_type.as?(Schema::PrimitiveType)
      raise general_err!("Expected a primitive parquet type, got a group") unless primitive

      basic_info = primitive.basic_info
      logical = basic_info.logical_type_ref

      # Any physical type can have the UNKNOWN logical type annotation.
      return ::Arrow::DataType.null if logical.is_a?(LogicalType::Unknown)

      case primitive.get_physical_type
      when Parquet::Type::BOOLEAN
        ::Arrow::DataType.boolean
      when Parquet::Type::INT32
        from_int32(basic_info, primitive.get_scale, primitive.get_precision)
      when Parquet::Type::INT64
        from_int64(basic_info, primitive.get_scale, primitive.get_precision)
      when Parquet::Type::INT96
        ::Arrow::DataType.timestamp(::Arrow::TimeUnit::Nanosecond, nil)
      when Parquet::Type::FLOAT
        ::Arrow::DataType.float32
      when Parquet::Type::DOUBLE
        ::Arrow::DataType.float64
      when Parquet::Type::BYTE_ARRAY
        from_byte_array(basic_info, primitive.get_precision, primitive.get_scale)
      else
        from_fixed_len_byte_array(
          basic_info, primitive.get_scale, primitive.get_precision, primitive.type_length
        )
      end
    end

    private def self.decimal_type(scale : Int32, precision : Int32) : ::Arrow::DataType
      if precision <= DECIMAL128_MAX_PRECISION
        decimal_128_type(scale, precision)
      else
        decimal_256_type(scale, precision)
      end
    end

    private def self.decimal_128_type(scale : Int32, precision : Int32) : ::Arrow::DataType
      raise arrow_err!("scale cannot be negative: #{scale}") if scale < 0
      raise arrow_err!("precision cannot be negative: #{precision}") if precision < 0
      ::Arrow::DataType.decimal128(precision, scale)
    end

    private def self.decimal_256_type(scale : Int32, precision : Int32) : ::Arrow::DataType
      raise arrow_err!("scale cannot be negative: #{scale}") if scale < 0
      raise arrow_err!("precision cannot be negative: #{precision}") if precision < 0
      ::Arrow::DataType.decimal256(precision, scale)
    end

    # Arrow's `DECIMAL128_MAX_PRECISION`.
    DECIMAL128_MAX_PRECISION = 38

    private def self.check_decimal_length(type_length : Int32) : Nil
      if type_length < 1 || type_length > 32
        raise general_err!(
          "DECIMAL must be a Fixed Length Byte Array with length 1 to 32, got #{type_length}"
        )
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def self.from_int32(
      info : Schema::BasicTypeInfo,
      scale : Int32,
      precision : Int32,
    ) : ::Arrow::DataType
      logical = info.logical_type_ref
      converted = info.converted_type

      case logical
      when LogicalType::Integer
        int_type = logical.value
        case {int_type.bit_width, int_type.is_signed}
        when {8, true}   then ::Arrow::DataType.int8
        when {16, true}  then ::Arrow::DataType.int16
        when {32, true}  then ::Arrow::DataType.int32
        when {8, false}  then ::Arrow::DataType.uint8
        when {16, false} then ::Arrow::DataType.uint16
        when {32, false} then ::Arrow::DataType.uint32
        else
          raise arrow_err!("Cannot create INT32 physical type from #{logical.rust_debug}")
        end
      when LogicalType::Decimal
        decimal = logical.value
        decimal_128_type(decimal.scale, decimal.precision)
      when LogicalType::Date
        ::Arrow::DataType.date32
      when LogicalType::Time
        time = logical.value
        unless time.unit.millis?
          raise arrow_err!("Cannot create INT32 physical type from #{time.unit}")
        end
        ::Arrow::DataType.time32(::Arrow::TimeUnit::Millisecond)
      when Nil
        case converted
        when ConvertedType::UINT_8      then ::Arrow::DataType.uint8
        when ConvertedType::UINT_16     then ::Arrow::DataType.uint16
        when ConvertedType::UINT_32     then ::Arrow::DataType.uint32
        when ConvertedType::INT_8       then ::Arrow::DataType.int8
        when ConvertedType::INT_16      then ::Arrow::DataType.int16
        when ConvertedType::INT_32      then ::Arrow::DataType.int32
        when ConvertedType::DATE        then ::Arrow::DataType.date32
        when ConvertedType::TIME_MILLIS then ::Arrow::DataType.time32(::Arrow::TimeUnit::Millisecond)
        when ConvertedType::DECIMAL     then decimal_128_type(scale, precision)
        when ConvertedType::NONE        then ::Arrow::DataType.int32
        else
          raise arrow_err!("Unable to convert parquet INT32 logical type #{logical_rust_debug(nil)} or converted type #{converted}")
        end
      else
        raise arrow_err!("Unable to convert parquet INT32 logical type #{logical_rust_debug(logical)} or converted type #{converted}")
      end
    end

    private def self.from_int64(
      info : Schema::BasicTypeInfo,
      scale : Int32,
      precision : Int32,
    ) : ::Arrow::DataType
      logical = info.logical_type_ref
      converted = info.converted_type

      case logical
      when LogicalType::Integer
        int_type = logical.value
        if int_type.bit_width != 64
          raise arrow_err!("Unable to convert parquet INT64 logical type #{logical_rust_debug(logical)} or converted type #{converted}")
        end
        int_type.is_signed ? ::Arrow::DataType.int64 : ::Arrow::DataType.uint64
      when LogicalType::Time
        time = logical.value
        case time.unit
        when Parquet::TimeUnit::MICROS then ::Arrow::DataType.time64(::Arrow::TimeUnit::Microsecond)
        when Parquet::TimeUnit::NANOS  then ::Arrow::DataType.time64(::Arrow::TimeUnit::Nanosecond)
        else
          raise arrow_err!("Cannot create INT64 from MILLIS time unit")
        end
      when LogicalType::Timestamp
        timestamp = logical.value
        unit = case timestamp.unit
               when Parquet::TimeUnit::MILLIS then ::Arrow::TimeUnit::Millisecond
               when Parquet::TimeUnit::MICROS then ::Arrow::TimeUnit::Microsecond
               else                                ::Arrow::TimeUnit::Nanosecond
               end
        ::Arrow::DataType.timestamp(unit, timestamp.is_adjusted_to_u_t_c ? "UTC" : nil)
      when LogicalType::Decimal
        decimal = logical.value
        decimal_128_type(decimal.scale, decimal.precision)
      when Nil
        case converted
        when ConvertedType::INT_64           then ::Arrow::DataType.int64
        when ConvertedType::UINT_64          then ::Arrow::DataType.uint64
        when ConvertedType::TIME_MICROS      then ::Arrow::DataType.time64(::Arrow::TimeUnit::Microsecond)
        when ConvertedType::TIMESTAMP_MILLIS then ::Arrow::DataType.timestamp(::Arrow::TimeUnit::Millisecond, "UTC")
        when ConvertedType::TIMESTAMP_MICROS then ::Arrow::DataType.timestamp(::Arrow::TimeUnit::Microsecond, "UTC")
        when ConvertedType::DECIMAL          then decimal_128_type(scale, precision)
        when ConvertedType::NONE             then ::Arrow::DataType.int64
        else
          raise arrow_err!("Unable to convert parquet INT64 logical type #{logical_rust_debug(nil)} or converted type #{converted}")
        end
      else
        raise arrow_err!("Unable to convert parquet INT64 logical type #{logical_rust_debug(logical)} or converted type #{converted}")
      end
    end

    private def self.from_byte_array(
      info : Schema::BasicTypeInfo,
      precision : Int32,
      scale : Int32,
    ) : ::Arrow::DataType
      logical = info.logical_type_ref
      converted = info.converted_type

      case logical
      when LogicalType::String, LogicalType::Json
        ::Arrow::DataType.utf8
      when LogicalType::Bson, LogicalType::Enum, LogicalType::Geometry, LogicalType::Geography,
           LogicalType::UnknownField
        ::Arrow::DataType.binary
      when LogicalType::Decimal
        decimal = logical.value
        decimal_type(decimal.scale, decimal.precision)
      when Nil
        case converted
        when ConvertedType::NONE    then ::Arrow::DataType.binary
        when ConvertedType::JSON    then ::Arrow::DataType.utf8
        when ConvertedType::BSON    then ::Arrow::DataType.binary
        when ConvertedType::ENUM    then ::Arrow::DataType.binary
        when ConvertedType::UTF8    then ::Arrow::DataType.utf8
        when ConvertedType::DECIMAL then decimal_type(scale, precision)
        else
          raise arrow_err!("Unable to convert parquet BYTE_ARRAY logical type #{logical_rust_debug(nil)} or converted type #{converted}")
        end
      else
        raise arrow_err!("Unable to convert parquet BYTE_ARRAY logical type #{logical_rust_debug(logical)} or converted type #{converted}")
      end
    end

    private def self.from_fixed_len_byte_array(
      info : Schema::BasicTypeInfo,
      scale : Int32,
      precision : Int32,
      type_length : Int32,
    ) : ::Arrow::DataType
      logical = info.logical_type_ref
      converted = info.converted_type

      case logical
      when LogicalType::Decimal
        decimal = logical.value
        check_decimal_length(type_length)
        # lengths 1..=16 map to Decimal128, 17..=32 to Decimal256
        if type_length <= 16
          decimal_128_type(decimal.scale, decimal.precision)
        else
          decimal_256_type(decimal.scale, decimal.precision)
        end
      when LogicalType::Float16
        if type_length == 2
          ::Arrow::DataType.float16
        else
          raise general_err!("FLOAT16 logical type must be Fixed Length Byte Array with length 2")
        end
      when Nil
        case converted
        when ConvertedType::DECIMAL
          check_decimal_length(type_length)
          if type_length <= 16
            decimal_128_type(scale, precision)
          else
            decimal_256_type(scale, precision)
          end
        when ConvertedType::INTERVAL
          if type_length != 12
            raise general_err!(
              "INTERVAL must be a Fixed Length Byte Array with length 12, got #{type_length}"
            )
          end
          # There is currently no reliable way of determining which IntervalUnit
          # to return without the original Arrow schema.
          ::Arrow::DataType.interval(::Arrow::IntervalUnit::DayTime)
        else
          ::Arrow::DataType.fixed_size_binary(type_length)
        end
      else
        ::Arrow::DataType.fixed_size_binary(type_length)
      end
    end

    # Rust-`Debug`-compatible rendering for the error messages.
    private def self.logical_rust_debug(logical : LogicalType?) : String
      logical ? logical.rust_debug : "None"
    end
  end
end
