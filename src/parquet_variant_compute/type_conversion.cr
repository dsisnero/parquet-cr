require "arrow-cr"
require "../parquet_variant"

# Ported from `parquet-variant-compute/src/type_conversion.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Casts a single `Variant` value to a strongly typed Arrow value. The Rust
# per-arrow-type traits (`PrimitiveFromVariant`, `TimestampFromVariant`,
# `ShredDecimalVariant`) become one function per target type, all built on the
# shared `as_num`/decimal helpers.
#
# Divergences: `usize`/`u8` parameters are `Int32`; Crystal raises instead of
# returning `Result` (`cast_with_options` raises `ParquetVariant::CastError` in
# strict mode); the cast error prints `Variant#debug_string`. Numeric conversion
# uses Arrow's native casts and rescaling, plus a decimal parser that preserves
# upstream exponent syntax and rounding without overflowing intermediate values.
module ParquetVariantCompute
  # Casting a single variant value to an Arrow value (upstream
  # `type_conversion.rs`).
  module TypeConversion
    # Precision of the variant decimal types (upstream `MAX_PRECISION`).
    DECIMAL4_MAX_PRECISION  =  9
    DECIMAL8_MAX_PRECISION  = 18
    DECIMAL16_MAX_PRECISION = 38

    # Casts one value with safe/strict semantics (upstream
    # `variant_cast_with_options`).
    #
    # Returns `nil` when the cast fails in safe mode or the source value is
    # `Variant::Null`; raises `ParquetVariant::CastError` when the cast fails in
    # strict mode.
    def self.cast_with_options(
      variant : ParquetVariant::Variant,
      cast_options : ::Arrow::CastOptions,
      & : ParquetVariant::Variant -> T?
    ) : T? forall T
      value = yield variant
      return value unless value.nil?
      return nil if variant.is_a?(ParquetVariant::Variant::Null) || cast_options.safe?

      raise ParquetVariant::CastError.new(
        "Failed to cast variant value #{variant.debug_string}"
      )
    end

    # --- Primitive conversions (upstream `PrimitiveFromVariant` impls) ---

    def self.int8(variant : ParquetVariant::Variant, shred : Bool = false) : Int8?
      shred ? variant.as_int8 : as_num(variant, Int8)
    end

    def self.int16(variant : ParquetVariant::Variant, shred : Bool = false) : Int16?
      shred ? variant.as_int16 : as_num(variant, Int16)
    end

    def self.int32(variant : ParquetVariant::Variant, shred : Bool = false) : Int32?
      shred ? variant.as_int32 : as_num(variant, Int32)
    end

    def self.int64(variant : ParquetVariant::Variant, shred : Bool = false) : Int64?
      shred ? variant.as_int64 : as_num(variant, Int64)
    end

    # Unsigned integers are never a shredding target.
    def self.uint8(variant : ParquetVariant::Variant, shred : Bool = false) : UInt8?
      shred ? nil : as_num(variant, UInt8)
    end

    def self.uint16(variant : ParquetVariant::Variant, shred : Bool = false) : UInt16?
      shred ? nil : as_num(variant, UInt16)
    end

    def self.uint32(variant : ParquetVariant::Variant, shred : Bool = false) : UInt32?
      shred ? nil : as_num(variant, UInt32)
    end

    def self.uint64(variant : ParquetVariant::Variant, shred : Bool = false) : UInt64?
      shred ? nil : as_num(variant, UInt64)
    end

    # Half floats are never a shredding target.
    def self.float16(
      variant : ParquetVariant::Variant,
      shred : Bool = false,
    ) : ::Arrow::Float16?
      shred ? nil : as_num(variant, ::Arrow::Float16)
    end

    def self.float32(variant : ParquetVariant::Variant, shred : Bool = false) : Float32?
      shred ? variant.as_f32 : as_num(variant, Float32)
    end

    def self.float64(variant : ParquetVariant::Variant, shred : Bool = false) : Float64?
      shred ? variant.as_f64 : as_num(variant, Float64)
    end

    # Days since the Unix epoch.
    def self.date32(variant : ParquetVariant::Variant, shred : Bool = false) : Int32?
      date = variant.as_naive_date
      return nil unless date

      (date.to_unix // 86_400).to_i32
    end

    # Milliseconds since the Unix epoch (midnight of the date).
    def self.date64(variant : ParquetVariant::Variant, shred : Bool = false) : Int64?
      date = variant.as_naive_date
      return nil unless date

      (date.to_unix // 86_400) * 86_400_000_i64
    end

    # Seconds since midnight; drops precision loss.
    def self.time32_second(variant : ParquetVariant::Variant, shred : Bool = false) : Int32?
      return nil if shred

      total = time_of_day_nanos(variant)
      return nil unless total
      return nil unless total % 1_000_000_000 == 0

      (total // 1_000_000_000).to_i32
    end

    # Milliseconds since midnight; drops precision loss.
    def self.time32_millisecond(variant : ParquetVariant::Variant, shred : Bool = false) : Int32?
      return nil if shred

      total = time_of_day_nanos(variant)
      return nil unless total
      return nil unless total % 1_000_000 == 0

      (total // 1_000_000).to_i32
    end

    # Microseconds since midnight.
    def self.time64_microsecond(variant : ParquetVariant::Variant, shred : Bool = false) : Int64?
      total = time_of_day_nanos(variant)
      return nil unless total

      total // 1_000
    end

    # Nanoseconds since midnight (never a shredding target).
    def self.time64_nanosecond(variant : ParquetVariant::Variant, shred : Bool = false) : Int64?
      return nil if shred

      time_of_day_nanos(variant)
    end

    # Seconds since the Unix epoch; drops precision loss (never a shredding
    # target).
    def self.timestamp_second(
      variant : ParquetVariant::Variant,
      ntz : Bool,
      shred : Bool = false,
    ) : Int64?
      return nil if shred

      time = timestamp_time(variant, ntz)
      return nil unless time
      return nil unless time.nanosecond == 0

      time.to_unix
    end

    # Milliseconds since the Unix epoch; drops precision loss (never a shredding
    # target).
    def self.timestamp_millisecond(
      variant : ParquetVariant::Variant,
      ntz : Bool,
      shred : Bool = false,
    ) : Int64?
      return nil if shred

      time = timestamp_time(variant, ntz)
      return nil unless time
      return nil unless time.nanosecond % 1_000_000 == 0

      time.to_unix * 1_000 + time.nanosecond // 1_000_000
    end

    # Microseconds since the Unix epoch.
    def self.timestamp_microsecond(
      variant : ParquetVariant::Variant,
      ntz : Bool,
      shred : Bool = false,
    ) : Int64?
      time = timestamp_micros_time(variant, ntz, shred)
      return nil unless time

      time.to_unix * 1_000_000 + time.nanosecond // 1_000
    end

    # Nanoseconds since the Unix epoch.
    def self.timestamp_nanosecond(
      variant : ParquetVariant::Variant,
      ntz : Bool,
      shred : Bool = false,
    ) : Int64?
      time =
        if shred
          ntz ? variant.as_timestamp_ntz_nanos : variant.as_timestamp_nanos
        else
          timestamp_time_any_unit(variant, ntz)
        end
      return nil unless time

      time.to_unix * 1_000_000_000 + time.nanosecond
    end

    # Casts a variant to a boolean (upstream `variant_to_boolean`).
    #
    # Shredded columns only accept boolean variants; unshredded ones also cast
    # numbers and boolean strings.
    def self.variant_to_boolean(variant : ParquetVariant::Variant, shred : Bool = false) : Bool?
      return variant.as_boolean if shred

      case variant
      when ParquetVariant::Variant::BooleanTrue  then true
      when ParquetVariant::Variant::BooleanFalse then false
      when ParquetVariant::Variant::Int8         then ::Arrow.cast_num_to_bool(variant.value)
      when ParquetVariant::Variant::Int16        then ::Arrow.cast_num_to_bool(variant.value)
      when ParquetVariant::Variant::Int32        then ::Arrow.cast_num_to_bool(variant.value)
      when ParquetVariant::Variant::Int64        then ::Arrow.cast_num_to_bool(variant.value)
      when ParquetVariant::Variant::Float        then ::Arrow.cast_num_to_bool(variant.value)
      when ParquetVariant::Variant::Double       then ::Arrow.cast_num_to_bool(variant.value)
      when ParquetVariant::Variant::ShortString
        ::Arrow.cast_single_string_to_boolean_default(variant.value.as_str)
      when ParquetVariant::Variant::String
        ::Arrow.cast_single_string_to_boolean_default(variant.value)
      else
        nil
      end
    end

    # --- Decimal unscaled conversions (upstream `variant_to_unscaled_decimal`) ---

    def self.unscaled_decimal32(
      variant : ParquetVariant::Variant,
      precision : Int32,
      scale : Int32,
    ) : Int32?
      unscaled_decimal(variant, precision, scale, Int32)
    end

    def self.unscaled_decimal64(
      variant : ParquetVariant::Variant,
      precision : Int32,
      scale : Int32,
    ) : Int64?
      unscaled_decimal(variant, precision, scale, Int64)
    end

    def self.unscaled_decimal128(
      variant : ParquetVariant::Variant,
      precision : Int32,
      scale : Int32,
    ) : Int128?
      unscaled_decimal(variant, precision, scale, Int128)
    end

    def self.unscaled_decimal256(variant : ParquetVariant::Variant, precision : Int32, scale : Int32) : ::Arrow::I256?
      unscaled_decimal(variant, precision, scale, ::Arrow::I256)
    end

    # The Variant encoding has no Decimal256 shredding representation.
    def self.shred_unscaled_decimal256(variant : ParquetVariant::Variant, precision : Int32, scale : Int32) : ::Arrow::I256?
      nil
    end

    # --- Decimal unscaled conversions used when shredding (upstream
    # `shred_variant_to_unscaled_decimal`) ---

    def self.shred_unscaled_decimal32(
      variant : ParquetVariant::Variant,
      precision : Int32,
      scale : Int32,
    ) : Int32?
      shred_unscaled_decimal(variant, precision, scale, Int32)
    end

    def self.shred_unscaled_decimal64(
      variant : ParquetVariant::Variant,
      precision : Int32,
      scale : Int32,
    ) : Int64?
      shred_unscaled_decimal(variant, precision, scale, Int64)
    end

    def self.shred_unscaled_decimal128(
      variant : ParquetVariant::Variant,
      precision : Int32,
      scale : Int32,
    ) : Int128?
      shred_unscaled_decimal(variant, precision, scale, Int128)
    end

    # --- helpers ---

    # Converts a boolean or numeric variant to `type` using Arrow's casting
    # rules (upstream `as_num`).
    private def self.as_num(variant : ParquetVariant::Variant, type : T.class) : T? forall T
      case variant
      when ParquetVariant::Variant::BooleanFalse then ::Arrow.single_bool_to_numeric(false, T)
      when ParquetVariant::Variant::BooleanTrue  then ::Arrow.single_bool_to_numeric(true, T)
      when ParquetVariant::Variant::Int8         then ::Arrow.num_cast(variant.value, T)
      when ParquetVariant::Variant::Int16        then ::Arrow.num_cast(variant.value, T)
      when ParquetVariant::Variant::Int32        then ::Arrow.num_cast(variant.value, T)
      when ParquetVariant::Variant::Int64        then ::Arrow.num_cast(variant.value, T)
      when ParquetVariant::Variant::Float        then ::Arrow.num_cast(variant.value, T)
      when ParquetVariant::Variant::Double       then ::Arrow.num_cast(variant.value, T)
      when ParquetVariant::Variant::Decimal4
        decimal_to_num(variant.value.integer, variant.value.scale, T)
      when ParquetVariant::Variant::Decimal8
        decimal_to_num(variant.value.integer, variant.value.scale, T)
      when ParquetVariant::Variant::Decimal16
        decimal_to_num(variant.value.integer, variant.value.scale, T)
      else
        nil
      end
    end

    # Converts an unscaled decimal to a number: integer targets truncate toward
    # zero, float targets divide lossily (upstream `cast_decimal_to_num`).
    private def self.decimal_to_num(
      raw : Int32 | Int64 | Int128,
      scale : UInt8,
      type : T.class,
    ) : T? forall T
      {% if T <= Float32 || T <= Float64 || T <= ::Arrow::Float16 %}
        ::Arrow.num_cast(raw.to_f64 / (10.0 ** scale.to_i32), T)
      {% else %}
        ::Arrow.num_cast(raw.to_i128.tdiv(10_i128 ** scale.to_i32), T)
      {% end %}
    end

    # The nanoseconds since midnight of a `Variant::Time` (or `nil`).
    private def self.time_of_day_nanos(variant : ParquetVariant::Variant) : Int64?
      span = variant.as_time_utc
      return nil unless span

      # `Time::Span#total_nanoseconds` is a `Float64`, but integer values below
      # 2^53 (~104 days) are exact, and a time of day is always below one day.
      span.total_nanoseconds.to_i64
    end

    # The UTC time of a `Variant::TimestampMicros`/`TimestampNanos` pair, ignoring
    # timezone awareness (upstream `convert_to_timestamp_nano`).
    private def self.timestamp_time_any_unit(
      variant : ParquetVariant::Variant,
      ntz : Bool,
    ) : ::Time?
      if ntz
        variant.as_timestamp_ntz_micros || variant.as_timestamp_ntz_nanos
      else
        variant.as_timestamp_micros || variant.as_timestamp_nanos
      end
    end

    # The UTC time of a micro/nanosecond timestamp for the second/millisecond
    # targets (upstream `convert_to_timestamp_nano`/`_ntz_nano`).
    private def self.timestamp_time(variant : ParquetVariant::Variant, ntz : Bool) : ::Time?
      timestamp_time_any_unit(variant, ntz)
    end

    # The UTC time of a microsecond timestamp.
    private def self.timestamp_micros_time(
      variant : ParquetVariant::Variant,
      ntz : Bool,
      shred : Bool,
    ) : ::Time?
      if shred
        ntz ? variant.as_timestamp_ntz_micros : variant.as_timestamp_micros
      elsif ntz
        variant.as_timestamp_ntz_micros
      else
        variant.as_timestamp_micros
      end
    end

    # Rescales a variant to the unscaled integer of `type` (upstream
    # `variant_to_unscaled_decimal`).
    private def self.unscaled_decimal(
      variant : ParquetVariant::Variant,
      precision : Int32,
      scale : Int32,
      type : T.class,
    ) : T? forall T
      case variant
      when ParquetVariant::Variant::Int8
        rescale_decimal(variant.value.to_i128, DECIMAL4_MAX_PRECISION, 0, precision, scale, T)
      when ParquetVariant::Variant::Int16
        rescale_decimal(variant.value.to_i128, DECIMAL4_MAX_PRECISION, 0, precision, scale, T)
      when ParquetVariant::Variant::Int32
        rescale_decimal(variant.value.to_i128, DECIMAL4_MAX_PRECISION, 0, precision, scale, T)
      when ParquetVariant::Variant::Int64
        rescale_decimal(variant.value.to_i128, DECIMAL8_MAX_PRECISION, 0, precision, scale, T)
      when ParquetVariant::Variant::Float
        float_to_decimal(variant.value.to_f64, precision, scale, T)
      when ParquetVariant::Variant::Double
        float_to_decimal(variant.value, precision, scale, T)
      when ParquetVariant::Variant::String
        parse_decimal(variant.value, precision, scale, T)
      when ParquetVariant::Variant::ShortString
        parse_decimal(variant.value.as_str, precision, scale, T)
      when ParquetVariant::Variant::Decimal4
        rescale_decimal(
          variant.value.integer.to_i128, DECIMAL4_MAX_PRECISION, variant.value.scale, precision,
          scale, T
        )
      when ParquetVariant::Variant::Decimal8
        rescale_decimal(
          variant.value.integer.to_i128, DECIMAL8_MAX_PRECISION, variant.value.scale, precision,
          scale, T
        )
      when ParquetVariant::Variant::Decimal16
        rescale_decimal(
          variant.value.integer.to_i128, DECIMAL16_MAX_PRECISION, variant.value.scale, precision,
          scale, T
        )
      else
        nil
      end
    end

    # Rescales a variant to the unscaled integer of `type`, accepting only exact
    # conversions (upstream `shred_variant_to_unscaled_decimal`).
    private def self.shred_unscaled_decimal(
      variant : ParquetVariant::Variant,
      precision : Int32,
      scale : Int32,
      type : T.class,
    ) : T? forall T
      case variant
      when ParquetVariant::Variant::Int8
        convert_exact_decimal(variant.value.to_i128, DECIMAL4_MAX_PRECISION, 0, precision, scale, T)
      when ParquetVariant::Variant::Int16
        convert_exact_decimal(variant.value.to_i128, DECIMAL4_MAX_PRECISION, 0, precision, scale, T)
      when ParquetVariant::Variant::Int32
        convert_exact_decimal(variant.value.to_i128, DECIMAL4_MAX_PRECISION, 0, precision, scale, T)
      when ParquetVariant::Variant::Int64
        convert_exact_decimal(variant.value.to_i128, DECIMAL8_MAX_PRECISION, 0, precision, scale, T)
      when ParquetVariant::Variant::Decimal4
        convert_exact_decimal(
          variant.value.integer.to_i128, DECIMAL4_MAX_PRECISION, variant.value.scale, precision,
          scale, T
        )
      when ParquetVariant::Variant::Decimal8
        convert_exact_decimal(
          variant.value.integer.to_i128, DECIMAL8_MAX_PRECISION, variant.value.scale, precision,
          scale, T
        )
      when ParquetVariant::Variant::Decimal16
        convert_exact_decimal(
          variant.value.integer.to_i128, DECIMAL16_MAX_PRECISION, variant.value.scale, precision,
          scale, T
        )
      else
        nil
      end
    end

    # Rescales with half-away-from-zero rounding, then narrows to `type`.
    private def self.rescale_decimal(
      value : Int128,
      input_precision : Int32,
      input_scale : Int32,
      precision : Int32,
      scale : Int32,
      type : T.class,
    ) : T? forall T
      {% if T == ::Arrow::I256 %}
        ::Arrow.rescale_decimal(::Arrow::I256.new(value), input_precision, input_scale, precision, scale)
      {% else %}
        rescaled = ::Arrow.rescale_decimal(value, input_precision, input_scale, precision, scale)
        return nil unless rescaled
        ::Arrow.num_cast(rescaled, T)
      {% end %}
    end

    # Rescales only when the conversion is lossless (upstream
    # `convert_to_unscaled_decimal`).
    private def self.convert_exact_decimal(
      value : Int128,
      input_precision : Int32,
      input_scale : Int32,
      precision : Int32,
      scale : Int32,
      type : T.class,
    ) : T? forall T
      converted = ::Arrow.rescale_decimal(value, input_precision, input_scale, precision, scale)
      return nil unless converted

      converted_back =
        ::Arrow.rescale_decimal(converted, precision, scale, input_precision, input_scale)
      return nil unless converted_back == value

      ::Arrow.num_cast(converted, T)
    end

    # Rust single_float_to_decimal rounds to the native integer range. It does
    # not apply a separate declared-precision or exact-f64-magnitude cutoff.
    private def self.float_to_decimal(
      value : Float64,
      precision : Int32,
      scale : Int32,
      type : T.class,
    ) : T? forall T
      rounded = (value * (10.0 ** scale)).round(:ties_away)
      return nil unless rounded.finite?
      {% if T == ::Arrow::I256 %}
        ::Arrow::I256.from_f64(rounded)
      {% else %}
        ::Arrow.num_cast(rounded, T)
      {% end %}
    end

    # Parse decimal syntax before narrowing: an oversized mantissa may become
    # representable after an exponent shift. Only retained digits are converted
    # to a native integer, bounding power-of-ten allocation by target precision.
    private def self.parse_decimal(
      value : String,
      precision : Int32,
      scale : Int32,
      type : T.class,
    ) : T? forall T
      max_precision = {% if T == ::Arrow::I256 %} 76 {% elsif T == Int128 %} 38 {% elsif T == Int64 %} 18 {% else %} 9 {% end %}
      return nil unless 0 <= precision <= max_precision
      text = value.strip { |char| char == ' ' || char == '\t' || char == '\n' || char == '\r' || char == '\v' || char == '\f' }
      match = /\A([+-]?)([0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE]([+-]?[0-9]+))?\z/.match(text)
      return nil unless match
      negative = match[1] == "-"
      integer, _, fraction = match[2].partition('.')
      digits = (integer + fraction).lstrip('0')
      return T.new(0) if digits.empty?
      exponent = decimal_exponent(match[3]?, text.bytesize.to_i64 + 256)
      power = scale.to_i64 + exponent - fraction.bytesize
      rounding = false
      if power >= 0
        return nil if digits.bytesize.to_i64 + power > precision
        digits += "0" * power.to_i32
      else
        keep = digits.bytesize.to_i64 + power
        return T.new(0) if keep < 0
        return nil if keep > precision
        rounding = digits.byte_at(keep.to_i32) >= '5'.ord
        digits = keep == 0 ? "0" : digits.byte_slice(0, keep.to_i32)
      end
      # A carry can grow the digit count (9.5 -> 10); recheck the result below.
      number = {% if T == ::Arrow::I256 %}
                 ::Arrow::I256.from_string(digits)
               {% else %}
                 digits.to_i128?
               {% end %}
      return nil unless number
      if rounding
        {% if T == ::Arrow::I256 %}
          number += ::Arrow::I256.new(1)
        {% else %}
          number += 1
        {% end %}
      end
      return nil if number.to_s.bytesize > precision && !number.zero?
      number = -number if negative
      {% if T == ::Arrow::I256 %}
        number
      {% else %}
        ::Arrow.num_cast(number, T)
      {% end %}
    end

    # Saturate only beyond all possible mantissa/scale influence. This handles
    # arbitrarily long signed exponents without native integer overflow.
    private def self.decimal_exponent(text : String?, bound : Int64) : Int64
      return 0_i64 unless text
      negative = text.starts_with?('-')
      start = negative || text.starts_with?('+') ? 1 : 0
      exponent = 0_i64
      text.byte_slice(start).each_byte do |digit|
        exponent = Math.min(bound, exponent * 10 + digit - '0'.ord)
      end
      negative ? -exponent : exponent
    end
  end
end
