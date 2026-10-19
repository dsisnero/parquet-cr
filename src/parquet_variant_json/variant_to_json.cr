require "json"
require "../parquet_variant"
require "../parquet/errors"
require "./formatting"

# Ported from `parquet-variant-json/src/to_json.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `VariantToJson` (`to_json`, `to_json_string`, `to_json_value`) and the
# object/list converters behind it.
#
# Divergences: the `VariantToJson` extension trait becomes methods on
# `ParquetVariant::Variant`; `serde_json::Value` is `JSON::Any` (which holds
# `Int64`/`Float64` numbers, so a `u64` above `Int64::MAX` becomes a `Float64`);
# `ArrowError` is `Parquet::ArrowError`; date/timestamp/time payloads are
# `Time`/`Time::Span`.
module ParquetVariantJson
  # Writes an object as a JSON object, preserving field order (upstream
  # `convert_object_to_json`).
  def self.convert_object_to_json(io : IO, object : ParquetVariant::VariantObject) : Nil
    io << '{'
    first = true
    object.iter.each do |(key, value)|
      unless first
        io << ','
      end
      first = false
      key.to_json(io)
      io << ':'
      value.to_json(io)
    end
    io << '}'
  end

  # Writes a list as a JSON array (upstream `convert_array_to_json`).
  def self.convert_list_to_json(io : IO, list : ParquetVariant::VariantList) : Nil
    io << '['
    first = true
    list.iter.each do |element|
      unless first
        io << ','
      end
      first = false
      element.to_json(io)
    end
    io << ']'
  end

  # Converts a `Decimal4` to a JSON number (upstream `to_json_value`'s
  # `Variant::Decimal4` branch).
  def self.decimal4_json(decimal : ParquetVariant::VariantDecimal4) : JSON::Any
    integer = decimal.integer
    scale = decimal.scale.to_i32
    return JSON::Any.new(integer.to_i64) if scale == 0

    divisor = 10_i32 ** scale
    if integer.remainder(divisor) != 0
      JSON::Any.new(integer.to_f64 / divisor.to_f64)
    else
      JSON::Any.new(integer.tdiv(divisor).to_i64)
    end
  end

  # Converts a `Decimal8` to a JSON number.
  def self.decimal8_json(decimal : ParquetVariant::VariantDecimal8) : JSON::Any
    integer = decimal.integer
    scale = decimal.scale.to_i32
    return JSON::Any.new(integer) if scale == 0

    divisor = 10_i64 ** scale
    if integer.remainder(divisor) != 0
      JSON::Any.new(integer.to_f64 / divisor.to_f64)
    else
      JSON::Any.new(integer.tdiv(divisor))
    end
  end

  # Converts a `Decimal16` to a JSON number.
  def self.decimal16_json(decimal : ParquetVariant::VariantDecimal16) : JSON::Any
    integer = decimal.integer
    scale = decimal.scale.to_i32
    unless scale == 0
      divisor = 10_i128 ** scale
      if integer.remainder(divisor) != 0
        return JSON::Any.new(integer.to_f64 / divisor.to_f64)
      end
      integer = integer.tdiv(divisor)
    end

    # `i128` has higher precision than any 64-bit type: narrow losslessly when
    # possible, otherwise fall back to a (lossy) `Float64`.
    if Int64::MIN <= integer <= Int64::MAX
      JSON::Any.new(integer.to_i64)
    else
      JSON::Any.new(integer.to_f64)
    end
  end
end

module ParquetVariant
  # `VariantToJson` (upstream trait) as instance methods.
  class Variant
    # Writes this variant as JSON to `io` (upstream `VariantToJson::to_json`).
    #
    # This is also Crystal's `Object#to_json(IO)` hook, so variants nested in
    # `JSON::Serializable` values serialize correctly.
    def to_json(io : IO) : Nil
      variant = self
      case variant
      when Null
        io << "null"
      when BooleanTrue
        io << "true"
      when BooleanFalse
        io << "false"
      when Int8, Int16, Int32, Int64
        io << variant.value
      when Float
        io << ParquetVariantJson::Formatting.float32_string(variant.value)
      when Double
        io << ParquetVariantJson::Formatting.float64_string(variant.value)
      when Decimal4, Decimal8, Decimal16
        io << variant.value
      when Date
        io << '"' << ParquetVariantJson::Formatting.date_string(variant.value) << '"'
      when TimestampMicros, TimestampNanos
        io << '"' << ParquetVariantJson::Formatting.rfc3339(variant.value) << '"'
      when TimestampNtzMicros
        io << '"' << ParquetVariantJson::Formatting.timestamp_ntz_string(variant.value, 6) << '"'
      when TimestampNtzNanos
        io << '"' << ParquetVariantJson::Formatting.timestamp_ntz_string(variant.value, 9) << '"'
      when Time
        io << '"' << ParquetVariantJson::Formatting.time_string(variant.value) << '"'
      when Binary
        # Encode binary as a base64 string.
        io << '"' << ParquetVariantJson::Formatting.binary_base64(variant.value) << '"'
      when String
        variant.value.to_json(io)
      when ShortString
        variant.value.as_str.to_json(io)
      when Uuid
        io << '"' << variant.value.to_s << '"'
      when Object
        ParquetVariantJson.convert_object_to_json(io, variant.value)
      when List
        ParquetVariantJson.convert_list_to_json(io, variant.value)
      else
        raise arrow_err!("Unsupported variant type: #{variant.class}")
      end
    end

    # Converts this variant to a JSON string (upstream
    # `VariantToJson::to_json_string`).
    def to_json_string : ::String
      ::String.build { |io| to_json(io) }
    end

    # Converts this variant to a `JSON::Any` (upstream
    # `VariantToJson::to_json_value`).
    def to_json_value : JSON::Any
      variant = self
      case variant
      when Null
        JSON::Any.new(nil)
      when BooleanTrue
        JSON::Any.new(true)
      when BooleanFalse
        JSON::Any.new(false)
      when Int8, Int16, Int32, Int64
        JSON::Any.new(variant.value.to_i64)
      when Float
        value = variant.value.to_f64
        raise arrow_err!("Invalid float value") unless value.finite?

        JSON::Any.new(value)
      when Double
        value = variant.value
        raise arrow_err!("Invalid double value") unless value.finite?

        JSON::Any.new(value)
      when Decimal4
        ParquetVariantJson.decimal4_json(variant.value)
      when Decimal8
        ParquetVariantJson.decimal8_json(variant.value)
      when Decimal16
        ParquetVariantJson.decimal16_json(variant.value)
      when Date
        JSON::Any.new(ParquetVariantJson::Formatting.date_string(variant.value))
      when TimestampMicros, TimestampNanos
        JSON::Any.new(ParquetVariantJson::Formatting.rfc3339(variant.value))
      when TimestampNtzMicros
        JSON::Any.new(ParquetVariantJson::Formatting.timestamp_ntz_string(variant.value, 6))
      when TimestampNtzNanos
        JSON::Any.new(ParquetVariantJson::Formatting.timestamp_ntz_string(variant.value, 9))
      when Time
        JSON::Any.new(ParquetVariantJson::Formatting.time_string(variant.value))
      when Binary
        JSON::Any.new(ParquetVariantJson::Formatting.binary_base64(variant.value))
      when String
        JSON::Any.new(variant.value)
      when ShortString
        JSON::Any.new(variant.value.as_str)
      when Uuid
        JSON::Any.new(variant.value.to_s)
      when Object
        hash = {} of ::String => JSON::Any
        variant.value.iter.each { |(key, value)| hash[key] = value.to_json_value }
        JSON::Any.new(hash)
      when List
        JSON::Any.new(variant.value.iter.map(&.to_json_value))
      else
        raise arrow_err!("Unsupported variant type: #{variant.class}")
      end
    end
  end
end
