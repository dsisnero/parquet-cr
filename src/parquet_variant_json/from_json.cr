require "json"
require "../parquet/errors"
require "../parquet_variant"
require "./variant_to_json"

# Ported from `parquet-variant-json/src/from_json.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `JsonToVariant`/`append_json` and the `variant_from_number` width selection.
#
# Divergences: `serde_json::Value` is `JSON::Any`, `ArrowError` is
# `Parquet::ArrowError` ("JSON format error: ..." for invalid documents), and
# the blanket `impl<T: VariantBuilderExt> JsonToVariant for T` is explicit
# `include`s on `VariantBuilder`, `ListBuilder` and `ObjectFieldBuilder`. JSON
# numbers become the narrowest `Int8..Int64` that fits, or a `Double` for
# fractional/exponent notation; upstream's decimal expectations are `#[ignore]`d
# (JSON text does not preserve decimals) and are not ported.
module ParquetVariantJson
  # Appends a JSON document to a variant builder (upstream `JsonToVariant`).
  module JsonToVariant
    # Parses `json` and appends it to this builder.
    def append_json(json : String) : Nil
      value =
        begin
          JSON.parse(json)
        rescue ex : JSON::ParseException
          raise arrow_err!("JSON format error: #{ex.message}")
        end

      ParquetVariantJson.append_json(value, self.as(ParquetVariant::VariantBuilderExt))
    end
  end

  # Appends an already parsed JSON value to `builder` (upstream free function
  # `append_json`).
  def self.append_json(json : JSON::Any, builder : ParquetVariant::VariantBuilderExt) : Nil
    raw = json.raw
    case raw
    when Nil
      builder.append_value(ParquetVariant::Variant.from_nil)
    when Bool
      builder.append_value(raw)
    when Int64
      builder.append_value(variant_from_int(raw))
    when Float64
      builder.append_value(ParquetVariant::Variant::Double.new(raw))
    when String
      builder.append_value(raw)
    when Array(JSON::Any)
      list = builder.try_new_list
      raw.each { |element| append_json(element, list) }
      list.finish
    when Hash(String, JSON::Any)
      object = builder.try_new_object
      raw.each do |key, value|
        append_json(value, ParquetVariant::ObjectFieldBuilder.new(key, object))
      end
      object.finish
    else
      raise arrow_err!("Unsupported JSON value: #{json}")
    end
    nil
  end

  # Picks the narrowest integer variant that fits (upstream
  # `variant_from_number`).
  private def self.variant_from_int(value : Int64) : ParquetVariant::Variant
    if Int8::MIN <= value <= Int8::MAX
      ParquetVariant::Variant::Int8.new(value.to_i8)
    elsif Int16::MIN <= value <= Int16::MAX
      ParquetVariant::Variant::Int16.new(value.to_i16)
    elsif Int32::MIN <= value <= Int32::MAX
      ParquetVariant::Variant::Int32.new(value.to_i32)
    else
      ParquetVariant::Variant::Int64.new(value)
    end
  end
end

module ParquetVariant
  class VariantBuilder
    include ParquetVariantJson::JsonToVariant
  end

  class ListBuilder
    include ParquetVariantJson::JsonToVariant
  end

  class ObjectFieldBuilder
    include ParquetVariantJson::JsonToVariant
  end
end
