require "arrow-cr"
require "../../basic"
require "../../schema/types"
require "../../../parquet_geospatial/types"
require "../../../parquet_variant_compute/variant_array"

# parquet/src/arrow/schema/extension.rs, apache/arrow-rs 60.0.0.
# Compile-time flags mirror the upstream nondefault Cargo features.
module Parquet
  # Conversions between Parquet thrift codes and GeoArrow interpolation edges.
  struct EdgeInterpolationAlgorithm
    def self.from(edges : ParquetGeospatial::Edges) : self
      new(edges.value)
    end

    def try_as_edges : ParquetGeospatial::Edges
      case self
      when SPHERICAL then ParquetGeospatial::Edges::Spherical
      when VINCENTY  then ParquetGeospatial::Edges::Vincenty
      when THOMAS    then ParquetGeospatial::Edges::Thomas
      when ANDOYER   then ParquetGeospatial::Edges::Andoyer
      when KARNEY    then ParquetGeospatial::Edges::Karney
      else
        raise general_err!("Unknown edge interpolation algorithm: #{self}")
      end
    end
  end

  module Arrow
    # Attaches an enabled logical type's Arrow extension to a copied field.
    def self.try_add_extension_type(field : ::Arrow::Field, parquet_type : Schema::Type) : ::Arrow::Field
      logical = parquet_type.basic_info.logical_type_ref
      extension = case logical
                  when LogicalType::Uuid
                    {% if flag?(:arrow_canonical_extension_types) %}
                      ::Arrow::Extension::Canonical::Uuid.new
                    {% else %} nil {% end %}
                  when LogicalType::Json
                    {% if flag?(:arrow_canonical_extension_types) %}
                      ::Arrow::Extension::Canonical::Json.new
                    {% else %} nil {% end %}
                  when LogicalType::Variant
                    {% if flag?(:variant_experimental) %}
                      ParquetVariantCompute::VariantType.new
                    {% else %} nil {% end %}
                  when LogicalType::Geometry
                    {% if flag?(:geospatial) %}
                      ParquetGeospatial::WkbType.new(ParquetGeospatial::Metadata.new(arrow_crs(logical.value.crs)))
                    {% else %} nil {% end %}
                  when LogicalType::Geography
                    {% if flag?(:geospatial) %}
                      edges = logical.value.algorithm_or_default.try_as_edges
                      ParquetGeospatial::WkbType.new(ParquetGeospatial::Metadata.new(arrow_crs(logical.value.crs), edges))
                    {% else %} nil {% end %}
                  else nil
                  end
      return field unless extension
      result = ::Arrow::Field.new(field.name, field.data_type, field.nullable?, field.metadata.dup)
      result.with_extension_type(extension)
    end

    # Whether logical conversion would attach extension metadata.
    def self.has_extension_type?(parquet_type : Schema::Type) : Bool
      case parquet_type.basic_info.logical_type_ref
      when LogicalType::Uuid, LogicalType::Json
        {{ flag?(:arrow_canonical_extension_types) }}
      when LogicalType::Variant
        {{ flag?(:variant_experimental) }}
      when LogicalType::Geometry, LogicalType::Geography
        {{ flag?(:geospatial) }}
      else false
      end
    end

    # Returns JSON logical type only for a validated canonical extension.
    def self.logical_type_for_string(field : ::Arrow::Field) : LogicalType?
      {% if flag?(:arrow_canonical_extension_types) %}
        return LogicalType::Json.new if field.has_valid_extension_type?(::Arrow::Extension::Canonical::Json)
      {% end %}
      LogicalType::String.new
    end

    # Valid UUID storage maps to Parquet UUID when canonical mapping is enabled.
    def self.logical_type_for_fixed_size_binary(field : ::Arrow::Field) : LogicalType?
      {% if flag?(:arrow_canonical_extension_types) %}
        return LogicalType::Uuid.new if field.has_valid_extension_type?(::Arrow::Extension::Canonical::Uuid)
      {% end %}
      nil
    end

    # Valid Variant storage maps to Parquet Variant when enabled.
    def self.logical_type_for_struct(field : ::Arrow::Field) : LogicalType?
      {% if flag?(:variant_experimental) %}
        return LogicalType.variant(nil) if field.has_valid_extension_type?(ParquetVariantCompute::VariantType)
      {% end %}
      nil
    end

    # Maps valid geoarrow.wkb metadata to Geometry or Geography.
    def self.logical_type_for_binary(field : ::Arrow::Field) : LogicalType?
      {% if flag?(:geospatial) %}
        return nil unless field.extension_type_name == ParquetGeospatial::WkbType::NAME
        begin
          metadata = field.try_extension_type(ParquetGeospatial::WkbType).metadata
        rescue ::Arrow::ArrowError | ParquetGeospatial::JsonError
          return nil
        end
        crs = if value = metadata.crs
                metadata.crs_is_lon_lat ? nil : (value.as_s? || value.to_json)
              else
                "srid:0"
              end
        if edges = metadata.algorithm
          algorithm = edges.spherical? ? nil : EdgeInterpolationAlgorithm.from(edges)
          LogicalType.geography(crs, algorithm)
        else
          LogicalType.geometry(crs)
        end
      {% else %}
        nil
      {% end %}
    end

    # Binary views share the binary extension mapping.
    def self.logical_type_for_binary_view(field : ::Arrow::Field) : LogicalType?
      logical_type_for_binary(field)
    end

    private def self.arrow_crs(crs : String?) : String?
      return "OGC:CRS84" unless crs
      crs == "srid:0" ? nil : crs
    end
  end
end
