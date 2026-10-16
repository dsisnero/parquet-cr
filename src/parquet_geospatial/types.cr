require "json"
require "../parquet/errors"

# Ported from `parquet-geospatial/src/types.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `Hint`, `Edges`, `Metadata`, and the metadata side of `WkbType`.
# Divergences: `serde_json::Value` is `JSON::Any`; `Result`/`Option` map to
# raises/`nil`; the `arrow_schema::ExtensionType` impl (`NAME`,
# `supports_data_type`, `try_new`, `ExtensionType` registration on `Field`) is
# deferred to the Arrow interop chunk; `ArrowError::JsonError` is
# `ParquetGeospatial::JsonError`.
module ParquetGeospatial
  # `ArrowError::JsonError`.
  class JsonError < ::Parquet::ParquetError
    def initialize(message : String)
      super("Json error: #{message}")
    end
  end

  # A hint at the likely Parquet geospatial logical type.
  enum Hint
    Geometry
    Geography
  end

  # The edge interpolation algorithms used with `GEOMETRY` logical types.
  enum Edges
    Spherical
    Vincenty
    Thomas
    Andoyer
    Karney

    def self.default : Edges
      Spherical
    end

    # The lowercase JSON spelling (`#[serde(rename_all = "lowercase")]`).
    def json_key : String
      to_s.downcase
    end

    def self.from_json_key(key : String) : Edges
      case key
      when "spherical" then Spherical
      when "vincenty"  then Vincenty
      when "thomas"    then Thomas
      when "andoyer"   then Andoyer
      when "karney"    then Karney
      else
        raise JsonError.new("unknown edges value: #{key}")
      end
    end
  end

  # The metadata associated with a `WkbType`.
  struct Metadata
    property crs : JSON::Any?
    property algorithm : Edges?

    def initialize(crs : String? = nil, algorithm : Edges? = nil)
      @crs = crs.nil? ? nil : Metadata.parse_crs(crs)
      @algorithm = algorithm
    end

    def self.default : Metadata
      new
    end

    # Returns a hint at the likely underlying logical type.
    def type_hint : Hint
      @algorithm.nil? ? Hint::Geometry : Hint::Geography
    end

    # Detects whether the CRS is a common lon/lat representation (WGS84).
    def crs_is_lon_lat : Bool
      crs = @crs
      return false if crs.nil?

      if string = crs.as_s?
        string == "EPSG:4326" || string == "OGC:CRS84"
      elsif crs.as_h?
        id = crs["id"]?
        return false if id.nil?

        authority = id["authority"]?.try(&.as_s?)
        code = id["code"]?
        return true if authority == "OGC" && code.try(&.as_s?) == "CRS84"
        return true if authority == "EPSG" && code.try(&.as_s?) == "4326"
        return true if authority == "EPSG" && code.try(&.as_i64?) == 4326

        false
      else
        false
      end
    end

    def to_json : String
      JSON.build { |json| to_json(json) }
    end

    def to_json(json : JSON::Builder) : Nil
      json.object do
        if crs = @crs
          json.field "crs", crs
        end
        if algorithm = @algorithm
          json.field "edges", algorithm.json_key
        end
      end
    end

    def self.from_json(string : String) : Metadata
      parser = JSON::PullParser.new(string)
      metadata = Metadata.new
      parser.read_object do |key|
        case key
        when "crs"   then metadata.crs = JSON::Any.new(parser)
        when "edges" then metadata.algorithm = Edges.from_json_key(parser.read_string)
        else              parser.skip
        end
      end
      metadata
    end

    def self.parse_crs(crs : String) : JSON::Any
      JSON.parse(crs)
    rescue JSON::ParseException
      JSON::Any.new(crs)
    end
  end

  # Well-Known Binary (WKB) extension type for geospatial data.
  class WkbType
    def initialize(metadata : Metadata? = nil)
      @metadata = metadata || Metadata.default
    end

    def metadata : Metadata
      @metadata
    end

    # Serializes the metadata as JSON, or `nil` on failure.
    def serialize_metadata : String?
      @metadata.to_json
    rescue
      nil
    end

    # Deserializes metadata JSON (absent metadata yields the default).
    def self.deserialize_metadata(metadata : String?) : Metadata
      return Metadata.default if metadata.nil?

      Metadata.from_json(metadata)
    rescue ex : JSON::ParseException
      raise JsonError.new(ex.message || "invalid JSON")
    end
  end
end
