require "./errors"

# Minimal Well-Known Binary (WKB) reader and geometry model.
#
# Upstream `bounding.rs` operates on geometries exposed through the `geo-traits`
# crate and parses WKB with the `wkb` crate; Crystal has neither, so this file
# provides the subset needed by `GeometryBounder`: an owned geometry model and a
# little/big-endian WKB reader for the seven ISO geometry types.
#
# Divergences: only the ISO 2D/Z/M/ZM geometry type codes are supported; WKB
# parse errors raise `ParquetGeospatial::InvalidArgumentError` (upstream maps
# `wkb` errors to `ArrowError::ExternalError`); empty points are represented by
# NaN coordinates (which the bounder ignores, matching upstream).
module ParquetGeospatial
  # The coordinate dimension of a geometry, encoded in the WKB type code.
  enum WkbDimension
    Xy
    Xyz
    Xym
    Xyzm

    # Number of coordinate components (2, 3, 3, or 4).
    def coord_count : Int32
      case self
      when Xy       then 2
      when Xyz, Xym then 3
      else               4
      end
    end
  end

  # ISO WKB geometry type codes.
  enum WkbGeometryKind
    Point              = 1
    LineString         = 2
    Polygon            = 3
    MultiPoint         = 4
    MultiLineString    = 5
    MultiPolygon       = 6
    GeometryCollection = 7
  end

  # A single coordinate (2–4 components, ordered x, y, [z], [m]).
  alias WkbCoord = Array(Float64)

  # A parsed WKB geometry.
  abstract class WkbGeometry
    getter dimension : WkbDimension

    def initialize(@dimension : WkbDimension)
    end

    abstract def kind : WkbGeometryKind
  end

  # A `Point` (an empty point holds NaN coordinates).
  class WkbPoint < WkbGeometry
    getter coord : WkbCoord

    def initialize(dimension : WkbDimension, @coord : WkbCoord)
      super(dimension)
    end

    def self.empty(dimension : WkbDimension) : WkbPoint
      new(dimension, WkbCoord.new(dimension.coord_count, Float64::NAN))
    end

    def empty? : Bool
      @coord.all?(&.nan?)
    end

    def kind : WkbGeometryKind
      WkbGeometryKind::Point
    end
  end

  # A `LineString`.
  class WkbLineString < WkbGeometry
    getter coords : Array(WkbCoord)

    def initialize(dimension : WkbDimension, @coords : Array(WkbCoord))
      super(dimension)
    end

    def kind : WkbGeometryKind
      WkbGeometryKind::LineString
    end
  end

  # A `Polygon` (the first ring is the exterior, the rest are interiors).
  class WkbPolygon < WkbGeometry
    getter rings : Array(Array(WkbCoord))

    def initialize(dimension : WkbDimension, @rings : Array(Array(WkbCoord)))
      super(dimension)
    end

    def kind : WkbGeometryKind
      WkbGeometryKind::Polygon
    end
  end

  # A `MultiPoint`.
  class WkbMultiPoint < WkbGeometry
    getter points : Array(WkbPoint)

    def initialize(dimension : WkbDimension, @points : Array(WkbPoint))
      super(dimension)
    end

    def kind : WkbGeometryKind
      WkbGeometryKind::MultiPoint
    end
  end

  # A `MultiLineString`.
  class WkbMultiLineString < WkbGeometry
    getter line_strings : Array(WkbLineString)

    def initialize(dimension : WkbDimension, @line_strings : Array(WkbLineString))
      super(dimension)
    end

    def kind : WkbGeometryKind
      WkbGeometryKind::MultiLineString
    end
  end

  # A `MultiPolygon`.
  class WkbMultiPolygon < WkbGeometry
    getter polygons : Array(WkbPolygon)

    def initialize(dimension : WkbDimension, @polygons : Array(WkbPolygon))
      super(dimension)
    end

    def kind : WkbGeometryKind
      WkbGeometryKind::MultiPolygon
    end
  end

  # A `GeometryCollection`.
  class WkbGeometryCollection < WkbGeometry
    getter geometries : Array(WkbGeometry)

    def initialize(dimension : WkbDimension, @geometries : Array(WkbGeometry))
      super(dimension)
    end

    def kind : WkbGeometryKind
      WkbGeometryKind::GeometryCollection
    end
  end

  # Parses WKB byte buffers into `WkbGeometry` values.
  class WkbReader
    def self.parse(bytes : Bytes) : WkbGeometry
      new(bytes).parse_geometry
    end

    def initialize(bytes : Bytes)
      @io = IO::Memory.new(bytes)
      @format = IO::ByteFormat::LittleEndian
    end

    # Parses one geometry (with its own byte-order marker).
    def parse_geometry : WkbGeometry
      read_byte_order
      type_id = read_u32
      base = (type_id % 1000).to_i32
      dim_code = (type_id // 1000).to_i32
      dimension = dimension_for(dim_code)
      kind = kind_for(base)

      case kind
      when WkbGeometryKind::Point
        WkbPoint.new(dimension, read_coord(dimension))
      when WkbGeometryKind::LineString
        WkbLineString.new(dimension, read_coord_sequence(dimension))
      when WkbGeometryKind::Polygon
        WkbPolygon.new(dimension, read_rings(dimension))
      when WkbGeometryKind::MultiPoint
        WkbMultiPoint.new(dimension, read_children.map(&.as(WkbPoint)))
      when WkbGeometryKind::MultiLineString
        WkbMultiLineString.new(dimension, read_children.map(&.as(WkbLineString)))
      when WkbGeometryKind::MultiPolygon
        WkbMultiPolygon.new(dimension, read_children.map(&.as(WkbPolygon)))
      when WkbGeometryKind::GeometryCollection
        WkbGeometryCollection.new(dimension, read_children)
      else
        raise InvalidArgumentError.new("Unsupported geometry type: #{base}")
      end
    end

    private def read_byte_order : Nil
      case read_u8
      when 1_u8 then @format = IO::ByteFormat::LittleEndian
      when 0_u8 then @format = IO::ByteFormat::BigEndian
      else
        raise InvalidArgumentError.new("Invalid WKB byte order")
      end
      nil
    end

    private def dimension_for(dim_code : Int32) : WkbDimension
      case dim_code
      when 0 then WkbDimension::Xy
      when 1 then WkbDimension::Xyz
      when 2 then WkbDimension::Xym
      when 3 then WkbDimension::Xyzm
      else
        raise InvalidArgumentError.new("Unsupported dimensions: #{dim_code}")
      end
    end

    private def kind_for(base : Int32) : WkbGeometryKind
      WkbGeometryKind.new(base)
    rescue ArgumentError
      raise InvalidArgumentError.new("Unsupported geometry type: #{base}")
    end

    private def read_children : Array(WkbGeometry)
      count = read_u32
      children = Array(WkbGeometry).new(count)
      count.times { children << parse_geometry }
      children
    end

    private def read_rings(dimension : WkbDimension) : Array(Array(WkbCoord))
      count = read_u32
      rings = Array(Array(WkbCoord)).new(count)
      count.times { rings << read_coord_sequence(dimension) }
      rings
    end

    private def read_coord_sequence(dimension : WkbDimension) : Array(WkbCoord)
      count = read_u32
      coords = Array(WkbCoord).new(count)
      count.times { coords << read_coord(dimension) }
      coords
    end

    private def read_coord(dimension : WkbDimension) : WkbCoord
      WkbCoord.new(dimension.coord_count) { read_f64 }
    end

    private def read_u8 : UInt8
      @io.read_byte || raise InvalidArgumentError.new("Unexpected end of WKB")
    end

    private def read_u32 : UInt32
      @io.read_bytes(UInt32, @format)
    rescue IO::EOFError
      raise InvalidArgumentError.new("Unexpected end of WKB")
    end

    private def read_f64 : Float64
      @io.read_bytes(Float64, @format)
    rescue IO::EOFError
      raise InvalidArgumentError.new("Unexpected end of WKB")
    end
  end
end
