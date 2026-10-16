require "./errors"
require "./interval"
require "./wkb"

# Ported from `parquet-geospatial/src/bounding.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `GeometryBounder` and its dimension-visiting helpers, accumulating
# Parquet geospatial bounds (`x`/`y`/`z`/`m` intervals and the set of geometry
# type codes).
#
# Divergences: geometries are the local `WkbGeometry` model (upstream operates on
# `geo-traits`); `update_wkb` parses with `ParquetGeospatial::WkbReader` and
# raises `InvalidArgumentError` on malformed input (upstream `ExternalError`);
# `update_geometry` is public so tests can feed the geometry model directly (as
# upstream tests do via the `wkt` crate); `with_wraparound_hint` mutates and
# returns `self`.
module ParquetGeospatial
  # Accumulates geospatial statistics for geometries as they are written.
  class GeometryBounder
    def initialize
      @x_left = Interval.empty
      @x_mid = Interval.empty
      @x_right = Interval.empty
      @y = Interval.empty
      @z = Interval.empty
      @m = Interval.empty
      @geometry_types = Set(Int32).new
      @wraparound_hint = Interval.empty
    end

    # Creates a new, empty bounder (empty input).
    def self.empty : GeometryBounder
      new
    end

    # Sets the hint used for potential wraparound xmin/xmax output.
    def with_wraparound_hint(wraparound_hint : Interval) : GeometryBounder
      @wraparound_hint = wraparound_hint
      self
    end

    # The union of all contiguous x intervals to the left of the wraparound
    # midpoint.
    def x : WraparoundInterval
      out_all = Interval.empty
        .merge_interval(@x_left)
        .merge_interval(@x_mid)
        .merge_interval(@x_right)

      if !@x_mid.empty? || !@wraparound_hint.contains_interval(out_all)
        return WraparoundInterval.from_interval(out_all)
      end

      out_width = (@x_left.hi - @wraparound_hint.lo) +
                  (@wraparound_hint.hi - @x_right.lo)
      if out_all.width < out_width
        return WraparoundInterval.from_interval(out_all)
      end

      WraparoundInterval.new(@x_right.lo, @x_left.hi)
    end

    # The y interval.
    def y : Interval
      @y
    end

    # The z interval.
    def z : Interval
      @z
    end

    # The m interval.
    def m : Interval
      @m
    end

    # The sorted, unique set of ISO WKB geometry type codes encountered.
    def geometry_types : Array(Int32)
      @geometry_types.to_a.sort!
    end

    # Updates this bounder with one geometry model value.
    def update_geometry(geom : WkbGeometry) : Nil
      @geometry_types << geometry_type(geom)

      visit_intervals(geom, 'x') { |interval| update_x(interval) }
      visit_intervals(geom, 'y') { |interval| @y.update_interval(interval) }
      visit_intervals(geom, 'z') { |interval| @z.update_interval(interval) }
      visit_intervals(geom, 'm') { |interval| @m.update_interval(interval) }
      nil
    end

    # Updates this bounder with one WKB-encoded geometry.
    def update_wkb(wkb : Bytes) : Nil
      update_geometry(WkbReader.parse(wkb))
    end

    private def update_x(x : Interval) : Nil
      if x.hi < @wraparound_hint.mid
        @x_left.update_interval(x)
      elsif x.lo > @wraparound_hint.mid
        @x_right.update_interval(x)
      else
        @x_mid.update_interval(x)
      end
      nil
    end

    # Visits the contiguous intervals of `geom` for the given dimension.
    private def visit_intervals(geom : WkbGeometry, dimension : Char, &func : Interval ->) : Nil
      index = dimension_index(geom.dimension, dimension)
      return if index.nil?

      case geom
      when WkbPoint
        coord = geom.coord
        visit_point(coord, index, func)
      when WkbLineString
        visit_sequence(geom.coords, index, func)
      when WkbPolygon
        geom.rings.each { |ring| visit_sequence(ring, index, func) }
      when WkbMultiPoint
        geom.points.each { |child| visit_intervals(child, dimension, &func) }
      when WkbMultiLineString
        geom.line_strings.each { |child| visit_intervals(child, dimension, &func) }
      when WkbMultiPolygon
        geom.polygons.each { |child| visit_intervals(child, dimension, &func) }
      when WkbGeometryCollection
        geom.geometries.each { |child| visit_intervals(child, dimension, &func) }
      end
      nil
    end

    # Visits a point as a degenerate interval.
    private def visit_point(coord : WkbCoord, n : Int32, func : Interval ->) : Nil
      value = coord[n]
      func.call(Interval.new(value, value))
    end

    # Visits a sequence of coordinates as one contiguous interval.
    private def visit_sequence(coords : Array(WkbCoord), n : Int32, func : Interval ->) : Nil
      interval = Interval.empty
      coords.each { |coord| interval.update_value(coord[n]) }
      func.call(interval)
    end

    # The ISO WKB geometry type code (type + dimension).
    private def geometry_type(geom : WkbGeometry) : Int32
      dimension_type = case geom.dimension
                       when WkbDimension::Xy  then 0
                       when WkbDimension::Xyz then 1000
                       when WkbDimension::Xym then 2000
                       else                        3000
                       end
      dimension_type + geom.kind.value
    end

    # The coordinate index for `target` within `dim`, or `nil`.
    private def dimension_index(dim : WkbDimension, target : Char) : Int32?
      case target
      when 'x' then 0
      when 'y' then 1
      when 'z'
        case dim
        when WkbDimension::Xyz, WkbDimension::Xyzm then 2
        else                                            nil
        end
      when 'm'
        case dim
        when WkbDimension::Xym  then 2
        when WkbDimension::Xyzm then 3
        else                         nil
        end
      else
        nil
      end
    end
  end
end
