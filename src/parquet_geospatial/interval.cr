require "./errors"

# Ported from `parquet-geospatial/src/interval.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `IntervalTrait`, `Interval`, and `WraparoundInterval` (1D intervals with
# wraparound support for geospatial bounds).
# Divergences: the `IntervalTrait` trait is a marker module (the concrete structs
# implement the methods), `Interval`/`WraparoundInterval` are value structs with
# explicit `==`, `TryFrom<WraparoundInterval>` is `Interval.from_wraparound`
# (raising), and NaN-ignoring `min`/`max` are explicit helpers (Crystal's
# `Float64#min`/`max` propagate NaN).
module ParquetGeospatial
  # Common behaviour shared by `Interval` and `WraparoundInterval`.
  module IntervalTrait
  end

  # A 1D interval that never wraps around.
  struct Interval
    include IntervalTrait

    getter lo : Float64
    getter hi : Float64

    def initialize(@lo : Float64, @hi : Float64)
    end

    # The empty interval (which intersects no values).
    def self.empty : Interval
      new(Float64::INFINITY, -Float64::INFINITY)
    end

    # The full interval (which intersects everything).
    def self.full : Interval
      new(-Float64::INFINITY, Float64::INFINITY)
    end

    # Converts a non-wraparound `WraparoundInterval`, raising otherwise.
    def self.from_wraparound(value : WraparoundInterval) : Interval
      if value.wraparound?
        raise InvalidArgumentError.new("Can't convert wraparound interval #{value.inspect} to Interval")
      end

      new(value.lo, value.hi)
    end

    # Expands this interval to the union of `self` and `other`, in place.
    def update_interval(other : Interval) : Nil
      @lo = Interval.min_ignoring_nan(@lo, other.lo)
      @hi = Interval.max_ignoring_nan(@hi, other.hi)
      nil
    end

    # Expands this interval to include `other`, in place.
    def update_value(other : Float64) : Nil
      @lo = Interval.min_ignoring_nan(@lo, other)
      @hi = Interval.max_ignoring_nan(@hi, other)
      nil
    end

    def wraparound? : Bool
      false
    end

    def intersects_value(value : Float64) : Bool
      value >= @lo && value <= @hi
    end

    def intersects_interval(other : Interval) : Bool
      @lo <= other.hi && other.lo <= @hi
    end

    def contains_interval(other : Interval) : Bool
      @lo <= other.lo && @hi >= other.hi
    end

    def width : Float64
      @hi - @lo
    end

    def mid : Float64
      @lo + width / 2.0
    end

    def empty? : Bool
      width == -Float64::INFINITY
    end

    def merge_interval(other : Interval) : Interval
      out = Interval.new(@lo, @hi)
      out.update_interval(other)
      out
    end

    def merge_value(other : Float64) : Interval
      out = Interval.new(@lo, @hi)
      out.update_value(other)
      out
    end

    def expand_by(distance : Float64) : Interval
      return self if empty? || distance.nan? || distance < 0.0

      Interval.new(@lo - distance, @hi + distance)
    end

    def ==(other : Interval) : Bool
      @lo == other.lo && @hi == other.hi
    end

    protected def self.min_ignoring_nan(a : Float64, b : Float64) : Float64
      return b if a.nan?
      return a if b.nan?

      a < b ? a : b
    end

    protected def self.max_ignoring_nan(a : Float64, b : Float64) : Float64
      return b if a.nan?
      return a if b.nan?

      a > b ? a : b
    end
  end

  # A 1D interval that may or may not wrap around.
  struct WraparoundInterval
    include IntervalTrait

    getter inner : Interval

    def initialize(@inner : Interval)
    end

    # Creates a wraparound interval from `lo`/`hi`.
    def self.new(lo : Float64, hi : Float64) : WraparoundInterval
      new(Interval.new(lo, hi))
    end

    # The empty interval.
    def self.empty : WraparoundInterval
      new(Interval.empty)
    end

    # The full interval.
    def self.full : WraparoundInterval
      new(Interval.full)
    end

    # Converts an `Interval` (upstream `From<Interval>`).
    def self.from_interval(value : Interval) : WraparoundInterval
      new(value)
    end

    def lo : Float64
      @inner.lo
    end

    def hi : Float64
      @inner.hi
    end

    def wraparound? : Bool
      !empty? && @inner.width < 0.0
    end

    def intersects_value(value : Float64) : Bool
      left, right = split
      left.intersects_value(value) || right.intersects_value(value)
    end

    def intersects_interval(other : WraparoundInterval) : Bool
      left, right = split
      other_left, other_right = other.split
      left.intersects_interval(other_left) ||
        left.intersects_interval(other_right) ||
        right.intersects_interval(other_left) ||
        right.intersects_interval(other_right)
    end

    def contains_interval(other : WraparoundInterval) : Bool
      left, right = split
      other_left, other_right = other.split
      left.contains_interval(other_left) && right.contains_interval(other_right)
    end

    def width : Float64
      wraparound? ? Float64::INFINITY : @inner.width
    end

    def mid : Float64
      wraparound? ? Float64::INFINITY : @inner.mid
    end

    def empty? : Bool
      @inner.empty?
    end

    def merge_interval(other : WraparoundInterval) : WraparoundInterval
      return other if empty?
      return self if other.empty?

      if wraparound? && other.wraparound?
        left, right = split
        other_left, other_right = other.split
        new_left = left.merge_interval(other_left)
        new_right = right.merge_interval(other_right)
        return new_left.intersects_interval(new_right) ? WraparoundInterval.full : WraparoundInterval.new(new_right.lo, new_left.hi)
      elsif !wraparound? && !other.wraparound?
        return WraparoundInterval.new(@inner.merge_interval(other.inner))
      end

      wraparound, not_wraparound = wraparound? ? {self, other} : {other, self}
      left, right = wraparound.split
      distance_not_wraparound_left = (not_wraparound.mid - left.hi).abs
      distance_not_wraparound_right = (not_wraparound.mid - right.lo).abs
      new_left, new_right =
        if distance_not_wraparound_left < distance_not_wraparound_right
          {left.merge_interval(not_wraparound.inner), right}
        else
          {left, right.merge_interval(not_wraparound.inner)}
        end

      if new_left.intersects_interval(new_right)
        WraparoundInterval.full
      else
        WraparoundInterval.new(new_right.lo, new_left.hi)
      end
    end

    def merge_value(value : Float64) : WraparoundInterval
      return self if intersects_value(value) || value.nan?

      unless wraparound?
        return WraparoundInterval.new(@inner.merge_value(value))
      end

      distance_left = value - @inner.hi
      distance_right = @inner.lo - value
      if distance_left < distance_right
        WraparoundInterval.new(Interval.new(@inner.lo, value))
      else
        WraparoundInterval.new(Interval.new(value, @inner.hi))
      end
    end

    def expand_by(distance : Float64) : WraparoundInterval
      return self if empty? || distance.nan? || distance < 0.0

      unless wraparound?
        return WraparoundInterval.new(@inner.expand_by(distance))
      end

      excluded_lo = @inner.hi + distance
      excluded_hi = @inner.lo - distance
      return WraparoundInterval.full if excluded_lo >= excluded_hi

      WraparoundInterval.new(excluded_hi, excluded_lo)
    end

    def ==(other : WraparoundInterval) : Bool
      @inner == other.inner
    end

    # Splits this interval into exactly two non-wraparound intervals.
    protected def split : Tuple(Interval, Interval)
      if wraparound?
        {
          Interval.new(-Float64::INFINITY, @inner.hi),
          Interval.new(@inner.lo, Float64::INFINITY),
        }
      else
        {@inner, Interval.empty}
      end
    end
  end
end
