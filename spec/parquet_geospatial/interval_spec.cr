require "../spec_helper"

# Ported from `parquet-geospatial/src/interval.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: the generic helpers take a constructor `Proc` in place of Rust's
# `T::new`; `f64::INFINITY`/`f64::NAN` are `Float64::INFINITY`/`Float64::NAN`;
# `try_into` is `Interval.from_wraparound` (raising).

private alias Interval = ParquetGeospatial::Interval
private alias WraparoundInterval = ParquetGeospatial::WraparoundInterval

private def interval_new(lo : Float64, hi : Float64) : Interval
  Interval.new(lo, hi)
end

private def wraparound_new(lo : Float64, hi : Float64) : WraparoundInterval
  WraparoundInterval.new(lo, hi)
end

private def test_empty(empty : T, full : T, new_interval : Proc(Float64, Float64, T)) : Nil forall T
  empty.should eq(empty)

  empty.intersects_value(0.0).should be_false
  empty.intersects_value(Float64::INFINITY).should be_false
  empty.intersects_value(-Float64::INFINITY).should be_false
  empty.intersects_value(Float64::NAN).should be_false

  empty.intersects_interval(new_interval.call(-10.0, 10.0)).should be_false
  empty.intersects_interval(empty).should be_false
  empty.intersects_interval(full).should be_true

  empty.contains_interval(new_interval.call(-10.0, 10.0)).should be_false
  empty.contains_interval(full).should be_false
  empty.contains_interval(empty).should be_true

  empty.merge_value(Float64::NAN).should eq(empty)
  empty.merge_interval(empty).should eq(empty)
  empty.merge_value(12.0).should eq(new_interval.call(12.0, 12.0))
  empty.merge_interval(new_interval.call(10.0, 20.0)).should eq(new_interval.call(10.0, 20.0))

  empty.expand_by(5.0).should eq(empty)
  empty.expand_by(0.0).should eq(empty)
  empty.expand_by(-1.0).should eq(empty)
  empty.expand_by(Float64::NAN).should eq(empty)
end

# ameba:disable Metrics/CyclomaticComplexity
private def test_finite(finite : T, empty : T, full : T, new_interval : Proc(Float64, Float64, T)) : Nil forall T
  finite.lo.should eq(10.0)
  finite.hi.should eq(20.0)
  finite.mid.should eq(15.0)
  finite.width.should eq(10.0)
  finite.wraparound?.should be_false
  finite.empty?.should be_false

  finite.intersects_value(10.0).should be_true
  finite.intersects_value(15.0).should be_true
  finite.intersects_value(20.0).should be_true

  finite.intersects_value(0.0).should be_false
  finite.intersects_value(Float64::INFINITY).should be_false
  finite.intersects_value(-Float64::INFINITY).should be_false
  finite.intersects_value(Float64::NAN).should be_false

  finite.intersects_interval(new_interval.call(14.0, 16.0)).should be_true
  finite.intersects_interval(new_interval.call(5.0, 15.0)).should be_true
  finite.intersects_interval(new_interval.call(15.0, 25.0)).should be_true
  finite.intersects_interval(new_interval.call(5.0, 25.0)).should be_true
  finite.intersects_interval(full).should be_true

  finite.intersects_interval(new_interval.call(5.0, 10.0)).should be_true
  finite.intersects_interval(new_interval.call(20.0, 25.0)).should be_true

  finite.intersects_interval(new_interval.call(0.0, 5.0)).should be_false
  finite.intersects_interval(new_interval.call(25.0, 30.0)).should be_false
  finite.intersects_interval(empty).should be_false

  finite.contains_interval(new_interval.call(14.0, 16.0)).should be_true
  finite.contains_interval(new_interval.call(10.0, 15.0)).should be_true
  finite.contains_interval(new_interval.call(15.0, 20.0)).should be_true
  finite.contains_interval(new_interval.call(10.0, 20.0)).should be_true
  finite.contains_interval(empty).should be_true

  finite.contains_interval(new_interval.call(5.0, 15.0)).should be_false
  finite.contains_interval(new_interval.call(15.0, 25.0)).should be_false
  finite.contains_interval(new_interval.call(5.0, 25.0)).should be_false
  finite.contains_interval(new_interval.call(0.0, 5.0)).should be_false
  finite.contains_interval(new_interval.call(25.0, 30.0)).should be_false
  finite.contains_interval(full).should be_false

  finite.merge_value(Float64::NAN).should eq(finite)
  finite.merge_value(Float64::INFINITY).should eq(new_interval.call(finite.lo, Float64::INFINITY))
  finite.merge_value(-Float64::INFINITY).should eq(new_interval.call(-Float64::INFINITY, finite.hi))
  finite.merge_value(15.0).should eq(finite)
  finite.merge_value(25.0).should eq(new_interval.call(10.0, 25.0))
  finite.merge_value(5.0).should eq(new_interval.call(5.0, 20.0))

  finite.merge_interval(empty).should eq(finite)
  finite.merge_interval(finite).should eq(finite)
  finite.merge_interval(full).should eq(full)
  finite.merge_interval(new_interval.call(14.0, 16.0)).should eq(finite)
  finite.merge_interval(new_interval.call(5.0, 15.0)).should eq(new_interval.call(5.0, 20.0))
  finite.merge_interval(new_interval.call(15.0, 25.0)).should eq(new_interval.call(10.0, 25.0))
  finite.merge_interval(new_interval.call(0.0, 5.0)).should eq(new_interval.call(0.0, 20.0))
  finite.merge_interval(new_interval.call(25.0, 30.0)).should eq(new_interval.call(10.0, 30.0))

  finite.expand_by(2.0).should eq(new_interval.call(8.0, 22.0))
  finite.expand_by(5.0).should eq(new_interval.call(5.0, 25.0))
  finite.expand_by(0.0).should eq(finite)
  finite.expand_by(-1.0).should eq(finite)
  finite.expand_by(Float64::NAN).should eq(finite)
end

describe ParquetGeospatial::Interval do
  # Upstream `interval_empty`.
  it "handles the empty interval" do
    test_empty(Interval.empty, Interval.full, ->interval_new(Float64, Float64))
  end

  # Upstream `interval_finite`.
  it "handles a finite interval" do
    test_finite(Interval.new(10.0, 20.0), Interval.empty, Interval.full, ->interval_new(Float64, Float64))
  end
end

describe ParquetGeospatial::WraparoundInterval do
  # Upstream `wraparound_interval_empty`.
  it "handles the empty interval" do
    empty = WraparoundInterval.empty
    test_empty(empty, WraparoundInterval.full, ->wraparound_new(Float64, Float64))

    empty.wraparound?.should be_false
    empty.merge_interval(WraparoundInterval.new(20.0, 10.0)).should eq(WraparoundInterval.new(20.0, 10.0))
  end

  # Upstream `wraparound_interval_finite`.
  it "handles a finite interval" do
    finite = WraparoundInterval.new(10.0, 20.0)
    test_finite(finite, WraparoundInterval.empty, WraparoundInterval.full, ->wraparound_new(Float64, Float64))
    Interval.from_wraparound(finite).should eq(Interval.new(10.0, 20.0))
  end

  # Upstream `wraparound_interval_actually_wraparound_accessors`.
  it "exposes wraparound accessors" do
    wraparound = WraparoundInterval.new(20.0, 10.0)
    wraparound.wraparound?.should be_true
    wraparound.empty?.should be_false
    wraparound.mid.should eq(Float64::INFINITY)
  end

  # Upstream `wraparound_interval_actually_wraparound_intersects_value`.
  it "checks value intersection" do
    wraparound = WraparoundInterval.new(20.0, 10.0)
    wraparound.intersects_value(10.0).should be_true
    wraparound.intersects_value(20.0).should be_true
    wraparound.intersects_value(15.0).should be_false
    wraparound.intersects_value(Float64::INFINITY).should be_true
    wraparound.intersects_value(-Float64::INFINITY).should be_true
    wraparound.intersects_value(Float64::NAN).should be_false
  end

  # Upstream `wraparound_interval_actually_wraparound_intersects_interval`.
  it "checks interval intersection" do
    wraparound = WraparoundInterval.new(20.0, 10.0)
    wraparound.intersects_interval(wraparound).should be_true
    wraparound.intersects_interval(WraparoundInterval.full).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(14.0, 16.0)).should be_false
    wraparound.intersects_interval(WraparoundInterval.new(16.0, 14.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(5.0, 15.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(15.0, 5.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(15.0, 25.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(25.0, 15.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(5.0, 10.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(10.0, 5.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(20.0, 25.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(25.0, 20.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(0.0, 5.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(5.0, 0.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(25.0, 30.0)).should be_true
    wraparound.intersects_interval(WraparoundInterval.new(30.0, 25.0)).should be_true
  end

  # Upstream `wraparound_interval_actually_wraparound_contains_interval`.
  it "checks interval containment" do
    wraparound = WraparoundInterval.new(20.0, 10.0)
    wraparound.contains_interval(wraparound).should be_true
    wraparound.contains_interval(WraparoundInterval.empty).should be_true
    wraparound.contains_interval(WraparoundInterval.full).should be_false
    wraparound.contains_interval(WraparoundInterval.new(14.0, 16.0)).should be_false
    wraparound.contains_interval(WraparoundInterval.new(22.0, 8.0)).should be_true
    wraparound.contains_interval(WraparoundInterval.new(18.0, 12.0)).should be_false
    wraparound.contains_interval(WraparoundInterval.new(0.0, 5.0)).should be_true
    wraparound.contains_interval(WraparoundInterval.new(-5.0, 10.0)).should be_true
    wraparound.contains_interval(WraparoundInterval.new(25.0, 30.0)).should be_false
    wraparound.contains_interval(WraparoundInterval.new(20.0, 25.0)).should be_false
    wraparound.contains_interval(WraparoundInterval.new(5.0, 15.0)).should be_false
    wraparound.contains_interval(WraparoundInterval.new(15.0, 25.0)).should be_false
    wraparound.contains_interval(WraparoundInterval.new(15.0, 5.0)).should be_false
    wraparound.contains_interval(WraparoundInterval.new(25.0, 15.0)).should be_false
  end

  # Upstream `wraparound_interval_actually_wraparound_merge_value`.
  it "merges values" do
    wraparound = WraparoundInterval.new(20.0, 10.0)
    wraparound.merge_value(Float64::NAN).should eq(wraparound)
    wraparound.merge_value(12.0).should eq(WraparoundInterval.new(20.0, 12.0))
    wraparound.merge_value(18.0).should eq(WraparoundInterval.new(18.0, 10.0))
    wraparound.merge_value(5.0).should eq(wraparound)
    wraparound.merge_value(10.0).should eq(wraparound)
    wraparound.merge_value(20.0).should eq(wraparound)
    wraparound.merge_value(25.0).should eq(wraparound)
  end

  # Upstream `wraparound_interval_actually_wraparound_merge_interval`.
  it "merges intervals" do
    wraparound = WraparoundInterval.new(20.0, 10.0)
    wraparound.merge_interval(WraparoundInterval.empty).should eq(wraparound)
    wraparound.merge_interval(wraparound).should eq(wraparound)
    wraparound.merge_interval(WraparoundInterval.new(16.0, 14.0)).should eq(WraparoundInterval.new(16.0, 14.0))
    wraparound.merge_interval(WraparoundInterval.new(25.0, 5.0)).should eq(wraparound)
    wraparound.merge_interval(WraparoundInterval.new(15.0, 5.0)).should eq(WraparoundInterval.new(15.0, 10.0))
    wraparound.merge_interval(WraparoundInterval.new(25.0, 15.0)).should eq(WraparoundInterval.new(20.0, 15.0))
    wraparound.merge_interval(WraparoundInterval.new(30.0, 25.0)).should eq(WraparoundInterval.full)
    wraparound.merge_interval(WraparoundInterval.new(5.0, 0.0)).should eq(WraparoundInterval.full)
    wraparound.merge_interval(WraparoundInterval.new(25.0, 30.0)).should eq(wraparound)
    wraparound.merge_interval(WraparoundInterval.new(0.0, 5.0)).should eq(wraparound)
    wraparound.merge_interval(WraparoundInterval.new(5.0, 15.0)).should eq(WraparoundInterval.new(20.0, 15.0))
    wraparound.merge_interval(WraparoundInterval.new(15.0, 25.0)).should eq(WraparoundInterval.new(15.0, 10.0))
    wraparound.merge_interval(WraparoundInterval.new(12.0, 15.0)).should eq(WraparoundInterval.new(20.0, 15.0))
    wraparound.merge_interval(WraparoundInterval.new(15.0, 18.0)).should eq(WraparoundInterval.new(15.0, 10.0))
  end

  # Upstream `wraparound_interval_actually_wraparound_expand_by`.
  it "expands wraparound intervals" do
    wraparound = WraparoundInterval.new(20.0, 10.0)
    wraparound.expand_by(2.0).should eq(WraparoundInterval.new(18.0, 12.0))
    wraparound.expand_by(4.0).should eq(WraparoundInterval.new(16.0, 14.0))
    wraparound.expand_by(5.0).should eq(WraparoundInterval.full)
    wraparound.expand_by(6.0).should eq(WraparoundInterval.full)
    wraparound.expand_by(100.0).should eq(WraparoundInterval.full)
    wraparound.expand_by(0.0).should eq(wraparound)
    wraparound.expand_by(-1.0).should eq(wraparound)
    wraparound.expand_by(Float64::NAN).should eq(wraparound)

    non_wraparound = WraparoundInterval.new(10.0, 20.0)
    non_wraparound.wraparound?.should be_false
    non_wraparound.expand_by(2.0).should eq(WraparoundInterval.new(8.0, 22.0))

    wraparound2 = WraparoundInterval.new(15.0, 5.0)
    wraparound2.expand_by(3.0).should eq(WraparoundInterval.new(12.0, 8.0))
    wraparound2.expand_by(5.0).should eq(WraparoundInterval.full)
  end

  # Upstream `wraparound_interval_actually_wraparound_convert`.
  it "rejects converting a wraparound interval" do
    wraparound = WraparoundInterval.new(20.0, 10.0)
    expect_raises(ParquetGeospatial::InvalidArgumentError, /Can't convert wraparound interval/) do
      Interval.from_wraparound(wraparound)
    end
  end
end
