require "../../spec_helper"

# Ported from `parquet/src/encodings/levels.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
private def reference_encode(max_level : Int16, values : Array(Int16)) : Bytes
  enc = LevelEncoder.v2_streaming(max_level)
  enc.put_with_observer(values) { |_, _| }
  enc.consume
end

describe Parquet::Encodings::LevelEncoder do
  # Upstream `test_put_n_with_observer_large_run`.
  it "bulk-extends a large uniform run" do
    max_level = 3_i16
    count = 10_000
    enc = LevelEncoder.v2_streaming(max_level)
    enc.put_n_with_observer(2_i16, count) { |_, _| }
    enc.consume.should eq(reference_encode(max_level, Array(Int16).new(count, 2_i16)))
  end

  # Upstream `test_put_n_with_observer_small_count`.
  it "handles a count below the RLE warmup" do
    max_level = 3_i16
    enc = LevelEncoder.v2_streaming(max_level)
    enc.put_n_with_observer(1_i16, 5) { |_, _| }
    enc.consume.should eq(reference_encode(max_level, Array(Int16).new(5, 1_i16)))
  end

  # Upstream `test_put_n_with_observer_exact_threshold`.
  it "handles exactly the warmup threshold" do
    max_level = 3_i16
    enc = LevelEncoder.v2_streaming(max_level)
    enc.put_n_with_observer(3_i16, 8) { |_, _| }
    enc.consume.should eq(reference_encode(max_level, Array(Int16).new(8, 3_i16)))
  end

  # Upstream `test_put_n_with_observer_single_value`.
  it "encodes a single value" do
    max_level = 1_i16
    enc = LevelEncoder.v2_streaming(max_level)
    enc.put_n_with_observer(1_i16, 1) { |_, _| }
    enc.consume.should eq(reference_encode(max_level, [1_i16]))
  end

  # Upstream `test_put_n_with_observer_zero_count`.
  it "encodes zero values" do
    max_level = 3_i16
    enc = LevelEncoder.v2_streaming(max_level)
    enc.put_n_with_observer(2_i16, 0) { |_, _| }
    enc.consume.should eq(reference_encode(max_level, [] of Int16))
  end

  # Upstream `test_put_n_with_observer_calls_observer_exactly_once`.
  it "calls the observer exactly once for put_n" do
    enc = LevelEncoder.v2_streaming(3_i16)
    calls = [] of Tuple(Int16, Int32)
    enc.put_n_with_observer(2_i16, 500) { |val, cnt| calls << {val, cnt} }
    calls.should eq([{2_i16, 500}])
  end

  # Upstream `test_put_n_with_observer_zero_count_calls_observer`.
  it "calls the observer once for a zero-count put_n" do
    enc = LevelEncoder.v2_streaming(3_i16)
    calls = [] of Tuple(Int16, Int32)
    enc.put_n_with_observer(1_i16, 0) { |val, cnt| calls << {val, cnt} }
    calls.should eq([{1_i16, 0}])
  end

  # Upstream `test_put_n_with_observer_followed_by_different_value`.
  it "transitions between runs" do
    max_level = 3_i16
    enc = LevelEncoder.v2_streaming(max_level)
    calls = [] of Tuple(Int16, Int32)
    enc.put_n_with_observer(1_i16, 100) { |v, c| calls << {v, c} }
    enc.put_n_with_observer(3_i16, 200) { |v, c| calls << {v, c} }
    calls.should eq([{1_i16, 100}, {3_i16, 200}])

    input = Array(Int16).new(100, 1_i16) + Array(Int16).new(200, 3_i16)
    enc.consume.should eq(reference_encode(max_level, input))
  end

  # Upstream `test_put_n_with_observer_interleaved_with_put_with_observer`.
  it "composes put_n and put" do
    max_level = 3_i16
    enc = LevelEncoder.v2_streaming(max_level)
    enc.put_n_with_observer(2_i16, 50) { |_, _| }
    enc.put_with_observer([0_i16, 0_i16, 1_i16, 1_i16, 3_i16]) { |_, _| }
    enc.put_n_with_observer(2_i16, 50) { |_, _| }

    input = Array(Int16).new(50, 2_i16) + [0_i16, 0_i16, 1_i16, 1_i16, 3_i16] + Array(Int16).new(50, 2_i16)
    enc.consume.should eq(reference_encode(max_level, input))
  end

  # Upstream `test_put_n_with_observer_v1_roundtrip`.
  it "round-trips through the v1 (length-prefixed) encoder" do
    max_level = 3_i16
    enc = LevelEncoder.v1_streaming(max_level)
    enc.put_n_with_observer(2_i16, 1000) { |_, _| }

    ref_enc = LevelEncoder.v1_streaming(max_level)
    ref_enc.put_with_observer(Array(Int16).new(1000, 2_i16)) { |_, _| }
    enc.consume.should eq(ref_enc.consume)
  end
end
