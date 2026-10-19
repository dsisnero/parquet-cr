require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/row_group_cache.rs` tests
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `RowGroupCache` stores decoded arrow arrays at batch granularity so the push
# decoder's filter stage and output stage share the decoded data.
private alias RowGroupCache = Parquet::Arrow::RowGroupCache
private alias BatchID = Parquet::Arrow::BatchID

private def i32_array(values : Array(Int32)) : Arrow::ArrayBase
  Arrow::PrimitiveArray(Int32).from_values(values)
end

describe Parquet::Arrow::RowGroupCache do
  it "inserts, misses and looks up by column and batch (upstream test_cache_basic_operations)" do
    cache = RowGroupCache.new(1000, Int32::MAX)
    array = i32_array([1, 2, 3, 4, 5])

    # Test insert and get
    batch_id = BatchID.new(0)
    cache.insert(0, batch_id, array).should be_true
    retrieved = cache.get(0, batch_id)
    retrieved.should_not be_nil
    retrieved.not_nil!.len.should eq(5)

    # Test miss
    cache.get(1, batch_id).should be_nil

    # Test different row_id
    cache.get(0, BatchID.new(1000)).should be_nil
  end

  it "removes entries and keeps the others (upstream test_cache_remove)" do
    cache = RowGroupCache.new(1000, Int32::MAX)
    array1 = i32_array([1, 2, 3])
    array2 = i32_array([4, 5, 6])

    # Insert arrays
    cache.insert(0, BatchID.new(0), array1).should be_true
    cache.insert(0, BatchID.new(1000), array2).should be_true
    cache.insert(1, BatchID.new(0), array1).should be_true

    # Verify they're there
    cache.get(0, BatchID.new(0)).should_not be_nil
    cache.get(0, BatchID.new(1000)).should_not be_nil
    cache.get(1, BatchID.new(0)).should_not be_nil

    # Remove one entry
    cache.remove(0, BatchID.new(0)).should be_true
    cache.get(0, BatchID.new(0)).should be_nil

    # Other entries should still be there
    cache.get(0, BatchID.new(1000)).should_not be_nil
    cache.get(1, BatchID.new(0)).should_not be_nil

    # Try to remove non-existent entry
    cache.remove(0, BatchID.new(0)).should be_false

    # Remove remaining entries
    cache.remove(0, BatchID.new(1000)).should be_true
    cache.remove(1, BatchID.new(0)).should be_true

    # Cache should be empty
    cache.get(0, BatchID.new(1000)).should be_nil
    cache.get(1, BatchID.new(0)).should be_nil
  end
end
