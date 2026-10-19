require "../../spec_helper"

# Ported from `parquet/src/arrow/array_reader/cached_array_reader.rs` tests
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `CachedArrayReader` wraps an `ArrayReader` and serves its batches from a
# shared `RowGroupCache` (populated by the filter stage and consumed by the
# output stage of the push decoder), falling back to the inner reader on a
# cache miss.
private alias BatchID = Parquet::Arrow::BatchID
private alias CacheRole = Parquet::Arrow::CacheRole
private alias CachedArrayReader = Parquet::Arrow::CachedArrayReader
private alias Metrics = Parquet::Arrow::ArrowReaderMetrics
private alias RowGroupCache = Parquet::Arrow::RowGroupCache

# Upstream `MockArrayReader`: yields the values `1, 2, 3, ...` of a fixed data
# vector, one `read_records`/`consume_batch` pair at a time.
private class MockArrayReader < Parquet::Arrow::ArrayReader
  def initialize(@data : Array(Int32))
    @position = 0
    @records_to_consume = 0
  end

  def data_type : Arrow::DataType
    Arrow::DataType.int32
  end

  def read_records(batch_size : Int32) : Int32
    remaining = @data.size - @position
    to_read = Math.min(batch_size, remaining)
    @records_to_consume += to_read
    to_read
  end

  def consume_batch : Arrow::ArrayBase
    start = @position
    finish = start + @records_to_consume
    slice = @data[start...finish]
    @position = finish
    @records_to_consume = 0
    Arrow::PrimitiveArray(Int32).from_values(slice)
  end

  def skip_records(num_records : Int32) : Int32
    remaining = @data.size - @position
    to_skip = Math.min(num_records, remaining)
    @position += to_skip
    to_skip
  end
end

private def int32_values(array : Arrow::ArrayBase) : Array(Int32)
  array.as(Arrow::PrimitiveArray(Int32)).values.to_a
end

private def cached_reader(inner : MockArrayReader, cache : RowGroupCache,
                          column_idx : Int32, role : CacheRole,
                          metrics : Metrics = Metrics.disabled) : CachedArrayReader
  CachedArrayReader.new(inner, cache, column_idx, role, metrics)
end

describe Parquet::Arrow::CachedArrayReader do
  it "reads and consumes batched records through the cache (upstream test_cached_reader_basic)" do
    cache = RowGroupCache.new(3, Int32::MAX) # Batch size 3
    reader = cached_reader(MockArrayReader.new([1, 2, 3, 4, 5]), cache, 0, CacheRole::Producer)

    reader.read_records(3).should eq(3)

    array = reader.consume_batch
    array.len.should eq(3)
    int32_values(array).should eq([1, 2, 3])

    # Read 3 more records: only 2 are left.
    reader.read_records(3).should eq(2)
  end

  it "combines reads and skips (upstream test_read_skip_pattern)" do
    cache = RowGroupCache.new(5, Int32::MAX) # Batch size 5
    reader = cached_reader(
      MockArrayReader.new([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cache, 0, CacheRole::Consumer
    )

    reader.read_records(2).should eq(2)

    array1 = reader.consume_batch
    array1.len.should eq(2)
    int32_values(array1).should eq([1, 2])

    reader.skip_records(2).should eq(2)

    reader.read_records(1).should eq(1)

    # Consume it (should be the 5th element after skipping 3, 4)
    array2 = reader.consume_batch
    array2.len.should eq(1)
    int32_values(array2).should eq([5])
  end

  it "accumulates multiple reads into one batch (upstream test_multiple_reads_before_consume)" do
    cache = RowGroupCache.new(3, Int32::MAX) # Batch size 3
    reader = cached_reader(
      MockArrayReader.new([1, 2, 3, 4, 5, 6]), cache, 0, CacheRole::Consumer
    )

    reader.read_records(2).should eq(2)
    reader.read_records(1).should eq(1)

    array = reader.consume_batch
    array.len.should eq(3)
    int32_values(array).should eq([1, 2, 3])
  end

  it "stops at end of file (upstream test_eof_behavior)" do
    cache = RowGroupCache.new(5, Int32::MAX) # Batch size 5
    reader = cached_reader(MockArrayReader.new([1, 2, 3]), cache, 0, CacheRole::Consumer)

    # Try to read more than available
    reader.read_records(5).should eq(3)

    array1 = reader.consume_batch
    array1.len.should eq(3)

    # Further reads should return 0
    reader.read_records(1).should eq(0)

    array2 = reader.consume_batch
    array2.len.should eq(0)
  end

  it "keeps columns separate in a shared cache (upstream test_cache_sharing)" do
    cache = RowGroupCache.new(5, Int32::MAX) # Batch size 5

    # First reader - populate cache
    reader1 = cached_reader(MockArrayReader.new([1, 2, 3, 4, 5]), cache, 0, CacheRole::Producer)
    reader1.read_records(3).should eq(3)
    array1 = reader1.consume_batch
    array1.len.should eq(3)

    # Second reader with different column index should not interfere
    reader2 = cached_reader(
      MockArrayReader.new([10, 20, 30, 40, 50]), cache, 1, CacheRole::Consumer
    )
    reader2.read_records(2).should eq(2)
    array2 = reader2.consume_batch
    array2.len.should eq(2)

    # Verify the second reader got its own data, not from cache
    int32_values(array2).should eq([10, 20])
  end

  it "removes consumed batches for a consumer (upstream test_consumer_removes_batches)" do
    cache = RowGroupCache.new(3, Int32::MAX) # Batch size 3
    reader = cached_reader(
      MockArrayReader.new([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cache, 0, CacheRole::Consumer
    )

    # Read first batch (positions 0-2, batch 0)
    reader.read_records(3).should eq(3)
    reader.outer_position.should eq(3)
    cache.get(0, BatchID.new(0)).should_not be_nil

    reader.consume_batch.len.should eq(3)

    # After first consume_batch, batch 0 should still be in cache
    # (current_batch_id = 3/3 = 1, cleanup only happens if current_batch_id > 1)
    cache.get(0, BatchID.new(0)).should_not be_nil

    # Read second batch (positions 3-5, batch 1)
    reader.read_records(3).should eq(3)
    reader.outer_position.should eq(6)
    reader.consume_batch.len.should eq(3)

    # After second consume_batch, batch 0 should be removed
    # (current_batch_id = 6/3 = 2, cleanup removes batches 0..(2-1) = 0..1)
    cache.get(0, BatchID.new(0)).should be_nil
    cache.get(0, BatchID.new(1)).should_not be_nil

    # Read third batch (positions 6-8, batch 2)
    reader.read_records(3).should eq(3)
    reader.outer_position.should eq(9)
    reader.consume_batch.len.should eq(3)

    # After third consume_batch, batches 0 and 1 should be removed
    cache.get(0, BatchID.new(0)).should be_nil
    cache.get(0, BatchID.new(1)).should be_nil
    cache.get(0, BatchID.new(2)).should_not be_nil
  end

  it "keeps consumed batches for a producer (upstream test_producer_keeps_batches)" do
    cache = RowGroupCache.new(3, Int32::MAX) # Batch size 3
    reader = cached_reader(
      MockArrayReader.new([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), cache, 0, CacheRole::Producer
    )

    # Read first batch (positions 0-2)
    reader.read_records(3).should eq(3)
    reader.consume_batch.len.should eq(3)
    cache.get(0, BatchID.new(0)).should_not be_nil

    # Read second batch (positions 3-5) - producer should NOT remove batch 0
    reader.read_records(3).should eq(3)
    reader.consume_batch.len.should eq(3)

    # Verify both batch 0 and batch 1 are still present (no removal for producer)
    cache.get(0, BatchID.new(0)).should_not be_nil
    cache.get(0, BatchID.new(1)).should_not be_nil
  end

  it "serves from the local cache after shared eviction (upstream test_local_cache_protects_against_eviction)" do
    cache = RowGroupCache.new(3, Int32::MAX) # Batch size 3
    reader = cached_reader(
      MockArrayReader.new([1, 2, 3, 4, 5, 6]), cache, 0, CacheRole::Consumer
    )

    # Read records which should populate both shared and local cache
    reader.read_records(3).should eq(3)

    cache.get(0, BatchID.new(0)).should_not be_nil
    reader.local_cache.has_key?(BatchID.new(0)).should be_true

    # Simulate cache eviction by manually removing from shared cache
    cache.remove(0, BatchID.new(0)).should be_true
    cache.get(0, BatchID.new(0)).should be_nil

    # Even though shared cache was evicted, consume_batch should still work
    # because data is preserved in local cache
    array = reader.consume_batch
    array.len.should eq(3)
    int32_values(array).should eq([1, 2, 3])

    # Local cache should be cleared after consume_batch
    reader.local_cache.empty?.should be_true
  end

  it "keeps reading with the cache disabled (upstream test_local_cache_is_cleared_properly)" do
    cache = RowGroupCache.new(3, 0) # Batch size 3, cache 0
    reader = cached_reader(MockArrayReader.new([1, 2, 3, 4]), cache, 0, CacheRole::Consumer)

    reader.read_records(1).should eq(1)
    reader.consume_batch.len.should eq(1)

    reader.read_records(3).should eq(3)
    reader.consume_batch.len.should eq(3)
  end

  it "mixes cached and newly decoded batches (upstream test_batch_id_calculation_with_incremental_reads)" do
    cache = RowGroupCache.new(3, Int32::MAX) # Batch size 3

    # Create a producer to populate cache
    producer = cached_reader(
      MockArrayReader.new([1, 2, 3, 4, 5, 6, 7, 8, 9]), cache, 0, CacheRole::Producer
    )
    producer.read_records(3)
    producer.consume_batch

    # Now create a consumer that will try to read from cache
    consumer = cached_reader(
      MockArrayReader.new([1, 2, 3, 4, 5, 6, 7, 8, 9]), cache, 0, CacheRole::Consumer
    )

    # - We want to read 4 records starting from position 0
    # - First 3 records (positions 0-2) should come from cache (batch 0)
    # - The 4th record (position 3) should come from the next batch
    consumer.read_records(4).should eq(4)

    array = consumer.consume_batch
    array.len.should eq(4)
    int32_values(array).should eq([1, 2, 3, 4])
  end
end
