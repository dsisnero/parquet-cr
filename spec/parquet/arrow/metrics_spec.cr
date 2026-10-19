require "../../spec_helper"

# Ported from `parquet/src/arrow/arrow_reader/metrics.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b):
# `ArrowReaderMetrics` (disabled/enabled, shared counters) and the builder's
# `with_metrics`.
#
# Upstream's metric tests live in `parquet/tests/arrow_reader/predicate_cache.rs`,
# which needs the async reader and the predicate cache (`CachedArrayReader`).
# Both are deferred here, and upstream's synchronous reader never increments the
# counters either (only the cache wrapper does; see arrow-rs#8000), so the
# wiring tests characterize the API shape, the shared counters and the fact
# that a synchronous read leaves them at zero.
#
# Divergences: `usize` counters are `Int32` (atomically updated, as upstream),
# `ArrowReaderMetrics` is a class holding an optional inner class (upstream
# `Disabled`/`Enabled(Arc<..>)` enum), copying it with `dup` shares the inner
# counters (like cloning the upstream `Arc`), and the builder exposes `metrics`
# (upstream `pub(crate)`).
private alias PArrow = Parquet::Arrow
private alias BytesCR = Parquet::File::BytesChunkReader
private alias ArrowReaderMetrics = Parquet::Arrow::ArrowReaderMetrics

private def serialized_reader(name : String) : Parquet::File::SerializedFileReader
  Parquet::File::SerializedFileReader.new(BytesCR.new(ParquetTestHelpers.fixture_bytes(name)))
end

private def id_reader : PArrow::ParquetRecordBatchReaderBuilder
  reader_file = serialized_reader("alltypes_plain.parquet")
  mask = PArrow::ProjectionMask.leaves(reader_file.metadata.file_metadata.schema_descr, [0])
  PArrow::ParquetRecordBatchReaderBuilder.try_new(reader_file).with_projection(mask)
end

describe Parquet::Arrow::ArrowReaderMetrics do
  it "collects nothing when disabled (upstream ArrowReaderMetrics::disabled)" do
    metrics = ArrowReaderMetrics.disabled
    metrics.disabled?.should be_true
    metrics.enabled?.should be_false
    metrics.records_read_from_inner.should be_nil
    metrics.records_read_from_cache.should be_nil

    # Increments are ignored while disabled.
    metrics.increment_inner_reads(10)
    metrics.increment_cache_reads(10)
    metrics.records_read_from_inner.should be_nil
    metrics.records_read_from_cache.should be_nil
  end

  it "collects records read from the inner reader and the cache (upstream ArrowReaderMetrics::enabled)" do
    metrics = ArrowReaderMetrics.enabled
    metrics.enabled?.should be_true
    metrics.disabled?.should be_false
    metrics.records_read_from_inner.should eq(0)
    metrics.records_read_from_cache.should eq(0)

    metrics.increment_inner_reads(3)
    metrics.increment_inner_reads(4)
    metrics.records_read_from_inner.should eq(7)

    metrics.increment_cache_reads(5)
    metrics.records_read_from_cache.should eq(5)
    metrics.records_read_from_inner.should eq(7)
  end

  it "shares its counters across copies (upstream Arc clone)" do
    metrics = ArrowReaderMetrics.enabled
    copy = metrics.dup

    metrics.increment_inner_reads(2)
    copy.records_read_from_inner.should eq(2)

    copy.increment_cache_reads(1)
    metrics.records_read_from_cache.should eq(1)

    # A disabled copy stays disabled, and increments are no-ops.
    disabled = ArrowReaderMetrics.disabled.dup
    disabled.increment_inner_reads(4)
    disabled.records_read_from_inner.should be_nil
  end
end

describe "ArrowReaderMetrics wiring" do
  it "is disabled by default and can be replaced on the builder (upstream with_metrics)" do
    builder = id_reader
    builder.metrics.disabled?.should be_true

    metrics = ArrowReaderMetrics.enabled
    builder = builder.with_metrics(metrics)
    builder.metrics.should be(metrics)

    # The synchronous reader has no predicate cache (arrow-rs#8000), so a plain
    # read leaves both counters at zero, as upstream.
    batches = builder.build.to_a
    batches.map(&.num_rows).should eq([8])
    metrics.records_read_from_inner.should eq(0)
    metrics.records_read_from_cache.should eq(0)
  end

  it "accepts metrics when building array readers" do
    reader_file = serialized_reader("alltypes_plain.parquet")
    metadata = PArrow::ArrowReaderMetadata.load(reader_file)
    metrics = ArrowReaderMetrics.enabled

    projection = PArrow::ProjectionMask.leaves(metadata.metadata.file_metadata.schema_descr, [0])
    array_reader = PArrow::ArrayReaderBuilder.new(
      PArrow::FileRowGroups.new(reader_file), 1024, metrics
    ).build_array_reader(metadata.fields, projection)

    array_reader.data_type.kind.struct?.should be_true
    array_reader.read_records(8).should eq(8)
    metrics.records_read_from_inner.should eq(0)
    metrics.records_read_from_cache.should eq(0)
  end
end
