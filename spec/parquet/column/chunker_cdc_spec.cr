require "../../spec_helper"

# Ported from `parquet/src/column/chunker/cdc.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# These are the seven crate-internal unit tests. The upstream `arrow_tests`
# module (CDC through the arrow writer/reader roundtrip + `get_arrow_chunks`) is
# deferred with the arrow layer.
#
# Adaptations: `roll_fixed::<4>(&(i as i32).to_le_bytes())` becomes
# `roll_fixed(le_i32(i))` (Crystal has no const-generic array width; `roll` and
# `roll_fixed` share the same byte loop); `ContentDefinedChunker::calculate_mask`
# takes `Int64`/`Int32` explicitly; `LevelDataRef::Absent`/`::Materialized(&_)`
# become `LevelDataRef.absent`/`.materialized`; `|c, i|` receives the chunker as
# its first block argument, like the upstream `FnMut(&mut Self, usize)`.

private alias Chunker = Parquet::Column::Chunker::ContentDefinedChunker
private alias CdcOptions = Parquet::File::Properties::CdcOptions
private alias LevelDataRef = Parquet::Column::Writer::LevelDataRef
private alias PType = Parquet::Type

private def make_desc(max_def_level : Int16, max_rep_level : Int16) : Parquet::Schema::ColumnDescriptor
  tp = Parquet::Schema::Type.primitive_type_builder("col", PType::INT32).build
  Parquet::Schema::ColumnDescriptor.new(
    tp, max_def_level, max_rep_level, Parquet::Schema::ColumnPath.new([] of String)
  )
end

private def le_i32(value : Int32) : Bytes
  Parquet::FromBytes.to_le_slice(value)
end

private def le_i64(value : Int64) : Bytes
  Parquet::FromBytes.to_le_slice(value)
end

describe Parquet::Column::Chunker::ContentDefinedChunker do
  # Upstream `test_calculate_mask_defaults`.
  it "calculates the default rolling hash mask" do
    mask = Chunker.calculate_mask(256_i64 * 1024, 1024_i64 * 1024, 0)
    # avg = 640 KiB, target = (640-256)*1024/8 = 49152, log2(49152) = 15
    # mask = u64::MAX << (64 - 15) = top 15 bits set
    expected = UInt64::MAX << (64 - 15)
    mask.should eq(expected)
  end

  # Upstream `test_calculate_mask_with_norm_level`.
  it "calculates the mask with a normalization level" do
    mask = Chunker.calculate_mask(256_i64 * 1024, 1024_i64 * 1024, 1)
    expected = UInt64::MAX << (64 - 14)
    mask.should eq(expected)
  end

  # Upstream `test_calculate_mask_invalid`.
  it "rejects invalid mask parameters" do
    expect_raises(Parquet::GeneralError, "min_chunk_size must be non-negative") do
      Chunker.calculate_mask(-1_i64, 100_i64, 0)
    end
    expect_raises(Parquet::GeneralError, "max_chunk_size must be greater than min_chunk_size") do
      Chunker.calculate_mask(100_i64, 50_i64, 0)
    end
    expect_raises(Parquet::GeneralError, "max_chunk_size must be greater than min_chunk_size") do
      Chunker.calculate_mask(100_i64, 100_i64, 0)
    end
    # A norm_level that pushes the mask bits out of the valid range.
    expect_raises(Parquet::GeneralError, "must be between 1 and 63") do
      Chunker.calculate_mask(256_i64 * 1024, 1024_i64 * 1024, 100)
    end
  end

  # Upstream `test_non_nested_non_null_single_chunk`.
  it "produces one chunk for a small non-nested non-null batch" do
    options = CdcOptions.new(8, 1024, 0)
    chunker = Chunker.new(make_desc(0_i16, 0_i16), options)

    # Write a small amount of data — should produce exactly 1 chunk.
    num_values = 4
    chunks = chunker.calculate(
      LevelDataRef.absent,
      LevelDataRef.absent,
      num_values,
    ) do |c, i|
      c.roll_fixed(le_i32(i))
    end

    chunks.size.should eq(1)
    chunks[0].level_offset.should eq(0)
    chunks[0].value_offset.should eq(0)
    chunks[0].num_levels.should eq(4)
  end

  # Upstream `test_max_chunk_size_forces_boundary`.
  it "forces boundaries at max_chunk_size" do
    options = CdcOptions.new(256, 1024, 0)
    chunker = Chunker.new(make_desc(0_i16, 0_i16), options)

    # Write enough data to exceed max_chunk_size multiple times.
    # Each i32 = 4 bytes, max_chunk_size=1024, so ~256 values per chunk max.
    num_values = 2000
    chunks = chunker.calculate(
      LevelDataRef.absent,
      LevelDataRef.absent,
      num_values,
    ) do |c, i|
      c.roll_fixed(le_i32(i))
    end

    # Should have multiple chunks
    chunks.size.should be > 1

    # Verify contiguity
    total_levels = 0
    chunks.each_with_index do |chunk, i|
      chunk.level_offset.should eq(total_levels)
      chunk.num_levels.should be > 0 if i < chunks.size - 1
      total_levels += chunk.num_levels
    end
    total_levels.should eq(num_values)
  end

  # Upstream `test_deterministic_chunks`.
  it "computes deterministic chunks" do
    options = CdcOptions.new(4, 64, 0)

    roll = ->(c : Chunker, i : Int32) { c.roll_fixed(le_i64(i.to_i64)) }

    chunker1 = Chunker.new(make_desc(0_i16, 0_i16), options)
    chunks1 = chunker1.calculate(LevelDataRef.absent, LevelDataRef.absent, 200, &roll)

    chunker2 = Chunker.new(make_desc(0_i16, 0_i16), options)
    chunks2 = chunker2.calculate(LevelDataRef.absent, LevelDataRef.absent, 200, &roll)

    chunks1.size.should eq(chunks2.size)
    chunks1.each_with_index do |a, i|
      b = chunks2[i]
      a.level_offset.should eq(b.level_offset)
      a.num_levels.should eq(b.num_levels)
      a.value_offset.should eq(b.value_offset)
      a.num_values.should eq(b.num_values)
    end
  end

  # Upstream `test_nullable_non_nested`.
  it "tracks levels and non-null values separately for nullable columns" do
    options = CdcOptions.new(4, 64, 0)
    chunker = Chunker.new(make_desc(1_i16, 0_i16), options)

    num_levels = 20
    # def_level=1 means non-null, def_level=0 means null
    # Pattern: null at indices 0, 3, 6, 9, 12, 15, 18 → 7 nulls, 13 non-null
    def_levels = Array(Int16).new(num_levels) { |i| (i % 3 != 0) ? 1_i16 : 0_i16 }
    expected_non_null = def_levels.count(1_i16)

    chunks = chunker.calculate(
      LevelDataRef.materialized(def_levels),
      LevelDataRef.absent,
      num_levels,
    ) do |c, i|
      c.roll_fixed(le_i32(i))
    end

    chunks.size.should be > 0
    total_levels = chunks.sum(&.num_levels)
    total_values = chunks.sum(&.num_values)
    total_levels.should eq(num_levels)
    total_values.should eq(expected_non_null)
    # With nulls present, total_values < total_levels
    total_values.should be < total_levels
  end

  # Characterization of the nested path, which upstream only covers through the
  # arrow-gated `test_cdc_list_roundtrip` family. The data is the example from the
  # `calculate` doc comment: `List<Int32?>` with `repeated_ancestor_def_level=2`
  # and `max_def=3`, rows `[1, null, 2]` and `[3, 4]`.
  #
  # Boundaries are only allowed at `rep_level == 0`, and `roll_value` must be fed
  # the *leaf* offset (which includes null slots) rather than the non-null value
  # offset. `max_chunk_size=32` is reached at the last level, which is the only
  # boundary source here (the CLT path needs 8 consecutive mask matches).
  it "chunks nested data only at top-level record boundaries" do
    options = CdcOptions.new(0, 32, 0)
    descr = Parquet::Schema::ColumnDescriptor.new_with_repeated_ancestor(
      Parquet::Schema::Type.primitive_type_builder("col", PType::INT32).build,
      3_i16,
      1_i16,
      Parquet::Schema::ColumnPath.new([] of String),
      2_i16,
    )
    chunker = Chunker.new(descr, options)

    #   leaf array:    [1, null, 2, 3, 4]
    #   def_levels:    [3,  2,   3, 3, 3]
    #   rep_levels:    [0,  1,   1, 0, 0]
    def_levels = [3_i16, 2_i16, 3_i16, 3_i16, 3_i16]
    rep_levels = [0_i16, 1_i16, 1_i16, 0_i16, 0_i16]
    rolled = [] of Int32

    chunks = chunker.calculate(
      LevelDataRef.materialized(def_levels),
      LevelDataRef.materialized(rep_levels),
      5,
    ) do |c, leaf_offset|
      rolled << leaf_offset
      c.roll_fixed(le_i32(leaf_offset))
    end

    # The null element is not hashed as a value, and hashing uses the leaf offset
    # (0, 2, 3, 4), not the non-null value offset (0, 1, 2, 3).
    rolled.should eq([0, 2, 3, 4])

    # One boundary forced by max_chunk_size at the top-level record starting at
    # level 4; the chunk before it holds 4 levels / 3 values.
    chunks.size.should eq(2)
    chunks[0].level_offset.should eq(0)
    chunks[0].num_levels.should eq(4)
    chunks[0].value_offset.should eq(0)
    chunks[0].num_values.should eq(3)
    chunks[1].level_offset.should eq(4)
    chunks[1].num_levels.should eq(1)
    chunks[1].value_offset.should eq(3)
    chunks[1].num_values.should eq(1)
    chunks.sum(&.num_levels).should eq(5)
    chunks.sum(&.num_values).should eq(4)
  end
end
