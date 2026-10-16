require "../spec_helper"

# Ported from `parquet/src/bloom_filter/mod.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: `Sbbf`/`Block` internals are exposed (`blocks`/`words`,
# `hash_to_block_index`, `insert_hash`, `fold_n`) so the upstream
# white-box tests translate directly; free functions live on the
# `Parquet::BloomFilter` module.

private alias BloomFilter = Parquet::BloomFilter
private alias Sbbf = Parquet::BloomFilter::Sbbf
private alias Block = Parquet::BloomFilter::Block
private alias BFHeader = Parquet::BloomFilter::BloomFilterHeader
private alias BFAlgo = Parquet::BloomFilterAlgorithm
private alias BFHash = Parquet::BloomFilterHash
private alias BFCompression = Parquet::BloomFilterCompression

private def hash_bytes(bytes : Bytes) : UInt64
  BloomFilter.hash_as_bytes(bytes)
end

private def high_bytes(value : String) : UInt64
  hash_bytes(value.to_slice)
end

describe Parquet::BloomFilter do
  # Upstream `test_hash_bytes`.
  it "hashes empty bytes with xxHash64/seed 0" do
    hash_bytes(Bytes.empty).should eq(17241709254077376921_u64)
  end

  # Upstream `test_mask_set_quick_check`.
  it "produces power-of-two masks" do
    (0...1_000_000).each do |i|
      mask = Block.mask(i.to_u32!)
      mask.words.all? { |word| word != 0_u32 && (word & (word - 1)) == 0_u32 }.should be_true
    end
  end

  # Upstream `test_block_insert_and_check`.
  it "inserts and checks a single block" do
    (0...1_000_000).each do |i|
      block = Block::ZERO
      block.insert(i.to_u32!)
      block.check(i.to_u32!).should be_true
    end
  end

  # Upstream `test_sbbf_insert_and_check`.
  it "inserts and checks an SBBF" do
    sbbf = Sbbf.new(Array(Block).new(1000, Block::ZERO))
    (0...1_000_000).each do |i|
      sbbf.insert(i)
      sbbf.check(i).should be_true
    end
  end

  # Upstream `test_with_fixture`.
  it "matches a parquet-mr fixture" do
    bitset = Bytes[
      200_u8, 1, 80, 20, 64, 68, 8, 109, 6, 37, 4, 67, 144, 80, 96, 32, 8, 132, 43, 33, 0, 5,
      99, 65, 2, 0, 224, 44, 64, 78, 96, 4,
    ]
    sbbf = Sbbf.new(bitset)
    (0_i64...10).each do |a|
      sbbf.check("a#{a}").should be_true
    end
  end

  # Upstream `test_bloom_filter_header_size_assumption`.
  it "reads a bloom filter header and length" do
    buffer = Bytes[21_u8, 64, 28, 28, 0, 0, 28, 28, 0, 0, 28, 28, 0, 0, 0, 99]
    header, read_length = BloomFilter.read_bloom_filter_header_and_length(buffer)
    read_length.should eq(15)
    header.algorithm.should eq(BFAlgo::BLOCK)
    header.compression.should eq(BFCompression::UNCOMPRESSED)
    header.hash.should eq(BFHash::XXHASH)
    header.num_bytes.should eq(32)
    BloomFilter::SBBF_HEADER_SIZE_ESTIMATE.should eq(20)
  end

  # Upstream `test_optimal_num_of_bytes`.
  it "computes the optimal number of bytes" do
    [{0, 32}, {9, 32}, {31, 32}, {32, 32}, {33, 64}, {99, 128}, {1024, 1024}, {999_000_000, 128 * 1024 * 1024}].each do |(input, expected)|
      BloomFilter.optimal_num_of_bytes(input.to_i64).should eq(expected)
    end
  end

  # Upstream `test_num_of_bits_from_ndv_fpp`.
  it "computes the number of bits from ndv/fpp" do
    [
      {0.1, 10, 57_u64},
      {0.01, 10, 96_u64},
      {0.001, 10, 146_u64},
      {0.1, 100, 577_u64},
      {0.01, 100, 968_u64},
      {0.001, 100, 1460_u64},
      {0.1, 1000, 5772_u64},
      {0.01, 1000, 9681_u64},
      {0.001, 1000, 14607_u64},
      {0.1, 10000, 57725_u64},
      {0.01, 10000, 96815_u64},
      {0.001, 10000, 146076_u64},
      {0.1, 100000, 577254_u64},
      {0.01, 100000, 968152_u64},
      {0.001, 100000, 1460769_u64},
      {0.1, 1000000, 5772541_u64},
      {0.01, 1000000, 9681526_u64},
      {0.001, 1000000, 14607697_u64},
    ].each do |(fpp, ndv, num_bits)|
      BloomFilter.num_of_bits_from_ndv_fpp(ndv.to_u64, fpp).should eq(num_bits)
    end

    BloomFilter.num_of_bits_from_ndv_fpp(1_000_000_000_000_u64, 1e-50).should eq(14226231280773240832_u64)
  end

  # Upstream `test_fold_n_halves_block_count`.
  it "halves the block count per fold" do
    sbbf = Sbbf.new_with_num_of_bytes(1024)
    sbbf.num_blocks.should eq(32)
    sbbf.fold_n(1)
    sbbf.num_blocks.should eq(16)
    sbbf.fold_n(1)
    sbbf.num_blocks.should eq(8)
  end

  # Upstream `test_fold_preserves_inserted_values`.
  it "preserves inserted values after folding" do
    sbbf = Sbbf.new_with_num_of_bytes(32 * 1024)
    values = (0...1000).map { |i| "value_#{i}" }
    values.each { |v| sbbf.insert(v) }

    original_blocks = sbbf.num_blocks
    sbbf.fold_to_target_fpp(0.05)
    (sbbf.num_blocks < original_blocks).should be_true

    values.each { |v| sbbf.check(v).should be_true }
  end

  # Upstream `test_fold_to_target_fpp_stops_before_exceeding_target`.
  it "stops folding before exceeding the target fpp" do
    sbbf = Sbbf.new_with_num_of_bytes(64 * 1024)
    (0...5000).each { |i| sbbf.insert(i) }

    target_fpp = 0.01
    sbbf.fold_to_target_fpp(target_fpp)

    total_bits = (sbbf.num_blocks * 256).to_f64
    set_bits = sbbf.blocks.sum(&.count_ones).to_u64
    fill = set_bits.to_f64 / total_bits
    current_fpp = fill ** 8
    (current_fpp <= target_fpp).should be_true
  end

  # Upstream `test_fold_empty_filter_folds_to_minimum`.
  it "folds an empty filter to the minimum" do
    sbbf = Sbbf.new_with_num_of_bytes(1024)
    sbbf.fold_to_target_fpp(0.01)
    sbbf.num_blocks.should eq(1)
  end

  # Upstream `test_fold_n_panics_at_minimum_size`.
  it "rejects folding below the minimum size" do
    sbbf = Sbbf.new_with_num_of_bytes(32)
    expect_raises(Exception, "Cannot fold 1 times: need at least 2 blocks, have 1") do
      sbbf.fold_n(1)
    end
  end

  # Upstream `test_sbbf_write_round_trip`.
  it "round-trips through serialized bytes" do
    bitset_bytes = Bytes.new(32, 0_u8)
    original = Sbbf.new(bitset_bytes)

    test_values = ["hello", "world", "rust", "parquet", "bloom", "filter"]
    test_values.each { |value| original.insert(value) }

    io = IO::Memory.new
    original.write(io)
    output = io.to_slice

    protocol = Parquet::Thrift::ThriftSliceInputProtocol.new(output)
    header = BFHeader.read_thrift(protocol)
    header.num_bytes.should eq(32)
    header.algorithm.should eq(BFAlgo::BLOCK)
    header.hash.should eq(BFHash::XXHASH)
    header.compression.should eq(BFCompression::UNCOMPRESSED)

    reconstructed = Sbbf.from_bytes(output)
    test_values.each { |value| reconstructed.check(value).should be_true }
  end

  # Upstream `test_sbbf_folded_equals_fresh`.
  it "folds to the same bits as a fresh filter" do
    values = (0...5000).map { |i| "elem_#{i}" }
    hashes = values.map { |v| high_bytes(v) }

    [64, 256, 1024].each do |num_blocks|
      half = num_blocks // 2

      original = Sbbf.new_with_num_of_bytes(num_blocks * 32)
      original.num_blocks.should eq(num_blocks)
      hashes.each { |h| original.insert_hash(h) }

      hashes.each do |h|
        mask = Block.mask(low_u32(h))
        orig_idx = original.hash_to_block_index(h)
        (orig_idx < num_blocks).should be_true

        fresh_idx = Sbbf.new(Array(Block).new(half, Block::ZERO)).hash_to_block_index(h)
        (orig_idx // 2).should eq(fresh_idx)

        8.times do |w|
          (original.blocks[orig_idx].words[w] & mask.words[w]).should_not eq(0_u32)
        end
      end

      folded = original.clone
      folded.fold_n(1)
      folded.num_blocks.should eq(half)

      fresh = Sbbf.new_with_num_of_bytes(half * 32)
      hashes.each { |h| fresh.insert_hash(h) }

      half.times do |j|
        folded.blocks[j].words.should eq(fresh.blocks[j].words)
      end
    end
  end

  # Upstream `test_multi_step_fold`.
  it "folds repeatedly to the same bits as fresh filters" do
    values = (0...3000).map { |i| "x_#{i}" }

    filter = Sbbf.new_with_num_of_bytes(512 * 32)
    values.each { |v| filter.insert(v) }

    [256, 128, 64, 32, 16, 8, 4, 2, 1].each do |expected_blocks|
      filter.fold_n(1)
      filter.num_blocks.should eq(expected_blocks)

      fresh = Sbbf.new_with_num_of_bytes(expected_blocks * 32)
      values.each { |v| fresh.insert(v) }

      filter.blocks.size.times do |j|
        filter.blocks[j].words.should eq(fresh.blocks[j].words)
      end
    end
  end

  # Upstream `test_fold_size_vs_optimal_fixed_size`.
  it "folds to the optimal size" do
    [{1000, 0.05}, {1000, 0.01}, {5000, 0.05}, {5000, 0.01}, {10000, 0.05}].each do |(ndv, target_fpp)|
      values = (0...ndv).map { |i| "d_#{i}" }

      folded = Sbbf.new_with_num_of_bytes(128 * 1024)
      values.each { |v| folded.insert(v) }
      folded.fold_to_target_fpp(target_fpp)

      folded_bytes = folded.num_blocks * 32
      optimal = Sbbf.new_with_ndv_fpp(ndv.to_u64, target_fpp)
      optimal_bytes = optimal.num_blocks * 32

      ratio = folded_bytes.to_f64 / optimal_bytes.to_f64
      ratio.should eq(1.0)
    end
  end

  # Upstream `test_folded_fpp_matches_fresh_fpp`.
  it "folds to the same empirical fpp as a fresh filter" do
    ndv = 2000
    num_probes = 50_000
    inserted = (0...ndv).map { |i| "ins_#{i}" }
    probes = (0...num_probes).map { |i| "probe_#{i}" }

    folded = Sbbf.new_with_num_of_bytes(512 * 32)
    inserted.each { |v| folded.insert(v) }

    [256, 128, 64, 32, 16, 8, 4, 2, 1].each do |expected_blocks|
      folded.fold_n(1)
      folded.num_blocks.should eq(expected_blocks)

      fresh = Sbbf.new_with_num_of_bytes(expected_blocks * 32)
      inserted.each { |v| fresh.insert(v) }

      folded_fp = 0_u64
      fresh_fp = 0_u64
      probes.each do |p|
        folded_fp += 1 if folded.check(p)
        fresh_fp += 1 if fresh.check(p)
      end

      folded_fp.should eq(fresh_fp)
    end
  end
end

private def low_u32(value : UInt64) : UInt32
  (value & 0xFFFF_FFFF_u64).to_u32!
end
