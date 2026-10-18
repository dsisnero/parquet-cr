require "../../spec_helper"

# Ported from `parquet/src/arrow/record_reader/definition_levels.rs`
# (`DefinitionLevelBuffer`, `build_filtered_validity_bitmap`,
# `DefinitionLevelBufferDecoder`, `PackedDecoder` + its test module)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
private alias RR = Parquet::Arrow::RecordReader
private alias PE = Parquet::Encoding
private alias Builder = Arrow::BooleanBufferBuilder
private alias DefinitionLevelDecoderImpl = Parquet::Column::Reader::DefinitionLevelDecoderImpl

private def mask_bits(bytes : Bytes, size : Int32) : Array(Bool)
  Array(Bool).new(size) { |i| Parquet::Util::BitUtil.get_bit(bytes, i) }
end

describe Parquet::Arrow::RecordReader do
  # Upstream `test_build_validity_bitmap_unfiltered_word_chunk`: 65 levels so the
  # unfiltered path crosses u64 boundaries (scalar in this port).
  it "builds an unfiltered validity bitmap" do
    def_levels = Array(Int16).new(65) { |i| i % 3 == 0 ? 2_i16 : 1_i16 }
    bitmap = Builder.new(0)

    RR.build_filtered_validity_bitmap(def_levels, nil, nil, 2_i16, bitmap).should eq(65)
    values = bitmap.finish

    def_levels.each_with_index do |level, idx|
      values.value(idx).should eq(level >= 2)
    end
  end

  it "builds a filtered validity bitmap" do
    def_levels = [3_i16, 2_i16, 0_i16, 2_i16, 3_i16]
    rep_levels = [0_i16, 1_i16, 0_i16, 0_i16, 1_i16]
    bitmap = Builder.new(0)

    count = RR.build_filtered_validity_bitmap(
      def_levels, {rep_levels, 1_i16}, 2_i16, 3_i16, bitmap
    )

    # Items with def >= 2 and rep <= 1: indices 0, 1, 3, 4; values at def == 3.
    count.should eq(4)
    mask_bits(bitmap.as_slice, 4).should eq([true, false, false, true])
  end

  # Upstream `test_packed_decoder`.
  it "decodes RLE and bit-packed level data" do
    random = Random.new(7)
    len = 512 + random.rand(512)

    encoder = Parquet::Encodings::RleEncoder.new(1, 1024)
    expected = Array(Bool).new(len) { random.next_bool }
    expected.each { |value| encoder.put(value ? 1_u64 : 0_u64) }
    encoded = encoder.consume

    decoder = RR::PackedDecoder.new
    decoder.set_data(PE::RLE, encoded)

    decoded = Builder.new(len)
    remaining = len
    while remaining > 0
      to_read = 1 + random.rand(remaining)
      decoder.read(decoded, to_read).should eq(to_read)
      remaining -= to_read
    end
    decoded.len.should eq(len)
    mask_bits(decoded.as_slice, len).should eq(expected)
  end

  # Upstream `test_packed_decoder_skip`.
  it "skips RLE and bit-packed level data" do
    random = Random.new(11)
    len = 512 + random.rand(512)

    encoder = Parquet::Encodings::RleEncoder.new(1, 1024)
    expected = Array(Bool).new(len) { random.next_bool }
    expected.each { |value| encoder.put(value ? 1_u64 : 0_u64) }
    total_values = expected.count(true)
    encoded = encoder.consume

    decoder = RR::PackedDecoder.new
    decoder.set_data(PE::RLE, encoded)

    skip_values = 0
    read_values = 0
    skip_levels = 0
    read_levels = 0

    loop do
      offset = skip_levels + read_levels
      remaining_levels = len - offset
      break if remaining_levels <= 0

      to_read_or_skip = 1 + random.rand(remaining_levels)
      if random.next_bool
        skipped_values, skipped_levels = decoder.skip(to_read_or_skip)
        skip_values += skipped_values
        skip_levels += skipped_levels
      else
        decoded = Builder.new(to_read_or_skip)
        levels_read = decoder.read(decoded, to_read_or_skip)
        read_levels += levels_read
        bits = mask_bits(decoded.as_slice, levels_read)
        read_values += bits.count(true)
        bits.should eq(expected[offset, levels_read])
      end
    end

    (read_levels + skip_levels).should eq(len)
    (read_values + skip_values).should eq(total_values)
  end

  # Upstream `test_try_consume_all_valid`.
  it "fast-paths all-valid RLE runs" do
    encode = ->(bools : Array(Bool)) do
      encoder = Parquet::Encodings::RleEncoder.new(1, 1024)
      bools.each { |value| encoder.put(value ? 1_u64 : 0_u64) }
      encoder.consume
    end

    len = 100
    decoder = RR::PackedDecoder.new
    decoder.set_data(PE::RLE, encode.call(Array(Bool).new(len, true)))
    decoder.try_consume_all_valid(len).should eq(len)

    decoder.set_data(PE::RLE, encode.call(Array(Bool).new(len, false)))
    decoder.try_consume_all_valid(len).should be_nil

    run = Array(Bool).new(10, true) + Array(Bool).new(10, false)
    decoder.set_data(PE::RLE, encode.call(run))
    # Requesting more than the current valid run does not look ahead.
    decoder.try_consume_all_valid(20).should be_nil

    decoder.set_data(PE::RLE, encode.call(run))
    decoder.try_consume_all_valid(5).should eq(5)
    decoder.try_consume_all_valid(5).should eq(5)
    # The valid run is exhausted; the next call parses the null run.
    decoder.try_consume_all_valid(5).should be_nil
  end
end

describe Parquet::Arrow::RecordReader::DefinitionLevelBuffer do
  it "accumulates definition levels and nulls in full mode" do
    descriptor = Parquet::Schema::SchemaDescriptor.new(Parquet::Schema.parse_message_type(%(
      message test_schema {
        OPTIONAL GROUP my_list (LIST) {
          REPEATED GROUP list {
            OPTIONAL INT32 element;
          }
        }
      }
    ))).column(0)

    buffer = RR::DefinitionLevelBuffer.new(descriptor, null_mask_only: false)
    decoder = RR::DefinitionLevelBufferDecoder.new(descriptor.max_def_level, packed: false)

    levels = [3_i16, 2_i16, 0_i16, 3_i16]
    encoder = Parquet::Encodings::RleEncoder.new(2, 1024)
    levels.each { |level| encoder.put(level.to_u64) }
    decoder.set_data(PE::RLE, encoder.consume)

    values_read, levels_read = decoder.read_def_levels(buffer, 4)
    values_read.should eq(2)
    levels_read.should eq(4)
    buffer.levels.should eq(levels)
    mask_bits(buffer.nulls.as_slice, 4).should eq([true, false, false, true])

    buffer.consume_levels.should eq(levels)
    buffer.consume_bitmask.should eq(Bytes[0b0000_1001])
  end

  it "only builds the null mask in mask mode" do
    descriptor = Parquet::Schema::SchemaDescriptor.new(Parquet::Schema.parse_message_type(%(
      message test_schema {
        OPTIONAL INT32 a;
      }
    ))).column(0)

    buffer = RR::DefinitionLevelBuffer.new(descriptor, null_mask_only: true)
    buffer.levels.should be_nil

    decoder = RR::DefinitionLevelBufferDecoder.new(1_i16, packed: true)
    encoder = Parquet::Encodings::RleEncoder.new(1, 1024)
    [1, 0, 1, 1, 0].each { |level| encoder.put(level.to_u64) }
    decoder.set_data(PE::RLE, encoder.consume)

    values_read, levels_read = decoder.read_def_levels(buffer, 5)
    values_read.should eq(3)
    levels_read.should eq(5)
    mask_bits(buffer.nulls.as_slice, 5).should eq([true, false, true, true, false])
  end

  it "skips definition levels" do
    descriptor = Parquet::Schema::SchemaDescriptor.new(Parquet::Schema.parse_message_type(%(
      message test_schema {
        OPTIONAL INT32 a;
      }
    ))).column(0)

    decoder = RR::DefinitionLevelBufferDecoder.new(1_i16, packed: true)
    encoder = Parquet::Encodings::RleEncoder.new(1, 1024)
    [1, 0, 1, 1, 0].each { |level| encoder.put(level.to_u64) }
    decoder.set_data(PE::RLE, encoder.consume)

    decoder.skip_def_levels(3).should eq({2, 3})
    decoder.skip_def_levels(2).should eq({1, 2})
  end
end
