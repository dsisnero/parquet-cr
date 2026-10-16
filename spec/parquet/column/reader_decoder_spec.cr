require "../../spec_helper"

private alias Enc = Parquet::Encoding

# Ported from `parquet/src/column/reader/decoder.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias RepLevelDecoder = Parquet::Column::Reader::RepetitionLevelDecoderImpl
private alias DefLevelDecoder = Parquet::Column::Reader::DefinitionLevelDecoderImpl
private alias ValDecoder = Parquet::Column::Reader::ColumnValueDecoderImpl

private def make_column_decoder_desc(physical : Parquet::Type, type_length : Int32) : Parquet::Schema::ColumnDescriptor
  ty = Parquet::Schema::Type.primitive_type_builder("col", physical).with_length(type_length).build
  Parquet::Schema::ColumnDescriptor.new(ty, 0_i16, 0_i16, Parquet::Schema::ColumnPath.new("col"))
end

describe Parquet::Column::Reader::RepetitionLevelDecoderImpl do
  # Upstream `test_skip_padding`.
  it "skips zero padding in the final bit-packed run" do
    encoder = RleEncoder.new(1, 1024)
    encoder.put(0_u64)
    3.times { encoder.put(1_u64) }
    data = encoder.consume

    decoder = RepLevelDecoder.new(1_i16)
    decoder.set_data(Enc::RLE, data)

    _, levels = decoder.skip_rep_levels(100, 4)
    levels.should eq(4)

    # The final bit-packed run's length is ambiguous, so the levels limit keeps
    # the decoder from reading zero padding.
    decoder = RepLevelDecoder.new(1_i16)
    decoder.set_data(Enc::RLE, data)
    _, levels = decoder.skip_rep_levels(100, 6)
    levels.should eq(6)
  end

  # Upstream `test_skip_rep_levels`.
  it "skips repetition levels consistently with reading them" do
    rng = Random.new(1234)
    10.times do
      total_len = 10_000
      encoded = Array(Int16).new(total_len) { rng.rand(5).to_i16 }
      encoded[0] = 0_i16

      encoder = RleEncoder.new(3, 1024)
      encoded.each { |value| encoder.put(value.to_u64) }
      data = encoder.consume

      decoder = RepLevelDecoder.new(5_i16)
      decoder.set_data(Enc::RLE, data)

      total_records = encoded.count(&.zero?)
      remaining_records = total_records
      remaining_levels = encoded.size
      loop do
        skip = rng.rand(2) == 0
        records = rng.rand(1..remaining_records.clamp(1, 5))
        records_read, levels_read =
          if skip
            decoder.skip_rep_levels(records, remaining_levels)
          else
            decoded = [] of Int16
            rr, lr = decoder.read_rep_levels(decoded, records, remaining_levels)
            decoded.should eq(encoded[encoded.size - remaining_levels, lr])
            {rr, lr}
          end

        remaining_levels -= levels_read
        if remaining_levels == 0
          (records_read + 1).should eq(records)
          records.should eq(remaining_records)
          break
        end
        records_read.should eq(records)
        remaining_records -= records
        remaining_records.should_not eq(0)
      end
    end
  end
end

describe Parquet::Column::Reader::DefinitionLevelDecoderImpl do
  it "counts non-null values at the max level" do
    encoder = RleEncoder.new(2, 1024)
    [0_i16, 2_i16, 2_i16, 1_i16, 2_i16, 0_i16].each { |v| encoder.put(v.to_u64) }
    data = encoder.consume

    decoder = DefLevelDecoder.new(2_i16)
    decoder.set_data(Enc::RLE, data)

    output = [] of Int16
    values, levels = decoder.read_def_levels(output, 6)
    output.should eq([0_i16, 2_i16, 2_i16, 1_i16, 2_i16, 0_i16])
    levels.should eq(6)
    values.should eq(3)
  end

  it "skips definition levels" do
    encoder = RleEncoder.new(2, 1024)
    [0_i16, 2_i16, 2_i16, 1_i16, 2_i16, 0_i16].each { |v| encoder.put(v.to_u64) }
    data = encoder.consume

    decoder = DefLevelDecoder.new(2_i16)
    decoder.set_data(Enc::RLE, data)

    values, levels = decoder.skip_def_levels(4)
    levels.should eq(4)
    values.should eq(2)
  end
end

describe Parquet::Column::Reader::ColumnValueDecoderImpl do
  it "decodes PLAIN values" do
    encoder = PlainEncoder(Int32).new
    encoder.put([1_i32, 2_i32, 3_i32])
    data = encoder.flush_buffer

    descr = make_column_decoder_desc(Parquet::Type::INT32, -1)
    decoder = ValDecoder(Int32).new(descr)
    decoder.set_data(Enc::PLAIN, data, 3, 3)

    output = [] of Int32
    decoder.read(output, 3).should eq(3)
    output.should eq([1_i32, 2_i32, 3_i32])

    decoder.skip_values(1).should eq(0)
  end

  it "decodes dictionary-encoded values" do
    descr = make_column_decoder_desc(Parquet::Type::INT32, -1)
    dict_encoder = Parquet::Encodings::DictEncoder(Int32).new(descr)
    dict_encoder.put([10_i32, 20_i32, 10_i32, 30_i32])
    indices = dict_encoder.flush_buffer
    dictionary = dict_encoder.write_dict

    decoder = ValDecoder(Int32).new(descr)
    decoder.set_dict(dictionary, dict_encoder.num_entries.to_u32, Enc::RLE_DICTIONARY, false)
    decoder.set_data(Enc::RLE_DICTIONARY, indices, 4, 4)

    output = [] of Int32
    decoder.read(output, 4).should eq(4)
    output.should eq([10_i32, 20_i32, 10_i32, 30_i32])
  end

  it "rejects a second dictionary page" do
    descr = make_column_decoder_desc(Parquet::Type::INT32, -1)
    decoder = ValDecoder(Int32).new(descr)
    decoder.set_dict(Bytes.new(0), 0_u32, Enc::RLE_DICTIONARY, false)
    expect_raises(Parquet::ParquetError, "Column cannot have more than one dictionary") do
      decoder.set_dict(Bytes.new(0), 0_u32, Enc::RLE_DICTIONARY, false)
    end
  end
end
