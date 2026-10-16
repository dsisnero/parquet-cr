require "../../spec_helper"

# Ported from `parquet/src/file/metadata/push_decoder.rs`,
# `parquet/src/file/metadata/parser.rs`, and `parquet/src/file/metadata/reader.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream's push-decoder tests build a Parquet file with the Arrow writer; here
# the same decoder is driven against a golden fixture file instead, keeping the
# same expectations for the footer/metadata/page-index state machine.

private def push_decoder_fixture : Bytes
  ParquetTestHelpers.fixture_bytes("data_index_bloom_encoding_stats.parquet")
end

private def push_ranges_for(decoder : PMeta::ParquetMetaDataPushDecoder, ranges : Array(Range(UInt64, UInt64)), bytes : Bytes) : Nil
  buffers = ranges.map { |range| bytes[range.begin.to_i32, (range.end - range.begin).to_i32] }
  decoder.push_ranges(ranges, buffers)
end

describe Parquet::DecodeResult do
  it "represents data, needs-data, and finished" do
    data = Parquet::DecodeResult(Int32).data(5)
    data.data?.should be_true
    data.data.should eq(5)

    needs = Parquet::DecodeResult(Int32).needs_data([0_u64...8_u64])
    needs.needs_data?.should be_true
    needs.ranges.should eq([0_u64...8_u64])

    finished = Parquet::DecodeResult(Int32).finished
    finished.finished?.should be_true
  end
end

describe Parquet::File::Metadata::PageIndexPolicy do
  it "defaults to Skip and converts from Bool" do
    Parquet::File::Metadata::PageIndexPolicy::Skip.should eq(Parquet::File::Metadata::PageIndexPolicy::Skip)
    Parquet::File::Metadata::PageIndexPolicy.from(true).should eq(Parquet::File::Metadata::PageIndexPolicy::Required)
    Parquet::File::Metadata::PageIndexPolicy.from(false).should eq(Parquet::File::Metadata::PageIndexPolicy::Skip)
  end
end

describe Parquet::File::Metadata::ParquetMetaDataPushDecoder do
  it "decodes metadata when the whole file is pushed" do
    bytes = push_decoder_fixture
    file_len = bytes.size.to_u64

    decoder = PMeta::ParquetMetaDataPushDecoder.try_new(file_len)
    decoder.push_range(0_u64...file_len, bytes)

    result = decoder.try_decode
    result.data?.should be_true
    metadata = result.data
    metadata.num_row_groups.should eq(1)
    decoder.try_decode.finished?.should be_true
  end

  it "requests the footer, metadata, and page index incrementally" do
    bytes = push_decoder_fixture
    file_len = bytes.size.to_u64

    decoder = PMeta::ParquetMetaDataPushDecoder.try_new(file_len)

    ranges = decoder.try_decode.ranges
    ranges.should eq([file_len - 8...file_len])
    push_ranges_for(decoder, ranges, bytes)

    push_ranges_for(decoder, decoder.try_decode.ranges, bytes)

    result = decoder.try_decode
    until result.data?
      push_ranges_for(decoder, result.ranges, bytes)
      result = decoder.try_decode
    end

    metadata = result.data
    metadata.num_row_groups.should eq(1)
    metadata.page_index.should_not be_nil
    metadata.page_index.not_nil!.complete?.should be_true
    decoder.try_decode.finished?.should be_true
  end

  it "skips page indexes with PageIndexPolicy::Skip" do
    bytes = push_decoder_fixture
    file_len = bytes.size.to_u64

    decoder = PMeta::ParquetMetaDataPushDecoder.try_new(file_len)
      .with_page_index_policy(Parquet::File::Metadata::PageIndexPolicy::Skip)

    push_ranges_for(decoder, decoder.try_decode.ranges, bytes)
    push_ranges_for(decoder, decoder.try_decode.ranges, bytes)

    result = decoder.try_decode
    result.data?.should be_true
    result.data.page_index.should be_nil
  end

  it "allows prefetching the whole file in one push" do
    bytes = push_decoder_fixture
    file_len = bytes.size.to_u64

    decoder = PMeta::ParquetMetaDataPushDecoder.try_new(file_len)
    decoder.push_range(0_u64...file_len, bytes)

    decoder.try_decode.data?.should be_true
  end

  it "clears buffered ranges and re-requests the footer" do
    bytes = push_decoder_fixture
    file_len = bytes.size.to_u64

    decoder = PMeta::ParquetMetaDataPushDecoder.try_new(file_len)
    decoder.push_range(0_u64...file_len, bytes)
    decoder.clear_all_ranges

    decoder.try_decode.ranges.should eq([file_len - 8...file_len])
  end

  it "rejects files shorter than the footer size" do
    expect_raises(Parquet::ParquetError, "Parquet files are at least 8 bytes long") do
      PMeta::ParquetMetaDataPushDecoder.try_new(4_u64)
    end
  end

  it "rejects pushes after decoding has finished" do
    bytes = push_decoder_fixture
    file_len = bytes.size.to_u64

    decoder = PMeta::ParquetMetaDataPushDecoder.try_new(file_len)
    decoder.push_range(0_u64...file_len, bytes)
    decoder.try_decode.data?.should be_true
    decoder.try_decode.finished?.should be_true

    expect_raises(Parquet::ParquetError, "cannot push data after decoding is finished") do
      decoder.push_range(0_u64...0_u64, Bytes.new(0))
    end
  end
end
