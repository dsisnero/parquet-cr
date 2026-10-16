require "../../spec_helper"

# Ported from `parquet/src/column/page.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias Page = Parquet::Column::Page
private alias CompressedPage = Parquet::Column::CompressedPage
private alias Enc = Parquet::Encoding
private alias PType = Parquet::PageType
private alias Stats = Parquet::Statistics

private def sample_stats : Stats
  Stats.int32(1, 2, nil, 1_u64, true)
end

describe Parquet::Column::Page do
  # Upstream `test_page`.
  it "exposes page accessors" do
    data_page = Page::DataPage.new(Bytes[0_u8, 1_u8, 2_u8], 10_u32, Enc::PLAIN, Enc::RLE, Enc::RLE, sample_stats)
    data_page.page_type.should eq(PType::DATA_PAGE)
    data_page.buffer.should eq(Bytes[0_u8, 1_u8, 2_u8])
    data_page.num_values.should eq(10)
    data_page.encoding.should eq(Enc::PLAIN)
    data_page.statistics.should eq(sample_stats)
    data_page.is_data_page?.should be_true
    data_page.is_dictionary_page?.should be_false

    data_page_v2 = Page::DataPageV2.new(
      Bytes[0_u8, 1_u8, 2_u8], 10_u32, Enc::PLAIN, 5_u32, 20_u32, 30_u32, 40_u32, false, sample_stats
    )
    data_page_v2.page_type.should eq(PType::DATA_PAGE_V2)
    data_page_v2.buffer.should eq(Bytes[0_u8, 1_u8, 2_u8])
    data_page_v2.num_values.should eq(10)
    data_page_v2.encoding.should eq(Enc::PLAIN)
    data_page_v2.statistics.should eq(sample_stats)

    dict_page = Page::DictionaryPage.new(Bytes[0_u8, 1_u8, 2_u8], 10_u32, Enc::PLAIN, false)
    dict_page.page_type.should eq(PType::DICTIONARY_PAGE)
    dict_page.buffer.should eq(Bytes[0_u8, 1_u8, 2_u8])
    dict_page.num_values.should eq(10)
    dict_page.encoding.should eq(Enc::PLAIN)
    dict_page.statistics.should be_nil
    dict_page.is_dictionary_page?.should be_true
  end
end

describe Parquet::Column::CompressedPage do
  # Upstream `test_compressed_page`.
  it "wraps a page with its uncompressed size" do
    data_page = Page::DataPage.new(Bytes[0_u8, 1_u8, 2_u8], 10_u32, Enc::PLAIN, Enc::RLE, Enc::RLE, sample_stats)
    cpage = CompressedPage.new(data_page, 5)

    cpage.page_type.should eq(PType::DATA_PAGE)
    cpage.uncompressed_size.should eq(5)
    cpage.compressed_size.should eq(3)
    cpage.num_values.should eq(10)
    cpage.encoding.should eq(Enc::PLAIN)
    cpage.data.should eq(Bytes[0_u8, 1_u8, 2_u8])
    cpage.memory_usage.should eq(3)
  end

  # Upstream `test_compressed_page_uncompressed_size_overflow`.
  it "rejects an uncompressed size overflow in the thrift header" do
    data_page = Page::DataPage.new(Bytes[0_u8, 1_u8, 2_u8], 10_u32, Enc::PLAIN, Enc::RLE, Enc::RLE, nil)
    cpage = CompressedPage.new(data_page, Int32::MAX.to_i64 + 1)

    err = expect_raises(Parquet::ParquetError) { cpage.to_thrift_header }
    err.to_s.should contain("Page uncompressed size overflow")
  end
end
