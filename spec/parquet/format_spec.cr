require "../spec_helper"

# Ported from `parquet/src/file/metadata/thrift/mod.rs` `mod tests`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
alias PFmt = Parquet::Format

describe Parquet::Format do
  # Characterization: a fully populated PageHeader round-trips.
  it "round-trips a PageHeader with a dictionary page header" do
    header = PFmt::PageHeader.new(
      Parquet::PageType::DICTIONARY_PAGE, 1, 1, nil, nil, nil,
      PFmt::DictionaryPageHeader.new(1, Parquet::Encoding::PLAIN, false),
      nil,
    )
    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(thrift_bytes(header))
    PFmt::PageHeader.read_thrift(prot).should eq(header)
  end

  # Upstream `malformed_bool_field_returns_error_not_panic`.
  it "returns an error (not a panic) for a malformed dictionary-page bool field" do
    page_header = PFmt::PageHeader.new(
      Parquet::PageType::DICTIONARY_PAGE, 1, 1, nil, nil, nil,
      PFmt::DictionaryPageHeader.new(1, Parquet::Encoding::PLAIN, false),
      nil,
    )

    buf = thrift_bytes(page_header)
    change_false_bool_field_to_i32(buf)

    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(buf)
    err = expect_raises(Parquet::ParquetError) do
      PFmt.read_page_header_without_stats(prot)
    end
    assert_malformed_bool_error(err)
  end

  # Upstream `malformed_data_page_v2_bool_field_returns_error_not_panic`.
  it "returns an error (not a panic) for a malformed data-page-v2 bool field" do
    data_page_header_v2 = PFmt::DataPageHeaderV2.new(
      1, 0, 1, Parquet::Encoding::PLAIN, 0, 0, false, nil,
    )

    buf = thrift_bytes(data_page_header_v2)
    change_false_bool_field_to_i32(buf)

    prot = Parquet::Thrift::ThriftSliceInputProtocol.new(buf)
    err = expect_raises(Parquet::ParquetError) do
      PFmt.read_data_page_header_v2_without_stats(prot)
    end
    assert_malformed_bool_error(err)
  end
end

private def thrift_bytes(value : T) : Bytes forall T
  io = IO::Memory.new
  writer = Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
  value.write_thrift(writer)
  io.to_slice
end

private def change_false_bool_field_to_i32(buf : Bytes) : Nil
  pos = buf.rindex(0x12_u8)
  raise "expected BOOL_FALSE field header byte" if pos.nil?

  buf[pos] = 0x15_u8
end

private def assert_malformed_bool_error(err : Parquet::ParquetError) : Nil
  err.to_s.should contain("Unexpected struct field type")
end
