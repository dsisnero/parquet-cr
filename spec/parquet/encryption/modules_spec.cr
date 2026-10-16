require "../../spec_helper"

# Characterization specs for `parquet/src/encryption/modules.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream has no dedicated tests for the AAD builders (they are exercised
# through the encrypted file writer/reader); these pin the AAD byte layout.

private alias Modules = Parquet::Encryption
private alias ModuleType = Parquet::Encryption::ModuleType

private def i16_le(value : Int32) : Bytes
  io = IO::Memory.new(2)
  io.write_bytes(value.to_i16, IO::ByteFormat::LittleEndian)
  io.to_slice
end

describe Parquet::Encryption do
  it "appends only the module byte for the footer" do
    Modules.create_footer_aad("aad".to_slice)
      .should eq("aad".to_slice + Bytes[ModuleType::Footer.value.to_u8])
  end

  it "appends row group and column ordinals for non-page modules" do
    Modules.create_module_aad("file".to_slice, ModuleType::ColumnMetaData, 5, 7, nil)
      .should eq("file".to_slice + Bytes[1] + i16_le(5) + i16_le(7))

    Modules.create_module_aad("file".to_slice, ModuleType::OffsetIndex, 0, 0, nil)
      .should eq("file".to_slice + Bytes[7] + i16_le(0) + i16_le(0))
  end

  it "appends row group, column, and page ordinals for data pages" do
    Modules.create_module_aad("file".to_slice, ModuleType::DataPage, 1, 2, 3)
      .should eq("file".to_slice + Bytes[2] + i16_le(1) + i16_le(2) + i16_le(3))

    Modules.create_module_aad("file".to_slice, ModuleType::DataPageHeader, 4, 5, 6)
      .should eq("file".to_slice + Bytes[4] + i16_le(4) + i16_le(5) + i16_le(6))
  end

  it "requires a page ordinal for data pages" do
    expect_raises(Parquet::GeneralError, /Page ordinal must be set/) do
      Modules.create_module_aad("file".to_slice, ModuleType::DataPage, 0, 0, nil)
    end
  end

  it "rejects ordinals that exceed i16" do
    expect_raises(Parquet::GeneralError, /more than 32767 row groups/) do
      Modules.create_module_aad("file".to_slice, ModuleType::ColumnMetaData, 32_768, 0, nil)
    end
    expect_raises(Parquet::GeneralError, /more than 32767 columns/) do
      Modules.create_module_aad("file".to_slice, ModuleType::ColumnMetaData, 0, 32_768, nil)
    end
    expect_raises(Parquet::GeneralError, /more than 32767 pages per column chunk/) do
      Modules.create_module_aad("file".to_slice, ModuleType::DataPage, 0, 0, 32_768)
    end
  end
end
