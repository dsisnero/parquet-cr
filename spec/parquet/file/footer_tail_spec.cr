require "../../spec_helper"

# Ported from `parquet/src/file/metadata/footer_tail.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Upstream documents these through the `FooterTail` doc examples; there are no
# `#[test]` cases in the module.
describe Parquet::File::Metadata::FooterTail do
  # Upstream doc example: non-encrypted footer with 28 bytes of metadata.
  it "parses a non-encrypted footer tail" do
    bytes = Bytes[0x1C_u8, 0x00_u8, 0x00_u8, 0x00_u8, 0x50_u8, 0x41_u8, 0x52_u8, 0x31_u8]

    tail = PMeta::FooterTail.try_from(bytes)

    tail.metadata_length.should eq(28_u32)
    tail.encrypted_footer?.should be_false
  end

  # Upstream doc example: encrypted footer with 512 bytes of metadata.
  it "parses an encrypted footer tail" do
    bytes = Bytes[0x00_u8, 0x02_u8, 0x00_u8, 0x00_u8, 0x50_u8, 0x41_u8, 0x52_u8, 0x45_u8]

    tail = PMeta::FooterTail.try_new(bytes)

    tail.metadata_length.should eq(512_u32)
    tail.encrypted_footer?.should be_true
  end

  it "rejects a corrupt magic" do
    bytes = Bytes[0x00_u8, 0x00_u8, 0x00_u8, 0x00_u8, 0x58_u8, 0x58_u8, 0x58_u8, 0x58_u8]

    expect_raises(Parquet::ParquetError, "Invalid Parquet file. Corrupt footer") do
      PMeta::FooterTail.try_from(bytes)
    end
  end

  it "rejects a wrong length" do
    expect_raises(Parquet::ParquetError, "Invalid footer length 4, expected 8") do
      PMeta::FooterTail.try_from(Bytes[0x00_u8, 0x00_u8, 0x00_u8, 0x00_u8])
    end
  end
end
