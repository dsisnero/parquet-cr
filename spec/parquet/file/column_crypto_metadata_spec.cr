require "../../spec_helper"

# Ported from `parquet/src/file/column_crypto_metadata.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: the union is a `struct` with a `Kind` + payload, so the
# footer-key variant is the `ENCRYPTION_WITH_FOOTER_KEY` constant and the
# column-key variant is built with `.encryption_with_column_key(..)`.

private alias CCM = Parquet::File::ColumnCryptoMetaData
private alias EWCK = Parquet::File::EncryptionWithColumnKey

describe Parquet::File::ColumnCryptoMetaData do
  # Upstream `test_column_crypto_roundtrip`.
  it "round-trips column crypto metadata" do
    ThriftRoundTrip.roundtrip(CCM::ENCRYPTION_WITH_FOOTER_KEY)
      .should eq(CCM::ENCRYPTION_WITH_FOOTER_KEY)

    path_in_schema = ["foo", "bar", "really"]
    ThriftRoundTrip
      .roundtrip(CCM.encryption_with_column_key(EWCK.new(path_in_schema, nil)))
      .should eq(CCM.encryption_with_column_key(EWCK.new(path_in_schema, nil)))

    key_metadata = Bytes.new(32) { 1_u8 }
    ThriftRoundTrip
      .roundtrip(CCM.encryption_with_column_key(EWCK.new(path_in_schema, key_metadata)))
      .should eq(CCM.encryption_with_column_key(EWCK.new(path_in_schema, key_metadata)))
  end
end
