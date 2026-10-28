require "../errors"
require "../macros"
require "../thrift"

# Ported from `parquet/src/file/column_crypto_metadata.rs` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `EncryptionWithColumnKey` and the `ColumnCryptoMetaData` union (column
# chunk encryption metadata). Divergences: the payload-carrying Rust enum is a
# `struct` with a `Kind` + payload (Crystal has no enums with payloads), matching
# the `ColumnOrder`/`Compression` convention; native `HeapSize` lives in metadata/memory.cr.
module Parquet
  module File
    # Encryption metadata for a column chunk encrypted with a column-specific key.
    define_thrift_struct(EncryptionWithColumnKey) do
      required 1, path_in_schema, Array(String)
      optional 2, key_metadata, Bytes
    end

    # Column chunk crypto metadata: either the footer key or a column key.
    define_thrift_union ColumnCryptoMetaData do
      unit 1, EncryptionWithFooterKey, encryption_with_footer_key
      variant 2, EncryptionWithColumnKey, encryption_with_column_key, EncryptionWithColumnKey
    end
  end
end
