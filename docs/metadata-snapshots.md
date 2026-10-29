# Reusable metadata snapshots

`ParquetMetaData#clone` copies file metadata, row groups and column chunks so a
snapshot can be edited and written independently:

```crystal
snapshot = metadata.clone
column = snapshot.row_group(0).column(0)
column.into_builder.set_compression(Parquet::Compression.zstd(7)).build
```

Builders retain native reference semantics: clone before passing existing
metadata into a builder when the original must remain unchanged. Changing a
codec describes the column's existing encoded data; it does not transcode pages.
Metadata stores the codec, so `compression_config` reconstructs its default
level, matching upstream's `compression()`. The existing native `compression`
codec property and `compression_codec` getter remain available.

Clones copy mutable arrays and bytes in statistics, histograms, full encoding
statistics, key/value metadata, column orders, sorting columns, encryption
parameters, signing metadata, encrypted column metadata and geospatial types.
All eight physical statistics variants retain counts and exactness/compatibility
flags, including fields not preserved by a Thrift round trip. Binary statistics
receive independent native byte buffers because Crystal exposes mutable slices.

Schema/column descriptors and page-index providers remain shared, matching
upstream's Arc references. A cloned file decryptor owns a new AAD buffer but
shares its decryption properties and block decryptor. Clone does not refetch
keys. Shared objects must be treated as immutable; cloning is not a concurrent
mutation guarantee.

`ParquetMetaDataWriter` clones borrowed row groups before back-filling page-index
offsets or encrypting column metadata. The same snapshot can therefore produce
multiple footers at different positions, including encrypted and plain outputs.
The lower-level `MetadataObjectWriter` and `ThriftMetadataWriter` consume their
supplied row groups by convention, retaining native references rather than
Rust's enforced moves. Their callers must clone inputs they intend to reuse.
Encrypted writes retain a separate plaintext row-group graph for returned
metadata, including the result of `SerializedFileWriter#close`.

The source is apache/arrow-rs 60.0.0 (`ef1fa157977633f0ba21aa921f9ef3d5c669235b`):
derived `Clone` in `file/metadata/mod.rs`, `encryption/decrypt.rs`, and the owned
vs borrowed write paths in `file/metadata/writer.rs`. Discovery does not generate
rows for derived Clone; explicit evidence lives in `metadata_snapshot_spec.cr`
and `metadata_snapshot_writer_spec.cr`. Their fixture derives from
`test_column_chunk_metadata_thrift_conversion_full_stats`; writer mutation tests
characterize the pinned implementation, which has no writer.rs unit tests.
Modern min/max fields reconstruct the compatibility flag from `old_format` on
wire decode; clone deliberately preserves the original in-memory flag instead.

Red evidence: the clone API was absent, and the three initial writer specs
reported two mutation failures and a missing plaintext-copy error. Eleven new
specs and eighteen existing metadata/encryption regression specs pass, including
actual encrypted-footer and signed-plaintext-footer file writes.
