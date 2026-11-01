# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Porting tracks the Apache Arrow Rust `parquet` crate. The pinned upstream
revision is recorded per entry.

## [Unreleased]

### Added

- Arrow RecordBatch writing with nested/null/dictionary/temporal/decimal data,
  row and byte limits, V1/V2 pages, content-defined chunking, pluggable serialized
  page storage and encrypted round trips.
- Variant numeric operations, including Decimal256, safe/strict Arrow casts,
  union output, shredding and unshredding; canonical extension mappings and
  virtual row-number/row-group columns. Optional mappings use the feature flags
  documented in [testing](docs/testing.md#canonical-extensions-and-virtual-columns).
- Streaming `parquet-rewrite` and CSV/TSV `parquet-fromcsv` commands, with
  pinned Rust interoperability checks and compressed-input coverage.
- [Asynchronous storage reads and writes](docs/async-storage.md), memory/HTTP
  object stores, suffix footer fetching, explicit execution contexts and
  buffered object publication. Native limitations are documented in the guide.
- [Metadata memory estimates](docs/metadata-memory.md) and reusable
  [metadata snapshots](docs/metadata-snapshots.md), including independent mutable
  statistics, encryption buffers and row-group metadata.
- [Typed physical-column APIs](docs/typed-columns.md) for all eight physical
  types and [generated mixed Thrift unions](docs/thrift-unions.md), integrated
  into crypto metadata.
- [On-demand page-index caches](docs/on-demand-page-index.md),
  [explicit row-group fetching](docs/explicit-row-group-fetching.md) and a
  [memory object-store round-trip example](docs/object-store-example.md), each
  with runnable examples and parity specs.
- A release-mode [round-trip benchmark](docs/round-trip-benchmark.md) covering
  all 46 pinned source cases, exact fixture validation, reusable write buffers,
  first/warm CSV samples and configurable smoke runs. Native RNG and sampling
  differences are documented; timings do not claim Rust performance equivalence.

- Resumable parity workflow: a **Where we are** block at the top of
  `plans/parity.md` (current focus, ordered "Next up", per-area chunk status),
  `docs/resuming.md`, and a session-continuity / definition-of-done section in
  `CLAUDE.md` (`AGENTS.md`).

- Initial project scaffolding for the Crystal port of `parquet`
  (apache/arrow-rs `60.0.0`, `ef1fa157977633f0ba21aa921f9ef3d5c669235b`).
- Vendored upstream as a git submodule at `vendor/arrow-rs`, pinned to
  `60.0.0`, with `parquet-testing` fixtures initialized.
- Baseline tooling: `Makefile`, `.ameba.yml`, `.rumdl.toml`, `.gitignore`,
  `shard.yml`.
- Baseline docs: `README.md`, `CLAUDE.md`/`AGENTS.md`, and
  `docs/{architecture,development,coding-guidelines,testing,pr-workflow}.md`.
- Rust parity ledger under `plans/inventory/` (3,515 API items, 2,059 tests) and
  the `plans/parity.md` porting roadmap.

### Changed

- Feature-sized parity phases and an ordered resume queue replace helper-sized
  milestones. The API ledger retains per-symbol status and intentional
  divergences; full upstream parity remains in progress.
- Formatting and Ameba gates cover `src`, `spec`, `bin`, `examples` and `bench`.
  `make test-extensions` checks optional canonical, geospatial and Variant mappings.
- README navigation now links capability guides, runnable examples and benchmark
  instructions, and states the current Crystal/sibling Arrow requirements.

### Fixed

- `FileChunkReader#get_read` now duplicates the open handle, seeks and streams
  through bounded buffering, following upstream instead of allocating the whole
  file tail or overflowing above 2 GiB. File seeks beyond EOF and shared-position
  behavior match Rust. Block-scoped reads close page/splice streams on success
  and exceptions. See [file-reader behavior](docs/file-chunk-reader.md).
- `with_row_filter` (and any row selection) no longer mis-reads nullable
  columns. `ColumnReaderImpl#skip_records` skipped physical values without
  consuming the arrow definition-level stream, so a null in a skipped or
  selected range shifted later values and could leak dictionary values from
  outside the selection. Skipping now advances the definition levels and derives
  the value count from them, matching upstream `GenericColumnReader`.
- [UTF-8 validation](docs/arrow-string-validation.md) respects value boundaries,
  validates dictionary entries and rejects invalid binary-to-string projections.
- [Variable-byte page decoding](docs/arrow-byte-page-decoding.md) and
  [fixed-byte decoding](docs/arrow-fixed-byte-decoding.md) preserve encoding and
  skip state across page/batch boundaries, with explicit malformed-input errors.
- Metadata footer writes preserve borrowed snapshots when filling index offsets
  or encrypting columns; encrypted writers return separate plaintext metadata.

### Ported

The entries below retain the earlier foundational port history. All Unreleased
work targets apache/arrow-rs `60.0.0`
(`ef1fa157977633f0ba21aa921f9ef3d5c669235b`); representation differences are tracked
in the [parity reference](plans/parity/divergences.md).

- `errors` — `ParquetError` variant set (`GeneralError`, `NYIError`, `EOFError`,
  `ArrowError`, `IndexOutOfBoundError`, `ExternalError`, `NeedMoreDataError`,
  `NeedMoreDataRangeError`), error macros, and upstream `Display` strings.
- Thrift compact protocol (`src/parquet/thrift.cr`) — `FieldType`,
  `ElementType`, `OrderedF64` (IEEE 754 total order),
  `ThriftSliceInputProtocol`, `ThriftReadInputProtocol`,
  `ThriftCompactOutputProtocol`, `read_thrift_vec`, and `validate_list_type`,
  with upstream protocol tests in `spec/parquet/thrift_spec.cr`.
- Thrift IDL macro equivalents (`src/parquet/macros.cr`): `define_thrift_enum`
  and `define_thrift_union_all_empty` synthesize compact-protocol support for
  natively declared Crystal enums.
- `basic` thrift enums (`src/parquet/basic.cr`): `Type`, `ConvertedType`,
  `FieldRepetitionType`/`Repetition`, `Encoding`, `CompressionCodec`,
  `PageType`, `BoundaryOrder`, and the all-empty union `TimeUnit`, with
  `from_str` conversions and upstream `basic.rs`/`parquet_thrift.rs` tests
  (`spec/parquet/basic_spec.cr`).
- `define_thrift_struct` (`src/parquet/macros.cr`): generates a Crystal struct
  plus compact-protocol read/write from a declarative field block.
- `basic::LogicalType` (`src/parquet/basic.cr`): the union plus `DecimalType`,
  `TimestampType`/`TimeType`, `IntType`, `VariantType`, `GeometryType`,
  `GeographyType`, and `EdgeInterpolationAlgorithm`; `ConvertedType.from` and
  `LogicalType.from_str`. Upstream `basic.rs` tests for these ported to
  `spec/parquet/basic_spec.cr`.
- `data_type` value types (`src/parquet/data_type.cr`): `Int96`, `ByteArray`,
  `FixedLenByteArray`, `Decimal`, and the `AsBytes` trait; upstream
  `data_type.rs` tests ported to `spec/parquet/data_type_spec.cr`.
- `basic::SortOrder` and `basic::ColumnOrder` (`src/parquet/basic.cr`):
  `column_order_for_type`, sort-order resolution by logical/converted/physical
  type, `TypeDefinedOrder` handling, and union read/write. Upstream `basic.rs`
  sort/column-order tests and `test_parse_encoding` ported to
  `spec/parquet/basic_spec.cr`.
- Rust-`Debug`-compatible formatting (`rust_debug`) for `LogicalType` and its
  member structs, so upstream `schema` error messages match exactly.
- `schema::types` core (`src/parquet/schema/types.cr`): `Type` (primitive/group),
  `BasicTypeInfo`, `PrimitiveTypeBuilder`, `GroupTypeBuilder` (full
  logical/converted/physical validation, decimal precision, `FILE` group
  validation), and `check_contains`; upstream `schema/types.rs` core tests
  ported to `spec/parquet/schema/types_spec.cr`.
- `schema::types` descriptors (`src/parquet/schema/types.cr`): `ColumnPath`,
  `ColumnDescriptor`, `SchemaDescriptor`, and the DFS definition/repetition-level
  tree builder; upstream `test_column_descriptor`, `test_schema_descriptor`, and
  `ColumnPath` doc-example characterization ported to
  `spec/parquet/schema/types_spec.cr`.
- `schema::parser` (`src/parquet/schema/parser.cr`): `Parquet::Schema.parse_message_type`
  with the upstream tokenizer and full message-type grammar (groups, primitives,
  logical/converted types, DECIMAL precision/scale, TIME/TIMESTAMP units,
  INTEGER bit width, FIXED_LEN_BYTE_ARRAY length, field ids). All upstream
  `parser.rs` tests ported to `spec/parquet/schema/parser_spec.cr`; the
  parser-driven `test_schema_build_tree_*` tests ported to `types_spec.cr`.
- Consolidated shared spec aliases into `spec/support/parquet_aliases.cr`.
- Thrift <-> schema conversion (`src/parquet/schema/thrift.cr`): the
  `SchemaElement` thrift struct, `write_schema`, `parquet_schema_from_array`,
  `num_nodes`, and `schema_to_buf`/`buf_to_schema_list`/`roundtrip_schema`
  helpers. Upstream `test_schema_type_thrift_conversion*` and
  `test_schema_from_thrift_*` ported to `spec/parquet/schema/thrift_spec.cr`.
  `define_thrift_struct` now generates all-required constructor parameters.
- Page-header thrift structs (`src/parquet/format.cr`): `PageHeader`,
  `DataPageHeader`, `DataPageHeaderV2`, `DictionaryPageHeader`,
  `IndexPageHeader`, `PageStatistics`, and the `read_*_without_stats` readers.
  Upstream malformed-bool tests ported to `spec/parquet/format_spec.cr`.
  Fixed   `define_thrift_struct` to emit optional `Bool` fields whose value is
  `false` (previously dropped by a truthiness check).
- File statistics API (`src/parquet/file/statistics.cr`): `Statistics` and
  `ValueStatistics<T>` with `Display`, Rust-compatible `Debug`, byte views of
  min/max, and `from_thrift_page_stats`/`page_stats_to_thrift` conversion.
  Upstream `statistics.rs` tests ported to
  `spec/parquet/file/statistics_spec.cr`; added `Int96.try_from_le_slice`.
- `LevelHistogram` (`src/parquet/file/metadata.cr`): the repetition/definition
  level histogram with `try_new`/`from`/`get`/`add`/`increment_by`/
  `update_from_levels`/`reset`. Upstream
  `test_level_histogram_update_from_levels_compat` ported to
  `spec/parquet/file/metadata_spec.cr`.
- `EncodingMask` (`src/parquet/basic.cr`): bitmask of the `Encoding`s used by a
  column chunk, with `try_new` validation, `new_from_encodings`, `insert`,
  `set?`/`only?`/`all_set`, `encodings`, and thrift reading. Upstream
  `test_encoding_roundtrip`, `test_invalid_encoding_mask`, and
  `test_encoding_mask_is_only` ported to `spec/parquet/basic_spec.cr`.
- `ColumnChunkMetaData` (`src/parquet/file/metadata/column_chunk.cr`): the
  column-chunk metadata type, its builder, the `KeyValue`/`PageEncodingStats`/
  `SortingColumn`/`SizeStatistics` thrift structs, and thrift
  read/write (`ColumnChunk`/`ColumnMetaData`, incl. the required-field mask
  validation and page-encoding-stats condensation). Upstream
  `test_column_chunk_metadata_thrift_conversion` ported to
  `spec/parquet/file/column_chunk_spec.cr`.
- `RowGroupMetaData` (`src/parquet/file/metadata/row_group.cr`): the row-group
  metadata type, its builder, and thrift read/write (`RowGroup`, including the
  column-count checks and required-field validation). Upstream
  `test_row_group_metadata_thrift_conversion{,_empty,_corrupted}` ported to
  `spec/parquet/file/row_group_spec.cr`; moved shared spec helpers into
  `spec/support/parquet_helpers.cr`.
- `FileMetaData` / `ParquetMetaData` (`src/parquet/file/metadata/parquet_metadata.cr`):
  the file metadata types plus footer read/write (`FileMetaData` thrift write,
  `parquet_metadata_from_bytes`, row-group ordinal backfill, and a
  `ParquetMetaDataWriter`/`ParquetMetaDataReader.decode_metadata` round-trip).
  Upstream ordinal tests ported to `spec/parquet/file/parquet_metadata_spec.cr`.
- Offset index (`src/parquet/file/page_index/offset_index.cr`): `PageLocation`
  and `OffsetIndexMetaData` thrift structs. Upstream `test_offset_idx_roundtrip`
  ported to `spec/parquet/file/offset_index_spec.cr`; added `PPI` spec alias.
- Column index core (`src/parquet/file/page_index/column_index.cr`): `ColumnIndex`,
  `PrimitiveColumnIndex<T>`, `ThriftColumnIndex`, and
  `FromBytes.try_from_le_slice`. Upstream `test_page_index_min_max_null`,
  `..._none`, `test_invalid_column_index`, and
  `test_column_index_rejects_mismatched_min_max_lengths` ported to
  `spec/parquet/file/column_index_spec.cr`.
- Harden `Makefile` gates by stripping macOS AppleDouble `._*` sidecars (whose
  names can match crystal's `*_spec.cr` glob on exFAT volumes).

[Unreleased]: https://github.com/dsisnero/parquet-cr/commits/main
