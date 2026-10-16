# parquet-cr Parity Roadmap

> **Start here.** New session? Read the [Where we are](#where-we-are) block
> below for the current focus and next chunk, then follow
> [docs/resuming.md](../docs/resuming.md). Do not re-derive state from git.

Curated, branch-sized feature roadmap. Generated manifests live in
`plans/inventory/`; the per-symbol ledger is
`plans/inventory/rust_port_inventory.tsv`.

- Upstream: <https://github.com/apache/arrow-rs>
- Pinned revision: `60.0.0`
  (`ef1fa157977633f0ba21aa921f9ef3d5c669235b`)
- Vendored at: `vendor/arrow-rs` (submodule)
- Scope: `parquet` (core), then `parquet-variant*`, `parquet-geospatial`,
  `parquet_derive*`
- Deferred: `parquet/src/arrow/` (Arrow interop) — depends on a Crystal Arrow
  implementation built separately in the sibling repo `../arrow-cr` (same
  `60.0.0` pin); `async_reader`/`async_writer`/`object_store`.

Status legend: `[ ]` not started, `[~]` in progress, `[x]` complete,
`[!]` intentional divergence.

Each item is a shippable feature (roughly one branch/commit). The exhaustive
method-level state lives in the ledger, not here.

Do not weaken upstream tests or fixtures. Port behavior, not style. Preserve
numeric widths, half-open ranges (`start..end` → `start...end`), and the on-disk
little-endian layout.

## Where we are

- Plan layout: single file. If it grows too large, split into
  `plans/parity/<area>.md`, link each here, and mark each split file
  complete/in-progress plus the file being worked on next.
- Current focus: **Phase 2 — file metadata + page-index integration.** The core
  value/wire layer (errors, thrift protocol, `basic`, `data_type` value types,
  `schema`), the file metadata + statistics + footer round-trip, the offset/
  column index core + decode, the page-index builders, the
  `ParquetMetaDataBuilder`/`ParquetMetaDataOptions` plumbing, and the
  sans-IO `ParquetMetaDataPushDecoder` are done.
- Next up (take the first unchecked item):
  1. Pull-based `ParquetMetaDataReader`/`ParquetMetaDataWriter` fetch/suffix
     machinery driven by `ChunkReader` (`reader.rs`/`writer.rs`); async/
     object-store suffix fetches are likely an intentional divergence.
  2. Encodings (RLE/bit-packing, levels, plain/dictionary/delta/ALP), then
     column reader/writer, then file reader/writer, then record API.
- Progress by counts (ledger): 516 `ported`, 34 `intentional_divergence`,
  1 `skipped`, 2964 `missing` (of 3515 rows).

Chunk status by area:

- Phase 0 initialization: complete.
- Phase 1.1 errors: complete.
- Phase 1.2 thrift protocol: complete.
- Phase 1.3 thrift macros: enum/union/struct/empty done; generic mixed-union
  macro and encryption IDL pending.
- Phase 1.4 basic: enums, `LogicalType`, `SortOrder`/`ColumnOrder`,
  `EncodingMask` done; `Compression` enum pending.
- Phase 1.5 data type: value types done; `DataType` trait + `make_type!` pending.
- Phase 1.6 schema: `types`, descriptors, parser, thrift conversion, page
  headers done; printer + primitive builder pending.
- Phase 1.7 util: pending (`bit_util` partially needed by column code).
- Phase 1.8 compression: pending.
- Phase 2.3 file metadata/statistics/page-index core, column-index decode,
  page-index builders, the `ParquetMetaData` page-index plumbing, and the
  sans-IO `ParquetMetaDataPushDecoder` done; pull-based reader/writer fetch
  machinery, file reader/writer, bloom filter pending.
- Phases 2.1 encodings, 2.2 column, 2.4 record, 2.5 CLI: pending.
- Phase 3 (variant/geospatial/derive/encryption/arrow/async): pending.

## Phase 0 — Project initialization

- [x] Upstream submodule pinned at `60.0.0`; fixtures via `parquet-testing`.
- [x] Baseline tooling: `Makefile`, `.ameba.yml`, `.rumdl.toml`, `.gitignore`,
  `shard.yml`.
- [x] Baseline docs: `README.md`, `CLAUDE.md`/`AGENTS.md`, `docs/*`.
- [x] Parity ledger generated (tree-sitter) — 3,515 API items, 2,059 tests.
- [x] Baseline verifier passes; `format`/`ameba`/`spec` green.

## Phase 1 — Core value and wire layer

### 1.1 Errors

- [x] `ParquetError` variant set (`src/parquet/errors.cr`). The `Result` alias is
      an intentional divergence — Crystal uses exceptions.
- [x] Upstream `errors.rs` test module ported to `spec/parquet/errors_spec.cr`.

### 1.2 Thrift compact protocol

- [x] `ElementType`, `FieldType` mappings.
- [x] `ReadThrift`/`WriteThrift`/`WriteThriftField` — mapped to protocol
      overloads and duck-typed `read_thrift`/`write_thrift` (intentional
      divergence; no associated consts in Crystal).
- [x] `ThriftCompactInputProtocol` + `ThriftSliceInputProtocol` +
  `ThriftReadInputProtocol`.
- [x] `ThriftCompactOutputProtocol`, `read_thrift_vec`, `validate_list_type`.
- [x] Upstream `parquet_thrift.rs` protocol tests ported to
  `spec/parquet/thrift_spec.cr`.

### 1.3 Thrift IDL macros (`parquet_macros.rs`)

- [x] Enum + all-empty-union support: `define_thrift_enum` and
  `define_thrift_union_all_empty` (`src/parquet/macros.cr`). Intentional
  divergence: upstream parses raw Thrift IDL; Crystal annotates native enums.
- [x] Struct support equivalent to `thrift_struct!`: `define_thrift_struct`
  (required/optional fields incl. empty structs, nested structs/enums, `bool`
  field encoding, all-required constructors, required-field validation,
  unknown-field skipping).
- [~] Mixed-union support equivalent to `thrift_union!` /
  `thrift_union_with_unknown!`. `LogicalType`/`ColumnOrder` read/write is
  hand-written; a generic union macro is still pending (needed for crypto
  metadata and encryption).
- [~] Parquet metadata IDL structs (`file/metadata/thrift/mod.rs`): done for
  `SchemaElement`, page headers, `KeyValue`, `PageEncodingStats`,
  `SortingColumn`, `SizeStatistics`, `ThriftColumnIndex`, `PageLocation`,
  `OffsetIndexMetaData`. Pending: encryption (`file/metadata/thrift/encryption.rs`).

### 1.4 Basic types

- [x] Thrift enums: `Type`, `ConvertedType`, `FieldRepetitionType`
  (`Repetition`), `Encoding`, `CompressionCodec`, `PageType`, `BoundaryOrder`,
  and the all-empty union `TimeUnit` (`src/parquet/basic.cr`).
- [x] `LogicalType` union (+ `DecimalType`, `TimestampType`/`TimeType`,
  `IntType`, `VariantType`, `GeometryType`, `GeographyType`,
  `EdgeInterpolationAlgorithm`), with `ConvertedType.from(LogicalType?)` and
  `LogicalType.from_str`.
- [x] `SortOrder`, `ColumnOrder` (incl. `column_order_for_type`).
- [x] `EncodingMask` (`try_new`, `new_from_encodings`, `set?`/`only?`/`all_set`,
  `encodings`, thrift reading).
- [ ] `Compression` enum, `CompressionCodec`<->`Compression` conversions, and
  `Compression` `Display`/`from_str`.

### 1.5 Data type model

- [x] Value types: `Int96`, `ByteArray`, `FixedLenByteArray`, `Decimal`, and the
  `AsBytes` trait (`src/parquet/data_type.cr`).
- [ ] `DataType` trait + `make_type!` (physical-type `TYPE`/`get_physical_type`/
  `get_type_size`, encode/decode) — depends on the column reader/writer.
- [ ] `AsBytes`/`SliceAsBytes` for primitives, `str`, `Vec<u8>`, `bool`. Until
      this lands, the column-index write path encodes primitives with
      `FromBytes.to_le_slice` (identical little-endian bytes).

### 1.6 Schema

- [x] `Type` (primitive/group), `BasicTypeInfo`, primitive/group builders
  (full validation), `check_contains` (`src/parquet/schema/types.cr`).
- [x] `ColumnPath`, `ColumnDescriptor`, `SchemaDescriptor`, def/rep-level tree.
- [x] Textual schema parser (`src/parquet/schema/parser.cr`).
- [x] Thrift <-> schema conversion (`src/parquet/schema/thrift.cr`).
- [x] Page-header thrift structs (`src/parquet/format.cr`).
- [ ] Schema printer (`schema/printer.rs`).
- [ ] Primitive builders (`schema/primitive.rs`). 4 upstream test modules.

### 1.7 Util

- [x] `PushBuffers` (`src/parquet/util/push_buffers.cr`).
- [ ] Bit utilities, cursor, page/bit readers (`util/`, remaining files).
- [ ] CRC32 checksum helpers.

### 1.8 Compression

- [ ] Codec dispatch + uncompressed/snappy/gzip/lzo/brotli/lz4/zstd paths.
- [ ] Select Crystal shards via `find-crystal-shards`; document any codec that
  must be an intentional divergence.

## Phase 2 — Encodings, columns, files

### 2.1 Encodings

- [ ] RLE / bit-packing hybrid (`encodings/rle.rs`).
- [ ] Levels (`encodings/levels.rs`).
- [ ] Plain, dictionary, delta, byte-stream-split, RLE codecs
  (`encodings/encoding`).
- [ ] ALP codec (`encodings/alp.rs`).
- [ ] 7 upstream test modules ported.

### 2.2 Column

- [ ] `ColumnDescriptor`, `ColumnReader`, `ColumnWriter`.
- [ ] `ColumnReaderImpl`/`ColumnWriterImpl`, page read/write.
- [ ] Writer properties + reader properties. 6 upstream test modules.

### 2.3 File

- [x] `LevelHistogram` (`src/parquet/file/metadata.cr`).
- [x] `ColumnChunkMetaData` + builder + thrift read/write
  (`src/parquet/file/metadata/column_chunk.cr`), plus thrift structs `KeyValue`,
  `PageEncodingStats`, `SortingColumn`, `SizeStatistics`.
- [x] `RowGroupMetaData` + builder + thrift read/write
  (`src/parquet/file/metadata/row_group.cr`).
- [x] `FileMetaData`, `ParquetMetaData`, and footer read/write
  (`src/parquet/file/metadata/parquet_metadata.cr`): `write_file_metadata`,
  `parquet_metadata_from_bytes`, ordinal backfill, and a
  `ParquetMetaDataWriter`/`ParquetMetaDataReader.decode_metadata` round-trip.
- [x] Statistics and value stats (`src/parquet/file/statistics.cr`):
  `Statistics`, `ValueStatistics<T>`, `from_thrift_page_stats`/
  `page_stats_to_thrift`.
- [x] Offset index (`src/parquet/file/page_index/offset_index.cr`):
  `PageLocation`, `OffsetIndexMetaData`.
- [x] Column index core (`src/parquet/file/page_index/column_index.cr`):
  `ColumnIndex`, `PrimitiveColumnIndex<T>`, `ThriftColumnIndex`, `FromBytes`.
- [x] `ByteArrayColumnIndex`, `ColumnIndexMetaData`, and `decode_column_index`
  (`src/parquet/file/page_index/{column_index,index_reader}.cr`), including the
  `WriteThrift` impls for both index types.
- [x] Page-index builders: `ColumnIndexBuilder`/`OffsetIndexBuilder`
  (`src/parquet/file/metadata.cr`) and `PageIndexProvider`/`RowGroupPageIndex`/
  `PageIndex`/`PageIndexBuilder` (`src/parquet/file/metadata/page_index.cr`).
  Tests `test_page_index_builder_partial_population`,
  `..._empty_to_none`, `test_rebuild_page_index` ported.
- [x] `ParquetMetaDataBuilder`, `ParquetMetaData` page-index plumbing
  (`page_index`/`page_index_for_row_group`/`into_builder`/`set_page_index`),
  `ParquetMetaDataOptions`/`ParquetStatisticsPolicy`
  (`src/parquet/file/metadata/{parquet_metadata,options}.cr`), plus
  `decode_offset_index`/`acc_range` (`index_reader.cr`).
- [x] `ParquetMetaDataPushDecoder` + `MetadataParser` + `parse_page_index` +
  `range_for_page_index` (`src/parquet/file/metadata/{push_decoder,parser}.cr`),
  `FooterTail` (`footer_tail.cr`), `PageIndexPolicy` (`reader.cr`), `DecodeResult`
  (`src/parquet/decode_result.cr`), and `PushBuffers`
  (`src/parquet/util/push_buffers.cr`).
- [ ] Pull-based `ParquetMetaDataReader`/`ParquetMetaDataWriter` fetch/suffix IO
  machinery (`ChunkReader`; async/object-store).
- [ ] Serialized reader/writer (footer + page headers).
- [ ] Reader/writer (`file/reader`, `file/writer`).
- [ ] Bloom filter (`bloom_filter/`).
- [ ] 12 upstream test modules ported.

### 2.4 Record API

- [ ] `Triplet`, `Field`, `Record` traits.
- [ ] `RowIter`/`RowAccessor`, record reader/writer (`record/`).
- [ ] 3 upstream test modules + `parquet/tests` record suites.

### 2.5 CLI

- [ ] `parquet` CLI (`bin/`, `cli` feature) with `clap`-equivalent arg parsing.

## Phase 3 — Advanced / optional

### 3.1 Variant crates

- [ ] `parquet-variant` (Variant binary encoding).
- [ ] `parquet-variant-compute`.
- [ ] `parquet-variant-json`.

### 3.2 Geospatial

- [ ] GeoParquet GEOMETRY/GEOGRAPHY logical types + bounding statistics.

### 3.3 Derive macros

- [ ] `parquet_derive` (Crystal macros) + `parquet_derive_test`.

### 3.4 Encryption

- [ ] Modular encryption (`file/metadata/thrift/encryption` consumers,
  `encryption/`). Map `ring` AEAD to Crystal crypto (or document divergence).

### 3.5 Arrow interop (deferred)

- [ ] `parquet/src/arrow/` — blocked on a Crystal Arrow implementation.
      Consume the sibling `../arrow-cr` port once it exposes arrays, schema,
      and record batches. Track here; keep out of the critical path.

### 3.6 Async / object_store (deferred)

- [ ] `async_reader`, `async_writer`, `object_store` integration. Rust uses
  tokio/futures; Crystal has fibers. Likely an intentional divergence — record
  the mapping decision before porting.

## Intentional divergences

- Errors: Rust `Result<T, E>` is mapped to Crystal exceptions. Fallible
  functions raise `ParquetError` subclasses instead of returning a result type;
  `ParquetError::External(Box<dyn Error>)` maps to `ExternalError` holding an
  `Exception`. Display strings are preserved exactly.
- Thrift traits: `ReadThrift`, `WriteThrift`, and `WriteThriftField` are Rust
  traits with an associated `ELEMENT_TYPE` constant. Crystal has no associated
  constants, so value dispatch lives on `ThriftCompactOutputProtocol` /
  `Thrift.*` overloads, and IDL types expose duck-typed
  `self.read_thrift(prot)` / `#write_thrift(prot)` methods.
- `ThriftProtocolError`: not modeled as a separate type; protocol routines raise
  the corresponding `ParquetError` directly with identical messages.
- Thrift IDL macros: upstream `parquet_macros.rs` parses raw Thrift IDL text at
  macro-expansion time. Crystal macros cannot consume IDL tokens, so
  `define_thrift_enum` / `define_thrift_union_all_empty` annotate a natively
  declared Crystal `enum` and `define_thrift_struct` annotates a `struct` block,
  synthesizing `read_thrift`, `write_thrift`, and `thrift_element_type` with
  identical wire behavior. Upstream's generated `VARIANTS`/`MAX_DISCRIMINANT`
  constants are not provided yet.
- `from_str` conversions use `Enum.parse?`/`upcase`; for `Encoding` this accepts
  mixed-case spellings in addition to upstream's all-upper/all-lower forms.
- `LogicalType`: Rust's payload-carrying enum is modelled as a sealed
  `LogicalType` class hierarchy; the `_Unknown { field_id }` variant is
  `LogicalType::UnknownField`. `EdgeInterpolationAlgorithm`'s `_Unknown(i32)`
  variant is retained by wrapping the i32 code in a value struct.
  `GeographyType::algorithm` is `GeographyType#algorithm_or_default`.
- `schema::Type`: upstream's enum (`Type::PrimitiveType`/`Type::GroupType`) is a
  sealed class hierarchy. Predicates use `?` (`primitive?`, `group?`,
  `schema?`, `optional?`, `has_repetition?`, `has_id?`); `TypePtr`/`Arc<Type>`
  is just `Type`. To keep upstream error strings intact, `LogicalType` and the
  logical-type structs expose `rust_debug` reproducing Rust `#[derive(Debug)]`.
  `schema/types` lives under `Parquet::Schema` (upstream `schema::types`).
- `SchemaElement` lives under `Parquet::Schema` (upstream `file::metadata::thrift`)
  to avoid a require cycle; it carries an owned `String` name instead of `<'a> str`.
- Page-header thrift structs live under `Parquet::Format` (upstream
  `file::metadata::thrift`); the `read_*_without_stats` readers are
  `Parquet::Format` module methods.
- Metadata types (`ColumnChunkMetaData`/`RowGroupMetaData`) expose mutable
  `property` fields (upstream keeps them private and mutates from the crate);
  `Rust Result`-returning builders return the value or raise.
- `Decimal`: upstream's `Int32`/`Int64`/`Bytes` enum is a single Crystal struct
  carrying the unscaled bytes; variant identity is the byte width. Equality and
  accessors match upstream (`precision`, `scale`, `data`).
- `FixedLenByteArray`: Rust `Deref`/`From` are explicit delegating methods and
  constructors in Crystal.
- `ByteArray::is_empty` maps to Crystal `empty?`.
- `ValueStatistics::is_*`/`ColumnIndex::is_null_page` map to `?` predicates
  (`min_max_deprecated?`, `null_page?`, ...); `Statistics` is a single class over
  a `ValueStatistics` union rather than an enum.
- `ColumnOrder`: upstream's enum with a `TYPE_DEFINED_ORDER(SortOrder)` payload
  is a Crystal struct carrying a `Kind` + inner `SortOrder`; unit variants are
  constants and `TYPE_DEFINED_ORDER(s)` is `ColumnOrder.type_defined_order(s)`.
  `SortOrder::is_signed` is `SortOrder#signed?`. The deprecated
  `ColumnOrder::sort_order_for_type` alias is not provided.
- `PrimitiveColumnIndex<T>` delegates `ColumnIndex` methods rather than using
  Rust `Deref`; `OffsetIndexMetaData::try_from_fast` fast-path decoder is omitted
  (the macro `read_thrift` decodes the same wire form).
- `ByteArrayColumnIndex`: same delegation divergence as
  `PrimitiveColumnIndex<T>`; offsets are `Int32` instead of `usize`.
- `ColumnIndexMetaData`: upstream's enum with eight variants
  (`BOOLEAN`/`INT32`/.../`FIXED_LEN_BYTE_ARRAY`) is a struct carrying the
  physical `Type` plus the inner index; recover the variant via `kind` and
  `inner.as(...)`. `is_sorted` → `sorted?`, `is_null_page` → `null_page?`, and
  `get_boundary_order` → `boundary_order` (non-optional); `num_pages` returns
  `Int32`. `decode_column_index` lives as `PageIndex.decode_column_index`
  (`index_reader.cr`); `decode_offset_index`/`acc_range` are deferred to the
  file-reader chunk.
- Column-index `WriteThrift`: Rust `WriteThrift::ELEMENT_TYPE` associated consts
  are not modelled; `write_thrift` is an instance method and the min/max
  `list<binary>` fields are encoded by hand exactly as upstream. Primitive
  min/max bytes are produced via `FromBytes.to_le_slice` (see Phase 1.5).
- `PageIndexProvider`: Rust trait with `as_any` downcasting is an abstract class;
  `as_any` is not modelled. Predicates use `?` (`has_column_indexes` →
  `column_indexes?`, `has_offset_indexes` → `offset_indexes?`, `is_complete` →
  `complete?`). `PageIndex: HeapSize` (`memory.rs`) is deferred.
- `PageIndexBuilder`: `empty_index`/`is_empty_index` are private instance methods
  (`empty_index`/`index_empty?`); the Rust `From<PageIndex>` impl maps to
  `PageIndexBuilder.new_from` (and `PageIndex#into_builder`). `ColumnIndexBuilder`
  `valid` → `valid?`; `set_boundary_order` keeps its upstream name under an
  ameba `AccessorMethodName` disable.
- `ParquetMetaData`: `page_index` is a plain `PageIndexProvider?` (no
  `Arc`/`as_any`); equality compares page indexes by value for `PageIndex` and by
  reference otherwise. `set_page_index` is public (upstream `pub(crate)`).
  `memory_size` (`HeapSize`) and the encryption `file_decryptor`
  plumbing are deferred.
- `ParquetMetaDataBuilder`: holds the fields directly rather than wrapping
  `ParquetMetaData`; the Rust `From<ParquetMetaData>` impl maps to
  `new_from_metadata`. `set_row_groups`/`set_page_index` keep upstream names under
  ameba disables.
- `ParquetMetaDataOptions`: Rust `with_*(mut self) -> Self` mutates and returns
  `self`; `encoding_stats_as_mask` → `encoding_stats_as_mask?`;
  `ParquetStatisticsPolicy::SkipExcept(Arc<HashSet>)` is
  `SkipExcept(Set(Int32))`, `KeepAll`/`SkipAll` are constants, and `is_skip` →
  `skip?`. `set_*` keep upstream names under ameba disables; `Default`/`new` map
  to the default initializer.
- `DecodeResult<T>`: Rust's generic enum is a generic class carrying a `Kind`
  plus the payload (`needs_data?`/`data?`/`finished?`, `ranges`, `data`).
- `FooterTail`: `is_encrypted_footer` → `encrypted_footer?`;
  `try_new`/`try_from` both take a `Bytes` (Crystal has no fixed-size array).
- `PushBuffers`: the `ChunkReader`/`Length`/`Display`/`Read` impls and the
  arrow-only `buffered_bytes`/`clear_ranges` are not ported; `has_range` →
  `has_range?`; `get_bytes` returns a `Bytes` slice and raises
  `NeedMoreDataRangeError`.
- `ParquetMetaDataPushDecoder`: `PageIndexPolicy` is an enum with
  `from(Bool)`; the `try_new_with_footer_tail`/`try_new_with_metadata`
  constructors (upstream `pub(crate)`) are public; encryption
  (`with_file_decryption_properties`) is not ported, and an encrypted footer
  raises as upstream does without the feature. `MetadataParser` threads
  `metadata_options` through but statistics-skipping/schema overrides are not
  applied yet.

## Notes

- Upstream test oracle: `cargo test -p parquet` from `vendor/arrow-rs`
  (Rust 1.99 toolchain available locally).
- Golden fixtures: `vendor/arrow-rs/parquet-testing/data`.
- Parity scripts must be run from the `crystal_forge` skill:
  `/Users/dominic/.agents/skills/crystal_forge/skills/cross-language-crystal-parity`.
- Environment: the checkout is on exFAT, which emits macOS AppleDouble `._*`
  sidecars; the `Makefile` strips them before format/lint/test (a `._foo_spec.cr`
  matches crystal's `*_spec.cr` glob). If a git pack op prints
  `non-monotonic index .../._pack-*.idx`, delete the sidecars.
