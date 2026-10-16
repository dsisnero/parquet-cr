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
  `ParquetMetaDataBuilder`/`ParquetMetaDataOptions` plumbing, the sans-IO
  `ParquetMetaDataPushDecoder`, the synchronous `ChunkReader`-driven
  `ParquetMetaDataReader`, and the   `ParquetMetaDataWriter`/
  `ThriftMetadataWriter` (footer + page-index serialization) are done, and the
  `util` bit layer (`BitReader`/`BitWriter`), the RLE/bit-packing hybrid
  (`RleEncoder`/`RleDecoder`), level encoding (`LevelEncoder`), all encodings
  (PLAIN, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY,
  dictionary, byte-stream-split, RLE value, ALP), and the `Encoder`/`Decoder`
  interfaces + `get_encoder`/`get_decoder` registries are done.
- Next up (take the first unchecked item):
  1. Column reader/writer (`column/`), then serialized/file reader+writer, then
     record API and bloom filter.
  2. Deferred: async/object-store suffix fetchers and encryption are intentional
     divergences (recorded).
- Progress by counts (ledger): 843 `ported`, 108 `intentional_divergence`,
  1 `skipped`, 2563 `missing` (of 3515 rows).

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
  page-index builders, the `ParquetMetaData` page-index plumbing, the sans-IO
  `ParquetMetaDataPushDecoder`, the synchronous `ChunkReader`-driven
  `ParquetMetaDataReader`, and the `ParquetMetaDataWriter`/`ThriftMetadataWriter`
  done; serialized/file reader+writer, bloom filter pending.
- Phase 2.1 encodings: all encodings, the `Encoder`/`Decoder` interfaces, and
  the `get_encoder`/`get_decoder` registries done.
- Phases 2.2 column, 2.4 record, 2.5 CLI: pending.
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
- [x] `BitReader`/`BitWriter` + helpers (`src/parquet/util/bit_util.cr`).
- [x] `Interner`/`Storage` (`src/parquet/util/interner.cr`) used by the
      dictionary encoder.
- [ ] Bit packing (`util/bit_pack.rs`), cursor, page readers (`util/` remaining).
- [ ] CRC32 checksum helpers.

### 1.8 Compression

- [ ] Codec dispatch + uncompressed/snappy/gzip/lzo/brotli/lz4/zstd paths.
- [ ] Select Crystal shards via `find-crystal-shards`; document any codec that
  must be an intentional divergence.

## Phase 2 — Encodings, columns, files

### 2.1 Encodings

- [x] RLE / bit-packing hybrid (`src/parquet/encodings/rle.cr`):
  `RleEncoder`/`RleDecoder`.
- [x] Levels (`src/parquet/encodings/levels.cr`): `LevelEncoder`
  (v1_streaming/v2_streaming/put_with_observer/put_n_with_observer/consume/
  flush_to).
- [x] Plain (`src/parquet/encodings/plain.cr`): `PlainEncoder`/`PlainDecoder`
  for all physical types (put/get/get_spaced/skip).
- [x] DELTA_BINARY_PACKED (`src/parquet/encodings/delta_bit_pack.cr`):
  `DeltaBitPackEncoder`/`DeltaBitPackDecoder` for INT32/INT64.
- [x] DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY
  (`src/parquet/encodings/delta_byte_array.cr`):
  `DeltaLengthByteArrayEncoder`/`Decoder` and
  `DeltaByteArrayEncoder(T)`/`DeltaByteArrayDecoder(T)`.
- [x] Dictionary codec (`src/parquet/encodings/dict.cr`): `DictEncoder(T)` and
  `DictDecoder(T)`, plus `KeyStorage(T)`; `util/interner.cr`.
- [x] BYTE_STREAM_SPLIT (`src/parquet/encodings/byte_stream_split.cr`):
  `ByteStreamSplitEncoder(T)`/`Decoder(T)` and
  `VariableWidthByteStreamSplitEncoder(T)`/`Decoder(T)`.
- [x] RLE value (`src/parquet/encodings/rle_value.cr`):
  `RleValueEncoder(T)`/`RleValueDecoder(T)` for BOOLEAN.
- [x] ALP (`src/parquet/encodings/alp.cr`): `AlpEncoder(F)`/`AlpDecoder(F)` for
  FLOAT/DOUBLE, plus `AlpHeader`/`AlpInfo`/`ForInfo(F)`/`AlpEncodedVectorView(F)`
  and the `Alp` sampling/encoding helpers.
- [x] `Encoder`/`Decoder` interfaces and the `get_encoder`/`get_decoder`
  registries (`src/parquet/encodings/traits.cr`, `registry.cr`), plus the
  `physical_type(T)` helper.
- [x] 7+ upstream test modules ported (RLE, levels, plain, delta binary/length/
  byte-array, dictionary, byte-stream-split, RLE value, ALP, registries).

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
- [x] `ChunkReader`/`BytesChunkReader`/`FileChunkReader`
  (`src/parquet/file/chunk_reader.cr`) and the synchronous, `ChunkReader`-driven
  `ParquetMetaDataReader` (`reader.cr`).
- [x] `ParquetMetaDataWriter`/`ThriftMetadataWriter` page-index footer
  serialization (`src/parquet/file/metadata/writer.cr`) and `TrackedWrite`/
  `file_magic` (`src/parquet/file/tracked_write.cr`).
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
- `ChunkReader`: Rust's `Length`/`ChunkReader` traits are one abstract class
  (`len`/`get_bytes`/`get_read`); the associated `T: Read` becomes `IO`, and
  `Bytes`/`File` are wrapped as `BytesChunkReader`/`FileChunkReader`.
- `ParquetMetaDataReader`: the async `MetadataFetch`/`MetadataSuffixFetch`
  loaders (`load_*`, `try_load*`, `with_prefetch_hint`, `get_prefetch_size`) and
  the encryption `with_decryption_properties` are not ported; `has_metadata` →
  `has_metadata?`, `parse_metadata` is public (upstream private), `metadata_size`
  returns `Int32?`, and `decode_schema` is implemented via a full metadata
  decode.
- `encodings::delta_byte_array`: `DeltaByteArrayEncoder(T)`/
  `DeltaByteArrayDecoder(T)` with `T.class` (BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY);
  `DeltaByteArrayDecoder#prefix_lengths` is exposed for the negative-length
  check; the decoder `get` constructs values via `ByteArray`/`FixedLenByteArray`
  rather than Rust `set_from_bytes`.
- `encodings::delta_bit_pack`: `DeltaBitPackEncoder(T)`/`DeltaBitPackDecoder(T)`
  with `T.class` (INT32/INT64 only, enforced by construction); the decoder `get`
  reads each mini-block's packed deltas via `BitReader#get_batch`; `skip`
  decodes into a scratch buffer instead of the upstream fixed skip buffer;
  `BitReader#get_aligned_bytes` is now ported.
- `encodings::plain`: Rust's `DataType`/`Encoder`/`Decoder` trait dispatch is
  `PlainEncoder(T)`/`PlainDecoder(T)` with `T.class` and macro branches per
  physical value type; `encode_all` is added as a test/characterization helper;
  the `get_encoder`/`get_decoder` registries and `put_spaced` are not ported yet.
- `encodings::levels`: the Rust `LevelEncoder::Rle`/`RleV2` enum is a struct
  with a `v2` flag plus the shared `RleEncoder`; observer callbacks are Crystal
  blocks; `flush_to` takes a block rather than a Rust `FnOnce`.
- `encodings::rle`: `FromBitpacked` generic dispatch becomes `T.class`
  (`get(T.class)`/`get_batch(T.class, buffer)`); the decoder's batch/dict paths
  read values with the scalar path (no `unpack`/`index_buf` chunking);
  `is_accumulating_rle` → `accumulating_rle?`, `is_empty` → `empty?`,
  `RleEncoder#flush_buffer_mut` is not ported.
- `util::bit_util`: Rust's `FromBytes`/`FromBitpacked` traits (with associated
  `BIT_CAPACITY`/`BATCH_SIZE` consts and SIMD `unpack_batch`) are replaced by
  explicit `T.class` dispatch; `get_batch` is a scalar loop (no `unpack8/16/32/
  64`); `put_aligned`/`put_aligned_offset` encode via `IO::ByteFormat::
  LittleEndian` rather than the `AsBytes` trait; `BitWriter#flush_buffer_mut`
  and `BitReader#get_aligned_bytes` are not ported.
- `util::interner`: Rust's `Storage` trait (with `Key`/`Value` associated types)
  is an abstract generic class `Storage(Key, Value)`; the `hashbrown::HashTable`
  dedup table is a `Hash(Bytes, Key)` keyed on `value.as_bytes`, preserving the
  upstream byte-wise comparison (identical NaN bit patterns dedup);
  `Interner#estimated_memory_size` and `Storage#estimated_memory_size` use
  element counts rather than Rust's retained `capacity()` (`Array` exposes no
  capacity), and the dedup table contributes a fixed preallocation estimate.
  `Interner::into_inner` (arrow-only) is not ported.
- `encodings::dict`: `DictEncoder(T)`/`DictDecoder(T)` with `T.class` and
  `KeyStorage(T)`; `DictDecoder#set_dict` accepts any object responding to
  `values_left`/`get` (in practice a `PlainDecoder(T)`) rather than a
  `Box<dyn Decoder<T>>`; `DictEncoder#put_one` is inlined into `put`;
  `is_sorted` → `sorted?`. `DictDecoder#get`/`#skip` leave `num_values`
  unchanged, matching upstream. `estimated_memory_size` is count-based as above.
- `data_type`: primitives now implement `AsBytes` (`as_bytes` for `Bool`,
  `Int32`, `Int64`, `Float32`, `Float64`, little-endian, matching upstream's
  native-byte `gen_as_bytes!`); `DataType`/`SliceAsBytes` for primitives,
  `str`, and `Vec<u8>` remain pending. `dict_encoding_size` /
  `variable_length_bytes` are realized per-type inside
  `DictEncoder::KeyStorage<T>` rather than on a `ParquetValueType` trait.
- `encodings::byte_stream_split`: `ByteStreamSplitEncoder(T)`/`Decoder(T)` and
  `VariableWidthByteStreamSplitEncoder(T)`/`Decoder(T)` with `T.class`; the
  constant `split_streams_const`/`join_streams_const` and `_variable` kernels
  are a single runtime `type_size` routine (byte-identical layout; the upstream
  block-4 loop only reorders the same writes); the fixed-width decoder joins
  into a scratch buffer and decodes each value with `IO::ByteFormat::LittleEndian`
  instead of a memory reinterpret; unsupported types raise as upstream.
- `encodings::rle_value`: `RleValueEncoder(T)`/`RleValueDecoder(T)` with
  `T.class` (BOOLEAN only, enforced by construction); `RleValueDecoder#get`
  decodes into a scratch `Array` before copying into the output buffer (the
  Crystal `RleDecoder#get_batch` only accepts an owning `Array`); the
  `DEFAULT_RLE_BUFFER_LEN` reservation is inlined as a `[0; 4]` length prefix.
- `encodings::alp`: `AlpFloat`/`AlpExact` traits and their associated types are
  replaced by `F.class`-dispatched `Alp` module methods (`Alp.encode_value`,
  `Alp.exact_wrapping_add`, ...); the exact integer types (`u32`/`u64`) are
  carried uniformly as `UInt64` with width-masked wrapping arithmetic so f32
  wraps at 32 bits; `AlpHeader#serialize` returns `Bytes` (not `[u8; 7]`);
  `AlpEncodedVectorView`'s `*_range` methods are inlined into `AlpDecoder`
  (slice arithmetic at `load_current_vector`); `Scratch`/`StreamingPage` use
  element counts for `estimated_memory_size` (no `Array` capacity); the
  `is_better`/`rank` helpers become `better?`/`rank`.
- `encodings::traits`/`registry`: the `Encoder`/`Decoder` traits become abstract
  generic classes (with `put_spaced`/`get_spaced` as concrete base methods); the
  `GetEncoder`/`GetDecoder` traits and `get_encoder`/`get_decoder` free functions
  become `T.class`-dispatched `Encodings.get_encoder`/`get_decoder` module
  methods returning `Encoder(T)`/`Decoder(T)`; `PhysicalType` is
  `Encodings.physical_type(T)`. Because Crystal instantiates generic classes per
  concrete type, combinations upstream would construct-but-fail-later (e.g.
  `DeltaBitPackEncoder<T>` for a non-integer `T`) raise `ParquetError` directly.
  `DeltaLengthByteArrayEncoder`/`Decoder` were made generic over `T` to match the
  upstream registry's cross-type encoder construction.
- `ParquetMetaDataWriter`/`ThriftMetadataWriter`: the `MetadataObjectWriter`
  encryption-dispatch layer is not modeled; the non-encryption write logic is
  inlined (`write_thrift_object`, `Metadata.write_file_metadata`), and the
  `TrackedWrite`/`file_magic` helpers live in `src/parquet/file/`. `TrackedWrite`
  exposes only `bytes_written`/`write` (no `inner*`/`into_inner`/`flush`/
  `write_vectored`; `write_all` comes from `IO`). Row groups are shallow-copied,
  so writing sets index offsets on the source `ColumnChunkMetaData` objects;
  magic is always `PAR1` (encrypted-footer magic is not produced).

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
