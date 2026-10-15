# parquet-cr Parity Roadmap

Curated, branch-sized feature roadmap. Generated manifests live in
`plans/inventory/`; the day-to-day ledger is
`plans/inventory/rust_port_inventory.tsv`.

- Upstream: <https://github.com/apache/arrow-rs>
- Pinned revision: `60.0.0`
  (`ef1fa157977633f0ba21aa921f9ef3d5c669235b`)
- Vendored at: `vendor/arrow-rs` (submodule)
- Initial scope: `parquet` (core), then `parquet-variant*`, `parquet-geospatial`,
  `parquet_derive*`
- Deferred: `parquet/src/arrow/` (Arrow interop) — depends on a Crystal Arrow
  implementation, being built separately in the sibling repo `../arrow-cr`
  (same `60.0.0` pin).

Status legend: `[ ]` not started, `[~]` in progress, `[x]` complete,
`[!]` intentional divergence.

Do not weaken upstream tests or fixtures. Port behavior, not style. Preserve
numeric widths, half-open ranges (`start..end` → `start...end`), and the on-disk
little-endian layout.

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
  `spec/parquet/thrift_spec.cr` (enum/union round-trips land with 1.4 `basic`).

### 1.3 Thrift IDL macros (`parquet_macros.rs`)

- [x] Enum + all-empty-union support: `define_thrift_enum` and
  `define_thrift_union_all_empty` (`src/parquet/macros.cr`). Intentional
  divergence: upstream parses raw Thrift IDL; Crystal annotates native enums.
- [x] Struct support equivalent to `thrift_struct!`: `define_thrift_struct`
  (required/optional scalar fields, nested structs/enums, `bool` field encoding,
  required-field validation, unknown-field skipping).
- [~] Mixed-union support equivalent to `thrift_union!` /
  `thrift_union_with_unknown!`. `LogicalType` read/write is hand-written; a
  generic union macro is still pending (needed for `ColumnOrder`, crypto
  metadata, encryption).
- [ ] Parquet metadata IDL structs (`file/metadata/thrift/mod.rs`).
- [ ] Encryption IDL structs (`file/metadata/thrift/encryption.rs`, gated).

### 1.4 Basic types

- [x] Thrift enums: `Type`, `ConvertedType`, `FieldRepetitionType`
  (`Repetition`), `Encoding`, `CompressionCodec`, `PageType`, `BoundaryOrder`,
  and the all-empty union `TimeUnit` (`src/parquet/basic.cr`). `Display`
  (variant name) and `from_str` ported; enum round-trips covered in
  `spec/parquet/basic_spec.cr` and `spec/parquet/thrift_spec.cr`.
- [x] `LogicalType` union (+ `DecimalType`, `TimestampType`/`TimeType`,
  `IntType`, `VariantType`, `GeometryType`, `GeographyType`,
  `EdgeInterpolationAlgorithm`), with `ConvertedType.from(LogicalType?)` and
  `LogicalType.from_str`. Modelled as a sealed class hierarchy; `_Unknown`
  maps to `UnknownField`.
- [ ] `Compression` enum, `CompressionCodec`<->`Compression` conversions, and
  `Compression` `Display`/`from_str`.
- [x] `SortOrder`, `ColumnOrder` (incl. `column_order_for_type`, sort-order
  resolution, union read/write). `TypeDefinedOrder` is a thrift empty struct and
  is folded into `ColumnOrder::Kind::TypeDefinedOrder`.
- [ ] `EncodingMask`.
- [ ] `Statistics` and typed statistics.

### 1.5 Data type model

- [x] Value types: `Int96`, `ByteArray`, `FixedLenByteArray`, `Decimal`, and the
  `AsBytes` trait (`src/parquet/data_type.cr`), with upstream `data_type.rs`
  tests in `spec/parquet/data_type_spec.cr`.
- [ ] `DataType` trait + `make_type!` (physical-type `TYPE`/`get_physical_type`/
  `get_type_size`, encode/decode) — depends on the column reader/writer.
- [ ] `AsBytes`/`SliceAsBytes` for primitives, `str`, `Vec<u8>`, `bool`, and
  `SliceAsBytes` (used by column encode/decode).

### 1.6 Schema

- [x] `Type` (primitive/group), `BasicTypeInfo`, the primitive/group builders
  (including all logical/converted/physical validation, decimal precision, and
  `FILE` group validation), and `check_contains` (`src/parquet/schema/types.cr`).
  Upstream `schema/types.rs` core tests ported to
  `spec/parquet/schema/types_spec.cr`.
- [x] `ColumnPath`, `ColumnDescriptor`, `SchemaDescriptor`, and the definition/
  repetition-level tree builder (`src/parquet/schema/types.cr`). Upstream
  `test_column_descriptor`, `test_schema_descriptor`, `test_schema_build_tree_*`,
  and `ColumnPath` doc-example characterization ported.
- [x] Textual schema parser (`src/parquet/schema/parser.cr`):
  `Parquet::Schema.parse_message_type`, tokenizer, and all upstream `parser.rs`
  tests ported to `spec/parquet/schema/parser_spec.cr`.
- [x] Thrift <-> schema conversion (`src/parquet/schema/thrift.cr`):
  `SchemaElement`, `write_schema`/`write_schema_helper`, `num_nodes`,
  `parquet_schema_from_array`/`schema_from_array_helper`, `check_logical_type`,
  and the `schema_to_buf`/`buf_to_schema_list`/`roundtrip_schema` helpers.
  Upstream `test_schema_type_thrift_conversion*` and `test_schema_from_thrift_*`
  ported to `spec/parquet/schema/thrift_spec.cr`.
- [x] Page-header thrift structs (`src/parquet/format.cr`): `PageHeader`,
  `DataPageHeader`, `DataPageHeaderV2`, `DictionaryPageHeader`,
  `IndexPageHeader`, `PageStatistics`, and the `read_*_without_stats` readers.
  Upstream `malformed_bool_field_returns_error_not_panic` and
  `malformed_data_page_v2_bool_field_returns_error_not_panic` ported to
  `spec/parquet/format_spec.cr`.
- [ ] Remaining metadata thrift structs (`file/metadata/thrift/mod.rs`:
  `FileMetaData`, `RowGroup`, `ColumnChunk`, `ColumnMetaData`, `Statistics`,
  `SizeStatistics`, `BoundingBox`, `GeospatialStatistics`).
- [ ] Schema printer (`schema/printer.rs`).
- [ ] Primitive builders (`schema/primitive.rs`). 4 upstream test modules.

### 1.7 Util

- [ ] Bit utilities, cursor, page/bit readers (`util/`, 11 files).
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

- [ ] `ParquetMetaData`, `FileMetaData`, `RowGroupMetaData`,
  `ColumnChunkMetaData`, builders (`file/metadata`).
- [ ] Serialized reader/writer (footer + page headers).
- [ ] Statistics and value stats (`file/statistics`).
- [ ] Reader/writer (`file/reader`, `file/writer`).
- [ ] Page index (column index, offset index).
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
  declared Crystal `enum`, synthesizing `read_thrift`, `write_thrift`, and
  `thrift_element_type` with identical wire behavior. Upstream's generated
  `VARIANTS`/`MAX_DISCRIMINANT` constants are not provided yet.
- `from_str` conversions use `Enum.parse?`/`upcase`; for `Encoding` this accepts
  mixed-case spellings in addition to upstream's all-upper/all-lower forms.
- `LogicalType`: Rust's payload-carrying enum is modelled as a sealed
  `LogicalType` class hierarchy; the `_Unknown { field_id }` variant is
  `LogicalType::UnknownField`. `EdgeInterpolationAlgorithm`'s `_Unknown(i32)`
  variant is retained by wrapping the i32 code in a value struct.
  `GeographyType::algorithm` is `GeographyType#algorithm_or_default` because a
  Crystal accessor cannot share the field's name.
- `schema::Type`: upstream's enum (`Type::PrimitiveType`/`Type::GroupType`) is a
  sealed class hierarchy. Predicates use `?` (`primitive?`, `group?`,
  `schema?`, `optional?`, `has_repetition?`, `has_id?`); `TypePtr`/`Arc<Type>`
  is just `Type`. To keep upstream error strings intact, `LogicalType` and the
  logical-type structs expose `rust_debug` reproducing Rust `#[derive(Debug)]`.
  `schema/types` lives under `Parquet::Schema` (upstream `schema::types`).
- `SchemaElement` lives under `Parquet::Schema` (upstream `file::metadata::thrift`)
  to avoid a require cycle; it carries an owned `String` name instead of `<'a> str`.
  `define_thrift_struct` makes every generated constructor parameter required
  (upstream struct literals specify all fields), since a required field may
  follow optional ones (as in `SchemaElement`).
- Page-header thrift structs live under `Parquet::Format` (upstream
  `file::metadata::thrift`); the `read_*_without_stats` readers are
  `Parquet::Format` module methods.
- `Decimal`: upstream's `Int32`/`Int64`/`Bytes` enum is a single Crystal struct
  carrying the unscaled bytes; variant identity is the byte width. Equality and
  accessors match upstream (`precision`, `scale`, `data`).
- `FixedLenByteArray`: Rust `Deref`/`From` are explicit delegating methods and
  constructors in Crystal.
- `ByteArray::is_empty` maps to Crystal `empty?`.
- `ColumnOrder`: upstream's enum with a `TYPE_DEFINED_ORDER(SortOrder)` payload
  is a Crystal struct carrying a `Kind` + inner `SortOrder`; unit variants are
  constants (`ColumnOrder::IEEE_754_TOTAL_ORDER`, `...::UNDEFINED`, `...::UNKNOWN`)
  and `TYPE_DEFINED_ORDER(s)` is `ColumnOrder.type_defined_order(s)`.
  `SortOrder::is_signed` is `SortOrder#signed?`. The deprecated
  `ColumnOrder::sort_order_for_type` alias is not provided.

## Notes

- Upstream test oracle: `cargo test -p parquet` from `vendor/arrow-rs`
  (Rust 1.99 toolchain available locally).
- Golden fixtures: `vendor/arrow-rs/parquet-testing/data`.
- Parity scripts must be run from the `crystal_forge` skill:
  `/Users/dominic/.agents/skills/crystal_forge/skills/cross-language-crystal-parity`.
