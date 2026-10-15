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

### 1.3 Thrift IDL structs

- [ ] `thrift_struct!`/`thrift_union!`/`thrift_enum!` macro system mirroring
  `parquet_macros.rs`.
- [ ] Parquet metadata IDL structs (`file/metadata/thrift/mod.rs`).
- [ ] Encryption IDL structs (`file/metadata/thrift/encryption.rs`, gated).

### 1.4 Basic types

- [ ] `Type`, `ConvertedType`, `Repetition`, `LogicalType`, `Encoding`,
  `CompressionCodec`, `PageType`, `ColumnOrder`, `TypeDefinedOrder`.
- [ ] `Statistics` and typed statistics (`basic.rs`, 1 test module).

### 1.5 Data type model

- [ ] `DataType` enum and accessors (`data_type.rs`).
- [ ] `ByteArray`, `FixedLenByteArray`, `Int96` semantics.

### 1.6 Schema

- [ ] `Type`/`TypePtr`/`ColumnDescriptor`/`SchemaDescriptor` (`schema/types.rs`).
- [ ] Thrift → schema parser (`schema/parser.rs`).
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

## Notes

- Upstream test oracle: `cargo test -p parquet` from `vendor/arrow-rs`
  (Rust 1.99 toolchain available locally).
- Golden fixtures: `vendor/arrow-rs/parquet-testing/data`.
- Parity scripts must be run from the `crystal_forge` skill:
  `/Users/dominic/.agents/skills/crystal_forge/skills/cross-language-crystal-parity`.
