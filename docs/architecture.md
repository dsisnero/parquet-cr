# Architecture

`parquet-cr` is a Crystal port of the `parquet` crate from
[apache/arrow-rs](https://github.com/apache/arrow-rs). The upstream crate is the
source of truth for behavior; this project follows its module layout closely so
that files can be compared one-to-one.

## Module layout

Upstream `parquet/src` and companion crates map to Crystal as follows:

| Upstream (Rust)      | Crystal                     | Responsibility                          |
| -------------------- | --------------------------- | --------------------------------------- |
| `errors.rs`          | `src/parquet/errors.cr`     | Error and result types                  |
| `basic.rs`           | `src/parquet/basic.cr`      | Logical/physical types, statistics      |
| `data_type.rs`       | `src/parquet/data_type.cr`  | Column data type model                  |
| `schema/`            | `src/parquet/schema/`       | Schema descriptors and parser           |
| `encodings/`         | `src/parquet/encodings/`    | RLE, dictionary, level, and ALP codecs  |
| `column/`            | `src/parquet/column/`       | Column reader/writer and descriptors    |
| `file/`              | `src/parquet/file/`         | File metadata, reader, writer           |
| `record/`            | `src/parquet/record/`       | Record (row) reader and writer API      |
| `geospatial/`        | `src/parquet/geospatial/`   | Geospatial statistics (bbox/accumulator) |
| `parquet-geospatial/` (crate) | `src/parquet_geospatial/` | WKB reader, intervals, geometry bounder |
| `arrow/`             | `src/parquet/arrow/`        | Arrow interoperability                  |
| `parquet_thrift.rs`  | `src/parquet/format/`       | Thrift-encoded Parquet metadata structs |

Companion Variant crates live under `src/parquet_variant*`; derived-record
support lives in `src/parquet/derive.cr`. `bin/` contains command-line tools,
`examples/` contains runnable integration workflows, and `bench/` contains the
round-trip measurement harness. These entry points reuse the library APIs.

## Local file streaming

[File chunk readers](file-chunk-reader.md) duplicate the open handle, seek and
return a buffered stream, matching upstream's file implementation. Reader
creation does not allocate the file tail. Block-scoped reads close handles on
success and exceptions; cloned file handles retain upstream shared-position
semantics.

## Metadata ownership

Metadata builders retain native references. Use
[`ParquetMetaData#clone`](metadata-snapshots.md) before editing a snapshot that
must remain reusable. Clones copy mutable metadata buffers while sharing schema
and page-index provider references. [Memory estimates](metadata-memory.md)
describe native retained storage rather than Rust allocator sizes.

[On-demand index providers](on-demand-page-index.md) explicitly populate sparse
caches; looking up an index does not fetch it. The
[row-group example](explicit-row-group-fetching.md) explicitly fetches projected
column chunks before constructing independent readers.

## Layering

The port preserves the upstream dependency direction:

1. `format` (Thrift structs) and `errors` have no internal dependencies.
2. `basic`, `data_type`, and `schema` depend only on the two above.
3. `encodings` build on `basic` and byte buffers.
4. `column` and `file` build on encodings and schema.
5. `record` and `arrow` are the highest-level APIs.

Do not introduce a dependency that reverses this direction.

## Dependencies

Run `make install` (`shards install`) before building.

| Shard                | Scope   | Purpose                                            |
| -------------------- | ------- | -------------------------------------------------- |
| `naqvis/snappy`      | runtime | SNAPPY codec (pure Crystal, raw/block)             |
| `paddor/flint`       | runtime | LZ4 raw block codec (pure Crystal; frames in-tree) |
| `paddor/zinc`        | runtime | ZSTD codec (pure Crystal, RFC 8878 frames)         |
| `OrelSokolov/brotli` | runtime | BROTLI codec (pure Crystal, RFC 7932)              |
| `arrow-cr` (path)    | runtime | Arrow interop (`parquet/src/arrow/` port)          |
| `ameba`              | dev     | `make lint` static analysis                        |

Everything else (GZIP via `compress/gzip`, the LZ4 frame container, Thrift,
protobuf-free metadata, crypto via OpenSSL) comes from the Crystal standard
library or is implemented in-tree. Prefer a maintained, active shard over new
in-tree code for third-party formats; record the decision in `plans/parity.md`
when adding a runtime dependency.

## Asynchronous storage reads and writes

The [execution/storage mapping](async-storage.md) records how yielding Crystal
methods replace Rust futures, how the existing push decoder drives demand
range reads, and how native memory/HTTP object stores and explicit execution
contexts integrate without a mandatory cloud SDK dependency. It also covers
buffered asynchronous writes and object publication. The
[object-store example](object-store-example.md) exercises normal and dedicated
execution-context reads after a batch write.
