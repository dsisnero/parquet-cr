# parquet-cr

A Crystal port of [apache/arrow-rs](https://github.com/apache/arrow-rs) — specifically the
[`parquet`](https://github.com/apache/arrow-rs/tree/60.0.0/parquet) crate, the
Apache Parquet implementation from apache/arrow-rs.

Upstream is vendored as a git submodule at `vendor/arrow-rs`, pinned to release
`60.0.0` (commit `ef1fa157977633f0ba21aa921f9ef3d5c669235b`). The upstream
`parquet/` directory is the source of truth for all ported behavior.

## Documentation

- [Resuming work](docs/resuming.md) — start here in a new session
- [Architecture](docs/architecture.md)
- [Asynchronous storage reads and writes](docs/async-storage.md)
- [Development](docs/development.md)
- [Coding Guidelines](docs/coding-guidelines.md)
- [Testing](docs/testing.md)
- [PR Workflow](docs/pr-workflow.md)
- [Changelog](CHANGELOG.md)

## Supported workflows

The port includes synchronous file/column and Arrow RecordBatch readers and
writers, nested data, compression, page indexes, bloom filters, modular
encryption, Variant/JSON operations and GeoParquet support. Parity is still in
progress: see the [current status](plans/parity.md) and
[documented divergences](plans/parity/divergences.md) for scope and limitations.

| Workflow | Guide |
| --- | --- |
| Typed physical columns and generated metadata codecs | [Typed columns](docs/typed-columns.md), [Thrift unions](docs/thrift-unions.md) |
| Arrow string and byte-page decoding | [UTF-8 validation](docs/arrow-string-validation.md), [variable bytes](docs/arrow-byte-page-decoding.md), [fixed bytes](docs/arrow-fixed-byte-decoding.md) |
| Metadata caching and reuse | [Memory estimates](docs/metadata-memory.md), [snapshots](docs/metadata-snapshots.md) |
| Explicit index and row-group loading | [On-demand indexes](docs/on-demand-page-index.md), [row-group fetching](docs/explicit-row-group-fetching.md) |
| Object-store batch round trips | [Runnable example](docs/object-store-example.md) |
| Encoding read/write measurements | [Round-trip benchmark](docs/round-trip-benchmark.md) |

## Runnable examples and benchmark

From a configured repository checkout:

```sh
crystal run examples/custom_page_index.cr
crystal run examples/read_with_rowgroup.cr
crystal run examples/object_store.cr
crystal build --release bench/parquet_round_trip.cr -o /private/tmp/parquet-round-trip
/private/tmp/parquet-round-trip --smoke --samples=3
```

The object-store example uses memory storage and needs no cloud credentials.
The benchmark validates values before timing all 46 source cases; its CSV
reports first and warm samples. See its guide for dimensions, measurement
boundaries and differences from upstream Criterion measurements.

## Rewrite Parquet files

```sh
crystal build bin/parquet-rewrite.cr -o bin/parquet-rewrite
bin/parquet-rewrite -i input.parquet -o output.parquet --compression zstd
bin/parquet-rewrite --help
```

The command preserves values and user metadata while changing encoding,
compression, row groups, pages, statistics and bloom settings. See
[command behavior](docs/development.md#parquet-rewrite-command) and
[pinned Rust interoperability checks](docs/testing.md#rewrite-interoperability-with-the-pinned-rust-tools).

## Import CSV or TSV

```sh
crystal build bin/parquet-fromcsv.cr -o bin/parquet-fromcsv
bin/parquet-fromcsv -s schema.txt -i input.csv -o output.parquet -h
bin/parquet-fromcsv --help
```

Provide a Parquet message schema in `schema.txt`; `-h` skips the header row.
Use `-f tsv` for tab-separated input. See
[command behavior](docs/development.md#csv-import-command) and
[pinned Rust checks](docs/testing.md#csv-import-interoperability).

## Upstream README Highlights

The upstream `parquet` crate provides a Rust implementation of the Apache
Parquet columnar file format, including file metadata, encodings, compression,
and Arrow interoperability. See the
[submodule README](vendor/arrow-rs/parquet/README.md) for the original project
description and feature matrix. Upstream tests in `parquet/tests` and inline
`#[cfg(test)]` modules are treated as the normative behavior for this port.

## Installation

Requires Crystal **1.22.0 or newer**. The current dependency configuration uses
`arrow-cr` from the sibling path `../arrow-cr`; prepare that checkout before
installing. See [development setup](docs/development.md) and the
[dependency requirements](plans/parity.md#where-we-are).

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     parquet-cr:
       github: dsisnero/parquet-cr
   ```

2. Run `shards install` with the sibling `arrow-cr` checkout available.

## Usage

```crystal
require "parquet-cr"
```

## Development

See [docs/development.md](docs/development.md) for checkout, parity tracking,
and environment notes.

## Contributors

- [Dominic Sisneros](https://github.com/dsisnero) - creator and maintainer
