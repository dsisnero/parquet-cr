# parquet-cr

A Crystal port of https://github.com/apache/arrow-rs — specifically the
[`parquet`](https://github.com/apache/arrow-rs/tree/master/parquet) crate, the
Apache Parquet implementation from apache/arrow-rs.

Upstream is vendored as a git submodule at `vendor/arrow-rs`, pinned to release
`60.0.0` (commit `ef1fa157977633f0ba21aa921f9ef3d5c669235b`). The upstream
`parquet/` directory is the source of truth for all ported behavior.

## Documentation

- [Resuming work](docs/resuming.md) — start here in a new session
- [Architecture](docs/architecture.md)
- [Development](docs/development.md)
- [Coding Guidelines](docs/coding-guidelines.md)
- [Testing](docs/testing.md)
- [PR Workflow](docs/pr-workflow.md)

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

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     parquet-cr:
       github: dsisnero/parquet-cr
   ```

2. Run `shards install`

## Usage

```crystal
require "parquet-cr"
```

## Development

See [docs/development.md](docs/development.md) for checkout, parity tracking,
and environment notes.

## Contributors

- [Dominic Sisneros](https://github.com/dsisnero) - creator and maintainer
