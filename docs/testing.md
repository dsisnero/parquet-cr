# Testing

## Gates

Run the full gates before closing a feature:

```bash
crystal tool format --check src spec bin examples
ameba src spec bin examples
crystal spec
```

`make lint` and `make test` wrap these. The runnable examples are included in
format/lint; on-demand index acceptance is exercised by the spec suite.

## Porting tests early

Port upstream tests as first-class work. For each upstream module, locate the
nearest `#[cfg(test)]` module or `parquet/tests/*` file and port its assertions
directly into a Crystal spec.

- Put specs under `spec/parquet/...` mirroring `src/parquet/...`.
- If upstream lacks tests, write characterization specs from observable
  behavior and mark inferred behavior in the spec name or a comment.
- Never weaken assertions or change fixtures to fit the current implementation.

## Red-green cycles

1. Port the next missing or failing parity spec.
2. Make the smallest change that turns it green.
3. Run focused checks (`crystal spec spec/parquet/<file>_spec.cr`).
4. Repeat until the feature is complete.

## Fixtures

Binary fixtures needed for round-trip tests are generated with a small helper in
spec support code so they remain reproducible. Do not commit opaque blobs that
cannot be regenerated.

## Canonical extensions and virtual columns

`make test` keeps the pinned upstream default feature behavior. Run the full
suite with optional canonical/geospatial/Variant schema mappings enabled using:

```sh
make test-extensions
```

This passes `-Darrow_canonical_extension_types -Dgeospatial
-Dvariant_experimental` to Crystal, mirroring the upstream Cargo features.
Virtual columns and the WKB extension contract are available in either build.
The feature specs are `virtual_type_spec.cr`, `virtual_reader_spec.cr` and
`extension_mapping_spec.cr` under `spec/parquet/arrow/`.

## Rewrite interoperability with the pinned Rust tools

The rewrite binary has no dedicated upstream unit tests. Its characterization
specs under `spec/parquet/bin/parquet_rewrite*` cover the pinned CLI/property
mapping and streamed values/schema/metadata, with INT96 resolution tests in
`primitive_array_reader_spec.cr`.

Build the pinned Rust tools and compare exact JSON records in both directions:

```sh
cargo build --locked --offline --manifest-path vendor/arrow-rs/parquet/Cargo.toml \
  --features cli --bin parquet-read --bin parquet-rewrite \
  --target-dir /private/tmp/parquet-rewrite-rust-target
crystal build bin/parquet-rewrite.cr -o /private/tmp/parquet-rewrite-cr
scripts/verify_rewrite_interop.sh /private/tmp/parquet-rewrite-cr \
  /private/tmp/parquet-rewrite-rust-target/debug/parquet-read \
  /private/tmp/parquet-rewrite-rust-target/debug/parquet-rewrite
```

The script preserves comparison files in a temporary directory. Its exact JSON
oracle is the pinned Rust rewrite output: both tools normalize INT96 to INT64
nanoseconds, which Rust's record JSON API renders differently from raw INT96.
It exercises
seven supported codecs, V1/V2 output, dictionary on/off, page/header statistics,
bloom options, omitted column paths, row-group splitting and INT96 input.

## CSV import interoperability

`spec/parquet/bin/parquet_fromcsv*` ports the pinned binary's parser, reader
configuration and compressed-input conversion tests, with stronger end-to-end
value/schema assertions and additional CLI, null, dialect and error cases.
Native help option coverage replaces the Clap-formatted help snapshot.

```sh
cargo build --locked --offline --manifest-path vendor/arrow-rs/parquet/Cargo.toml \
  --features cli --bin parquet-fromcsv --bin parquet-read \
  --target-dir /private/tmp/parquet-rewrite-rust-target
crystal build bin/parquet-fromcsv.cr -o /private/tmp/parquet-fromcsv-cr
crystal run scripts/verify_fromcsv_interop.cr -- /private/tmp/parquet-fromcsv-cr \
  /private/tmp/parquet-rewrite-rust-target/debug/parquet-fromcsv \
  /private/tmp/parquet-rewrite-rust-target/debug/parquet-read \
  /private/tmp/parquet-fromcsv-interop
```

The script generates compressed input fixtures, imports each with Crystal and
pinned Rust, then compares exact records using the pinned Rust reader. It covers
all six supported input/output codecs, V1/V2, schema-directed typed/null values,
headers, batch/group boundaries, bloom filters, TSV/default quoting, custom
characters, concatenated Gzip/Zstd/LZ4 streams and per-codec empty-stream
behavior. JSON and Parquet files remain
in the supplied directory for inspection; no generated files enter the repo.

## Asynchronous storage reads and writes

See [the execution/storage mapping and verification cases](async-storage.md).
The six `spec/parquet/arrow/async_*_spec.cr` suites use pinned fixtures,
deterministic generated files and an ephemeral loopback HTTP server. A sandbox
must allow local socket binding for `make test` and `make test-extensions`;
external services and credentials are not required. Focused command:

```sh
crystal spec spec/parquet/arrow/async_reader_spec.cr \
  spec/parquet/arrow/async_metadata_spec.cr \
  spec/parquet/arrow/async_store_spec.cr spec/parquet/arrow/async_spawn_spec.cr \
  spec/parquet/arrow/async_writer_spec.cr spec/parquet/arrow/async_writer_store_spec.cr
```

## Metadata memory accounting

See [metadata memory estimates](metadata-memory.md) for native layout and
ownership rules. `spec/parquet/file/metadata_memory_spec.cr` ports upstream
memory/decryptor assertions with exact Crystal-layout sums and verifies parsed
file/index integration, opaque providers, encryption/geospatial payloads and
retained container capacity.

## Typed physical columns

`spec/parquet/column/typed_physical_spec.cr` ports the upstream nested-list
example and four level/value validation tests, then covers all eight descriptor
identities/sizes, V1/V2 nullable round trips and the complete mismatch matrix.
See [typed columns](typed-columns.md) for the native type/ownership mapping.

## Generated mixed Thrift unions

`mixed_union_spec.cr` and `file/mixed_union_integration_spec.cr` test generated
codecs against exact wire fixtures, malformed/unknown variants on slice and IO
protocols, and complete encrypted files. Run them with metadata_memory_spec.cr
to retain independent layout checks. See [Thrift unions](thrift-unions.md).
