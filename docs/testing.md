# Testing

## Gates

Run the full gates before closing a feature:

```bash
crystal tool format --check src spec
ameba src spec
crystal spec
```

`make lint` and `make test` wrap these.

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
