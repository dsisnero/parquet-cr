# Development

## Source of truth

The upstream project is [apache/arrow-rs](https://github.com/apache/arrow-rs),
vendored as a git submodule:

- Path: `vendor/arrow-rs`
- Pin: release `60.0.0`
- Commit: `ef1fa157977633f0ba21aa921f9ef3d5c669235b`
- Ported subdirectory: `vendor/arrow-rs/parquet`

Upstream tests and fixtures are normative. When Crystal behavior disagrees with
upstream, upstream wins unless a divergence is explicitly documented in
`plans/parity.md`.

## Checkout and updates

```bash
git submodule update --init --recursive
# Arrow interop depends on the sibling checkout at ../arrow-cr
make install   # shards install (runtime deps: naqvis/snappy, paddor/flint,
               # paddor/zinc, OrelSokolov/brotli, and the sibling ../arrow-cr
               # checkout via `path:`; dev: ameba)
```

The submodule is shallow and pinned. Do not move the pin without regenerating
the parity inventory.

## Parity tracking

- Feature roadmap: `plans/parity.md`
- Working ledger: `plans/inventory/rust_port_inventory.tsv`
- Generated manifests: `plans/inventory/rust_source_parity.tsv` and
  `plans/inventory/rust_test_parity.tsv`

The parity scripts live in the `cross-language-crystal-parity` skill. Run them
from the skill directory rather than copying them into this repo.

## Environment notes

This checkout lives on an exFAT volume, which causes macOS to emit AppleDouble
`._*` sidecar files. They are ignored by `.gitignore`. If a git pack operation
prints `non-monotonic index .../._pack-*.idx` warnings, delete the sidecars:

```bash
find .git -name '._*' -delete
```

The warnings are non-fatal.

## Virtual columns and canonical extension mappings

Configure generated columns through `Parquet::Arrow::ArrowReaderOptions`:

```crystal
field = Arrow::Field.new("row_number", Arrow::DataType.int64, false)
  .with_extension_type(Parquet::Arrow::RowNumber.new)
options = Parquet::Arrow::ArrowReaderOptions.new.with_virtual_columns([field])
reader = Parquet::Arrow::ParquetRecordBatchReaderBuilder
  .try_new_with_options(file_reader, options).build
```

`RowNumber` emits file-global row positions; `RowGroupIndex` emits physical
metadata indices. Selected/reordered row groups, row selection, predicates,
offset/limit and the push decoder preserve those identities. Virtual fields are
appended at the root and remain included when physical projection is empty.
They use Int64 storage and required empty-string extension metadata. The pinned
source recognizes `parquet.virtual.` while its validation error text mentions
`arrow.virtual.`; the port preserves both behaviors.

UUID/JSON, WKB Geometry/Geography and Variant logical schema mapping follow the
upstream optional features, enabled with `-Darrow_canonical_extension_types`,
`-Dgeospatial` and `-Dvariant_experimental`. These apply in both writer and reader
schema conversion. Defaults remain unchanged. `ParquetGeospatial::WkbType`
implements Arrow's extension contract for Binary/LargeBinary/BinaryView storage;
metadata parse errors retain the `ParquetGeospatial::JsonError` name and inherit
`Arrow::ArrowError` so `Field#has_valid_extension_type?` returns false on them.

## Parquet rewrite command

Build and run the standalone command:

```sh
crystal build bin/parquet-rewrite.cr -o bin/parquet-rewrite
bin/parquet-rewrite -i input.parquet -o output.parquet \
  --compression zstd --writer-version 2.0 --max-row-group-size 100000
bin/parquet-rewrite --help
```

All options from pinned `parquet-rewrite.rs` are available. Boolean options
require explicit `true` or `false`; enum values use lowercase kebab spelling.
The command streams Arrow batches, copies source key/value metadata (including
valueless/duplicate user entries), and refreshes the embedded Arrow schema.
Writer defaults apply to unspecified settings, rather than copying input
compression/encoding settings. Bloom FPP/NDV/position apply only with explicit
`--bloom-filter-enabled true`; page-header statistics force page statistics even
if `--statistics-enabled none` was also given. LZO remains unsupported by both
pinned implementations. INT96 input converts to Arrow timestamps and is written
back as annotated INT64, preserving its values.

CLI diagnostics use Crystal formatting; argument errors return 2, operational
errors return 1, and `--version` reports the port's package version. Unsigned
sizes must fit the port's Int32 APIs; compression levels use u32 semantics,
including upstream's wrapping cast to i32 for Zstd. Input/schema/property
validation precedes opening the destination. Output writing itself is streaming
and can leave partial output on a later failure, as in upstream.

## CSV import command

```sh
crystal build bin/parquet-fromcsv.cr -o bin/parquet-fromcsv
bin/parquet-fromcsv -s schema.txt -i input.csv -o output.parquet -h \
  --parquet-compression zstd --batch-size 1000 --max-row-group-size 100000
```

All options from pinned `parquet-fromcsv.rs` are available. The three paths are
required named options. The schema file contains a Parquet message definition,
for example:

```text
message events { required int32 id; optional binary name (STRING); }
```

The root name is preserved. Arrow CSV applies the declared types and empty-field
null semantics; nested/binary types unsupported by upstream Arrow CSV still fail.
Input is converted in Arrow batches; batch size defaults to 1000, or
`PARQUET_FROM_CSV_BATCHSIZE` when set, with explicit CLI values taking precedence.
A zero batch size produces an empty output, matching the pinned tool.

`-h` means `--has-header`; help uses `--help`. CSV defaults to comma and CR/LF
terminators; TSV defaults to tab and LF. Both retain Arrow CSV's default double
quote unless `--quote-char` overrides it: upstream's unset TSV quote does not
actually disable quoting. The `--double-quote` option is parsed but unused,
exactly as in the pinned source. Quote, delimiter and escape each accept one
Unicode character and truncate its code point to u8 as upstream does.

Input compression accepts UNCOMPRESSED, SNAPPY (framed), GZIP (multiple members),
BROTLI, LZ4 (frames) and ZSTD (concatenated/skippable frames). Output uses the same
codec names, defaulting to SNAPPY; names are case-insensitive. LZO parses but is
unsupported by both tools. Writer versions are `1` and `2`; bloom settings take
explicit `true`/`false`. Sizes must fit Int32. Brotli/LZ4/Zstd input uses existing
buffer-based codecs, retaining compressed and decompressed bytes in memory;
uncompressed/Snappy/Gzip CSV reads are incremental. Arrow batches are written as
they arrive without collecting the table. Empty uncompressed/Snappy/LZ4 input
produces an empty table; empty Gzip/Brotli/Zstd input fails, matching pinned Rust.

Schema reading/parsing precedes output creation; the input is opened after the
output, matching upstream's error order. A later failure can leave a truncated
or partial output. Native exceptions and contextual messages replace Rust error
enums; argument errors return 2, runtime errors return 1. Help formatting and
`--version` use the Crystal port's conventions and version.

## Metadata cache budgets

Use `ParquetMetaData#memory_size` to estimate the retained bytes of cached
metadata, including loaded indexes and decryptor state. See
[metadata memory estimates](metadata-memory.md) for native layout, ownership
exclusions and verification rules.

## Physical typed-column APIs

[Typed columns](typed-columns.md) documents all eight DataType descriptors,
optional/raising conversions, native T.class bridges and serialized-column
read/write usage. Both entry styles reuse the existing native implementations.

## Generated mixed Thrift unions

[Thrift unions](thrift-unions.md) documents the strict and forward-compatible
unit/payload DSL, native representation, pinned diagnostics/limitations and
production crypto-metadata integration. Use generated codecs for new mixed
unions and retain exact wire/API tests when replacing manual ones.

## Arrow decoding and metadata reuse

The decoding guides cover [string validation](arrow-string-validation.md),
[variable-byte pages](arrow-byte-page-decoding.md) and
[fixed-byte pages](arrow-fixed-byte-decoding.md), including supported encodings,
skip behavior and malformed-input diagnostics. Preserve their upstream
assertions when extending reader behavior.

Use [metadata snapshots](metadata-snapshots.md) when reusing metadata across
builders or footer writes. [On-demand page indexes](on-demand-page-index.md)
and [explicit row-group fetching](explicit-row-group-fetching.md) document
explicit cache/fetch boundaries and runnable examples.

## Object-store example and measurements

Run `crystal run examples/object_store.cr` for a memory-store batch round trip
with normal and dedicated-context reads. See the
[object-store guide](object-store-example.md) for error and lifecycle behavior.

Build and run the [round-trip benchmark](round-trip-benchmark.md) in release
mode. Its smoke matrix checks all 46 cases; use repeated independent processes
for measurements and report dimensions, sample counts and environment. Exact
schema/value checks occur outside timing. The native CSV harness and RNG differ
from upstream Criterion/StdRng, so they do not establish Rust performance parity.
