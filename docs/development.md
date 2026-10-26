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
