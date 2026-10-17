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
make install   # shards install (runtime deps: naqvis/snappy, paddor/flint; dev: ameba)
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
