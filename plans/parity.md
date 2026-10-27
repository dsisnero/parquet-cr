# parquet-cr Parity Roadmap

> **Start here.** Resume from **Where we are**, then follow
> [docs/resuming.md](../docs/resuming.md). Do not reconstruct the next task from
> historical commits or the archived module checklist.

- Source of truth: `vendor/arrow-rs`, apache/arrow-rs `60.0.0`, commit
  `ef1fa157977633f0ba21aa921f9ef3d5c669235b`.
- Scope: `parquet`, `parquet-variant*`, `parquet-geospatial`, `parquet_derive*`.
- Arrow dependency: sibling `../arrow-cr`, at the same upstream pin.
- Symbol status: [curated API ledger](inventory/rust_port_inventory.tsv).
  Generated discovery manifests remain under `plans/inventory/`.
- Repository workflow and definition of done: [AGENTS.md](../AGENTS.md).

## Where we are

- **Current focus:** Phase 5 — CSV import command is complete on
  `codex/parquet-fromcsv`. Phases 1–5 are delivered; Phases 6–7 remain deferred
  pending the asynchronous storage/concurrency mapping below.
- **Next up**, in order:
  1. **Phase 6 — Asynchronous storage reads**, first record the Crystal
     fibers/ExecutionContext mapping and object-store dependency, then port
     end-to-end reader tests before implementation.
  2. **Phase 7 — Asynchronous storage writes**, after Phase 6 and its storage
     decisions; keep these as complete API features rather than helper phases.
- **Last completed feature:** `port: CSV import command`, on
  `codex/parquet-fromcsv`. Thirty tests-first upstream-derived/characterization
  specs cover CLI options, reader/writer configuration, compressed input,
  typed/null values, schema/root, groups, dialects, empty streams and errors.
  Observed red-green cycles include missing APIs, decoder error context and
  per-codec empty-stream behavior. Previous feature: `d6bc63a` (Parquet rewrite).
- **Last code gates:** `make lint` (191 files), interop script lint (1 file),
  `make test` (1862 examples), `make test-extensions` (1868 examples), library
  and CLI no-codegen builds, standalone CLI build, scoped inventory validation
  (3515 items), and `git diff --check`, all green with zero failures/errors/pending.
  Exact pinned Rust-read records match the Rust CSV importer for six input/output
  codecs, V1/V2, typed/null values, dialects, concatenated and empty streams.
- **Dependency requirements:** sibling arrow-cr supplies extension machinery
  (`925255d`) and canonical Uuid/Json types. Existing Decimal256 prerequisites
  remain `9a5612c`, `b05366a` and `49d07ce`; see the resolved dependency notes in
  `shards_info/`. No shard patch or dependency edit was made.
- **Ledger counts:** 2704 `ported`, 239 `intentional_divergence`, 28 `skipped`,
  544 `missing` (3515 total).
- **Plan layout:** this file is the active feature queue and completion record.
  [Implementation reference](parity/implementation-reference.md) is a complete
  archival snapshot of earlier module-level evidence, including stale historical
  checkboxes. [Divergences](parity/divergences.md) is a maintained reference for
  representation differences and verification notes. Neither is a separate
  work queue. Work next in this file, Phase 6 (resolve its deferred decisions
  first); update divergences as needed.

## Feature sizing and completion

Each numbered phase is one independently usable capability, suitable for one
feature branch and one atomic completion commit. Define completion by observable
behavior and upstream parity tests. A phase may touch several modules and take
multiple sessions; session boundaries do not shrink its scope.

Port upstream tests first, observe the expected failure, then implement in small
red-green cycles. Levels, buffers, builders, readers, factories, dispatch helpers
and individual types are implementation steps inside their owning feature.
Track their symbols in the ledger; do not promote each to a new phase or stop
at a helper-only commit. If a phase needs splitting, each replacement must have
its own usable API or command and end-to-end acceptance test.

A phase is complete only when its entire supported API and integration work,
upstream tests or equivalent characterizations pass without weakened assertions,
`make lint` and `make test` pass, the no-codegen build and scoped inventory check
pass, divergences are recorded, and the ledger and **Where we are** block are
updated in the same atomic `port:` commit. A module unit test alone does not
complete an integrated feature. Keep blocked phases explicit rather than
reporting their prerequisites as the finished capability.

## Completed capability baseline

These are existing capabilities, not new implementation phases. Detailed source
and spec mappings remain in the implementation reference and ledger.

| Capability | Delivered behavior | Remaining work |
| --- | --- | --- |
| Synchronous Parquet files | Schema/Thrift, encodings/compression, column and file readers/writers, metadata, page indexes, bloom filters, record API | Generic API compatibility decisions below |
| Arrow batch reading | Schema conversion both ways, IPC schema metadata, primitive/composite readers, selection, filtering, pruning, metrics and push decoding | Recorded representation differences; individual coverage gaps remain in the ledger |
| Arrow batch writing | Public writer/options, nested levels, physical encoding, row/byte limits, stored-page streaming, metadata, CDC and encrypted round trips | Recorded representation/performance differences; Decimal256 IPC needs arrow-cr 9a5612c |
| Variant binary and JSON | Paths, values/builders, exact binary encoding, JSON in both directions | Recorded ownership and calendar-range divergences |
| Variant Arrow operations | Arrays/builders, safe/strict casts, path extraction, shredding and unshredding, scalar/composite/encoded arrays | Recorded representation differences; encoded Decimal256 requires the Arrow fixes above |
| Geospatial | GeoParquet metadata, bounds, WKB support and core statistics integration | WKB extension and schema mapping delivered; ISO WKB limitation documented |
| Derived records | Schema generation and typed record read/write round trips | Recorded Crystal macro/trait representation differences |
| Modular encryption | Cipher/metadata/properties and synchronous encrypted file/page read/write paths | Recorded unsupported upstream modes and representation differences |
| Serialized page storage | Opaque UInt64 keys, store/factory contracts, column context, in-memory backend and resident-byte accounting | Integrated into ArrowWriter |
| Existing command-line tools | Inspect/read/index/schema/rowcount/layout/bloom-filter, concatenate, rewrite and CSV/TSV import workflows | Recorded CLI representation differences |

## Phase 1 — Arrow RecordBatch writing

Status: **complete**, with 84 new upstream-derived/characterization specs and
all repository gates passing. This feature includes the full public writer,
low-level column factory/chunk API and page-store integration in one commit.

- [x] Write Arrow `RecordBatch`es through the public synchronous `ArrowWriter`
  API and read the resulting Parquet files back with equivalent values/schema.

Scope includes definition/repetition levels for flat and nested arrays,
primitive and byte-array column encoders, dictionary/data pages, buffering,
row-group boundaries, writer options, flush/close/metadata behavior, schema
metadata, and factory configuration/routing through the existing `PageStore`.
Include the writer's content-defined chunker integration and its deferred Arrow
round-trip tests. Main upstream area: `parquet/src/arrow/arrow_writer/`, including
`levels.rs`, `byte_array.rs` and `mod.rs`, plus shared Arrow/schema/buffer glue
required by that API.

Completion evidence: port the upstream level, byte-array and writer tests first;
then demonstrate flat/nested/null/dictionary/multiple-row-group round trips,
custom page-store output equivalence and dictionary-page routing. Exercise the
supported writer configuration and error paths. Keep dependencies in this
phase until the public writer and integration tests are green; completing
`levels.rs` or a page writer alone is intermediate progress.

Delivered in `src/parquet/arrow/arrow_writer.cr` and `arrow_writer/{levels,
column,pages}.cr`, exported by `src/parquet.cr`. Existing column/file writers
now expose estimates and distinct overrides and splice stored blobs with
correct dictionary offsets and encryption metadata. Integration tests also
fixed reader storage widths for narrow/unsigned integers, decimal targets and
coerced Date64; Float16 reading is implemented and negative DayTime intervals
preserve their packed bits. The old Float16-NYI characterization was replaced
with exact bit/value assertions. Custom sparse-key stores produce byte-identical
files, route dictionary bytes through the store and release every stored blob.
Public tests cover nested/sliced collections, all dictionary key/run widths,
Decimal32/64/128/256, temporal types, plain/dictionary fallback, V1/V2/codecs,
row/byte limits, metadata options, CDC, bloom filters and encrypted round trips.

## Phase 2 — Complete Variant numeric conversions

- [x] Convert the remaining supported Variant numeric values to typed Arrow
  output with upstream safe/strict semantics and decimal precision intact.

Scope includes Decimal256 primitive and union output, decimal exponent notation,
and shared conversion logic used by casts, path extraction and shredding.
Arrow-to-Variant Decimal256 input already works and remains covered. Reuse
arrow-cr's existing I256 support; record any additional dependency gaps.

Completion evidence: upstream-derived numeric boundary and overflow tests,
exponent/scale/precision cases, union selection, SQL null versus Variant null,
safe/strict errors, and public-kernel round trips. Decimal helpers alone do not
complete this phase.

Delivered in `src/parquet_variant_compute/type_conversion.cr` and
`variant_to_arrow.cr`, with 28 tests-first upstream-derived/characterization specs
in `numeric_conversion_spec.cr`. Decimal256 rescaling widens before multiplying;
all decimal widths parse exponent notation, ASCII whitespace and long mantissas
with upstream precision/rounding semantics. Float conversions use native ranges.
Public tests cover dense/sparse union ranking, nested paths/lists, SQL/Variant
nulls, safe/strict errors, dictionary/run-end output and reverse casts. Builders
validate precision/scale. Decimal256 shredding rejection matches upstream and is
correctly marked ported in the ledger.

The existing `12.345` parser test now asserts upstream rounding to `1235` at
scale 2. Independent Arrow fixes `b05366a` and `49d07ce` resolve the two encoded
output blockers; [reproductions and resolution](../shards_info/arrow_cr.4.md).
All original expected encoded values remain asserted; the added dictionary test
compares nullable keys directly to correct a tuple/array container mismatch.
No shard patch or helper-only commit was made. All required gates pass, including
1776 full-suite examples with zero failures/errors.

## Phase 3 — Canonical extensions and virtual columns

- [x] Read/project virtual columns and round-trip supported canonical extension
  metadata through the Arrow/Parquet schema and array interfaces.

The canonical extension APIs are now available in `../arrow-cr`, including
Uuid/Json and Field validation/registration. No shard patch is needed. Scope includes
`arrow/schema/virtual_type.rs`, `arrow/schema/extension.rs`, virtual field/level
conversion, the virtual array reader and public reader/projection wiring, plus
`parquet-geospatial`'s `WkbType` Arrow extension implementation. Existing
primitive schema conversion and ProjectionMask behavior are already delivered.

Completion evidence: upstream extension validation/metadata tests and public
reader tests for virtual-column values, projection and row selection, plus WKB
extension/schema round trips. Do not mark complete when only the extension
constant or schema type exists.

Delivered in `schema/{virtual_type,extension}.cr` and
`array_reader/{row_number,row_group_index}.cr`, with schema/level, builder,
reader-option and sync/push decoder wiring. Virtual fields preserve global row
positions and physical group indices under reordered groups, projection,
selection, predicates, batching and offset/limit. Ordinal errors and buffered
read/skip behavior match upstream. Virtual types preserve upstream value equality.

Canonical Uuid/Json, WKB Geometry/Geography and Variant schema mapping uses
matching compile flags (`arrow_canonical_extension_types`, `geospatial`,
`variant_experimental`); defaults remain upstream-compatible. WKB field storage,
metadata validation, CRS defaults/unset CRS and all five edge algorithms are
covered. WKB JsonError keeps its port name and now inherits ArrowError so Arrow
field validation catches it. Rust Any downcasting is Crystal `is_a?`/`as?`, and
run-length group-index state replaces the flattened RepeatN iterator.

Three new spec files cover 38 enabled examples (32 default), including public
file round trips with/without IPC hints. The existing flat-primitives JSON
expectation now uses upstream's feature-conditional `json_field` helper.
Without IPC hints Variant binary leaves reconstruct as Binary; exact data/nulls
and the expected storage types are asserted in both paths. `make test-extensions`
provides the full enabled gate. See [development](../docs/development.md) and
[testing](../docs/testing.md). All gates pass, with 1808 default and 1814 enabled
full-suite examples, and the ledger closes 38 affected rows.

## Phase 4 — Parquet rewrite command

- [x] Run `parquet-rewrite` end to end with upstream options and equivalent
  data/schema in the output Parquet file.

Requires Phase 1. Include command parsing, reader-to-writer streaming, supported
rewrite options, errors and executable wiring. Port upstream CLI tests first;
verify output with both the local reader and the pinned upstream reader where
available. Parser helpers alone do not complete the command.

Delivered in `src/parquet/bin/parquet_rewrite.cr` and `bin/parquet-rewrite.cr`.
The pinned binary has no unit tests, so upstream-derived characterizations were
written first, observed red, then implemented. Twenty command specs cover all
option spellings, validation and dependent properties, batch streaming, nested
and nullable values, metadata, row/page boundaries, codecs, empty files and
failure paths. Four additional reader specs cover native INT96 conversion at
all timestamp resolutions, including nulls and pre-epoch values; the public
rewrite accepts the pinned legacy fixture and normalizes timestamps as Rust does.

`scripts/verify_rewrite_interop.sh` compares exact records with the pinned Rust
rewrite output for all seven supported codecs and both page versions, including
Rust→Crystal rewrites. LZO remains unsupported by both implementations. Native
OptionParser diagnostics, Int32 size limits, port version reporting and opening
the input once are recorded in [divergences](parity/divergences.md). No shard
patch or dependency edit was made. All gates above pass.

## Phase 5 — CSV import command

- [x] Run `parquet-fromcsv` end to end to produce valid Parquet with upstream
  schema/type/null handling and command behavior.

Requires Phase 1 and any needed arrow-cr CSV facilities. Include executable
wiring, parsing, supported import options and errors. Port upstream tests first;
verify typed/null values and schema after reading the output file. Schema
inference or argument parsing alone does not complete the command.

Delivered implementation in `src/parquet/bin/parquet_fromcsv.cr`, exported by
`src/parquet.cr`, with `bin/parquet-fromcsv.cr` executable wiring. Upstream
argument/default, dialect, compression and reader configuration tests were
ported before implementation and observed red. Public conversion tests followed
with a missing conversion API failure, then green implementation; a later
malformed-compression test exposed eager initialization losing read context,
fixed by lazy decoder initialization. Empty-stream comparisons with pinned Rust
exposed native codec differences; a separate red-green cycle corrected them
in the command adapter. Thirty specs assert exact values, nulls, schema/root,
row groups, all six compressed inputs, properties, empty and zero-batch files,
per-codec empty streams, required-null rejection, CLI behavior and error ordering.
Native help coverage replaces the Clap snapshot without dropping option checks.

`scripts/verify_fromcsv_interop.cr` imports generated CSV through both Crystal
and pinned Rust and compares exact records with the pinned Rust reader. It
covers six input/output codecs, V1/V2, typed/null values, headers, batch/group
limits, bloom, custom dialects, TSV quoting and concatenated streams. Existing
Arrow CSV facilities suffice; no shard patch or dependency edit was made.
Buffer-based Brotli/LZ4/Zstd input, native errors/help/version and Int32 sizes
are recorded in [divergences](parity/divergences.md). All gates above pass.
The optional upstream Rust unit run could not start offline because
`async-trait 0.1.91` is not cached (the pinned binary build and interop do run).

## Phase 6 — Asynchronous storage reads

- [ ] Read Parquet batches from an asynchronous/object-store source through a
  usable Crystal reader API with upstream selection and error semantics.

Deferred until the Rust tokio/futures versus Crystal fibers/ExecutionContext
mapping and object-store dependency are recorded. Include asynchronous range
fetches, metadata, buffering and reader integration. Completion requires
upstream-derived tests against a deterministic storage backend, including
partial/ranged reads and propagated storage errors, not just an async adapter.

## Phase 7 — Asynchronous storage writes

- [ ] Write and close Parquet data through a usable asynchronous/object-store
  writer API and read the result back without data loss.

Requires Phase 1 and the storage/concurrency decisions from Phase 6. Include
buffering, flushing, close/finalization and error propagation. Completion
requires upstream-derived end-to-end writer/storage tests and readable output,
not just a sink interface.

## Deferred compatibility decisions

These remain visible in the API ledger and divergence reference. They are not
standalone helper phases or automatic next tasks:

- Generic `DataType`/`make_type!` and `column/mod.rs` typed dispatch: the port
  currently uses Crystal generic classes and `T.class` dispatch. If an upstream
  compatibility feature is chosen, bundle its public typed reader/writer API,
  dispatch, tests and any required macros into one capability.
- Generic mixed-union Thrift macros: concrete logical/column-order/encryption
  unions already have implementations. Introduce generic macro support only as
  a complete reusable API with generated read/write and round-trip tests.
- Remaining tests, benches/examples, memory-size traits and source-only symbols:
  reconcile with their owning feature or document a deliberate divergence;
  do not turn each missing row into a feature commit. The ledger retains all
  outstanding rows and remains the authority for exact symbol coverage.

## Working notes

- Do not weaken upstream assertions or change fixtures to fit the port.
- Run tests/builds with
  `CRYSTAL_CACHE_DIR=/private/tmp/parquet-cr-crystal-cache` in this sandbox.
- `make install` requires `../arrow-cr`; avoid editing installed shards as an
  untracked workaround. Dependency gaps belong in `shards_info/`.
- Canonical inventory scripts live at
  `/Users/dominic/.agents/skills/crystal_forge/skills/cross-language-crystal-parity`.
- Codec cross-language validation, fixture paths and exFAT/AppleDouble notes
  are preserved in [divergences and verification notes](parity/divergences.md).
