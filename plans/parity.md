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

- **Current focus:** finish Arrow interop with a usable synchronous
  `ArrowWriter`. Core synchronous Parquet I/O, the Arrow batch reader, Variant
  operations and page storage already exist. Writer levels are an internal step
  within this feature, not the next standalone feature.
- **Next up**, in order:
  1. **Phase 1 — Arrow RecordBatch writing**, including nested levels, column
     encoding, page-store integration and the public writer API.
  2. **Phase 2 — Complete Variant numeric conversions**, closing the existing
     Decimal256 output and decimal exponent-notation gaps.
  3. **Phase 3 — Canonical extensions and virtual columns**, once arrow-cr
     supplies the canonical extension types; advance to Phase 4 while blocked.
  4. **Phase 4 — Parquet rewrite command**, using the complete reader/writer.
  5. **Phase 5 — CSV import command**, using the complete writer.
  6. **Phases 6–7 — Asynchronous storage reads and writes**, deferred until the
     Crystal concurrency and object-store mapping is decided.
- **Last completed feature:** `b9eeca7`, `port: column page store`. Includes all
  five upstream tests plus five contract characterizations, ported tests-first
  through two red-green cycles. Page-store configuration and routing through
  `ArrowWriter` belong to Phase 1.
- **Last code gates:** `make lint` (179 files), `make test` (1664 examples,
  zero failures/errors), `crystal build --no-codegen src/parquet-cr.cr`, and
  scoped inventory validation (3515 tracked items), all passed for `b9eeca7`.
  The roadmap regrouping changes documentation only; these are the recorded
  code results, not a new implementation signoff.
- **Ledger counts:** 2552 `ported`, 207 `intentional_divergence`, 17 `skipped`,
  739 `missing` (3515 total). Regrouping does not change symbol status.
- **Plan layout:** this file is the active feature queue and completion record.
  [Implementation reference](parity/implementation-reference.md) is a complete
  archival snapshot of earlier module-level evidence, including stale historical
  checkboxes. [Divergences](parity/divergences.md) is a maintained reference for
  representation differences and verification notes. Neither is a separate
  work queue. Work next in this file, Phase 1; update divergences as needed.

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
| Synchronous Parquet files | Schema/Thrift, encodings/compression, column and file readers/writers, metadata, page indexes, bloom filters, record API | Generic API compatibility decisions below; Arrow batch writing is Phase 1 |
| Arrow batch reading | Schema conversion both ways, IPC schema metadata, primitive/composite readers, selection, filtering, pruning, metrics and push decoding | Canonical extension/virtual-column integration in Phase 3; individual coverage gaps remain in the ledger |
| Variant binary and JSON | Paths, values/builders, exact binary encoding, JSON in both directions | Recorded ownership and calendar-range divergences |
| Variant Arrow operations | Arrays/builders, safe/strict casts, path extraction, shredding and unshredding, scalar/composite/encoded arrays | Numeric conversion completion in Phase 2 |
| Geospatial | GeoParquet metadata, bounds, WKB support and core statistics integration | WkbType Arrow extension integration in Phase 3; ISO WKB limitation documented |
| Derived records | Schema generation and typed record read/write round trips | Recorded Crystal macro/trait representation differences |
| Modular encryption | Cipher/metadata/properties and synchronous encrypted file/page read/write paths | Recorded unsupported upstream modes and representation differences |
| Serialized page storage | Opaque UInt64 keys, store/factory contracts, column context, in-memory backend and resident-byte accounting | Wire into ArrowWriter in Phase 1 |
| Existing command-line tools | Inspect/read/index/schema/rowcount/layout/bloom-filter and concatenate workflows | Rewrite and CSV import are Phases 4–5 |

## Phase 1 — Arrow RecordBatch writing

- [ ] Write Arrow `RecordBatch`es through the public synchronous `ArrowWriter`
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

## Phase 2 — Complete Variant numeric conversions

- [ ] Convert the remaining supported Variant numeric values to typed Arrow
  output with upstream safe/strict semantics and decimal precision intact.

Scope includes Decimal256 primitive and union output, decimal exponent notation,
and shared conversion logic used by casts, path extraction and shredding.
Arrow-to-Variant Decimal256 input already works and remains covered. Reuse
arrow-cr's existing I256 support; record any additional dependency gaps.

Completion evidence: upstream-derived numeric boundary and overflow tests,
exponent/scale/precision cases, union selection, SQL null versus Variant null,
safe/strict errors, and public-kernel round trips. Decimal helpers alone do not
complete this phase.

## Phase 3 — Canonical extensions and virtual columns

- [ ] Read/project virtual columns and round-trip supported canonical extension
  metadata through the Arrow/Parquet schema and array interfaces.

Blocked on the required canonical extension APIs in `../arrow-cr`; consult
`shards_info/arrow_cr.*.md` and verify the blocker before starting. Scope includes
`arrow/schema/virtual_type.rs`, `arrow/schema/extension.rs`, virtual field/level
conversion, the virtual array reader and public reader/projection wiring, plus
`parquet-geospatial`'s `WkbType` Arrow extension implementation. Existing
primitive schema conversion and ProjectionMask behavior are already delivered.

Completion evidence: upstream extension validation/metadata tests and public
reader tests for virtual-column values, projection and row selection, plus WKB
extension/schema round trips. Do not mark complete when only the extension
constant or schema type exists.

## Phase 4 — Parquet rewrite command

- [ ] Run `parquet-rewrite` end to end with upstream options and equivalent
  data/schema in the output Parquet file.

Requires Phase 1. Include command parsing, reader-to-writer streaming, supported
rewrite options, errors and executable wiring. Port upstream CLI tests first;
verify output with both the local reader and the pinned upstream reader where
available. Parser helpers alone do not complete the command.

## Phase 5 — CSV import command

- [ ] Run `parquet-fromcsv` end to end to produce valid Parquet with upstream
  schema/type/null handling and command behavior.

Requires Phase 1 and any needed arrow-cr CSV facilities. Include executable
wiring, parsing, supported import options and errors. Port upstream tests first;
verify typed/null values and schema after reading the output file. Schema
inference or argument parsing alone does not complete the command.

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
