# Resolved: Decimal256 IPC schema encoding and decoding

Found while porting the complete synchronous Parquet ArrowWriter against
apache/arrow-rs 60.0.0 (`ef1fa157977633f0ba21aa921f9ef3d5c669235b`). The installed
path dependency points at `../arrow-cr` (HEAD at discovery:
`04c217a4465ecae241db0de604f1a35950fabdae`; that checkout has unrelated local
changes, which this task has not modified).

The user explicitly requested **no shard patch**. The Arrow repository was
independently fixed in `9a5612c` (`port: support Decimal256 IPC schemas for
Parquet writers`). The original Parquet default-options test now passes. This
task made no dependency edits, fork, branch push or PR. This note preserves the
reproduction and records the required sibling dependency revision.

## Reproduction

From parquet-cr:

```sh
CRYSTAL_CACHE_DIR=/private/tmp/parquet-cr-crystal-cache crystal spec \
  spec/parquet/arrow/arrow_writer/writer_spec.cr \
  --example "round trips Decimal256 stored as fixed-width big-endian bytes"
```

This upstream-derived writer test constructs a nullable Decimal256(60, 3) array,
creates an ArrowWriter with default options and round-trips its values. Before
9a5612c, it failed before encoding pages:

```text
Not Implemented: IPC cannot encode Decimal256(60, 3) (NotImplementedError)
lib/arrow-cr/src/arrow/ipc/schema.cr: build_field_type
src/parquet/arrow/schema.cr: encode_arrow_schema
src/parquet/arrow/schema.cr: add_encoded_arrow_schema_to_metadata
```

## Behavior supplied by 9a5612c

Arrow IPC Decimal schemas support a bit width of 256. In
`src/arrow/ipc/schema.cr`:

- `build_field_type` now dispatches Decimal256 with bit width 256, preserving
  precision and signed scale alongside Decimal32/64/128.
- `decimal_data_type` now decodes width 256 as `DataType.decimal256`.
- The Arrow change includes dedicated IPC Decimal256 schema round-trip specs;
  the host's original default-options writer test verifies the dependency fix
  through public Parquet write/read integration.

The Parquet writer's Decimal256 physical-byte and reader paths work when callers
explicitly use `ArrowWriterOptions#with_skip_arrow_metadata(true)`. A separate
passing test covers this option; it does not replace or weaken the default
writer test. The original test and full gates now pass (1748 examples).
