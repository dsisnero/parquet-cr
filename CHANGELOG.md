# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Porting tracks the Apache Arrow Rust `parquet` crate. The pinned upstream
revision is recorded per entry.

## [Unreleased]

### Added

- Initial project scaffolding for the Crystal port of `parquet`
  (apache/arrow-rs `60.0.0`, `ef1fa157977633f0ba21aa921f9ef3d5c669235b`).
- Vendored upstream as a git submodule at `vendor/arrow-rs`, pinned to
  `60.0.0`, with `parquet-testing` fixtures initialized.
- Baseline tooling: `Makefile`, `.ameba.yml`, `.rumdl.toml`, `.gitignore`,
  `shard.yml`.
- Baseline docs: `README.md`, `CLAUDE.md`/`AGENTS.md`, and
  `docs/{architecture,development,coding-guidelines,testing,pr-workflow}.md`.
- Rust parity ledger under `plans/inventory/` (3,515 API items, 2,059 tests) and
  the `plans/parity.md` porting roadmap.

### Ported

- `errors` — `ParquetError` variant set (`GeneralError`, `NYIError`, `EOFError`,
  `ArrowError`, `IndexOutOfBoundError`, `ExternalError`, `NeedMoreDataError`,
  `NeedMoreDataRangeError`), error macros, and upstream `Display` strings.
- Thrift compact protocol (`src/parquet/thrift.cr`) — `FieldType`,
  `ElementType`, `OrderedF64` (IEEE 754 total order),
  `ThriftSliceInputProtocol`, `ThriftReadInputProtocol`,
  `ThriftCompactOutputProtocol`, `read_thrift_vec`, and `validate_list_type`,
  with upstream protocol tests in `spec/parquet/thrift_spec.cr`.
- Thrift IDL macro equivalents (`src/parquet/macros.cr`): `define_thrift_enum`
  and `define_thrift_union_all_empty` synthesize compact-protocol support for
  natively declared Crystal enums.
- `basic` thrift enums (`src/parquet/basic.cr`): `Type`, `ConvertedType`,
  `FieldRepetitionType`/`Repetition`, `Encoding`, `CompressionCodec`,
  `PageType`, `BoundaryOrder`, and the all-empty union `TimeUnit`, with
  `from_str` conversions and upstream `basic.rs`/`parquet_thrift.rs` tests
  (`spec/parquet/basic_spec.cr`).
- `define_thrift_struct` (`src/parquet/macros.cr`): generates a Crystal struct
  plus compact-protocol read/write from a declarative field block.
- `basic::LogicalType` (`src/parquet/basic.cr`): the union plus `DecimalType`,
  `TimestampType`/`TimeType`, `IntType`, `VariantType`, `GeometryType`,
  `GeographyType`, and `EdgeInterpolationAlgorithm`; `ConvertedType.from` and
  `LogicalType.from_str`. Upstream `basic.rs` tests for these ported to
  `spec/parquet/basic_spec.cr`.
- `data_type` value types (`src/parquet/data_type.cr`): `Int96`, `ByteArray`,
  `FixedLenByteArray`, `Decimal`, and the `AsBytes` trait; upstream
  `data_type.rs` tests ported to `spec/parquet/data_type_spec.cr`.
- `basic::SortOrder` and `basic::ColumnOrder` (`src/parquet/basic.cr`):
  `column_order_for_type`, sort-order resolution by logical/converted/physical
  type, `TypeDefinedOrder` handling, and union read/write. Upstream `basic.rs`
  sort/column-order tests and `test_parse_encoding` ported to
  `spec/parquet/basic_spec.cr`.

[Unreleased]: https://github.com/dsisnero/parquet-cr/commits/main
