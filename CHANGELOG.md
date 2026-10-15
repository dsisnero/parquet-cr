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

[Unreleased]: https://github.com/dsisnero/parquet-cr/commits/main
