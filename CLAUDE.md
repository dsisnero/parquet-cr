# parquet-cr

Crystal port of the Apache Parquet `parquet` crate from
[apache/arrow-rs](https://github.com/apache/arrow-rs), vendored at
`vendor/arrow-rs` (release `60.0.0`, commit
`ef1fa157977633f0ba21aa921f9ef3d5c669235b`).

## Commands

```bash
make install   # shards install
make update    # shards update
make format    # crystal tool format src spec
make lint      # format --check + ameba
make test      # crystal spec
make clean     # rm -rf ./temp/*
```

## Docs

- [Architecture](docs/architecture.md)
- [Development](docs/development.md)
- [Coding Guidelines](docs/coding-guidelines.md)
- [Testing](docs/testing.md)
- [PR Workflow](docs/pr-workflow.md)

## Principles

1. Upstream behavior is the source of truth; `vendor/arrow-rs/parquet` wins over
   local preference.
2. Port behavior, not style.
3. Port upstream tests early and never weaken assertions.
4. Track parity in `plans/parity.md` and `plans/inventory/`.
5. Commit one completed feature at a time.
