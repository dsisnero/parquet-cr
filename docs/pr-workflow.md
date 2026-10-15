# PR Workflow

## Branching

- One feature per branch, named `port/<feature>`.
- Keep branches small; split large upstream modules into several features.

## Commit discipline

Commit after every completed feature in `plans/parity.md`. A feature may span
many files but must land as one atomic commit.

Commit message format:

```
port: <feature name>

- <what was implemented>
- <tests added>
```

Before committing:

1. `git status` and `git diff --stat` to review scope.
2. Stage only files belonging to the feature plus inventory/plan updates.
3. Run `crystal tool format --check src spec` and
   `crystal build --no-codegen src/parquet-cr.cr`.
4. Never commit secrets, generated blobs, or the `vendor/` submodule contents.

## Review checklist

- Upstream source module and nearest tests were read before coding.
- Parity inventory rows updated for affected symbols.
- Feature checkbox in `plans/parity.md` checked.
- Any intentional divergence documented.
