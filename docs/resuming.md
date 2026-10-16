# Resuming Work

This is the first thing to read in a new session. It takes a few minutes to get
back to a known-good state.

## 1. Read where we are

- `plans/parity.md` — start at the **Where we are** block. It names the current
  focus, the ordered "Next up" list, and per-area chunk status. Do not re-derive
  state from git history.
- `plans/inventory/rust_port_inventory.tsv` — the per-symbol ledger (status +
  `crystal_refs`). Use it to see what is left inside the current chunk:

  ```bash
  tail -n +2 plans/inventory/rust_port_inventory.tsv | cut -f3 | sort | uniq -c
  ```

## 2. Confirm the working tree

```bash
git log --oneline -10
git status
```

The last commit should be the previous feature (`port: ...`), committed
atomically. If the tree is dirty, finish or stash before starting.

## 3. Confirm a clean baseline

```bash
make lint   # crystal tool format --check src spec + ameba src spec
make test   # crystal spec
```

Both must pass before changing anything. (`make lint`/`make test` also strip
macOS AppleDouble `._*` sidecars — see below.)

## 4. Pick the next feature

Take the first unchecked item in the **Next up** list (or the next `[ ]` item in
the current phase). For each feature:

1. Read the upstream source under `vendor/arrow-rs/parquet` and the nearest
   upstream tests.
2. Port the tests first (red → green); never weaken assertions.
3. Satisfy the **Definition of done** in `CLAUDE.md` (API, tests, gates,
   inventory rows, `plans/parity.md` status + **Where we are** refresh,
   divergences).
4. Commit atomically with the `port: ...` message format from `CLAUDE.md`.

## Environment note (exFAT)

The checkout lives on an exFAT volume, so macOS writes AppleDouble `._*`
sidecar files. A `._foo_spec.cr` sidecar matches crystal's `*_spec.cr` glob and
can break `crystal spec`. The `Makefile` strips them before `format`/`lint`/
`test`. If a git pack operation prints `non-monotonic index .../._pack-*.idx`,
delete the sidecars:

```bash
find .git -name '._*' -delete
```

## If the plan is split

When `plans/parity.md` grows too large it is split into `plans/parity/<area>.md`
files. In that case every split file is linked from `plans/parity.md`, and
`plans/parity.md` marks each file complete/in-progress and names the file being
worked on plus the next file/phase to start.
