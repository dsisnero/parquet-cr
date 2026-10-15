# Testing

## Gates

Run the full gates before closing a feature:

```bash
crystal tool format --check src spec
ameba src spec
crystal spec
```

`make lint` and `make test` wrap these.

## Porting tests early

Port upstream tests as first-class work. For each upstream module, locate the
nearest `#[cfg(test)]` module or `parquet/tests/*` file and port its assertions
directly into a Crystal spec.

- Put specs under `spec/parquet/...` mirroring `src/parquet/...`.
- If upstream lacks tests, write characterization specs from observable
  behavior and mark inferred behavior in the spec name or a comment.
- Never weaken assertions or change fixtures to fit the current implementation.

## Red-green cycles

1. Port the next missing or failing parity spec.
2. Make the smallest change that turns it green.
3. Run focused checks (`crystal spec spec/parquet/<file>_spec.cr`).
4. Repeat until the feature is complete.

## Fixtures

Binary fixtures needed for round-trip tests are generated with a small helper in
spec support code so they remain reproducible. Do not commit opaque blobs that
cannot be regenerated.
