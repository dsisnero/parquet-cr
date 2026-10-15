# Coding Guidelines

## Fidelity first

Port behavior, not style. Preserve parameter order, edge cases, and
invalid-input behavior. Express the result with Crystal idioms only where
semantics stay unchanged.

## Types and numeric widths

- Use explicit numeric widths when signedness or range matters.
- Use `Int32`/`Int64`/`UInt32`/`UInt64` deliberately; do not collapse to
  `Int32` by default.
- Use `Bytes` for binary data, never `String`.
- Preserve upstream data-structure and boundary semantics when behavior depends
  on them (for example, little-endian on-disk layout).

## Naming

- Keep upstream module and type names where practical so files remain mappable.
- Crystal type names are `CamelCase`; methods and variables are
  `snake_case`; constants are `SCREAMING_CASE`.
- Record intentional naming differences in the port inventory `notes` column.

## Errors

- Mirror upstream error variants so callers can branch as they do upstream.
- Do not swallow errors; propagate the closest matching variant.

## Comments

Do not add comments unless behavior is non-obvious or a divergence needs
explaining. Prefer self-documenting code.
