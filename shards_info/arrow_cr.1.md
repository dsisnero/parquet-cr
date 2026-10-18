# arrow-cr needs / bugs: Arrow ↔ Parquet interop (chunk 1)

Requested by `parquet-cr` while porting `parquet/src/arrow/`
(apache/arrow-rs `60.0.0`, `ef1fa157977633f0ba21aa921f9ef3d5c669235b`).

`parquet-cr` depends on this repo via `path: ../arrow-cr`. The Arrow → Parquet
schema direction and the `ARROW:schema` metadata round-trip are ported and use:

- `Arrow::Schema`, `Field`, `Fields`, `Metadata`, `DataType` +
  `TimeUnit`/`IntervalUnit` (all fine),
- **`Arrow::Ipc.schema_message(schema)`** (upstream
  `IpcDataGenerator::schema_to_bytes_with_dictionary_tracker(...).ipc_message`)
  and **`Arrow::Ipc.read_message(bytes)`** (upstream `root_as_message` +
  `header_as_schema` + `try_fb_to_schema`) for `encode_arrow_schema` /
  `get_arrow_schema_from_metadata` — thank you, these were exactly what was
  needed (an earlier note claimed `arrow-ipc` was missing; it was stale).

Two follow-ups:

## 1. Bug: `Field#with_*` mutates the receiver

`arrow-cr/src/arrow/field.cr`:

```crystal
def with_name(name : String) : self
  @name = name
  self
end
```

Same for `with_data_type`, `with_nullable`, `with_metadata` (all overloads).

Upstream `arrow-schema`'s `Field::with_*` take `self` **by value** and return a
new `Field`; callers rely on the original being untouched:

```rust
pub fn with_name(mut self, name: impl Into<String>) -> Self { ... }
```

Because the port mutates in place, any consumer that builds a modified copy of a
field corrupts the caller's schema. This already bites `parquet-cr`: coercing
list element / map key+value names (`with_coerce_types(true)`) renamed the
fields *in the caller's Arrow schema*, so a later uncoerced conversion saw the
coerced names. `parquet-cr` works around it by constructing
`Arrow::Field.new(name, data_type, nullable?, metadata)` copies
(`Parquet::Arrow.renamed_field`/`retyped_field`), which should be deleted once
`with_*` returns a copy.

**Needed:** make `Field#with_name`/`#with_data_type`/`#with_nullable`/
`#with_metadata` return a new `Field` (keep the setters `name=`, `metadata=`,
... for in-place mutation) and add specs asserting the receiver is unchanged.
Please audit `Schema#with_metadata`, `Fields` helpers and `DataType` builders for
the same pattern.

## 2. Bug (robustness): flatbuffer decoding raises `IndexError` on bad input

`Arrow::Ipc.read_message` on a truncated/garbage buffer raises
`IndexError: Index out of bounds` from `Arrow::Ipc::FlatBuffer#field`/`#i32`
instead of an `ArrowError`. Upstream's `flatbuffers` crate *verifies* the buffer
and returns a typed error, which `parquet` wraps as
`Arrow: Unable to get root as message stored in ARROW:schema, …`.

This matters for `parquet-cr` because `ARROW:schema` comes from untrusted file
metadata: `Parquet::Arrow.get_arrow_schema_from_metadata` currently has to
`rescue` every exception (`rescue error : ParquetError` re-raised, anything else
wrapped as `ArrowError`) to reproduce upstream's behavior.

**Needed:** bounds-check flatbuffer reads (root table/vector/vtable offsets) and
raise `ArrowError.new("Parser error: …")` for invalid buffers; add specs that
feed truncated and random bytes to `read_message`/`schema_from_flatbuffer`.
Once that lands, `parquet-cr` can narrow its rescue to `ArrowError`.

## 3. Optional: canonical extension types

Upstream maps some Arrow extension types to Parquet logical types, but only when
`arrow_canonical_extension_types` / `geospatial` / `variant_experimental` are
enabled — all **off** in upstream's default feature set, which is what
`parquet-cr` ports. Needed if we ever enable that mapping:

- `arrow-schema`'s `ExtensionType` trait plus the canonical `Uuid` and `Json`
  extension types,
- `ARROW:extension:name` metadata helpers (`Field#extension_type_name`,
  `Field#try_extension_type`, `Field#try_with_extension_type`).

Low priority: without them `parquet-cr` follows upstream's default build
(`Utf8` → `STRING`, no other extension-derived logical types), and the Parquet
side would also need `parquet-variant-compute`/`parquet-geospatial` extension
types.
