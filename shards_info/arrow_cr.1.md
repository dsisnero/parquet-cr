# arrow-cr needs: Arrow ↔ Parquet schema interop (chunk 1)

Requested by `parquet-cr` while porting `parquet/src/arrow/` (apache/arrow-rs
`60.0.0`, `ef1fa157977633f0ba21aa921f9ef3d5c669235b`).

`parquet-cr` now depends on this repo via `path: ../arrow-cr` and ports the
Arrow → Parquet schema direction (`Parquet::Arrow::ArrowSchemaConverter`).
Everything that direction needs already exists (`Arrow::Schema`, `Field`,
`Fields`, `Metadata`, `DataType` + `TimeUnit`/`IntervalUnit`). Two follow-ups:

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
new `Field`; callers rely on the original field being untouched:

```rust
pub fn with_name(mut self, name: impl Into<String>) -> Self { ... }
```

Because the port mutates in place, any consumer that builds a modified copy of a
field corrupts the caller's schema. This already breaks `parquet-cr`: coercing
list element / map key+value names (`with_coerce_types(true)`) renamed the
fields *in the caller's Arrow schema*, so a subsequent uncoerced conversion saw
the coerced names. `parquet-cr` works around it by constructing
`Arrow::Field.new(name, data_type, nullable?, metadata)` copies
(`Parquet::Arrow.renamed_field`/`retyped_field`), which should be deleted once
`with_*` returns a copy.

**Needed:** make `Field#with_name`/`#with_data_type`/`#with_nullable`/
`#with_metadata` return a new `Field` (keeping the setters `name=` etc. for
in-place mutation), and add specs that assert the receiver is unchanged.
Please also check `Schema#with_metadata`/`Fields` helpers for the same pattern.

## 2. Missing: `arrow-ipc` schema flatbuffer encode/decode

`parquet/src/arrow/schema/mod.rs` stores the original Arrow schema in the
Parquet key-value metadata under `ARROW:schema`, base64-encoded, using the Arrow
**IPC** schema message:

- `encode_arrow_schema(schema) -> String`:
  `arrow_ipc::writer::IpcDataGenerator::schema_to_bytes_with_dictionary_tracker`
  plus a legacy 8-byte (4×`0xFF` + little-endian length) prefix, then
  `BASE64_STANDARD`.
- `get_arrow_schema_from_metadata(encoded_meta) -> Result<Schema>`:
  base64-decode, strip the prefix, and read the schema back with
  `arrow_ipc::reader::read_schema_from_bytes`.

`parquet-cr` cannot port `encode_arrow_schema` /
`add_encoded_arrow_schema_to_metadata` / `get_arrow_schema_from_metadata` (and
therefore the fidelity path of `parquet_to_arrow_schema`, which prefers the
embedded Arrow schema over the Parquet-derived one) without it.

**Needed in `arrow-cr`:** `arrow-ipc` support for the schema message at least:

- a flatbuffer `Schema`/`Field`/`DataType` encoder (writer side) that produces
  the IPC message bytes `IpcDataGenerator::schema_to_bytes` produces, and
- a decoder (`read_schema_from_bytes`) that reconstructs `Arrow::Schema` from
  those bytes (including `DictionaryEncoding`, extension-free metadata, and the
  `Fields` recursion).

The `parquet-cr` entry points to port afterwards are `encode_arrow_schema`,
`add_encoded_arrow_schema_to_metadata`, `get_arrow_schema_from_metadata`, and
`parquet_to_arrow_schema*`'s "use the embedded schema" branch. Please note that
the IPC *format* also drives `parquet-rewrite`/`parquet-fromcsv` later, so a
full `arrow-ipc` port is the long-term answer; only the schema message is
blocking today.

## 3. Optional: canonical extension types

Upstream maps some Arrow extension types to Parquet logical types, but only
when the `arrow_canonical_extension_types` / `geospatial` /
`variant_experimental` features are enabled (they are **off** in upstream's
default feature set, which is what `parquet-cr` ports):

- `arrow-schema`'s `ExtensionType` trait plus the canonical `Uuid` and `Json`
  extension types (for Parquet `UUID` / `JSON` logical types),
- `ARROW:extension:name` metadata helpers (`Field#extension_type_name`,
  `Field#try_extension_type`, `Field#try_with_extension_type`).

Without them `parquet-cr` follows upstream's default build: `Utf8` → `STRING`
and no other extension-derived logical types. This is only needed if we decide
to enable the canonical-extension mapping (it also needs
`parquet-variant-compute`/`parquet-geospatial` extension types on the Parquet
side). Low priority.
