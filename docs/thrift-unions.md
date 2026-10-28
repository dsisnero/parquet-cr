# Generated mixed Thrift unions

`define_thrift_union` and `define_thrift_union_with_unknown` port
`parquet_macros.rs`'s `thrift_union!` and `thrift_union_with_unknown!` from
apache/arrow-rs 60.0.0 (`ef1fa157977633f0ba21aa921f9ef3d5c669235b`). They generate
native union types and compact-protocol codecs, including fields/list composition.

```crystal
require "parquet-cr"

define_thrift_struct ChangePayload do
  required 1, count, Int32
  optional 2, enabled, Bool
end

define_thrift_union Change do
  unit 1, Unchanged, unchanged
  variant 2, Changed, changed, ChangePayload
  variant 17, Ids, ids, Array(Int32)
end

value = Change.changed(ChangePayload.new(42, false))
value.kind                     # Change::Kind::Changed
value.changed                  # ChangePayload?
value.changed!                 # ChangePayload
Change::UNCHANGED == Change.unchanged
```

A `unit` declaration takes field id, Kind label and constructor name. Its wire
payload is an empty struct; its uppercase constructor name is also a constant.
A `variant` adds a native payload type and generates a typed constructor,
nullable getter and checked bang accessor. IDs must be distinct Int16 integer
literals; extended and negative IDs use the existing compact-protocol writer.
Kind labels and constructor names must be unique. Forward mode reserves
Kind::Unknown and its `unknown` constructor.
The generated struct's initializer is private. `kind` and, in forward mode,
`unknown_field_id` are reserved implementation fields. Kind ordinal values follow
declaration order; field IDs remain independent wire identifiers.

The native representation is a Kind plus optional payloads, not Rust's tagged
enum. Struct equality compares that representation. Payloads are native owned
values; Rust borrowed string/binary lifetimes are not enforced. Both existing
ThriftSliceInputProtocol and IO-backed ThriftReadInputProtocol work for owned
payloads. `read_value(String)` uses owned bytes, as pinned Rust String does;
the separate borrowed `read_string`/`read_bytes` paths keep their existing IO
limitations. Invalid owned UTF-8 is rejected.

## Strict and forward-compatible behavior

Both macros implement `self.read_thrift`, `write_thrift` and
`self.thrift_element_type`. The shared Thrift field writer and list reader use
these methods when a union is nested in metadata or a list. Unit payloads must
be empty structs. Known variants dispatch by field ID and declared payload type;
the pinned macros do not additionally validate a known field's wire tag.
Exactly one union field is accepted: empty input, a duplicate field or a second
variant raises GeneralError. Truncated input propagates EOFError.

Strict unions reject unknown IDs before reading the body. Forward-compatible
unions use the same declarations with `define_thrift_union_with_unknown`.
They skip the unknown field's entire body, enforce the union's final stop, then
return Kind::Unknown with `unknown_field_id : Int16?`. Only the ID is retained,
matching Rust `_Unknown { field_id }`; the payload is discarded. Known variants
have nil unknown IDs. `Type.unknown(id)` constructs the same unknown value.
Writing it raises `Trying to write unknown variant`, as upstream.

Two pinned quirks are intentionally preserved:

- The mixed-union multi-field diagnostic contains a literal `{}` immediately
  before the union name. Rust's `concat!` goes through general_err's expression
  arm without format substitution. A standalone Rust check of the pinned error
  macro confirms this. The earlier manual production codec normalized the text;
  generated production codecs now follow the pin. The all-empty union macro's
  separately formatted diagnostic remains unchanged.
- A direct Bool payload writes its value in the field header, but pinned
  `__thrift_read_variant` reads a separate bool byte. It does not round-trip.
  Put bool in a struct field (as ChangePayload does) or use Array(Bool). The
  generated accessors correctly retain false; the inherited wire limitation is
  characterized rather than silently changed.

## Production integration and verification

ColumnCryptoMetaData and EncryptionAlgorithm use the generated strict codec.
Their existing Kind ordinals, constants, constructors, nullable getters and
column-key bang accessor remain available. Their wire bytes and native memory
layouts are unchanged. FileCryptoMetaData, column metadata and full encrypted
files therefore use the generator through the existing read/write pipeline.

The 20 specs in `mixed_union_spec.cr` and
`file/mixed_union_integration_spec.cr` were written before implementation. They
cover exact unit/struct/list/extended-ID bytes, text/binary and UTF-8, nested
fields/lists, malformed unions, forward unknown nested map/list/bool skipping,
cursor alignment and pinned quirks. Production wire fixtures passed against the
old codecs before replacement. Complete files round-trip two row groups and
exact values with both encrypted/plaintext footers and footer/column keys.
The metadata-memory specs retain their independent exact layout assertions.

The current source inventory does not expand Rust macro-generated codec methods.
The ledger records production layout and owned-String evidence; this explicit
API guide and integration specs record macro coverage. No generated discovery
rows are invented and no dependency/shard change is required.

```sh
crystal spec spec/parquet/mixed_union_spec.cr \
  spec/parquet/file/mixed_union_integration_spec.cr \
  spec/parquet/file/metadata_memory_spec.cr
```
