# Typed physical-column APIs

The public physical descriptors and typed column bridges follow
`data_type.rs`, `column/{reader,writer/mod}.rs` and `file/writer.rs` in
apache/arrow-rs 60.0.0 (`ef1fa157977633f0ba21aa921f9ef3d5c669235b`).

| Descriptor | Physical type | Native value alias `T` | `get_type_size` |
| --- | --- | --- | --- |
| BoolType | BOOLEAN | Bool | 1 |
| Int32Type | INT32 | Int32 | 4 |
| Int64Type | INT64 | Int64 | 8 |
| Int96Type | INT96 | Parquet::Int96 | sizeof(Int96) |
| FloatType | FLOAT | Float32 | 4 |
| DoubleType | DOUBLE | Float64 | 8 |
| ByteArrayType | BYTE_ARRAY | Parquet::ByteArray | sizeof(ByteArray) |
| FixedLenByteArrayType | FIXED_LEN_BYTE_ARRAY | Parquet::FixedLenByteArray | sizeof(FixedLenByteArray) |

Each descriptor includes `Parquet::DataType`, exposes `PHYSICAL_TYPE` and
`get_physical_type`, and provides the associated native alias `T`.
`get_type_size : Int32` reports the **native representation size**, including
byte-wrapper layout, as upstream does for its own Rust representations. It is
not the encoded size, variable payload length or schema's fixed byte width.
The generation macro `make_type` remains internal implementation machinery.

## Writing and reading a complete file

```crystal
require "parquet-cr"

schema = Parquet::Schema.parse_message_type("message schema { required INT32 id; }")
output = IO::Memory.new
properties = Parquet::File::Properties::WriterProperties.builder.build
file = Parquet::File::SerializedFileWriter.new(output, schema, properties)
group = file.next_row_group
column = group.next_column || raise "missing column"
column.typed(Parquet::Int32Type).write_batch([1, 2, 3], nil, nil)
column.close
group.close
file.close

reader = Parquet::File::SerializedFileReader.new(
  Parquet::File::BytesChunkReader.new(output.to_slice)
)
source = reader.get_row_group(0).get_column_reader(0)
typed = Parquet::Column.get_typed_column_reader(Parquet::Int32Type, source)
values = [] of Parquet::Int32Type::T
records, count, levels = typed.read_records(3, nil, nil, values)
# values == [1, 2, 3]; records == count == levels == 3
```

Optional/repeated columns still require definition/repetition levels. Descriptor
conversion selects the existing physical reader/writer specialization; it does
not change values, levels, codecs, page versions, buffering or finalization.
Serialized columns still close through `SerializedColumnWriter#close`, which
runs the existing row-group callback. Close the row group and file to finalize
all metadata/footer offsets.

## Conversions and ownership

Every descriptor provides nullable `get_column_reader`, `get_column_writer`,
`get_column_writer_ref` and `get_column_writer_mut` conversions. They return
nil for a different physical specialization, matching upstream's Option.

`Column.get_typed_column_reader`, `Column::Writer.get_typed_column_writer`,
`get_typed_column_writer_ref`, `get_typed_column_writer_mut` and serialized
`column.typed` raise GeneralError on mismatches, retaining upstream's diagnostic
text. The panic maps to a native exception, as with the existing column API.
The writer ref/mut helpers support both descriptor classes and native classes.

Native `column.typed(Int32)` and `get_typed_column_reader(Int32, source)` callers
continue to work. Both entry styles return the same reader/writer object.
Implementation generics remain specialized on native values, for example
`GenericColumnWriter(Int32)`; `Int32Type` selects that specialization through
its alias and bridge rather than creating a second encoder implementation.

Crystal shares references and has no consuming moves, immutable borrows or Rust
lifetimes. Owned/ref/mut conversions therefore return the same reference;
conversion does not consume the untyped handle, and failed conversions leave it
usable. Keep one owner of reader progress/writer mutation and finalize each
serialized column through its owning wrapper. Physical constants live on the
descriptors rather than adding associated constants to builtin scalar classes.

## Verification and inventory coverage

`spec/parquet/column/typed_physical_spec.cr` first failed on missing BoolType.
Its 29 cases cover every descriptor/native alias/size, all eight nullable physical
round trips across two row groups and both page versions, all 56 mismatched
physical pairs, optional and raising conversion forms, reference identity and
continued usability after errors. It ports `column/mod.rs`'s nested-list example
with exact values and levels, plus the four upstream definition/repetition/value
validation tests without weakening their messages.

The current source discovery does not expand Rust's make_type macro or inventory
free functions/trait declarations. The ledger's PHYSICAL_TYPE and serialized
`typed` rows record this surface; the explicit table, APIs and integration specs
above cover the generated descriptors and conversion bridges. Private native
value-buffer/codec helpers retain their existing independent inventory states.
No shard or dependency change is required.

```sh
crystal spec spec/parquet/column/typed_physical_spec.cr
```
