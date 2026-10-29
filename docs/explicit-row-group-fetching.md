# Explicit row-group fetching

Run the pinned `read_with_rowgroup` workflow with:

```sh
crystal run examples/read_with_rowgroup.cr
crystal run examples/read_with_rowgroup.cr -- path/to/file.parquet
```

The default is the vendored `alltypes_plain.parquet` fixture, matching upstream.
An optional input path replaces upstream test-data environment discovery.
The runner opens the file through `AsyncSeekableReader`, loads metadata once,
then fetches and prints each row group's Arrow batches using Arrow's existing
pretty-table renderer. AsyncFileReader operations may suspend the current
Crystal fiber; no Tokio runtime is needed.

The reusable source is `Parquet::Examples::ReadWithRowGroup::InMemoryRowGroup`:

```crystal
metadata = input.get_metadata
mask = Parquet::Arrow::ProjectionMask.leaves(metadata.file_metadata.schema_descr, [0])
group = Parquet::Examples::ReadWithRowGroup::InMemoryRowGroup.create(metadata, 0, mask)
group.async_fetch_data(input)
group.build_reader(1024).each { |batch| puts Arrow.pretty_format_batches([batch]) }
```

`async_fetch_data` requests every projected column's full byte range, one at a
time, in leaf order. Every explicit call refetches these chunks. Its optional
selection argument is deliberately ignored during fetching, matching the pin;
pass selection to `build_reader` to filter decoded rows. Empty projection issues
no data requests and yields zero-column batches with the row group's row count.
This is a manual fetch/decode example, not the optimized page-aware async stream.

Each `build_reader` starts an independent decoder and one-shot column iterator.
Schema conversion uses no embedded Arrow field hints, as in upstream. An
unfetched column raises the pinned `Invalid column index ..., column was not
fetched` error. Clone owns metadata, the projection and the chunk-slot vector;
chunk data objects/bytes remain shared like upstream Arc. Treat shared bytes and
metadata as immutable during reading. Native row/batch/chunk offsets use Int32,
while fetch ranges use UInt64. Existing dense readers guard invalid offsets with
GeneralError instead of Rust slice panics.

The source takes the column-slot vector before fetching. If IO fails, the row
group remains with an empty slot vector, including loss of earlier fetched
slots; this behavior is retained. Discard that failed row-group instance.
The source example's derived Clone is implemented, while Debug formatting is not
reproduced. The inherited core RowGroups adapter supplies existing encryption
context handling; encrypted example parity is not claimed here.

Source: apache/arrow-rs 60.0.0, `ef1fa157977633f0ba21aa921f9ef3d5c669235b`,
`parquet/examples/read_with_rowgroup.rs`. The source has no unit tests. Eight
characterization specs were written first, observed red on the absent API, then
implemented. They cover exact fixture batches/tables and ranges, multiple row
groups, nullable values, projection, selection, independent readers/iterators,
clone ownership, repeated fetches, empty projection, failed-fetch state and the
reused dense reader's absolute offsets.
