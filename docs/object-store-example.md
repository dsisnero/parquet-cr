# Object-store round-trip example

```sh
crystal run examples/object_store.cr
```

This runs the complete pinned `parquet/examples/object_store.rs` workflow using
a native in-memory object store. It needs no network service or credentials:

```text
read 3 rows
read 3 rows via dedicated I/O runtime
```

It writes the non-null Int64 column `col = [1,2,3]` through
`AsyncArrowWriter` and `BufferedObjectWriter`, completes publication, then reads
the object twice. Both reads check exact complete-batch equality with the
original, including its schema. The first read uses the caller's execution
context; the second uses `SpawnedReader` with a separate one-worker Crystal
parallel execution context. The executable raises a Parquet GeneralError on a
batch mismatch, replacing upstream's assertion panic.

The reusable example adapter is
`Parquet::Examples::ObjectStoreExample::ObjectStoreReader`. It implements
`AsyncFileReader` and `MetadataSuffixFetch`, delegating byte ranges, ordered
multi-range reads and suffix reads directly to the native `ObjectStore`.
Metadata is loaded with `ParquetMetaDataReader.with_arrow_reader_options` and
suffix fetches, so no HEAD request or file size is required. Metadata options,
including independent column/offset index policy, are retained. Storage errors
become `ExternalError` carrying the original exception; footer parse errors
remain metadata errors. Clone shares the store reference and immutable path,
matching the source Arc/path contract. Metadata is fetched anew on each call.

The example reuses existing native storage, buffered publication and spawned
reader implementations. The buffered writer holds the complete object in
memory and publishes with one PUT. Object-store SDK/multipart behavior is not
reproduced. Async calls may suspend Crystal fibers; its execution context
replaces Tokio's runtime. Crystal manages worker lifetime; there is no matching
explicit `shutdown_background` call. The test proves execution-context/thread
separation, without claiming a speedup. Source Debug formatting is not reproduced.
Counts and suffix sizes are Int32; fetch ranges use UInt64 and paths use String.

Source: apache/arrow-rs 60.0.0, `ef1fa157977633f0ba21aa921f9ef3d5c669235b`.
The two source main assertions were ported first, supplemented by adapter
characterizations. Six specs cover the complete example, actual dedicated IO,
ordered range delegation, suffix-only metadata, index options, cloned readers,
publication timing and storage-versus-parser error conversion. Initial red was
the absent example API. No installed shard or dependency changes are required.
