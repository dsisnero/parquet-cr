# Asynchronous storage reads

The pinned upstream is apache/arrow-rs 60.0.0, commit
`ef1fa157977633f0ba21aa921f9ef3d5c669235b`. Phase 6 ports
`parquet/src/arrow/async_reader/{mod,metadata,store,spawn}.rs` and the async
loaders in `file/metadata/reader.rs`.

## Execution model and ownership

Rust `BoxFuture`/`await` maps to a Crystal method that may suspend the calling
fiber; `futures::Stream` maps to `Iterator(Arrow::RecordBatch)`. Run a scan in
`spawn` to overlap it with other work. Network IO yields to Crystal's scheduler.
The decoder remains owned by the scanning fiber, and only requests data needed
for the next batch or row-group reader. It does not collect the whole file or
launch a background prefetch producer. A stream must have one consumer; use
separate builders/streams to scan concurrently.

`ParquetRecordBatchStreamBuilder.new(source)` loads metadata, while `build`
constructs a lazy stream. All existing builder settings apply: projection,
ordered row groups, global/local row selections, predicate pushdown/cache,
metrics, batch size, offset and limit. `next_row_group` returns an independent
batch reader; use either batch iteration or row-group iteration for a scan.
Storage/decode errors raise once, then both iteration methods remain finished.
Schema projection preserves field metadata and strips top-level user metadata,
as upstream. Virtual fields remain independent of physical projection.

`ArrowReaderMetadata.load_async` and `new_with_metadata` allow metadata reuse
without additional metadata IO. `get_row_group_column_bloom_filter` reads only
the auxiliary bloom bytes, including the legacy path without a recorded length.
Reader options carry index policies, metadata statistics policies, supplied
Parquet/Arrow schemas, virtual columns and file decryption properties through
the shared footer and page decoding machinery.

For blocking storage callbacks, or to isolate IO from decoding, wrap a source in
`SpawnedReader.new(source, context)` with an explicit `Fiber::ExecutionContext`.
The context is caller-owned; this library does not resize the global context.
Contexts are not OS-thread identities; tests assert the configured context.
Crystal shares reader references rather than requiring Rust `Clone`, so the
wrapper serializes access with `Sync::Mutex`. Reply channels have capacity one;
exceptions retain their identity across the context boundary. Crystal contexts
are process-managed and do not expose tokio runtime shutdown or task-cancellation
errors. Those Rust shutdown tests have no equivalent lifecycle API. No automatic
cancellation/forced interruption is promised for an in-flight storage call.

`AsyncSeekableReader.new(io)` adapts a borrowed seekable IO, using a mutex around
each seek/read pair and strict short-read checks. It leaves the IO open. Whether
its underlying IO yields or blocks depends on that implementation; an explicit
execution context is available when needed. Uint64 absolute positions and file
sizes are retained, but individual buffers, metadata lengths and row counts use
the port's Int32 APIs.

## Storage dependency decision

The upstream `object_store` crate is a provider-independent interface with
separate cloud implementations. The Crystal port uses a native `ObjectStore`
range/suffix/head contract and supplies `MemoryObjectStore` and `HttpObjectStore`.
No shard installation is required; `shard.yml` and `shard.lock` are unchanged.
Existing applications can implement the same contract around their chosen SDK.

Candidates considered:

| Candidate | Fit and tradeoff | Decision |
| --- | --- | --- |
| [awscr-s3](https://github.com/taylorfinnell/awscr-s3) | S3-specific client, documented installation/API, MIT; provider authentication and SDK maintenance would become a mandatory dependency | Optional application adapter |
| [aws](https://github.com/jgaskins/aws) | AWS clients for S3/SQS/SNS; broader AWS scope still does not model a provider-neutral storage interface | Optional application adapter |
| Crystal stdlib HTTP + native contract | Existing runtime dependency; supports real range endpoints, caller-supplied headers/query authentication, local deterministic tests and custom providers; no built-in cloud credential discovery/signing | Chosen |

The exact resulting dependency state is **no additional `shard.yml` entry and no
`shards install` step**. This is not a replacement cloud SDK: automatic signing,
credential discovery, listing, multipart uploads and provider-specific retries
belong to application adapters. Object keys are Strings instead of Rust Path.

## Object reads and metadata IO

```crystal
require "parquet-cr"

alias AsyncParquet = Parquet::Arrow
store = AsyncParquet::HttpObjectStore.new("https://objects.example/data/")
source = AsyncParquet::ParquetObjectReader.new(store, "events.parquet")
stream = AsyncParquet::ParquetRecordBatchStreamBuilder.new(source)
  .with_batch_size(1024)
  .with_limit(10000)
  .build
stream.each { |batch| puts batch.num_rows }
```

`HttpObjectStore` requires actual `206 Partial Content` responses and validates
`Content-Range` and body length before publishing bytes. It rejects ignored or
incorrect ranges rather than silently downloading an entire object. URI paths
are escaped and optional caller-supplied headers/base query parameters are
preserved. Transport failures include the object key and retain their cause.
The default multi-range implementation reads requested ranges in order; a custom
store can override `get_ranges` to coalesce requests or issue concurrent reads.

With a known size, call `source.with_file_size(store.head(key))` or pass a size
already obtained by the application. Without a size, the metadata reader issues
suffix GETs directly and does not perform HEAD. `with_footer_size_hint` controls
the initial read (minimum 8 bytes); sufficiently large bounded hints avoid a
second footer/index fetch. Metadata loading preserves upstream remainder reuse
and checks truncated/corrupt footers. Page indexes are loaded on demand using
absolute bounded ranges, even without knowing the full file size.

The deprecated upstream `ParquetObjectReader` options are retained:
`with_preload_column_index`, `with_preload_offset_index`, `with_file_size`,
`with_footer_size_hint` and `with_runtime(context)`. Default `Skip` policies use
the preload flags; if either explicit Arrow index policy is non-Skip, both
explicit policies override the preload settings. `SpawnedReader` is the preferred
way to move an entire source's operations to another execution context.

## Verification

The `async_reader_spec.cr`, `async_metadata_spec.cr`, `async_store_spec.cr` and
`async_spawn_spec.cr` specs port upstream tests and characterize the Crystal
mapping. They cover recorded projection/page-pruning requests, exact arrays and
schemas, the upstream 100-case selection fuzz test with a reproducible seed,
row-group-local selections, nested/null data, virtual columns, encrypted
footers/pages, bloom filters, cache/filter/limit wiring, prefetch request counts,
metadata policies, missing/short/corrupt reads, fused errors and real execution
contexts. HTTP integration uses an ephemeral **loopback-only** server; sandboxed
runs need permission to bind a local socket. No cloud credentials or external
service is needed. Run `make lint`, `make test`, and `make test-extensions`.
