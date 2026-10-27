# Metadata memory estimates

`Parquet::File::Metadata::ParquetMetaData#memory_size : Int64` estimates retained
bytes for a metadata cache. It is available on metadata loaded by synchronous,
push and asynchronous readers, including page indexes and decryption state.
The implementation ports `file/metadata/memory.rs`, `ParquetMetaData::memory_size`
and related schema/encryption/geospatial accounting from apache/arrow-rs 60.0.0
(`ef1fa157977633f0ba21aa921f9ef3d5c669235b`).

```crystal
reader = Parquet::File::SerializedFileReader.new(
  Parquet::File::BytesChunkReader.new(bytes)
)
puts reader.metadata.memory_size
```

Sum this value over cached metadata entries when enforcing an application byte
budget. The estimate includes this metadata object and its retained descendants;
it is not a process-memory measurement, allocator statistic or compressed file
size. Loading page indexes or retaining more statistics increases the estimate.
The calculation only reads metadata and does not fetch pages or mutate state.

## Ownership rules

The file owns the schema descriptor and schema tree. Row groups do not recount
their schema descriptor, and column chunks do not recount their column descriptor.
Schema leaf descriptors count paths, excluding their already-counted primitive
type. Other shared descendants are counted per reference, as upstream, so
sharing across cache entries or repeated strings/buffers can overestimate actual
unique storage. The method does not deduplicate an arbitrary object graph.

The default PageIndex includes column/offset tables, null/min/max/count buffers,
histograms, byte payloads/offsets and page locations. Opaque custom
PageIndexProvider implementations are excluded, matching pinned Rust's concrete
PageIndex downcast. File decryption properties, explicit key maps, cipher keys
and file AAD are included. Application-owned KeyRetriever state is opaque and
excluded. No callback is invoked to estimate memory.

## Crystal layout mapping

`HeapSize#heap_size` counts descendant allocations, excluding the receiver's
inline/object storage. `MemorySize.retained` additionally counts native class
storage when a reference is retained. Struct inline sizes are counted by their
containing class or array, rather than charged again for every nested value.
These helpers support the native Parquet metadata graph; they are not a general
purpose runtime profiler.

- Classes use `instance_sizeof`; Rust Arc counters and Box wrappers are absent.
- Arrays include their class header and reserved capacity times `sizeof(T)`,
  followed by retained descendants of live elements.
- Hashes include their class storage, native entry/index capacity and live
  key/value descendants. Rust hashbrown's bucket approximation is not reused.
- Strings include Crystal's header, UTF-8 byte length and terminating zero.
- Slice/Bytes storage uses the visible element count. A view cannot reveal its
  original backing allocation's size, and shared views can be counted repeatedly.
- Scalar numbers, booleans, enums and inline value-only structs add no descendant
  bytes. Byte-array statistics include retained min/max payloads.
- LogicalType variants are allocated classes in Crystal, so their object storage
  and retained CRS strings are included. Rust keeps this data inline in enums.
- NaN-count buffers are included with the native page indexes; pinned Rust's
  memory.rs omits this allocated buffer. This is an intentional difference.

Allocator overhead, GC bookkeeping and unused capacity outside a visible Slice
are excluded. Capacity queries use read-only Crystal 1.22 Array/Hash layout
fields because those containers have no public capacity API. A runtime layout
change requires reviewing these queries and the exact native-layout specs.
All totals are Int64. No shard changes or additional dependencies are required.

## Verification

`spec/parquet/file/metadata_memory_spec.cr` ports the upstream metadata and
decryptor tests using independent exact native-layout sums instead of Rust's
Vec/Arc byte constants. It also covers schema sharing, array/hash reserve,
byte statistics/indexes, NaN counts/histograms, native logical types and CRS,
encryption/geospatial payloads, opaque providers/retrievers and real parsed
metadata with loaded indexes. The tests first failed on the missing public API;
exact totals exposed omitted class storage, and a later red-green test exposed
omitted logical-type allocations. Assertions were kept intact.

Run `make lint`, `make test` and `make test-extensions`. A focused run is:

```sh
crystal spec spec/parquet/file/metadata_memory_spec.cr
```
