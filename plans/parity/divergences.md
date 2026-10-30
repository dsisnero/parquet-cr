# Intentional divergences and verification notes

Status: **maintained reference**; existing entries are recorded decisions, with
remaining implementation gaps tracked in the [active plan](../parity.md) and
[API ledger](../inventory/rust_port_inventory.tsv). Update this file when a
feature changes a documented divergence; it is not a separate feature queue.
The next work file is `plans/parity.md`, Phase 7 (asynchronous storage writes).


- Errors: Rust `Result<T, E>` is mapped to Crystal exceptions. Fallible
  functions raise `ParquetError` subclasses instead of returning a result type;
  `ParquetError::External(Box<dyn Error>)` maps to `ExternalError` holding an
  `Exception`. Display strings are preserved exactly.
- Variant value layer: the `Variant` enum is a sealed class hierarchy; built-in
  type names are shadowed inside the class body (built-ins written `::Foo`).
  `Option<()>` → `Bool` (`Variant#as_null`); Rust `From`/`TryFrom` become
  `Variant.from_*` factories; the Rust `Debug` impls become
  `Variant#debug_string(pretty)`/`#debug_to`. Fallible accessors raise instead of
  returning `Result` (see the Errors entry).
- Variant builders: Rust's affine borrows and `Drop`-based rollback are not
  modelled, so an unfinished child builder is rolled back explicitly via
  `abandon` (`ParentState#rollback`), and multiple children may coexist (Rust
  forbids it). `Into<Variant>` is the `VariantValue` union +
  `ValueConversion.to_variant`; bare `Time` (date/UTC/naive) is deliberately not
  a `VariantValue` (wrap with `Variant.from_date`/`from_datetime_utc`/
  `from_naive_datetime`; only `Time::Span` maps via `from_time`). The
  `VariantBuilderExt` trait is a module (`src/parquet_variant/builder/ext.cr`)
  included by `VariantBuilder`/`ListBuilder` and implemented by
  `ObjectFieldBuilder`; its associated `State` type is dropped, `new_list`/
  `new_object` delegate to the fallible `try_*` methods (which raise instead of
  returning `Result`), and `append_null` appends `Variant::Null` (an object
  field's `append_null` inserts nothing, as upstream). `ValueBuilder#splice`
  inserts in place (one tail `memmove`); the old rebuild-the-buffer version made
  deep/large variants quadratic.
- Geospatial bounding: upstream `bounding.rs` traverses `geo-traits` geometries
  and parses WKB with the `wkb` crate. Crystal has neither, so
  `parquet_geospatial/wkb.cr` provides a minimal `WkbReader` + owned
  `WkbGeometry` model (ISO 2D/Z/M/ZM types only) and `GeometryBounder` operates
  on that model. `update_wkb` raises `InvalidArgumentError` on malformed input
  (upstream `ArrowError::ExternalError`); `update_geometry` is public so tests
  can feed the model directly (upstream tests build geometries from WKT via the
  `wkt` crate).
- Core geospatial statistics: `BoundingBox` is a value `struct` exposing scalar
  `xmin`/`xmax`/`ymin`/`ymax`/`zmin`/`zmax`/`mmin`/`mmax` getters instead of Rust
  `(f64, f64)` range tuples, and its `with_*range` builders mutate and return
  `self` (Rust consumes `mut self`). `GeospatialStatistics` is a class
  (`Option<Box<_>>` → plain reference). The
  `OnceLock<Arc<dyn GeoStatsAccumulatorFactory>>` global accumulator factory is a
  module class variable (lazily initialized on first
  `try_new_geo_stats_accumulator`, and `init_geo_stats_accumulator_factory`
  raises `GeneralError` once set; not thread-safe). The accumulators are abstract
  classes, `update_wkb` catches `ParquetGeospatial::InvalidArgumentError`
  (upstream checks `Result::is_err`), `is_valid`/`is_z_valid`/`is_m_valid` keep
  their upstream names, and native heap accounting lives in
  `file/metadata/memory.cr`. The domain types are separate from the thrift structs, converted by
  `convert_geo_stats`/`convert_bounding_box` (read) and
  `geo_stats_to_thrift`/`bounding_box_to_thrift` (write) in place of the upstream
  `WriteThrift` impls on the domain types.
- Content-defined chunking: `ContentDefinedChunker.new` raises `GeneralError`
  (upstream returns `Result<Self>`), `roll_fixed` takes `Bytes` instead of a
  const-generic `&[u8; N]` (same byte loop), `usize` offsets are `Int32`, the
  `FnMut(&mut Self, usize)` callback is a block receiving the chunker, chunk
  validation runs under `{% unless flag?(:release) %}` (upstream
  `debug_assertions`), and assertions raise `GeneralError`. `get_arrow_chunks` is implemented by `Parquet::Arrow.arrow_chunks` and
  `arrow_value_bytes`; integrated Arrow writer round trips exercise CDC. The
  gear-hash table is transcribed verbatim from `cdc_generated.rs`.
- Snappy codec: upstream depends on the `snap` crate (`SnappyCodec` holds a
  `snap::raw::{Encoder, Decoder}`); the port uses the `naqvis/snappy` shard
  (pure Crystal, raw/block format) behind `SnappyCodec`. Divergences: the shard's
  `encode` raises `IndexError` on empty input (0.2.0), so `compress` emits the
  canonical empty block (`0x00`) itself; the shard's `decode(src, dest)` accepts a
  destination at least as large as the recorded length, so `decompress` checks
  equality explicitly (as `snap::raw::Decoder` does); malformed input raises
  `ExternalError` wrapping `Compress::Snappy::SnappyError` (upstream
  `snap::Error` → `ParquetError::External`). No `max_compress_len`
  preallocation is needed: the codec appends the encoded block to the output
  buffer. The shard was probed for roundtrip correctness across sizes 1..300,
  literal boundaries (60/61/255/256/65535/65536), long-distance matches, and
  mixed runs; its output decodes with an independent format-spec decoder.
- LZ4 codecs: upstream uses the `lz4_flex` crate for both the raw block format
  and the frame container. The port uses the `paddor/flint` shard (pure Crystal,
  MIT, `~> 0.1.1`) for blocks (`LZ4RawCodec` holds a `Flint::BlockCodec`;
  `Flint::BlockCodec#compress_raw`/`#decompress_raw_into`) and implements the
  frame container in-tree (`src/parquet/lz4.cr`, `Parquet::Lz4`), since no
  active Crystal shard covers frames. Divergences: flint emits a zero-length
  block for empty input, so `LZ4RawCodec#compress` writes the canonical
  single-token empty block (`0x00`); `Flint::Error` maps to `ExternalError`
  (upstream `lz4_flex` errors → `ParquetError::External`), so decode error
  *messages* come from flint instead of `lz4_flex`; `LZ4RawCodec#decompress`
  does not leave reserved zeros in the output buffer when decompression fails
  (upstream resizes before decoding); `LZ4Codec` (upstream `experimental`)
  emits a single frame with 1 MiB independent blocks rather than streaming 4 KiB
  chunks (`LZ4_BUFFER_SIZE` is `skipped` in the ledger); `try_decompress_hadoop`
  is a private instance method that returns a `Bytes` view instead of writing
  through an `io::Result`; raw blocks are decompressed into an exactly sized
  `Bytes` and then appended. `XxHash32` (frame checksums) is in-tree next to the
  existing `XxHash64` and matches the reference vectors.
- ZSTD/BROTLI codecs: upstream uses the `zstd` (libzstd bindings) and `brotli`
  crates. The port uses the `paddor/zinc` shard (pure Crystal, ISC, `~> 0.2.1`,
  RFC 8878 frames) and the `OrelSokolov/brotli` shard (pure Crystal, MIT,
  `~> 0.3.0`, RFC 7932). Divergences: `zinc` implements compression levels
  -8..4 (raw-fast/Fast/DFast), so `ZSTDCodec` clamps the (upstream-validated)
  `ZstdLevel` into that range — frames stay standard, only the ratio differs
  from libzstd at |level| > 4 (upstream's libzstd clamps internally too);
  `BrotliCodec` is one-shot (`BROTLI_DEFAULT_BUFFER_SIZE` is `skipped` in the
  ledger) with `lgwin = 22` (`BrotliCodec::LG_WINDOW_SIZE`), and a fresh
  `Brotli::Decoder` is created per call because the shard's decoder accumulates
  into an output buffer it never resets; both codecs ignore `uncompress_size`
  (self-describing streams), matching upstream's use of it as a buffer hint;
  `Zinc::Error`/`Brotli::DecodeError` map to `ExternalError` (upstream
  `zstd`/`brotli` IO errors → `ParquetError::External`), so decode error
  *messages* come from the shards. `ZSTDCodec`/`BrotliCodec` reuse one codec
  instance per `create_codec` call (per column writer/reader), as upstream does,
  and `uncompress_size` is only a hint (zinc's bounded `max_output_size:` mode
  exists but is not used, mirroring upstream's reserve-only use of the size).
- Arrow interop, schema (Arrow → Parquet): `src/parquet/arrow/schema.cr` lives
  under `Parquet::Arrow` (upstream `parquet::arrow`, re-exported from
  `arrow/mod.rs`). `ArrowSchemaConverter#with_coerce_types`/`#schema_root`
  mutate the converter and return `self` (upstream consumes `self`);
  `arrow_to_parquet_type`, `field_id`, `decimal_length_from_precision`, and the
  `logical_type_for_*` helpers are module methods (upstream: private free
  functions, plus `decimal_length_from_precision` as `pub fn` in
  `arrow::schema`). `DataType` pattern matching is `case data_type.kind` plus the
  accessor getters. Union (`unimplemented!("See ARROW-8817.")`) raises
  `NYIError`; invalid time units raise `GeneralError` (upstream `panic!`).
  Optional extension mapping uses Crystal compile flags matching upstream's
  `arrow_canonical_extension_types`/`geospatial`/`variant_experimental` Cargo
  features. UUID/JSON, WKB Geometry/Geography and Variant mapping works in both
  directions; without flags it preserves the upstream default behavior. arrow-cr's `Field#with_*` builders currently
  mutate the receiver, so the converter builds copies via
  `renamed_field`/`retyped_field` (see `shards_info/arrow_cr.1.md`).
  `SchemaDescriptor` gained the upstream-derived `==` (`schema`/`leaves`/
  `leaf_to_base`) so descriptors can be compared like upstream's
  `assert_eq!(schema, expected)`. Schema metadata: `encode_arrow_schema` uses
  arrow-cr's `Arrow::Ipc.schema_message` (upstream
  `IpcDataGenerator::schema_to_bytes_with_dictionary_tracker`), and
  `get_arrow_schema_from_metadata` feeds the decoded bytes to
  `Arrow::Ipc.read_message` (upstream strips the legacy prefix and calls
  `root_as_message`). arrow-cr's flatbuffer reader is bounds-checked and only
  raises `ArrowError`, which the port wraps as
  `ArrowError("Unable to get root as message stored in ARROW:schema, …")`;
  hostile/truncated metadata is covered by specs.
  `WriterProperties#key_value_metadata` is a writable `property` (upstream
  `pub(crate)`) so `add_encoded_arrow_schema_to_metadata` can mutate it.
  Upstream's `{:?}` error formatting is approximated with the underlying
  message. `ProjectionMask` exposes its `mask` and a `leaves_with_mask`
  constructor (upstream keeps the field private; its tests construct it
  in-crate), has the upstream-derived `==` (`Option<Vec<bool>>` equality) and
  `leaf_included` → `leaf_included?`, and `without_nested_types` is public
  (upstream `pub(crate)`); `convert_primitive`'s
  `apply_hint`/`from_parquet` are private module methods, and the physical/logical
  dispatch is a `case` over `LogicalType` subclasses plus the `ConvertedType`
  fallback with `arrow_err!`/`general_err!` where upstream has `arrow_err!`/
  `General`. `parquet_column` returns `Tuple(Int32, Arrow::Field)?` (upstream
  `Option<(usize, &FieldRef)>`).
- Arrow interop, leaf array readers (`byte_array.rs`/`byte_view_array.rs`/
  `byte_array_dictionary.rs`): native `ByteArrayDecoder` plain/dictionary/delta
  classes implement page-aware encoding state, bounded dictionary reads/skips,
  dictionary replacement and explicit malformed-payload errors. Enum `new` maps
  to `for_encoding`. Offset/view column adapters share native codecs with view
  mode selecting strict plain reads and view dictionary errors; output stays
  `ArrayValuesBuffer(ByteArray)` rather than `OffsetBuffer`/`ViewBuffer`.
  Dictionary fallback now uses the same codecs, with no empty-payload bypass.
  Encoding-family and adapter rows record this intentional representation
  difference. Arrow arrays are built at batch consumption; retained view-block
  reuse and batched/SIMD validation are not claimed. UTF-8 checks are per-value
  with native diagnostics. Pinned offset PLAIN count over-reporting on corrupt
  metadata, unchecked skipped payload ends, whole-page skip bypass and delta
  prefix cast/truncation are characterized. Native post-overskip reads return
  zero instead of Rust debug underflow. See
  [byte-page decoding](../../docs/arrow-byte-page-decoding.md) and
  [string validation](../../docs/arrow-string-validation.md). Null padding
  uses an empty `ByteArray` as the pad value (a nil payload cannot be read
  back). Dictionary hashing uses a deterministic FNV-1a instead of `ahash`
  (only equality of equal byte slices matters), and the `hash_scratch` buffer is
  dropped. `ListArrayReader`/`StructArrayReader` use
  `Int32` counts, so upstream's offset-overflow check (`OffsetSize::from_usize`
  above `Int32::MAX`/`Int64::MAX`) cannot trigger and is omitted; the unchecked
  `ArrayDataBuilder::build_unchecked`/`StructArray::new_unchecked_with_length`
  map to arrow-cr's positional constructors. `MapArrayReader` is a
  `ListArrayReader` over a two-field `StructArrayReader`; upstream's
  `assert!`/`unreachable!` panics are `GeneralError`s and the output list data is
  rebuilt as `MapArray` data with `ArrayData.builder(map_type)` (upstream
  `into_builder`). `ListViewArrayReader` performs the upstream
  `From<GenericListArray>` conversion by hand (`OffsetBuffer#inner` gives the
  list offsets; arrow-cr's list view only exposes `try_new`/`from(ArrayData)`),
  dropping the trailing list offset and deriving the sizes, and upstream's
  `unreachable!()` for a non-list-view type is a `GeneralError`.
  `FixedSizeListArrayReader` uses `MutableArrayData#try_extend`/
  `try_extend_nulls` (which raise on error in Crystal instead of returning
  `Result`) for the null-list blocks and an empty batch returns a zero-length
  fixed-size list over an empty child.
- Arrow interop, array reader builder: `ArrayReaderBuilder` takes the reader
  metrics and optional cache options (`with_cache_options`, wrapping every leaf
  selected by the cache projection in a `CachedArrayReader`). Virtual columns
  use `ParquetFieldType::Virtual` and file-global metadata supplied through
  `with_parquet_metadata` in both sync and push readers. The per-field
  `ReaderArgs` struct is replaced by explicit
  `(field, mask, padding_threshold)` parameters. INT96 now decodes to Int64
  timestamps at the requested second/millisecond/microsecond/nanosecond resolution,
  preserving null masks, timezone hints and the native wrapping conversions.
- Virtual columns: `RowNumber`/`RowGroupIndex` are Arrow extension classes with
  Int64 storage and empty-string metadata. Rust's Any downcast is Crystal
  `is_a?`/`as?`; batch/skip counts remain Int32. Row numbers keep unmaterialized
  Int64 ranges; group indices use a deque of `(index, count)` runs instead of a
  flattened RepeatN iterator, with a map-free single-group path. Metadata is
  interpreted in physical file order and selected groups retain caller order.
  Extension-name recognition uses `parquet.virtual.` while the upstream error
  text says `arrow.virtual.`; both are preserved. Supplied-schema metadata keeps
  upstream's physical-only schema accessor while actual read batches include
  virtual fields. `Field#with_*` mutation is contained by copying extension
  metadata before attachment. Parquet without an IPC hint reconstructs Variant
  BinaryView leaves as Binary; exact bytes/nulls are asserted for both paths.
- Arrow interop, predicate cache (`array_reader/row_group_cache.rs` +
  `array_reader/cached_array_reader.rs`): `BatchID`/`CacheKey` are structs,
  `RowGroupCache` a class keyed by `(column_idx, batch_id)` with the upstream
  insert/evict accounting, and `CachedArrayReader` wraps an `ArrayReader` with a
  `CacheRole` (`Producer` inserts into the shared cache, `Consumer` removes
  batches once consumed) plus a per-reader local cache so `consume_batch` still
  works after eviction. `CacheOptionsBuilder`/`CacheOptions` (in
  `array_reader/builder.cr`) carry the projection, cache and role. Divergences:
  upstream shares the cache as `Arc<RwLock<RowGroupCache>>` because its reader is
  multi-threaded; Crystal fibers share the cache object directly, so
  `CacheOptions`/`CachedArrayReader` hold the `RowGroupCache` itself. Byte
  accounting uses `Arrow::ArrayBase#array_memory_size` for every type instead of
  `get_array_memory_size_for_cache`'s `Utf8View` special case (that workaround
  exists because StringView arrays share buffers, which arrow-cr's
  `buffer_memory_size` already accounts for). Upstream's `assert_eq!` on the
  skipped row count and on a duplicate insert raise `GeneralError`/are documented
  instead; the positions and caches the upstream unit tests read directly
  (`outer_position`, `inner_position`, `local_cache`, `batch_size`,
  `current_cache_size`) are exposed as getters because Crystal specs cannot
  reach private state. `read_records`'s parameter is `batch_size` (upstream
  `num_records`) to match the `ArrayReader` interface. `get_def_levels`/
  `get_rep_levels` return `nil` like upstream ("we don't allow nullable parent
  for now"), which is why upstream — and the port — exclude nested leaves from
  the cache projection. Tests: `spec/parquet/arrow/row_group_cache_spec.cr` (the
  two upstream cache tests) and `spec/parquet/arrow/cached_array_reader_spec.cr`
  (all ten upstream `CachedArrayReader` tests, with the upstream
  `MockArrayReader`), plus the `ArrayReaderBuilder#with_cache_options` wiring.
- Arrow interop, null columns: `NullArrayReader` follows upstream in fixing the
  record reader to INT32 (a Null column carries no non-null values); skipping an
  all-null column is not supported because the shared skip path treats each
  skipped level as a value (see the skipping note above).
- Arrow interop, fixed-length byte arrays: the reader now uses
  `FixedLenByteArrayBuffer` and `FixedLenByteArrayValueDecoder`, with contiguous
  native Array(UInt8) storage, pending capacity hints, reverse null padding and
  plain/dictionary/delta/byte-stream-split page state. Reservation replaces the
  native array when required; null moves use a scalar loop for all widths
  rather than the VEC_CUTOFF optimization. Counts/capacities are Int32, with
  checked arithmetic instead of Rust usize/saturating allocation behavior.
  Native GeneralErrors replace panic-only width/dictionary/key states; explicit
  returned errors and read/skip width-check distinctions match the pin.
  Decimals/intervals use PrimitiveArray(T) (Int32/Int64/Int128/I256), Float16
  uses the native half-float array, and MonthDayNano remains NYI as upstream.
  Narrow/unsigned primitive targets and decimal/date targets use their actual
  Arrow native storage widths, even when their Parquet physical type differs.
  See [fixed-byte decoding](../../docs/arrow-fixed-byte-decoding.md).
- Arrow interop, batch reader: the reader options live in the generic
  `ArrowReaderBuilder(Input)` (`src/parquet/arrow/arrow_reader.cr`), which the
  synchronous `ParquetRecordBatchReaderBuilder` (input
  `SyncReader(File::FileReader)`, upstream `ArrowReaderBuilder<SyncReader<T>>`)
  and the push decoder's `ParquetPushDecoderBuilder` (input `PushDecoderInput`)
  subclass; the async stream builder uses `AsyncFileReader` directly and the
  same generic builder state. Rust's hidden AsyncReader newtype is unnecessary.
  The shared `with_batch_size` clamps to the file row count like upstream (porting
  `test_batch_size_overallocate`), the shared `with_max_predicate_cache_size` is
  accepted by all builders (the push decoder, including async streams, uses it,
  as upstream), and each subclass's `build` destructures the shared state as upstream's `impl` blocks
  do. `ParquetRecordBatchReaderBuilder` takes a `File::FileReader` (upstream any
  `ChunkReader`); row selection, row-group selection, offset/limit plans,
  `RowFilter`, `StatisticsConverter` and reader metrics are ported. The async
  stream builder also supports bloom lookups and index policies. Arrow options
  now carry page-index, encoding/statistics policies, Parquet/Arrow schema hints,
  virtual columns and decryption properties into async metadata loading. A
  synchronous builder still receives an already-decoded FileReader; configure
  its metadata/index/decryption behavior using File::ReadOptions at file opening.
  The supplied-schema field-metadata mismatch message renders Crystal's `Hash?`
  inspection where upstream uses Rust's `Debug` formatting.
  The reader's `schema` keeps the projected field metadata (upstream strips it
  from `RecordBatchReader::schema`). The upstream `arrow_reader` tests that
  build their fixture with `ArrowWriter` (`test_with_schema`, `test_skip_metadata`,
  `test_read_binary_as_utf8`, …) are characterized with the ported
  `SerializedFileWriter` instead; `test_dictionary_preservation` and
  `test_null_schema_inference` are ported directly.
- Arrow interop, row selection: both backings of `RowSelection` are modelled
  (the RLE `Array(RowSelector)` and the bitmap `MaskSelection`), along with
  `from_boolean_buffer`/`from_filters`/`as_mask`, `MaskRunIter`, the mask
  primitives (`SelectionMasks`: `to_selectors`, `has_at_least_runs?`,
  `split_off`, `trim`, `offset`, `limit`, `from_selectors`) and the
  `and_then`/`intersection`/`union` algebra (`RowSelectionAlgebra`, in
  `arrow/row_selection/algebra.cr`). Divergences: `usize` counts are `Int32`;
  `MaskSelection`'s `OnceLock`s are nilable ivars (`selectors_cached?` exposes
  the RLE-cache cell for the ported clone test); `iter` returns `&RowSelector`
  upstream but is `#each`/`Enumerable` here, and `into_selectors`/
  `into_inner` do not consume the receiver (`inner` is a public getter);
  `RowSelection#dup` is the `Clone` path (it drops a mask's RLE cache like
  upstream); `FromIterator<RowSelection>` is `RowSelection.concat`;
  `RowSelection::from_selectors`/`from_mask_selection` are public (upstream
  private) because the algebra module lives in another file; the mask algebra
  is bit-at-a-time rather than `u64`-word kernels, and the unequal-length mask
  combination always yields a zero-offset buffer (upstream carries the longer
  mask's sub-byte offset); the `trim`/`last_set_bit_position` scan is per-bit
  (upstream scans bytes backwards) and `boolean_mask_from_selectors` fills
  through `BooleanBufferBuilder#append_n` (same bits); the upstream
  `panic!`s are `GeneralError`s with the same messages; page pruning
  (`scan_ranges`/`row_ranges_for_selected_pages`, in `row_selection/ranges.cr`)
  returns `UInt64` byte ranges (page offsets are `i64` on disk) and `Int32` row
  indices, and `expand_to_batch_boundaries`/`auto_selection_strategy` are
  ported (the strategy enum lives in `row_selection.cr`, upstream
  `selection/cursor.rs`); `from_consecutive_ranges` raises
  `GeneralError("out of order")` where upstream panics.
- Arrow interop, read plan: both cursor strategies are modelled
  (`RowSelectionPolicy`/`RowSelectionStrategy`, `MaskCursor`/`MaskChunk`,
  `LoadedRowRanges`, `build_cursor`), selected per plan from the selection's
  density (default `Auto(32)`, as upstream). Divergences: `RowSelectionPolicy`
  is a struct with a `Kind` plus the `Auto` threshold (`SELECTORS`/`MASK`
  constants, `auto(threshold)`, `default`), `RowSelectionStrategy` lives in
  `row_selection.cr`, `usize` counts/offsets are `Int32`, `ReadPlan` exposes
  its cursor directly instead of `row_selection_cursor_mut`,
  `with_loaded_row_ranges` is public (upstream `pub(crate)`) and
  `LoadedRowRanges#ranges` is a public getter (upstream `#[cfg(test)]`),
  `MaskCursor#next_chunk`'s debug assertions are `GeneralError`s,
  `ReadPlanBuilder#dup` is a shallow copy (`build` never mutates the
  selection), and `BooleanArray#take_n_true` lives in `read_plan.cr` because
  arrow-cr does not provide it. Page pruning is only wired through the plan
  (the sync reader never computes loaded row ranges upstream either); the
  reader's `read_mask_batch` combines chunk masks with
  `FilterMaskAccumulator` and filters once per output batch with the upstream
  error messages.
- Arrow interop, row-group selection (`RowGroupSelection`/`RowGroupPlan` +
  `ArrowReaderBuilder::with_row_groups`): `RowGroupPlan` is a public mutable
  class carrying a `Kind` (`Global`/`PerRowGroup`/`Conflicting`) instead of a
  `pub(crate)` enum whose `set_*` methods reassign `self`, built through
  `RowGroupPlan.global`/`.per_row_group`/`.conflicting`; `into_global` raises
  the upstream messages ("Row-group-local selections are not supported by the
  synchronous reader" and the `with_row_group_selections cannot be combined
  with with_row_groups or with_row_selection` conflict). `with_row_groups`
  takes an `Enumerable(Int32)` (row-group indices are `Int32`) and
  `FileRowGroups` gained an optional index list (upstream `ReaderRowGroups`) so
  `num_rows`/`column_chunks`/`row_groups` cover only the chosen row groups, in
  the supplied order. `build` resolves the plan, passes its global selection to
  the read plan and sizes the offset/limit plan from the selected row groups'
  row count (the batch size still caps at the file row count, as upstream).
  Per-row-group selections are only reachable through the plan (the synchronous
  builder never sets them). The public virtual-column specs now exercise both
  values and file-global row numbers under reordered groups, selection,
  predicates, offset/limit and push decoding.
- Arrow interop, reader metrics (`metrics.rs`): `ArrowReaderMetrics`
  (`disabled`/`enabled`, `records_read_from_inner`/`records_read_from_cache`
  returning nil when disabled, `increment_*`) is a class holding an optional
  inner class (upstream `Disabled`/`Enabled(Arc<..>)` enum); `#dup` shares the
  inner counters like cloning the upstream `Arc`, counters are `Atomic(Int32)`
  (`usize` → `Int32`), and `increment_*` are public (upstream `pub(crate)`).
  `ParquetRecordBatchReaderBuilder#with_metrics` stores the metrics (exposed by a
  `metrics` getter, upstream `pub(crate)`) and threads them into
  `ArrayReaderBuilder.new(row_groups, batch_size, metrics)` (upstream
  `new(row_groups, &metrics)`), which hands them to the predicate cache. Only
  `CachedArrayReader` increments the counters upstream, and the synchronous
  reader does not use the cache, so synchronous reads report zero — the same as
  upstream (arrow-rs#8000); the push decoder's cached leaves do report counts
  (`spec/parquet/arrow/push_decoder_spec.cr` asserts 400 inner / 149 cache reads
  for a cached filter column). Upstream's metric tests live in
  `tests/arrow_reader/predicate_cache.rs`; `metrics_spec.cr` characterizes the
  API/counter sharing, while `async_reader_spec.cr` verifies enabled cache
  counters and zero disabled-cache counters in sparse-page async reads.
- Arrow interop, in-memory row groups (`in_memory_row_group.rs`):
  `InMemoryRowGroup` is a `RowGroups` source over caller-supplied bytes
  (row count, per-column `ColumnChunkData?` slots, the row group's
  `RowGroupPageIndex`, index and metadata). `fetch_ranges` returns whole column
  chunk ranges when there is no selection or no offset index, otherwise the
  page ranges that `RowSelection#scan_ranges` selects (prepending the
  dictionary-page range when the first page does not start at the chunk start)
  plus each column's page start offsets; for columns selected by `cache_mask`
  it scans the selection expanded to batch boundaries
  (`expand_to_batch_boundaries`), so a cache miss can fetch a full batch from
  the retained sparse chunks. `fill_column_chunks`
  assembles dense `ColumnChunkData::Dense (offset, data)` or sparse
  `ColumnChunkData::Sparse (length, [{page offset, bytes}])` chunks. Divergences:
  `usize` → `Int32`, byte ranges are `Range(UInt64, UInt64)`, `FetchRanges` is a
  struct, `ColumnChunkData` is an abstract `ChunkReader` with `Dense`/`Sparse`
  subclasses, and the sparse lookup is a linear scan (upstream binary search)
  raising the upstream message for unknown offsets. Upstream has no dedicated
  tests for this module; the port characterizes dense/sparse fetching, page
  pruning with a real offset index, the unfetched-column error, and decoding the
  selected rows from memory (cross-checked against the file reader).
- Arrow interop, push decoder (`push_decoder/{mod,remaining,reader_builder}`):
  `ParquetPushDecoderBuilder` (upstream `ArrowReaderBuilder<PushDecoderInput>`)
  carries the reader options directly (metadata/schema/fields, batch size,
  projection, `RowGroupPlan` via `with_row_groups`/`with_row_selection`,
  offset/limit, row-selection policy, metrics and `with_buffers`), and
  `ParquetPushDecoder#try_decode` walks the upstream
  `ReadingRowGroup`/`DecodingRowGroup`/`Finished` transitions: it asks for the
  row group's `fetch_ranges` when the `PushBuffers` do not cover them
  (`NeedsData`), otherwise fills an `InMemoryRowGroup`, clears the consumed
  ranges, and returns a `ParquetRecordBatchReader` (`Data`) that yields the row
  group's batches (with the upstream
  "Cannot push data to a finished decoder" error). `RowBudget` ports
  upstream's offset/limit accounting across row groups
  (`rows_after`/`advance`/`selected_row_limit`/`apply_to_plan`; the applied
  budget is carried into `WaitingOnData` so it is not re-applied when the data
  arrives), and `remaining.rs` is ported as its own classes:
  `QueuedRowGroups` (`Global`/`PerRowGroup` tagged by a `Kind` enum,
  `into_plan`, `front`, `clear`, `global_selection_exhausted?`,
  `pop_front_selection`, `dup` for peeking), `RowGroupFrontier` (queued work +
  budget + `has_predicates`, `next_readable_row_group`,
  `plan_selected_row_group`, `update_budget_after_row_group`, `clear_remaining`,
  `dup`/`peek_next_row_group`) and `RemainingRowGroups` (+
  `RemainingRowGroupsParts` via `into_parts`). The per-row-group decode state
  machine lives in `RowGroupReaderBuilder` (upstream `reader_builder/mod.rs`:
  `Start`/`Filters`/`WaitingOnFilterData`/`StartData`/`WaitingOnData`/
  `Finished`, `RowGroupInfo`, `NextRowGroup`, `DataRequest`/
  `DataRequestBuilder`, `RowGroupBuildResult`, `RowGroupReaderBuilderParts` via
  `into_parts`).
  Adaptive scans are ported: `try_next_reader` hands the row group's reader to
  the caller (leaving the decoder at a row-group boundary, so `into_builder`
  works while that reader is still being iterated), `peek_next_row_group`
  reports the index the next `try_next_reader` will produce — `nil` when
  finished, internally mid-row-group, or when every remaining row group is
  skipped by selection/offset/limit — by running the real advance logic on a
  copy of the frontier (`RowGroupFrontier#dup` + `QueuedRowGroups#dup`, which
  deep-copies the queued row groups and the global `RowSelection`), and
  `into_builder` recovers a builder for the not-yet-decoded row groups
  (`QueuedRowGroups#into_plan` + the still-pending offset/limit budget) carrying
  the pushed buffers across the rebuild, so widening/narrowing the projection
  or changing the limit resumes exactly where the decoder stopped. The upstream
  error paths are preserved: `into_builder` raises mid-row-group
  ("...check is_at_row_group_boundary() first") and on a finished decoder, and
  `push_*` on a finished decoder raises "Cannot push data to a finished
  decoder". Row-group-local selections are ported: `with_row_group_selections`
  queues `RowGroupSelection`s (supplied order, duplicates allowed, a local
  selection shorter than its row group skips trailing rows, an empty list reads
  nothing), replacing earlier local configuration; `QueuedRowGroups.try_new`
  validates at build (the upstream "Row selection for row group N contains X
  rows, but the row group has Y" and "Row group index N out of bounds for file
  with M row groups" messages — the latter required adding upstream's bounds
  check to `ParquetMetaData#row_group_num_rows`), and mixing local selections
  with `with_row_groups`/`with_row_selection` raises the upstream conflict
  error.
  Row filters are ported: `ParquetPushDecoderBuilder#with_row_filter` takes a
  `RowFilter` and the builder computes `has_predicates` (non-empty predicate
  list) so `RowGroupFrontier` never skips a row group whose rows a filter could
  still select. `RowGroupReaderBuilder` keeps the filter while no row group is
  active and hands it to `FilterInfo` for the duration of a row group
  (`FilterInfo#current`/`#last?`/`#advance` → `FilterAdvanceResult::Continue`/
  `Done`, `#into_filter` puts it back on the builder); each predicate drives its
  own data request over its own `ProjectionMask` (`Filters` →
  `WaitingOnFilterData`), the fetched column chunks are carried into the next
  predicate and the output stage (so a filter column that is also projected is
  not fetched twice), and the resulting selection is folded into the plan with
  `PredicateOptions`/`ReadPlanBuilder#with_predicate_options`. When the current
  predicate is the last one and an output limit is set, the predicate limit
  (`RowBudget#selected_row_limit`, i.e. offset + limit) is passed to
  `PredicateOptions#with_limit`, so filter evaluation stops early once enough
  matching rows have been accumulated and later row groups are never requested.
  `CacheInfo` (the cache projection computed by upstream's
  `compute_cache_projection`/`_inner`/`exclude_nested_columns_from_cache`, which
  are ported) accompanies each `FilterInfo` together with one `RowGroupCache`
  per row group; the predicate array reader is built with the producer cache
  options, the output array reader with the consumer ones, and
  `DataRequestBuilder#with_cache_projection` feeds the filter-stage
  `fetch_ranges` `cache_mask`, so a column that is both filtered and projected is
  decoded once per row group (the final output request deliberately does not
  call `with_cache_projection`, as upstream comments).
  Page skipping is ported too: before evaluating each predicate and before
  building the output reader, the plan's configured policy is restored and
  `prepare_selection_for_page_skipping` pins it to the resolved strategy,
  attaching `LoadedRowRanges` (via `loaded_row_ranges_for_projection`) when mask
  execution would otherwise decode across a page that `fetch_ranges` pruned away;
  `row_group_offset_index` supplies the `RowGroupPageIndex` for the row group
  (upstream both use `metadata.page_index()` when it has offset indexes).
  Divergences: `usize` → `Int32`, the state/build-result enums are sealed class
  hierarchies, `RowGroupNextState` replaces upstream's `NextState`, `inspect`
  results are exceptions (Crystal has no `Result`, so `build`/`try_new_decoder`
  and `into_builder` raise where upstream returns `Err`), `QueuedRowGroups#dup`
  copies rather than moves the global selection, `RowGroupFrontier`'s
  copy constructor is `protected` (Crystal cannot call a private constructor
  with an explicit receiver), `StartData`/`WaitingOnData` default `cache_info` to
  `nil` instead of spelling out `None`, `prepare_selection_for_page_skipping`/
  `loaded_row_ranges_for_projection` are public `Parquet::Arrow` module functions
  instead of private free functions (their upstream unit tests live in the
  module; Crystal specs cannot reach private methods), and
  `ParquetPushDecoderBuilder` is a Crystal subclass of
  `ArrowReaderBuilder(PushDecoderInput)` rather than an alias over one struct
  (`PushDecoderInput` models upstream's buffers-only input, so `with_buffers`
  replaces the input while `#buffers` reads it). `RemainingRowGroups` carries the
  `::Arrow::Schema` only for `into_parts`.
  Tests: `spec/parquet/arrow/push_decoder_spec.cr` covers the decoder, row
  groups/selections, adaptive scans, row filters (single/multiple predicates,
  page reuse, empty filters, filters with a row selection, offset+limit, the
  predicate-limit short-circuit, row-group-local selections with a filter, and
  both filter-based `into_builder` tests), the predicate cache (the filter
  column is decoded once and the output is served from the cache; with
  `with_max_predicate_cache_size(0)` nothing is cached and the cache metrics stay
  at zero) and page skipping (both upstream unit tests — loaded ranges
  intersected across columns, and Auto staying on mask execution — plus
  end-to-end mask-strategy reads with and without a filter over a 50-row-page
  fixture, which fail with the sparse-offset error without the preparation).
  The fixture's metadata is read with `ReadOptionsBuilder#with_page_index`, like
  upstream's `test_file_parquet_metadata` (whose metadata decoder fetches the
  page index), so page pruning is active in every push decoder test. Upstream's
  `test_decoder_size` (a `size_of` assertion) is not ported; the port's
  projection tests assert metadata-derived chunk ranges instead of upstream's
  hardcoded ones (its fixture writes BYTE_ARRAY, not StringView).

- Arrow interop, row filter (`filter.rs` + `ReadPlanBuilder::with_predicate`
  + `ArrowReaderBuilder::with_row_filter`): `ArrowPredicate` is an abstract
  class with `projection`/`evaluate`; `ArrowPredicateFn` adapts a
  `RecordBatch -> BooleanArray` block (upstream
  `FnMut(..) -> Result<BooleanArray, ArrowError>`, so predicate errors raise),
  and `RowFilter` takes an `Array(ArrowPredicate)` or a splat of predicates.
  `Send + 'static` bounds are dropped. `with_predicate_options` ports the
  match-limit/`total_rows` early-termination path (used by DataFusion-style
  TopK), including the padded "not selected" tail; predicates are evaluated
  eagerly in `build` (upstream does the same for the sync reader) through a
  nested `ParquetRecordBatchReader` over the current selection, and the
  per-predicate array reader is built over `predicate.projection` from the same
  `FileRowGroups`. Because the plan is selector-backed, predicates are folded
  in with `RowSelection#and_then` on selectors (the upstream mask path via
  `filters_to_boolean_buffer` is implemented but only reached when the prior
  selection is mask-backed). `with_max_predicate_cache_size` remains a no-op,
  exactly as upstream notes for the sync reader (arrow-rs#8000).
- Arrow interop, statistics (`arrow_reader/statistics.rs`): `StatisticsConverter`
  (`try_new`/`from_column_index`/`parquet_column_index`/`arrow_field`/
  `with_missing_null_counts_as_zero`), the row-group extractors
  (`row_group_mins`/`maxes`/`is_min|max_value_exact`/`null_counts`/
  `nan_counts`/`distinct_counts`/`row_counts`) and the data-page extractors
  (`data_page_mins`/`maxes`/`null_counts`/`nan_counts`/`row_counts`) are
  ported. Divergences: the upstream module-private free functions
  (`min_statistics`, `max_statistics`, `min|max_page_statistics`,
  `null|nan_counts_page_statistics`, `from_bytes_to_*`, the
  `make_stats_iterator!` adapters) live in `StatisticsExtractors` (a module
  named `Statistics` would shadow `Parquet::Statistics`); the per-type iterator
  structs are replaced by extracting a canonical `StatValue`
  (`Bool|Int32|Int64|Float32|Float64|Bytes`) and building the arrow array from
  it, which behaves the same (the Date64 physical-type split, decimal
  width/sign-extension, Rust `as` casts for `UInt32`/`UInt64`, `try_from`
  narrowing for `Int8/16`/`UInt8/16`, null on invalid UTF-8 and
  `FixedSizeBinary` width mismatches are all preserved); `usize` counts are
  `Int32`; the `IntoIterator` parameters are `Enumerable`/`Array` and
  `row_group_row_counts`/`data_page_row_counts` return a nilable
  `PrimitiveArray(UInt64)`; `Float16` uses arrow-cr's `Arrow::Float16` and
  `i256` is `Arrow::I256`; the page null/NaN-count helpers append one value per
  count instead of upstream's `len`-sized validity (identical when a chunk's
  counts match its page count); decimal precision/scale are carried by
  `with_data_type` rather than `with_precision_and_scale` validation.
  `data_page_*` for `Date64` returns nulls (as upstream) while `row_group_*`
  scales INT32 days to milliseconds. Testing note: the upstream tests live in
  `parquet/tests/arrow_reader/statistics.rs` and build fixtures with
  `ArrowWriter`, so `spec/parquet/arrow/statistics_spec.cr` writes the same
  shapes with `SerializedFileWriter` (one row per batch when a page row-count
  limit is requested, unsigned/decimal converted types where the writer's
  min/max ordering needs them, one value per row group where signed-vs-unsigned
  byte ordering would otherwise change min/max).
- Writer bug found while porting statistics: `GenericColumnWriter` tested
  nilable min/max values by truthiness (`if min`, `if (min = ...) && (max =
  ...)`), so a BOOLEAN column whose minimum is `false` wrote no min/max
  statistics; `ColumnIndex` thrift serialization had the same bug
  (`value ? to_le_slice(value) : Bytes.new(0)`), writing an empty BOOLEAN min
  into the page index, which made the page index undecodable. Both now test
  against `nil`, and the statistics spec covers boolean row-group and
  data-page statistics.
- Arrow interop, skipping: `ColumnReaderImpl#skip_records` (like upstream) only
  consults the classic definition-level decoder, so with the arrow
  `DefinitionLevelBuffer` in use every skipped level is treated as a value. Skip
  therefore assumes there are no null slots (skipping levels, not nulls); the
  upstream tests only skip required/all-present columns, and the port's specs do
  the same (recorded here because it is invisible from the call sites).
- Derive macros: `parquet_derive`'s proc macros (`#[derive(ParquetRecordWriter)]`
  / `ParquetRecordReader`) are replaced by `include Parquet::Derive::Record`.
  Rust impls `RecordWriter for &[T]` and `RecordReader for Vec<T>`; Crystal
  cannot subclass slices, so nested `T::Writer < RecordWriter(T)` (holding
  `Array(T)`) and `T::Reader < RecordReader(T)` (with a `records` getter) are
  generated instead. The reader builds records positionally via the record's
  `initialize` (fields must be declared in parameter order; Rust uses `Default`
  + public field assignment). Field types are Crystal-native (`Bool`,
  `Int8..64`, `UInt8..64`, `Float32/64`, `String`, `Bytes`, `UUID`, `Time` as
  `TIMESTAMP_MILLIS`, `Parquet::Derive::Date` as `DATE`); nilable `T?` is
  `OPTIONAL`; the borrowed Rust shapes (`&str`, `Arc<str>`, `&Option<..>`,
  `Vec<u8>`, `[u8; N]`) and raw identifiers have no analogue. The `syn` AST
  walkers (`Type.from*`/`last_part`/`leaf_type_recursive`) are replaced by
  Crystal macro type introspection (`@type.instance_vars`, `TypeNode#union?`),
  and the Rust `Result`-alias macro-hygiene test is not applicable.
- Encryption ciphers: upstream uses `ring` (`AES_128/256_GCM`, `SystemRandom`).
  Crystal's `OpenSSL::Cipher` exposes GCM tags but no AAD setter, so
  `encryption/ciphers.cr` implements a minimal `AesGcm` directly on OpenSSL EVP
  (re-binding `evp_aes_128_gcm`/`evp_aes_256_gcm` in `LibCrypto`, using
  `EVP_CipherUpdate` with a null output for AAD), and `Random::Secure` for the
  counter-nonce seed. Errors raise `Parquet::GeneralError`; `Send`/`Sync` and
  native decryptor `HeapSize` replaces Rust layout accounting; `RingGcmBlock{Encryptor,Decryptor}` are
  `GcmBlock{Encryptor,Decryptor}` and `CounterNonce#get_bytes` is
  `CounterNonce#bytes`.
- Thrift traits: `ReadThrift`, `WriteThrift`, and `WriteThriftField` are Rust
  traits with an associated `ELEMENT_TYPE` constant. Crystal has no associated
  constants, so value dispatch lives on `ThriftCompactOutputProtocol` /
  `Thrift.*` overloads, and IDL types expose duck-typed
  `self.read_thrift(prot)` / `#write_thrift(prot)` methods.
- `ThriftProtocolError`: not modeled as a separate type; protocol routines raise
  the corresponding `ParquetError` directly with identical messages.
- Thrift IDL macros: upstream `parquet_macros.rs` parses raw Thrift IDL text at
  macro-expansion time. Crystal macros cannot consume IDL tokens, so
  `define_thrift_enum` / `define_thrift_union_all_empty` annotate a natively
  declared Crystal `enum`; declarative `define_thrift_struct` and mixed-union
  blocks generate native types and `read_thrift`, `write_thrift` and
  `thrift_element_type`. Mixed unions now drive production crypto metadata;
  see [Thrift unions](../../docs/thrift-unions.md). Upstream's generated `VARIANTS`/`MAX_DISCRIMINANT`
  constants are not provided yet.
- `from_str` conversions use `Enum.parse?`/`upcase`; for `Encoding` this accepts
  mixed-case spellings in addition to upstream's all-upper/all-lower forms.
- `LogicalType`: Rust's payload-carrying enum is modelled as a sealed
  `LogicalType` class hierarchy; the `_Unknown { field_id }` variant is
  `LogicalType::UnknownField`. `EdgeInterpolationAlgorithm`'s `_Unknown(i32)`
  variant is retained by wrapping the i32 code in a value struct.
  `GeographyType::algorithm` is `GeographyType#algorithm_or_default`.
- `schema::Type`: upstream's enum (`Type::PrimitiveType`/`Type::GroupType`) is a
  sealed class hierarchy. Predicates use `?` (`primitive?`, `group?`,
  `schema?`, `optional?`, `has_repetition?`, `has_id?`); `TypePtr`/`Arc<Type>`
  is just `Type`. To keep upstream error strings intact, `LogicalType` and the
  logical-type structs expose `rust_debug` reproducing Rust `#[derive(Debug)]`.
  `schema/types` lives under `Parquet::Schema` (upstream `schema::types`).
- `SchemaElement` lives under `Parquet::Schema` (upstream `file::metadata::thrift`)
  to avoid a require cycle; it carries an owned `String` name instead of `<'a> str`.
- Page-header thrift structs live under `Parquet::Format` (upstream
  `file::metadata::thrift`); the `read_*_without_stats` readers are
  `Parquet::Format` module methods.
- Metadata types (`ColumnChunkMetaData`/`RowGroupMetaData`) expose mutable
  `property` fields (upstream keeps them private and mutates from the crate);
  `Rust Result`-returning builders return the value or raise.
- `Decimal`: upstream's `Int32`/`Int64`/`Bytes` enum is a single Crystal struct
  carrying the unscaled bytes; variant identity is the byte width. Equality and
  accessors match upstream (`precision`, `scale`, `data`).
- `FixedLenByteArray`: Rust `Deref`/`From` are explicit delegating methods and
  constructors in Crystal.
- `ByteArray::is_empty` maps to Crystal `empty?`.
- `ValueStatistics::is_*`/`ColumnIndex::is_null_page` map to `?` predicates
  (`min_max_deprecated?`, `null_page?`, ...); `Statistics` is a single class over
  a `ValueStatistics` union rather than an enum.
- `ColumnOrder`: upstream's enum with a `TYPE_DEFINED_ORDER(SortOrder)` payload
  is a Crystal struct carrying a `Kind` + inner `SortOrder`; unit variants are
  constants and `TYPE_DEFINED_ORDER(s)` is `ColumnOrder.type_defined_order(s)`.
  `SortOrder::is_signed` is `SortOrder#signed?`. The deprecated
  `ColumnOrder::sort_order_for_type` alias is not provided.
- `PrimitiveColumnIndex<T>` delegates `ColumnIndex` methods rather than using
  Rust `Deref`; `OffsetIndexMetaData::try_from_fast` fast-path decoder is omitted
  (the macro `read_thrift` decodes the same wire form).
- `ByteArrayColumnIndex`: same delegation divergence as
  `PrimitiveColumnIndex<T>`; offsets are `Int32` instead of `usize`.
- `ColumnIndexMetaData`: upstream's enum with eight variants
  (`BOOLEAN`/`INT32`/.../`FIXED_LEN_BYTE_ARRAY`) is a struct carrying the
  physical `Type` plus the inner index; recover the variant via `kind` and
  `inner.as(...)`. `is_sorted` → `sorted?`, `is_null_page` → `null_page?`, and
  `get_boundary_order` → `boundary_order` (non-optional); `num_pages` returns
  `Int32`. `decode_column_index` lives as `PageIndex.decode_column_index`
  (`index_reader.cr`); `decode_offset_index`/`acc_range` are deferred to the
  file-reader chunk.
- Column-index `WriteThrift`: Rust `WriteThrift::ELEMENT_TYPE` associated consts
  are not modelled; `write_thrift` is an instance method and the min/max
  `list<binary>` fields are encoded by hand exactly as upstream. Primitive
  min/max bytes are produced via `FromBytes.to_le_slice` (see Phase 1.5).
- `PageIndexProvider`: Rust trait with `as_any` downcasting is an abstract class;
  `as_any` is not modelled. Predicates use `?` (`has_column_indexes` →
  `column_indexes?`, `has_offset_indexes` → `offset_indexes?`, `is_complete` →
  `complete?`). `PageIndex: HeapSize` uses the native accounting in `metadata/memory.cr`.
- `PageIndexBuilder`: `empty_index`/`is_empty_index` are private instance methods
  (`empty_index`/`index_empty?`); the Rust `From<PageIndex>` impl maps to
  `PageIndexBuilder.new_from` (and `PageIndex#into_builder`). `ColumnIndexBuilder`
  `valid` → `valid?`; `set_boundary_order` keeps its upstream name under an
  ameba `AccessorMethodName` disable.
- `ParquetMetaData`: `page_index` is a plain `PageIndexProvider?` (no
  `Arc`/`as_any`); equality compares page indexes by value for `PageIndex` and by
  reference otherwise. `set_page_index` is public (upstream `pub(crate)`).
  `file_decryptor` is a plain `FileDecryptor?` (compared by value, as upstream)
  set via `with_file_decryptor`/`ParquetMetaDataBuilder#set_file_decryptor`;
  `memory_size` reports the native retained-byte estimate; see [metadata memory](../../docs/metadata-memory.md).
- `ParquetMetaDataBuilder`: holds the fields directly rather than wrapping
  `ParquetMetaData`; the Rust `From<ParquetMetaData>` impl maps to
  `new_from_metadata`. `set_row_groups`/`set_page_index` keep upstream names under
  ameba disables.
- `ParquetMetaDataOptions`: Rust `with_*(mut self) -> Self` mutates and returns
  `self`; `encoding_stats_as_mask` → `encoding_stats_as_mask?`;
  `ParquetStatisticsPolicy::SkipExcept(Arc<HashSet>)` is
  `SkipExcept(Set(Int32))`, `KeepAll`/`SkipAll` are constants, and `is_skip` →
  `skip?`. `set_*` keep upstream names under ameba disables; `Default`/`new` map
  to the default initializer.
- `DecodeResult<T>`: Rust's generic enum is a generic class carrying a `Kind`
  plus the payload (`needs_data?`/`data?`/`finished?`, `ranges`, `data`).
- `FooterTail`: `is_encrypted_footer` → `encrypted_footer?`;
  `try_new`/`try_from` both take a `Bytes` (Crystal has no fixed-size array).
- `PushBuffers`: the `ChunkReader`/`Length`/`Display`/`Read` impls and the
  arrow-only `buffered_bytes`/`clear_ranges` are not ported; `has_range` →
  `has_range?`; `get_bytes` returns a `Bytes` slice and raises
  `NeedMoreDataRangeError`.
- `ParquetMetaDataPushDecoder`: `PageIndexPolicy` is an enum with
  `from(Bool)`; the `try_new_with_footer_tail`/`try_new_with_metadata`
  constructors (upstream `pub(crate)`) are public. Decryption properties route
  encrypted footers and column metadata through the encryption decoder; without
  properties the raw parser rejects encrypted footers. Metadata options now
  apply supplied schemas and statistics skipping/full-mask encoding choices
  during plaintext and encrypted Thrift decoding.
- `ChunkReader`: Rust's `Length`/`ChunkReader` traits are one abstract class
  (`len`/`get_bytes`/`get_read`); the associated `T: Read` becomes `IO`, and
  `Bytes`/`File` are wrapped as `BytesChunkReader`/`FileChunkReader`.
- `ParquetMetaDataReader`: `MetadataFetch`/`MetadataSuffixFetch` loaders use
  fiber-yielding methods and native closure adapters. Bounded/suffix requests,
  prefetch hints, remainder/page-index reuse and decryption follow upstream.
  Rust `with_decryption_properties` keeps the port's existing name
  `with_file_decryption_properties`; `has_metadata` → `has_metadata?`,
  `parse_metadata` is public, `metadata_size` returns `Int32?`, and `decode_schema`
  uses a full metadata decode.
- Asynchronous storage reads: Rust futures/Poll/Stream become yielding storage
  calls and Iterator, with the request state on the scanning fiber stack. The
  shared push decoder preserves lazy projection, page pruning, selection,
  filters/cache, nested/virtual/encrypted data and row-group iteration. Errors
  raise once and fuse the stream. Native inspection replaces Rust Debug.
  SpawnedReader uses an explicit ExecutionContext and a mutex-protected shared
  reader instead of Clone; buffered replies preserve exception identity without
  blocking a completed worker. Crystal has no tokio runtime-shutdown API or
  cancellation error/panic-payload distinction; those lifecycle tests have no
  equivalent. No forced cancellation of active IO is promised.
  Rust object_store maps to a native range/suffix/head interface with memory and
  real HTTP(S) backends; cloud SDK signing/credential discovery is supplied by
  application adapters. Keys are Strings, buffers/counts use Int32, and absolute
  byte offsets use UInt64. HTTP requires validated 206/Content-Range/body lengths,
  with no full-object fallback. The deprecated object reader's flags/context
  options are retained. See [async storage](../../docs/async-storage.md) for the
  dependency decision, ownership model, usage and tests. No new shard is needed.
- `encodings::delta_byte_array`: `DeltaByteArrayEncoder(T)`/
  `DeltaByteArrayDecoder(T)` with `T.class` (BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY);
  `DeltaByteArrayDecoder#prefix_lengths` is exposed for the negative-length
  check; the decoder `get` constructs values via `ByteArray`/`FixedLenByteArray`
  rather than Rust `set_from_bytes`.
- `encodings::delta_bit_pack`: `DeltaBitPackEncoder(T)`/`DeltaBitPackDecoder(T)`
  with `T.class` (INT32/INT64 only, enforced by construction); the decoder `get`
  reads each mini-block's packed deltas via `BitReader#get_batch`; `skip`
  decodes into a scratch buffer instead of the upstream fixed skip buffer;
  `BitReader#get_aligned_bytes` is now ported.
- `encodings::plain`: Rust's `DataType`/`Encoder`/`Decoder` trait dispatch is
  `PlainEncoder(T)`/`PlainDecoder(T)` with `T.class` and macro branches per
  physical value type; `encode_all` is added as a test/characterization helper;
  the `get_encoder`/`get_decoder` registries and `put_spaced` are not ported yet.
- `encodings::levels`: the Rust `LevelEncoder::Rle`/`RleV2` enum is a struct
  with a `v2` flag plus the shared `RleEncoder`; observer callbacks are Crystal
  blocks; `flush_to` takes a block rather than a Rust `FnOnce`.
- `encodings::rle`: `FromBitpacked` generic dispatch becomes `T.class`
  (`get(T.class)`/`get_batch(T.class, buffer)`); the decoder's batch/dict paths
  read values with the scalar path (no `unpack`/`index_buf` chunking);
  `is_accumulating_rle` → `accumulating_rle?`, `is_empty` → `empty?`,
  `RleEncoder#flush_buffer_mut` is not ported.
- `util::bit_util`: Rust's `FromBytes`/`FromBitpacked` traits (with associated
  `BIT_CAPACITY`/`BATCH_SIZE` consts and SIMD `unpack_batch`) are replaced by
  explicit `T.class` dispatch; `get_batch` is a scalar loop (no `unpack8/16/32/
  64`); `put_aligned`/`put_aligned_offset` encode via `IO::ByteFormat::
  LittleEndian` rather than the `AsBytes` trait; `BitWriter#flush_buffer_mut`
  and `BitReader#get_aligned_bytes` are not ported.
- `util::bit_pack`: the `unpack::<NUM_BITS>` const generic is an explicit
  `num_bits` parameter and the `seq_macro`/SIMD kernels are a scalar loop; the
  fixed `[T; bits]` output is an `Array(T)` of the same length; the helpers are
  `Parquet::Util::BitPack.unpack8`/`16`/`32`/`64`.
- `util::interner`: Rust's `Storage` trait (with `Key`/`Value` associated types)
  is an abstract generic class `Storage(Key, Value)`; the `hashbrown::HashTable`
  dedup table is a `Hash(Bytes, Key)` keyed on `value.as_bytes`, preserving the
  upstream byte-wise comparison (identical NaN bit patterns dedup);
  `Interner#estimated_memory_size` and `Storage#estimated_memory_size` use
  element counts rather than Rust's retained `capacity()` (`Array` exposes no
  capacity), and the dedup table contributes a fixed preallocation estimate.
  `Interner::into_inner` (arrow-only) is not ported.
- `encodings::dict`: `DictEncoder(T)`/`DictDecoder(T)` with `T.class` and
  `KeyStorage(T)`; `DictDecoder#set_dict` accepts any object responding to
  `values_left`/`get` (in practice a `PlainDecoder(T)`) rather than a
  `Box<dyn Decoder<T>>`; `DictEncoder#put_one` is inlined into `put`;
  `is_sorted` → `sorted?`. `DictDecoder#get`/`#skip` leave `num_values`
  unchanged, matching upstream. `estimated_memory_size` is count-based as above.
- `data_type`: primitives now implement `AsBytes` (`as_bytes` for `Bool`,
  `Int32`, `Int64`, `Float32`, `Float64`, little-endian, matching upstream's
  native-byte `gen_as_bytes!`); `DataType` descriptors and typed bridges now live in `typed_columns.cr`.
  Native slice/str/Vec byte views retain their independent inventory coverage. `dict_encoding_size` /
  `variable_length_bytes` are realized per-type inside
  `DictEncoder::KeyStorage<T>` rather than on a `ParquetValueType` trait.
- `Compression`/levels: Rust's payload enum is a Crystal struct carrying a
  `Kind` + optional level (`Int32?`); the unit variants are constants and the
  payload variants are `Compression.gzip`/`.brotli`/`.zstd` (validating the
  level). `Display` reproduces the Rust `#[derive(Debug)]` form. The
  `CompressionLevel<T>` trait (associated `MINIMUM_LEVEL`/`MAXIMUM_LEVEL`) is a
  `CompressionUtil.validate_level` helper; gzip/brotli levels are `Int32`
  (upstream `u32`). `CompressionCodec::from(Compression)` is
  `CompressionCodec.from`; `From<CompressionCodec>` is `Compression.from_codec`.
  The compression *codecs* (`Codec` implementations) remain pending (Phase 1.8).
- `file::properties`: `usize` fields are `Int32`; Rust `panic!`/`assert!` become
  Crystal exceptions; `BloomFilterPropertiesBuilder#try_build` raises instead of
  returning `Result`; `WriterVersion`/`EnabledStatistics` `FromStr` errors are
  raised; `WriterProperties::new` maps to `WriterProperties.default`; upstream
  `pub(crate)` accessors (`column_override`, `resolve_column_properties`,
  `default_column_properties`, `column_properties`) are public. Encryption
  (`with_file_encryption_properties`/`file_encryption_properties`) is not ported.
  The builder/config types keep upstream `set_*`/non-`?` bool accessor names;
  `Naming/AccessorMethodName` and `Naming/QueryBoolMethods` are excluded for
  `file/properties.cr` and `compression.cr` in `.ameba.yml`.
- `column::reader::decoder`: the `ColumnLevelDecoder`/`RepetitionLevelDecoder`/
  `DefinitionLevelDecoder`/`ColumnValueDecoder` traits (with associated `Buffer`
  types) become abstract classes; all buffers are `Array(Int16)`/`Array(T)`;
  `EncodingMask::is_set` → `set?`; the per-encoding decoder bucket array is sized
  `EncodingMask::MAX_ENCODING + 1` (no `Encoding::MAX_DISCRIMINANT`); the value
  decoder decodes into a scratch `Array(T)` and appends (no in-place tail write).
- `column::page`: Rust's `Page` enum becomes an abstract `Page` class with
  nested `DataPage`/`DataPageV2`/`DictionaryPage` subclasses;
  `uncompressed_size` is `Int64` (so the i32-overflow check is reachable),
  other sizes `Int32`; the `is_*` predicates keep upstream names under ameba
  disables; the `PageReader` `Iterator` impl is not ported and the `PageIterator`
  marker trait is a module including `Iterator(PageReader)`; the encryption-only
  `with_new_compressed_buffer` is not ported. `PageMetadata`'s
  `TryFrom<&PageHeader>` is `PageMetadata.from_thrift`.
- `bloom_filter`: `twox_hash::XxHash64` is replaced by the in-repo
  `Parquet::XxHash64` (exact values, seed 0); `AsBytes` trait-object dispatch is
  a `String` overload plus a generic `as_bytes`; `Block` is a struct over
  `StaticArray(UInt32, 8)` (value semantics for folding) with `|` in place of
  `BitOr`/`BitOrAssign`; the endianness helpers (`to_le_bytes`/`to_ne_bytes`/
  `swap_bytes`) and `Index`/`IndexMut` become the `words` getter + LE writes;
  `estimated_memory_size` uses the block count; internal helpers
  (`Block.mask`, `Sbbf#hash_to_block_index`/`insert_hash`/`fold_n`/`blocks`)
  are exposed for the upstream white-box tests.
- `column::writer::encoder`: the `ColumnValueEncoder` trait (associated `T`/
  `Values`) is an abstract `ColumnValueEncoder(T)` class; `ColumnValues` is
  `Array(T)`; `usize` is `Int32`; `f16`/float `total_cmp` are hand-rolled IEEE
  total-order keys; `is_nan`/`compare_greater` dispatch on `T.class`; the
  statistics/encoding helpers (`get_min_max`, `update_min`/`update_max`,
  `fallback_encoding`, `has_dictionary_support`, `create_bloom_filter`,
  byte-budget counting) live on the `Column::Writer` module; geospatial
  accumulators are not ported (`flush_geospatial_statistics` returns `nil`).
- `column::reader`: Rust's `ColumnReader` enum becomes an abstract `ColumnReader`
  class with the single generic `ColumnReaderImpl(T)` subclass (instead of one
  variant per physical type); `GenericColumnReader<R, D, V>` is not
  parameterized (`ColumnReaderImpl(T)` fixes the level/value decoders);
  `read_records` drops the internal reservation callback; `get_column_reader`/
  `get_typed_column_reader`/`parse_v1_level`/`check_partial_record_fits` are
  `Parquet::Column` module methods. The upstream `ColumnReaderTester`/`make_pages`
  random test infrastructure is not ported (characterization specs used).
- `column::writer::truncate`: the statistics-truncation helpers (`truncate_utf8`,
  `truncate_and_increment_utf8`, `increment_utf8`, `increment`) and
  `can_truncate_value`/`is_utf8`/`truncate_min_value`/`truncate_max_value` are
  `Parquet::Column::Writer` module methods (upstream: free functions +
  `GenericColumnWriter` methods), including `truncate_statistics` (which rebuilds
  the `Statistics` value).
- `compression::codecs`: the `Codec` trait (mutating `Vec<u8>` buffers) becomes
  an abstract class appending to `Array(UInt8)`; the GZIP codec uses the Crystal
  stdlib (`Compress::Gzip`), SNAPPY the `naqvis/snappy` shard, the LZ4 block
  codec the `paddor/flint` shard (the LZ4 frame container is in-tree), ZSTD the
  `paddor/zinc` shard, and BROTLI the `OrelSokolov/brotli` shard. LZO has no
  implementation — `create_codec` raises `NYIError` for it, exactly as upstream
  does. `create_codec` is `Parquet::CompressionCodecs.create_codec`.
- `file::serialized_reader`/`serialized_writer`: `SerializedPageWriter`'s
  `W: Write` becomes the concrete `TrackedWrite`; page encryption is not ported
  here (see the `PageEncryptor` note); CRC verification is always performed
  (`decode_page`); the reader constructor is
  `SerializedPageReader.new_for_column`. `decode_page` is a `Parquet::File`
  module method. The reader state is a sealed class hierarchy
  (`SerializedPageReader::State` with `Values`/`Pages`) instead of the Rust
  enum, `usize` counters are `Int32` (`total_rows`, `page_index`) or `UInt64`
  (`offset`, `remaining_bytes`), and the `PageLocation` queue is a `Deque`. The
  `Iterator` impl yields `Column::Page` and signals exhaustion with
  `Iterator::Stop` (Rust: `Item = Result<Page>`); errors raise.
  `peek_next_page_offset` is `#[cfg(test)]`-only upstream and public here.
  `read_page_header_len_from_bytes` is a private instance method that measures
  the header with `IO::Memory#pos`. Reading through page locations is switched on
  by `ReadOptionsBuilder#with_page_index` (upstream's core reader always passes
  `None`; only its arrow layer enables the index). Known limitation (matches
  upstream): the page indexes of *encrypted* columns are written encrypted but
  neither port nor upstream decrypts them in the metadata reader, so
  `with_page_index` on an encrypted file fails while decoding the index.
- `file::writer`: `SerializedFileWriter<W: Write>` is a single
  `SerializedFileWriter` over an `IO` (via `TrackedWrite`); `TrackedWrite` now
  exposes `inner`/`inner_mut`/`into_inner`/`flush` (all over the erased `IO`).
  The `OnCloseColumnChunk`/`OnCloseRowGroup` callbacks are `Proc`s whose
  closures capture the owning writer (the row-group path threads the
  `SerializedPageWriter`/`ColumnWriter` and the file path threads the row-group
  metadata/index arrays); `next_column_with_factory` and `get_on_close` are
  inlined (`build_on_close`). `append_column`/`append_column_from_read`/
  `begin_appended_column`/`finish_appended_column` are ported (`ChunkReader`'s
  `Read` is an `IO`; `IO.copy` splices; the geospatial statistics branch is
  omitted). Encryption (`file_encryptor`, `set_column_crypto_metadata`, the page
  encryptor) are wired through the file writer. `append_column_from_pages`
  streams one stored blob at a time and preserves column crypto metadata while
  shifting offsets. Rust Debug formatting and the `parquet_buf`/`HeapSize`
  helpers remain unmodelled; `start_file` is public.
  `SerializedRowGroupWriter#close` returns the same `RowGroupMetaData` instance
  recorded in the file (upstream returns a pre-bloom-filter clone), so
  `flushed_row_groups` and `close` alias.
- `file::reader`: `FileReader`/`RowGroupReader` (upstream traits) are abstract
  classes; `ReadGroupPredicate` is a `Proc`; `SerializedFileReader<R>`/
  `SerializedRowGroupReader<R>` are non-generic over `ChunkReader` and their
  `Arc<R>`/lifetimes are erased. `with_range(start, end)` takes `end_` (Crystal
  reserves `end`) and raises `GeneralError` where upstream panics.
  `get_row_iter` is added by `record/reader.cr` (reopening the abstract classes);
  `FilePageIterator`'s `Box<dyn Iterator<Item = usize>>` row-group list is an
  `Iterator(Int32)`, its `Arc<dyn FileReader>` is a `FileReader`, and it includes
  the `PageIterator` module (`Iterator(Column::PageReader)`); the
  `TryFrom<File>`/path constructors map to `SerializedFileReader.new`, and the
  `IntoIterator`/`try_from` impls are not modelled.
- `record::api`: Rust's payload-carrying `Field` enum is a sealed class
  hierarchy (`Field::Int`, `Field::Str`, ...) with `type_name`/`primitive?`
  (`is_primitive`); the `RowAccessor`/`ListAccessor`/`MapAccessor`/`RowFormatter`
  traits are modules (`fmt` returns a `String`); `usize` is `Int32`;
  `serde_json::Value` is `JSON::Any` (integers stored as `Int64`); the `f16`
  value is carried as the decoded `Float64`; float/date/time/decimal `Display`
  strings are reproduced with helpers (`rust_display_f32`/`_f64`/`_f16`,
  `convert_decimal_to_string` via `BigInt`); `Row`/`List`/`Map` constructors are
  generic over `Field`-subtype tuples/arrays (Crystal tuple invariance);
  `RowColumnIter` is an `Iterator`.
- `record::triplet`: the `TripletIter` enum is an abstract class with the generic
  `TypedTripletIter(T)`; `TypedTripletIter#current_value` (raw `T`) is renamed
  `typed_value` (it would otherwise clash with `TripletIter#current_value`, which
  returns a `Field`).
- `record::reader`: the `Reader` enum is an abstract class with
  `PrimitiveReader`/`OptionReader`/`GroupReader`/`RepeatedReader`/
  `KeyValueReader` subclasses; `GroupReader` widens its child array; the
  `Either` file-reader wrapper is an optional `FileReader`; `RowIter`/
  `ReaderIter` are `Iterator(Row)` (errors raise); `get_row_iter` is added by
  reopening `FileReader`/`RowGroupReader`. The `record_reader.rs`/
  `record_writer.rs` traits (used by `parquet_derive`) are not ported.
- `bin`: the CLI tools live under `src/parquet/bin/` (logic, linted/tested) with
  thin `bin/` executables; `clap` argument parsing is replaced by
  `OptionParser`. `Cli.parse_column_path` (upstream `cli/mod.rs`) raises
  `GeneralError` where upstream returns `Result<_, String>`. `parquet-index`'s
  `Args.run` writes to an `IO` parameter (upstream prints to stdout) and the
  byte-array max fallback reproduces upstream's `min`-label copy-paste bug for
  output parity. `parquet-read`/`parquet-rowcount`/`parquet-show-bloom-filter`/
  `parquet-layout` likewise write to an `IO`; `parquet-layout` emits JSON via
  `JSON.build` (pretty, 2-space) and its serde `DebugSerialize` wrapper is
  `to_s`; `parquet-show-bloom-filter` raises on an invalid column path (upstream
  prints and exits); `parquet-schema` (`-v`/`--verbose`) and `parquet-concat`
  (`concat`/`run`, taking an output `IO` and `ChunkReader`s) write to an `IO`.
  `parquet-rewrite` now streams Arrow reader batches to ArrowWriter, with
  characterization specs for every CLI option and public file behavior.
  Clap's private value enums are folded into validated Args tables and property
  mapping, preserving kebab spellings, booleans, bloom dependencies and page
  statistics precedence. OptionParser renders diagnostics/help; argument errors
  return 2, runtime errors 1, and version output uses the port package version.
  Size options fit Int32 rather than usize; compression level retains u32 parsing
  and the upstream Zstd wrapping cast. Source metadata arrays are copied and
  ArrowWriter refreshes ARROW:schema. Input is opened once (upstream twice);
  input/schema/property validation precedes destination creation, and handles
  close on failure. Later errors may leave partial output, as upstream.
  LZO remains unsupported by both pinned implementations. Legacy INT96 inputs
  normalize to annotated INT64 timestamps, exactly as the pinned Rust rewrite;
  its record JSON rendering differs from raw INT96. The interop script compares
  exact records against Rust rewrite output and exercises seven codecs in both
  directions, V1/V2, bloom/header statistics, omitted column paths and row groups.
  `parquet-fromcsv` ports schema-directed CSV/TSV conversion with source root
  preservation, batch/group limits, compression and bloom settings. Native
  exceptions/cause replace the error enum and From conversions; contextual
  display retains the original message followed by context. Help option coverage
  replaces Clap's exact formatted snapshot; diagnostics/status/version follow
  the native CLI conventions above. Int32 sizes replace usize; single-character
  options retain Rust's truncating u8 cast. PARQUET_FROM_CSV_BATCHSIZE provides the
  batch default, overridden by the CLI. The parsed double-quote option is unused
  in the pinned binary, and TSV's unset quote retains Arrow's default double
  quote; both behaviors are preserved. Schema validation precedes output
  creation, then input opening, as upstream; later failures can leave partial
  output. Snappy uses framed streams, Gzip accepts multiple members, LZ4 accepts
  frames and Zstd handles concatenated/skippable frames. Brotli/LZ4/Zstd use
  existing buffer APIs (peak memory includes compressed/decompressed input),
  while uncompressed/Snappy/Gzip reads and Arrow batch writing are incremental.
  Lazy decoder creation puts malformed input under CSV read context. Buffered
  EOF detection preserves upstream empty-stream acceptance for Snappy/LZ4 and
  rejection for Gzip/Brotli/Zstd despite native codec defaults. LZO is
  unsupported as upstream. Tests assert exact values, nulls, schema/root and
  row groups; scripts/verify_fromcsv_interop.cr compares exact records with
  pinned Rust for six input/output codecs, V1/V2, dialects and concatenated input.
- `schema::printer`: the `print_*` free functions are `Parquet::Schema` module
  methods; `print_row_group_metadata`/`print_column_chunk_metadata` are public;
  `fmt::Write`/`io::Write` are `IO` (the `Printer` writes to a `String::Builder`
  or `IO`); logical-type payloads are read through the wrapper's `value`
  (`LogicalType::Integer#value.bit_width`); `has_id` → `has_id?`; the `_Unknown`
  variant is `LogicalType::UnknownField`; `Variant`'s `specification_version` is
  printed with `Some(..)`/`None`.
- `parquet-variant::path`: lives in `src/parquet_variant/` (`ParquetVariant`
  module) and is required from `src/parquet-cr.cr`;
  `arrow_schema::ArrowError::ParseError` is `ParquetVariant::ParseError` (Display
  `"Parser error: {msg}"`); `Cow` field names are owned `String`s; `usize` index
  is `Int32`; `VariantPath#deref` to `[VariantPathElement]` maps to `#[]`/`#path`;
  `join` mutates and returns `self` (upstream consumes); the parser scans bytes
  (ASCII delimiters never occur inside a UTF-8 sequence).
- `parquet-variant::utils`/`decoder`/`metadata`: `utils` helpers are
  `ParquetVariant::Utils` module methods over `Bytes` (no `SliceIndex` generic;
  `array_from_slice` returns a `Bytes` slice); because Crystal's
  `String.new(Bytes)`/`String#byte_slice?` do **not** validate UTF-8,
  `string_from_slice`/`slice_value` check `valid_encoding?` explicitly;
  `OffsetSizeBytes` is an enum with `unpack_u32*` methods; `VariantMetadata` is a
  class (mutable `bytes`) with `is_sorted`→`sorted?`, `is_empty`→`empty?`,
  `is_fully_validated`→`fully_validated?`, `get` raising instead of `Result`, and
  `iter_try == iter`. `VariantMetadata#borrowed_field_id`'s zero-copy fast path
  cannot be modelled (Crystal copies substrings), so it always returns `nil` and
  `get_entry` always uses the name search (an intentional divergence; behavior is
  still correct, just slower).
- `parquet-variant::decimal`: `VariantDecimalType` is a marker module (the
  `Into<Variant>` supertrait, `From`/`TryFrom` conversions, and the
  `MAX_PRECISION`/`MAX_UNSCALED_VALUE` associated consts are deferred until
  `Variant` is ported; the consts are per-struct, generated by the
  `define_variant_decimal` macro). `Int128` is the `VariantDecimal16` native type;
  `%`/`/` map to `Int#remainder`/`Int#tdiv` to match Rust's truncated semantics;
  overflow during rescale raises `InvalidArgumentError`.
- `parquet-variant::builder::metadata`: `MetadataBuilder` is a marker module
  (`int_size`/`write_offset` are module methods); `IndexSet` is an
  insertion-ordered `Array(String)` + `Hash(String, Int32)`;
  `Extend`/`FromIterator` are `#extend`/`.from_iter`; `is_sorted` → `sorted?`;
  `into_inner` returns a `Bytes` copy; the private `metadata_size` reserve helper
  is not ported; `ReadOnlyMetadataBuilder`'s borrowed fast path is disabled, so
  it always searches by name and raises `InvalidArgumentError` for unknown
  fields.
- `parquet-variant::decoder`/`ShortString`: `VariantBasicType`/
  `VariantPrimitiveType` are enums (`try_from` → `from_byte`, raising
  `InvalidArgumentError`); the primitive `decode_*` helpers are `Decoder` module
  methods over `Bytes`; `OffsetSizeBytes#unpack_u32*`/`map_bytes_to_offsets` as
  before. chrono `NaiveDate`/`DateTime`/`NaiveTime` are `Time`/`Time`/`Time::Span`
  (Crystal has no date-only or time-of-day type), `CastError` replaces
  `ArrowError::CastError`, and `Uuid` is `UUID`; `ShortString` is a struct
  (owned `String`; `Deref` → `#value`/`#as_str`, `From` → `.try_new`).
- `parquet-geospatial`: lives in `src/parquet_geospatial/` (`ParquetGeospatial`
  module), required from `src/parquet-cr.cr`; `serde_json::Value` is `JSON::Any`
  (`Metadata#to_json`/`.from_json` are hand-written to preserve the
  `{"crs":..,"edges":..}` field order and skip absent fields);
  `ArrowError::JsonError`/`InvalidArgumentError` are
  `ParquetGeospatial::JsonError`/`InvalidArgumentError` (`errors.cr`); `Edges`'s
  `#[serde(rename_all = "lowercase")]` is `json_key`/`from_json_key`; the
  WKB implements `Arrow::Extension::ExtensionType(Metadata)` for binary storage.
  `JsonError` now inherits `Arrow::ArrowError` so malformed WKB metadata returns
  false from Arrow Field validation, retaining the port's error name/prefix.
  Other geospatial error classes keep their existing Parquet hierarchy.
- `parquet-geospatial::interval`: `IntervalTrait` is a marker module;
  `Interval`/`WraparoundInterval` are value structs with explicit `==` and
  `Debug`→`inspect`; `TryFrom<WraparoundInterval>` is `Interval.from_wraparound`
  (raising); `is_wraparound`/`is_empty` are `wraparound?`/`empty?`; NaN-ignoring
  `f64::min`/`max` are explicit helpers (Crystal's propagate NaN);
  `WraparoundInterval#split` is `protected`; `From<(f64,f64)>`/`From<Interval>`
  are `WraparoundInterval.new(lo, hi)`/`.from_interval`.
- `parquet-variant-json` (Variant → JSON): lives in `src/parquet_variant_json/`
  (`ParquetVariantJson` module), required from `src/parquet-cr.cr`; the upstream
  `VariantToJson` extension trait becomes `ParquetVariant::Variant#to_json`
  (also Crystal's `Object#to_json(IO)` hook), `#to_json_string` and
  `#to_json_value`. Divergences: `serde_json::Value` is `JSON::Any` (numbers are
  `Int64`/`Float64`, so a `u64` above `Int64::MAX` becomes a `Float64`),
  `arrow_schema::ArrowError` is `Parquet::ArrowError` ("Invalid float value" /
  "Invalid double value" for non-finite floats), chrono's
  `NaiveDate`/`NaiveDateTime`/`NaiveTime` are `Time`/`Time::Span`, and
  `Formatting` reproduces chrono's `to_rfc3339` (shortest 0/3/6/9 fraction +
  `+HH:MM`) and Rust's `Display` float spelling (`0` rather than `0.0`;
  Crystal's exponent form is kept for magnitudes Crystal prints in scientific
  notation). Upstream's `test_decimal_precision_behavior` equality between the
  parsed string and `to_json_value` is relaxed by one ULP for values above
  2^53: serde_json's float parser multiplies by powers of ten (matching
  `integer as f64 / divisor as f64`) while Crystal's parser is correctly
  rounded. The JSON → Variant direction is `JsonToVariant#append_json`
  (included in `VariantBuilder`, `ListBuilder` and `ObjectFieldBuilder`, since
  Crystal has no blanket trait impl): numbers become the narrowest
  `Int8..Int64` that fits or a `Double`, invalid documents raise
  `ArrowError("JSON format error: ...")` (upstream `InvalidArgumentError`), and
  upstream's `#[ignore]`d decimal expectations are not ported. The
  `test_json_to_variant_object_very_large` fixture (34 MB) is split into a
  full-size encoded-size check plus a reduced-fan-out roundtrip check to keep
  the suite fast; its upstream value/metadata lengths (34_147_849/2485) are
  asserted exactly.
- `parquet-variant-compute` (variant arrays): lives in
  `src/parquet_variant_compute/` (`ParquetVariantCompute` module), required from
  `src/parquet-cr.cr`. The array layer includes `VariantArray`,
  `ShreddingState`, the `arrow.parquet.variant` `VariantType` (an
  `Arrow::Extension::ExtensionType(String)` with empty metadata),
  `VariantArrayBuilder` and the JSON kernels. Divergences: `usize` counts are
  `Int32`; `arrow_schema::ArrowError` is `Parquet::ArrowError` (message bodies
  keep the upstream text, e.g. "Invalid VariantArray: requires StructArray as
  input"); upstream's panicking `value()` raises; `VariantArrayIter` is a
  Crystal `Iterator(Variant?)` (a null row is `nil`), so `next_back` is a plain
  method and the tests materialize (`to_a.reverse`) instead of using `.rev()`;
  the RunEndEncoded metadata encoding is not ported (Binary/LargeBinary/
  BinaryView/Dictionary are); `StructArray#nullable?` (logical null count)
  stands in for upstream `StructArray::is_nullable` in `VariantArray#field`; and
  `VariantArrayBuilder` builds the final `BinaryViewArray`s with
  `Arrow::BinaryViewBuilder` per row instead of upstream's raw-buffer
  `binary_view_array_from_buffers` (same values, different buffer sharing).
  Shredded arrays are supported: `typed_value` interpretation covers the
  types with a direct variant counterpart (composite types require
  `unshred_variant`, as upstream's `try_value` does), the lenient
  read synthesizes an all-null `value` column, and
  `canonicalize_and_verify_data_type` (public here so the ported test can call
  it) narrows decimals and tags untagged `FixedSizeBinary(16)` leaves with the
  `arrow.uuid` extension. Canonicalization copies fields with a copied
  `Metadata` because the port's `Field#with_*` helpers mutate (upstream clones
  an `Arc<Field>` with immutable metadata).
  `unshred_variant` is ported: the Rust enum of row builders is a sealed class
  hierarchy, the primitive/timestamp/decimal builders reuse
  `VariantArray.typed_value_to_variant` (so failures raise the "Cast failed at
  index N ..." message rather than the per-type messages), and the
  `Time64`/`Timestamp`/`FixedSizeBinary` "not a valid variant shredding type"
  errors render the full data type. `VariantValueArrayBuilder` builds its
  `BinaryViewArray` with `Arrow::BinaryViewBuilder` (nulls applied while
  appending) instead of upstream's raw-buffer path. `TypeConversion` maps the
  per-arrow-type traits to one function per target type, keeps the errors as
  exceptions (`CastError` in strict mode, printing `Variant#debug_string`),
  derives the decimal target kind from the Crystal type instead of the
  `DecimalCastTarget` trait, reimplements `single_float_to_decimal` with upstream
  half-away rounding and native-range checks, and parses decimal strings with bounded digit arithmetic
  before narrowing. This supports exponent notation, ASCII whitespace, long
  mantissas, precision overflow and all Decimal32/64/128/256 widths without
  overflowing intermediate integers. Decimal256 rescaling widens to Arrow::I256
  first. Decimal256 shredding returns nil and public shredding rejects that
  target, matching the pinned upstream's explicit behavior. `VariantToArrow`
  collapses upstream's builder enums into a `RowBuilder` hierarchy with one generic
  `ValueRowBuilder(B, T)` for the macro-generated builders, uses `Int32` for
  capacities, raises (`CastError`/`InvalidArgumentError`/`NYIError`) where
  upstream returns `Result`, and reuses the data type for decimal
  precision/scale and timestamp timezones instead of builder setters. The list
  builders collapse upstream's four `(offset width, view)` types into one
  generic `ListRowBuilder(O)` with a `view` flag; `ShredVariant::ElementBuilder`
  wraps a shredded (value, typed_value) pair for list elements. The path
  extractor wraps any `RowBuilder` instead of nesting
  the builder enum, and the binary-variant output returns the `VariantArray`'s
  inner `StructArray` (`VariantArray` is not an `Arrow::ArrayBase` in the port).
  `UnionRowBuilder` keeps the upstream ranking table, including Decimal256
  fit checks and natural decimal-width preferences. Shredding uses a `RowBuilder`
  hierarchy with shared primitive/list buffers and an ordered `Hash` of object
  children, creates residual binary objects lazily, and uses a sorted schema
  tree. `ShreddedSchemaBuilder#with_path` mutates and returns self; the
  `IntoShreddingField` trait is `ShreddingField.from` overloads; list schema paths
  raise `InvalidArgumentError` instead of upstream's panic. Error bodies use
  Crystal's type/path rendering. Counts are `Int32` and errors raise. Composite
  typed values must be unshredded before `VariantArray#value`, as upstream.
  `variant_get` uses a value `GetOptions` struct, `ShreddingState | PathStep`
  for upstream's three-way path step, Int32 counts/indices, and raising APIs.
  Variant output is the storage `Arrow::StructArray`. Perfect matches preserve
  array identity when no parent mask is needed; masked matches rebuild only
  ArrayData nulls. Safe materialization errors map to nulls and strict ones
  propagate. Invalid Variant extension tags are treated as concrete requests,
  bridging the custom extension's Parquet error hierarchy. Decimal256 dictionary
  output requires Arrow `b05366a`; run-end output requires the I256 comparator
  from `49d07ce`. Both integration tests pass with these independent fixes;
  see `shards_info/arrow_cr.4.md`.
  Arrow → Variant uses a RowBuilder class hierarchy and scalar conversion
  closures instead of Rust enums/macros. Dictionary keys are read directly;
  run-end cursors use raw run ends as in the pinned source (including slices).
  Fixed-size binary remains binary even at 16 bytes. Nanosecond timestamps
  preserve precision, matching implementation rather than the stale upstream
  truncation comment. Crystal Time supports years 1..9999, a narrower range
  than chrono; out-of-range dates/timestamps produce Variant null in safe mode
  and conversion errors in strict mode. Boundary tests cover representable
  dates/timestamps and rejection of year zero. Microsecond decoding splits
  seconds/remainder to avoid an intermediate Int64 nanosecond overflow.
- `chunk_reader`: `BytesChunkReader#get_read`/`get_bytes` and
  `FileChunkReader#get_bytes` raise the upstream `EOF` messages
  ("Expected to read ... while file has length ..."); `FileChunkReader` reads
  through `File#read_at` blocks (Crystal's `read_at` yields an `IO`).
- `column::page_store`: Rust traits become abstract `PageStore` and
  `PageStoreFactory` classes; `PageKey` is a UInt64 value struct with comparison
  and hashing, and `PageStoreArgs` retains its descriptor by reference.
  Column indices/array slots are Int32, resident-byte counts are Int64, and
  fallible methods raise `GeneralError`. The args constructor is public rather
  than crate-only. `Bytes` slices transfer by convention (callers must not mutate
  stored slices), since Crystal does not enforce Rust's ownership/immutable Bytes.
  Stores are single-owner and unsynchronized; Rust Send/Sync bounds have no
  direct Crystal equivalent. Factories must coordinate any shared state they add.
- `column::writer` data holders: Rust's `LevelDataRef<'a>` enum (borrowed
  `&[i16]`) is a Crystal struct carrying a `Kind` with an owned `Array(Int16)`;
  `usize` sizes are `Int32`; `ColumnCloseResult::update_dictionary_location(mut
  self)` mutates and returns `self`; `PageMetrics`/`ColumnMetrics<T>` are
  mutable classes.
- `column::writer::byte_budget_chunker`: `SubBatchStrategy` is a struct carrying
  a `Kind` + `Int32` size; `usize` fields are `Int32`; `pick_sub_batch` takes the
  value type `T` explicitly (the encoder's `Values` associated type is not
  modelled here).
- `column::writer`: `GenericColumnWriter<'a, E>` becomes `GenericColumnWriter(T)`
  fixing `E = ColumnValueEncoderImpl(T)`; the `ColumnWriter` enum becomes an
  abstract class with the single generic `ColumnWriterImpl(T)` subclass; `usize`
  becomes `Int32`; `encodings` is a `Set(Encoding)`. Arrow size/memory estimates
  and distinct-count overrides are exposed. Encryption properties travel through
  the file writer and page encryptor instead of a separate Arrow column setter. `Naming/AccessorMethodName`, `Lint/NotNil`,
  `Lint/ShadowingOuterLocalVar`, and `Metrics/CyclomaticComplexity` are excluded
  for `src/parquet/column/writer/writer.cr` in `.ameba.yml`.
- `encodings::byte_stream_split`: `ByteStreamSplitEncoder(T)`/`Decoder(T)` and
  `VariableWidthByteStreamSplitEncoder(T)`/`Decoder(T)` with `T.class`; the
  constant `split_streams_const`/`join_streams_const` and `_variable` kernels
  are a single runtime `type_size` routine (byte-identical layout; the upstream
  block-4 loop only reorders the same writes); the fixed-width decoder joins
  into a scratch buffer and decodes each value with `IO::ByteFormat::LittleEndian`
  instead of a memory reinterpret; unsupported types raise as upstream.
- `encodings::rle_value`: `RleValueEncoder(T)`/`RleValueDecoder(T)` with
  `T.class` (BOOLEAN only, enforced by construction); `RleValueDecoder#get`
  decodes into a scratch `Array` before copying into the output buffer (the
  Crystal `RleDecoder#get_batch` only accepts an owning `Array`); the
  `DEFAULT_RLE_BUFFER_LEN` reservation is inlined as a `[0; 4]` length prefix.
- `encodings::alp`: `AlpFloat`/`AlpExact` traits and their associated types are
  replaced by `F.class`-dispatched `Alp` module methods (`Alp.encode_value`,
  `Alp.exact_wrapping_add`, ...); the exact integer types (`u32`/`u64`) are
  carried uniformly as `UInt64` with width-masked wrapping arithmetic so f32
  wraps at 32 bits; `AlpHeader#serialize` returns `Bytes` (not `[u8; 7]`);
  `AlpEncodedVectorView`'s `*_range` methods are inlined into `AlpDecoder`
  (slice arithmetic at `load_current_vector`); `Scratch`/`StreamingPage` use
  element counts for `estimated_memory_size` (no `Array` capacity); the
  `is_better`/`rank` helpers become `better?`/`rank`.
- `encodings::traits`/`registry`: the `Encoder`/`Decoder` traits become abstract
  generic classes (with `put_spaced`/`get_spaced` as concrete base methods); the
  `GetEncoder`/`GetDecoder` traits and `get_encoder`/`get_decoder` free functions
  become `T.class`-dispatched `Encodings.get_encoder`/`get_decoder` module
  methods returning `Encoder(T)`/`Decoder(T)`; `PhysicalType` is
  `Encodings.physical_type(T)`. Because Crystal instantiates generic classes per
  concrete type, combinations upstream would construct-but-fail-later (e.g.
  `DeltaBitPackEncoder<T>` for a non-integer `T`) raise `ParquetError` directly.
  `DeltaLengthByteArrayEncoder`/`Decoder` were made generic over `T` to match the
  upstream registry's cross-type encoder construction.
- Metadata snapshots now clone owned metadata arrays/bytes directly, preserving
  statistics flags and full encoding statistics. Schema/column descriptors and
  page-index providers remain shared; file decryptors copy AAD while sharing
  properties/block decryptors, matching the source's Arc semantics. Native
  binary statistics copy mutable slices rather than sharing immutable Rust Bytes.
  Builders/lower-level writers retain reference ownership conventions; clone
  before editing/reusing inputs. The high-level metadata writer clones borrowed
  rows before index backfill/encryption, and encrypted file writers return a
  plaintext metadata graph. The old shallow-copy/missing-plaintext divergence is
  resolved. Compression configuration is named `compression_config` to retain
  the native `compression` codec property; `set_compression` stores only the
  codec. `column_descr` supplies the source's shared `column_descr_ptr`, and
  `encryption_algorithm=` replaces its consuming file-metadata helper. See
  [metadata snapshots](../../docs/metadata-snapshots.md) for tests and ownership.
  The Rust FileMeta/WriteThrift wrapper remains inlined in MetadataObjectWriter.
- On-demand page indexes port the full custom_page_index example, promoted to
  a reusable native provider and executable. Native `?` predicates/downcasting,
  Int32 indices and stable borrowed metadata/file bytes replace Rust ownership
  and usize; range violations raise GeneralError rather than slice panics.
  Clones own metadata and decoded index buffers while sharing stable file bytes.
  The sample uses memory IO rather than a temporary path. Sparse cache flags,
  absent entries, retries and explicit fetch behavior match the pin; no automatic
  page pruning, encrypted-index decryption or network IO reduction is claimed.
  See [on-demand page indexes](../../docs/on-demand-page-index.md).
- Explicit row-group fetching ports the complete read_with_rowgroup example,
  reusing native dense ChunkReader and one-shot PageIterator adapters. Native
  fiber-suspending AsyncFileReader replaces futures/Tokio; Int32 counts/offsets
  and guarded dense offsets replace usize and slice panics. The runnable example
  uses the pinned default fixture and existing Arrow pretty tables (whose native
  rendering differences remain). Selection affects decoding, not whole-chunk
  fetching. Repeated fetches and the taken-slot state after IO failure match the
  source. Clone owns metadata/projection/slots and shares chunk bytes. See
  [explicit row-group fetching](../../docs/explicit-row-group-fetching.md).
- The complete object-store example uses native MemoryObjectStore,
  BufferedObjectWriter and SpawnedReader, preserving both exact batch equality
  checks and printed row counts. The minimal example reader uses suffix-based
  metadata without HEAD and converts store exceptions to ExternalError with their
  original cause. String paths, Int32 sizes and fiber-suspending IO replace Rust
  types/futures; a one-worker Crystal context replaces Tokio and has runtime-managed
  worker lifetime instead of shutdown_background. The writer uses one in-memory
  buffered PUT rather than SDK multipart behavior. Mismatch guards raise GeneralError
  instead of assertion panics; Debug formatting and speedup parity are not claimed.
  See [object-store example](../../docs/object-store-example.md).
- The full round-trip benchmark preserves 46 case names/order, dimensions,
  encoding choices and seed derivation, while native RNG and first/warm CSV
  sampling replace StdRng/Criterion. Random bytes/statistical timing equivalence
  are not claimed. Exact values are validated outside timing; each timed write
  resets its buffer instead of appending source benchmark files. Positive and
  batch-divisible dimensions guard source loop failure cases. Native structs,
  Int32 counts and GeneralError assertions replace Rust conventions. See
  [round-trip benchmark](../../docs/round-trip-benchmark.md).

## Notes

- Upstream test oracle: `cargo test -p parquet` from `vendor/arrow-rs`
  (Rust 1.99 toolchain available locally).
- `arrow-cr` is a `path: ../arrow-cr` dependency (checked out next to this repo;
  `make install`/`shards install` symlinks it into `lib/arrow-cr`). Arrow gaps
  that must be filled in the sibling repo are filed as
  `shards_info/arrow_cr.<n>.md` — read them before starting arrow work.
- Codec write-path interop: build the upstream CLI once with the needed codecs
  (`cargo build -p parquet --features cli,lz4,snap --bin parquet-read`), then
  `/tmp/parquet-read <file>` reads files this port wrote (used to validate the
  LZ4/LZ4_RAW writer: multi-row-group INT64 and dictionary-encoded string
  pages). Delete `vendor/arrow-rs/target` afterwards — the multi-GB target tree
  triggers Spotlight (`spotlightknowledged`/`fskitd`) on the external volume,
  which starves spec runs (minutes of blocked I/O).
- Golden fixtures: `vendor/arrow-rs/parquet-testing/data`.
- Parity scripts must be run from the `crystal_forge` skill:
  `/Users/dominic/.agents/skills/crystal_forge/skills/cross-language-crystal-parity`.
- Environment: the checkout is on exFAT, which emits macOS AppleDouble `._*`
  sidecars; the `Makefile` strips them before format/lint/test (a `._foo_spec.cr`
  matches crystal's `*_spec.cr` glob). If a git pack op prints
  `non-monotonic index .../._pack-*.idx`, delete the sidecars.

## Synchronous Arrow writer implementation choices

- `ArrowWriter` owns an IO-based `SerializedFileWriter`; mutable option builders
  follow the port's conventions. It holds the row group's column encoders
  directly rather than wrapping them in a separate ArrowRowGroupWriter class.
  The row-group factory absorbs the private column factory. Low-level
  compute_leaves/ArrowColumnWriter/ArrowColumnChunk splicing remains available.
- `LevelInfoBuilder` uses recursive per-record traversal rather than Rust enum
  variants with bulk/direct/scan fast paths. Private LevelData is represented
  by optional materialized Int16 arrays; nil is Absent, uniform runs expand.
  Exact nested definition/repetition levels and value indices are tested.
- ByteArray gathers materialize native ByteArray values before using the
  existing ColumnValueEncoderImpl(ByteArray). Its dictionary, fallback,
  statistics, byte budgets and bloom filters are reused. The private Rust
  view-aware zero-copy encoder/storage wrappers are not separate Crystal APIs.
- ArrowPageWriter keeps opaque store keys without a shared mutex (synchronous
  single-owner mutation). Plaintext headers/data use separate blobs; encrypted
  pages use a single serialized blob. Both stream dictionary-first and reclaim
  each blob as consumed. Store resident-byte reporting feeds writer estimates.
- Distinct values use owned raw-byte String keys instead of a set of 64-bit
  hashes. Dictionary keys are counted and byte-view arrays skipped, matching
  the pinned source's selection. Memory allocation/performance differs; no
  zero-copy or throughput equivalence is claimed. Writer counts are Int32,
  byte estimates Int64, and errors raise Parquet error subclasses.
- Decimal256 IPC required the independent arrow-cr commit 9a5612c. The original
  default writer round-trip assertion stayed intact; this task did not edit the
  shard. MonthDayNano interval writing remains NYI, as upstream.

## Asynchronous Arrow writers and object publication

- Rust futures/AsyncWrite/boxed generic sinks map to yielding methods on
  AsyncFileWriter plus an explicit AsyncIOWriter adapter. IO completion flushes
  and closes unless configured as borrowed. Writers have one owning fiber;
  into_inner returns a shared reference without finalizing, rather than
  consuming the writer. No implicit finalizer, background queue or safe retry
  of a failed AsyncArrowWriter is promised. Detached Bytes chunks copy the
  internal IO::Memory buffer; offsets remain cumulative through TrackedWrite.
- The deprecated ParquetObjectWriter wraps native BufferedObjectWriter rather
  than Rust object_store::buffered::BufWriter. It buffers a whole object for a
  single PUT, using Int32-sized buffers; multipart uploads/abort/retries and
  SDK authentication remain application adapter responsibilities. Memory and
  HTTP implementations support publication, and HTTP PUT accepts 200/201/204.
  Existing read-only ObjectStore subclasses retain their API through a default
  NYI put method. See [async storage](../../docs/async-storage.md) for lifecycle,
  failure semantics and tests. No dependency or installed shard was changed.

## Metadata memory accounting

`ParquetMetaData#memory_size` and recursive HeapSize support use Crystal layout,
not Rust's Vec/Arc/hashbrown/enum sizes. MemorySize.retained counts native class
storage, reserved array/hash buffers, String headers/text/terminators, and visible
Slice bytes. Struct/scalar inline storage belongs to the containing object or
array. There are no Arc reference-count allocations. Counts are Int64, not usize.
Known row-group/schema and column/descriptor back-references are omitted;
other shared references can be counted repeatedly. Custom page-index providers
and application key retrievers remain opaque and excluded, as upstream.

Native LogicalType classes and CRS text are allocated, unlike Rust enum data,
and are included. Native index NaN-count buffers are also included; the pinned
Rust memory.rs omits that buffer. These explicit differences make the estimate
useful for the actual port's metadata caches. Read-only container layout queries
follow Crystal 1.22; allocator overhead, GC bookkeeping and invisible backing
capacity of Slice views are excluded. See [metadata memory](../../docs/metadata-memory.md)
for exact accounting rules and tests. No installed shard was modified.

## Typed physical-column descriptors

Rust DataType associated native types become per-descriptor `T` aliases on all
eight public physical descriptors (BoolType through FixedLenByteArrayType).
Native implementation generics stay specialized on raw value types; descriptor
helpers select those implementations. `PHYSICAL_TYPE` lives on descriptors
rather than builtin scalars; get_type_size returns Int32 native representation
size instead of Rust usize/layout. Nullable conversions use checked casts;
convenience conversion panics become GeneralError with upstream messages.

Rust owned/ref/mut conversions and serialized mutable borrows become shared
Crystal references, without consumption or borrow/lifetime enforcement. Existing
native T.class callers and descriptor callers receive the same reader/writer.
See [typed columns](../../docs/typed-columns.md) for supported APIs, ownership,
exact integration coverage and macro/free-function inventory limitations.

## Generated mixed Thrift unions

`define_thrift_union{,_with_unknown}` use a declarative unit/variant block to
generate native Kind/optional-payload structs, constructors, getters and codecs.
Rust raw-IDL parsing, borrowed payload lifetimes and trait associated constants
map to native declarations, owned values and thrift_element_type. The production
ColumnCryptoMetaData/EncryptionAlgorithm constructors, accessors, Kind ordinals,
wire bytes and exact native memory layouts remain intact.

Strict/forward unknown handling, empty/unit validation and single-field rules
follow the pinned macros. Their multi-field diagnostic's literal `{}` is now
preserved (the old manual codecs normalized it); a standalone Rust expansion of
the pinned general_err/concat expression confirms the output. Direct Bool union
payloads retain the pin's non-round-tripping read limitation; use a struct bool
field or Array(Bool). Owned String dispatch now uses read_bytes_owned, correcting
the previous borrowed-path mapping on IO input without changing borrowed APIs.
See [Thrift unions](../../docs/thrift-unions.md) for API, coverage and the source
discovery limitation for generated methods. No installed shard was modified.
