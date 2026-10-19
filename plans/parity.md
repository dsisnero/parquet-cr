# parquet-cr Parity Roadmap

> **Start here.** New session? Read the [Where we are](#where-we-are) block
> below for the current focus and next chunk, then follow
> [docs/resuming.md](../docs/resuming.md). Do not re-derive state from git.

Curated, branch-sized feature roadmap. Generated manifests live in
`plans/inventory/`; the per-symbol ledger is
`plans/inventory/rust_port_inventory.tsv`.

- Upstream: <https://github.com/apache/arrow-rs>
- Pinned revision: `60.0.0`
  (`ef1fa157977633f0ba21aa921f9ef3d5c669235b`)
- Vendored at: `vendor/arrow-rs` (submodule)
- Scope: `parquet` (core), then `parquet-variant*`, `parquet-geospatial`,
  `parquet_derive*`
- Deferred: `parquet/src/arrow/` (Arrow interop) — depends on a Crystal Arrow
  implementation built separately in the sibling repo `../arrow-cr` (same
  `60.0.0` pin); `async_reader`/`async_writer`/`object_store`.

Status legend: `[ ]` not started, `[~]` in progress, `[x]` complete,
`[!]` intentional divergence.

Each item is a shippable feature (roughly one branch/commit). The exhaustive
method-level state lives in the ledger, not here.

Do not weaken upstream tests or fixtures. Port behavior, not style. Preserve
numeric widths, half-open ranges (`start..end` → `start...end`), and the on-disk
little-endian layout.

## Where we are

- Plan layout: single file. If it grows too large, split into
  `plans/parity/<area>.md`, link each here, and mark each split file
  complete/in-progress plus the file being worked on next.
- Current focus: **Arrow interop is underway.** `parquet-cr` now depends on the
  sibling `arrow-cr` port (`path: ../arrow-cr`) and the first chunk — the
  Arrow → Parquet *schema* direction (`Parquet::Arrow::ArrowSchemaConverter`) —
  is complete, and so is the `ARROW:schema` IPC metadata round-trip
  (`encode_arrow_schema`/`get_arrow_schema_from_metadata`/
  `add_encoded_arrow_schema_to_metadata`, using arrow-cr's `arrow-ipc` schema
  message), plus `ProjectionMask` and the primitive type mapping
  (`convert_primitive`), and the Parquet → Arrow schema direction
  (`ParquetField`/`Visitor`/`convert_schema`/`parquet_to_arrow_schema*`/
  `parquet_to_arrow_field`/`FieldLevels`). The schema module is now complete in
  both directions, and the arrow-reader foundations are started (bit utilities
  the null-padding `ValuesBuffer`, and the definition-level buffer/packed
  decoder, plus the `ArrayReader`/`RowGroups` interfaces and
  `EmptyArrayReader`), and the column reader now exposes the pluggable-decoder
  path the record reader needs (`new_with_decoders` +
  `read_records_with_reservation`), and `GenericRecordReader`
  (`arrow/record_reader/mod.rs`), the numeric `PrimitiveArrayReader` and the
  byte-array `OffsetBuffer` (values buffer + null padding) and the arrow
  specialized decoders (`DictIndexDecoder`, `DeltaByteArrayDecoder`) are
  complete, `PrimitiveArrayReader` covers BOOLEAN as well, and the byte-view
  `ViewBuffer` is ported, and the upstream `array_reader/test_util.rs` helpers
  are available to specs (`spec/support/array_reader_util.cr`). The
  variable-length leaf readers are done: `ByteArrayReader` (Utf8/Binary, small
  and large offsets) and `ByteViewArrayReader` (Utf8View/BinaryView), both over
  the shared record reader, and the composite readers `StructArrayReader`,
  `ListArrayReader` (List + LargeList, nested lists, selective padding),
  `MapArrayReader` (a list of key/value entry structs),
  `ListViewArrayReader` (ListView + LargeListView),
  `FixedSizeListArrayReader`, the `ArrayReaderBuilder` (which wires all of them
  to real parquet schema/row groups, propagating the projection mask and the
  selective-padding threshold) and the batch-reading core
  (`ArrowReaderMetadata`, `ParquetRecordBatchReaderBuilder`,
  `ParquetRecordBatchReader`) are done, so a parquet file can be read end to end
  as `Arrow::RecordBatch`es, and row selection is wired end to end: the
  `RowSelection`/`RowSelector` types (both the RLE and — as of the latest
  commit — the bitmap backing: `MaskSelection`, `MaskRunIter`,
  `from_boolean_buffer`/`from_filters`/`as_mask` and the
  `and_then`/`intersection`/`union` algebra), the selector cursor and the
  `ReadPlan`/`ReadPlanBuilder`/`LimitedReadPlanBuilder` behind
  `with_row_selection`/`with_offset`/`with_limit` (skips are pushed down as
  `skip_records`), and row-level push-down filtering is ported too:
  `ArrowPredicate`/`ArrowPredicateFn`/`RowFilter` (`src/parquet/arrow/filter.cr`),
  `PredicateOptions` + `ReadPlanBuilder#with_predicate[_options]`
  (`src/parquet/arrow/read_plan.cr`) and
  `ParquetRecordBatchReaderBuilder#with_row_filter`, which eagerly evaluates
  each predicate over its own projection in `build` and folds the result into
  the plan's selection with `and_then`. The arrow array readers are now all
  ported: `NullArrayReader`
  (arrow-cr now has `Arrow::NullArray`), `FixedLenByteArrayReader`
  (`FixedSizeBinary`, `Decimal32/64/128/256` and `Interval(YearMonth|DayTime)`
  targets; only `Float16` needs an arrow-cr half-float array because Crystal has
  no `Float16`, and `Interval(MonthDayNano)` is NYI as upstream) and
  `ByteArrayDictionaryReader` + `DictionaryBuffer` (dictionary preservation,
  spilling and recomputation, view conversion), and the `ArrayReaderBuilder`
  routes dictionary-typed `BYTE_ARRAY`/`FIXED_LEN_BYTE_ARRAY` columns to it.
  The user-facing schema override is ported too: `ArrowReaderOptions`
  (`with_schema`/`with_skip_arrow_metadata`) +
  `ArrowReaderMetadata.load`/`try_new`/`with_supplied_schema` (field count,
  type, nullability and metadata validation with the upstream messages) +
  `ParquetRecordBatchReaderBuilder.try_new_with_options`, so a supplied or
  embedded `Dictionary(..)` field actually reaches the array reader builder and
  the upstream `arrow_reader` `test_dictionary_preservation` end-to-end test
  passes.
  **Next up:** statistics pruning (`arrow_reader/statistics.rs`:
  `StatisticsConverter` + the page-statistics helpers, the last piece of the
  filter item), then the mask cursor (`selection/cursor.rs`:
  `RowSelectionPolicy`/`MaskCursor`/`LoadedRowRanges`) and page pruning
  (`row_ranges_for_selected_pages`/`scan_ranges` in `selection/ranges.rs`).
  Everything else
  on this roadmap is complete, blocked (async/object_store), or deferred
  (`parquet-variant-compute`/`-json`, the arrow-only CLIs,
  `column/page_store.rs`, the arrow reader/writer). Done so far:
  - `parquet-variant` (`src/parquet_variant/`): `VariantPath`/
    `VariantPathElement` + dot/bracket parser (`path.cr`); `utils` helpers
    (`utils.cr`); `OffsetSizeBytes`/`map_bytes_to_offsets`, `VariantBasicType`/
    `VariantPrimitiveType`, and the primitive `decode_*` helpers (`decoder.cr`);
    `ShortString` (`short_string.cr`); `VariantMetadata`
    (`variant/metadata.cr`); `VariantDecimal4`/`8`/`16` (`variant/decimal.cr`);
    the metadata builders `MetadataBuilder`/`WritableMetadataBuilder`/
    `ReadOnlyMetadataBuilder` (`builder/metadata.cr`); the **value layer** read
    side `Variant` (`variant.cr`), `VariantObject` (`variant/object.cr`),
    `VariantList` (`variant/list.cr`), Rust-compatible `Debug`
    (`variant/debug.cr`); and the **value builder** `ValueBuilder`/`ParentState`/
    `BuilderSpecificState` (`builder/state.cr`), `VariantBuilder`
    (`builder/variant_builder.cr`), `ObjectBuilder` (`builder/object_builder.cr`),
    and `ListBuilder` (`builder/list_builder.cr`).
  - `parquet-geospatial` (`src/parquet_geospatial/`): `Hint`/`Edges`/`Metadata`
    + the metadata side of `WkbType` (`types.cr`); `Interval`/`WraparoundInterval`
    (`interval.cr`); the `GeometryBounder` (`bounding.cr`) with a minimal
    `WkbReader` + geometry model (`wkb.cr`); and the `Testing` WKB helpers
    (`testing.cr`).
  - `parquet_derive` (`src/parquet/derive.cr`): the `include
    Parquet::Derive::Record` macro (generating `T::Writer`/`T::Reader`) plus the
    `Codec(T)` type mapping, with `spec/parquet/derive_spec.cr`.
  - `encryption` (`src/parquet/encryption/ciphers.cr`): the AES-GCM block
    encryptor/decryptor (`GcmBlockEncryptor`/`GcmBlockDecryptor`/`CounterNonce`)
    with a minimal AAD-capable AES-GCM (`AesGcm`) over OpenSSL EVP, plus
    `spec/parquet/encryption/ciphers_spec.cr`; and the encryption Thrift IDL
    (`src/parquet/file/column_crypto_metadata.cr`:
    `EncryptionWithColumnKey`/`ColumnCryptoMetaData`;
    `src/parquet/file/metadata/thrift/encryption.cr`: `AesGcmV1`/`AesGcmCtrV1`/
    `EncryptionAlgorithm`/`FileCryptoMetaData`) with round-trip specs; and the
    AAD builders (`src/parquet/encryption/modules.cr`: `ModuleType`,
    `create_footer_aad`/`create_module_aad`) with `modules_spec.cr`; the
    encryption properties (`src/parquet/encryption/encrypt.cr`:
    `FileEncryptionProperties`/`EncryptionPropertiesBuilder`/`FileEncryptor` and
    the thrift-encryption helpers) with `encrypt_spec.cr`; and the decryption
    properties (`src/parquet/encryption/decrypt.cr`: `KeyRetriever`,
    `FileDecryptionProperties`/`DecryptionPropertiesBuilder[WithRetriever]`,
    `FileDecryptor`, `CryptoContext`, `read_and_decrypt`) with `decrypt_spec.cr`;
    and the `ColumnChunkMetaData` crypto wiring
    (`src/parquet/file/metadata/column_chunk.cr`: `column_crypto_metadata`/
    `encrypted_column_metadata`/`plaintext_footer_mode` + fields 8/9 read/write,
    `column_chunk_spec.cr`); the encryption-aware metadata writer
    (`src/parquet/file/metadata/writer.cr`: `MetadataObjectWriter` + wiring into
    `ThriftMetadataWriter`/`ParquetMetaDataWriter`, `writer_encryption_spec.cr`);
    and the public writer API wiring (`WriterProperties#file_encryption_properties`,
    the column writer attaching `column_crypto_metadata`, and
    `SerializedFileWriter` passing its `file_encryptor` through,
    `spec/parquet/file/writer_encryption_spec.cr`); and the reader metadata
    decryption path (`ParquetMetaDataReader`/`ParquetMetaDataPushDecoder`
    `with_file_decryption_properties`, `SerializedFileReader` read options,
    `spec/parquet/file/reader_encryption_spec.cr`); page-data encryption on
    write (`src/parquet/column/page_encryption.cr`: `PageEncryptor`,
    `CompressedPage#with_new_compressed_buffer`, `SerializedPageWriter` wiring,
    `spec/parquet/column/page_encryption_spec.cr`); and page-data decryption on
    read (`ParquetMetaData#file_decryptor`, `SerializedPageReader#add_crypto_context`
    + encrypted header/data handling, `spec/parquet/file/page_decryption_spec.cr`).
  - Phase 1.7: `util/bit_pack.cr` (`BitPack.unpack8/16/32/64`) and `decode_page`
    CRC verification (`file/serialized_reader.cr`). The thrift `crc` field is a
    signed `i32`, so `decode_page` compares it as `u32` with a wrapping
    conversion (`to_u32!`) exactly like upstream's `expected_crc as u32`; a
    checked conversion raised `OverflowError` on real files whose checksum has
    the high bit set (`hadoop_lz4_compressed_larger.parquet`).
  - Core geospatial (`src/parquet/geospatial/`): `BoundingBox`
    (`bounding_box.cr`), `GeospatialStatistics` (`statistics.cr`), and the
    accumulator layer (`accumulator.cr`: `GeoStatsAccumulator`/
    `GeoStatsAccumulatorFactory`, `DefaultGeoStatsAccumulatorFactory`,
    `VoidGeoStatsAccumulator`, `ParquetGeoStatsAccumulator`,
    `try_new_geo_stats_accumulator`, `init_geo_stats_accumulator_factory`), plus
    the `GeospatialStatistics`/`BoundingBox` thrift structs + conversion helpers
    and field 17 wiring in `ColumnChunkMetaData`/`build_column_metadata`
    (`column/writer/{encoder,writer}.cr`) and `file/writer.cr`; specs:
    `spec/parquet/geospatial/{bounding_box,accumulator,geo_statistics}_spec.cr`.
  - Content-defined chunker (Phase 2.2): `src/parquet/column/chunker.cr`
    (`CdcChunk`) and `src/parquet/column/chunker/{cdc,cdc_generated}.cr`
    (`ContentDefinedChunker`: `calculate_mask`, `roll`/`roll_fixed`/`roll_level`,
    `need_new_chunk`, `calculate` for the flat/nullable/nested paths,
    `validate_chunks`, plus the verbatim 8x256 `GEARHASH_TABLE`); spec
    `spec/parquet/column/chunker_cdc_spec.cr`.
  - SNAPPY codec (Phase 1.8a): `SnappyCodec` wraps the `naqvis/snappy` shard
    (raw/block format) and is wired into `create_codec`
    (`src/parquet/compression_codecs.cr`, `shard.yml` dependency);
    specs `spec/parquet/compression_codecs_spec.cr` (roundtrips, golden
    wire-format streams, empty input, size mismatch, corrupt input, repetitive
    data), plus the newly enabled real-fixture tests
    `test_file_reader_rows_nulls` (`nulls.snappy.parquet`) and
    `test_file_reader_datapage_v2` (`datapage_v2.snappy.parquet`, compressed
    data page v2) and the SNAPPY writer/page roundtrips that previously used
    GZIP.
  - LZ4 codecs (Phase 1.8b): `LZ4RawCodec` (raw/block format via the
    `paddor/flint` shard, `Flint::BlockCodec#compress_raw`/`#decompress_raw_into`),
    `LZ4HadoopCodec` (8-byte big-endian `(uncompressed_size,
    compressed_size)` prefix + raw block, with the upstream decode fallback
    chain), and `LZ4Codec`; the frame container lives in
    `src/parquet/lz4.cr` (magic/descriptor/blocks/skippable frames + XXH32
    header and content checksums). All three are wired into `create_codec`
    (`LZ4` → Hadoop codec honouring `CodecOptions#backward_compatible_lz4`,
    `LZ4_RAW` → raw codec). `XxHash32` was added to
    `src/parquet/util/xxhash.cr`. Specs:
    `spec/parquet/compression_codecs_spec.cr` (upstream `test_codec_lz4`/
    `test_codec_lz4_raw`, Hadoop prefix, golden block vectors, empty input,
    corrupt input/size mismatch, raw + frame fallbacks, frame roundtrips) and
    `spec/parquet/file/lz4_fixture_spec.cr` (all five `lz4`/`hadoop_lz4`
    fixtures, incl. the `non_hadoop` raw-block fallback and strict-mode
    rejection). Writer/page roundtrips now cover `LZ4` and `LZ4_RAW`.
  - ZSTD and BROTLI codecs (Phase 1.8c): `ZSTDCodec` wraps the `paddor/zinc`
    shard (`~> 0.2.1`, pure Crystal RFC 8878 frames; `Zinc::FrameCodec`) and
    `BrotliCodec` wraps the `OrelSokolov/brotli` shard (`~> 0.3.0`, pure Crystal
    RFC 7932; one-shot quality 0..11, `lgwin` 22). Both are wired into
    `create_codec`; LZO now raises `NYIError` like upstream
    (`nyi_err!("The codec type {} is not supported yet")`). Specs:
    `spec/parquet/compression_codecs_spec.cr` (upstream `test_codec_brotli` and
    `test_codec_zstd` level sweeps, reference-encoder golden streams, corrupt
    input, empty input, level clamping, LZO) and
    `spec/parquet/file/zstd_brotli_fixture_spec.cr` (upstream
    `test_read_float32_float64_byte_stream_split` and `test_alp_extended`, plus
    the `large_string_map.brotli.parquet` metadata/value-column case). Writer and
    page roundtrips now cover `ZSTD` and `BROTLI`.
  - Offset-index page reader (item 10): `SerializedPageReader` now carries an
    upstream-style state (`SerializedPageReader::State` with `Values` and
    `Pages` subclasses). `Pages` consumes the column chunk's `PageLocation`
    queue, synthesizes the dictionary page from the un-indexed chunk prefix,
    derives `PageMetadata#num_rows` from the `first_row_index` deltas, skips
    locations in `skip_next_page`, always reports a record boundary, and
    `new_for_column` accepts `page_locations` (previously `NYIError`) — wired
    through `ReadOptionsBuilder#with_page_index`.
    `peek_next_page_offset` (upstream test-only) is public here, and
    `SerializedPageReader` now implements `Iterator(Column::Page)`. Specs:
    `spec/parquet/file/serialized_reader_pages_spec.cr` (page-for-page
    equivalence with the offset-stream state on `alltypes_tiny_pages.parquet`,
    dictionary-page handling, row counts, offset peeking in both states, skips,
    record boundaries, the iterator, and an end-to-end row-iterator comparison).
  - Arrow interop, schema (Arrow → Parquet): `src/parquet/arrow/schema.cr`
    (`Parquet::Arrow::ArrowSchemaConverter`, `arrow_to_parquet_type`,
    `field_id`, `decimal_length_from_precision`, the
    `PARQUET_*_NAME`/`PARQUET_FIELD_ID_META_KEY`/`ARROW_SCHEMA_META_KEY`
    constants). Covers every Arrow type upstream converts (nulls, integers with
    signedness, Float16, timestamps/time/date/duration/interval, decimals
    incl. the `Decimal32/64/128/256` precision split, binary/string/view,
    nested lists incl. fixed-size/list-view, structs, maps, dictionaries, and
    run-end-encoded values), plus `with_coerce_types` list/map naming. Specs:
    `spec/parquet/arrow/schema_spec.cr` (upstream `test_field_to_column_desc`,
    `test_coerced_map_list`, `test_empty_struct_field` + the doc example,
    field ids, non-logical-type physical mappings, date64 coercion, root name,
    decimal byte lengths). Schema metadata: `encode_arrow_schema`
    (legacy `0xFF*4 + u32le(len)` prefix + base64 of the arrow-cr IPC schema
    `Message`), `get_arrow_schema_from_metadata` (handles both the framed and
    raw-flatbuffer shapes), and `add_encoded_arrow_schema_to_metadata` (adds or
    replaces `ARROW:schema` in `WriterProperties#key_value_metadata`, flattening
    run-end-encoded fields). The encoded metadata written by this port is
    accepted by upstream `parquet-read` (arrow-rs 60.0.0), and the port decodes
    it back to the original schema (incl. schema/field metadata, dictionaries,
    decimals, timestamps, nested lists). Projection/primitive layer:
    `Parquet::Arrow::ProjectionMask` (`all`/`none`/`leaves`/`roots`/`columns`/
    `leaf_included?`/`union`/`intersect`), `Parquet::Arrow.parquet_column`, and
    `Parquet::Arrow.convert_primitive` (parquet primitive → arrow `DataType`
    with the embedded-schema hint logic: timestamp/time/date64/duration, offset
    and view width, interval unit, decimal width, dictionary preservation), in
    `src/parquet/arrow/mod.cr` and `arrow/schema/primitive.cr`. Specs:
    `spec/parquet/arrow/projection_spec.cr` (upstream
    `test_mask_from_column_names`/`test_projection_mask_union`/
    `test_projection_mask_intersect` + roots/none/leaf checks and
    `parquet_column`) and `spec/parquet/arrow/primitive_spec.cr` (every
    `from_parquet` branch, the upstream FLBA decimal/interval length tests, and
    `apply_hint`). Parquet → Arrow schema:
    `ParquetField`/`ParquetFieldType`/`VisitorContext`/`Visitor`
    (`src/parquet/arrow/schema/complex.cr`: `visit_primitive`/`visit_struct`/
    `visit_list`/`visit_map`/`dispatch`, including the backward-compatibility
    rules for legacy list/map encodings), `convert_field`/`convert_schema`/
    `convert_type`, and `parquet_to_arrow_schema[_by_columns]`/
    `parquet_to_arrow_schema_and_fields`/`parquet_to_arrow_field_levels`/
    `parquet_to_arrow_field`/`FieldLevels`/`parse_key_value_metadata` (preferring
    the decoded `ARROW:schema` hint when present). Specs:
    `spec/parquet/arrow/parquet_to_arrow_spec.cr` (upstream
    `test_flat_primitives`, `test_decimal_fields`, `test_byte_array_fields`,
    `test_duplicate_fields`, `test_parquet_lists`, `test_parquet_maps`,
    `test_nested_schema`, `test_repeated_nested_schema`,
    `test_column_desc_to_field`, `test_metadata`, the `convert_schema` roundtrip
    invariant, and a compact arrow→parquet→arrow roundtrip covering
    field ids/metadata). Arrow reader foundations:
    `Parquet::Arrow::Buffer` (`src/parquet/arrow/buffer/bit_util.cr`:
    `count_set_bits`, `iter_set_bits_rev`, `sign_extend_be`) and
    `Parquet::Arrow::RecordReader::ValuesBuffer`/`ArrayValuesBuffer`
    (`src/parquet/arrow/record_reader/buffer.cr`: `reserve_exact`, `pad_nulls`),
    with `spec/parquet/arrow/buffer_spec.cr` porting upstream `test_bit_fns` and
    covering `sign_extend_be` and the padding cases (valid positions only, null
    slots hold stale values — as upstream). Definition levels
    (`src/parquet/arrow/record_reader/definition_levels.cr`):
    `DefinitionLevelBuffer` (full/mask modes: `consume_levels`,
    `consume_bitmask`, `levels`, `nulls`), `build_filtered_validity_bitmap`,
    `DefinitionLevelBufferDecoder` (packed fast path + fallback), and
    `PackedDecoder` (RLE/bit-packed runs straight into a bitmask:
    `set_data`/`read`/`skip`/`try_consume_all_valid`). Specs:
    `spec/parquet/arrow/definition_levels_spec.cr` (upstream
    `test_build_validity_bitmap_unfiltered_word_chunk`, `test_packed_decoder`,
    `test_packed_decoder_skip`, `test_try_consume_all_valid` + filtered-bitmap
    and both buffer-mode cases). Array-reader interfaces
    (`src/parquet/arrow/array_reader.cr`): `ArrayReader` (`data_type`,
    `read_records`/`consume_batch`/`skip_records`, `def_levels`/`rep_levels`,
    `max_def_level`, `next_batch`), `RowGroups` + `FileRowGroups` (over
    `FileReader`/`FilePageIterator`), `EmptyArrayReader` and
    `make_empty_array_reader`. Specs:
    `spec/parquet/arrow/array_reader_spec.cr` (empty-array row counting,
    `next_batch`, and the `RowGroups` file adapter). Column-reader pluggable
    decoders (`src/parquet/column/reader.cr`):
    `ColumnReaderImpl.new_with_decoders` (upstream `new_with_decoders`) takes
    the values decoder explicitly and swaps the definition-level decoder for an
    arrow `DefinitionLevelBufferDecoder` (fed from both v1 and v2 data pages), and
    `read_records_with_reservation` invokes a reservation callback with the
    decoded value/level counts before reading values (`read_records` delegates
    with a no-op callback). Specs:
    `spec/parquet/column/arrow_column_reader_spec.cr` (arrow level buffer
    decoding + reservation counts, required columns, and the missing-decoder
    invariant). Record reader (`src/parquet/arrow/record_reader.cr`):
    `RecordReader::GenericRecordReader(V, CV)` — parameterized by the values
    buffer and the value decoder, like upstream's
    `GenericRecordReader<V, CV>` — (`set_page_reader`, `read_records`,
    `skip_records`, `num_records`/`num_values`, `consume_record_data`/
    `consume_def_levels`/`consume_rep_levels`/`consume_bitmap`/
    `consume_compact_bitmap`, `values_written`, `reset`, `set_padding_threshold`,
    `max_def_level`, and the private `read_one_batch` with full/selective null
    padding), plus the `typed_record_reader` helper for the classic
    `(ArrayValuesBuffer(T), ColumnValueDecoderImpl(T))` pairing (upstream's
    `RecordReader<T>`). Specs: `spec/parquet/arrow/record_reader_spec.cr` (upstream
    `test_read_required_records`, `test_capacity_hint_preserved_across_fragmented_reads`,
    `test_read_optional_records`, `test_read_repeated_records`,
    `test_read_more_than_one_batch`, `test_row_group_boundary`,
    `test_skip_required_records`, and both `test_selective_padding_*`), backed by
    the ported `util/test_common/page_util` builders in
    `spec/support/page_util.cr` (`DataPageBuilderImpl`, `InMemoryPageReader`,
    `InMemoryPageIterator`). Primitive array reader
    (`src/parquet/arrow/array_reader/primitive_array.cr`):
    `PrimitiveArrayReader(T)` for INT32/INT64/FLOAT/DOUBLE (`data_type`,
    `read_records`/`skip_records` driven by the page iterator,
    `consume_batch`, `def_levels`/`rep_levels`, `max_def_level`,
    `padding_threshold`) plus `read_records_from_pages`/`skip_records_from_pages`
    (upstream `array_reader::{read_records, skip_records}`) and
    `null_buffer_from` (upstream `NullBuffer::from_unsliced_buffer`). Specs:
    `spec/parquet/arrow/primitive_array_reader_spec.cr` (optional/required
    columns across pages, empty page iterators, definition/repetition levels,
    skipping, and BOOLEAN values with nulls). Byte-array values buffer
    (`src/parquet/arrow/buffer/offset_buffer.cr`): `OffsetBuffer(I)` with
    `with_capacity`/`len`/`empty?`/`try_push` (UTF-8 codepoint-start check),
    `extend_from_dictionary`, `check_valid_utf8`, `into_array`
    (string/binary arrays) and the `ValuesBuffer` `pad_nulls` (leading/trailing
    nulls, corrupt-input errors). Specs:
    `spec/parquet/arrow/offset_buffer_spec.cr` ports all seven upstream
    `offset_buffer.rs` tests. Arrow decoders (`src/parquet/arrow/decoders.cr`):
    `Decoders::DictIndexDecoder` (`RLE_DICTIONARY` indices, chunked decoding and
    skipping, bit-width validation) and `Decoders::DeltaByteArrayDecoder`
    (`DELTA_BYTE_ARRAY`: prefix/suffix length decoding via
    `DeltaBitPackDecoder`, incremental values, skip, EOF reporting). Specs:
    `spec/parquet/arrow/decoders_spec.cr` (both upstream error tests + chunked
    decode/skip round-trips). Byte-view values buffer
    (`src/parquet/arrow/buffer/view_buffer.cr`): `ViewBuffer` (`append_block`,
    `into_array` for `Utf8View`/`BinaryView`, and `pad_nulls` over the view
    slots). Specs: `spec/parquet/arrow/view_buffer_spec.cr` ports all three
    upstream `view_buffer.rs` tests. Variable-length leaf readers
    (`src/parquet/arrow/array_reader/{byte_array,byte_view_array}.cr`):
    `Parquet::Arrow.make_byte_array_reader`/`ByteArrayReader(I)` for
    Utf8/LargeUtf8/Binary/LargeBinary and
    `Parquet::Arrow.make_byte_view_array_reader`/`ByteViewArrayReader` for
    Utf8View/BinaryView, both driven by the page iterator over
    `GenericRecordReader` (plain, dictionary and delta encodings, null padding,
    `def_levels`/`rep_levels`, skipping). Specs:
    `spec/parquet/arrow/byte_array_reader_spec.cr` and
    `spec/parquet/arrow/byte_view_array_reader_spec.cr`. Struct reader
    (`src/parquet/arrow/array_reader/struct_array.cr`): `StructArrayReader`
    (child readers kept in sync on read/skip with the upstream out-of-sync
    errors, `data_type`, `consume_batch` building the `StructArray` with a null
    bitmap from the first child's levels via
    `build_filtered_validity_bitmap`, `def_levels`/`rep_levels` forwarded from
    the first child, `max_def_level`, selective padding). Specs:
    `spec/parquet/arrow/struct_array_reader_spec.cr` ports the upstream
    `test_struct_array_reader` (`[true, false, false, false, false]` nulls plus
    the level buffers) and `test_struct_array_reader_list` (a struct of lists,
    list values `[[1, 2, null], [], null, null]` with struct validity
    `0b0000_0111`), and adds coverage for the struct def level, a non-nullable
    struct, an empty reader, skipping, both out-of-sync errors, and selective
    padding (parent-level entries dropped). List reader
    (`src/parquet/arrow/array_reader/list_array.cr`): `ListArrayReader(O)` for
    List (Int32) and LargeList (Int64) — offsets and validity rebuilt from the
    item reader's definition/repetition levels with the upstream
    `d >= def_level`/`r <= rep_level` scan, list-level null/empty handling,
    `parent_threshold` for nested lists, the "first repetition level of batch
    must be 0" and "Failed to reconstruct list from level data" errors, and
    level/data-type/max-def forwarding. Specs:
    `spec/parquet/arrow/list_array_reader_spec.cr` ports the upstream
    `test_nullable_list`/`test_required_list`/`test_nested_list` for both
    offset types (`test_list_array_reader`/`test_large_list_array_reader`)
    plus level forwarding, skipping, empty batches and both error paths. Map
    reader (`src/parquet/arrow/array_reader/map_array.cr`): `MapArrayReader`
    (struct def level from the key reader's max def level, struct rep level
    `rep_level + 1`, non-nullable entry struct, `List(element)` feeding the list
    reader, `consume_batch` reinterpreting the list data as a `MapArray`, and
    the "expected map type"/"expected struct with two fields"/"map struct
    cannot be nullable" errors). Specs:
    `spec/parquet/arrow/map_array_reader_spec.cr` covers the upstream map data
    shape (two null maps plus `{three..seven}` string→int32, checking
    `MapArray#value` keys/values), the `keys`/`values`/`value_length` helpers,
    level forwarding, skipping and both type errors. The upstream
    `read_map_array_column` test itself round-trips through `ArrowWriter` +
    `ParquetRecordBatchReader`, neither of which is ported yet. List-view
    reader (`src/parquet/arrow/array_reader/list_view_array.cr`):
    `ListViewArrayReader(O)` wraps a `ListArrayReader` over the corresponding
    List/LargeList type and converts the result to a ListView/LargeListView
    (offsets reused, one offset+size per value, sizes as consecutive offset
    differences, empty batches via `new_null(0)`), with the "expected list view
    type" and converted-type checks. Specs:
    `spec/parquet/arrow/list_view_array_reader_spec.cr` ports the upstream
    `test_nullable_list_view`/`test_required_list_view` for both offset types
    (upstream `test_list_view_array_reader`/`test_large_list_view_array_reader`)
    plus data-type/level forwarding, empty batches, skipping, and the
    non-list-view error. Fixed-size list reader
    (`src/parquet/arrow/array_reader/fixed_size_list_array.cr`):
    `FixedSizeListArrayReader` — a single pass collects the def level at each
    list boundary, validates that every non-null row has exactly `fixed_size`
    items (upstream "Encountered misaligned row with length N (expected length
    M)" and "Encountered repetition level too large for definition level"
    errors) and that the child count matches, reuses the compact child when no
    list is null, otherwise inserts `fixed_size` null entries per null list with
    `MutableArrayData#try_extend`/`try_extend_nulls`, and derives the list null
    bitmap from the boundary def levels at `def_level - 1`. Specs:
    `spec/parquet/arrow/fixed_size_list_array_reader_spec.cr` ports the upstream
    `test_nullable_list`, `test_required_list`, `test_misaligned_rows_error`,
    `test_empty_list` (zero-size lists) and `test_nested_list` (nested
    fixed-size lists over two batches), and adds data-type/level forwarding, an
    empty batch and skipping. Not ported yet: `test_nested_var_list` (built with
    `FixedSizeListBuilder` over a `ListBuilder`) and the two `ArrowWriter`
    round-trip tests. Array reader builder
    (`src/parquet/arrow/array_reader/builder.cr`): `ArrayReaderBuilder` —
    recursive `build_reader` dispatch over the `ParquetField` tree (primitive,
    struct, map, list/list-view, fixed-size list), `build_array_reader` with
    the `make_empty_array_reader(num_rows)` fallback when the field is absent or
    fully unprojected, `with_batch_size`, and selective-padding propagation
    (children of lists/maps are padded at the parent's `def_level`). Specs:
    `spec/parquet/arrow/array_reader_builder_spec.cr` ports the upstream
    `test_create_array_reader` (projected `nested.snappy`-style `b_struct`
    leaf, plus reading the 8 all-null rows), and adds the unprojected/absent
    field fallbacks, list and map fixtures (`list_columns.parquet`,
    `nested_maps.snappy.parquet`), the batch-size hint, and the fixed-length
    byte array path (`fixed_length_decimal.parquet`). Not ported: reader
    metrics, cache options/`CachedArrayReader`, virtual columns (so upstream's
    `test_create_array_reader_with_row_numbers` is skipped), and the INT96 leaf
    path (which raises `NYIError`); dictionary-typed byte array columns route to
    `make_byte_array_dictionary_reader` for both BYTE_ARRAY and
    FIXED_LEN_BYTE_ARRAY, but the user-facing way to request a dictionary arrow
    type (`with_schema`) is not ported yet.
    Batch reader (`src/parquet/arrow/arrow_reader.cr`): `ParquetRecordBatchReader`
    wraps the `ArrayReader` + projected schema and yields batches via
    `RecordBatch.from(struct_array)` (so an empty projection still carries the
    row count), splitting by `batch_size` (capped at the file row count) and
    returning `nil` for empty batches. `ArrowReaderOptions` (skip-embedded-
    metadata + supplied arrow schema) feeds `ArrowReaderMetadata.load`/
    `try_new` (`with_supplied_schema` validates the field count, data types,
    nullability and metadata against the hinted parquet conversion), and
    `ParquetRecordBatchReaderBuilder.try_new_with_options` is the entry point,
    so a supplied `Dictionary(..)` field (or an `ARROW:schema` one) reaches the
    array reader builder and the dictionary encoding is preserved end to end.
    Specs: `spec/parquet/arrow/arrow_reader_spec.cr` covers an all-null struct
    column (`nulls.snappy.parquet`), batch splitting, a 10-leaf projection over
    `alltypes_plain.parquet` (INT32/BOOLEAN/INT64/FLOAT/DOUBLE/BYTE_ARRAY values
    checked), list columns (`list_columns.parquet`), nested lists
    (`nested_lists.snappy.parquet`), the `try_new` convenience constructor, the
    empty projection, `ArrowReaderMetadata.load`/`try_new`, the upstream
    `test_dictionary_preservation` end-to-end dictionary case (six batches, row
    counts/null counts, dictionary identity inside a row group and recomputation
    across row groups), `test_null_schema_inference` (skipped arrow metadata on
    `null_list.parquet`), the supplied-schema dictionary read (top level and
    inside a struct, matching upstream `test_with_schema`'s dictionary
    assertions) and the supplied-schema validation errors (`test_schema_too_few_
    columns`, `test_schema_too_many_columns`, `test_schema_mismatched_column_
    names`, `test_schema_incompatible_columns`, `test_schema_error_bad_types`,
    `test_schema_error_bad_nullability` with the upstream messages). Row selection
    (`src/parquet/arrow/row_selection.cr`): `RowSelector` (`select`/`skip`,
    `row_count`, `skip?`), the `RowSelectionInner` backing enum
    (`Selectors`/`Mask`) and `RowSelection` over either backing (normalizing
    construction that drops empty and combines adjacent selectors,
    `from_consecutive_ranges`, `from_boolean_buffer`/`from_filters`/`as_mask`,
    `concat` (upstream `FromIterator<RowSelection>`),
    `each`/`row_count`/`skipped_row_count`/`total_row_count`/`selects_any?`,
    `offset`/`limit`/`trim`/`split_off`, `and_then`/`intersection`/`union`,
    mixed-backing equality and `dup`), plus the `RowSelectorTransforms`
    helpers (`split_off_selectors`/`offset_selectors`/`limit_selectors`), the
    bitmap layer (`MaskSelection` with its RLE/popcount caches, `MaskRunIter`,
    and `SelectionMasks`: `to_selectors`, `has_at_least_runs?`, `split_off`,
    `trim`, `offset`, `limit`, `from_selectors`), and the algebra
    (`RowSelectionAlgebra` in `src/parquet/arrow/row_selection/algebra.cr`:
    `and_then` over selectors/selectors, selectors/mask and mask/anything,
    `intersect_row_selections`/`union_row_selections`, and the bitwise
    `intersect_masks`/`union_masks` with the longer side's tail passed
    through). Specs: `spec/parquet/arrow/row_selection_spec.cr` ports all
    upstream `selection/selector.rs` tests (normalization, combining, ranges,
    `offset`, `limit`, `split_off`, `trim`), every `selection/boolean.rs`
    test (mask runs, cache-drop on clone, cache reuse, set-algebra agreement,
    `from_boolean_buffer`/`from_filters` equivalence with fuzz loops,
    offset/limit/split/trim edge cases, cached-count propagation,
    `boolean_mask_from_selectors` and `mask_has_at_least_runs`), every
    `selection/algebra.rs` test (`test_and`/`test_and_fuzz`, the
    longer/shorter errors, `test_intersection`/`test_union` over selectors,
    mask-backing preservation, mixed-backing intersections/unions, uneven
    tails, offset/length mask kernels and the mask fuzz loop) and the
    mixed-backing `selection/mod.rs` tests (`from_filters`, equality
    mismatches, concatenation into a mask/selectors/empty). Read plan
    (`src/parquet/arrow/read_plan.cr`):
    `ReadPlanBuilder` (`new`, `with_selection`, `selection`, `selects_any?`,
    `num_rows_selected`, `limited`), `LimitedReadPlanBuilder` (`with_offset`,
    `with_limit`, `build_limited` — clamping the limit to the row count,
    emptying the plan when the offset covers every row, applying offset/limit
    to an existing selection) and `ReadPlan` (`batch_size`, `cursor`) with the
    `RowSelectionCursor` (`AllCursor` / `SelectorsCursor` with
    `next_selector`/`return_selector`/`position`). The reader consumes the
    cursor in `next_batch`: skip runs call `ArrayReader#skip_records` (errors
    when fewer rows were skipped), select runs are read up to the batch size
    and returned to the cursor when partially consumed. Specs:
    `spec/parquet/arrow/read_plan_spec.cr` (builder/cursor/limits) and the row
    plan group of `spec/parquet/arrow/arrow_reader_spec.cr` (offset+limit,
    row selection, selected batches split by `batch_size`, selection combined
    with offset/limit, and empty plans). Null reader
    (`src/parquet/arrow/array_reader/null_array.cr`): `NullArrayReader` reads a
    `DataType::Null` column (only level data is used; the bitmap is consumed and
    discarded) as an `Arrow::NullArray`, and the `ArrayReaderBuilder` now builds
    it instead of raising `NYIError`. Specs:
    `spec/parquet/arrow/null_array_reader_spec.cr` covers an all-null column,
    empty batches, batch splitting, and the builder wiring over
    `nulls.snappy.parquet`'s `b_c_int` leaf with a Null arrow field.
    Fixed-length byte array reader
    (`src/parquet/arrow/array_reader/fixed_len_byte_array.cr`):
    `make_fixed_len_byte_array_reader` (upstream byte-length validation for each
    target, `NYIError` for `Float16`) and `FixedLenByteArrayReader` — the values
    are decoded through the shared record reader and converted at
    `consume_batch` time to `FixedSizeBinary`, `Decimal32/64/128/256`
    (big-endian, sign-extended via `sign_extend_be`, as `PrimitiveArray(T)` with
    the decimal data type) or `Interval(YearMonth|DayTime)` (little-endian
    components; `MonthDayNano` NYI). Specs:
    `spec/parquet/arrow/fixed_len_byte_array_reader_spec.cr` covers decimals of
    every width, fixed-size binary, both interval units, null padding, and the
    type/byte-length/Float16 errors; the builder spec now reads
    `fixed_length_decimal.parquet` (Decimal128(25, 2)) instead of expecting
    `NYIError`.
    Dictionary-encoded byte array reader
    (`src/parquet/arrow/array_reader/byte_array_dictionary.cr` +
    `src/parquet/arrow/buffer/dictionary_buffer.cr`):
    `make_byte_array_dictionary_reader` (the upstream `make_reader!` macro's
    `(key type, value type)` dispatch over `UInt8`…`Int64` keys ×
    Binary/Utf8/LargeBinary/LargeUtf8/FixedSizeBinary/view values, plus the
    "invalid non-dictionary data type"/"unsupported data type" errors),
    `ByteArrayDictionaryReader(K, V)` (`try_new` with the view `buffer_type`
    mapping, `read_records`/`skip_records`, `consume_batch` with the
    `new_empty_array` path once consumed, `def_levels`/`rep_levels`/
    `max_def_level`), `convert_values_to_view` (Utf8View/BinaryView), and
    `DictionaryDecoder(K, V)` (`set_dict` with the encoding/index-width
    validation, `set_data` splitting dictionary indices from the fallback
    decoder, `read`/`skip_values`). `DictionaryBuffer(K, V)` is the
    `ValuesBuffer` that is either dictionary encoded (`Kind::Dict`) or flat
    (`Kind::Values`): `as_keys`/`spill_values` switch the representation in
    place, `pad_nulls` pads keys or offsets, `into_array` builds the
    dictionary array (key validation, `FixedSizeBinary` normalisation) or packs
    a fresh dictionary from flat values via `pack_values_from_offsets[_impl]`
    with FNV-1a hashing and collision overflow handling. Specs:
    `spec/parquet/arrow/byte_array_dictionary_reader_spec.cr` ports all eight
    upstream `byte_array_dictionary.rs` tests (dictionary preservation and
    skips, fallback/fallback-skip, too-large dictionary, all-null dictionaries,
    empty index page, invalid bit width) and adds reader-level coverage
    (single-page preservation, recomputed dictionary for plain pages, spilling
    when a batch spans column chunks, skipping, Utf8View conversion, and the
    non-dictionary data type error);
    `spec/parquet/arrow/dictionary_buffer_spec.cr` ports all six upstream
    `dictionary_buffer.rs` tests (preserve/spill/rebuild, key validation, large
    utf8 and binary value types, the Int8 key overflow boundary and hash
    collision deduplication).
    Divergences: `MaybeDictionaryDecoder` is an abstract class with two
    subclasses (the variant never changes after construction); the fallback
    (non-dictionary) path decodes through the shared
    `ColumnValueDecoderImpl(ByteArray)` into an `Array(ByteArray)` scratch and
    pushes into the record reader's `OffsetBuffer` (no dedicated
    `ByteArrayDecoder`/`ByteArrayDecoderPlain`), with an empty page payload
    short-circuited to zero values and a truncated payload still raising; the
    Rust payload enum's `Arc` identity check is `Reference#same?`; upstream's
    `ahash` dictionary hashing is a deterministic FNV-1a (only equality of equal
    byte slices matters); the `hash_scratch: MutableBuffer` parameter is
    dropped; `into_array` takes the pre-built `NullBuffer` and builds arrow-cr
    arrays positionally; `convert_values_to_view` takes the key type explicitly
    and rebuilds the data type (arrow-cr `DictionaryArray#with_values` keeps the
    stale type).
  - Both crates are required from `src/parquet-cr.cr` (`./parquet_variant`,
    `./parquet_geospatial`) and are linted/tested by `make lint`/`make test`.
- Next up (take the first unchecked item; each is ~one commit):
  1. ~~Phase 3.1 `parquet-variant` value layer.~~ **complete** —
     `src/parquet_variant/variant.cr`, `variant/object.cr`, `variant/list.cr`,
     `variant/debug.cr`, `builder/{state,variant_builder,object_builder,list_builder}.cr`
     + specs. All `parquet-variant` core tests are ported (including the exact
     `Debug`/`HexString`/`InvalidVariant` formatting test).
  2. ~~Phase 3.2 `parquet-geospatial` `bounding.rs` (`GeometryBounder`).~~
     **complete** — ported a minimal WKB reader + geometry model
     (`src/parquet_geospatial/wkb.cr`), the `GeometryBounder`
     (`src/parquet_geospatial/bounding.cr`), and the `Testing` WKB helpers
     (`src/parquet_geospatial/testing.cr`) + all upstream `bounding.rs` tests.
  3. ~~Phase 3.3 `parquet_derive` + `parquet_derive_test`.~~ **complete** —
     `src/parquet/derive.cr` (`Parquet::Derive::Record` macro + `Codec(T)`) and
     `spec/parquet/derive_spec.cr` (schema generation, read/write round-trip,
     optional-but-valid, pruned/shuffled).
  4. ~~Phase 3.4 encryption.~~ **complete (3.4a–3.4l)** — cipher layer, Thrift
     IDL, AAD builders, properties, `ColumnChunkMetaData` crypto wiring,
     encrypted-footer reader, encryption-aware metadata writer, public writer
     API, reader metadata decryption, page-data encryption on write, and
     page-data decryption on read via `CryptoContext`. `AES_GCM_CTR_V1` raises
     `NYIError`, matching upstream 60.0.0 (not a porting gap).
  5. ~~Phase 3.7 core geospatial module (`parquet/src/geospatial/`).~~
     **complete** — `bounding_box.cr` (`BoundingBox` x/y[/z/m]),
     `statistics.cr` (`GeospatialStatistics`), and `accumulator.cr`
     (`GeoStatsAccumulator`/`GeoStatsAccumulatorFactory`, the module-level global
     factory + `init_geo_stats_accumulator_factory`/`try_new_geo_stats_accumulator`,
     `VoidGeoStatsAccumulator`, `ParquetGeoStatsAccumulator` built on
     `ParquetGeospatial::GeometryBounder`). Wired through
     `ColumnChunkMetaData#geo_statistics` (+ builder `set_geo_statistics` and
     thrift field 17), `ColumnValueEncoderImpl#flush_geospatial_statistics`,
     `GenericColumnWriter#build_column_metadata`, and
     `SerializedFileWriter#finish_appended_column`. Geometry/Geography columns now
     skip min/max statistics (undefined sort order), matching upstream. Tests:
     `spec/parquet/geospatial/{bounding_box,accumulator,geo_statistics}_spec.cr`.
  6. ~~Phase 2.2 content-defined chunker (`column/chunker/cdc.rs` +
     `cdc_generated.rs`).~~ **complete** — `src/parquet/column/chunker.cr`
     (`CdcChunk`) + `chunker/cdc_generated.cr` (verbatim 8x256 `GEARHASH_TABLE`,
     `NUM_GEARHASH_TABLES`) + `chunker/cdc.cr` (`ContentDefinedChunker#initialize`
     (upstream `Result` → raises), `.calculate_mask`, `#roll`/`#roll_fixed`/
     `#roll_level`, `#need_new_chunk`, `#calculate` for the flat/nullable/nested
     paths, `#validate_chunks` under the non-release build). Tests:
     `spec/parquet/column/chunker_cdc_spec.cr` — all seven upstream unit tests
     (`test_calculate_mask_*`, `test_non_nested_non_null_single_chunk`,
     `test_max_chunk_size_forces_boundary`, `test_deterministic_chunks`,
     `test_nullable_non_nested`) plus a nested-path characterization (top-level
     boundaries + leaf-vs-value offsets). Cross-checked against an independent
     transliteration of the Rust algorithm (identical chunk boundaries for the
     flat/nullable/nested cases). `get_arrow_chunks` and the `arrow_tests`
     roundtrip module are deferred with the arrow layer; the chunker is only
     consumed by the arrow writer upstream, so no writer wiring was needed.
  7. ~~Phase 1.8a SNAPPY codec.~~ **complete** — `SnappyCodec` delegates to the
     `naqvis/snappy` shard (raw/block format; `~> 0.2.0`, MIT, pure Crystal) and
     is wired into `create_codec`; `spec/parquet/compression_codecs_spec.cr`
     ports `test_codec_snappy` and adds golden wire-format vectors, empty input,
     a size-mismatch check (upstream `snap` requires the destination length to
     match the preamble), corrupt-input handling, and a repetitive-data test. The
     erstwhile GZIP substitutes are back to upstream's SNAPPY in
     `spec/parquet/file/{writer,serialized_page}_spec.cr`, and the real fixtures
     now run: `test_file_reader_rows_nulls` (`nulls.snappy.parquet`) and
     `test_file_reader_datapage_v2` (`datapage_v2.snappy.parquet`). Shard output
     was cross-checked with an independent format-spec decoder (incl. empty,
     60/61/256/61 KiB/16 MiB+ literals) and decodes the Google-snappy fixtures.
  8. ~~Phase 1.8b LZ4 behind `create_codec`.~~ **complete** — `LZ4_RAW`
     (`LZ4RawCodec`) uses the `paddor/flint` shard (`~> 0.1.1`, MIT, pure
     Crystal block codec; `compress_raw`/`decompress_raw_into`), and `LZ4`
     (`LZ4HadoopCodec`) writes the 8-byte big-endian `(uncompressed_size,
     compressed_size)` prefix plus a raw block. Decoding honours
     `CodecOptions#backward_compatible_lz4` with upstream's exact fallback chain:
     Hadoop framing → LZ4 *frame* format (`LZ4Codec`, in-tree container) → raw
     block. `LZ4Codec` (upstream `experimental`) is always available since
     Crystal has no feature flags. Upstream's `test_codec_lz4` and
     `test_codec_lz4_raw` are ported, and all five fixtures now read
     (`lz4_raw_compressed{,_larger}`, `hadoop_lz4_compressed{,_larger}`,
     `non_hadoop_lz4_compressed`). Anchors:
     `vendor/arrow-rs/parquet/src/compression.rs` (`lz4_raw_codec`,
     `lz4_hadoop_codec`, `LZ4_BUFFER_SIZE`/`PREFIX_LEN`/`SIZE_U32`),
     `src/parquet/compression_codecs.cr`, `src/parquet/lz4.cr`,
     `src/parquet/compression.cr`.
  9. ~~Phase 1.8c Zstd/Brotli behind `create_codec`.~~ **complete** — `ZSTDCodec`
     uses the `paddor/zinc` shard (`~> 0.2.1`, pure Crystal RFC 8878: frame
     encoder levels -8..4 and a full frame decoder, no libzstd) and
     `BrotliCodec` uses `OrelSokolov/brotli` (`~> 0.3.0`, pure Crystal RFC 7932
     decoder + encoder, quality 0..11); both are wired into `create_codec` and
     `create_codec` now raises `NYIError` for LZO exactly like upstream (which
     has no LZO implementation at 60.0.0). Upstream `test_codec_brotli` and
     `test_codec_zstd` are ported (full level sweeps, with and without
     `uncompress_size`); the previously pending fixtures now run
     (`byte_stream_split.zstd.parquet`, `alp_extended.zstd.parquet`,
     `large_string_map.brotli.parquet`). Divergence: `zinc` implements levels
     -8..4, so `ZSTDCodec` clamps the validated `ZstdLevel` into that range
     (ratio only; upstream's libzstd also clamps internally). Anchors:
     `vendor/arrow-rs/parquet/src/compression.rs` (`brotli_codec`,
     `zstd_codec`, `BROTLI_DEFAULT_BUFFER_SIZE`/`BROTLI_DEFAULT_LG_WINDOW_SIZE`),
     `src/parquet/compression_codecs.cr`, `src/parquet/compression.cr`.
  10. ~~The offset-index-driven `SerializedPageReader` state.~~ **complete** —
      `SerializedPageReader::State` (`Values`/`Pages`) ports upstream
      `SerializedPageReaderState`; `Pages` consumes the chunk's `PageLocation`
      queue (with the synthesized dictionary location), and
      `peek_next_page_offset` plus the `Iterator(Column::Page)` impl are
      available. Reading through page locations is enabled by
      `ReadOptionsBuilder#with_page_index`. Tests:
      `spec/parquet/file/serialized_reader_pages_spec.cr`. Anchors:
      `vendor/arrow-rs/parquet/src/file/serialized_reader.rs`
      (`SerializedPageReaderState`), `src/parquet/file/serialized_reader.cr`,
      `src/parquet/file/reader.cr`.
  11. ~~Arrow interop: Arrow → Parquet schema + `ARROW:schema` metadata.~~
      **complete** — see the Phase 3.5 checklist; `arrow-cr` is a
      `path: ../arrow-cr` dependency now. The next arrow feature is the
      Parquet → Arrow schema direction (Phase 3.5, first unchecked item).
  12. Deferred / blocked (recorded divergences and candidates):
     - **Blocked on the separate `../arrow-cr` port**: `parquet/src/arrow/`, the
       `column/page_store.rs` module (only consumed by the arrow writer), the
       `parquet-rewrite`/`parquet-fromcsv` CLIs, and `parquet-variant-compute`.
     - **Blocked by design**: `async_reader`/`async_writer`/`object_store`
       suffix fetchers (Rust tokio/futures; the fiber mapping decision has to be
       recorded first).
     - **Actionable without arrow** (pick one next): `parquet-variant-json`
       (`serde_json` → `JSON::Any`, no arrow dependency), and the `column/mod.rs`
       glue that Phase 2.2 still lists as pending.
- Remaining `missing` ledger rows are dominated by deferred areas (arrow,
  variant-compute, benches/examples), `HeapSize`/`memory_size` (intentional
  divergence), and auto-generated `data_type` trait impls (`as_any`/`set_data`/
  `skip`).
- **Scoping notes / gotchas (found while porting; avoids re-derivation):**
  - Crystal `String.new(Bytes)`/`String#byte_slice?` do **not** validate UTF-8
    and **copy** substrings, so the variant crate's zero-copy APIs
    (`VariantMetadata#borrowed_field_id`, borrowed `&str` returns) are
    intentional divergences implemented as name searches.
  - Crystal has no date-only/time-of-day types: variant/geospatial dates and
    times map to `Time` / `Time::Span`; `Uuid` → `UUID`.
  - `Variant` (the payload enum) is a sealed class hierarchy; built-in type
    names (`Int32`, `String`, `Time`, `Float`, ...) are shadowed by the nested
    subtypes inside the class body, so built-ins are written with a leading `::`.
    Optional payloads (`Option<()>`) map to `Bool` (`Variant#as_null`), and
    `String`/`Bytes` copy rather than borrow. Rust `From`/`TryFrom` impls are
    `Variant.from_*` factory methods (`from_date`, `from_datetime_utc`,
    `from_naive_datetime`, `from_time`, `from(value)`), since Crystal cannot
    overload on distinct date/time types. `Descriptor#is_fully_validated`
    (upstream name) is kept with a `fully_validated?` alias; the
    `Naming/PredicateName` rule is excluded for the variant value files.
  - Crystal has no cargo feature flags: feature-gated code (e.g. the `crc` page
    checksum, `json`) is always on.
  - Compression: SNAPPY uses the `naqvis/snappy` shard (`~> 0.2.0`, pure Crystal,
    MIT; the repo's first runtime dependency) for the raw/block format, kept
    behind `CompressionCodecs::SnappyCodec` so `create_codec`/tests are
    shard-agnostic. The `find-crystal-shards` review of the rest: the old
    `naqvis/lz4.cr` (last push 2023-07), `naqvis/brotli.cr` (2021-03), and
    `didactic-drunk/zstd.cr` (2024-05) options are stale or lib bindings, and
    the newer pure-Crystal shards won for all three: `paddor/flint` (LZ4
    blocks), `paddor/zinc` (ZSTD), `OrelSokolov/brotli` (BROTLI) — see the
    dedicated notes below. LZO has no shard and no upstream implementation.
    Shard quirk: `Compress::Snappy.encode` raises `IndexError` on empty input, so
    `SnappyCodec#compress` emits the canonical empty block (`0x00`) itself.
    Malformed input raises `ExternalError` wrapping `Compress::Snappy::SnappyError`
    (upstream `snap::Error` → `ParquetError::External`). Verified against real
    Google-snappy fixtures and an independent format-spec decoder.
  - LZ4: blocks come from the `paddor/flint` shard (`~> 0.1.1`, pure Crystal,
    MIT; `Flint::BlockCodec#compress_raw`/`#decompress_raw_into`) behind
    `LZ4RawCodec`. No active shard implements the LZ4 *frame* container
    (`naqvis/lz4.cr` is a stale `liblz4` binding), so frames are implemented
    in-tree in `src/parquet/lz4.cr` (magic, descriptor, block framing, skippable
    frames, XXH32 header/content checksums; `XxHash32` lives in
    `util/xxhash.cr`). Details: `flint` emits a zero-length block for empty
    input, so `LZ4RawCodec#compress` writes the canonical single-token empty
    block (`0x00`); flint errors (`Flint::Error`) map to `ExternalError`
    (upstream `lz4_flex` errors → `ParquetError::External`); `LZ4Codec` emits one
    frame with 1 MiB independent blocks instead of streaming 4 KiB chunks
    (`LZ4_BUFFER_SIZE` is skipped in the ledger) — still a valid frame, just a
    different block layout. `LZ4HadoopCodec` ports the `TryDecompressHadoop`
    loop verbatim, including its quirk of only continuing to another frame while
    the remaining input is longer than the frame just decoded. Verified three
    ways: the five real fixtures (incl. the `non_hadoop` raw-block fallback),
    byte-level interop with the `lz4` CLI (our frames decode with liblz4 and
    liblz4 frames — content checksums, 64 KiB blocks, incompressible blocks,
    concatenated frames — decode with ours), and upstream
    `parquet-read` reading files **written** by this port with `LZ4`/`LZ4_RAW`
    (multi-row-group INT64 and dictionary-encoded string pages).
  - ZSTD/BROTLI: `paddor/zinc` (`~> 0.2.1`; pure Crystal RFC 8878 frames,
    decoder + Fast/DFast encoder levels -8..4) and `OrelSokolov/brotli`
    (`~> 0.3.0`; pure Crystal RFC 7932 decoder + encoder, quality 0..11,
    `lgwin` 10..22) are both active and have their own test suites (zinc: 156
    minitest cases incl. `zstd`-CLI cross-verification; brotli: 56 specs plus
    differential runs against the reference C decoder). `ZSTDCodec` clamps
    levels to -8..4 (see the divergence note); zinc's `FrameCodec` writes
    standard frames (magic `0xFD2FB528`, content size), and brotli streams are
    self-terminating. `Brotli::Decoder#decode` copies the input into a padded
    buffer internally, so pages sliced out of a larger buffer are safe, but the
    decoder never resets its output buffer — create a new one per call. Verified
    three ways: the reference `zstd`/`brotli` CLIs decode this port's output
    (`zstd -d`, `brotli -d`, including the 1-byte empty brotli stream), this
    port decodes CLI output (`zstd -1`/`-19`, `brotli -q 1`/`-q 11`, incl.
    multi-block 300 KiB inputs and empty input), and upstream `parquet-read`
    (built with `--features cli,brotli,zstd`) reads files written here with
    `ZSTD`/`BROTLI` (multi-row-group INT64 and dictionary-encoded string pages).
  - Enum-with-payload Rust types are modelled either as a sealed class hierarchy
    (`Variant`, `Field`, `Page`, `Type`, `LogicalType`) or a struct with a
    `Kind` + payload (`Compression`, `DecodeResult`, `ColumnOrder`); pick per
    module and record the choice in `plans/parity.md`.
  - `T.class` dispatch replaces Rust traits with associated consts/generics
    (`Encodings.get_encoder`, `FromBytes`, `AsBytes`).
  - Variant value builders: Rust's affine borrows and `Drop`-based rollback are
    not modelled, so an unfinished child builder must be rolled back explicitly
    via `abandon` (`ValueBuilder.truncate`/`ParentState#rollback`); multiple
    children may exist at once (Rust forbids it). `Into<Variant>` is the
    `VariantValue` union + `ValueConversion.to_variant`; bare `Time`
    (date/UTC/naive) is not a `VariantValue` (wrap with `Variant.from_date`/
    `from_datetime_utc`/`from_naive_datetime`; only `Time::Span` maps via
    `from_time`). `VariantBuilderExt`/`ObjectFieldBuilder` (used by
    `parquet-variant-compute`) are not ported. The Rust `Debug` impls are
    `Variant#debug_string(pretty)`/`Variant#debug_to` (`variant/debug.cr`).
  - Geospatial bounding: upstream operates on `geo-traits` and parses WKB with
    the `wkb` crate; Crystal has neither, so `wkb.cr` provides a minimal
    `WkbReader` + owned `WkbGeometry` model (ISO 2D/Z/M/ZM types only) and
    `update_wkb` raises `InvalidArgumentError` on malformed input (upstream
    `ExternalError`). `update_geometry` is public so tests feed the model
    directly (upstream tests use the `wkt` crate); the `Testing` WKB helpers are
    ported. Inventory has no rows for `testing.rs`/`wkb` helpers (not in the
    generated ledger).
  - Core geospatial (`parquet/src/geospatial/`): `BoundingBox` is a value
    `struct` with scalar `xmin`/`xmax`/`ymin`/`ymax`/`zmin`/... getters (Rust
    `(f64, f64)` tuples), whose `with_*range` builders mutate and return `self`
    (Rust consumes `mut self`); `is_z_valid`/`is_m_valid` keep the upstream names
    (they carry line-level `ameba:disable Naming/PredicateName`).
    `GeospatialStatistics` is a class (`Option<Box<..>>` → plain reference).
    The `OnceLock<Arc<dyn GeoStatsAccumulatorFactory>>` global is a module class
    variable: it is lazily initialized by `try_new_geo_stats_accumulator`
    (`OnceLock::get_or_init`), `init_geo_stats_accumulator_factory` raises
    `GeneralError` once set, and it is not thread-safe (divergence).
    `ParquetGeoStatsAccumulator#update_wkb` catches
    `ParquetGeospatial::InvalidArgumentError` (upstream checks
    `Result::is_err`); `is_valid` keeps the upstream trait name and
    `src/parquet/geospatial/accumulator.cr` is excluded from
    `Naming/PredicateName` in `.ameba.yml`. The domain types and the thrift
    structs are separate (`Parquet::Geospatial::BoundingBox` vs
    `Parquet::File::Metadata::BoundingBox`), converted by
    `convert_geo_stats`/`convert_bounding_box` (read) and
    `geo_stats_to_thrift`/`bounding_box_to_thrift` (write), which stand in for the
    upstream `WriteThrift` impls on the domain types; empty
    `geospatial_types` lists are filtered to `nil` on read (upstream
    `filter(|v| !v.is_empty())`). Encoder wiring: when a geo accumulator exists
    the encoder skips min/max/nan stats entirely (Geometry/Geography have
    undefined sort order) but still updates `variable_length_bytes` and the
    bloom filter; `SerializedFileWriter#finish_appended_column` re-attaches
    `geo_statistics` when copying column metadata.
  - Thrift doubles (found while porting 3.7): `ThriftSliceInputProtocol#
    read_double`, `ThriftReadInputProtocol#read_double`, and
    `ThriftCompactOutputProtocol#write_double` had never been instantiated and
    called non-existent `Slice#read_bytes`/`#write_bytes`; they now go through
    `IO::Memory`. `serialize_column_meta_data` also did not advance
    `last_field_id` after the `SizeStatistics` field (field 16), which produced a
    wrong delta for the next field; it now assigns the return value like
    upstream.
  - Derive macros: Rust's `#[derive(ParquetRecordWriter/Reader)]` (proc macros
    over `syn`) become `include Parquet::Derive::Record` (a Crystal macro using
    `@type.instance_vars` inside method bodies via `{% verbatim %}` +
    `{% begin %}`). Upstream impls `RecordWriter for &[T]` / `RecordReader for
    Vec<T>`; Crystal cannot subclass slices, so nested `T::Writer`/`T::Reader`
    classes are generated. The reader constructs records positionally through
    the record's `initialize` (declare fields in the same order; Rust uses
    `Default` + pub field assignment). Field typing is Crystal-native
    (`T?` => OPTIONAL; `Time` => TIMESTAMP_MILLIS; `Parquet::Derive::Date` =>
    DATE). The syn-AST walkers (`Type.from*`, `last_part`, `leaf_type_recursive`)
    and the Rust `Result`-alias hygiene test have no analogue.
  - Encryption ciphers: Crystal's `OpenSSL::Cipher` exposes GCM tags but no AAD
    setter, so `encryption/ciphers.cr` adds a minimal `AesGcm` over raw OpenSSL
    EVP (`evp_aes_128_gcm`/`evp_aes_256_gcm`, re-bound in `LibCrypto`) to supply
    AAD. Rust `ring` is replaced; the `RingGcmBlock*` names drop the prefix and
    `HeapSize#heap_size` has no analogue.
  - Encryption IDL: the payload-carrying encryption unions
    (`ColumnCryptoMetaData`, `EncryptionAlgorithm`) are hand-written
    `struct` + `Kind` + payload values (like `ColumnOrder`/`Compression`); the
    `HeapSize` impls are not ported. The encrypted-footer reading helpers
    (`row_group_from_encrypted_thrift`/`parquet_metadata_with_encryption`/
    `get_file_decryptor`) are deferred with the properties/AAD layer.
  - Encryption AAD: `ModuleType` and the `create_footer_aad`/
    `create_module_aad` builders are ported (`parquet/encryption/modules.cr`);
    `usize` ordinals are `Int32`, overflow raises `GeneralError`, and the
    dead-code-marked `_BloomFilter*` variants drop the underscore.
    No ledger rows (the extractor omits these free functions/enum).
  - Encryption properties: `FileEncryptionProperties`/`EncryptionPropertiesBuilder`
    (`Vec<u8>`→`Bytes`, `HashMap`→`Hash`, `Result` raises,
    `Arc<FileEncryptionProperties>` degrades to the class) and `FileEncryptor`
    (random `aad_file_unique` via `Random::Secure`, `is_column_encrypted` →
    `column_encrypted?`, `get_*_encryptor` → `*_encryptor`). `EncryptionKey` is
    public (upstream private) since it appears in the exposed
    `column_keys_map`/`column_keys`.
  - Decryption properties: `Cow<'_, Vec<u8>>`→`Bytes`; `Arc<dyn BlockDecryptor>`/
    `Arc<dyn KeyRetriever>`→the `BlockDecryptor`/`KeyRetriever` abstract classes;
    `DecryptionKeys` is a struct with a `Kind`; `FileDecryptionProperties#keys`
    and `FileDecryptor#decryption_properties`/`#footer_decryptor` are public
    (upstream private); `uses_key_retriever`→`uses_key_retriever?`; `HeapSize`
    dropped. `read_and_decrypt` rebuilds the zero-prefixed block the same way.
  - `ColumnChunkMetaData` crypto: `column_crypto_metadata`/`encrypted_column_metadata`/
    `plaintext_footer_mode` are public `property`s; `crypto_metadata` accessor
    added. Fields 8/9 are read/written, the plaintext `meta_data` (field 3) is
    skipped when `encrypted_column_metadata` is present and `plaintext_footer_mode`
    is false, and the reader returns early on `encrypted_column_metadata`.
  - Encrypted-footer reader: the helpers live in
    `file/metadata/thrift/encryption.cr` (`get_file_decryptor`,
    `row_group_from_encrypted_thrift`, `parquet_metadata_with_encryption`);
    `FileMetaData` gains `encryption_algorithm`/`footer_signing_key_metadata`
    (thrift fields 8/9). `ParquetMetaData` gains a `file_decryptor` field,
    `with_file_decryptor`, and `ParquetMetaDataBuilder#set_file_decryptor`
    (included in `==`, as upstream). `AES_GCM_CTR_V1` raises `NYIError`
    (upstream 60.0.0 does the same).
  - Encryption writer: `MetadataObjectWriter` ports the encrypted/signed-plaintext
    footer, row-group/column-chunk encryption, page-index encryption, and the
    `PARE`/`PAR1` magic; `ThriftMetadataWriter`/`ParquetMetaDataWriter` take a
    `FileEncryptor?` via `with_file_encryptor`. Divergence: the Rust
    `FileMeta`/`WriteThrift` wrapper is inlined (the file metadata is serialized
    to a buffer before encryption) and `apply_row_group_encryption` mutates row
    groups in place and returns `nil` for the unencrypted copy (upstream returns a
    deep copy); `get_file_magic`→`file_magic`.
  - Public writer API: `WriterProperties#file_encryption_properties` (builder
    `set_file_encryption_properties`, upstream `with_file_encryption_properties`),
    `GenericColumnWriter#build_column_metadata` attaches
    `Encryption.get_column_crypto_metadata`, and `SerializedFileWriter` builds a
    `FileEncryptor`     in its constructor and passes it to the metadata writer
    (`getter file_encryptor`; upstream `get_file_encryptor` returns a `Result`).
  - Reader metadata decryption:
    `ParquetMetaDataPushDecoder#with_file_decryption_properties` routes the
    footer decode through `parquet_metadata_with_encryption`, and
    `ParquetMetaDataReader#with_file_decryption_properties` forwards it;
    `ReadOptions`/`ReadOptionsBuilder` gain a decryption-properties option so
    `SerializedFileReader.new_with_options` can read encrypted metadata (a
    Crystal extension; upstream 60.0.0 exposes decryption via Arrow options).
    The `MetadataParser` decryption hook is instead handled at the push-decoder
    level.
  - Page encryption on write: `PageEncryptor`
    (`src/parquet/column/page_encryption.cr`) encrypts page headers/data with
    DataPageHeader/DataPage (or DictionaryPageHeader/DictionaryPage) AAD and an
    advancing page index; `CompressedPage#with_new_compressed_buffer` swaps in
    the ciphertext; `SerializedPageWriter`/`SerializedRowGroupWriter` create an
    encryptor only for encrypted columns. The upstream no-encryption
    `page_encryption_disabled.rs` stub is not needed (a nil encryptor is used).
  - Page decryption on read: `SerializedPageReader#add_crypto_context` builds a
    `CryptoContext` from `ParquetMetaData#file_decryptor` + the column's
    `crypto_metadata`; `read_page_header`/`decrypt_page_data` decrypt the
    (length-prefixed) page header and page payload before thrift/decode, with
    DataPage/DictionaryPage AADs and the advancing page ordinal. Divergence:
    upstream attaches the context only in the arrow reader; here
    `SerializedRowGroupReader#get_column_page_reader` (which now carries the row
    group index + full `ParquetMetaData`) also attaches it, so encrypted files
    are readable without an arrow layer.
  - Page-crypto details (learned while porting 3.4l): encrypted headers/pages are
    framed as `4-byte LE length + ciphertext` (the AesGcm ciphertext carries
    nonce+tag), so the header byte count is measured with `CountingIO` around
    `read_and_decrypt`; the dictionary-page flag is `@require_dictionary`
    (matches upstream), and data decryption uses
    `with_page_ordinal(@page_index)`/`for_dictionary_page`. `ParquetMetaData#==`
    now compares `file_decryptor`, which requires `FileDecryptor#==` /
    `FileDecryptionProperties#==` (the latter had a stale method name, fixed).
    Tests: `spec/parquet/file/page_decryption_spec.cr`; force multiple pages
    with `set_data_page_row_count_limit(1)` and corrupted-page failure by
    flipping the chunk's last ciphertext byte.
  - Content-defined chunking (Phase 2.2): `cdc_generated.cr` is a literal
    transcription of the upstream `[[u64; 256]; 8]` gear-hash table (parsed from
    `cdc_generated.rs`, verified value-for-value), so chunk boundaries match
    upstream byte-for-byte. Divergences: `ContentDefinedChunker.new` raises
    `GeneralError` (upstream `Result`); `roll_fixed::<N>(&[u8; N])` is
    `roll_fixed(bytes : Bytes)` sharing `roll`'s loop (no const-generic array
    width — behavior identical); `usize` offsets are `Int32`; the
    `FnMut(&mut Self, usize)` callback is a block receiving the chunker and the
    value offset; `debug_assertions` → `{% unless flag?(:release) %}` around
    `validate_chunks`; Rust `assert!`/`expect` → `GeneralError`. `roll`/
    `roll_fixed`/`roll_level`/`need_new_chunk`/`validate_chunks` are public
    because the `calculate` block drives them (upstream keeps them private but its
    child test module can reach them). `get_arrow_chunks` and the `arrow_tests`
    roundtrip module are deferred with the arrow layer, and the 8 `CDC_*`/
    `*_LENGTH` constants are test-only (in `arrow_tests`), hence `skipped` in the
    ledger. `CdcChunk` lives in `chunker.cr` (upstream `chunker/mod.rs`).
- Last commit: `port: arrow row filter`; gates green
  (`make lint`, `make test` -> 1206 examples, 0 failures). Next feature:
  `statistics.rs` (`StatisticsConverter`), then the mask cursor and page
  pruning. Note `make install` now requires the sibling checkout `../arrow-cr`.
- Progress by counts (ledger): 2123 `ported`, 219 `intentional_divergence`,
  15 skipped, 1158 `missing` (of 3515 rows).

Chunk status by area:

- Phase 0 initialization: complete.
- Phase 1.1 errors: complete.
- Phase 1.2 thrift protocol: complete.
- Phase 1.3 thrift macros: enum/union/struct/empty done; mixed-union unions
  (`LogicalType`/`ColumnOrder`/`encryption`) are hand-written and the encryption
  IDL is now ported (`ColumnCryptoMetaData`/`EncryptionAlgorithm`); a generic
  mixed-union macro is still pending.
- Phase 1.4 basic: complete (enums, `LogicalType`, `SortOrder`/`ColumnOrder`,
  `EncodingMask`, `Compression` + levels).
- Phase 1.5 data type: value types + `AsBytes` done; `DataType` trait +
  `make_type!` deferred (`T.class` dispatch is used instead).
- Phase 1.6 schema: `types`, descriptors, parser, thrift conversion, page
  headers, and printer done (`schema/primitive.rs` does not exist at 60.0.0;
  `PrimitiveTypeBuilder` covers it).
- Phase 1.7 util: `BitReader`/`BitWriter`, `Interner`/`Storage`, `PushBuffers`,
  `common_prefix_length`, bit packing (`BitPack.unpack*`), and `decode_page` CRC
  verification done (no `util/cursor.rs` at 60.0.0).
- Phase 1.8 compression: complete — `Codec` framework, `GZipCodec`,
  `SnappyCodec`, `ZSTDCodec` (`paddor/zinc`), `BrotliCodec`
  (`OrelSokolov/brotli`), and the three LZ4 codecs (`LZ4RawCodec` via the
  `paddor/flint` shard, `LZ4HadoopCodec` with the upstream fallback chain, and
  the in-tree frame container `LZ4Codec`). LZO raises `NYIError`, as upstream
  does (no LZO implementation at 60.0.0).
- Phase 2.3 file metadata/statistics/page-index core, column-index decode,
  page-index builders, the `ParquetMetaData` page-index plumbing, the sans-IO
  `ParquetMetaDataPushDecoder`, the synchronous `ChunkReader`-driven
  `ParquetMetaDataReader`, the `ParquetMetaDataWriter`/`ThriftMetadataWriter`,
  the bloom filter (`Sbbf`), the serialized page reader/writer (offset-stream
  and offset-index states), and the file-level
  `SerializedFileReader`/`SerializedFileWriter` done.
- Phase 2.1 encodings: complete (all codecs, the `Encoder`/`Decoder`
  interfaces, and the `get_encoder`/`get_decoder` registries).
- Phase 2.2 column: `ColumnDescriptor`, column page types/interfaces, the
  level/value decoders, the writer value encoder, the byte-budget chunker, the
  truncation helpers, `ColumnReaderImpl`, `GenericColumnWriter`, and the
  content-defined chunker (`src/parquet/column/chunker/`) done;
  `column/mod.rs` glue pending and `page_store` deferred (only the arrow writer
  consumes it).
- Phase 2.4 record: `api` (`Field`/`Row`/`List`/`Map`), `triplet`
  (`TripletIter`), `reader` (`TreeBuilder`/`Reader`/`RowIter`), and the
  `RecordReader(T)`/`RecordWriter(T)` traits done; `get_row_iter` wired;
  `FilePageIterator` + the `PageIterator` trait done.
- Phase 2.5 CLI: `cli` shared helper (`parse_column_path`), `parquet-index`,
  `parquet-read`, `parquet-rowcount`, `parquet-show-bloom-filter`,
  `parquet-layout`, `parquet-schema`, and `parquet-concat` done;
  `parquet-rewrite`/`parquet-fromcsv` need arrow.
- Phase 3 (variant/geospatial/derive/encryption/arrow/async): `parquet-variant`'s
  `VariantPath`/`VariantPathElement` + parser, the `utils` helpers,
  `OffsetSizeBytes`/`map_bytes_to_offsets`, the value decoder + `ShortString`,
  `VariantMetadata`, `VariantDecimal4`/`8`/`16`, the metadata builders, the value
  layer read side (`Variant`/`VariantObject`/`VariantList` + `Debug`) and the
  value builder (`ValueBuilder`/`VariantBuilder`/`ObjectBuilder`/`ListBuilder`)
  done (core crate complete);
  `parquet-geospatial`'s `Hint`/`Edges`/`Metadata` + `WkbType` metadata,
  `Interval`/`WraparoundInterval`, and `GeometryBounder` (+ minimal `WkbReader`/
  `WkbGeometry` model and `Testing` helpers) done; `parquet_derive`
  (`src/parquet/derive.cr`), the encryption cipher layer
  (`src/parquet/encryption/ciphers.cr`), the encryption Thrift IDL
  (`src/parquet/file/column_crypto_metadata.cr`,
  `src/parquet/file/metadata/thrift/encryption.cr`), the AAD builders
  (`src/parquet/encryption/modules.cr`), the encryption properties
  (`src/parquet/encryption/encrypt.cr`), the decryption properties
  (`src/parquet/encryption/decrypt.cr`), the `ColumnChunkMetaData` crypto
  wiring (`src/parquet/file/metadata/column_chunk.cr`), the encrypted-footer
  reader (`src/parquet/file/metadata/thrift/encryption.cr`), and the
  encryption-aware metadata writer (`src/parquet/file/metadata/writer.cr`), and
  the public writer API (`WriterProperties`/column writer/`SerializedFileWriter`),
  and reader metadata decryption (`ParquetMetaDataReader`/`SerializedFileReader`),
  and page-data encryption/decryption (`PageEncryptor`; `CryptoContext` wired
  through `SerializedPageReader`) done; the core `parquet/src/geospatial`
  module (`BoundingBox`, `GeospatialStatistics`, the accumulator + writer
  wiring) done; the `WkbType` `ExtensionType` impl, arrow, and async remain
  pending.

## Phase 0 — Project initialization

- [x] Upstream submodule pinned at `60.0.0`; fixtures via `parquet-testing`.
- [x] Baseline tooling: `Makefile`, `.ameba.yml`, `.rumdl.toml`, `.gitignore`,
  `shard.yml`.
- [x] Baseline docs: `README.md`, `CLAUDE.md`/`AGENTS.md`, `docs/*`.
- [x] Parity ledger generated (tree-sitter) — 3,515 API items, 2,059 tests.
- [x] Baseline verifier passes; `format`/`ameba`/`spec` green.

## Phase 1 — Core value and wire layer

### 1.1 Errors

- [x] `ParquetError` variant set (`src/parquet/errors.cr`). The `Result` alias is
      an intentional divergence — Crystal uses exceptions.
- [x] Upstream `errors.rs` test module ported to `spec/parquet/errors_spec.cr`.

### 1.2 Thrift compact protocol

- [x] `ElementType`, `FieldType` mappings.
- [x] `ReadThrift`/`WriteThrift`/`WriteThriftField` — mapped to protocol
      overloads and duck-typed `read_thrift`/`write_thrift` (intentional
      divergence; no associated consts in Crystal).
- [x] `ThriftCompactInputProtocol` + `ThriftSliceInputProtocol` +
  `ThriftReadInputProtocol`.
- [x] `ThriftCompactOutputProtocol`, `read_thrift_vec`, `validate_list_type`.
- [x] Upstream `parquet_thrift.rs` protocol tests ported to
  `spec/parquet/thrift_spec.cr`.

### 1.3 Thrift IDL macros (`parquet_macros.rs`)

- [x] Enum + all-empty-union support: `define_thrift_enum` and
  `define_thrift_union_all_empty` (`src/parquet/macros.cr`). Intentional
  divergence: upstream parses raw Thrift IDL; Crystal annotates native enums.
- [x] Struct support equivalent to `thrift_struct!`: `define_thrift_struct`
  (required/optional fields incl. empty structs, nested structs/enums, `bool`
  field encoding, all-required constructors, required-field validation,
  unknown-field skipping).
- [~] Mixed-union support equivalent to `thrift_union!` /
  `thrift_union_with_unknown!`. `LogicalType`/`ColumnOrder` read/write is
  hand-written; a generic union macro is still pending (needed for crypto
  metadata and encryption).
- [~] Parquet metadata IDL structs (`file/metadata/thrift/mod.rs`): done for
  `SchemaElement`, page headers, `KeyValue`, `PageEncodingStats`,
  `SortingColumn`, `SizeStatistics`, `ThriftColumnIndex`, `PageLocation`,
  `OffsetIndexMetaData`. Pending: encryption (`file/metadata/thrift/encryption.rs`).

### 1.4 Basic types

- [x] Thrift enums: `Type`, `ConvertedType`, `FieldRepetitionType`
  (`Repetition`), `Encoding`, `CompressionCodec`, `PageType`, `BoundaryOrder`,
  and the all-empty union `TimeUnit` (`src/parquet/basic.cr`).
- [x] `LogicalType` union (+ `DecimalType`, `TimestampType`/`TimeType`,
  `IntType`, `VariantType`, `GeometryType`, `GeographyType`,
  `EdgeInterpolationAlgorithm`), with `ConvertedType.from(LogicalType?)` and
  `LogicalType.from_str`.
- [x] `SortOrder`, `ColumnOrder` (incl. `column_order_for_type`).
- [x] `EncodingMask` (`try_new`, `new_from_encodings`, `set?`/`only?`/`all_set`,
  `encodings`, thrift reading).
- [x] `Compression` enum, `CompressionCodec`<->`Compression` conversions, and
  `Compression` `Display`/`from_str`; `GzipLevel`/`BrotliLevel`/`ZstdLevel`
  (`src/parquet/compression.cr`).

### 1.5 Data type model

- [x] Value types: `Int96`, `ByteArray`, `FixedLenByteArray`, `Decimal`, and the
  `AsBytes` trait (`src/parquet/data_type.cr`).
- [ ] `DataType` trait + `make_type!` (physical-type `TYPE`/`get_physical_type`/
  `get_type_size`, encode/decode) — depends on the column reader/writer.
- [x] `AsBytes` for primitives, `str`, `Bytes`, `bool` (`as_bytes` on the value
  types; `data_type_spec.cr` ports `test_as_bytes`). `SliceAsBytes` is not
  modelled (Crystal uses `Bytes`/`Slice` directly); the column-index write path
  encodes primitives with `FromBytes.to_le_slice`.

### 1.6 Schema

- [x] `Type` (primitive/group), `BasicTypeInfo`, primitive/group builders
  (full validation), `check_contains` (`src/parquet/schema/types.cr`).
- [x] `ColumnPath`, `ColumnDescriptor`, `SchemaDescriptor`, def/rep-level tree.
- [x] Textual schema parser (`src/parquet/schema/parser.cr`).
- [x] Thrift <-> schema conversion (`src/parquet/schema/thrift.cr`).
- [x] Page-header thrift structs (`src/parquet/format.cr`).
- [x] Schema printer (`src/parquet/schema/printer.cr`): `print_parquet_metadata`,
  `print_file_metadata`, `print_schema`, `Printer`, `print_timeunit`,
  `print_logical_and_converted`; `printer_spec.cr` (10 upstream tests) ported.
- [x] Primitive builders: `schema/primitive.rs` does not exist at `60.0.0`;
  `PrimitiveTypeBuilder` (`schema/types.cr`) covers the API (ported in Phase 1.6
  with the schema tests).

### 1.7 Util

- [x] `PushBuffers` (`src/parquet/util/push_buffers.cr`).
- [x] `BitReader`/`BitWriter` + helpers (`src/parquet/util/bit_util.cr`).
- [x] `Interner`/`Storage` (`src/parquet/util/interner.cr`) used by the
      dictionary encoder.
- [x] `common_prefix_length` (`src/parquet/util/prefix.cr`) used by
      DELTA_BYTE_ARRAY; the upstream 32-byte block scan is a scalar scan.
- [x] Bit packing (`src/parquet/util/bit_pack.cr`): `BitPack.unpack8`/`16`/`32`/
  `64` (scalar; the `NUM_BITS` const generic is a `num_bits` parameter);
  `bit_pack_spec.cr` ported.
- [x] Cursor / page readers: there is no `util/cursor.rs` at `60.0.0`; the
  `SerializedPageReader` (Phase 2.3) covers page reading.
- [x] CRC32 page-checksum verification (`src/parquet/file/serialized_reader.cr`):
  `decode_page` verifies `page_header.crc` via `Digest::CRC32` (always on, since
  Crystal has no feature flags); `page_crc_spec.cr` ported.

### 1.8 Compression

- [x] Codec dispatch + uncompressed/snappy/gzip/brotli/lz4/zstd paths:
  `Codec` framework + `create_codec` + every codec upstream implements at
  60.0.0 (`src/parquet/compression_codecs.cr`); LZO raises `NYIError`, matching
  upstream's `nyi_err!` for it.
- [x] SNAPPY (`SnappyCodec`): delegates to the `naqvis/snappy` shard (pure
  Crystal, MIT, `~> 0.2.0`; raw/block format) and, when `uncompress_size` is
  given, enforces the exact recorded length like `snap::raw::Decoder`. Upstream
  `test_codec_snappy` ported plus golden wire-format vectors, empty-input and
  size-mismatch cases, corrupt-input handling, and real fixtures
  (`nulls.snappy.parquet`, `datapage_v2.snappy.parquet`); the SNAPPY writer/page
  roundtrips no longer substitute GZIP.
- [x] LZ4 (`LZ4RawCodec`/`LZ4HadoopCodec`/`LZ4Codec`): raw blocks via the
  `paddor/flint` shard; Hadoop framing (`PREFIX_LEN`/`SIZE_U32`, big-endian
  sizes) with upstream's decode fallback chain (frame → raw) gated on
  `CodecOptions#backward_compatible_lz4`; the LZ4 frame container (magic,
  descriptor, independent 1 MiB blocks, skippable frames, XXH32 header/content
  checksums) lives in `src/parquet/lz4.cr`. Upstream `test_codec_lz4`/
  `test_codec_lz4_raw` ported, plus golden block vectors, corrupt input, the
  Hadoop prefix, both fallbacks, and the five real fixtures
  (`spec/parquet/file/lz4_fixture_spec.cr`); writer/page roundtrips cover
  `LZ4` and `LZ4_RAW`.
- [x] ZSTD / BROTLI (`ZSTDCodec`/`BrotliCodec`): `paddor/zinc` (pure Crystal
  RFC 8878 frames, levels -8..4) and `OrelSokolov/brotli` (pure Crystal RFC
  7932, quality 0..11, `lgwin` 22) replace upstream's `zstd`/`brotli` crates.
  Both ignore `uncompress_size` (their streams are self-describing), like
  upstream which only uses it as a buffer hint. Upstream `test_codec_zstd`/
  `test_codec_brotli` level sweeps ported, plus reference-encoder golden
  streams, corrupt/empty input, level clamping, and the
  `byte_stream_split.zstd`/`alp_extended.zstd`/`large_string_map.brotli`
  fixtures (`spec/parquet/file/zstd_brotli_fixture_spec.cr`).
- [x] Shard selection (`find-crystal-shards`): `naqvis/snappy` (SNAPPY),
  `paddor/flint` (LZ4 blocks), `paddor/zinc` (ZSTD), `OrelSokolov/brotli`
  (BROTLI) are all active pure-Crystal implementations; LZO has no active shard
  and no upstream implementation, so `create_codec` raises `NYIError` for it
  (an upstream parity point, not a porting gap). The LZ4 frame container needed
  in-tree code (`src/parquet/lz4.cr`) because no shard covers frames.

## Phase 2 — Encodings, columns, files

### 2.1 Encodings

- [x] RLE / bit-packing hybrid (`src/parquet/encodings/rle.cr`):
  `RleEncoder`/`RleDecoder`.
- [x] Levels (`src/parquet/encodings/levels.cr`): `LevelEncoder`
  (v1_streaming/v2_streaming/put_with_observer/put_n_with_observer/consume/
  flush_to).
- [x] Plain (`src/parquet/encodings/plain.cr`): `PlainEncoder`/`PlainDecoder`
  for all physical types (put/get/get_spaced/skip).
- [x] DELTA_BINARY_PACKED (`src/parquet/encodings/delta_bit_pack.cr`):
  `DeltaBitPackEncoder`/`DeltaBitPackDecoder` for INT32/INT64.
- [x] DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY
  (`src/parquet/encodings/delta_byte_array.cr`):
  `DeltaLengthByteArrayEncoder`/`Decoder` and
  `DeltaByteArrayEncoder(T)`/`DeltaByteArrayDecoder(T)`.
- [x] Dictionary codec (`src/parquet/encodings/dict.cr`): `DictEncoder(T)` and
  `DictDecoder(T)`, plus `KeyStorage(T)`; `util/interner.cr`.
- [x] BYTE_STREAM_SPLIT (`src/parquet/encodings/byte_stream_split.cr`):
  `ByteStreamSplitEncoder(T)`/`Decoder(T)` and
  `VariableWidthByteStreamSplitEncoder(T)`/`Decoder(T)`.
- [x] RLE value (`src/parquet/encodings/rle_value.cr`):
  `RleValueEncoder(T)`/`RleValueDecoder(T)` for BOOLEAN.
- [x] ALP (`src/parquet/encodings/alp.cr`): `AlpEncoder(F)`/`AlpDecoder(F)` for
  FLOAT/DOUBLE, plus `AlpHeader`/`AlpInfo`/`ForInfo(F)`/`AlpEncodedVectorView(F)`
  and the `Alp` sampling/encoding helpers.
- [x] `Encoder`/`Decoder` interfaces and the `get_encoder`/`get_decoder`
  registries (`src/parquet/encodings/traits.cr`, `registry.cr`), plus the
  `physical_type(T)` helper.
- [x] 7+ upstream test modules ported (RLE, levels, plain, delta binary/length/
  byte-array, dictionary, byte-stream-split, RLE value, ALP, registries).

### 2.2 Column

- [x] `ColumnDescriptor` + `ColumnReader` done; column writer in progress.
- [~] `ColumnWriterImpl` (`src/parquet/column/writer/writer.cr`):
  `GenericColumnWriter(T)` (write_batch/close/add_data_page/write_mini_batch/
  write_granular_chunk/column+offset index/metadata) + `ColumnWriter` dispatch
  (`get_column_writer`/`get_typed_column_writer`); arrow/geospatial/encryption
  paths deferred.
- [x] Column writer statistics-truncation helpers
  (`src/parquet/column/writer/truncate.cr`): `truncate_utf8`/
  `truncate_and_increment_utf8`/`increment_utf8`/`increment`,
  `can_truncate_value`/`is_utf8`/`truncate_min_value`/`truncate_max_value`/
  `truncate_statistics`.
- [x] Column writer data holders (`src/parquet/column/writer.cr`):
  `ColumnCloseResult`, `PageMetrics`, `ColumnMetrics(T)`, `LevelDataRef`.
- [x] Column writer byte-budget chunker
  (`src/parquet/column/writer/byte_budget_chunker.cr`): `ByteBudgetChunker` +
  `SubBatchStrategy`.
- [x] Content-defined chunker (`src/parquet/column/chunker.cr`,
  `src/parquet/column/chunker/{cdc,cdc_generated}.cr`): `CdcChunk`,
  `ContentDefinedChunker` (`calculate_mask`, `roll`/`roll_fixed`/`roll_level`,
  `need_new_chunk`, `calculate` for flat/nullable/nested data,
  `validate_chunks`) and the upstream gear-hash table; all seven upstream unit
  tests + a nested-path characterization (`spec/parquet/column/chunker_cdc_spec.cr`).
  `get_arrow_chunks`/`arrow_tests` are deferred with the arrow writer.
- [x] `ColumnReaderImpl` (`src/parquet/column/reader.cr`): `get_column_reader`/
  `get_typed_column_reader`, `ColumnReaderImpl(T)` (read_records/skip_records/
  has_next/read_new_page/parse_v1_level).
- [ ] `ColumnWriterImpl`.
- [x] Column writer value encoder + statistics/encoding helpers
  (`src/parquet/column/writer/encoder.cr`): `ColumnValueEncoder`(+Impl(T)),
  `DictionaryPage`, `DataPageValues(T)`, `get_min_max`/`update_min`/`update_max`/
  `is_nan`/`compare_greater*`, `fallback_encoding`, `has_dictionary_support`,
  `create_bloom_filter`, byte-budget counting.
- [x] Column page types/interfaces (`src/parquet/column/page.cr`): `Page`
  (`DataPage`/`DataPageV2`/`DictionaryPage`), `CompressedPage`, `PageWriteSpec`,
  `PageMetadata`, `PageReader`/`PageWriter`.
- [x] Column level/value decoders (`src/parquet/column/reader/decoder.cr`):
  `ColumnLevelDecoder`/`RepetitionLevelDecoder`/`DefinitionLevelDecoder`/
  `ColumnValueDecoder` abstract classes + `DefinitionLevelDecoderImpl`,
  `RepetitionLevelDecoderImpl`, `ColumnValueDecoderImpl(T)`, `LevelDecoder`.
- [x] Writer properties + reader properties (`src/parquet/file/properties.cr`):
  `WriterProperties`(+builder), `ReaderProperties`(+builder), `WriterVersion`,
  `EnabledStatistics`, `BloomFilterPosition`, `BloomFilterProperties`(+builder),
  `CdcOptions`, `ColumnProperties`/resolution; `CodecOptions` in
  `compression.cr`. 6+ upstream test modules.

### 2.3 File

- [x] `LevelHistogram` (`src/parquet/file/metadata.cr`).
- [x] `ColumnChunkMetaData` + builder + thrift read/write
  (`src/parquet/file/metadata/column_chunk.cr`), plus thrift structs `KeyValue`,
  `PageEncodingStats`, `SortingColumn`, `SizeStatistics`.
- [x] `RowGroupMetaData` + builder + thrift read/write
  (`src/parquet/file/metadata/row_group.cr`).
- [x] `FileMetaData`, `ParquetMetaData`, and footer read/write
  (`src/parquet/file/metadata/parquet_metadata.cr`): `write_file_metadata`,
  `parquet_metadata_from_bytes`, ordinal backfill, and a
  `ParquetMetaDataWriter`/`ParquetMetaDataReader.decode_metadata` round-trip.
- [x] Statistics and value stats (`src/parquet/file/statistics.cr`):
  `Statistics`, `ValueStatistics<T>`, `from_thrift_page_stats`/
  `page_stats_to_thrift`.
- [x] Offset index (`src/parquet/file/page_index/offset_index.cr`):
  `PageLocation`, `OffsetIndexMetaData`.
- [x] Column index core (`src/parquet/file/page_index/column_index.cr`):
  `ColumnIndex`, `PrimitiveColumnIndex<T>`, `ThriftColumnIndex`, `FromBytes`.
- [x] `ByteArrayColumnIndex`, `ColumnIndexMetaData`, and `decode_column_index`
  (`src/parquet/file/page_index/{column_index,index_reader}.cr`), including the
  `WriteThrift` impls for both index types.
- [x] Page-index builders: `ColumnIndexBuilder`/`OffsetIndexBuilder`
  (`src/parquet/file/metadata.cr`) and `PageIndexProvider`/`RowGroupPageIndex`/
  `PageIndex`/`PageIndexBuilder` (`src/parquet/file/metadata/page_index.cr`).
  Tests `test_page_index_builder_partial_population`,
  `..._empty_to_none`, `test_rebuild_page_index` ported.
- [x] `ParquetMetaDataBuilder`, `ParquetMetaData` page-index plumbing
  (`page_index`/`page_index_for_row_group`/`into_builder`/`set_page_index`),
  `ParquetMetaDataOptions`/`ParquetStatisticsPolicy`
  (`src/parquet/file/metadata/{parquet_metadata,options}.cr`), plus
  `decode_offset_index`/`acc_range` (`index_reader.cr`).
- [x] `ParquetMetaDataPushDecoder` + `MetadataParser` + `parse_page_index` +
  `range_for_page_index` (`src/parquet/file/metadata/{push_decoder,parser}.cr`),
  `FooterTail` (`footer_tail.cr`), `PageIndexPolicy` (`reader.cr`), `DecodeResult`
  (`src/parquet/decode_result.cr`), and `PushBuffers`
  (`src/parquet/util/push_buffers.cr`).
- [x] `ChunkReader`/`BytesChunkReader`/`FileChunkReader`
  (`src/parquet/file/chunk_reader.cr`) and the synchronous, `ChunkReader`-driven
  `ParquetMetaDataReader` (`reader.cr`).
- [x] `ParquetMetaDataWriter`/`ThriftMetadataWriter` page-index footer
  serialization (`src/parquet/file/metadata/writer.cr`) and `TrackedWrite`/
  `file_magic` (`src/parquet/file/tracked_write.cr`).
- [x] Serialized page reader/writer: `SerializedPageWriter` + `SerializedPageReader`
  (both the offset-stream `Values` and the offset-index `Pages` state) +
  `decode_page` done (`src/parquet/file/serialized_{writer,reader}.cr`);
  `peek_next_page_offset` and the `Iterator(Column::Page)` impl are ported
  (`spec/parquet/file/serialized_reader_pages_spec.cr`).
- [x] `SerializedFileWriter`/`SerializedRowGroupWriter`/`SerializedColumnWriter`
  + `write_bloom_filters`/`OnClose*` (`src/parquet/file/writer.cr`) and
  `FileReader`/`RowGroupReader`/`SerializedFileReader`/`SerializedRowGroupReader`
  + `ReadOptions`/`ReadOptionsBuilder`/`get_midpoint_offset`
  (`src/parquet/file/reader.cr`); `get_row_iter` is wired in `record/reader.cr`
  (Phase 2.4); `FilePageIterator` + the `PageIterator` trait are done
  (Phase 2.3); the `append_column`/`append_column_from_read` splice paths are
  done (only the arrow-only `append_column_from_pages` remains deferred). Writer
  spec + reader spec ported (`writer_spec.cr`, `reader_spec.cr`).
- [x] Bloom filter (`src/parquet/bloom_filter.cr`): `Sbbf` + `Block`, the
  `BloomFilterHeader` thrift struct, and the header/serialization helpers;
  `Parquet::XxHash64` (`src/parquet/util/xxhash.cr`).
- [~] Upstream test modules ported: `writer.rs` (`test_file_writer_*`,
  `test_row_group_writer_*`, `test_bytes_writer_*`, `test_column_offset_index_file`,
  `test_boolean_roundtrip`) in `writer_spec.cr`, and `reader.rs`/
  `serialized_reader.rs` (`test_bytes_chunk_reader_*`, `test_file_reader`,
  `test_reuse_file_chunk`, `test_file_reader_with_no_filter`/`_predicate`/
  `_range`) in `reader_spec.cr`; the remaining `arrow`/record-dependent modules
  are pending.

### 2.4 Record API

- [x] `record/api.cr`: `Field` (sealed hierarchy), `Row`, `List`, `Map`,
  `RowAccessor`/`ListAccessor`/`MapAccessor`/`RowFormatter` (modules),
  `RowColumnIter`, the `convert_*` builders, and the date/time/decimal/float
  formatting helpers + `to_json_value` (`JSON::Any`). `api_spec.cr` ported.
- [x] `record/triplet.cr`: `TripletIter` (abstract) + `TypedTripletIter(T)`.
  `triplet_spec.cr` ported (uncompressed fixtures; SNAPPY cases deferred).
- [x] `record/reader.cr`: `TreeBuilder`, `Reader` (+
  `Primitive/Group/Option/Repeated/KeyValue` subclasses), `ReaderIter`,
  `RowIter`; `get_row_iter` added to `FileReader`/`RowGroupReader`.
  `reader_spec.cr` ported.
- [x] Record reader/writer traits (`src/parquet/record/record_reader.cr`,
  `record_writer.cr`): `RecordReader(T)`/`RecordWriter(T)` abstract classes, used
  by `parquet_derive` (Phase 3.3); not tracked in the ledger. Characterized in
  `record_traits_spec.cr`.
- [~] 3 upstream test modules ported (the SNAPPY-fixture cases are covered with
  uncompressed fixtures); `parquet/tests` record suites pending.
- [x] `FilePageIterator` + the `PageIterator` trait (`src/parquet/file/reader.cr`,
  `src/parquet/column/page.cr`); `page_iterator_spec.cr` ported.

### 2.5 CLI

- [x] CLI foundation: `src/parquet/bin/cli.cr` (`Cli.parse_column_path`; upstream
  `cli/mod.rs` tests ported) and the `bin/` entry points; `OptionParser` replaces
  `clap`.
- [x] `parquet-index` (`src/parquet/bin/parquet_index.cr`,
  `bin/parquet-index.cr`); `run` writes to an `IO`; `compute_row_counts`
  characterized. `parquet_index_spec.cr` ported.
- [x] `parquet-read` (`parquet_read.cr`, `bin/parquet-read.cr`; `run`/`run_stdin`,
  `-n`/`-j`), `parquet-rowcount` (`parquet_rowcount.cr`,
  `bin/parquet-rowcount.cr`), `parquet-show-bloom-filter`
  (`parquet_show_bloom_filter.cr`, `bin/parquet-show-bloom-filter.cr`), and
  `parquet-layout` (`parquet_layout.cr`, `bin/parquet-layout.cr`; `JSON.build`
  pretty output), and `parquet-schema` (`parquet_schema.cr`,
  `bin/parquet-schema.cr`; uses the schema printer). Specs in `spec/parquet/bin/`.
- [x] `parquet-concat` (`parquet_concat.cr`, `bin/parquet-concat.cr`; uses the
  `append_column` splice path, preserving the inputs' page indexes/bloom
  filters). Spec in `spec/parquet/bin/parquet_concat_spec.cr`.
- [ ] `parquet-rewrite`/`parquet-fromcsv` — require `arrow`.

## Phase 3 — Advanced / optional

### 3.1 Variant crates

- [~] `parquet-variant` (Variant binary encoding): `VariantPath`/
  `VariantPathElement` + the dot/bracket parser (`src/parquet_variant/path.cr`,
  `path_spec.cr`), the `utils` byte-slicing helpers
  (`src/parquet_variant/utils.cr`), `OffsetSizeBytes`/`map_bytes_to_offsets`
  (`src/parquet_variant/decoder.cr`: `OffsetSizeBytes`, `map_bytes_to_offsets`,
  `VariantBasicType`/`VariantPrimitiveType`, `get_basic_type`/
  `get_primitive_type`, and the primitive `decode_*` helpers), `ShortString`
  (`src/parquet_variant/short_string.cr`, `decoder_spec.cr`), `VariantMetadata`
  (`src/parquet_variant/variant/metadata.cr`, `metadata_spec.cr`), and
  `VariantDecimal4`/`8`/`16` + `VariantDecimalType`
  (`src/parquet_variant/variant/decimal.cr`, `decimal_spec.cr`), the metadata
  builders `MetadataBuilder`/`WritableMetadataBuilder`/`ReadOnlyMetadataBuilder`
  (`src/parquet_variant/builder/metadata.cr`, `builder_metadata_spec.cr`), the
  value layer read side `Variant` (`src/parquet_variant/variant.cr`),
  `VariantObject` (`src/parquet_variant/variant/object.cr`), `VariantList`
  (`src/parquet_variant/variant/list.cr`), and the Rust-compatible `Debug`
  formatting (`src/parquet_variant/variant/debug.cr`), and the value builder
  `ValueBuilder`/`ParentState`/`BuilderSpecificState`
  (`src/parquet_variant/builder/state.cr`), `VariantBuilder`
  (`src/parquet_variant/builder/variant_builder.cr`), `ObjectBuilder`
  (`src/parquet_variant/builder/object_builder.cr`), and `ListBuilder`
  (`src/parquet_variant/builder/list_builder.cr`) with full specs
  (`spec/parquet_variant/variant_*_spec.cr`, `builder_*_spec.cr`) done. All core
  `parquet-variant` upstream tests are ported (including the builder rollback
  tests via `abandon` and the exact `Debug`/`HexString`/`InvalidVariant`
  formatting test).
  `VariantMetadata#borrowed_field_id`'s zero-copy fast path, the raw-pointer
  zero-copy `Variant` byte/model APIs, Rust `Drop`-based rollback, and
  `VariantBuilderExt`/`ObjectFieldBuilder` are intentional divergences (Crystal
  copies substrings; explicit `abandon`; builder handles all types directly).
- [ ] `parquet-variant-compute`.
- [ ] `parquet-variant-json`.

### 3.2 Geospatial

- [~] GeoParquet GEOMETRY/GEOGRAPHY logical types + bounding statistics:
  `Hint`/`Edges`/`Metadata` + the metadata side of `WkbType`
  (`src/parquet_geospatial/types.cr`, `types_spec.cr`),
  `IntervalTrait`/`Interval`/`WraparoundInterval`
  (`src/parquet_geospatial/interval.cr`, `interval_spec.cr`), `GeometryBounder`
  (`src/parquet_geospatial/bounding.cr`, `bounding_spec.cr`) with a minimal
  `WkbReader`/`WkbGeometry` model (`src/parquet_geospatial/wkb.cr`) and the
  `Testing` WKB helpers (`src/parquet_geospatial/testing.cr`, `testing_spec.cr`)
  done; the `WkbType` `ExtensionType` impl (arrow-schema `Field`/`DataType`)
  pending. Divergence: upstream `bounding.rs` uses `geo-traits`/`wkb`; the local
  reader supports only the ISO 2D/Z/M/ZM type codes.

### 3.3 Derive macros

- [x] `parquet_derive` (`src/parquet/derive.cr`, `spec/parquet/derive_spec.cr`):
  the `include Parquet::Derive::Record` macro generates nested `T::Writer`/
  `T::Reader` classes from the record's `instance_vars`, and `Codec(T)` maps
  `Bool`/`Int8..64`/`UInt8..64`/`Float32/64`/`String`/`Bytes`/`UUID`/`Time`/
  `Parquet::Derive::Date` (and their nilable forms) to schema, writer, and reader
  code. Ported `parquet_derive_test` tests: schema generation, read/write
  round-trip, optional-but-valid columns, pruned/shuffled columns. See the
  divergence note for the `&[T]`/`Vec<T>` -> `T::Writer`/`T::Reader` mapping and
  `syn`-AST replacement.

### 3.4 Encryption

- [~] Modular encryption: done — the cipher layer
  (`src/parquet/encryption/ciphers.cr`, `spec/parquet/encryption/ciphers_spec.cr`)
  (AES-GCM `GcmBlockEncryptor`/`GcmBlockDecryptor`/`CounterNonce` on a minimal
  AAD-capable `AesGcm` over OpenSSL EVP; the `ring` AAD block is resolved) and
  the encryption Thrift IDL (`src/parquet/file/column_crypto_metadata.cr`:
  `EncryptionWithColumnKey`/`ColumnCryptoMetaData`;
  `src/parquet/file/metadata/thrift/encryption.cr`: `AesGcmV1`/`AesGcmCtrV1`/
  `EncryptionAlgorithm`/`FileCryptoMetaData`, with round-trip specs) and the
  AAD builders (`src/parquet/encryption/modules.cr`: `ModuleType`,
  `create_footer_aad`/`create_module_aad`, `modules_spec.cr`), the encryption
  properties (`src/parquet/encryption/encrypt.cr`: `FileEncryptionProperties`/
  `EncryptionPropertiesBuilder`/`FileEncryptor` + thrift-encryption helpers,
  `encrypt_spec.cr`), and the decryption properties
  (`src/parquet/encryption/decrypt.cr`: `KeyRetriever`/`FileDecryptionProperties`/
  `DecryptionPropertiesBuilder[WithRetriever]`/`FileDecryptor`/`CryptoContext`/
  `read_and_decrypt`, `decrypt_spec.cr`), the `ColumnChunkMetaData` crypto
  wiring (`src/parquet/file/metadata/column_chunk.cr`: fields 8/9 +
  `plaintext_footer_mode`, `column_chunk_spec.cr`), and the encrypted-footer
  reader (`src/parquet/file/metadata/thrift/encryption.cr`:
  `get_file_decryptor`/`row_group_from_encrypted_thrift`/
  `parquet_metadata_with_encryption` + `FileMetaData` fields 8/9,
  `encryption_reader_spec.cr`), and the encryption-aware metadata writer
  (`src/parquet/file/metadata/writer.cr`: `MetadataObjectWriter` + wiring,
  `writer_encryption_spec.cr`), and the public writer API
  (`WriterProperties#file_encryption_properties`, the column writer attaching
  `column_crypto_metadata`, `SerializedFileWriter#file_encryptor`,
  `spec/parquet/file/writer_encryption_spec.cr`), and reader metadata
  decryption (`ParquetMetaDataReader`/`ParquetMetaDataPushDecoder`
  `with_file_decryption_properties` + `SerializedFileReader` read options,
  `spec/parquet/file/reader_encryption_spec.cr`), and page-data encryption on
  write (`src/parquet/column/page_encryption.cr`: `PageEncryptor` +
  `SerializedPageWriter`/`SerializedRowGroupWriter` wiring,
  `spec/parquet/column/page_encryption_spec.cr`), and page-data decryption on
  read (`ParquetMetaData#file_decryptor` + `SerializedPageReader` page crypto
  context/header/data handling, `spec/parquet/file/page_decryption_spec.cr`).
  `AES_GCM_CTR_V1` is NYI upstream as well.

### 3.7 Core geospatial module

- [x] `parquet/src/geospatial/` — `bounding_box.cr` (`BoundingBox` x/y[/z/m]
      min/max + `new`/`with_*range`/`is_*_valid` accessors), `statistics.cr`
      (`GeospatialStatistics`), `accumulator.cr`
      (`GeoStatsAccumulator`/`GeoStatsAccumulatorFactory`,
      `init_geo_stats_accumulator_factory`/`try_new_geo_stats_accumulator` with a
      module-level global factory, `DefaultGeoStatsAccumulatorFactory`,
      `VoidGeoStatsAccumulator`, `ParquetGeoStatsAccumulator` built on
      `ParquetGeospatial::GeometryBounder`), and `mod.rs`
      (`src/parquet/geospatial.cr`). Wired through
      `ColumnChunkMetaData#geo_statistics` (+ builder `set_geo_statistics`, thrift
      field 17 via the new `BoundingBox`/`GeospatialStatistics` thrift structs and
      `convert_geo_stats`/`convert_bounding_box`/`geo_stats_to_thrift`/
      `bounding_box_to_thrift`), `ColumnValueEncoderImpl`'s
      `geo_stats_accumulator` + `flush_geospatial_statistics` (geospatial columns
      skip min/max; `update_geo_stats_accumulator` feeds `AsBytes` values),
      `GenericColumnWriter#build_column_metadata`, and
      `SerializedFileWriter#finish_appended_column`. Tests:
      `spec/parquet/geospatial/bounding_box_spec.cr` (upstream
      `test_bounding_box`), `accumulator_spec.cr` (upstream
      `test_void_accumulator`, `test_default_accumulator_geospatial_factory`,
      `test_geometry_accumulator`), and `geo_statistics_spec.cr` (write/read bbox +
      geometry types on a GEOMETRY column, Z/M round-trip, and non-geospatial
      columns unaffected). This is distinct from the completed `parquet-geospatial`
      crate (3.2) and reuses its `GeometryBounder`/`Interval`/WKB model.
      Intentional divergence: the `HeapSize` impls are dropped; the accumulator's
      `is_valid` keeps the upstream name.
      Bugs fixed along the way (upstream already correct): the thrift
      double read/write helpers used non-existent `Slice#read_bytes`/
      `#write_bytes` (never instantiated before), and
      `serialize_column_meta_data` did not advance `last_field_id` after the
      `SizeStatistics` field (mismatched delta once field 17 followed).

### 3.5 Arrow interop

- [~] `parquet/src/arrow/` — now unblocked via the sibling `../arrow-cr` port
      (`path: ../arrow-cr` dependency). Done:
  - [x] `arrow/schema/mod.rs` Arrow → Parquet direction:
        `ArrowSchemaConverter` (+ `with_coerce_types`, `schema_root`),
        `arrow_to_parquet_type`, `field_id`, `decimal_length_from_precision`,
        and the `PARQUET_*` naming constants (`src/parquet/arrow/schema.cr`,
        `spec/parquet/arrow/schema_spec.cr`).
  - [x] `arrow/mod.rs` `ProjectionMask` (`all`, `none`, `leaves`, `roots`,
        `columns`, `leaf_included?`, `union`, `intersect`) and `parquet_column`
        (`src/parquet/arrow/mod.cr`, `spec/parquet/arrow/projection_spec.cr`);
        `without_nested_types` is a follow-up (used by `push_decoder`/selection).
  - [x] `arrow/schema/primitive.rs` `convert_primitive`/`apply_hint`/
        `from_parquet` (`src/parquet/arrow/schema/primitive.cr`,
        `spec/parquet/arrow/primitive_spec.cr`).
  - [x] `arrow/schema/mod.rs` schema metadata: `encode_arrow_schema`,
        `get_arrow_schema_from_metadata`, `add_encoded_arrow_schema_to_metadata`
        (+ `flatten_ree_field`) over arrow-cr's `Arrow::Ipc.schema_message`/
        `read_message`; `ARROW_SCHEMA_META_KEY`/`PARQUET_FIELD_ID_META_KEY`.
  - [x] `arrow/schema` Parquet → Arrow direction:
        `ParquetField`/`ParquetFieldType`/`VisitorContext`/`Visitor`
        (`src/parquet/arrow/schema/complex.cr`), `convert_type`/`convert_schema`/
        `convert_field`, `parquet_to_arrow_schema[_by_columns]`/
        `parquet_to_arrow_schema_and_fields`, `parquet_to_arrow_field`,
        `FieldLevels`/`parquet_to_arrow_field_levels`, and
        `parse_key_value_metadata` (`src/parquet/arrow/schema.cr`).  Remaining in
        this area: `parquet_to_arrow_field_levels_with_virtual` + virtual
        columns (`VirtualColumnType`/`convert_virtual_field`, needs arrow-cr
        canonical extension types), `ProjectionMask::without_nested_types`, and
        the upstream `test_parquet_list_nullable`/`test_nested_schema_partial*`
        plus the remaining `complex.rs` hint tests.
  - [~] `arrow/record_reader/` + `arrow/array_reader/` leaf readers:
        `GenericRecordReader`, `ArrayReader`/`RowGroups`/`EmptyArrayReader`,
        `PrimitiveArrayReader` (numeric + BOOLEAN), `ByteArrayReader`
        (Utf8/LargeUtf8/Binary/LargeBinary) and `ByteViewArrayReader`
        (Utf8View/BinaryView) are done, with the `OffsetBuffer`/`ViewBuffer`
        values buffers, `DefinitionLevelBuffer` and the `DictIndexDecoder`/
        `DeltaByteArrayDecoder`. Composite readers:
        `StructArrayReader` (`src/parquet/arrow/array_reader/struct_array.cr`,
        level-derived struct null bitmap, children kept in sync) and
        `ListArrayReader(O)` (`src/parquet/arrow/array_reader/list_array.cr`,
        List/LargeList offsets + validity rebuilt from the item reader's
        levels, nested lists and `parent_threshold`), and `MapArrayReader`
        (`src/parquet/arrow/array_reader/map_array.cr`, a list reader over a
        two-field struct reader, output reinterpreted as a `MapArray`), and
        `ListViewArrayReader(O)` (`src/parquet/arrow/array_reader/
        list_view_array.cr`, a list reader whose output is converted to
        offsets+sizes), and `FixedSizeListArrayReader`
        (`src/parquet/arrow/array_reader/fixed_size_list_array.cr`, compact
        child + `MutableArrayData` null blocks, per-row alignment checks) and
        the `ArrayReaderBuilder` (`src/parquet/arrow/array_reader/builder.cr`,
        field-tree dispatch with projection mask + padding-threshold
        propagation and the empty-reader fallback), and the batch reader
        (`src/parquet/arrow/arrow_reader.cr`: `ArrowReaderMetadata`,
        `ParquetRecordBatchReaderBuilder`, `ParquetRecordBatchReader`) and row
        selection (`src/parquet/arrow/row_selection.cr`: `RowSelector` +
        `RowSelection` over the RLE and bitmap backings, with consecutive
        ranges, `from_filters`, offset/limit/trim/split_off, the set algebra
        and the row counts), plus the read plan
        (`src/parquet/arrow/read_plan.cr`: `ReadPlan`,
        `ReadPlanBuilder`/`LimitedReadPlanBuilder`, and the
        `RowSelectionCursor` all/selectors cursors), `NullArrayReader`
        (`src/parquet/arrow/array_reader/null_array.cr`) and
        `FixedLenByteArrayReader`
        (`src/parquet/arrow/array_reader/fixed_len_byte_array.cr`). Remaining:
        `ByteArrayDictionaryReader` (arrow-cr `DictionaryArray` exists, so only
        the reader is missing), the byte-array/value decoders
        (`ByteArrayColumnValueDecoder`/`ByteViewArrayColumnValueDecoder`), the
        `Float16`/`MonthDayNano` fixed-length byte array targets, and the
        placeholder/cached/virtual readers.
- [ ] `arrow/array_reader/`, `arrow/record_reader/`, `arrow/levels.rs`
      (parquet pages → Arrow arrays).
- [~] `arrow/arrow_reader/`: the batch core is ported
      (`src/parquet/arrow/arrow_reader.cr` — `ArrowReaderMetadata.load`/
      `try_new` with `skip_arrow_metadata`, `ParquetRecordBatchReaderBuilder`
      (`try_new`, `new_with_metadata`, `with_batch_size`, `with_projection`,
      `metadata`/`schema`/`parquet_schema`, `build`), and
      `ParquetRecordBatchReader` (`try_new`, `next_batch`, `Iterator`,
      `schema`) reading every row of every row group, plus `RowSelector`/
      `RowSelection` over both backings (`selection/selector.rs`,
      `selection/boolean.rs`, `selection/algebra.rs`) and the offset/limit read
      plan (`src/parquet/arrow/read_plan.cr`, wired through
      `with_row_selection`/`with_offset`/`with_limit`) and the row filter
      (`src/parquet/arrow/filter.cr` + `ReadPlanBuilder#with_predicate` +
      `with_row_filter`, see `spec/parquet/arrow/row_filter_spec.cr`).
      Remaining: the mask cursor/`RowSelectionPolicy` (`selection/cursor.rs`,
      `loaded_row_ranges`), page pruning (`selection/ranges.rs`), row-group
      selection, `statistics.rs`, `metrics.rs`, and `arrow/push_decoder/`.
- [ ] `arrow/arrow_writer/` (`ArrowWriter`, `levels.rs`, `byte_array.rs`),
      `arrow/mod.rs` (`ProjectionMask`, `ARROW_SCHEMA_META_KEY` plumbing,
      `apply_schema`), and `arrow/buffer/`.
- [ ] `arrow/schema/primitive.rs`, `arrow/schema/virtual_type.rs`, and the
      extension-type mapping (`arrow/schema/extension.rs`; needs arrow-cr
      canonical extension types — optional, off in upstream's default build).
- [ ] The arrow-only CLI tools (`parquet-rewrite`, `parquet-fromcsv`) once the
      reader/writer exist, and `column/page_store.rs` (only consumed by the
      arrow writer).

### 3.6 Async / object_store (deferred)

- [ ] `async_reader`, `async_writer`, `object_store` integration. Rust uses
  tokio/futures; Crystal has fibers. Likely an intentional divergence — record
  the mapping decision before porting.

## Intentional divergences

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
  `VariantBuilderExt` trait and its `ObjectFieldBuilder` wrapper (only used by
  `parquet-variant-compute`) are not ported; the builders expose the equivalent
  inherent methods, so `append_null` maps to `append_value(Variant.from_nil)`.
  The `try_*` builder methods raise instead of returning `Result`.
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
  their upstream names, and the `HeapSize` impls are dropped. The domain types are
  separate from the thrift structs, converted by
  `convert_geo_stats`/`convert_bounding_box` (read) and
  `geo_stats_to_thrift`/`bounding_box_to_thrift` (write) in place of the upstream
  `WriteThrift` impls on the domain types.
- Content-defined chunking: `ContentDefinedChunker.new` raises `GeneralError`
  (upstream returns `Result<Self>`), `roll_fixed` takes `Bytes` instead of a
  const-generic `&[u8; N]` (same byte loop), `usize` offsets are `Int32`, the
  `FnMut(&mut Self, usize)` callback is a block receiving the chunker, chunk
  validation runs under `{% unless flag?(:release) %}` (upstream
  `debug_assertions`), and assertions raise `GeneralError`. `get_arrow_chunks`
  and the `arrow_tests` roundtrip module are deferred with the arrow layer; the
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
  Arrow *extension types* are not mapped (upstream compiles that out unless
  `arrow_canonical_extension_types`/`geospatial`/`variant_experimental` are on,
  and arrow-cr does not model them): only `Utf8` → `STRING` is set from the
  `logical_type_for_*` helpers. arrow-cr's `Field#with_*` builders currently
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
  in-crate) and `leaf_included` → `leaf_included?`; `convert_primitive`'s
  `apply_hint`/`from_parquet` are private module methods, and the physical/logical
  dispatch is a `case` over `LogicalType` subclasses plus the `ConvertedType`
  fallback with `arrow_err!`/`general_err!` where upstream has `arrow_err!`/
  `General`. `parquet_column` returns `Tuple(Int32, Arrow::Field)?` (upstream
  `Option<(usize, &FieldRef)>`).
- Arrow interop, leaf array readers (`byte_array.rs`/`byte_view_array.rs`/
  `byte_array_dictionary.rs`): upstream decodes straight into the arrow values
  buffer with dedicated decoders (`ByteArrayColumnValueDecoder`,
  `ByteViewArrayColumnValueDecoder`, the `ByteArrayDecoder*`/
  `ByteViewArrayDecoder*` families and `ByteArrayDecoderPlain` in the
  dictionary reader). The port's `GenericRecordReader(V, CV)`/
  `ColumnReaderImpl(B, CV)` *are* generic over both the values buffer and the
  value decoder (like upstream `GenericRecordReader<V, CV>`), so
  `ByteArrayDictionaryReader` drives them with
  `(DictionaryBuffer(K, V), DictionaryDecoder(K, V))`. What is still missing is
  the byte-array value decoders themselves: `ByteArrayReader`,
  `ByteViewArrayReader` and the dictionary reader's fallback path decode through
  `ColumnValueDecoderImpl(ByteArray)` and convert at `consume_batch` time
  (`ByteArrayReader` copies values into `OffsetBuffer(I)`/arrow byte arrays,
  `ByteViewArrayReader` builds `Utf8View`/`BinaryView` through arrow-cr's view
  builders, the dictionary fallback pushes into the record reader's
  `OffsetBuffer`). Behaviour matches upstream (values, null padding,
  dictionary/delta encodings); only batch-time allocation differs. The
  byte-array/view value decoders therefore stay `missing` in the ledger, and
  `ViewBuffer` (`append_block`/`into_array`/`pad_nulls`) exists but is not yet
  driven by a decoder. Because `ColumnValueDecoderImpl`/`PlainDecoder` never
  reads past the values a page claims, an *empty* value payload is
  short-circuited to zero values (upstream's decoders simply stop at the end of
  the page buffer); a truncated non-empty payload still raises. Null padding
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
- Arrow interop, array reader builder: `ArrayReaderBuilder` drops the reader
  metrics, cache options (`CachedArrayReader`/`RowGroupCache`) and virtual
  columns (`ParquetFieldType` only models primitive/group fields in this port),
  and the per-field `ReaderArgs` struct is replaced by explicit
  `(field, mask, padding_threshold)` parameters. `INT96` (the only leaf reader
  not ported, because the numeric primitive reader does not model it) raises
  `NYIError`.
- Arrow interop, null columns: `NullArrayReader` follows upstream in fixing the
  record reader to INT32 (a Null column carries no non-null values); skipping an
  all-null column is not supported because the shared skip path treats each
  skipped level as a value (see the skipping note above).
- Arrow interop, fixed-length byte arrays: the reader decodes through
  `ColumnValueDecoderImpl(FixedLenByteArray)` and converts at `consume_batch`
  time, so decimals/intervals are `PrimitiveArray(T)` values carrying the
  matching decimal/interval data type (arrow-cr has no `Decimal*Array`/
  `Interval*Array` classes); `Float16` raises `NYIError` (no Crystal `Float16`)
  and `Interval(MonthDayNano)` is NYI as upstream.
- Arrow interop, batch reader: `ParquetRecordBatchReaderBuilder` takes a
  `File::FileReader` (upstream any `ChunkReader`) and reads **all** row groups
  (`with_row_groups`/`RowGroupSelection` is not modelled); row selection,
  offset/limit plans and `RowFilter` are ported, while `statistics.rs`,
  `metrics.rs`, bloom filters, `RowSelectionPolicy`/the mask cursor, page
  pruning and page-index policies are not. `ArrowReaderOptions`
  models only `skip_arrow_metadata` and the supplied arrow schema (no
  page-index/encoding-statistics policies, decryption properties or virtual
  columns), and the supplied-schema field-metadata mismatch message renders
  Crystal's `Hash?` inspection where upstream uses Rust's `Debug` formatting.
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
  `panic!`s are `GeneralError`s with the same messages; `scan_ranges`/
  `row_ranges_for_selected_pages` (page pruning, `selection/ranges.rs`) and
  `expand_to_batch_boundaries`/`auto_selection_strategy` (the mask cursor) are
  still deferred; `from_consecutive_ranges` raises
  `GeneralError("out of order")` where upstream panics.
- Arrow interop, read plan: the plan is always selector-backed (the bitmap
  backing exists, but the `RowSelectionPolicy`/mask cursor is not ported), has
  no loaded-row-ranges/page pruning and no row-group selection. `ReadPlan`
  exposes its cursor directly instead of `row_selection_cursor_mut`, and
  `usize` counts/offsets/limits are `Int32`. `ReadPlanBuilder#dup` is a shallow
  copy (`build` never mutates the selection), and `BooleanArray#take_n_true`
  lives in `read_plan.cr` because arrow-cr does not provide it.
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
  `HeapSize` bounds are dropped; `RingGcmBlock{Encryptor,Decryptor}` are
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
  declared Crystal `enum` and `define_thrift_struct` annotates a `struct` block,
  synthesizing `read_thrift`, `write_thrift`, and `thrift_element_type` with
  identical wire behavior. Upstream's generated `VARIANTS`/`MAX_DISCRIMINANT`
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
  `complete?`). `PageIndex: HeapSize` (`memory.rs`) is deferred.
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
  `memory_size` (`HeapSize`) is deferred.
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
  constructors (upstream `pub(crate)`) are public; encryption
  (`with_file_decryption_properties`) is not ported, and an encrypted footer
  raises as upstream does without the feature. `MetadataParser` threads
  `metadata_options` through but statistics-skipping/schema overrides are not
  applied yet.
- `ChunkReader`: Rust's `Length`/`ChunkReader` traits are one abstract class
  (`len`/`get_bytes`/`get_read`); the associated `T: Read` becomes `IO`, and
  `Bytes`/`File` are wrapped as `BytesChunkReader`/`FileChunkReader`.
- `ParquetMetaDataReader`: the async `MetadataFetch`/`MetadataSuffixFetch`
  loaders (`load_*`, `try_load*`, `with_prefetch_hint`, `get_prefetch_size`) and
  the encryption `with_decryption_properties` are not ported; `has_metadata` →
  `has_metadata?`, `parse_metadata` is public (upstream private), `metadata_size`
  returns `Int32?`, and `decode_schema` is implemented via a full metadata
  decode.
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
  native-byte `gen_as_bytes!`); `DataType`/`SliceAsBytes` for primitives,
  `str`, and `Vec<u8>` remain pending. `dict_encoding_size` /
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
  encryptor), the `Debug` impl, the arrow-only `append_column_from_pages`, and
  the `parquet_buf`/`HeapSize` helpers are not ported; `start_file` is public.
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
  `parquet-rewrite`/`parquet-fromcsv` need `arrow` and are deferred.
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
  `arrow_schema::ExtensionType` impl (`NAME`, `supports_data_type`, `try_new`,
  `Field` registration) is deferred to the Arrow interop chunk.
- `parquet-geospatial::interval`: `IntervalTrait` is a marker module;
  `Interval`/`WraparoundInterval` are value structs with explicit `==` and
  `Debug`→`inspect`; `TryFrom<WraparoundInterval>` is `Interval.from_wraparound`
  (raising); `is_wraparound`/`is_empty` are `wraparound?`/`empty?`; NaN-ignoring
  `f64::min`/`max` are explicit helpers (Crystal's propagate NaN);
  `WraparoundInterval#split` is `protected`; `From<(f64,f64)>`/`From<Interval>`
  are `WraparoundInterval.new(lo, hi)`/`.from_interval`.
- `chunk_reader`: `BytesChunkReader#get_read`/`get_bytes` and
  `FileChunkReader#get_bytes` raise the upstream `EOF` messages
  ("Expected to read ... while file has length ..."); `FileChunkReader` reads
  through `File#read_at` blocks (Crystal's `read_at` yields an `IO`).
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
  becomes `Int32`; `encodings` is a `Set(Encoding)`; the writer's geospatial,
  encryption, and arrow-only paths (`get_estimated_total_bytes`, `memory_size`,
  `set_distinct_count_override`, `set_column_chunk_encryption_properties`) are
  not ported. `Naming/AccessorMethodName`, `Lint/NotNil`,
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
- `ParquetMetaDataWriter`/`ThriftMetadataWriter`: the `MetadataObjectWriter`
  encryption-dispatch layer is not modeled; the non-encryption write logic is
  inlined (`write_thrift_object`, `Metadata.write_file_metadata`), and the
  `TrackedWrite`/`file_magic` helpers live in `src/parquet/file/`. `TrackedWrite`
  exposes only `bytes_written`/`write` (no `inner*`/`into_inner`/`flush`/
  `write_vectored`; `write_all` comes from `IO`). Row groups are shallow-copied,
  so writing sets index offsets on the source `ColumnChunkMetaData` objects;
  magic is always `PAR1` (encrypted-footer magic is not produced).

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
