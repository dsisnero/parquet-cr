# Arrow fixed-length byte decoding

The fixed-length Arrow reader uses a dedicated byte buffer and decoder for
PLAIN, RLE_DICTIONARY/PLAIN_DICTIONARY, DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT.
The contract comes from `parquet/src/arrow/array_reader/fixed_len_byte_array.rs`
at apache/arrow-rs 60.0.0, `ef1fa157977633f0ba21aa921f9ef3d5c669235b`.

Delta reads reject values whose decoded width differs from the column width,
before binary, decimal, Float16 or interval conversion. Delta skips preserve
prefix state without validating skipped widths, matching the pin. Dictionary
installation checks the declared byte count; dictionary pages may be replaced.
Index reads/skips use the page's count bound despite padded RLE runs.

Plain and byte-stream-split reads/skips use the complete values in the payload,
not the level-count hint. Byte-stream-split reconstructs each value using the
full payload stride, including during partial reads after skips. Both codecs
ignore incomplete trailing bytes like upstream. Empty dictionary reads return
zero, while dictionary skips still advance the index stream. Whole-page skips
retain the column reader's existing bypass behavior.

`FixedLenByteArrayBuffer` holds contiguous bytes and records the maximum pending
value-count reservation until the first read establishes the byte width. Null
padding moves whole values backward into valid level positions, retaining prior
batches, and rejects inconsistent buffer lengths. Null-slot bytes are arbitrary
and are masked by the Arrow validity bitmap.

## Verification

- `fixed_byte_buffer_spec.cr` ports
  `test_pending_reservation_tracks_max_capacity` and characterizes byte padding
  at widths 2/4/8/32, prior-batch offsets, dictionary bounds and payload exhaustion.
- `fixed_byte_decoding_spec.cr` ports `test_decimal_list` with the exact
  Decimal256 precision/scale, offsets `[0,0,1,3,3,4,5,8]`, validity `0x57` and
  batch slices 3/3/1. It also covers malformed widths/dictionaries, byte-stream-
  split widths 1/2/3/4/8/16/32, nullable V1/V2 files, page/group boundaries and
  all-null row groups.
- The existing fixed-length reader specs retain their binary/decimal/Float16/
  interval conversion assertions. No upstream assertions were weakened.

The integration uses the existing Arrow schema, column and file readers, so the
same decoder is selected by synchronous, push and async batch-reading paths.

## Native representation

The buffer is `Array(UInt8)` rather than Rust `Vec<u8>`. Reservation maintains a
lower bound by replacing the array when needed; Crystal has no public reserve
method. Null padding uses one scalar copy loop at all widths, replacing Rust's
`VEC_CUTOFF` optimization. No allocation or SIMD/performance equivalence is
claimed. Counts/capacities are Int32 with checked arithmetic, so oversized hints
can overflow earlier than Rust's usize/saturating allocation path.

`ValueDecoder` maps to `FixedLenByteArrayValueDecoder`; the Rust decoder enum
becomes an encoding case with separate index/delta state. Native guards return
GeneralError for nonpositive widths, mismatched output-buffer widths, missing
dictionaries or out-of-range keys where the pin asserts, unwraps or slices.
Other explicitly returned upstream errors retain their class and message.

Batch conversion retains native `FixedSizeBinaryArray` and `PrimitiveArray(T)`
representations for decimals, half floats and intervals. Conversion adapters
materialize fixed-value views over the byte buffer. The supported types and
MonthDayNano NYI behavior are unchanged. No installed shard or dependency code
is modified.
