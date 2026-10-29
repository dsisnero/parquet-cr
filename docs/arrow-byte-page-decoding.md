# Arrow variable-byte page decoding

Offset, view and dictionary-output Arrow readers use page-aware byte-array
codecs for PLAIN, RLE_DICTIONARY/PLAIN_DICTIONARY, DELTA_LENGTH_BYTE_ARRAY and
DELTA_BYTE_ARRAY. The contract is pinned to apache/arrow-rs 60.0.0,
`ef1fa157977633f0ba21aa921f9ef3d5c669235b`.

Dictionary index reads and skips stop at the page's known value count (or its
V1 level-count upper bound), including RLE padding. Empty dictionaries yield
zero values. Dictionary pages can be replaced independently of data-page
encoding changes; a missing dictionary is reported when read/skip needs it.
Delta codecs use embedded counts, preserve prefix state during skips and reject
negative delta lengths, insufficient delta-length payloads and inconsistent
prefix/suffix counts at installation. Truncated plain headers/payloads return
the pinned `EOF: eof decoding byte array` error. UTF-8 validation remains as
recorded in [Arrow string decoding](arrow-string-validation.md).

`spec/parquet/arrow/byte_page_decoding_spec.cr` ports upstream's
`test_byte_array_decoder`, `test_byte_array_decoder_skip` and
`test_byte_array_decoder_nulls`. View behavior is checked through the shared
native codec and reader factories. Source-derived characterizations cover
malformed payloads, dictionary state/errors, count bounds and prefix casts.
File acceptance tests read nullable V1/V2 files through small/large offsets,
views and dictionary targets across pages, row groups, all-null groups and
batch boundaries. Existing UTF-8 specs provide independent regression coverage.

## Pinned edge behavior

- Whole-page skips bypass value decoding. Partial skips install the codec and
  therefore perform constructor-time delta-length validation.
- PLAIN skips inspect length headers, without validating skipped UTF-8 or the
  skipped payload's end offset. Offset reads return zero on an exhausted
  payload; view reads require the requested values and report EOF.
- The pinned offset PLAIN decoder can report the requested count when corrupt
  metadata claims extra values and the payload ends at a complete value. This
  count/output mismatch is preserved and explicitly characterized; view mode
  reports EOF instead.
- Delta prefix lengths use Rust's `i32 as usize` followed by `Vec::truncate`:
  oversized and negative prefixes retain the previous bytes. Native code
  reproduces this behavior rather than interpreting negative range endpoints
  relative to the array. A standalone Rust oracle confirmed the cast/truncation.

## Native representation

`ByteArrayDecoder.for_encoding` replaces the enum constructor, dispatching to
plain/dictionary/delta-length/delta classes. `ByteArrayValueDecoder` and
`ByteViewArrayValueDecoder` own dictionary/page state and adapt these codecs to
`ArrayValuesBuffer(ByteArray)`. View mode shares codec implementations while
selecting the pin's stricter plain read and view dictionary error messages.

Upstream writes directly into `OffsetBuffer`/`ViewBuffer` and reuses view page
blocks. Native code builds Arrow arrays at batch consumption; no zero-copy,
SIMD or allocation/performance equivalence is claimed. Counts are Int32 and
plain offsets/length arithmetic use Int64. After a corrupt skip advances past
payload end, native offset reads return zero instead of Rust debug underflow.
General Rust panic/unchecked malformed-input behavior is not an error-contract
guarantee; the explicitly returned upstream errors above are characterized.

The 31 encoding-family ledger rows record native buffer/view divergence rather
than remaining missing APIs. Fixed-length byte arrays retain their separate
physical-width/byte-stream-split contract and are outside this feature. No
installed shard or dependency code is changed.
