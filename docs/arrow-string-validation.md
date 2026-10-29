# Arrow string decoding

Arrow readers reject malformed UTF-8 in Parquet STRING/UTF8 columns for plain,
dictionary, DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY encodings. Offset and
view readers validate read values during `read_records`; dictionary entries are
validated when loaded, including unused entries. Lower-level binary targets do
not bypass a STRING annotation. File-level schema compatibility rules still
reject STRING-to-binary overrides.

Unannotated binary columns preserve arbitrary bytes. Supplying a string schema
requires valid UTF-8 when the array is materialized, including dictionary string
and view targets. Validation checks each value independently: two truncated
codepoints cannot become a valid string by concatenation across offsets.
Skipped non-dictionary values are not UTF-8 validated, matching upstream.

The contract comes from apache/arrow-rs 60.0.0,
`ef1fa157977633f0ba21aa921f9ef3d5c669235b`, specifically:

- `parquet/tests/arrow_reader/invalid_utf8.rs`: all six malformed-value cases,
  small/large strings and views, dictionary/plain/both delta encodings, and the
  original inline malformed-file bytes.
- `parquet/src/arrow/arrow_reader/mod.rs::test_read_non_utf8_binary_as_utf8`.
- `parquet/src/arrow/array_reader/{byte_array,byte_view_array}.rs`: annotation
  selection, validation on dictionary load/read, and no validation on skip.

`spec/parquet/arrow/string_validation_spec.cr` writes the same bytes through
the low-level physical writer instead of constructing invalid Arrow arrays with
Rust's unsafe constructors. It also covers nulls, valid multibyte/empty/long
strings, batch boundaries, binary preservation, binary targets and unused
dictionary entries.

## Representation differences

The native column value decoders implement annotation-driven UTF-8 validation
for offset and view readers. Page decoding now uses the Arrow-specific codecs
recorded in [Arrow byte-page decoding](arrow-byte-page-decoding.md), retaining
`ArrayValuesBuffer(ByteArray)` output and batch-time Arrow array construction.
Dictionary entries are decoded once into a native array. Validation is per value
using Crystal's `String#valid_encoding?`, rather than upstream's batched/SIMD
validation. No throughput or zero-copy parity is claimed.

Errors use `Parquet::GeneralError` and the prefix `encountered non UTF-8 data`.
Crystal supplies a validity predicate rather than Rust's failing byte position
and error length; diagnostics use `invalid utf-8 sequence` without those details.
Unannotated binary-to-string conversion uses the same native Parquet error,
where upstream may return an Arrow cast error.

The column-value adapters and encoding-specific families record native buffer
and view representation differences in the ledger. Their page-state/error
contract and remaining malformed-input limits are described in
[Arrow byte-page decoding](arrow-byte-page-decoding.md). No dependency or shard
code changes are required.
