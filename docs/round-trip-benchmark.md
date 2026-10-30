# Round-trip benchmark matrix

Build once, then measure the complete small matrix:

```sh
crystal build --release bench/parquet_round_trip.cr -o /private/tmp/parquet-round-trip
/private/tmp/parquet-round-trip --smoke --samples=3
```

Without `--smoke`, dimensions match the pin: 10 groups × 10,000 rows, 2,000 rows
per page, 10 numeric columns or 5 byte columns, uncompressed. All 46 source
case names, ordering and encoding choices are retained: Int32/Int64, Float32/64,
strings/string views and binary at widths 20/100, fixed bytes at widths 2/16,
with dictionary/plain/delta/byte-stream-split combinations applicable to each.
The generator reuses a batch of min(rows/group,100) rows, seeds columns with
31 × columns + column index, and uses null density 0.0001. String/binary lengths
are in [max/2,max), with alphanumeric strings and arbitrary binary bytes.

The smoke matrix uses two columns, two groups, 32 rows/group and 16 rows/page:
64 rows per file, one sequential worker, 46 cases and 92 write/read results.
Use `--case=TEXT`, `--columns=N`, `--groups=N`, `--rows=N`, `--page-rows=N` and
`--samples=N` to control size and scope. Numeric cases use plain as the comparison
baseline, with dictionary, delta or byte-stream-split as applicable. The complete
matrix supplies the corresponding byte-column comparisons as well.

CSV reports the first measured sample separately (`cold_seconds`), the average
of N subsequent samples (`warm_seconds`), rows/second from that warm mean, and
file size. The first measured sample follows untimed fixture preparation and
exact-value validation: it is not process-start or cold-cache latency. Three
warm samples are the default. Run the executable at least three times for
comparisons and retain first-sample values rather than mixing them into means.
No background IO or parallel work is launched. Timing uses `Time.instant`.

Write measurements include generating one seed batch, encoding repeated batches,
closing the footer and checking row/group counts, as source file_from_spec does.
Read measurements construct the reader, iterate/discard batches and assert the
row count. Full schema/value validation runs outside measurements, including
across reader/seed-batch boundaries. Read input is snapshotted outside timing;
the write buffer is reused. These are in-memory paths, not disk IO benchmarks.

Intentional differences from apache/arrow-rs 60.0.0
(`ef1fa157977633f0ba21aa921f9ef3d5c669235b`),
`parquet/benches/parquet_round_trip.rs`:

- Crystal's deterministic RNG replaces Rust StdRng; seed derivation, dimensions,
  null density and value domains match, but random bytes are not identical.
- CSV first/warm sampling replaces Criterion's statistical machinery and black
  box implementation. Native read/write costs include their allocation/runtime
  behavior. No Rust-versus-Crystal performance equivalence is claimed.
- The pinned timed writer appends files to one buffer across iterations. The
  port clears the buffer for every write to keep per-iteration output and
  allocation bounded to one generated file; a regression spec verifies stable bytes/size on repeated writes.
- Positive dimensions are required. Rows/group above 100 must be a multiple of
  100, preventing the source loop's partial-batch overshoot; invalid settings
  raise ArgumentError rather than hanging or failing shape assertions later.
- Counts use Int32, specs are copyable native structs with public fields, and
  file/row/value assertion failures raise GeneralError. Returned file bytes
  borrow the reusable buffer until its next write.

Seven tests-first characterizations cover defaults/immutable updates, the full
matrix, seeded nullable generation for every type, all 46 file round trips,
reused-buffer reset, dimension guards and measurement output. Initial red was
the absent benchmark API. Bench executables are included in format/lint gates.

## Verification measurement

After the gates finished, three sequential processes ran:

```sh
/private/tmp/parquet-round-trip --case=int32 --samples=3
```

Environment: Darwin arm64, Crystal 1.22.1, LLVM 23.1.4, release build; one
sequential worker. Each case writes 100,000 rows, 10 columns, 10 row groups,
2,000-row page limits. No other agent test/compiler job ran during these three
processes. First values below average the three first-measured samples; warm
values average nine samples (three per process). Plain is the named baseline.

| Int32 case | File bytes | Write first / warm ms | Read first / warm ms |
| --- | ---: | ---: | ---: |
| int32 plain | 4,041,531 | 99.169 / 95.065 | 38.824 / 44.493 |
| int32 dict | 967,799 | 149.802 / 153.835 | 24.941 / 25.723 |
| int32 delta_binary | 4,147,311 | 102.340 / 92.827 | 37.747 / 40.056 |
| int32 byte_stream_split | 4,041,531 | 96.495 / 96.357 | 45.003 / 46.148 |

On this deliberately repeated-100-row corpus, dictionary encoding traded slower
writes for smaller output and lower read time than plain. This verifies that
the harness measures a real encoding tradeoff; it does not establish a general
encoding recommendation or a Rust comparison. No engine optimization is made.
The next measurement should use application-like data and more samples before
choosing an optimization. Three full `--smoke --samples=3` runs also completed
all 46 cases (92 result rows per process), with exact values validated.
