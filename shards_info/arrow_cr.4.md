# Resolved: Decimal256 dictionary packing and run-end comparison

Found by tests-first Variant numeric conversion integration against apache/
arrow-rs 60.0.0 (`ef1fa157977633f0ba21aa921f9ef3d5c669235b`). The path dependency
is `../arrow-cr`; this task makes no shard patch, per the user's instruction.

## Reproduce from parquet-cr

```sh
CRYSTAL_CACHE_DIR=/private/tmp/parquet-cr-crystal-cache crystal spec \
  spec/parquet_variant_compute/numeric_conversion_spec.cr \
  --example 'encodes Decimal256'
```

The tests extract `[12.34, 12.34, SQL null, 56.78]` into Decimal256(76, 2)
encoded arrays through public `variant_get`. Primitive conversion succeeds;
`EncodedRowBuilder#finish` delegates to `Arrow.cast_with_options`, matching the
pinned upstream builder. Both tests remain enabled with their original expected values.

## Dictionary packing — resolved by b05366a

Expected: dictionary values `[1234, 5678]` at Decimal256(76, 2), Int32 keys
`[0, 0, null, 1]`.

Before the independent Arrow fix:

```text
Cast error: Unsupported output type for dictionary packing: Decimal256(76, 2)
lib/arrow-cr/src/arrow/cast.cr: pack_numeric_to_dictionary
```

Arrow commit `b05366a` adds the missing Decimal256/I256 dictionary dispatcher
branch. The original public-kernel test now reaches its key/value assertions.
Its tuple-versus-array test-container mismatch was corrected to compare all
nullable keys `[0, 0, nil, 1]`; expected values were preserved.
Upstream `arrow-cast/src/cast/dictionary.rs` dispatches
`Decimal256` to `pack_decimal_to_dictionary::<K, Decimal256Type>`.

## Run-end encoding — resolved by 49d07ce

Expected: Int32 run ends `[2, 3, 4]`, Decimal256 values `[1234, null, 5678]`,
expanding to the original four rows.

Before the independent Arrow fix:

```text
Invalid argument error: The data type Decimal256(76, 2) has no natural order
lib/arrow-cr/src/arrow/ord.cr: make_comparator
lib/arrow-cr/src/arrow/partition.cr: partition_boundaries
lib/arrow-cr/src/arrow/cast.cr: cast_to_run_end_encoded_with_key
```

Arrow commit `49d07ce` adds `PrimitiveArray(I256)` dispatch to the primitive
comparator. Run-end casting can now partition Decimal256 values, matching
`arrow-ord/src/ord.rs` and `arrow-ord/src/partition.rs`. The public-kernel test
passes exact run ends, nullable physical values and expanded logical values.

With both independent Arrow fixes, all 28 numeric conversion specs pass without
skips. This task made no dependency edits. The resolved requirements are
`b05366a` (dictionary packing) and `49d07ce` (run-end comparison); the sibling
checkout at final verification is `eec0b46`, which includes both.

Final host gates pass: `make lint` (183 files), `make test` (1776 examples,
zero failures/errors), no-codegen library build, scoped inventory validation
(3515 tracked items), and `git diff --check`.
