# arrow-cr needs: `BooleanArray#take_n_true` (Arrow row filter chunk)

Requested by `parquet-cr` while porting `RowFilter`/`ReadPlanBuilder::with_predicate`
(apache/arrow-rs `60.0.0`, `ef1fa157977633f0ba21aa921f9ef3d5c669235b`).

## 1. `arrow-array`: `BooleanArray::take_n_true`

`arrow-array/src/array/boolean_array.rs` has:

```rust
/// Returns a new `BooleanArray` of the same length where only the first `n`
/// non-null `true` positions remain `true`; any `true` positions beyond the
/// first `n` are replaced with `false`. The null buffer is preserved unchanged.
/// If this array has at most `n` non-null `true` values, `self` is returned
/// unchanged.
pub fn take_n_true(self, n: usize) -> BooleanArray
```

It backs `PredicateOptions::with_limit` in `parquet`'s read plan (the
`LIMIT`/TopK early-termination path): a predicate batch is truncated at the
`n`-th match so the remaining rows are never decoded.

**Needed:** `Arrow::BooleanArray#take_n_true(n : Int32) : BooleanArray` (or a
module function such as `Arrow.take_n_true(filter, n)`), counting only non-null
trues and preserving the null buffer. Upstream also asserts the semantics in
`arrow-array`'s tests (`test_take_n_true_keeps_first_n_matches`).

`parquet-cr` works around it with a private per-bit implementation in
`src/parquet/arrow/read_plan.cr` (`ReadPlanBuilder#take_n_true`) and exercises
it in `spec/parquet/arrow/row_filter_spec.cr` ("stops at the match limit and
pads the tail"). The workaround can be deleted once arrow-cr provides the
method; behaviour (including the `n == 0` and "fewer than `n` trues" cases) has
been checked against the upstream loops.

No other gaps: `Arrow.prep_null_mask_filter`, `Arrow::BooleanBufferBuilder`
(`append_buffer`/`append_n`), `Arrow.gt`/`lt` and `Arrow::BooleanArray#values`
were all present and behaved as upstream.
