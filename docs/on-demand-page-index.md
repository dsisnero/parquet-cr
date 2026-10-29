# On-demand page indexes

Run the complete upstream example with:

```sh
crystal run examples/custom_page_index.cr
```

It creates the pinned six-column dataset in memory (150 rows, three row groups,
five ten-row batches per group), writes page statistics with a 100-byte page
size limit, and reports dense versus selectively populated indexes. Values and
writer settings match apache/arrow-rs 60.0.0 `parquet/examples/custom_page_index.rs`
(`ef1fa157977633f0ba21aa921f9ef3d5c669235b`). Memory IO replaces the temporary file;
the temporary path/progress line is omitted. Make format/lint includes examples.

The example's provider is also exported as
`Parquet::File::Metadata::OnDemandPageIndexProvider`:

```crystal
provider = Parquet::File::Metadata::OnDemandPageIndexProvider.new(metadata.clone, file_bytes)
provider.fetch_column_index(0, 0)
[0, 1, 4].each { |column| provider.fetch_offset_index(0, column) }
metadata = metadata.into_builder.set_page_index(provider).build
```

Start from footer metadata loaded with `PageIndexPolicy::Skip`. Fetch explicitly
before querying: `column_index`, `offset_index`, `num_data_pages` and
`page_locations` only consult sparse nested caches. They never decode or read
additional indexes. Repeated fetches reuse cached decoded buffers. Missing index
ranges leave no cache entry; decode failures propagate and also leave the slot
vacant. As upstream, the availability predicates describe initialized caches:
`column_indexes?`/`offset_indexes?` become true even if a requested chunk has no
index. Consequently `complete?` does not mean every chunk has both indexes.

The example fetches column statistics only for row group 0, column 0, and offset
indexes only for row group 0, columns 0/1/4. All other cells remain absent. This
is an index-selection demonstration; it does not execute a page-pruning query.
The entire file remains in memory, so this saves decoded-index storage, not
network IO or the file-byte allocation. No performance claim is made.

Native constructor inputs remain references; treat metadata and file bytes as
stable while fetching. Clone metadata first for independent edits. Provider
`clone` copies metadata, sparse maps and all cached index arrays/bytes directly;
it shares stable file bytes. Metadata clone shares the provider, matching the
source's Arc. Mutable cached arrays must not be edited during concurrent use;
this provider has no synchronization. Encrypted index decryption is outside
this example, as in the pin. Metadata writers intentionally omit custom
providers' indexes, following the pinned writer's concrete PageIndex downcast.

Native predicate names use `?`; `as_any` becomes Crystal `is_a?`/`as`. Counts are
Int32. Invalid file ranges raise `GeneralError` rather than Rust slice panics.
Derived Debug formatting is not reproduced. The otherwise unused derived Clone
is covered explicitly beyond the discovery ledger.

The pinned example has no unit tests. Ten tests-first characterizations cover
its dataset and workflow, sparse lookup, dense equivalence for all sample
columns/groups, repeated fetches, missing/corrupt ranges and retry behavior,
independent populated-cache clones and the executable's report. Initial red:
the example API was absent; the second red found the missing provider Clone.
