#!/usr/bin/env bash
set -euo pipefail

# Pass binaries built from this port and the pinned vendor/arrow-rs checkout:
#   scripts/verify_rewrite_interop.sh CRYSTAL_REWRITE RUST_READ RUST_REWRITE [DIR]
crystal_rewrite=${1:?Expected Crystal parquet-rewrite binary}
rust_read=${2:?Expected pinned Rust parquet-read binary}
rust_rewrite=${3:?Expected pinned Rust parquet-rewrite binary}
interop_dir=${4:-$(mktemp -d)}
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture="$repo_root/vendor/arrow-rs/parquet-testing/data/alltypes_plain.parquet"
mkdir -p "$interop_dir"
"$rust_read" --json "$fixture" > "$interop_dir/source.json"
# Both rewrite commands normalize INT96 to annotated INT64 nanoseconds. Rust's
# record JSON API prints those differently, so use Rust rewrite as the exact
# output oracle rather than comparing two physical timestamp representations.
"$rust_rewrite" -i "$fixture" -o "$interop_dir/upstream-normalized.parquet"
"$rust_read" --json "$interop_dir/upstream-normalized.parquet" > "$interop_dir/original.json"
for codec in none snappy gzip brotli lz4 zstd lz4-raw; do
  crystal_output="$interop_dir/crystal-$codec.parquet"
  rust_output="$interop_dir/rust-$codec.parquet"
  crystal_again="$interop_dir/crystal-again-$codec.parquet"
  "$crystal_rewrite" -i "$fixture" -o "$crystal_output" --compression "$codec" \
    --dictionary-enabled false --encoding plain --writer-version 2.0 \
    --max-row-group-size 3 --write-batch-size 1 --data-page-row-count-limit 2 \
    --statistics-enabled none --write-page-header-statistics true \
    --write-path-in-schema false --bloom-filter-enabled true \
    --bloom-filter-fpp 0.02 --bloom-filter-ndv 8 --bloom-filter-position end
  "$rust_read" --json "$crystal_output" > "$interop_dir/crystal-$codec.json"
  diff -u "$interop_dir/original.json" "$interop_dir/crystal-$codec.json"
  "$rust_rewrite" -i "$crystal_output" -o "$rust_output" --compression zstd \
    --writer-version 1.0 --max-row-group-size 2
  "$crystal_rewrite" -i "$rust_output" -o "$crystal_again" --compression none
  "$rust_read" --json "$crystal_again" > "$interop_dir/again-$codec.json"
  diff -u "$interop_dir/original.json" "$interop_dir/again-$codec.json"
done
printf 'Rewrite interoperability passed: 7 codecs, V1/V2, INT96 input; files in %s\n' "$interop_dir"
