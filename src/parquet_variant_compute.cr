# Ported from `parquet-variant-compute/src/lib.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b): `VariantArray` and compute kernels
# for the Variant Binary Encoding.
#
# Ported so far (see `plans/parity.md`): the unshredded `VariantArray` (with
# `ShreddingState` and the `arrow.parquet.variant` extension type), its
# `VariantArrayBuilder`, and the JSON kernels `json_to_variant` /
# `variant_to_json`. Deferred: the shredding kernels (`shred_variant`/
# `unshred_variant`), `variant_get`/`GetOptions`, the `cast_to_variant`/
# `variant_to_arrow`/`arrow_to_variant` conversions, `type_conversion` and
# `VariantValueArrayBuilder`.
require "./parquet_variant_compute/variant_array"
require "./parquet_variant_compute/variant_array_builder"
require "./parquet_variant_compute/from_json"
require "./parquet_variant_compute/to_json"
