# Ported from `parquet-variant-compute/src/lib.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b): `VariantArray` and compute kernels
# for the Variant Binary Encoding.
#
# Ported so far (see `plans/parity.md`): the `VariantArray` (unshredded and
# shredded, with `ShreddingState` and the `arrow.parquet.variant` extension
# type), `VariantArrayBuilder`/`VariantValueArrayBuilder`, the JSON kernels
# `json_to_variant`/`variant_to_json` and `unshred_variant`, the
# `TypeConversion` layer that casts a single variant value to a typed Arrow
# value, and the `VariantToArrow` row builders (every target shape, including
# unions), `shred_variant` with `ShreddedSchemaBuilder`, and
# `variant_get`/`GetOptions`. Deferred: `cast_to_variant`/`arrow_to_variant`.
require "./parquet_variant_compute/type_conversion"
require "./parquet_variant_compute/variant_to_arrow"
require "./parquet_variant_compute/variant_array"
require "./parquet_variant_compute/variant_array_builder"
require "./parquet_variant_compute/variant_value_array_builder"
require "./parquet_variant_compute/unshred_variant"
require "./parquet_variant_compute/from_json"
require "./parquet_variant_compute/to_json"
require "./parquet_variant_compute/shred_variant"
require "./parquet_variant_compute/variant_get"
