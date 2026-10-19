# Ported from `parquet-variant-json/src/lib.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b): conversion between JSON and the
# Variant binary encoding.
#
# Both directions are ported: Variant → JSON
# (`Variant#to_json`/`#to_json_string`/`#to_json_value`) and JSON → Variant
# (`JsonToVariant#append_json`, built on `VariantBuilderExt`/
# `ObjectFieldBuilder`).
require "./parquet_variant_json/variant_to_json"
require "./parquet_variant_json/from_json"
