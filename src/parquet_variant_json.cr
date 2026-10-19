# Ported from `parquet-variant-json/src/lib.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b): conversion between JSON and the
# Variant binary encoding.
#
# This port currently provides the Variant → JSON direction
# (`Variant#to_json`/`#to_json_string`/`#to_json_value`). The JSON → Variant
# direction (`JsonToVariant`/`append_json`) is deferred: it needs
# `VariantBuilderExt`/`ObjectFieldBuilder`, which are only used by
# `parquet-variant-compute` and are not ported.
require "./parquet_variant_json/variant_to_json"
