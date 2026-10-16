require "../spec_helper"

# Ported from `parquet-geospatial/src/types.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: `serde_json::Value` is `JSON::Any`; `Result`/`Option` map to
# raises/`nil`; `ArrowError::JsonError` is `ParquetGeospatial::JsonError`. The
# `ExtensionType` tests (`test_extension_type_*`) need `arrow-schema` and are
# deferred to the Arrow interop chunk.

private alias Meta = ParquetGeospatial::Metadata
private alias Hint = ParquetGeospatial::Hint
private alias Edges = ParquetGeospatial::Edges
private alias WkbType = ParquetGeospatial::WkbType
private alias JsonError = ParquetGeospatial::JsonError

describe ParquetGeospatial::Metadata do
  # Upstream `test_metadata_empty_roundtrip`.
  it "round-trips empty metadata" do
    wkb = WkbType.new(Meta.default)
    serialized = wkb.serialize_metadata.not_nil!
    serialized.should eq("{}")

    deserialized = WkbType.deserialize_metadata(serialized)
    deserialized.crs.should be_nil
    deserialized.algorithm.should be_nil
  end

  # Upstream `test_metadata_crs_string_roundtrip`.
  it "round-trips a string CRS" do
    wkb = WkbType.new(Meta.new("srid:1234", nil))
    serialized = wkb.serialize_metadata.not_nil!
    serialized.should eq(%({"crs":"srid:1234"}))

    deserialized = WkbType.deserialize_metadata(serialized)
    deserialized.crs.should eq(JSON::Any.new("srid:1234"))
    deserialized.algorithm.should be_nil
  end

  # Upstream `test_metadata_crs_json_object_roundtrip`.
  it "round-trips a JSON object CRS" do
    crs_json = %({"type":"custom_json","properties":{"name":"EPSG:4326"}})
    wkb = WkbType.new(Meta.new(crs_json, nil))
    serialized = wkb.serialize_metadata.not_nil!

    parsed = JSON.parse(serialized)
    parsed["crs"]["type"].should eq("custom_json")
    parsed["crs"]["properties"]["name"].should eq("EPSG:4326")

    crs = WkbType.deserialize_metadata(serialized).crs.not_nil!
    crs.as_h?.should_not be_nil
    crs["type"].should eq("custom_json")
    crs["properties"]["name"].should eq("EPSG:4326")
  end

  # Upstream `test_metadata_algorithm_roundtrip`.
  it "round-trips an algorithm" do
    wkb = WkbType.new(Meta.new(nil, Edges::Spherical))
    serialized = wkb.serialize_metadata.not_nil!
    serialized.should eq(%({"edges":"spherical"}))

    deserialized = WkbType.deserialize_metadata(serialized)
    deserialized.crs.should be_nil
    deserialized.algorithm.should eq(Edges::Spherical)
  end

  # Upstream `test_metadata_full_roundtrip`.
  it "round-trips a CRS and algorithm" do
    wkb = WkbType.new(Meta.new("srid:1234", Edges::Spherical))
    serialized = wkb.serialize_metadata.not_nil!
    serialized.should eq(%({"crs":"srid:1234","edges":"spherical"}))

    deserialized = WkbType.deserialize_metadata(serialized)
    deserialized.crs.should eq(JSON::Any.new("srid:1234"))
    deserialized.algorithm.should eq(Edges::Spherical)
  end

  # Upstream `test_metadata_deserialize_none`.
  it "defaults when metadata is absent" do
    deserialized = WkbType.deserialize_metadata(nil)
    deserialized.crs.should be_nil
    deserialized.algorithm.should be_nil
  end

  # Upstream `test_metadata_deserialize_invalid_json`.
  it "rejects invalid JSON" do
    expect_raises(JsonError) do
      WkbType.deserialize_metadata("not valid json {")
    end
  end

  # Upstream `test_type_hint_geometry`.
  it "hints geometry without an algorithm" do
    Meta.new(nil, nil).type_hint.should eq(Hint::Geometry)
  end

  # Upstream `test_type_hint_edges_is_geography`.
  it "hints geography with any algorithm" do
    [Edges::Spherical, Edges::Vincenty, Edges::Thomas, Edges::Andoyer, Edges::Karney].each do |algorithm|
      Meta.new(nil, algorithm).type_hint.should eq(Hint::Geography)
    end
  end

  # Upstream `test_crs_is_lon_lat`.
  it "detects common lon/lat CRS representations" do
    Meta.new("EPSG:4326", nil).crs_is_lon_lat.should be_true
    Meta.new("OGC:CRS84", nil).crs_is_lon_lat.should be_true
    Meta.new(%({"id":{"authority":"EPSG","code":"4326"}}), nil).crs_is_lon_lat.should be_true
    Meta.new(%({"id":{"authority":"EPSG","code":4326}}), nil).crs_is_lon_lat.should be_true
    Meta.new(%({"id":{"authority":"OGC","code":"CRS84"}}), nil).crs_is_lon_lat.should be_true

    Meta.new("srid:1234", nil).crs_is_lon_lat.should be_false
    Meta.new("EPSG:3857", nil).crs_is_lon_lat.should be_false
    Meta.new(nil, nil).crs_is_lon_lat.should be_false
  end
end
