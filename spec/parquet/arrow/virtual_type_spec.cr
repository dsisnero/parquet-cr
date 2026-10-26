require "../../spec_helper"

# parquet/src/arrow/schema/virtual_type.rs and parquet-geospatial/src/types.rs,
# apache/arrow-rs 60.0.0 (ef1fa157977633f0ba21aa921f9ef3d5c669235b).
describe "Virtual column extension types" do
  {% for type in [Parquet::Arrow::RowNumber, Parquet::Arrow::RowGroupIndex] %}
    it "validates {{type}} storage and metadata" do
      field = Arrow::Field.new("virtual", Arrow::DataType.int64, false).with_extension_type({{type}}.new)
      field.extension_type_name.should eq {{type}}::NAME
      field.extension_type_metadata.should eq ""
      extension = field.try_extension_type({{type}})
      extension.metadata.should eq ""
      extension.should eq {{type}}.new
      extension.hash.should eq {{type}}.new.hash
      Parquet::Arrow.virtual_column?(field).should be_true
    end

    it "rejects {{type}} without an extension name" do
      field = Arrow::Field.new("", Arrow::DataType.int64, false).with_metadata({"ARROW:extension:metadata" => ""})
      expect_raises(Arrow::ArrowError, "Extension type name missing") { field.extension_type({{type}}) }
    end

    it "rejects {{type}} storage other than Int64" do
      expect_raises(Arrow::ArrowError, "expected Int64, found Int32") do
        Arrow::Field.new("", Arrow::DataType.int32, false).with_extension_type({{type}}.new)
      end
    end

    it "rejects {{type}} missing metadata" do
      field = Arrow::Field.new("", Arrow::DataType.int64, false).with_metadata({"ARROW:extension:name" => {{type}}::NAME})
      expect_raises(Arrow::ArrowError, "expects an empty string as metadata") { field.extension_type({{type}}) }
    end

    it "rejects {{type}} nonempty metadata" do
      field = Arrow::Field.new("", Arrow::DataType.int64, false).with_metadata({"ARROW:extension:name" => {{type}}::NAME, "ARROW:extension:metadata" => "non-empty"})
      expect_raises(Arrow::ArrowError, "expects an empty string as metadata") { field.extension_type({{type}}) }
    end
  {% end %}

  it "recognizes only the upstream parquet.virtual prefix" do
    Parquet::Arrow.virtual_column?(Arrow::Field.new("plain", Arrow::DataType.int64, false)).should be_false
    field = Arrow::Field.new("future", Arrow::DataType.int64, false).with_metadata({"ARROW:extension:name" => "parquet.virtual.future"})
    Parquet::Arrow.virtual_column?(field).should be_true
  end
end

describe "WKB Arrow extension" do
  it "accepts Binary, LargeBinary and BinaryView storage" do
    [Arrow::DataType.binary, Arrow::DataType.large_binary, Arrow::DataType.binary_view].each do |type|
      field = Arrow::Field.new("geometry", type, true).with_extension_type(ParquetGeospatial::WkbType.new)
      field.extension_type_name.should eq "geoarrow.wkb"
      field.try_extension_type(ParquetGeospatial::WkbType).metadata.should eq ParquetGeospatial::Metadata.new
    end
    expect_raises(Arrow::ArrowError, "Geometry data type mismatch") do
      ParquetGeospatial::WkbType.new.supports_data_type(Arrow::DataType.utf8)
    end
  end

  it "round trips CRS and edges through field metadata" do
    metadata = ParquetGeospatial::Metadata.new("EPSG:4326", ParquetGeospatial::Edges::Karney)
    field = Arrow::Field.new("geography", Arrow::DataType.binary, true).with_extension_type(ParquetGeospatial::WkbType.new(metadata))
    restored = field.try_extension_type(ParquetGeospatial::WkbType).metadata
    restored.crs.should eq metadata.crs
    restored.algorithm.should eq metadata.algorithm
    expect_raises(ParquetGeospatial::JsonError) do
      ParquetGeospatial::WkbType.try_new(Arrow::DataType.binary, ParquetGeospatial::WkbType.deserialize_metadata("{"))
    end
  end
end

describe "WKB field validation errors" do
  it "treats malformed metadata as an invalid extension instead of escaping Arrow's error handling" do
    field = Arrow::Field.new("geometry", Arrow::DataType.binary, true).with_metadata({"ARROW:extension:name" => "geoarrow.wkb", "ARROW:extension:metadata" => "{"})
    field.has_valid_extension_type?(ParquetGeospatial::WkbType).should be_false
  end
end
