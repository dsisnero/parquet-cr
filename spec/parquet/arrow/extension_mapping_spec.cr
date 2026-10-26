require "../../spec_helper"

# parquet/src/arrow/schema/extension.rs and feature-gated schema tests,
# apache/arrow-rs 60.0.0. Flags mirror upstream's nondefault Cargo features.
private alias EA = Parquet::Arrow
private alias ED = Arrow::DataType
private alias EL = Parquet::LogicalType

private def extension_leaf(logical : EL, physical = Parquet::Type::BYTE_ARRAY, length = -1) : Parquet::Schema::Type
  Parquet::Schema::Type.primitive_type_builder("a", physical).with_repetition(Parquet::Repetition::OPTIONAL)
    .with_logical_type(logical).with_length(length).build
end

private def extension_field(logical : EL, type : ED, physical = Parquet::Type::BYTE_ARRAY, length = -1) : Arrow::Field
  schema = Parquet::Schema::SchemaDescriptor.new(Parquet::Schema::Type.group_type_builder("schema").with_fields([extension_leaf(logical, physical, length)] of Parquet::Schema::Type).build)
  EA.parquet_to_arrow_field_levels(schema, EA::ProjectionMask.all, nil).fields.to_a.first
end

describe "Canonical Arrow Parquet extension mapping" do
  it "reports enabled logical extension kinds and preserves plain fields" do
    EA.has_extension_type?(extension_leaf(EL::Json.new)).should eq({{ flag?(:arrow_canonical_extension_types) }})
    EA.has_extension_type?(extension_leaf(EL.geometry(nil))).should eq({{ flag?(:geospatial) }})
    plain = extension_leaf(EL::String.new)
    EA.has_extension_type?(plain).should be_false
    field = Arrow::Field.new("a", ED.utf8, true).with_metadata({"user" => "kept"})
    EA.try_add_extension_type(field, plain).should eq field
  end

  {% if flag?(:arrow_canonical_extension_types) %}
    it "maps valid canonical UUID and JSON extensions in both directions" do
      uuid = Arrow::Field.new("a", ED.fixed_size_binary(16), true).with_extension_type(Arrow::Extension::Canonical::Uuid.new)
      json = Arrow::Field.new("a", ED.utf8, true).with_extension_type(Arrow::Extension::Canonical::Json.new)
      EA.arrow_to_parquet_type(uuid, false).basic_info.logical_type_ref.should eq EL::Uuid.new
      EA.arrow_to_parquet_type(json, false).basic_info.logical_type_ref.should eq EL::Json.new
      extension_field(EL::Uuid.new, ED.fixed_size_binary(16), Parquet::Type::FIXED_LEN_BYTE_ARRAY, 16).has_valid_extension_type?(Arrow::Extension::Canonical::Uuid).should be_true
      extension_field(EL::Json.new, ED.utf8).has_valid_extension_type?(Arrow::Extension::Canonical::Json).should be_true
      [ED.utf8, ED.large_utf8, ED.utf8_view].each do |type|
        field = Arrow::Field.new("a", type, false).with_extension_type(Arrow::Extension::Canonical::Json.new)
        EA.logical_type_for_string(field).should eq EL::Json.new
      end
    end

    it "falls back for invalid tags and rejects incompatible logical storage" do
      invalid = Arrow::Field.new("a", ED.fixed_size_binary(8), false).with_metadata({"ARROW:extension:name" => "arrow.uuid"})
      EA.logical_type_for_fixed_size_binary(invalid).should be_nil
      json = Arrow::Field.new("a", ED.utf8, false).with_metadata({"ARROW:extension:name" => "arrow.json"})
      EA.logical_type_for_string(json).should eq EL::String.new
      expect_raises(Arrow::ArrowError, "Uuid data type mismatch") do
        EA.try_add_extension_type(Arrow::Field.new("a", ED.binary, true), extension_leaf(EL::Uuid.new, Parquet::Type::FIXED_LEN_BYTE_ARRAY, 16))
      end
    end
  {% else %}
    it "keeps upstream default UUID/JSON mapping disabled" do
      uuid = Arrow::Field.new("a", ED.fixed_size_binary(16), true).with_extension_type(Arrow::Extension::Canonical::Uuid.new)
      json = Arrow::Field.new("a", ED.utf8, true).with_extension_type(Arrow::Extension::Canonical::Json.new)
      EA.logical_type_for_fixed_size_binary(uuid).should be_nil
      EA.logical_type_for_string(json).should eq EL::String.new
    end
  {% end %}

  {% if flag?(:geospatial) %}
    it "maps CRS defaults and unset CRS for geometry and geography" do
      [nil, "srid:0", "EPSG:3857"].each do |crs|
        [false, true].each do |geography|
          logical = geography ? EL.geography(crs, Parquet::EdgeInterpolationAlgorithm::KARNEY) : EL.geometry(crs)
          field = extension_field(logical, ED.binary)
          wkb = field.try_extension_type(ParquetGeospatial::WkbType)
          wkb.metadata.crs.should eq(crs == "srid:0" ? nil : JSON::Any.new(crs || "OGC:CRS84"))
          wkb.metadata.algorithm.should eq(geography ? ParquetGeospatial::Edges::Karney : nil)
          mapped = EA.logical_type_for_binary(field)
          expected_crs = crs.nil? ? nil : crs
          mapped.should eq(geography ? EL.geography(expected_crs, Parquet::EdgeInterpolationAlgorithm::KARNEY) : EL.geometry(expected_crs))
        end
      end
    end

    it "normalizes lon-lat, serializes PROJJSON, and handles invalid WKB metadata" do
      ["EPSG:4326", "OGC:CRS84", %({"id":{"authority":"EPSG","code":4326}})].each do |crs|
        field = Arrow::Field.new("a", ED.binary_view, true).with_extension_type(ParquetGeospatial::WkbType.new(ParquetGeospatial::Metadata.new(crs)))
        EA.logical_type_for_binary_view(field).should eq EL.geometry(nil)
      end
      field = Arrow::Field.new("a", ED.large_binary, true).with_extension_type(ParquetGeospatial::WkbType.new(ParquetGeospatial::Metadata.new(%({"name":"custom"}))))
      EA.logical_type_for_binary(field).should eq EL.geometry(%({"name":"custom"}))
      field.with_metadata({"ARROW:extension:name" => "geoarrow.wkb", "ARROW:extension:metadata" => "{"})
      EA.logical_type_for_binary(field).should be_nil
      field = extension_field(EL.geography(nil, nil), ED.binary)
      field.try_extension_type(ParquetGeospatial::WkbType).metadata.algorithm.should eq ParquetGeospatial::Edges::Spherical
      EA.logical_type_for_binary(field).should eq EL.geography(nil, nil)
    end
  {% end %}

  {% if flag?(:variant_experimental) %}
    it "maps the Variant struct extension in both directions" do
      storage = ParquetVariantCompute::VariantArray.from_iter([ParquetVariant::Variant.from(42_i32)]).inner.data_type
      field = Arrow::Field.new("variant", storage, true).with_extension_type(ParquetVariantCompute::VariantType.new)
      EA.logical_type_for_struct(field).should eq EL.variant(nil)
      parquet = EA.arrow_to_parquet_type(field, false)
      parquet.basic_info.logical_type_ref.should eq EL.variant(nil)
      descriptor = Parquet::Schema::SchemaDescriptor.new(Parquet::Schema::Type.group_type_builder("schema").with_fields([parquet] of Parquet::Schema::Type).build)
      restored = EA.parquet_to_arrow_field_levels(descriptor, EA::ProjectionMask.all, nil).fields.to_a.first
      restored.has_valid_extension_type?(ParquetVariantCompute::VariantType).should be_true
    end
  {% end %}
end

{% if flag?(:arrow_canonical_extension_types) && flag?(:geospatial) && flag?(:variant_experimental) %}
  describe "Canonical extension file round trips" do
    it "preserves schema metadata and recreates extensions without an Arrow IPC hint" do
      strings = Arrow::GenericStringBuilder(Int32).new
      strings.append_value(%({"x":1}))
      strings.append_null
      uuid = Arrow::FixedSizeBinaryArray.from_nullable([Bytes.new(16, 1_u8), nil])
      wkb = Arrow::GenericBinaryBuilder(Int32).new
      # POINT(0 0), little-endian WKB.
      point = Bytes[1, 1, 0, 0, 0] + Bytes.new(16, 0_u8)
      wkb.append_value(point)
      wkb.append_null
      variant = ParquetVariantCompute::VariantArray.from_iter([ParquetVariant::Variant.from(42_i32), nil] of ParquetVariant::Variant?).inner
      arrays = [uuid, strings.finish, wkb.finish, variant] of Arrow::ArrayBase
      fields = [
        Arrow::Field.new("uuid", uuid.data_type, true).with_extension_type(Arrow::Extension::Canonical::Uuid.new),
        Arrow::Field.new("json", ED.utf8, true).with_extension_type(Arrow::Extension::Canonical::Json.new),
        Arrow::Field.new("geometry", ED.binary, true).with_extension_type(ParquetGeospatial::WkbType.new(ParquetGeospatial::Metadata.new("EPSG:3857", ParquetGeospatial::Edges::Karney))),
        Arrow::Field.new("variant", variant.data_type, true).with_extension_type(ParquetVariantCompute::VariantType.new),
      ]
      schema = Arrow::Schema.new(Arrow::Fields.from(fields))
      batch = Arrow::RecordBatch.try_new(schema, arrays)
      sink = IO::Memory.new
      writer = EA::ArrowWriter.try_new(sink, schema)
      writer.write(batch)
      writer.close
      [false, true].each do |skip|
        file = Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(sink.to_slice))
        options = EA::ArrowReaderOptions.new.with_skip_arrow_metadata(skip)
        result = EA::ParquetRecordBatchReaderBuilder.try_new_with_options(file, options).build.to_a.first
        result.num_rows.should eq 2
        result.schema.fields.to_a.map(&.extension_type_name).should eq ["arrow.uuid", "arrow.json", "geoarrow.wkb", "arrow.parquet.variant"]
        result.schema.fields.to_a[2].try_extension_type(ParquetGeospatial::WkbType).metadata.crs.should eq JSON::Any.new("EPSG:3857")
        result.schema.fields.to_a[2].try_extension_type(ParquetGeospatial::WkbType).metadata.algorithm.should eq ParquetGeospatial::Edges::Karney
        arrays.first(3).each_with_index { |array, index| result.column(index).should eq array }
        actual_variant = result.column(3).as(Arrow::StructArray)
        expected_kind = skip ? ED.binary : ED.binary_view
        actual_variant.fields.to_a.map(&.data_type).should eq [expected_kind, expected_kind]
        2.times do |index|
          2.times do |row|
            ParquetVariantCompute::VariantArray.binary_array_value(actual_variant.column(index), row).should eq ParquetVariantCompute::VariantArray.binary_array_value(variant.column(index), row)
          end
        end
        actual_variant.nulls.should eq variant.nulls
      end
    end
  end
{% end %}

{% if flag?(:geospatial) %}
  describe "Parquet edge interpolation conversion" do
    it "round trips all algorithms and rejects unknown thrift codes" do
      ParquetGeospatial::Edges.each do |edges|
        Parquet::EdgeInterpolationAlgorithm.from(edges).try_as_edges.should eq edges
      end
      expect_raises(Parquet::GeneralError, "Unknown edge interpolation algorithm: _Unknown(99)") do
        Parquet::EdgeInterpolationAlgorithm.new(99).try_as_edges
      end
    end
  end
{% end %}
