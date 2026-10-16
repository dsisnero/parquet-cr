# Shared helpers for Parquet specs.
module ParquetTestHelpers
  # Builds a two-column (`a`, `b`, both INT32) test schema descriptor.
  def self.test_schema_descr : Parquet::Schema::SchemaDescriptor
    a = Parquet::Schema::Type.primitive_type_builder("a", Parquet::Type::INT32).build.as(Parquet::Schema::Type)
    b = Parquet::Schema::Type.primitive_type_builder("b", Parquet::Type::INT32).build.as(Parquet::Schema::Type)
    schema = Parquet::Schema::Type.group_type_builder("schema").with_fields([a, b]).build.as(Parquet::Schema::Type)
    Parquet::Schema::SchemaDescriptor.new(schema)
  end

  # Builds a `SchemaDescriptor` from three INT32 columns (`a`, `b`, `c`).
  def self.three_column_schema_descr : Parquet::Schema::SchemaDescriptor
    fields = ["a", "b", "c"].map do |name|
      Parquet::Schema::Type.primitive_type_builder(name, Parquet::Type::INT32).build.as(Parquet::Schema::Type)
    end
    schema = Parquet::Schema::Type.group_type_builder("schema").with_fields(fields).build.as(Parquet::Schema::Type)
    Parquet::Schema::SchemaDescriptor.new(schema)
  end

  def self.enc_mask(*encodings : Parquet::Encoding) : Parquet::EncodingMask
    Parquet::EncodingMask.new_from_encodings(encodings)
  end
end
