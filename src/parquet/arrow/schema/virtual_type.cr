require "arrow-cr"

# parquet/src/arrow/schema/virtual_type.rs, apache/arrow-rs 60.0.0
# (ef1fa157977633f0ba21aa921f9ef3d5c669235b).
module Parquet
  module Arrow
    {% for config in [{"RowNumber", "row_number"}, {"RowGroupIndex", "row_group_index"}] %}
      # An Int64 virtual column with required empty-string extension metadata.
      class {{config[0].id}} < ::Arrow::Extension::ExtensionType(String)
        NAME = "parquet.virtual." + {{config[1]}}

        def ==(other : self) : Bool
          true
        end

        def hash(hasher)
          self.class.hash(hasher)
        end

        def self.extension_name : String
          NAME
        end

        def metadata : String
          ""
        end

        def serialize_metadata : String?
          ""
        end

        def self.deserialize_metadata(metadata : String?) : String
          return "" if metadata == ""
          raise ::Arrow::ArrowError.new("Virtual column extension type expects an empty string as metadata")
        end

        def supports_data_type(data_type : ::Arrow::DataType) : Nil
          return if data_type.kind.int64?
          raise ::Arrow::ArrowError.new("Virtual column data type mismatch, expected Int64, found #{data_type}")
        end

        def self.try_new(data_type : ::Arrow::DataType, metadata : String) : self
          extension = new
          extension.supports_data_type(data_type)
          extension
        end

        def self.validate(data_type : ::Arrow::DataType, metadata : String) : Nil
          new.supports_data_type(data_type)
        end
      end
    {% end %}

    # Recognition follows the pinned source's parquet.virtual. prefix (its
    # validation error text historically says arrow.virtual.).
    def self.virtual_column?(field : ::Arrow::Field) : Bool
      field.extension_type_name.try(&.starts_with?("parquet.virtual.")) || false
    end
  end
end
