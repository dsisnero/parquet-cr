require "./data_type"
require "./column/reader"
require "./column/writer/writer"
require "./file/writer"

# DataType/make_type and typed dispatch, apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b. Associated native types become
# per-descriptor T aliases; implementations stay specialized on native types.
module Parquet
  module DataType
    module ClassMethods
      # ameba:disable Naming/AccessorMethodName
      abstract def get_physical_type : ::Parquet::Type
      # ameba:disable Naming/AccessorMethodName
      abstract def get_type_size : Int32
    end

    macro included
      extend ::Parquet::DataType::ClassMethods
    end
  end

  # Internal generation equivalent of upstream make_type!. Column enum arms
  # become checked casts of the existing native reader/writer specializations.
  macro make_type(name, native, physical)
    struct {{name}}
      include ::Parquet::DataType
      alias T = {{native}}
      PHYSICAL_TYPE = {{physical}}

      def self.get_physical_type : ::Parquet::Type
        PHYSICAL_TYPE
      end

      # Representation size, not encoded/on-disk width. Byte wrappers and
      # optional payloads follow the native Crystal layout.
      def self.get_type_size : Int32
        sizeof({{native}})
      end

      def self.get_column_reader(reader : ::Parquet::Column::ColumnReader)
        reader.as?(::Parquet::Column::ColumnReaderImpl(Array({{native}}), ::Parquet::Column::Reader::ColumnValueDecoderImpl({{native}})))
      end

      def self.get_column_writer(writer : ::Parquet::Column::Writer::ColumnWriter)
        writer.as?(::Parquet::Column::Writer::GenericColumnWriter({{native}}))
      end

      # Crystal references have neither consuming moves nor immutable borrows.
      def self.get_column_writer_ref(writer : ::Parquet::Column::Writer::ColumnWriter)
        get_column_writer(writer)
      end

      def self.get_column_writer_mut(writer : ::Parquet::Column::Writer::ColumnWriter)
        get_column_writer(writer)
      end
    end

    module ::Parquet::Column
      def self.get_typed_column_reader(type : {{name}}.class, reader : ColumnReader)
        type.get_column_reader(reader) || raise general_err!(
          "Failed to convert column reader into a typed column reader for `#{type.get_physical_type}` type"
        )
      end
    end

    module ::Parquet::Column::Writer
      def self.get_typed_column_writer(type : {{name}}.class, writer : ColumnWriter)
        type.get_column_writer(writer) || raise general_err!(
          "Failed to convert column writer into a typed column writer for `#{type.get_physical_type}` type"
        )
      end
    end

    class ::Parquet::File::SerializedColumnWriter
      def typed(type : {{name}}.class)
        ::Parquet::Column::Writer.get_typed_column_writer(type, @inner)
      end
    end
  end

  make_type BoolType, ::Bool, ::Parquet::Type::BOOLEAN
  make_type Int32Type, ::Int32, ::Parquet::Type::INT32
  make_type Int64Type, ::Int64, ::Parquet::Type::INT64
  make_type Int96Type, ::Parquet::Int96, ::Parquet::Type::INT96
  make_type FloatType, ::Float32, ::Parquet::Type::FLOAT
  make_type DoubleType, ::Float64, ::Parquet::Type::DOUBLE
  make_type ByteArrayType, ::Parquet::ByteArray, ::Parquet::Type::BYTE_ARRAY
  make_type FixedLenByteArrayType, ::Parquet::FixedLenByteArray, ::Parquet::Type::FIXED_LEN_BYTE_ARRAY
end

module Parquet::Column::Writer
  # Source owned/ref/mut writer conversions all preserve the native reference.
  # Delegating supports both physical descriptors and existing T.class callers.
  def self.get_typed_column_writer_ref(type : T.class, writer : ColumnWriter) forall T
    get_typed_column_writer(type, writer)
  end

  def self.get_typed_column_writer_mut(type : T.class, writer : ColumnWriter) forall T
    get_typed_column_writer(type, writer)
  end
end
