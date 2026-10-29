require "arrow-cr"
require "../../errors"
require "../array_reader"
require "../record_reader"
require "./byte_array_value_decoder"

# Ported from `parquet/src/arrow/array_reader/byte_view_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `make_byte_view_array_reader` and `ByteViewArrayReader` for `Utf8View`/
# `BinaryView` columns.
#
# Divergence: upstream decodes with a dedicated
# `ByteViewArrayColumnValueDecoder` that writes `ViewBuffer` views (and hence
# needs a pluggable value decoder in the record reader); the port decodes through
# the shared `ByteViewArrayValueDecoder` and converts the batch into a
# byte-view array at `consume_batch` time, using arrow-cr's view builders to pick
# the inline (`<= 12` byte) or block reference representation. Validation matches
# upstream (values, null padding, dictionary/delta encodings); only the
# batch-time allocation strategy differs.
module Parquet
  module Arrow
    # Returns an `ArrayReader` that decodes the provided byte array column to
    # view types (upstream `make_byte_view_array_reader`).
    def self.make_byte_view_array_reader(
      pages : Column::PageIterator,
      column_desc : Schema::ColumnDescriptor,
      arrow_type : ::Arrow::DataType?,
      batch_size : Int32,
      padding_threshold : Int16?,
    ) : ArrayReader
      data_type =
        if arrow_type
          arrow_type
        else
          case parquet_to_arrow_field(column_desc).data_type.kind
          when .utf8?, .utf8_view? then ::Arrow::DataType.utf8_view
          else                          ::Arrow::DataType.binary_view
          end
        end

      case data_type.kind
      when .binary_view?, .utf8_view?
        ByteViewArrayReader.new(pages, data_type, column_desc, batch_size, padding_threshold)
      else
        raise general_err!(
          "invalid data type for byte array reader read to view type - #{data_type}"
        )
      end
    end

    # An `ArrayReader` for variable-length byte arrays stored as views
    # (upstream `ByteViewArrayReader`).
    class ByteViewArrayReader < ArrayReader
      # Crystal cannot infer this from the `typed_record_reader` call (generic
      # method calls need an explicit instance variable type).
      @record_reader : RecordReader::GenericRecordReader(
        RecordReader::ArrayValuesBuffer(Parquet::ByteArray),
        ByteViewArrayValueDecoder,
      )

      def initialize(
        @pages : Column::PageIterator,
        @data_type : ::Arrow::DataType,
        column_desc : Schema::ColumnDescriptor,
        batch_size : Int32,
        padding_threshold : Int16?,
      )
        # Null slots are padded with an empty byte array (their values are
        # masked off by the null bitmap).
        @record_reader = RecordReader::GenericRecordReader(
          RecordReader::ArrayValuesBuffer(Parquet::ByteArray), ByteViewArrayValueDecoder,
        ).new(column_desc, batch_size, -> { RecordReader::ArrayValuesBuffer(Parquet::ByteArray).new(Parquet::ByteArray.new(Bytes.empty)) })
        if threshold = padding_threshold
          @record_reader.set_padding_threshold(threshold)
        end
        @def_levels_buffer = nil.as(Array(Int16)?)
        @rep_levels_buffer = nil.as(Array(Int16)?)
      end

      def data_type : ::Arrow::DataType
        @data_type
      end

      def read_records(batch_size : Int32) : Int32
        Arrow.read_records_from_pages(@record_reader, @pages, batch_size)
      end

      def skip_records(num_records : Int32) : Int32
        Arrow.skip_records_from_pages(@record_reader, @pages, num_records)
      end

      def consume_batch : ::Arrow::ArrayBase
        values = @record_reader.consume_record_data.values
        len = @record_reader.values_written
        bitmap = @record_reader.consume_compact_bitmap

        @def_levels_buffer = @record_reader.consume_def_levels
        @rep_levels_buffer = @record_reader.consume_rep_levels

        array =
          case @data_type.kind
          when .utf8_view?
            options = Array(String?).new(len) do |i|
              valid_slot?(bitmap, i) ? utf8_value(values[i]) : nil
            end
            ::Arrow::StringViewArray.from_options(options).as(::Arrow::ArrayBase)
          else
            options = Array(Bytes?).new(len) do |i|
              valid_slot?(bitmap, i) ? values[i].data : nil
            end
            ::Arrow::BinaryViewArray.from_options(options).as(::Arrow::ArrayBase)
          end

        @record_reader.reset
        array
      end

      def def_levels : Array(Int16)?
        @def_levels_buffer
      end

      def rep_levels : Array(Int16)?
        @rep_levels_buffer
      end

      def max_def_level : Int16
        @record_reader.max_def_level
      end

      # Whether slot `i` is valid; a missing bitmap means every slot is valid.
      private def valid_slot?(bitmap : Bytes?, index : Int32) : Bool
        bytes = bitmap
        return true unless bytes
        Parquet::Util::BitUtil.get_bit(bytes, index)
      end

      # Converts a UTF-8 column value (upstream `check_valid_utf8`).
      private def utf8_value(value : Parquet::ByteArray) : String
        Parquet::Util.check_valid_utf8(value.data)
        String.new(value.data)
      end
    end
  end
end
