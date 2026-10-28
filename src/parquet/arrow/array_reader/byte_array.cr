require "arrow-cr"
require "../../errors"
require "../array_reader"
require "../record_reader"
require "./byte_array_value_decoder"

# Ported from `parquet/src/arrow/array_reader/byte_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `make_byte_array_reader` and `ByteArrayReader` for `Utf8`/`LargeUtf8`/
# `Binary`/`LargeBinary` columns.
#
# Divergence: upstream decodes with a dedicated `ByteArrayColumnValueDecoder`
# writing straight into an `OffsetBuffer` (the arrow values buffer); the port
# decodes through the shared `ByteArrayValueDecoder` and copies the
# values into the arrow offset buffer at `consume_batch` time. Behaviour is the
# same (one value per non-null slot, null padding handled by the record reader);
# only batch-time allocation differs. The decimal targets (`Decimal128`/
# `Decimal256`, which reinterpret big-endian FLBA payloads) are deferred with the
# arrow cast layer.
module Parquet
  module Arrow
    # Returns an `ArrayReader` that decodes the provided byte array column
    # (upstream `make_byte_array_reader`).
    def self.make_byte_array_reader(
      pages : Column::PageIterator,
      column_desc : Schema::ColumnDescriptor,
      arrow_type : ::Arrow::DataType?,
      batch_size : Int32,
      padding_threshold : Int16?,
    ) : ArrayReader
      data_type = arrow_type || parquet_to_arrow_field(column_desc).data_type

      case data_type.kind
      when .utf8?, .binary?
        ByteArrayReader(Int32).new(pages, column_desc, data_type, batch_size, padding_threshold)
      when .large_utf8?, .large_binary?
        ByteArrayReader(Int64).new(pages, column_desc, data_type, batch_size, padding_threshold)
      else
        raise general_err!("invalid data type for byte array reader - #{data_type}")
      end
    end

    # An `ArrayReader` for variable-length byte arrays
    # (upstream `ByteArrayReader<I>`).
    class ByteArrayReader(I) < ArrayReader
      # Crystal cannot infer this from the `typed_record_reader` call (generic
      # method calls need an explicit instance variable type).
      @record_reader : RecordReader::GenericRecordReader(
        RecordReader::ArrayValuesBuffer(Parquet::ByteArray),
        ByteArrayValueDecoder,
      )

      def initialize(
        @pages : Column::PageIterator,
        column_desc : Schema::ColumnDescriptor,
        @data_type : ::Arrow::DataType,
        batch_size : Int32,
        padding_threshold : Int16?,
      )
        # Null slots are padded with an empty byte array (their values are
        # masked off by the null bitmap).
        @record_reader = RecordReader::GenericRecordReader(
          RecordReader::ArrayValuesBuffer(Parquet::ByteArray), ByteArrayValueDecoder,
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
        nulls = Arrow.null_buffer_from(@record_reader.consume_compact_bitmap, len)

        offsets = [I.new(0)]
        data = [] of UInt8
        values.each do |value|
          bytes = value.data
          data.concat(bytes)
          offsets << I.new(data.size)
        end

        offset_buffer = ::Arrow::OffsetBuffer(I).new(
          ::Arrow::ScalarBuffer(I).from(offsets), false
        )
        buffer = ::Arrow::Buffer.from_slice_ref(Bytes.new(data.size) { |i| data[i] })

        array =
          case @data_type.kind
          when .utf8?, .large_utf8?
            values.each { |value| Parquet::Util.check_valid_utf8(value.data) }
            ::Arrow::GenericStringArray(I).new(offset_buffer, buffer, nulls)
          else
            ::Arrow::GenericBinaryArray(I).new(offset_buffer, buffer, nulls)
          end

        @def_levels_buffer = @record_reader.consume_def_levels
        @rep_levels_buffer = @record_reader.consume_rep_levels
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
    end
  end
end
