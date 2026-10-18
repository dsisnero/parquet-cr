require "arrow-cr"
require "../../errors"
require "../array_reader"
require "../record_reader"
require "../buffer/bit_util"

# Ported from `parquet/src/arrow/array_reader/fixed_len_byte_array.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `make_fixed_len_byte_array_reader` and `FixedLenByteArrayReader`: raw
# fixed-length byte array values are decoded through the shared record reader and
# converted to the arrow target array at `consume_batch` time
# (`FixedSizeBinary`, `Decimal32/64/128/256`, `Interval(YearMonth|DayTime)`).
#
# Divergences: upstream decodes straight into a `FixedLenByteArrayBuffer` and
# builds specialized decimal/interval arrays; the port keeps
# `ColumnValueDecoderImpl(FixedLenByteArray)` and represents decimals/intervals
# as `PrimitiveArray(T)` (Int32/Int64/Int128/`I256`) with the matching
# decimal/interval data type, because arrow-cr has no `Decimal*Array`/
# `Interval*Array` classes. The `Float16` target raises `NYIError` (Crystal has
# no `Float16`), and `Interval(MonthDayNano)` is NYI as upstream. Decimals are
# big-endian and sign-extended with the ported `sign_extend_be`; intervals are
# little-endian (the upstream conversions are matched exactly).
module Parquet
  module Arrow
    # Returns an `ArrayReader` for a fixed-length byte array column (upstream
    # `make_fixed_len_byte_array_reader`).
    # ameba:disable Metrics/CyclomaticComplexity
    def self.make_fixed_len_byte_array_reader(
      pages : Column::PageIterator,
      column_desc : Schema::ColumnDescriptor,
      arrow_type : ::Arrow::DataType?,
      batch_size : Int32,
      padding_threshold : Int16?,
    ) : ArrayReader
      data_type = arrow_type || parquet_to_arrow_field(column_desc).data_type

      unless column_desc.physical_type == Parquet::Type::FIXED_LEN_BYTE_ARRAY
        raise general_err!(
          "invalid physical type for fixed length byte array reader - #{column_desc.physical_type}"
        )
      end
      byte_length = column_desc.type_length

      case data_type.kind
      when .fixed_size_binary?
        # any length is allowed
      when .decimal32?
        if byte_length > 4
          raise general_err!("decimal 32 type too large, must be less then 4 bytes, got #{byte_length}")
        end
      when .decimal64?
        if byte_length > 8
          raise general_err!("decimal 64 type too large, must be less then 8 bytes, got #{byte_length}")
        end
      when .decimal128?
        if byte_length > 16
          raise general_err!("decimal 128 type too large, must be less than 16 bytes, got #{byte_length}")
        end
      when .decimal256?
        if byte_length > 32
          raise general_err!("decimal 256 type too large, must be less than 32 bytes, got #{byte_length}")
        end
      when .interval?
        unless byte_length == 12
          raise general_err!("interval type must consist of 12 bytes got #{byte_length}")
        end
      when .float16?
        unless byte_length == 2
          raise general_err!("float 16 type must be 2 bytes, got #{byte_length}")
        end
        raise NYIError.new(
          "the Float16 target needs an arrow-cr half-float array (Crystal has no Float16)"
        )
      else
        raise general_err!(
          "invalid data type for fixed length byte array reader - #{data_type}"
        )
      end

      FixedLenByteArrayReader.new(
        pages, column_desc, data_type, byte_length, batch_size, padding_threshold
      )
    end

    # An `ArrayReader` for fixed-length byte array columns (upstream
    # `FixedLenByteArrayReader`).
    class FixedLenByteArrayReader < ArrayReader
      # Crystal cannot infer this from the `typed_record_reader` call (generic
      # method calls need an explicit instance variable type).
      @record_reader : RecordReader::GenericRecordReader(
        RecordReader::ArrayValuesBuffer(Parquet::FixedLenByteArray),
        Column::Reader::ColumnValueDecoderImpl(Parquet::FixedLenByteArray),
      )

      def initialize(
        @pages : Column::PageIterator,
        column_desc : Schema::ColumnDescriptor,
        @data_type : ::Arrow::DataType,
        @byte_length : Int32,
        batch_size : Int32,
        padding_threshold : Int16?,
      )
        # Null slots are padded with an all-zero value of the column's length.
        @record_reader = RecordReader.typed_record_reader(
          column_desc, batch_size,
          Parquet::FixedLenByteArray.new(Array(UInt8).new(@byte_length, 0_u8))
        )
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

        array =
          case @data_type.kind
          when .fixed_size_binary?
            buffer = Bytes.new(len * @byte_length)
            len.times do |idx|
              data = values[idx].value.data
              data.copy_to(buffer[idx * @byte_length, @byte_length]) if data.size == @byte_length
            end
            ::Arrow::FixedSizeBinaryArray.new(
              @byte_length, ::Arrow::Buffer.from_slice_ref(buffer), nulls
            ).as(::Arrow::ArrayBase)
          when .decimal32?
            ::Arrow::PrimitiveArray(Int32).new(
              ::Arrow::ScalarBuffer(Int32).from(Array(Int32).new(len) { |idx| be_int32(values[idx]) }),
              nulls, @data_type
            ).as(::Arrow::ArrayBase)
          when .decimal64?
            ::Arrow::PrimitiveArray(Int64).new(
              ::Arrow::ScalarBuffer(Int64).from(Array(Int64).new(len) { |idx| be_int64(values[idx]) }),
              nulls, @data_type
            ).as(::Arrow::ArrayBase)
          when .decimal128?
            ::Arrow::PrimitiveArray(Int128).new(
              ::Arrow::ScalarBuffer(Int128).from(Array(Int128).new(len) { |idx| be_int128(values[idx]) }),
              nulls, @data_type
            ).as(::Arrow::ArrayBase)
          when .decimal256?
            ::Arrow::PrimitiveArray(::Arrow::I256).new(
              ::Arrow::ScalarBuffer(::Arrow::I256).from(Array(::Arrow::I256).new(len) { |idx| be_i256(values[idx]) }),
              nulls, @data_type
            ).as(::Arrow::ArrayBase)
          when .interval?
            build_interval(values, len, nulls)
          else
            raise general_err!(
              "invalid data type for fixed length byte array reader - #{@data_type}"
            )
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

      private def build_interval(values : Array(FixedLenByteArray), len : Int32,
                                 nulls : ::Arrow::NullBuffer?) : ::Arrow::ArrayBase
        case @data_type.interval_unit || raise general_err!("interval data type without a unit")
        when .year_month?
          ::Arrow::PrimitiveArray(Int32).new(
            ::Arrow::ScalarBuffer(Int32).from(Array(Int32).new(len) { |idx| le_int32(values[idx], 0) }),
            nulls, @data_type
          ).as(::Arrow::ArrayBase)
        when .day_time?
          ::Arrow::PrimitiveArray(Int64).new(
            ::Arrow::ScalarBuffer(Int64).from(
              Array(Int64).new(len) do |idx|
                days = le_int32(values[idx], 4).to_i64
                millis = le_int32(values[idx], 8).to_i64
                days | (millis << 32)
              end
            ),
            nulls, @data_type
          ).as(::Arrow::ArrayBase)
        else
          raise NYIError.new("MonthDayNano intervals not supported")
        end
      end

      # Big-endian, sign-extended decimal conversions.
      private def be_int32(value : FixedLenByteArray) : Int32
        ::IO::ByteFormat::BigEndian.decode(Int32, sign_extended(value, 4))
      end

      private def be_int64(value : FixedLenByteArray) : Int64
        ::IO::ByteFormat::BigEndian.decode(Int64, sign_extended(value, 8))
      end

      private def be_int128(value : FixedLenByteArray) : Int128
        ::IO::ByteFormat::BigEndian.decode(Int128, sign_extended(value, 16))
      end

      private def be_i256(value : FixedLenByteArray) : ::Arrow::I256
        ::Arrow::I256.from_be_bytes(sign_extended(value, 32))
      end

      private def sign_extended(value : FixedLenByteArray, size : Int32) : Bytes
        Arrow::Buffer.sign_extend_be(value.value.data, size)
      end

      # Little-endian interval component at `offset`.
      private def le_int32(value : FixedLenByteArray, offset : Int32) : Int32
        ::IO::ByteFormat::LittleEndian.decode(Int32, value.value.data[offset, 4])
      end
    end
  end
end
