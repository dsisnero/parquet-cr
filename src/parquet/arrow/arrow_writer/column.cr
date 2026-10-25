require "./levels"
require "./pages"

module Parquet::Arrow
  # Materialize physical Parquet values only at the leaf encoding boundary.
  module WriterValues
    def self.decode(array : ::Arrow::ArrayBase) : ::Arrow::ArrayBase
      case array
      when ::Arrow::DictionaryArray(Int8), ::Arrow::DictionaryArray(Int16), ::Arrow::DictionaryArray(Int32), ::Arrow::DictionaryArray(Int64),
           ::Arrow::DictionaryArray(UInt8), ::Arrow::DictionaryArray(UInt16), ::Arrow::DictionaryArray(UInt32), ::Arrow::DictionaryArray(UInt64)
        ::Arrow.take(array.values, array.keys)
      else
        array
      end
    end

    def self.integer(array : ::Arrow::ArrayBase, index : Int32) : Int128
      case array
      when ::Arrow::PrimitiveArray(Int8), ::Arrow::PrimitiveArray(Int16), ::Arrow::PrimitiveArray(Int32), ::Arrow::PrimitiveArray(Int64),
           ::Arrow::PrimitiveArray(UInt8), ::Arrow::PrimitiveArray(UInt16), ::Arrow::PrimitiveArray(UInt32), ::Arrow::PrimitiveArray(UInt64), ::Arrow::PrimitiveArray(Int128)
        array.value(index).to_i128
      when ::Arrow::PrimitiveArray(::Arrow::I256)
        array.value(index).low.to_i128!
      else
        raise ArrowError.new("Cannot coerce #{array.data_type} to integer")
      end
    end

    def self.floating(array : ::Arrow::ArrayBase, index : Int32) : Float64
      case array
      when ::Arrow::PrimitiveArray(Float32), ::Arrow::PrimitiveArray(Float64)
        array.value(index).to_f64
      else
        raise ArrowError.new("Cannot coerce #{array.data_type} to floating point")
      end
    end

    def self.bytes(array : ::Arrow::ArrayBase, index : Int32, width : Int32 = 0) : Bytes
      return interval_bytes(array, index) if array.data_type.kind.interval?
      case array
      when ::Arrow::GenericStringArray(Int32), ::Arrow::GenericStringArray(Int64), ::Arrow::StringViewArray
        array.value(index).to_slice
      when ::Arrow::GenericBinaryArray(Int32), ::Arrow::GenericBinaryArray(Int64), ::Arrow::BinaryViewArray, ::Arrow::FixedSizeBinaryArray
        array.value(index)
      when ::Arrow::PrimitiveArray(Int32), ::Arrow::PrimitiveArray(Int64), ::Arrow::PrimitiveArray(Int128)
        buffer = Bytes.new(16)
        IO::ByteFormat::BigEndian.encode(array.value(index).to_i128, buffer)
        buffer[16 - width, width]
      when ::Arrow::PrimitiveArray(::Arrow::I256)
        buffer = array.value(index).to_be_bytes
        buffer[32 - width, width]
      when ::Arrow::PrimitiveArray(::Arrow::Float16)
        buffer = Bytes.new(2)
        IO::ByteFormat::LittleEndian.encode(array.value(index).bits, buffer)
        buffer
      else
        raise NYIError.new("Cannot write #{array.data_type} as bytes")
      end
    end
  end

  # Parquet intervals are (months, days, milliseconds), each a little-endian i32.
  module WriterValues
    private def self.interval_bytes(array : ::Arrow::ArrayBase, index : Int32) : Bytes
      bytes = Bytes.new(12, 0_u8)
      case array
      when ::Arrow::PrimitiveArray(Int32)
        IO::ByteFormat::LittleEndian.encode(array.value(index), bytes[0, 4])
      when ::Arrow::PrimitiveArray(Int64)
        days, millis = ::Arrow.interval_day_time_parts(array.value(index))
        IO::ByteFormat::LittleEndian.encode(days, bytes[4, 4])
        IO::ByteFormat::LittleEndian.encode(millis, bytes[8, 4])
      else
        raise NYIError.new("Attempting to write an Arrow type that is not yet implemented: MonthDayNano interval")
      end
      bytes
    end
  end

  # Encodes a leaf using the existing typed column writer and page store.
  class ArrowColumnWriter
    getter pages : ArrowPageWriter
    @writer : Column::Writer::ColumnWriter
    @distinct : Set(String)?

    def initialize(@descriptor : Schema::ColumnDescriptor, @properties : File::Properties::WriterProperties, @pages)
      @writer = Column::Writer.get_column_writer(@descriptor, @properties, @pages)
      @distinct = Set(String).new if @properties.write_row_group_number_distinct_values
    end

    def write(levels : ArrowLeafColumn) : Nil
      array = WriterValues.decode(levels.array)
      indices = levels.non_null_indices
      if seen = @distinct
        # The pinned source counts dictionary keys and skips byte-view values.
        kind = levels.array.data_type.kind
        unless kind.utf8_view? || kind.binary_view?
          indices.each { |index| seen << String.new(Parquet::Arrow.arrow_value_bytes(levels.array, index)) }
        end
      end
      writer = @writer
      case writer
      when Column::Writer::GenericColumnWriter(Int32)
        values = indices.map do |index|
          number = WriterValues.integer(array, index)
          number = number.tdiv(86_400_000) if array.data_type.kind.date64?
          number.to_i32!
        end
        writer.write_batch(values, levels.def_levels, levels.rep_levels)
      when Column::Writer::GenericColumnWriter(Int64)
        writer.write_batch(indices.map { |index| WriterValues.integer(array, index).to_i64! }, levels.def_levels, levels.rep_levels)
      when Column::Writer::GenericColumnWriter(Bool)
        booleans = array.as(::Arrow::BooleanArray)
        writer.write_batch(indices.map { |index| booleans.value(index) }, levels.def_levels, levels.rep_levels)
      when Column::Writer::GenericColumnWriter(Float32)
        writer.write_batch(indices.map { |index| WriterValues.floating(array, index).to_f32 }, levels.def_levels, levels.rep_levels)
      when Column::Writer::GenericColumnWriter(Float64)
        writer.write_batch(indices.map { |index| WriterValues.floating(array, index) }, levels.def_levels, levels.rep_levels)
      when Column::Writer::GenericColumnWriter(ByteArray)
        writer.write_batch(indices.map { |index| ByteArray.new(WriterValues.bytes(array, index)) }, levels.def_levels, levels.rep_levels)
      when Column::Writer::GenericColumnWriter(FixedLenByteArray)
        width = @descriptor.type_length
        writer.write_batch(indices.map { |index| FixedLenByteArray.new(ByteArray.new(WriterValues.bytes(array, index, width))) }, levels.def_levels, levels.rep_levels)
      else
        raise NYIError.new("Unsupported Arrow column writer #{@descriptor.physical_type}")
      end
      nil
    end

    def add_data_page : Nil
      @writer.add_data_page
    end

    def close : ArrowColumnChunk
      @writer.distinct_count_override = @distinct.try(&.size.to_u64)
      ArrowColumnChunk.new(@pages, @writer.close)
    end

    def memory_size : Int64
      @writer.memory_size
    end

    # ameba:disable Naming/AccessorMethodName
    def get_estimated_total_bytes : Int64
      estimated_total_bytes
    end

    def estimated_total_bytes : Int64
      @writer.estimated_total_bytes
    end
  end
end
