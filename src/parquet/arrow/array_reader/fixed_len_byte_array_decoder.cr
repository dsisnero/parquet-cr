require "../../column/reader/decoder"
require "../decoders"
require "../record_reader/buffer"
require "../buffer/bit_util"

# fixed_len_byte_array.rs, apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b.
module Parquet::Arrow
  class FixedLenByteArrayBuffer < RecordReader::ValuesBuffer
    getter buffer : Array(UInt8)
    getter byte_length : Int32?
    getter values_capacity : Int32?

    def self.with_capacity(capacity : Int32) : FixedLenByteArrayBuffer
      new(capacity)
    end

    def initialize(capacity : Int32 = 0)
      @buffer = [] of UInt8
      @byte_length = nil
      @values_capacity = capacity
      @allocated_capacity = 0
    end

    def reserve_exact(additional : Int32) : Nil
      if width = @byte_length
        reserve_bytes(@buffer.size + additional * width)
      else
        @values_capacity = Math.max(@values_capacity || 0, additional)
      end
    end

    def ensure_byte_length(width : Int32) : Nil
      if current = @byte_length
        raise general_err!("fixed-length buffer width mismatch: #{current} != #{width}") unless current == width
      else
        @byte_length = width
        if @buffer.empty?
          reserve_bytes((@values_capacity || 0) * width)
          @values_capacity = nil
        end
      end
    end

    def pad_nulls(read_offset : Int32, values_read : Int32, levels_read : Int32, valid_mask : Bytes) : Nil
      width = @byte_length || 0
      expected = (read_offset + values_read) * width
      unless @buffer.size == expected
        raise general_err!("found inconsistent buffer length while padding nulls: expected #{expected} bytes, got #{@buffer.size}")
      end
      target = (read_offset + levels_read) * width
      if @buffer.size > target
        @buffer.truncate(0...target)
      else
        (target - @buffer.size).times { @buffer << 0_u8 }
      end
      remaining = values_read
      value_pos = read_offset + values_read - 1
      Buffer.iter_set_bits_rev(valid_mask).each do |level_pos|
        break if remaining == 0 || level_pos <= value_pos
        width.times { |i| @buffer[level_pos * width + i] = @buffer[value_pos * width + i] }
        remaining -= 1
        value_pos -= 1
      end
    end

    # Array-valued conversion adapters retain the existing native Arrow API.
    def values : Array(Parquet::FixedLenByteArray)
      width = @byte_length
      return [] of Parquet::FixedLenByteArray unless width
      bytes = Bytes.new(@buffer.to_unsafe, @buffer.size)
      Array(Parquet::FixedLenByteArray).new(@buffer.size // width) do |i|
        Parquet::FixedLenByteArray.new(Parquet::ByteArray.new(bytes[i * width, width]))
      end
    end

    # Crystal has no public Array::reserve API. Preserve the reservation lower
    # bound by replacing the backing array when a larger hint is required.
    private def reserve_bytes(capacity : Int32) : Nil
      if capacity > @allocated_capacity
        @buffer = Array(UInt8).new(capacity).concat(@buffer)
        @allocated_capacity = capacity
      end
    end
  end

  class FixedLenByteArrayValueDecoder < Parquet::Column::Reader::ColumnValueDecoder
    @byte_length : Int32
    @dict : Bytes?
    @encoding : Parquet::Encoding?
    @indices : Decoders::DictIndexDecoder?
    @delta : Decoders::DeltaByteArrayDecoder?

    def initialize(desc : Parquet::Schema::ColumnDescriptor)
      @byte_length = desc.type_length
      raise general_err!("fixed byte length must be positive") if @byte_length <= 0
      @dict = nil
      @encoding = nil
      @indices = nil
      @delta = nil
      @data = Bytes.empty
      @offset = 0
    end

    def set_dict(buf : Bytes, num_values : UInt32, encoding : Parquet::Encoding, is_sorted : Bool) : Nil
      unless encoding == Parquet::Encoding::PLAIN || encoding == Parquet::Encoding::RLE_DICTIONARY || encoding == Parquet::Encoding::PLAIN_DICTIONARY
        raise nyi_err!("Invalid/Unsupported encoding type for dictionary: #{encoding}")
      end
      expected = num_values.to_i64 * @byte_length
      if expected > buf.size
        raise general_err!("too few bytes in dictionary page, expected #{expected} got #{buf.size}")
      end
      @dict = buf
      _ = is_sorted
    end

    def set_data(encoding : Parquet::Encoding, data : Bytes, num_levels : Int32, num_values : Int32?) : Nil
      case encoding
      when Parquet::Encoding::PLAIN, Parquet::Encoding::BYTE_STREAM_SPLIT
        @data = data
        @offset = 0
      when Parquet::Encoding::RLE_DICTIONARY, Parquet::Encoding::PLAIN_DICTIONARY
        @indices = Decoders::DictIndexDecoder.new(data, num_levels, num_values)
      when Parquet::Encoding::DELTA_BYTE_ARRAY
        @delta = Decoders::DeltaByteArrayDecoder.new(data)
      else
        raise general_err!("unsupported encoding for fixed length byte array: #{encoding}")
      end
      @encoding = encoding
    end

    def read(output : FixedLenByteArrayBuffer, num_values : Int32) : Int32
      output.ensure_byte_length(@byte_length)
      case @encoding || raise general_err!("no decoder set")
      when Parquet::Encoding::PLAIN
        count = Math.min(num_values, @data.size // @byte_length - @offset)
        output.buffer.concat(@data[@offset * @byte_length, count * @byte_length])
        @offset += count
        count
      when Parquet::Encoding::BYTE_STREAM_SPLIT
        total = @data.size // @byte_length
        count = Math.min(num_values, total - @offset)
        count.times do |i|
          @byte_length.times { |j| output.buffer << @data[j * total + @offset + i] }
        end
        @offset += count
        count
      when Parquet::Encoding::RLE_DICTIONARY, Parquet::Encoding::PLAIN_DICTIONARY
        read_dictionary(output, num_values)
      when Parquet::Encoding::DELTA_BYTE_ARRAY
        delta = @delta || raise general_err!("no delta decoder set")
        delta.read(num_values) do |bytes|
          unless bytes.size == @byte_length
            raise general_err!("encountered array with incorrect length, got #{bytes.size} expected #{@byte_length}")
          end
          output.buffer.concat(bytes)
          nil
        end
      else
        raise general_err!("unsupported encoding for fixed length byte array")
      end
    end

    private def read_dictionary(output : FixedLenByteArrayBuffer, num_values : Int32) : Int32
      dictionary = @dict || raise general_err!("missing dictionary page for column")
      return 0 if dictionary.empty?
      indices = @indices || raise general_err!("no dictionary decoder set")
      indices.read(num_values) do |keys|
        keys.each do |key|
          offset = key.to_i64 * @byte_length
          if offset < 0 || offset + @byte_length > dictionary.size
            raise general_err!("dictionary key beyond bounds of dictionary: #{key}")
          end
          output.buffer.concat(dictionary[offset.to_i32, @byte_length])
        end
        nil
      end
    end

    def skip_values(num_values : Int32) : Int32
      case @encoding || raise general_err!("no decoder set")
      when Parquet::Encoding::PLAIN, Parquet::Encoding::BYTE_STREAM_SPLIT
        count = Math.min(num_values, @data.size // @byte_length - @offset)
        @offset += count
        count
      when Parquet::Encoding::RLE_DICTIONARY, Parquet::Encoding::PLAIN_DICTIONARY
        (@indices || raise general_err!("no dictionary decoder set")).skip(num_values)
      when Parquet::Encoding::DELTA_BYTE_ARRAY
        (@delta || raise general_err!("no delta decoder set")).skip(num_values)
      else
        raise general_err!("unsupported encoding for fixed length byte array")
      end
    end
  end
end
