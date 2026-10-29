require "../decoders"
require "../../util/utf8"

# Arrow-specific page codecs from byte_array.rs and byte_view_array.rs,
# apache/arrow-rs 60.0.0 (ef1fa157977633f0ba21aa921f9ef3d5c669235b).
# Native output is Array(ByteArray); offset/view buffers are built by readers.
module Parquet::Arrow
  abstract class ByteArrayDecoder
    def self.for_encoding(encoding : Parquet::Encoding, data : Bytes, num_levels : Int32,
                          num_values : Int32?, validate_utf8 : Bool, view : Bool = false) : ByteArrayDecoder
      case encoding
      when Parquet::Encoding::PLAIN
        ByteArrayDecoderPlain.new(data, num_levels, num_values, validate_utf8, view)
      when Parquet::Encoding::RLE_DICTIONARY, Parquet::Encoding::PLAIN_DICTIONARY
        ByteArrayDecoderDictionary.new(data, num_levels, num_values, view)
      when Parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
        ByteArrayDecoderDeltaLength.new(data, validate_utf8)
      when Parquet::Encoding::DELTA_BYTE_ARRAY
        ByteArrayDecoderDelta.new(data, validate_utf8)
      else
        raise general_err!("unsupported encoding for byte array: #{encoding}")
      end
    end

    abstract def read(output : Array(Parquet::ByteArray), len : Int32, dict : Array(Parquet::ByteArray)?) : Int32
    abstract def skip(len : Int32, dict : Array(Parquet::ByteArray)?) : Int32
  end

  class ByteArrayDecoderPlain < ByteArrayDecoder
    @remaining : Int32
    @offset : Int64

    def initialize(@data : Bytes, num_levels : Int32, num_values : Int32?, @validate_utf8 : Bool, @view : Bool = false)
      @remaining = num_values || num_levels
      @offset = 0_i64
    end

    def read(output : Array(Parquet::ByteArray), len : Int32, dict : Array(Parquet::ByteArray)? = nil) : Int32
      to_read = Math.min(len, @remaining)
      return 0 if !@view && @offset >= @data.size
      read = 0
      while read < to_read && (@view || @offset < @data.size)
        length = next_length
        start = @offset + 4
        finish = start + length
        raise eof_err!("eof decoding byte array") if finish > @data.size
        bytes = @data[start.to_i32, length.to_i32]
        Parquet::Util.check_valid_utf8(bytes) if @validate_utf8
        output << Parquet::ByteArray.new(bytes)
        @offset = finish
        read += 1
      end
      # The pinned offset decoder returns the requested bound even for a
      # corrupt page ending on a complete value before that bound. Views are
      # strict and report EOF instead. Preserve this observable distinction.
      @remaining -= to_read
      to_read
    end

    def skip(len : Int32, dict : Array(Parquet::ByteArray)? = nil) : Int32
      to_skip = Math.min(len, @remaining)
      skipped = 0
      while skipped < to_skip && @offset < @data.size
        @offset += 4 + next_length
        skipped += 1
      end
      @remaining -= skipped
      skipped
    end

    private def next_length : Int64
      if @offset + 4 > @data.size
        raise eof_err!("eof decoding byte array")
      end
      IO::ByteFormat::LittleEndian.decode(UInt32, @data[@offset.to_i32, 4]).to_i64
    end
  end

  class ByteArrayDecoderDeltaLength < ByteArrayDecoder
    @lengths : Array(Int32)
    @length_offset : Int32
    @data_offset : Int32

    def initialize(@data : Bytes, @validate_utf8 : Bool)
      decoder = Parquet::Encodings::DeltaBitPackDecoder(Int32).new
      decoder.set_data(@data, 0)
      @lengths = Array(Int32).new(decoder.values_left, 0)
      decoder.get(@lengths)
      @data_offset = decoder.get_offset
      @length_offset = 0
      total = 0_i64
      @lengths.each do |length|
        raise general_err!("negative delta length byte array length") if length < 0
        total += length
      end
      if total + @data_offset > @data.size
        raise general_err!("Insufficient delta length byte array bytes")
      end
    end

    def read(output : Array(Parquet::ByteArray), len : Int32, dict : Array(Parquet::ByteArray)? = nil) : Int32
      count = Math.min(len, @lengths.size - @length_offset)
      count.times do
        length = @lengths[@length_offset]
        bytes = @data[@data_offset, length]
        Parquet::Util.check_valid_utf8(bytes) if @validate_utf8
        output << Parquet::ByteArray.new(bytes)
        @data_offset += length
        @length_offset += 1
      end
      count
    end

    def skip(len : Int32, dict : Array(Parquet::ByteArray)? = nil) : Int32
      count = Math.min(len, @lengths.size - @length_offset)
      count.times do
        @data_offset += @lengths[@length_offset]
        @length_offset += 1
      end
      count
    end
  end

  class ByteArrayDecoderDelta < ByteArrayDecoder
    def initialize(data : Bytes, @validate_utf8 : Bool)
      @decoder = Decoders::DeltaByteArrayDecoder.new(data)
    end

    def read(output : Array(Parquet::ByteArray), len : Int32, dict : Array(Parquet::ByteArray)? = nil) : Int32
      @decoder.read(len) do |bytes|
        Parquet::Util.check_valid_utf8(bytes) if @validate_utf8
        output << Parquet::ByteArray.new(bytes)
        nil
      end
    end

    def skip(len : Int32, dict : Array(Parquet::ByteArray)? = nil) : Int32
      @decoder.skip(len)
    end
  end

  class ByteArrayDecoderDictionary < ByteArrayDecoder
    def initialize(data : Bytes, num_levels : Int32, num_values : Int32?, @view : Bool = false)
      @decoder = Decoders::DictIndexDecoder.new(data, num_levels, num_values)
    end

    def read(output : Array(Parquet::ByteArray), len : Int32, dict : Array(Parquet::ByteArray)? = nil) : Int32
      dictionary = require_dictionary(dict)
      return 0 if dictionary.empty?
      @decoder.read(len) do |keys|
        keys.each do |key|
          if key < 0 || key >= dictionary.size
            message = @view ? "invalid key=#{key} for dictionary" : "dictionary key beyond bounds of dictionary: 0..#{dictionary.size}"
            raise general_err!(message)
          end
          output << dictionary[key]
        end
        nil
      end
    end

    def skip(len : Int32, dict : Array(Parquet::ByteArray)? = nil) : Int32
      dictionary = require_dictionary(dict)
      return 0 if dictionary.empty?
      @decoder.skip(len)
    end

    private def require_dictionary(dict : Array(Parquet::ByteArray)?) : Array(Parquet::ByteArray)
      dict || raise general_err!(@view ? "dictionary required for dictionary encoding" : "missing dictionary page for column")
    end
  end
end
