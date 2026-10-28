require "../../column/reader/decoder"
require "../../util/utf8"

module Parquet::Arrow
  # Shared Arrow byte-array decoding contract from byte_array.rs and
  # byte_view_array.rs (arrow-rs 60.0.0). The physical decoder supplies values;
  # STRING annotations require validation before they reach the record buffer.
  # Unlike the physical column API, dictionary entries are validated on load,
  # even when an entry is never referenced. Skipping data values deliberately
  # delegates without inspecting UTF-8, matching upstream.
  #
  # Native allocation divergence: output remains ArrayValuesBuffer(ByteArray),
  # not OffsetBuffer/ViewBuffer. Dictionary validation decodes a temporary
  # plain buffer before installing the physical dictionary decoder.
  class ByteArrayValueDecoder < Parquet::Column::Reader::ColumnValueDecoder
    @validate_utf8 : Bool

    def initialize(@descr : Parquet::Schema::ColumnDescriptor)
      @decoder = Parquet::Column::Reader::ColumnValueDecoderImpl(Parquet::ByteArray).new(@descr)
      @validate_utf8 = @descr.converted_type == Parquet::ConvertedType::UTF8
    end

    def set_dict(buf : Bytes, num_values : UInt32, encoding : Parquet::Encoding, is_sorted : Bool) : Nil
      unless encoding == Parquet::Encoding::PLAIN || encoding == Parquet::Encoding::PLAIN_DICTIONARY || encoding == Parquet::Encoding::RLE_DICTIONARY
        raise nyi_err!("Invalid/Unsupported encoding type for dictionary: #{encoding}")
      end
      if @validate_utf8
        plain = Parquet::Encodings::PlainDecoder(Parquet::ByteArray).new(@descr.type_length)
        plain.set_data(buf, num_values.to_i32)
        values = Array(Parquet::ByteArray).new(num_values.to_i32) { Parquet::ByteArray.new }
        count = plain.get(values)
        count.times { |i| Parquet::Util.check_valid_utf8(values[i].data) }
      end
      @decoder.set_dict(buf, num_values, encoding, is_sorted)
    end

    def set_data(encoding : Parquet::Encoding, data : Bytes, num_levels : Int32, num_values : Int32?) : Nil
      @decoder.set_data(encoding, data, num_levels, num_values)
    end

    def read(dest : Array(Parquet::ByteArray) | RecordReader::ArrayValuesBuffer(Parquet::ByteArray), num_values : Int32) : Int32
      values = [] of Parquet::ByteArray
      count = @decoder.read(values, num_values)
      if @validate_utf8
        values.each { |value| Parquet::Util.check_valid_utf8(value.data) }
      end
      dest.concat(values)
      count
    end

    def skip_values(num_values : Int32) : Int32
      @decoder.skip_values(num_values)
    end
  end
end
