require "../../column/reader/decoder"
require "./byte_array_decoder"

module Parquet::Arrow
  # Native-buffer counterpart of ByteArrayColumnValueDecoder and
  # ByteViewArrayColumnValueDecoder. Pages own their count/encoding state;
  # dictionaries may be replaced independently of the active page.
  class ByteArrayValueDecoder < Parquet::Column::Reader::ColumnValueDecoder
    @validate_utf8 : Bool
    @dict : Array(Parquet::ByteArray)?
    @decoder : ByteArrayDecoder?

    def initialize(descr : Parquet::Schema::ColumnDescriptor, @view : Bool = false)
      @validate_utf8 = descr.converted_type == Parquet::ConvertedType::UTF8
      @dict = nil
      @decoder = nil
    end

    def set_dict(buf : Bytes, num_values : UInt32, encoding : Parquet::Encoding, is_sorted : Bool) : Nil
      unless encoding == Parquet::Encoding::PLAIN || encoding == Parquet::Encoding::PLAIN_DICTIONARY || encoding == Parquet::Encoding::RLE_DICTIONARY
        raise nyi_err!("Invalid/Unsupported encoding type for dictionary: #{encoding}")
      end
      dictionary = [] of Parquet::ByteArray
      ByteArrayDecoderPlain.new(buf, num_values.to_i32, num_values.to_i32, @validate_utf8, @view)
        .read(dictionary, Int32::MAX)
      @dict = dictionary
      _ = is_sorted
    end

    def set_data(encoding : Parquet::Encoding, data : Bytes, num_levels : Int32, num_values : Int32?) : Nil
      @decoder = ByteArrayDecoder.for_encoding(encoding, data, num_levels, num_values, @validate_utf8, @view)
    end

    def read(dest : Array(Parquet::ByteArray) | RecordReader::ArrayValuesBuffer(Parquet::ByteArray), num_values : Int32) : Int32
      decoder = @decoder || raise general_err!("no decoder set")
      values = [] of Parquet::ByteArray
      count = decoder.read(values, num_values, @dict)
      dest.concat(values)
      count
    end

    def skip_values(num_values : Int32) : Int32
      decoder = @decoder || raise general_err!("no decoder set")
      decoder.skip(num_values, @dict)
    end
  end

  class ByteViewArrayValueDecoder < ByteArrayValueDecoder
    def initialize(descr : Parquet::Schema::ColumnDescriptor)
      super(descr, true)
    end
  end
end
