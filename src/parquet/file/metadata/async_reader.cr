require "./reader"
require "../../arrow/async_reader/metadata"

# Async loaders from parquet/src/file/metadata/reader.rs, arrow-rs 60.0.0.
# Fetch methods yield through Crystal IO; existing footer/decryption/index
# decoders are shared with the synchronous reader. No full-file buffer is used.
module Parquet::File::Metadata
  class ParquetMetaDataReader
    @prefetch_hint : Int32? = nil

    def with_prefetch_hint(hint : Int32?) : self
      raise ArgumentError.new("prefetch hint must be nonnegative") if hint && hint < 0
      @prefetch_hint = hint
      self
    end

    def with_arrow_reader_options(options : Parquet::Arrow::ArrowReaderOptions?) : self
      if options
        with_metadata_options(options.metadata_options)
        with_column_index_policy(options.column_index_policy)
        with_offset_index_policy(options.offset_index_policy)
        with_file_decryption_properties(options.file_decryption_properties)
      end
      self
    end

    def load_and_finish(fetch : Parquet::Arrow::MetadataFetch, file_size : UInt64) : ParquetMetaData
      try_load(fetch, file_size)
      finish
    end

    def load_via_suffix_and_finish(fetch : Parquet::Arrow::MetadataSuffixFetch) : ParquetMetaData
      try_load_via_suffix(fetch)
      finish
    end

    def try_load(fetch : Parquet::Arrow::MetadataFetch, file_size : UInt64) : Nil
      metadata, remainder = load_metadata(fetch, file_size)
      @metadata = metadata
      load_page_index_with_remainder(fetch, remainder) unless @column_index.skip? && @offset_index.skip?
    end

    def try_load_via_suffix(fetch : Parquet::Arrow::MetadataSuffixFetch) : Nil
      metadata, remainder = load_metadata_via_suffix(fetch)
      @metadata = metadata
      load_page_index_with_remainder(fetch, remainder) unless @column_index.skip? && @offset_index.skip?
    end

    def load_page_index(fetch : Parquet::Arrow::MetadataFetch) : Nil
      load_page_index_with_remainder(fetch, nil)
    end

    private def get_prefetch_size : Int32
      Math.max(@prefetch_hint || 0, Parquet::File::FOOTER_SIZE)
    end

    private def load_metadata(fetch : Parquet::Arrow::MetadataFetch, file_size : UInt64) : Tuple(ParquetMetaData, Tuple(UInt64, Bytes)?)
      raise Parquet::EOFError.new("file size of #{file_size} is less than footer") if file_size < 8
      count = Math.min(get_prefetch_size.to_u64, file_size).to_i32
      start = file_size - count
      suffix = fetch.fetch(start...file_size)
      check_metadata_length(suffix, count)
      footer = FooterTail.try_new(suffix[suffix.size - 8, 8])
      length = footer.metadata_length.to_i32
      if file_size < length.to_u64 + 8
        raise Parquet::EOFError.new("file size of #{file_size} is less than footer + metadata #{length.to_u64 + 8}")
      end
      if length > suffix.size - 8
        bytes = fetch.fetch((file_size - length - 8)...(file_size - 8))
        check_metadata_length(bytes, length)
        {decode_footer_metadata(bytes, file_size, footer), nil}
      else
        offset = suffix.size - length - 8
        bytes = suffix[offset, length]
        {decode_footer_metadata(bytes, file_size, footer), {start, suffix[0, offset]}}
      end
    end

    private def load_metadata_via_suffix(fetch : Parquet::Arrow::MetadataSuffixFetch) : Tuple(ParquetMetaData, Tuple(UInt64, Bytes)?)
      suffix = fetch.fetch_suffix(get_prefetch_size)
      if suffix.size < 8
        raise Parquet::EOFError.new("footer metadata requires 8 bytes, but could only read #{suffix.size}")
      end
      footer = FooterTail.try_new(suffix[suffix.size - 8, 8])
      length = footer.metadata_length.to_i32
      file_size = length.to_u64 + 8
      if length > suffix.size - 8
        bytes = fetch.fetch_suffix(length + 8)
        check_metadata_length(bytes, length + 8)
        {decode_footer_metadata(bytes[0, length], file_size, footer), nil}
      else
        offset = suffix.size - length - 8
        {decode_footer_metadata(suffix[offset, length], file_size, footer), {0_u64, suffix[0, offset]}}
      end
    end

    private def check_metadata_length(bytes : Bytes, expected : Int32) : Nil
      if bytes.size < expected
        raise Parquet::EOFError.new("metadata requires #{expected} bytes, but could only read #{bytes.size}")
      end
    end

    private def load_page_index_with_remainder(fetch : Parquet::Arrow::MetadataFetch, remainder : Tuple(UInt64, Bytes)?) : Nil
      metadata = @metadata || raise general_err!("Footer metadata is not present")
      @metadata = nil
      decoder = ParquetMetaDataPushDecoder.try_new_with_metadata(UInt64::MAX, metadata)
        .with_column_index_policy(@column_index).with_offset_index_policy(@offset_index).with_metadata_options(@metadata_options)
      decoder = prepare_push_decoder(decoder)
      needed = needs_index_data(decoder)
      if needed.is_a?(ParquetMetaData)
        @metadata = needed
        return
      end
      range = needed
      bytes = if remainder && remainder[0] <= range.begin
                start, buffer = remainder
                offset = (range.begin - start).to_i32
                ending = range.end - start
                if ending > buffer.size
                  raise general_err!("Corrupted parquet file: index data range (#{range}) exceeds remainder length (#{buffer.size})")
                end
                buffer[offset, (range.end - range.begin).to_i32]
              else
                fetch.fetch(range)
              end
      decoder.push_range(range, bytes)
      @metadata = parse_index_data(decoder)
    end
  end
end
