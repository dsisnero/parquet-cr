require "digest/crc32"
require "../errors"
require "../thrift"
require "../basic"
require "../format"
require "../compression_codecs"
require "./metadata/column_chunk"
require "./properties"
require "./statistics"
require "./chunk_reader"
require "../column/page"
require "../encryption/decrypt"

# Ported from `parquet/src/file/serialized_reader.rs` (`decode_page` and
# `SerializedPageReader`) (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the serialized page reader (the offset-stream state) and `decode_page`.
# Divergence: the offset-index-driven `Pages` state is not ported; `R:
# ChunkReader` is a concrete generic parameter. The `crc`-feature page-checksum
# verification is always performed (Crystal has no feature flags). Page
# decryption is also wired through the core `SerializedRowGroupReader` (upstream
# only attaches the crypto context in the arrow reader).
module Parquet
  module File
    # Decodes a `Page` from the provided `buffer`.
    # ameba:disable Metrics/CyclomaticComplexity
    def self.decode_page(
      page_header : Format::PageHeader,
      buffer : Bytes,
      physical_type : Parquet::Type,
      decompressor : CompressionCodecs::Codec?,
    ) : Column::Page
      if expected_crc = page_header.crc
        # Upstream compares `crc::checksum(buffer) != expected_crc as u32`: the
        # thrift field is a signed `i32`, so the conversion truncates rather
        # than raising on negative checksums (which occur for real files).
        if Digest::CRC32.checksum(buffer) != expected_crc.to_u32!
          raise general_err!("Page CRC checksum mismatch")
        end
      end

      offset = 0
      can_decompress = true

      if header_v2 = page_header.data_page_header_v2
        def_len = header_v2.definition_levels_byte_length
        rep_len = header_v2.repetition_levels_byte_length
        if def_len < 0 || rep_len < 0 || def_len + rep_len > page_header.uncompressed_page_size
          raise general_err!(
            "DataPage v2 header contains implausible values for definition_levels_byte_length " \
            "(#{def_len}) and repetition_levels_byte_length (#{rep_len}) given DataPage header " \
            "provides uncompressed_page_size (#{page_header.uncompressed_page_size})"
          )
        end
        offset = def_len + rep_len
        compressed_flag = header_v2.is_compressed
        can_decompress = compressed_flag.nil? ? true : compressed_flag
      end

      if (dec = decompressor) && can_decompress
        uncompressed_page_size = page_header.uncompressed_page_size
        if offset > buffer.size || offset > uncompressed_page_size
          raise general_err!("Invalid page header")
        end

        decompressed_size = uncompressed_page_size - offset
        decompressed = [] of UInt8
        decompressed.concat(buffer[0, offset])
        if decompressed_size > 0
          dec.decompress(buffer[offset, buffer.size - offset], decompressed, decompressed_size)
        end

        if decompressed.size != uncompressed_page_size
          raise general_err!(
            "Actual decompressed size doesn't match the expected one (#{decompressed.size} vs #{uncompressed_page_size})"
          )
        end
        buffer = Slice.new(decompressed.to_unsafe, decompressed.size).dup
      end

      case page_header.type
      when PageType::DICTIONARY_PAGE
        dict_header = page_header.dictionary_page_header || raise general_err!("Missing dictionary page header")
        sorted_flag = dict_header.is_sorted
        is_sorted = sorted_flag.nil? ? false : sorted_flag
        Column::Page::DictionaryPage.new(buffer, dict_header.num_values.to_u32!, dict_header.encoding, is_sorted)
      when PageType::DATA_PAGE
        header = page_header.data_page_header || raise general_err!("Missing V1 data page header")
        Column::Page::DataPage.new(
          buffer,
          header.num_values.to_u32!,
          header.encoding,
          header.definition_level_encoding,
          header.repetition_level_encoding,
          Statistics.from_thrift_page_stats(physical_type, header.statistics),
        )
      when PageType::DATA_PAGE_V2
        header = page_header.data_page_header_v2 || raise general_err!("Missing V2 data page header")
        compressed_flag = header.is_compressed
        is_compressed = compressed_flag.nil? ? true : compressed_flag
        Column::Page::DataPageV2.new(
          buffer,
          header.num_values.to_u32!,
          header.encoding,
          header.num_nulls.to_u32!,
          header.num_rows.to_u32!,
          header.definition_levels_byte_length.to_u32!,
          header.repetition_levels_byte_length.to_u32!,
          is_compressed,
          Statistics.from_thrift_page_stats(physical_type, header.statistics),
        )
      else
        raise general_err!("Page type #{page_header.type} is not supported")
      end
    end

    # An `IO` wrapper that counts the bytes read through it.
    private class CountingIO < IO
      getter count : Int32

      def initialize(@io : IO)
        @count = 0
      end

      def read(slice : Bytes) : Int32
        read = @io.read(slice)
        @count += read if read > 0
        read
      end

      def write(slice : Bytes) : Nil
        @io.write(slice)
      end
    end

    # A serialized implementation of `PageReader` for a column chunk.
    class SerializedPageReader(R) < Column::PageReader
      def initialize(
        @reader : R,
        @decompressor : CompressionCodecs::Codec?,
        @physical_type : Parquet::Type,
        @offset : UInt64,
        @remaining_bytes : UInt64,
        @require_dictionary : Bool,
        @read_stats : Bool,
      )
        @next_page_header = nil.as(Format::PageHeader?)
        @page_index = 0
        @crypto_context = nil.as(Encryption::CryptoContext?)
      end

      # Adds the crypto context needed to decrypt this column's pages.
      def add_crypto_context(
        rg_idx : Int32,
        column_idx : Int32,
        parquet_meta_data : Metadata::ParquetMetaData,
        column_chunk_metadata : Metadata::ColumnChunkMetaData,
      ) : self
        file_decryptor = parquet_meta_data.file_decryptor
        return self unless file_decryptor

        crypto_metadata = column_chunk_metadata.crypto_metadata
        return self unless crypto_metadata

        @crypto_context = Encryption::CryptoContext.for_column(
          file_decryptor, crypto_metadata, rg_idx, column_idx
        )
        self
      end

      # Creates a reader for a column chunk.
      def self.new_for_column(
        reader : R,
        meta : Metadata::ColumnChunkMetaData,
        total_rows : Int32,
        page_locations : Array(Metadata::PageLocation)?,
        props : Properties::ReaderProperties,
      ) : SerializedPageReader(R) forall R
        raise nyi_err!("SerializedPageReader with page locations is not supported") unless page_locations.nil?

        _ = total_rows
        decompressor = CompressionCodecs.create_codec(Compression.from_codec(meta.compression), props.codec_options)
        start = (meta.dictionary_page_offset || meta.data_page_offset).to_u64
        length = meta.total_compressed_size.to_u64
        new(reader, decompressor, meta.column_type, start, length, !meta.dictionary_page_offset.nil?, props.read_page_stats)
      end

      # ameba:disable Naming/AccessorMethodName
      def get_next_page : Column::Page?
        loop do
          return nil if @remaining_bytes == 0

          read = @reader.get_read(@offset)
          header =
            if buffered = @next_page_header
              @next_page_header = nil
              buffered
            else
              header_len, parsed = read_page_header_len(read, @page_index, @require_dictionary)
              verify_page_header_len(header_len, @remaining_bytes)
              @offset += header_len.to_u64
              @remaining_bytes -= header_len.to_u64
              parsed
            end

          verify_page_size(header.compressed_page_size, header.uncompressed_page_size, @remaining_bytes)

          data_len = header.compressed_page_size
          data_start = @offset
          @offset += data_len.to_u64
          @remaining_bytes -= data_len.to_u64

          next if header.type == PageType::INDEX_PAGE

          buffer = @reader.get_bytes(data_start, data_len)
          buffer = decrypt_page_data(buffer, @page_index, @require_dictionary)
          page = File.decode_page(header, buffer, @physical_type, @decompressor)
          if page.is_data_page?
            @page_index += 1
          elsif page.is_dictionary_page?
            @require_dictionary = false
          end
          return page
        end
      end

      def peek_next_page : Column::PageMetadata?
        loop do
          return nil if @remaining_bytes == 0

          if buffered = @next_page_header
            metadata = Column::PageMetadata.from_thrift(buffered)
            return metadata
          end

          read = @reader.get_read(@offset)
          header_len, header = read_page_header_len(read, @page_index, @require_dictionary)
          verify_page_header_len(header_len, @remaining_bytes)
          @offset += header_len.to_u64
          @remaining_bytes -= header_len.to_u64

          if header.type == PageType::INDEX_PAGE
            next
          end

          @next_page_header = header
          return Column::PageMetadata.from_thrift(header)
        end
      end

      def skip_next_page : Nil
        if buffered = @next_page_header
          verify_page_size(buffered.compressed_page_size, buffered.uncompressed_page_size, @remaining_bytes)
          data_page_size = buffered.compressed_page_size.to_u64
          @offset += data_page_size
          @remaining_bytes -= data_page_size
          @next_page_header = nil
        else
          read = @reader.get_read(@offset)
          header_len, header = read_page_header_len(read, @page_index, @require_dictionary)
          verify_page_header_len(header_len, @remaining_bytes)
          verify_page_size(header.compressed_page_size, header.uncompressed_page_size, @remaining_bytes)
          data_page_size = header.compressed_page_size.to_u64
          @offset += header_len.to_u64 + data_page_size
          @remaining_bytes -= header_len.to_u64 + data_page_size
        end

        if @require_dictionary
          @require_dictionary = false
        else
          @page_index += 1
        end
        nil
      end

      private def read_page_header_len(
        input : IO,
        page_index : Int32,
        dictionary_page : Bool,
      ) : Tuple(Int32, Format::PageHeader)
        counting = CountingIO.new(input)
        header = read_page_header(counting, page_index, dictionary_page)
        {counting.count, header}
      end

      private def read_page_header(
        input : IO,
        page_index : Int32,
        dictionary_page : Bool,
      ) : Format::PageHeader
        if context = page_crypto_context(page_index, dictionary_page)
          buffer =
            begin
              Encryption.read_and_decrypt(
                context.data_decryptor, input, context.create_page_header_aad
              )
            rescue
              raise general_err!(
                "Error decrypting page header for column #{context.column_ordinal}, " \
                "decryption key may be wrong"
              )
            end
          protocol = Thrift::ThriftSliceInputProtocol.new(buffer)
          if @read_stats
            Format::PageHeader.read_thrift(protocol)
          else
            Format.read_page_header_without_stats(protocol)
          end
        else
          protocol = Thrift::ThriftReadInputProtocol.new(input)
          if @read_stats
            Format::PageHeader.read_thrift(protocol)
          else
            Format.read_page_header_without_stats(protocol)
          end
        end
      end

      private def decrypt_page_data(
        buffer : Bytes,
        page_index : Int32,
        dictionary_page : Bool,
      ) : Bytes
        return buffer unless context = page_crypto_context(page_index, dictionary_page)
        context.data_decryptor.decrypt(buffer, context.create_page_aad)
      end

      private def page_crypto_context(
        page_index : Int32,
        dictionary_page : Bool,
      ) : Encryption::CryptoContext?
        context = @crypto_context
        return nil unless context
        dictionary_page ? context.for_dictionary_page : context.with_page_ordinal(page_index)
      end

      private def verify_page_header_len(header_len : Int32, remaining_bytes : UInt64) : Nil
        if header_len.to_u64 > remaining_bytes
          raise eof_err!("Invalid page header")
        end
      end

      private def verify_page_size(compressed_size : Int32, uncompressed_size : Int32, remaining_bytes : UInt64) : Nil
        if compressed_size < 0 || compressed_size.to_u64 > remaining_bytes || uncompressed_size < 0
          raise eof_err!("Invalid page header")
        end
      end
    end
  end
end
