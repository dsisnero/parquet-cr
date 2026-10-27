# Ported from `parquet/src/file/metadata/reader.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `PageIndexPolicy` and the synchronous, `ChunkReader`-driven
# `ParquetMetaDataReader` (`parse_and_finish`/`try_parse`/`try_parse_sized`/
# `read_page_indexes`/`read_page_indexes_sized`/`parse_metadata`/
# `decode_footer_metadata`). Fiber-yielding MetadataFetch/MetadataSuffixFetch
# loaders live in async_reader.cr; `with_file_decryption_properties` routes
# the footer decode through
# `parquet_metadata_with_encryption`; `parse_metadata` is public (upstream
# private) for tests.
require "./thrift/encryption"

module Parquet
  module File
    module Metadata
      # Describes the policy for reading page indexes.
      enum PageIndexPolicy
        # Do not read the page index.
        Skip
        # Read the page index if it exists, otherwise do not error.
        Optional
        # Require the page index to exist, and error if it does not.
        #
        # Only enforced for the offset index; this is the same as `Optional` for
        # the column index.
        Required

        # Mirrors Rust `From<bool>`: `true` -> `Required`, `false` -> `Skip`.
        def self.from(value : Bool) : PageIndexPolicy
          value ? Required : Skip
        end
      end

      # Reads `ParquetMetaData` from a `ChunkReader`.
      class ParquetMetaDataReader
        @metadata : ParquetMetaData?
        @column_index : PageIndexPolicy
        @offset_index : PageIndexPolicy
        @metadata_options : ParquetMetaDataOptions?
        @metadata_size : Int32?
        @file_decryption_properties : ::Parquet::Encryption::FileDecryptionProperties?

        def initialize
          @metadata = nil
          @column_index = PageIndexPolicy::Skip
          @offset_index = PageIndexPolicy::Skip
          @metadata_options = nil
          @metadata_size = nil
          @file_decryption_properties = nil
        end

        # Creates a reader populated with `metadata` obtained via other means.
        def self.new_with_metadata(metadata : ParquetMetaData) : ParquetMetaDataReader
          reader = new
          reader.metadata = metadata
          reader
        end

        protected setter metadata : ParquetMetaData?

        # Sets the page index policy for the column and offset indexes.
        def with_page_index_policy(policy : PageIndexPolicy) : self
          @column_index = policy
          @offset_index = policy
          self
        end

        # Sets the page index policy for the column index.
        def with_column_index_policy(policy : PageIndexPolicy) : self
          @column_index = policy
          self
        end

        # Sets the page index policy for the offset index.
        def with_offset_index_policy(policy : PageIndexPolicy) : self
          @offset_index = policy
          self
        end

        # Sets the options to use when decoding.
        def with_metadata_options(options : ParquetMetaDataOptions?) : self
          @metadata_options = options
          self
        end

        # Sets the properties used to decrypt an encrypted footer / column metadata.
        def with_file_decryption_properties(
          properties : ::Parquet::Encryption::FileDecryptionProperties?,
        ) : self
          @file_decryption_properties = properties
          self
        end

        # Whether metadata has already been parsed.
        def has_metadata? : Bool
          !@metadata.nil?
        end

        # Returns the parsed metadata, raising if none is available.
        def finish : ParquetMetaData
          @metadata || raise general_err!("could not parse parquet metadata")
        end

        # Parses and returns the metadata in a single pass.
        def parse_and_finish(reader : Parquet::File::ChunkReader) : ParquetMetaData
          try_parse(reader)
          finish
        end

        # Attempts to parse the footer metadata (and optionally page indexes).
        def try_parse(reader : Parquet::File::ChunkReader) : Nil
          try_parse_sized(reader, reader.len)
        end

        # Same as `try_parse`, but provides the original file size for readers
        # that do not contain the entire file.
        def try_parse_sized(reader : Parquet::File::ChunkReader, file_size : UInt64) : Nil
          @metadata = begin
            parse_metadata(reader)
          rescue ex : Parquet::NeedMoreDataError
            needed = ex.needed
            if file_size == reader.len || needed > file_size
              raise Parquet::EOFError.new(
                "Parquet file too small. Size is #{file_size} but need #{needed}"
              )
            else
              raise ex
            end
          end

          return if @column_index == PageIndexPolicy::Skip && @offset_index == PageIndexPolicy::Skip

          read_page_indexes_sized(reader, file_size)
        end

        # Reads the page index structures when metadata has already been read.
        def read_page_indexes(reader : Parquet::File::ChunkReader) : Nil
          read_page_indexes_sized(reader, reader.len)
        end

        # Reads the page index structures for a reader that does not contain the
        # entire file.
        def read_page_indexes_sized(reader : Parquet::File::ChunkReader, file_size : UInt64) : Nil
          metadata = @metadata
          raise general_err!("Tried to read page indexes without ParquetMetaData metadata") if metadata.nil?

          push_decoder = ParquetMetaDataPushDecoder.try_new_with_metadata(file_size, metadata)
            .with_offset_index_policy(@offset_index)
            .with_column_index_policy(@column_index)
            .with_metadata_options(@metadata_options)
          push_decoder = prepare_push_decoder(push_decoder)

          needed = needs_index_data(push_decoder)
          if needed.is_a?(ParquetMetaData)
            @metadata = needed
            return
          end
          range = needed.as(Range(UInt64, UInt64))

          file_start = file_size > reader.len ? file_size - reader.len : 0_u64
          file_range = file_start..file_size
          unless file_range.includes?(range.begin) && file_range.includes?(range.end)
            if range.end > file_size
              raise Parquet::EOFError.new(
                "Parquet file too small. Range #{range.begin}..#{range.end} is beyond file bounds #{file_size}"
              )
            else
              raise Parquet::NeedMoreDataError.new(file_size - range.begin)
            end
          end

          if metadata_size = @metadata_size
            metadata_start = file_size - metadata_size.to_u64
            if range.end > metadata_start
              raise Parquet::EOFError.new(
                "Parquet file too small. Page index range #{range.begin}..#{range.end} overlaps with file metadata #{metadata_start}..#{file_size}"
              )
            end
          end

          bytes_needed = (range.end - range.begin).to_i32!
          bytes = reader.get_bytes(range.begin - file_start, bytes_needed)
          push_decoder.push_range(range, bytes)
          @metadata = parse_index_data(push_decoder)
        end

        # One-shot parse of the footer. Sets `metadata_size` as a side effect.
        def parse_metadata(reader : Parquet::File::ChunkReader) : ParquetMetaData
          file_size = reader.len
          footer_len = ::Parquet::File::FOOTER_SIZE
          if file_size < footer_len
            raise Parquet::NeedMoreDataError.new(footer_len.to_u64)
          end

          footer_bytes = reader.get_bytes(file_size - footer_len, footer_len)
          footer = FooterTail.try_new(footer_bytes)
          metadata_len = footer.metadata_length
          footer_metadata_len = footer_len.to_u32 + metadata_len
          @metadata_size = footer_metadata_len.to_i32!

          if footer_metadata_len.to_u64 > file_size
            raise Parquet::NeedMoreDataError.new(footer_metadata_len.to_u64)
          end

          start = file_size - footer_metadata_len
          bytes = reader.get_bytes(start, metadata_len.to_i32!)
          decode_footer_metadata(bytes, file_size, footer)
        end

        # Size of the serialized thrift metadata plus the 8-byte footer, if
        # `parse_metadata` has been called.
        def metadata_size : Int32?
          @metadata_size
        end

        # Decodes `ParquetMetaData` from the provided metadata bytes (without
        # the 8-byte footer).
        def decode_footer_metadata(buf : Bytes, file_size : UInt64, footer_tail : FooterTail) : ParquetMetaData
          footer_len = ::Parquet::File::FOOTER_SIZE.to_u64
          ending_offset = file_size >= footer_len ? file_size - footer_len : raise general_err!(
            "file size #{file_size} is smaller than footer size #{footer_len}"
          )
          starting_offset = ending_offset >= buf.size ? ending_offset - buf.size : raise general_err!(
            "file size #{file_size} is smaller than buffer size #{buf.size} + footer size #{footer_len}"
          )
          range = starting_offset...ending_offset

          push_decoder = ParquetMetaDataPushDecoder.try_new_with_footer_tail(file_size, footer_tail)
            .with_page_index_policy(PageIndexPolicy::Skip)
            .with_metadata_options(@metadata_options)
          push_decoder = prepare_push_decoder(push_decoder)
          push_decoder.push_range(range, buf)

          result = push_decoder.try_decode
          if result.data?
            result.data
          elsif result.finished?
            raise general_err!("could not parse parquet metadata -- previously finished")
          else
            raise general_err!("could not parse parquet metadata, needs ranges #{result.ranges}")
          end
        end

        # Decodes `ParquetMetaData` from the provided metadata bytes.
        def self.decode_metadata(buf : Bytes) : ParquetMetaData
          Metadata.decode_metadata(buf, nil)
        end

        # Like `decode_metadata` but accepts metadata parsing options.
        def self.decode_metadata_with_options(buf : Bytes, options : ParquetMetaDataOptions?) : ParquetMetaData
          Metadata.decode_metadata(buf, options)
        end

        # Decodes the schema from the Parquet footer metadata bytes.
        def self.decode_schema(buf : Bytes) : Parquet::Schema::SchemaDescriptor
          decode_metadata(buf).file_metadata.schema_descr
        end

        private def prepare_push_decoder(push_decoder : ParquetMetaDataPushDecoder) : ParquetMetaDataPushDecoder
          push_decoder.with_file_decryption_properties(@file_decryption_properties)
        end

        # Determines the combined range of bytes needed to read the page
        # indexes, or returns the metadata when no further data is needed.
        private def needs_index_data(push_decoder : ParquetMetaDataPushDecoder) : ParquetMetaData | Range(UInt64, UInt64)
          result = push_decoder.try_decode
          if result.needs_data?
            ranges = result.ranges
            raise general_err!("Internal error: no ranges provided") if ranges.empty?

            ranges.reduce { |a, b| Math.min(a.begin, b.begin)...Math.max(a.end, b.end) }
          elsif result.data?
            result.data
          else
            raise general_err!("Internal error: decoder was finished")
          end
        end

        # Runs a push decoder that has had the needed ranges pushed to it.
        private def parse_index_data(push_decoder : ParquetMetaDataPushDecoder) : ParquetMetaData
          result = push_decoder.try_decode
          if result.needs_data?
            raise general_err!("Internal error: decoder still needs data after reading required range")
          elsif result.data?
            result.data
          else
            raise general_err!("Internal error: decoder was finished")
          end
        end
      end
    end
  end
end
