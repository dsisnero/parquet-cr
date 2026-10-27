# Ported from `parquet/src/file/metadata/parser.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `MetadataParser`, `parse_page_index`, and the column/offset index
# helpers. Divergence: the encryption branch is not ported, so
# `MetadataParser#decode_metadata` errors on an encrypted footer (as upstream
# does when the `encryption` feature is disabled). Footer loaders route encrypted
# metadata through the encryption decoder; schema/statistics options apply to
# both encrypted and plaintext Thrift metadata.
module Parquet
  module File
    module Metadata
      # Decodes thrift-encoded metadata (encryption not ported).
      struct MetadataParser
        getter metadata_options : ParquetMetaDataOptions?

        def initialize(@metadata_options : ParquetMetaDataOptions? = nil)
        end

        def with_metadata_options(options : ParquetMetaDataOptions?) : MetadataParser
          MetadataParser.new(options)
        end

        def decode_metadata(buf : Bytes, encrypted_footer : Bool) : ParquetMetaData
          if encrypted_footer
            raise general_err!(
              "Parquet file has an encrypted footer but the encryption feature is disabled"
            )
          end

          Metadata.decode_metadata(buf, @metadata_options)
        end
      end

      # Decodes `ParquetMetaData` from the thrift-encoded footer bytes.
      def self.decode_metadata(buf : Bytes, options : ParquetMetaDataOptions? = nil) : ParquetMetaData
        parquet_metadata_from_bytes(buf, options)
      end

      # Parses the page index from `bytes` and attaches it to `metadata`.
      #
      # `start_offset` is the offset where `bytes` begin in the file.
      def self.parse_page_index(
        metadata : ParquetMetaData,
        column_index_policy : PageIndexPolicy,
        offset_index_policy : PageIndexPolicy,
        bytes : Bytes,
        start_offset : UInt64,
      ) : Nil
        return if column_index_policy == PageIndexPolicy::Skip && offset_index_policy == PageIndexPolicy::Skip

        num_row_groups = metadata.num_row_groups
        num_columns = metadata.file_metadata.schema_descr.num_columns
        builder = PageIndexBuilder.new(0, 0)

        unless column_index_policy == PageIndexPolicy::Skip
          builder.allocate_column_indexes(num_row_groups, num_columns)
          parse_column_index(metadata, builder, bytes, start_offset)
        end
        unless offset_index_policy == PageIndexPolicy::Skip
          builder.allocate_offset_indexes(num_row_groups, num_columns)
          parse_offset_index(metadata, offset_index_policy, builder, bytes, start_offset)
        end

        page_index = builder.build
        return unless page_index.column_indexes? || page_index.offset_indexes?

        metadata.set_page_index(page_index)
      end

      # Returns the byte range needed to read the offset/page indexes for the
      # given policies, or `nil` if no page indexes are needed.
      def self.range_for_page_index(
        metadata : ParquetMetaData,
        column_index_policy : PageIndexPolicy,
        offset_index_policy : PageIndexPolicy,
      ) : Range(UInt64, UInt64)?
        range = nil.as(Range(UInt64, UInt64)?)
        metadata.row_groups.each do |row_group|
          row_group.num_columns.times do |col_idx|
            column = row_group.column(col_idx)
            unless column_index_policy == PageIndexPolicy::Skip
              range = ::Parquet::File::PageIndex.acc_range(range, column.column_index_range)
            end
            unless offset_index_policy == PageIndexPolicy::Skip
              range = ::Parquet::File::PageIndex.acc_range(range, column.offset_index_range)
            end
          end
        end
        range
      end

      private def self.parse_column_index(
        metadata : ParquetMetaData,
        page_index_builder : PageIndexBuilder,
        bytes : Bytes,
        start_offset : UInt64,
      ) : Nil
        metadata.num_row_groups.times do |rg_idx|
          row_group = metadata.row_group(rg_idx)
          row_group.num_columns.times do |col_idx|
            column = row_group.column(col_idx)
            next unless range = column.column_index_range

            index = ::Parquet::File::PageIndex.decode_column_index(
              slice_range(bytes, range, start_offset), column.column_type
            )
            page_index_builder.put_column_index(index, rg_idx, col_idx)
          end
        end
      end

      private def self.parse_offset_index(
        metadata : ParquetMetaData,
        offset_index_policy : PageIndexPolicy,
        page_index_builder : PageIndexBuilder,
        bytes : Bytes,
        start_offset : UInt64,
      ) : Nil
        metadata.num_row_groups.times do |rg_idx|
          row_group = metadata.row_group(rg_idx)
          row_group.num_columns.times do |col_idx|
            column = row_group.column(col_idx)
            if range = column.offset_index_range
              index = ::Parquet::File::PageIndex.decode_offset_index(
                slice_range(bytes, range, start_offset)
              )
              page_index_builder.put_offset_index(index, rg_idx, col_idx)
            elsif offset_index_policy == PageIndexPolicy::Required
              raise general_err!("missing offset index")
            end
          end
        end
      end

      private def self.slice_range(bytes : Bytes, range : Range(UInt64, UInt64), start_offset : UInt64) : Bytes
        r_start = (range.begin - start_offset).to_i32
        r_end = (range.end - start_offset).to_i32
        bytes[r_start, r_end - r_start]
      end
    end
  end
end
