require "../file/chunk_reader"
require "../file/properties"
require "../file/serialized_reader"
require "../file/metadata"
require "../file/metadata/page_index"
require "./array_reader"
require "./mod"

# Ported from `parquet/src/arrow/in_memory_row_group.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `InMemoryRowGroup` holds the column chunks of one row group in memory, as
# caller-supplied bytes, and is a `RowGroups` source for the array readers. It
# is the basis of the push decoder: `fetch_ranges` says which byte ranges to
# fetch (whole column chunks, or only the pages a row selection needs when an
# offset index is available), and `fill_column_chunks` assembles the fetched
# bytes into dense or sparse `ColumnChunkData`.
#
# Divergences: `usize` counts/offsets are `Int32`, byte ranges are
# `Range(UInt64, UInt64)`, `FetchRanges` is a struct, the `ColumnChunkData` enum
# is an abstract `ChunkReader` with `Dense`/`Sparse` subclasses, `fetch_ranges`
# takes an explicit `batch_size`, and `fill_column_chunks` takes an
# `Enumerable(Bytes)`.
module Parquet
  module Arrow
    # An in-memory column chunk (upstream `ColumnChunkData`).
    #
    # It is a `ChunkReader` so that a `SerializedPageReader` can read pages from
    # it, reading either a dense byte buffer or individual data pages held
    # sparsely.
    abstract class ColumnChunkData < File::ChunkReader
      # Total length of the full column chunk.
      abstract def len : UInt64

      # Returns the bytes from `start` to the end of the chunk/page.
      abstract def get(start : UInt64) : Bytes

      def get_bytes(start : UInt64, length : Int32) : Bytes
        bytes = get(start)
        bytes[0, length]
      end

      def get_read(start : UInt64) : IO
        IO::Memory.new(get(start))
      end

      # A full column chunk and its offset within the original file.
      class Dense < ColumnChunkData
        # Offset of `data` within the parquet file.
        getter offset : Int32
        getter data : Bytes

        def initialize(@offset : Int32, @data : Bytes)
        end

        def len : UInt64
          @data.size.to_u64
        end

        def get(start : UInt64) : Bytes
          index = start.to_i32 - @offset
          if index < 0 || index > @data.size
            raise general_err!("Invalid offset #{start} in dense column chunk data")
          end
          @data[index, @data.size - index]
        end
      end

      # A subset of a column chunk's data pages (e.g. when a row selection only
      # needs some pages).
      class Sparse < ColumnChunkData
        # Length of the full column chunk.
        getter length : Int32
        # `{page offset within the file, page bytes}`, ordered by offset.
        getter data : Array(Tuple(UInt64, Bytes))

        def initialize(@length : Int32, @data : Array(Tuple(UInt64, Bytes)))
        end

        def len : UInt64
          @length.to_u64
        end

        def get(start : UInt64) : Bytes
          @data.each do |(offset, bytes)|
            return bytes if offset == start
          end

          raise general_err!(
            "Invalid offset in sparse column chunk data: #{start}, no matching page found." \
            "If you are using a `SelectionStrategyPolicy::Mask`, ensure that the OffsetIndex " \
            "is provided when creating the InMemoryRowGroup."
          )
        end
      end
    end

    # An iterator over a single column chunk's page reader
    # (upstream `ColumnChunkIterator`).
    class ColumnChunkIterator
      include Column::PageIterator

      def initialize(@reader : Column::PageReader?)
      end

      def next
        reader = @reader
        @reader = nil
        reader || Iterator::Stop::INSTANCE
      end
    end

    # An in-memory collection of column chunks (upstream `InMemoryRowGroup`).
    class InMemoryRowGroup < RowGroups
      getter page_index : File::Metadata::RowGroupPageIndex?
      # Column chunks for this row group; `nil` means "not fetched".
      getter column_chunks : Array(ColumnChunkData?)
      getter row_count : Int32
      getter row_group_idx : Int32
      getter metadata : File::Metadata::ParquetMetaData

      def initialize(
        @row_count : Int32,
        @column_chunks : Array(ColumnChunkData?),
        @page_index : File::Metadata::RowGroupPageIndex?,
        @row_group_idx : Int32,
        @metadata : File::Metadata::ParquetMetaData,
      )
      end

      # What ranges to fetch for the columns in this row group (upstream
      # `FetchRanges`).
      struct FetchRanges
        # The byte ranges to fetch.
        getter ranges : Array(Range(UInt64, UInt64))
        # If non-nil, the start offsets of each page for each column chunk.
        getter page_start_offsets : Array(Array(UInt64))?

        def initialize(@ranges : Array(Range(UInt64, UInt64)), @page_start_offsets : Array(Array(UInt64))?)
        end
      end

      # Returns the byte ranges to fetch for the columns specified in
      # `projection` and `selection` (upstream `fetch_ranges`).
      #
      # `cache_mask` indicates which columns, if any, are being cached by
      # `RowGroupCache`. The `selection` for cached columns is expanded to batch
      # boundaries to simplify accounting for what data is cached.
      #
      # When a selection and an offset index are available, only the pages that
      # contain selected rows are fetched (plus the dictionary page when the
      # first page does not start at the beginning of the column chunk).
      # Otherwise the whole column chunks are fetched.
      def fetch_ranges(
        projection : ProjectionMask,
        selection : RowSelection?,
        batch_size : Int32,
        cache_mask : ProjectionMask?,
      ) : FetchRanges
        row_group = @metadata.row_group(@row_group_idx)

        if (sel = selection) && (page_index = @page_index)
          expanded_selection = sel.expand_to_batch_boundaries(batch_size, @row_count)
          page_start_offsets = [] of Array(UInt64)

          ranges = [] of Range(UInt64, UInt64)
          @column_chunks.each_with_index do |chunk, idx|
            next unless chunk.nil? && projection.leaf_included?(idx)

            column_metadata = row_group.column(idx)
            start, length = column_metadata.byte_range
            column_ranges = [] of Range(UInt64, UInt64)

            offset_index = page_index.offset_index(idx)
            unless offset_index
              # No offset index for this column: fetch the entire column.
              ranges << (start.to_u64...(start + length).to_u64)
              next
            end

            locations = offset_index.page_locations
            # If the first page does not start at the beginning of the column,
            # the preceding bytes hold a dictionary page.
            if first = locations.first?
              column_ranges << (start.to_u64...(first.offset.to_u64)) unless first.offset.to_u64 == start.to_u64
            end
            # Expand the selection to batch boundaries for cached columns (see
            # the doc comment for this function).
            use_expanded = cache_mask.try(&.leaf_included?(idx)) || false
            column_ranges.concat(
              (use_expanded ? expanded_selection : sel).scan_ranges(locations)
            )

            page_start_offsets << column_ranges.map(&.begin)
            ranges.concat(column_ranges)
          end

          FetchRanges.new(ranges, page_start_offsets)
        else
          ranges = [] of Range(UInt64, UInt64)
          @column_chunks.each_with_index do |chunk, idx|
            next unless chunk.nil? && projection.leaf_included?(idx)

            start, length = row_group.column(idx).byte_range
            ranges << (start.to_u64...(start + length).to_u64)
          end

          FetchRanges.new(ranges, nil)
        end
      end

      # Fills `column_chunks` with the data fetched for the ranges returned by
      # `fetch_ranges` (upstream `fill_column_chunks`).
      def fill_column_chunks(
        projection : ProjectionMask,
        page_start_offsets : Array(Array(UInt64))?,
        chunk_data : Enumerable(Bytes),
      ) : Nil
        chunks = chunk_data.to_a
        row_group = @metadata.row_group(@row_group_idx)
        chunk_index = 0

        if offsets_per_column = page_start_offsets
          offsets_index = 0
          @column_chunks.each_with_index do |chunk, idx|
            next if !chunk.nil? || !projection.leaf_included?(idx)

            offsets = offsets_per_column[offsets_index]?
            offsets_index += 1
            next unless offsets

            data = [] of Tuple(UInt64, Bytes)
            offsets.each do |offset|
              data << {offset, chunks[chunk_index]}
              chunk_index += 1
            end

            _, length = row_group.column(idx).byte_range
            @column_chunks[idx] = ColumnChunkData::Sparse.new(length.to_i32, data)
          end
        else
          @column_chunks.each_with_index do |chunk, idx|
            next if !chunk.nil? || !projection.leaf_included?(idx)

            start, _ = row_group.column(idx).byte_range
            data = chunks[chunk_index]?
            chunk_index += 1
            next unless data

            @column_chunks[idx] = ColumnChunkData::Dense.new(start.to_i32, data)
          end
        end
        nil
      end

      def num_rows : Int64
        @row_count.to_i64
      end

      def column_chunks(i : Int32) : Column::PageIterator
        data = @column_chunks[i]?
        unless data
          raise general_err!("Invalid column index #{i}, column was not fetched")
        end

        page_locations = @page_index.try(&.page_locations(i))
        column_metadata = @metadata.row_group(@row_group_idx).column(i)
        page_reader = File::SerializedPageReader.new_for_column(
          data, column_metadata, @row_count, page_locations,
          File::Properties::ReaderProperties.builder.build,
        )
        page_reader = page_reader.add_crypto_context(@row_group_idx, i, @metadata, column_metadata)
        ColumnChunkIterator.new(page_reader)
      end

      def row_groups : Iterator(File::Metadata::RowGroupMetaData)
        [@metadata.row_group(@row_group_idx)].each
      end
    end
  end
end
