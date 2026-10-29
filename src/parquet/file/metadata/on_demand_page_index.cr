require "./page_index"
require "../page_index/index_reader"

# Selective provider from parquet/examples/custom_page_index.rs (arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b). Promoted from the example
# to a reusable native API. Lookups never fetch; callers explicitly populate
# sparse nested caches. Metadata/file bytes are borrowed and must remain stable.
module Parquet::File::Metadata
  class OnDemandPageIndexProvider < PageIndexProvider
    @column_indexes : Hash(Int32, Hash(Int32, ColumnIndexMetaData))?
    @offset_indexes : Hash(Int32, Hash(Int32, OffsetIndexMetaData))?

    def initialize(@metadata : ParquetMetaData, @file_bytes : Bytes)
    end

    # Populate this chunk's column statistics, unless already cached or absent.
    def fetch_column_index(row_group_idx : Int32, column_idx : Int32) : Nil
      cache = @column_indexes ||= Hash(Int32, Hash(Int32, ColumnIndexMetaData)).new
      row = cache[row_group_idx] ||= Hash(Int32, ColumnIndexMetaData).new
      return if row.has_key?(column_idx)

      column = @metadata.row_group(row_group_idx).column(column_idx)
      if range = column.column_index_range
        row[column_idx] = Parquet::File::PageIndex.decode_column_index(index_bytes(range), column.column_type)
      end
    end

    # Populate this chunk's page locations, unless already cached or absent.
    def fetch_offset_index(row_group_idx : Int32, column_idx : Int32) : Nil
      cache = @offset_indexes ||= Hash(Int32, Hash(Int32, OffsetIndexMetaData)).new
      row = cache[row_group_idx] ||= Hash(Int32, OffsetIndexMetaData).new
      return if row.has_key?(column_idx)

      column = @metadata.row_group(row_group_idx).column(column_idx)
      if range = column.offset_index_range
        row[column_idx] = Parquet::File::PageIndex.decode_offset_index(index_bytes(range))
      end
    end

    # As upstream, these report cache initialization, even after an absent index.
    def column_indexes? : Bool
      !@column_indexes.nil?
    end

    def offset_indexes? : Bool
      !@offset_indexes.nil?
    end

    def column_index(row_group_idx : Int32, column_idx : Int32) : ColumnIndexMetaData?
      @column_indexes.try(&.[row_group_idx]?).try(&.[column_idx]?)
    end

    def offset_index(row_group_idx : Int32, column_idx : Int32) : OffsetIndexMetaData?
      @offset_indexes.try(&.[row_group_idx]?).try(&.[column_idx]?)
    end

    # Upstream derives Clone: metadata and cached index buffers are owned;
    # immutable file bytes remain shared. No previously unfetched slot is read.
    def clone : self
      snapshot = dup
      snapshot.copy_owned_fields(self)
      snapshot
    end

    protected def copy_owned_fields(other : self) : Nil
      @metadata = other.@metadata.clone
      @column_indexes = clone_cache(other.@column_indexes)
      @offset_indexes = clone_cache(other.@offset_indexes)
    end

    private def clone_cache(cache : Hash(Int32, Hash(Int32, T))?) : Hash(Int32, Hash(Int32, T))? forall T
      cache.try do |groups|
        groups.to_h do |group, columns|
          {group, columns.to_h { |column, index| {column, clone_index(index)} }}
        end
      end
    end

    private def clone_index(index : ColumnIndexMetaData) : ColumnIndexMetaData
      ColumnIndexMetaData.new(index.kind, clone_index(index.inner))
    end

    private def clone_index(index : OffsetIndexMetaData) : OffsetIndexMetaData
      OffsetIndexMetaData.new(index.page_locations.dup, index.unencoded_byte_array_data_bytes.try(&.dup))
    end

    {% for type in [Bool, Int32, Int64, Parquet::Int96, Float32, Float64] %}
      private def clone_index(index : ::Parquet::File::PageIndex::PrimitiveColumnIndex({{type}})) : ::Parquet::File::PageIndex::PrimitiveColumnIndex({{type}})
        ::Parquet::File::PageIndex::PrimitiveColumnIndex({{type}}).new(
          clone_common(index.column_index), index.min_values.dup, index.max_values.dup
        )
      end
    {% end %}

    private def clone_index(index : ::Parquet::File::PageIndex::ByteArrayColumnIndex) : ::Parquet::File::PageIndex::ByteArrayColumnIndex
      ::Parquet::File::PageIndex::ByteArrayColumnIndex.new(
        clone_common(index.column_index), index.min_bytes.dup, index.min_offsets.dup,
        index.max_bytes.dup, index.max_offsets.dup
      )
    end

    private def clone_common(index : ::Parquet::File::PageIndex::ColumnIndex) : ::Parquet::File::PageIndex::ColumnIndex
      ::Parquet::File::PageIndex::ColumnIndex.new(
        index.null_pages.dup, index.boundary_order, index.null_counts.try(&.dup),
        index.nan_counts.try(&.dup), index.repetition_level_histograms.try(&.dup),
        index.definition_level_histograms.try(&.dup)
      )
    end

    private def index_bytes(range : Range(UInt64, UInt64)) : Bytes
      if range.end > @file_bytes.size.to_u64 || range.end < range.begin
        raise general_err!("Page index range exceeds file bytes")
      end
      @file_bytes[range.begin.to_i32, (range.end - range.begin).to_i32]
    end
  end
end
