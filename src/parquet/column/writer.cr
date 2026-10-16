require "./writer/encoder"
require "../file/metadata"
require "../file/metadata/column_chunk"
require "../file/page_index/column_index"
require "../bloom_filter"

# Ported from `parquet/src/column/writer/mod.rs` (data holders)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `ColumnCloseResult`, `PageMetrics`, `ColumnMetrics<T>`, and
# `LevelDataRef`.
#
# Divergence: the Rust `LevelDataRef<'a>` enum (borrowed `&[i16]`) is a Crystal
# struct carrying a `Kind` with an owned `Array(Int16)`; `usize` becomes `Int32`;
# `ColumnCloseResult::update_dictionary_location(mut self)` mutates and returns
# `self`.
module Parquet
  module Column
    module Writer
      # The result of closing a column writer.
      class ColumnCloseResult
        property bytes_written : UInt64
        property rows_written : UInt64
        property metadata : File::Metadata::ColumnChunkMetaData
        property bloom_filter : BloomFilter::Sbbf?
        property column_index : File::PageIndex::ColumnIndexMetaData?
        property offset_index : File::Metadata::OffsetIndexMetaData?

        def initialize(
          @bytes_written : UInt64,
          @rows_written : UInt64,
          @metadata : File::Metadata::ColumnChunkMetaData,
          @bloom_filter : BloomFilter::Sbbf? = nil,
          @column_index : File::PageIndex::ColumnIndexMetaData? = nil,
          @offset_index : File::Metadata::OffsetIndexMetaData? = nil,
        )
        end

        # Rewrite the page offsets for a dictionary-first on-disk layout.
        def update_dictionary_location(dictionary_len : Int32) : ColumnCloseResult
          if dictionary_len > 0
            @metadata = @metadata.into_builder
              .set_dictionary_page_offset(0_i64)
              .set_data_page_offset(dictionary_len.to_i64)
              .build
            if offset_index = @offset_index
              locations = offset_index.page_locations
              offset = dictionary_len.to_i64
              index = 0
              while index < locations.size
                location = locations[index]
                location.offset = offset
                offset += location.compressed_page_size.to_i64
                locations[index] = location
                index += 1
              end
            end
          end
          self
        end
      end

      # Metrics per page.
      class PageMetrics
        property num_buffered_values : UInt32
        property num_buffered_rows : UInt32
        property page_size_exemption : Int32
        property num_page_nulls : UInt64
        property num_page_nans : UInt64?
        property repetition_level_histogram : File::Metadata::LevelHistogram?
        property definition_level_histogram : File::Metadata::LevelHistogram?

        def initialize
          @num_buffered_values = 0_u32
          @num_buffered_rows = 0_u32
          @page_size_exemption = 0
          @num_page_nulls = 0_u64
          @num_page_nans = nil
          @repetition_level_histogram = nil
          @definition_level_histogram = nil
        end

        def with_repetition_level_histogram(max_level : Int16) : PageMetrics
          @repetition_level_histogram = File::Metadata::LevelHistogram.try_new(max_level)
          self
        end

        def with_definition_level_histogram(max_level : Int16) : PageMetrics
          @definition_level_histogram = File::Metadata::LevelHistogram.try_new(max_level)
          self
        end

        # Resets this `PageMetrics` to the initial state.
        def new_page : Nil
          @num_buffered_values = 0_u32
          @num_buffered_rows = 0_u32
          @page_size_exemption = 0
          @num_page_nulls = 0_u64
          @num_page_nans = nil
          @repetition_level_histogram.try(&.reset)
          @definition_level_histogram.try(&.reset)
          nil
        end
      end

      # Metrics per column writer.
      class ColumnMetrics(T)
        property total_bytes_written : UInt64
        property total_rows_written : UInt64
        property total_uncompressed_size : UInt64
        property total_compressed_size : UInt64
        property total_num_values : UInt64
        property dictionary_page_offset : UInt64?
        property data_page_offset : UInt64?
        property min_column_value : T?
        property max_column_value : T?
        property num_column_nulls : UInt64
        property num_column_nans : UInt64?
        property column_distinct_count : UInt64?
        property variable_length_bytes : Int64?
        property repetition_level_histogram : File::Metadata::LevelHistogram?
        property definition_level_histogram : File::Metadata::LevelHistogram?

        def initialize
          @total_bytes_written = 0_u64
          @total_rows_written = 0_u64
          @total_uncompressed_size = 0_u64
          @total_compressed_size = 0_u64
          @total_num_values = 0_u64
          @dictionary_page_offset = nil
          @data_page_offset = nil
          @min_column_value = nil.as(T?)
          @max_column_value = nil.as(T?)
          @num_column_nulls = 0_u64
          @num_column_nans = nil
          @column_distinct_count = nil
          @variable_length_bytes = nil
          @repetition_level_histogram = nil
          @definition_level_histogram = nil
        end

        def with_repetition_level_histogram(max_level : Int16) : ColumnMetrics(T)
          @repetition_level_histogram = File::Metadata::LevelHistogram.try_new(max_level)
          self
        end

        def with_definition_level_histogram(max_level : Int16) : ColumnMetrics(T)
          @definition_level_histogram = File::Metadata::LevelHistogram.try_new(max_level)
          self
        end

        # Sums `page_histogram` into `chunk_histogram`.
        def self.update_histogram(
          chunk_histogram : File::Metadata::LevelHistogram?,
          page_histogram : File::Metadata::LevelHistogram?,
        ) : Nil
          if (page_hist = page_histogram) && (chunk_hist = chunk_histogram)
            chunk_hist.add(page_hist)
          end
        end

        # Sums the provided `PageMetrics` histograms into the chunk histograms.
        def update_from_page_metrics(page_metrics : PageMetrics) : Nil
          ColumnMetrics(T).update_histogram(@definition_level_histogram, page_metrics.definition_level_histogram)
          ColumnMetrics(T).update_histogram(@repetition_level_histogram, page_metrics.repetition_level_histogram)
        end

        # Sums the page variable-length bytes into the chunk total.
        def update_variable_length_bytes(variable_length_bytes : Int64?) : Nil
          if var_bytes = variable_length_bytes
            @variable_length_bytes = (@variable_length_bytes || 0_i64) + var_bytes
          end
        end
      end

      # Borrowed view of level data.
      struct LevelDataRef
        enum Kind
          Absent
          Materialized
          Uniform
        end

        getter kind : Kind
        getter levels : Array(Int16)?
        getter uniform_value : Int16
        getter uniform_count : Int32

        private def initialize(@kind : Kind, @levels : Array(Int16)? = nil, @uniform_value : Int16 = 0_i16, @uniform_count : Int32 = 0)
        end

        def self.absent : LevelDataRef
          new(Kind::Absent)
        end

        def self.materialized(levels : Array(Int16)) : LevelDataRef
          new(Kind::Materialized, levels)
        end

        def self.uniform(value : Int16, count : Int32) : LevelDataRef
          new(Kind::Uniform, nil, value, count)
        end

        # Builds a `LevelDataRef` from optional levels.
        def self.from(levels : Array(Int16)?) : LevelDataRef
          if levels.nil?
            absent
          else
            materialized(levels)
          end
        end

        def len : Int32
          case @kind
          when Kind::Absent       then 0
          when Kind::Materialized then (@levels || [] of Int16).size
          else                         @uniform_count
          end
        end

        def first : Int16?
          case @kind
          when Kind::Absent
            nil
          when Kind::Materialized
            (@levels || [] of Int16).first?
          else
            @uniform_count > 0 ? @uniform_value : nil
          end
        end

        def value_at(idx : Int32) : Int16?
          case @kind
          when Kind::Absent
            nil
          when Kind::Materialized
            (@levels || [] of Int16)[idx]?
          else
            idx < @uniform_count ? @uniform_value : nil
          end
        end

        def slice(offset : Int32, len : Int32) : LevelDataRef
          case @kind
          when Kind::Absent
            self
          when Kind::Materialized
            LevelDataRef.materialized((@levels || [] of Int16)[offset, len])
          else
            LevelDataRef.uniform(@uniform_value, len)
          end
        end

        # Count of positions that represent an actual value (level == `max_def`).
        def value_count(total : Int32, max_def : Int16) : Int32
          case @kind
          when Kind::Absent
            total
          when Kind::Materialized
            count = 0
            (@levels || [] of Int16).each { |level| count += 1 if level == max_def }
            count
          else
            @uniform_value == max_def ? @uniform_count : 0
          end
        end
      end
    end
  end
end
