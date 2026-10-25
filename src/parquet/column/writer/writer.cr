require "../writer"
require "../writer/encoder"
require "../writer/truncate"
require "../writer/byte_budget_chunker"
require "../../file/metadata"
require "../../file/metadata/column_chunk"
require "../../file/page_index/column_index"
require "../../encryption/encrypt"

# Ported from `parquet/src/column/writer/mod.rs` (`ColumnWriter`,
# `GenericColumnWriter`, `get_column_writer`/`get_typed_column_writer*`)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Divergences: `GenericColumnWriter<'a, E>` becomes `GenericColumnWriter(T)`
# fixing `E = ColumnValueEncoderImpl(T)`; the `ColumnWriter` enum becomes an
# abstract class with the single generic `ColumnWriterImpl(T)` subclass;
# `usize` becomes `Int32`; Arrow metrics and distinct overrides are exposed;
# `encodings` is a `Set(Encoding)`; truncation helpers are `Writer` module
# methods.
module Parquet
  module Column
    module Writer
      # A column writer for a Parquet column chunk.
      abstract class ColumnWriter
        property distinct_count_override : UInt64?

        # Finalizes writes and returns the column chunk metadata and indexes.
        abstract def close : ColumnCloseResult
        abstract def memory_size : Int64
        abstract def estimated_total_bytes : Int64
        abstract def add_data_page : Nil

        def get_estimated_total_bytes : UInt64
          estimated_total_bytes.to_u64
        end

        def set_distinct_count_override(count : UInt64) : Nil
          self.distinct_count_override = count
        end
      end

      # Typed column writer for a primitive column.
      alias ColumnWriterImpl = GenericColumnWriter

      # A generic column writer for a primitive Parquet column.
      class GenericColumnWriter(T) < ColumnWriter
        @column_props : File::Properties::ResolvedColumnProperties
        @codec : Compression
        @compressor : CompressionCodecs::Codec?
        @encoder : ColumnValueEncoderImpl(T)
        @encodings : Set(Encoding)
        @page_metrics : PageMetrics
        @column_metrics : ColumnMetrics(T)
        @column_index_builder : File::Metadata::ColumnIndexBuilder
        @offset_index_builder : File::Metadata::OffsetIndexBuilder?
        @def_levels_encoder : Encodings::LevelEncoder
        @rep_levels_encoder : Encodings::LevelEncoder
        @data_pages : Deque(Column::CompressedPage)
        @encoding_stats : Array(File::Metadata::PageEncodingStats)
        @data_page_boundary_ascending : Bool
        @data_page_boundary_descending : Bool
        @last_non_null_data_page_min_max : Tuple(T, T)?

        getter descr : Schema::ColumnDescriptor
        getter props : File::Properties::WriterProperties
        getter column_props : File::Properties::ResolvedColumnProperties
        getter page_writer : Column::PageWriter
        getter codec : Compression
        getter compressor : CompressionCodecs::Codec?

        def initialize(
          @descr : Schema::ColumnDescriptor,
          @props : File::Properties::WriterProperties,
          @page_writer : Column::PageWriter,
        )
          @column_props = @props.resolve_column_properties(@descr.path)
          @codec = @column_props.compression
          @compressor = CompressionCodecs.create_codec(@codec, CodecOptions.new(true))
          @encoder = ColumnValueEncoderImpl(T).try_new(@descr, @props, @column_props)

          statistics_enabled = @column_props.statistics_enabled

          @encodings = Set(Encoding).new
          @encodings << Encoding::RLE

          @page_metrics = PageMetrics.new
          @column_metrics = ColumnMetrics(T).new
          if statistics_enabled != File::Properties::EnabledStatistics::None
            @page_metrics = @page_metrics
              .with_repetition_level_histogram(@descr.max_rep_level)
              .with_definition_level_histogram(@descr.max_def_level)
            @column_metrics = @column_metrics
              .with_repetition_level_histogram(@descr.max_rep_level)
              .with_definition_level_histogram(@descr.max_def_level)
          end

          @column_index_builder = File::Metadata::ColumnIndexBuilder.new(@descr.physical_type)
          @column_index_builder.to_invalid if statistics_enabled != File::Properties::EnabledStatistics::Page

          @offset_index_builder =
            if @props.offset_index_disabled
              nil
            else
              File::Metadata::OffsetIndexBuilder.new
            end

          @def_levels_encoder = create_level_encoder(@descr.max_def_level, @props)
          @rep_levels_encoder = create_level_encoder(@descr.max_rep_level, @props)
          @data_pages = Deque(Column::CompressedPage).new
          @encoding_stats = [] of File::Metadata::PageEncodingStats
          @data_page_boundary_ascending = true
          @data_page_boundary_descending = true
          @last_non_null_data_page_min_max = nil.as(Tuple(T, T)?)
        end

        # Writes a batch of values, definition levels and repetition levels.
        # Returns the number of values processed.
        def write_batch(values : Array(T), def_levels : Array(Int16)?, rep_levels : Array(Int16)?) : Int32
          write_batch_internal(values, LevelDataRef.from(def_levels), LevelDataRef.from(rep_levels), nil, nil, nil)
        end

        # Writes a batch with pre-calculated chunk statistics.
        def write_batch_with_statistics(
          values : Array(T),
          def_levels : Array(Int16)?,
          rep_levels : Array(Int16)?,
          min : T?,
          max : T?,
          distinct_count : UInt64?,
        ) : Int32
          write_batch_internal(values, LevelDataRef.from(def_levels), LevelDataRef.from(rep_levels), min, max, distinct_count)
        end

        def memory_size : Int64
          @encoder.estimated_memory_size.to_i64 + @data_pages.sum(0_i64, &.memory_usage.to_i64) + @page_writer.buffered_memory_size
        end

        def estimated_total_bytes : Int64
          @data_pages.sum(0_i64, &.data.size.to_i64) + @column_metrics.total_bytes_written.to_i64 + @encoder.estimated_data_page_size + (@encoder.estimated_dict_page_size || 0)
        end

        def get_total_bytes_written : UInt64
          @column_metrics.total_bytes_written
        end

        def get_total_rows_written : UInt64
          @column_metrics.total_rows_written
        end

        def get_descriptor : Schema::ColumnDescriptor
          @descr
        end

        # Finalizes writes and returns the column chunk metadata and indexes.
        def close : ColumnCloseResult
          add_data_page if @page_metrics.num_buffered_values > 0
          write_dictionary_page if @encoder.has_dictionary
          flush_data_pages

          metadata = build_column_metadata
          @page_writer.close

          write_bloom_filter = @props.bloom_filter_for_dictionary_encoded_chunks || has_non_dictionary_data_page
          bloom_filter = @encoder.flush_bloom_filter
          bloom_filter = nil unless write_bloom_filter

          boundary_order =
            if @data_page_boundary_ascending
              BoundaryOrder::ASCENDING
            elsif @data_page_boundary_descending
              BoundaryOrder::DESCENDING
            else
              BoundaryOrder::UNORDERED
            end
          @column_index_builder.set_boundary_order(boundary_order)

          column_index = @column_index_builder.valid? ? @column_index_builder.build : nil
          offset_index = @offset_index_builder.try(&.build)

          ColumnCloseResult.new(
            @column_metrics.total_bytes_written,
            @column_metrics.total_rows_written,
            metadata,
            bloom_filter,
            column_index,
            offset_index,
          )
        end

        private def write_batch_internal(values, def_levels : LevelDataRef, rep_levels : LevelDataRef, min : T?, max : T?, distinct_count : UInt64?) : Int32
          if def_levels.len != 0 && rep_levels.len != 0 && def_levels.len != rep_levels.len
            raise general_err!("Inconsistent length of definition and repetition levels: #{def_levels.len} != #{rep_levels.len}")
          end

          num_levels = Math.max(def_levels.len, rep_levels.len)
          num_levels = values.size if num_levels == 0

          # `min`/`max` are nilable `T`, so they must be tested against nil: a
          # BOOLEAN `false` would otherwise be treated as absent.
          @column_metrics.min_column_value = Writer.update_min(@descr, min, @column_metrics.min_column_value) unless min.nil?
          @column_metrics.max_column_value = Writer.update_max(@descr, max, @column_metrics.max_column_value) unless max.nil?

          @column_metrics.column_distinct_count = @encoder.num_values == 0 ? distinct_count : nil

          values_offset = 0
          levels_offset = 0
          both_levels_compact = !def_levels.materialized? && !rep_levels.materialized?
          has_levels = !def_levels.absent? || !rep_levels.absent?
          base_batch_size = (both_levels_compact && has_levels) ? @props.data_page_row_count_limit : @props.write_batch_size

          chunker = ByteBudgetChunker.new(@descr, @column_props, base_batch_size)
          while levels_offset < num_levels
            end_offset = Math.min(num_levels, levels_offset + base_batch_size)
            if levels = rep_levels.levels
              while end_offset < levels.size && levels[end_offset] != 0
                end_offset += 1
              end
            end

            chunk_size = end_offset - levels_offset
            chunk_def = def_levels.slice(levels_offset, chunk_size)
            chunk_rep = rep_levels.slice(levels_offset, chunk_size)

            sub_batch = chunker.pick_sub_batch(T, @encoder, values, nil, chunk_def, values_offset, chunk_size)
            if sub_batch.nil?
              values_offset += write_mini_batch(values, values_offset, chunk_size, chunk_def, chunk_rep)
            else
              values_offset += write_granular_chunk(values, values_offset, chunk_size, chunk_def, chunk_rep, sub_batch.not_nil!)
            end
            levels_offset = end_offset
          end
          values_offset
        end

        private def write_granular_chunk(values : Array(T), values_offset : Int32, chunk_size : Int32, chunk_def : LevelDataRef, chunk_rep : LevelDataRef, sub_batch : SubBatchStrategy) : Int32
          max_def_level = @descr.max_def_level
          values_consumed = 0
          sub_start = 0
          while sub_start < chunk_size
            window_end =
              case sub_batch.kind
              when SubBatchStrategy::Kind::Values
                window_end_for_values(chunk_def, chunk_size, max_def_level, sub_start, sub_batch.size)
              else
                Math.min(sub_start + sub_batch.size, chunk_size)
              end

            sub_end =
              if levels = chunk_rep.levels
                e = window_end
                while e < chunk_size && levels[e] != 0
                  e += 1
                end
                e
              else
                window_end
              end

            sub_len = sub_end - sub_start
            written = write_mini_batch(
              values,
              values_offset + values_consumed,
              sub_len,
              chunk_def.slice(sub_start, sub_len),
              chunk_rep.slice(sub_start, sub_len),
            )
            values_consumed += written
            sub_start = sub_end
          end
          values_consumed
        end

        private def has_non_dictionary_data_page : Bool
          @encoding_stats.any? do |stats|
            (stats.page_type == PageType::DATA_PAGE || stats.page_type == PageType::DATA_PAGE_V2) &&
              stats.encoding != Encoding::PLAIN_DICTIONARY && stats.encoding != Encoding::RLE_DICTIONARY
          end
        end

        private def window_end_for_values(chunk_def : LevelDataRef, chunk_size : Int32, max_def_level : Int16, start : Int32, max_values : Int32) : Int32
          if !chunk_def.materialized?
            return Math.min(start + max_values, chunk_size) if chunk_def.absent? || chunk_def.uniform_value == max_def_level
            return chunk_size
          end

          levels = chunk_def.levels.not_nil!
          seen = 0
          e = start
          while e < chunk_size && seen < max_values
            seen += 1 if levels[e] == max_def_level
            e += 1
          end
          e
        end

        private def create_level_encoder(max_level : Int16, props : File::Properties::WriterProperties) : Encodings::LevelEncoder
          case props.writer_version
          when File::Properties::WriterVersion::PARQUET_1_0
            Encodings::LevelEncoder.v1_streaming(max_level)
          else
            Encodings::LevelEncoder.v2_streaming(max_level)
          end
        end

        private def write_mini_batch(values : Array(T), values_offset : Int32, num_levels : Int32, def_levels : LevelDataRef, rep_levels : LevelDataRef) : Int32
          values_to_write =
            if @descr.max_def_level > 0
              max_def = @descr.max_def_level
              if !def_levels.materialized?
                if def_levels.absent?
                  raise general_err!("Definition levels are required, because max definition level = #{@descr.max_def_level}")
                end
                encoder = @def_levels_encoder
                if hist = @page_metrics.definition_level_histogram
                  histogram = hist
                  encoder.put_n_with_observer(def_levels.uniform_value, def_levels.uniform_count) { |level, run_len| histogram.increment_by(level, run_len.to_i64) }
                else
                  encoder.put_n_with_observer(def_levels.uniform_value, def_levels.uniform_count) { |_, _| }
                end
                count = def_levels.uniform_count
                v = count * (def_levels.uniform_value == max_def ? 1 : 0)
                @page_metrics.num_page_nulls += (count - v).to_u64
                v
              else
                levels = def_levels.levels.not_nil!
                v = 0
                encoder = @def_levels_encoder
                if hist = @page_metrics.definition_level_histogram
                  histogram = hist
                  encoder.put_with_observer(levels) do |level, count|
                    v += count * (level == max_def ? 1 : 0)
                    histogram.increment_by(level, count.to_i64)
                  end
                else
                  encoder.put_with_observer(levels) do |level, count|
                    v += count * (level == max_def ? 1 : 0)
                  end
                end
                @page_metrics.num_page_nulls += (levels.size - v).to_u64
                v
              end
            else
              num_levels
            end

          if @descr.max_rep_level > 0
            first_level = rep_levels.first
            if first_level.nil?
              raise general_err!("Repetition levels are required, because max repetition level = #{@descr.max_rep_level}")
            end
            if first_level != 0
              raise general_err!("Write must start at a record boundary, got non-zero repetition level of #{first_level}")
            end

            new_rows = 0_u32
            encoder = @rep_levels_encoder
            if !rep_levels.materialized?
              if hist = @page_metrics.repetition_level_histogram
                histogram = hist
                encoder.put_n_with_observer(rep_levels.uniform_value, rep_levels.uniform_count) do |level, run_len|
                  new_rows += run_len.to_u32 * (level == 0 ? 1_u32 : 0_u32)
                  histogram.increment_by(level, run_len.to_i64)
                end
              else
                encoder.put_n_with_observer(rep_levels.uniform_value, rep_levels.uniform_count) do |level, run_len|
                  new_rows += run_len.to_u32 * (level == 0 ? 1_u32 : 0_u32)
                end
              end
            else
              levels = rep_levels.levels.not_nil!
              if hist = @page_metrics.repetition_level_histogram
                histogram = hist
                encoder.put_with_observer(levels) do |level, count|
                  new_rows += count.to_u32 * (level == 0 ? 1_u32 : 0_u32)
                  histogram.increment_by(level, count.to_i64)
                end
              else
                encoder.put_with_observer(levels) do |level, count|
                  new_rows += count.to_u32 * (level == 0 ? 1_u32 : 0_u32)
                end
              end
            end
            @page_metrics.num_buffered_rows += new_rows
          else
            @page_metrics.num_buffered_rows += num_levels.to_u32
          end

          @encoder.write(values, values_offset, values_to_write)

          page_was_empty = @page_metrics.num_buffered_values == 0
          @page_metrics.num_buffered_values += num_levels.to_u32
          set_page_size_exemption if page_was_empty && values_to_write == 1

          add_data_page if should_add_data_page
          dict_fallback if should_dict_fallback

          values_to_write
        end

        private def should_dict_fallback : Bool
          size = @encoder.estimated_dict_page_size
          size.nil? ? false : size >= @column_props.dictionary_page_size_limit
        end

        private def set_page_size_exemption : Nil
          return unless @encoder.compresses_against_previous_value

          size = @encoder.estimated_data_page_size
          @page_metrics.page_size_exemption = size if size >= @column_props.data_page_size_limit
        end

        private def should_add_data_page : Bool
          return false if @page_metrics.num_buffered_values == 0

          @page_metrics.num_buffered_rows >= @props.data_page_row_count_limit ||
            (@encoder.estimated_data_page_size - @page_metrics.page_size_exemption) >= @column_props.data_page_size_limit
        end

        private def dict_fallback : Nil
          add_data_page if @page_metrics.num_buffered_values > 0
          write_dictionary_page
          flush_data_pages
        end

        private def nan_count_opt : Int64?
          case @descr.physical_type
          when Parquet::Type::FLOAT, Parquet::Type::DOUBLE
            (@page_metrics.num_page_nans || 0_u64).to_i64
          when Parquet::Type::FIXED_LEN_BYTE_ARRAY
            if @descr.logical_type_ref.is_a?(LogicalType::Float16)
              (@page_metrics.num_page_nans || 0_u64).to_i64
            end
          end
        end

        private def update_column_offset_index(page_statistics : ValueStatistics(T)?, page_variable_length_bytes : Int64?) : Nil
          null_page = @page_metrics.num_buffered_rows.to_u64 == @page_metrics.num_page_nulls

          if null_page && @column_index_builder.valid?
            @column_index_builder.append(null_page, Bytes.new(0), Bytes.new(0), @page_metrics.num_page_nulls.to_i64, nan_count_opt)
          elsif @column_index_builder.valid?
            if page_statistics.nil?
              @column_index_builder.to_invalid
            else
              stat = page_statistics
              new_min = stat.min_opt.not_nil!
              new_max = stat.max_opt.not_nil!
              if last = @last_non_null_data_page_min_max
                last_min, last_max = last
                basic_info = @descr.get_basic_info
                if @data_page_boundary_ascending
                  not_ascending = Writer.compare_greater(basic_info, last_min, new_min) || Writer.compare_greater(basic_info, last_max, new_max)
                  @data_page_boundary_ascending = false if not_ascending
                end
                if @data_page_boundary_descending
                  not_descending = Writer.compare_greater(basic_info, new_min, last_min) || Writer.compare_greater(basic_info, new_max, last_max)
                  @data_page_boundary_descending = false if not_descending
                end
              end
              @last_non_null_data_page_min_max = {new_min, new_max}

              if Writer.can_truncate_value(@descr)
                min_bytes, _ = Writer.truncate_min_value(@descr, @props.column_index_truncate_length, stat.min_bytes_opt.not_nil!)
                max_bytes, _ = Writer.truncate_max_value(@descr, @props.column_index_truncate_length, stat.max_bytes_opt.not_nil!)
                @column_index_builder.append(null_page, min_bytes, max_bytes, @page_metrics.num_page_nulls.to_i64, nan_count_opt)
              else
                @column_index_builder.append(null_page, stat.min_bytes_opt.not_nil!, stat.max_bytes_opt.not_nil!, @page_metrics.num_page_nulls.to_i64, nan_count_opt)
              end
            end
          end

          @column_index_builder.append_histograms(@page_metrics.repetition_level_histogram, @page_metrics.definition_level_histogram)

          if builder = @offset_index_builder
            builder.append_row_count(@page_metrics.num_buffered_rows.to_i64)
            builder.append_unencoded_byte_array_data_bytes(page_variable_length_bytes)
          end
        end

        def add_data_page : Nil
          values_data = @encoder.flush_data_page
          max_def_level = @descr.max_def_level
          max_rep_level = @descr.max_rep_level

          @column_metrics.num_column_nulls += @page_metrics.num_page_nulls

          if nan_count = values_data.nan_count
            @column_metrics.num_column_nans = (@column_metrics.num_column_nans || 0_u64) + nan_count
            @page_metrics.num_page_nans = nan_count
          end

          # `min`/`max` are nilable `T`, so test against nil rather than
          # truthiness: a BOOLEAN `false` is a valid minimum.
          min_value = values_data.min_value
          max_value = values_data.max_value
          page_statistics = nil.as(ValueStatistics(T)?)
          unless min_value.nil? || max_value.nil?
            @column_metrics.min_column_value = Writer.update_min(@descr, min_value, @column_metrics.min_column_value)
            @column_metrics.max_column_value = Writer.update_max(@descr, max_value, @column_metrics.max_column_value)
            if @column_props.statistics_enabled == File::Properties::EnabledStatistics::Page
              page_statistics = ValueStatistics(T).new(min_value, max_value, nil, @page_metrics.num_page_nulls, false).with_nan_count(values_data.nan_count)
            end
          end

          update_column_offset_index(page_statistics, values_data.variable_length_bytes)
          @column_metrics.update_from_page_metrics(@page_metrics)
          @column_metrics.update_variable_length_bytes(values_data.variable_length_bytes)

          header_statistics =
            if @column_props.write_page_header_statistics
              page_statistics.try { |stats| Writer.truncate_statistics(@descr, @props, Statistics.new(stats)) }
            end

          compressed_page = build_compressed_page(values_data, max_rep_level, max_def_level, header_statistics)

          if @encoder.has_dictionary && !@page_writer.defers_dictionary_ordering
            @data_pages.push(compressed_page)
          else
            write_data_page(compressed_page)
          end

          @column_metrics.total_rows_written += @page_metrics.num_buffered_rows.to_u64
          @page_metrics.new_page
          nil
        end

        private def build_compressed_page(values_data, max_rep_level : Int16, max_def_level : Int16, page_statistics : Statistics?) : Column::CompressedPage
          if @props.writer_version == File::Properties::WriterVersion::PARQUET_1_0
            buffer = [] of UInt8
            @rep_levels_encoder.flush_to { |data| buffer.concat(data) } if max_rep_level > 0
            @def_levels_encoder.flush_to { |data| buffer.concat(data) } if max_def_level > 0
            buffer.concat(values_data.buf)
            uncompressed_size = buffer.size

            if cmpr = @compressor
              compressed_buf = [] of UInt8
              cmpr.compress(Slice.new(buffer.to_unsafe, buffer.size), compressed_buf)
              buffer = compressed_buf
            end

            data_page = Column::Page::DataPage.new(
              Slice.new(buffer.to_unsafe, buffer.size).dup,
              @page_metrics.num_buffered_values,
              values_data.encoding,
              Encoding::RLE,
              Encoding::RLE,
              page_statistics,
            )
            Column::CompressedPage.new(data_page, uncompressed_size.to_i64)
          else
            rep_levels_byte_len = 0
            def_levels_byte_len = 0
            buffer = [] of UInt8

            if max_rep_level > 0
              @rep_levels_encoder.flush_to { |data| buffer.concat(data) }
              rep_levels_byte_len = buffer.size
            end
            if max_def_level > 0
              @def_levels_encoder.flush_to { |data| buffer.concat(data) }
              def_levels_byte_len = buffer.size - rep_levels_byte_len
            end

            uncompressed_size = rep_levels_byte_len + def_levels_byte_len + values_data.buf.size

            is_compressed =
              if cmpr = @compressor
                buffer_len = buffer.size
                cmpr.compress(values_data.buf, buffer)
                compressed_values_size = buffer.size - buffer_len
                threshold = @column_props.data_page_v2_compression_ratio_threshold
                if compressed_values_size.to_f64 >= uncompressed_size.to_f64 * threshold
                  buffer = buffer[0, buffer_len]
                  buffer.concat(values_data.buf)
                  false
                else
                  true
                end
              else
                buffer.concat(values_data.buf)
                false
              end

            data_page = Column::Page::DataPageV2.new(
              Slice.new(buffer.to_unsafe, buffer.size).dup,
              @page_metrics.num_buffered_values,
              values_data.encoding,
              @page_metrics.num_page_nulls.to_u32,
              @page_metrics.num_buffered_rows,
              def_levels_byte_len.to_u32,
              rep_levels_byte_len.to_u32,
              is_compressed,
              page_statistics,
            )
            Column::CompressedPage.new(data_page, uncompressed_size.to_i64)
          end
        end

        private def flush_data_pages : Nil
          add_data_page if @page_metrics.num_buffered_values > 0
          while page = @data_pages.shift?
            write_data_page(page)
          end
          nil
        end

        private def build_column_metadata : File::Metadata::ColumnChunkMetaData
          builder = File::Metadata::ColumnChunkMetaData.builder(@descr)
            .set_compression_codec(CompressionCodec.from(@codec))
            .set_encodings_mask(EncodingMask.new_from_encodings(@encodings))
            .set_page_encoding_stats(@encoding_stats)
            .set_total_compressed_size(@column_metrics.total_compressed_size.to_i64)
            .set_total_uncompressed_size(@column_metrics.total_uncompressed_size.to_i64)
            .set_num_values(@column_metrics.total_num_values.to_i64)
            .set_data_page_offset((@column_metrics.data_page_offset || 0_u64).to_i64)
            .set_dictionary_page_offset(@column_metrics.dictionary_page_offset.try(&.to_i64))

          if @column_props.statistics_enabled != File::Properties::EnabledStatistics::None
            backwards_compatible_min_max = @descr.sort_order.signed?
            stats = ValueStatistics(T).new(
              @column_metrics.min_column_value,
              @column_metrics.max_column_value,
              @distinct_count_override || @column_metrics.column_distinct_count,
              @column_metrics.num_column_nulls,
              false,
            ).with_nan_count(@column_metrics.num_column_nans).with_backwards_compatible_min_max(backwards_compatible_min_max)
            statistics = Writer.truncate_statistics(@descr, @props, Statistics.new(stats))

            builder = builder
              .set_statistics(statistics)
              .set_unencoded_byte_array_data_bytes(@column_metrics.variable_length_bytes)
              .set_repetition_level_histogram(@column_metrics.repetition_level_histogram)
              .set_definition_level_histogram(@column_metrics.definition_level_histogram)

            if geo_stats = @encoder.flush_geospatial_statistics
              builder = builder.set_geo_statistics(geo_stats)
            end
          end

          if encryption = @props.file_encryption_properties
            builder = builder.set_column_crypto_metadata(
              Parquet::Encryption.get_column_crypto_metadata(encryption, @descr)
            )
          end

          builder.build
        end

        private def write_data_page(page : Column::CompressedPage) : Nil
          @encodings << page.encoding

          last = @encoding_stats.last?
          if last && last.page_type == page.page_type && last.encoding == page.encoding
            last.count += 1
          else
            @encoding_stats << File::Metadata::PageEncodingStats.new(page.page_type, page.encoding, 1)
          end

          page_spec = @page_writer.write_page(page)
          if builder = @offset_index_builder
            builder.append_offset_and_size(page_spec.offset.to_i64, page_spec.compressed_size.to_i)
          end
          update_metrics_for_page(page_spec)
          nil
        end

        private def write_dictionary_page : Nil
          page = @encoder.flush_dict_page || raise general_err!("Dictionary encoder is not set")
          uncompressed_size = page.buf.size
          buf = page.buf
          if cmpr = @compressor
            output_buf = [] of UInt8
            cmpr.compress(buf, output_buf)
            buf = Slice.new(output_buf.to_unsafe, output_buf.size).dup
          end

          dict_page = Column::Page::DictionaryPage.new(buf, page.num_values.to_u32, @props.dictionary_page_encoding, page.is_sorted)
          compressed_page = Column::CompressedPage.new(dict_page, uncompressed_size.to_i64)

          @encodings << compressed_page.encoding
          @encoding_stats << File::Metadata::PageEncodingStats.new(PageType::DICTIONARY_PAGE, compressed_page.encoding, 1)
          page_spec = @page_writer.write_page(compressed_page)
          update_metrics_for_page(page_spec)
          nil
        end

        private def update_metrics_for_page(page_spec : Column::PageWriteSpec) : Nil
          @column_metrics.total_uncompressed_size += page_spec.uncompressed_size.to_u64
          @column_metrics.total_compressed_size += page_spec.compressed_size.to_u64
          @column_metrics.total_bytes_written += page_spec.bytes_written

          case page_spec.page_type
          when PageType::DATA_PAGE, PageType::DATA_PAGE_V2
            @column_metrics.total_num_values += page_spec.num_values.to_u64
            if @column_metrics.data_page_offset.nil?
              @column_metrics.data_page_offset = page_spec.offset
            end
          when PageType::DICTIONARY_PAGE
            if !@column_metrics.dictionary_page_offset.nil?
              raise "Dictionary offset is already set"
            end
            @column_metrics.dictionary_page_offset = page_spec.offset
          end
        end
      end

      # Gets a column writer for `descr`, writing pages to `page_writer`.
      def self.get_column_writer(descr : Schema::ColumnDescriptor, props : File::Properties::WriterProperties, page_writer : Column::PageWriter) : ColumnWriter
        case descr.physical_type
        when Parquet::Type::BOOLEAN              then GenericColumnWriter(Bool).new(descr, props, page_writer)
        when Parquet::Type::INT32                then GenericColumnWriter(Int32).new(descr, props, page_writer)
        when Parquet::Type::INT64                then GenericColumnWriter(Int64).new(descr, props, page_writer)
        when Parquet::Type::INT96                then GenericColumnWriter(Int96).new(descr, props, page_writer)
        when Parquet::Type::FLOAT                then GenericColumnWriter(Float32).new(descr, props, page_writer)
        when Parquet::Type::DOUBLE               then GenericColumnWriter(Float64).new(descr, props, page_writer)
        when Parquet::Type::BYTE_ARRAY           then GenericColumnWriter(ByteArray).new(descr, props, page_writer)
        when Parquet::Type::FIXED_LEN_BYTE_ARRAY then GenericColumnWriter(FixedLenByteArray).new(descr, props, page_writer)
        else
          raise general_err!("Cannot get column writer for physical type #{descr.physical_type}")
        end
      end

      # Up-casts `col_writer` to a typed `GenericColumnWriter(T)`.
      def self.get_typed_column_writer(type : T.class, col_writer : ColumnWriter) : GenericColumnWriter(T) forall T
        col_writer.as(GenericColumnWriter(T))
      rescue TypeCastError
        raise general_err!("Failed to convert column writer into a typed column writer for `#{Encodings.physical_type(T)}` type")
      end
    end
  end
end
