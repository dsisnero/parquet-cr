require "../basic"
require "../compression"
require "../schema/types"
require "./metadata"
require "../encryption/encrypt"

# Ported from `parquet/src/file/properties.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `WriterProperties`/`WriterPropertiesBuilder`, `ReaderProperties`/
# `ReaderPropertiesBuilder`, `WriterVersion`, `EnabledStatistics`,
# `BloomFilterPosition`, `BloomFilterProperties`(+builder), `CdcOptions`, and the
# per-column `ColumnProperties`/
# resolution. Divergence: `usize` becomes `Int32`; Rust `panic!`/`assert!` become
# Crystal exceptions; `Result`-returning `try_build` raises; `WriterVersion`/
# `EnabledStatistics` `FromStr` errors are raised; the file encryption properties
# are exposed on `WriterProperties` (feature-gated upstream).
module Parquet
  module File
    module Properties
      DEFAULT_PAGE_SIZE                                  = 1024 * 1024
      DEFAULT_WRITE_BATCH_SIZE                           = 1024
      DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT                 = DEFAULT_PAGE_SIZE
      DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT                  = 20_000
      DEFAULT_WRITE_PAGE_HEADER_STATISTICS               = false
      DEFAULT_MAX_ROW_GROUP_ROW_COUNT                    = 1024 * 1024
      DEFAULT_BLOOM_FILTER_FOR_DICTIONARY_ENCODED_CHUNKS = true
      DEFAULT_CREATED_BY                                 = "parquet-rs version 60.0.0"
      DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH               =       64
      DEFAULT_BLOOM_FILTER_FPP                           = 0.05_f64
      DEFAULT_BLOOM_FILTER_NDV                           = DEFAULT_MAX_ROW_GROUP_ROW_COUNT.to_u64
      DEFAULT_STATISTICS_TRUNCATE_LENGTH                 = 64
      DEFAULT_OFFSET_INDEX_DISABLED                      = false
      DEFAULT_COERCE_TYPES                               = false
      DEFAULT_WRITE_ROW_GROUP_NUMBER_DISTINCT_VALUES     = false
      DEFAULT_DATA_PAGE_V2_COMPRESSION_RATIO_THRESHOLD   = 1.0_f64
      DEFAULT_WRITE_PATH_IN_SCHEMA                       = true
      DEFAULT_CDC_MIN_CHUNK_SIZE                         = 256 * 1024
      DEFAULT_CDC_MAX_CHUNK_SIZE                         = 1024 * 1024
      DEFAULT_CDC_NORM_LEVEL                             = 0

      # Parquet writer version.
      enum WriterVersion
        PARQUET_1_0
        PARQUET_2_0

        # Returns the writer version as `Int32`.
        def as_num : Int32
          case self
          in PARQUET_1_0 then 1
          in PARQUET_2_0 then 2
          end
        end

        def self.from_str(str : String) : WriterVersion
          case str
          when "PARQUET_1_0", "parquet_1_0" then PARQUET_1_0
          when "PARQUET_2_0", "parquet_2_0" then PARQUET_2_0
          else
            raise "Invalid writer version: #{str}"
          end
        end

        def self.from_str?(str : String) : WriterVersion?
          from_str(str)
        rescue Exception
          nil
        end
      end

      # Where in the file Bloom filters are written.
      enum BloomFilterPosition
        AfterRowGroup
        End
      end

      DEFAULT_WRITER_VERSION        = WriterVersion::PARQUET_1_0
      DEFAULT_COMPRESSION           = Compression::UNCOMPRESSED
      DEFAULT_DICTIONARY_ENABLED    = true
      DEFAULT_STATISTICS_ENABLED    = EnabledStatistics::Page
      DEFAULT_BLOOM_FILTER_POSITION = BloomFilterPosition::AfterRowGroup

      # Controls the level of statistics computed by the writer.
      enum EnabledStatistics
        None
        Chunk
        Page

        def self.default : EnabledStatistics
          DEFAULT_STATISTICS_ENABLED
        end

        def self.from_str(str : String) : EnabledStatistics
          case str
          when "NONE", "none"   then None
          when "CHUNK", "chunk" then Chunk
          when "PAGE", "page"   then Page
          else
            raise "Invalid statistics arg: #{str}"
          end
        end

        def self.from_str?(str : String) : EnabledStatistics?
          from_str(str)
        rescue Exception
          nil
        end
      end

      # EXPERIMENTAL: options for content-defined chunking.
      record CdcOptions, min_chunk_size : Int32, max_chunk_size : Int32, norm_level : Int32 do
        def self.default : CdcOptions
          new(DEFAULT_CDC_MIN_CHUNK_SIZE, DEFAULT_CDC_MAX_CHUNK_SIZE, DEFAULT_CDC_NORM_LEVEL)
        end
      end

      # Controls the bloom filter computed by the writer.
      class BloomFilterProperties
        getter fpp : Float64
        property ndv : UInt64

        def initialize(@fpp : Float64, @ndv : UInt64)
        end

        def self.default : BloomFilterProperties
          new(DEFAULT_BLOOM_FILTER_FPP, DEFAULT_BLOOM_FILTER_NDV)
        end

        def self.builder : BloomFilterPropertiesBuilder
          BloomFilterPropertiesBuilder.new
        end

        def clone : BloomFilterProperties
          BloomFilterProperties.new(@fpp, @ndv)
        end

        def_equals @fpp, @ndv
        def_hash @fpp, @ndv
      end

      # Builder for `BloomFilterProperties`.
      class BloomFilterPropertiesBuilder
        def initialize
          @fpp = nil.as(Float64?)
          @ndv = nil.as(UInt64?)
        end

        def with_fpp(value : Float64) : BloomFilterPropertiesBuilder
          @fpp = value
          self
        end

        def with_max_ndv(value : UInt64) : BloomFilterPropertiesBuilder
          @ndv = value
          self
        end

        def build : BloomFilterProperties
          try_build
        end

        def try_build : BloomFilterProperties
          fpp = @fpp || DEFAULT_BLOOM_FILTER_FPP
          Properties.validate_bloom_filter_fpp(fpp)
          ndv = @ndv || DEFAULT_BLOOM_FILTER_NDV
          BloomFilterProperties.new(fpp, ndv)
        end
      end

      def self.validate_bloom_filter_fpp(fpp : Float64) : Nil
        unless fpp > 0.0 && fpp < 1.0
          raise "fpp must be between 0.0 and 1.0 exclusive, got #{fpp}"
        end
      end

      # Per-column writer settings; `nil` means "unset, inherit the default".
      class ColumnProperties
        getter encoding : Encoding?
        getter codec : Compression?
        getter data_page_size_limit : Int32?
        getter dictionary_page_size_limit : Int32?
        getter dictionary_enabled : Bool?
        getter statistics_enabled : EnabledStatistics?
        getter write_page_header_statistics : Bool?
        getter bloom_filter_properties : BloomFilterProperties?
        getter data_page_v2_compression_ratio_threshold : Float64?

        def initialize
          @encoding = nil
          @codec = nil
          @data_page_size_limit = nil
          @dictionary_page_size_limit = nil
          @dictionary_enabled = nil
          @statistics_enabled = nil
          @write_page_header_statistics = nil
          @bloom_filter_properties = nil
          @bloom_filter_ndv_is_set = false
          @data_page_v2_compression_ratio_threshold = nil
        end

        # ameba:disable Naming/AccessorMethodName
        def set_encoding(value : Encoding) : Nil
          if value == Encoding::PLAIN_DICTIONARY || value == Encoding::RLE_DICTIONARY
            raise "Dictionary encoding can not be used as fallback encoding"
          end
          @encoding = value
        end

        # ameba:disable Naming/AccessorMethodName
        def set_compression(value : Compression) : Nil
          @codec = value
        end

        # ameba:disable Naming/AccessorMethodName
        def set_data_page_size_limit(value : Int32) : Nil
          @data_page_size_limit = value
        end

        # ameba:disable Naming/AccessorMethodName
        def set_dictionary_enabled(enabled : Bool) : Nil
          @dictionary_enabled = enabled
        end

        # ameba:disable Naming/AccessorMethodName
        def set_dictionary_page_size_limit(value : Int32) : Nil
          @dictionary_page_size_limit = value
        end

        # ameba:disable Naming/AccessorMethodName
        def set_statistics_enabled(enabled : EnabledStatistics) : Nil
          @statistics_enabled = enabled
        end

        # ameba:disable Naming/AccessorMethodName
        def set_write_page_header_statistics(enabled : Bool) : Nil
          @write_page_header_statistics = enabled
        end

        # ameba:disable Naming/AccessorMethodName
        def set_bloom_filter_enabled(value : Bool) : Nil
          if value && @bloom_filter_properties.nil?
            @bloom_filter_properties = BloomFilterProperties.default
          elsif !value
            @bloom_filter_properties = nil
          end
        end

        # ameba:disable Naming/AccessorMethodName
        def set_bloom_filter_fpp(value : Float64) : Nil
          Properties.validate_bloom_filter_fpp(value)
          bf = @bloom_filter_properties || BloomFilterProperties.default
          @bloom_filter_properties = BloomFilterProperties.new(value, bf.ndv)
        end

        # ameba:disable Naming/AccessorMethodName
        def set_bloom_filter_ndv(value : UInt64) : Nil
          bf = @bloom_filter_properties || BloomFilterProperties.default
          bf.ndv = value
          @bloom_filter_properties = bf
          @bloom_filter_ndv_is_set = true
        end

        # ameba:disable Naming/AccessorMethodName
        def set_bloom_filter_properties(value : BloomFilterProperties) : Nil
          @bloom_filter_properties = value
          @bloom_filter_ndv_is_set = true
        end

        # ameba:disable Naming/AccessorMethodName
        def set_data_page_v2_compression_ratio_threshold(value : Float64) : Nil
          unless value.finite? && value > 0.0
            raise "data_page_v2_compression_ratio_threshold must be a positive finite number, got #{value}"
          end
          @data_page_v2_compression_ratio_threshold = value
        end

        # Resolves the bloom filter NDV to `default_ndv` if it was not set.
        def resolve_bloom_filter_ndv(default_ndv : UInt64) : Nil
          if !@bloom_filter_ndv_is_set && (bf = @bloom_filter_properties)
            bf.ndv = default_ndv
          end
        end
      end

      # Every per-column writer setting for one leaf column, resolved.
      class ResolvedColumnProperties
        getter encoding : Encoding?
        getter compression : Compression
        getter dictionary_enabled : Bool
        getter statistics_enabled : EnabledStatistics
        getter write_page_header_statistics : Bool
        getter data_page_size_limit : Int32
        getter dictionary_page_size_limit : Int32
        getter data_page_v2_compression_ratio_threshold : Float64
        getter bloom_filter_properties : BloomFilterProperties?

        def initialize(
          @encoding,
          @compression,
          @dictionary_enabled,
          @statistics_enabled,
          @write_page_header_statistics,
          @data_page_size_limit,
          @dictionary_page_size_limit,
          @data_page_v2_compression_ratio_threshold,
          @bloom_filter_properties,
        )
        end
      end

      # Resolved state of `set_offset_index_disabled`.
      enum OffsetIndexSetting
        Enabled
        Disabled
        DisabledOverridden
      end

      # Configuration settings for writing parquet files.
      class WriterProperties
        getter data_page_row_count_limit : Int32
        getter write_batch_size : Int32
        getter max_row_group_row_count : Int32?
        getter max_row_group_bytes : Int32?
        getter bloom_filter_position : BloomFilterPosition
        getter bloom_filter_for_dictionary_encoded_chunks : Bool
        getter writer_version : WriterVersion
        getter created_by : String
        # Divergence: upstream keeps this `pub(crate)`; the arrow schema
        # metadata helper mutates it (`add_encoded_arrow_schema_to_metadata`).
        property key_value_metadata : Array(Metadata::KeyValue)?
        getter sorting_columns : Array(Metadata::SortingColumn)?
        getter column_index_truncate_length : Int32?
        getter statistics_truncate_length : Int32?
        getter coerce_types : Bool
        getter write_row_group_number_distinct_values : Bool
        getter content_defined_chunking : CdcOptions?
        getter write_path_in_schema : Bool
        getter file_encryption_properties : Encryption::FileEncryptionProperties?
        getter offset_index_setting : OffsetIndexSetting
        getter default_column_properties : ColumnProperties
        getter column_properties : Hash(Schema::ColumnPath, ColumnProperties)

        def initialize(
          @data_page_row_count_limit,
          @write_batch_size,
          @max_row_group_row_count,
          @max_row_group_bytes,
          @bloom_filter_position,
          @bloom_filter_for_dictionary_encoded_chunks,
          @writer_version,
          @created_by,
          @offset_index_setting,
          @key_value_metadata,
          @default_column_properties,
          @column_properties,
          @sorting_columns,
          @column_index_truncate_length,
          @statistics_truncate_length,
          @coerce_types,
          @write_row_group_number_distinct_values,
          @content_defined_chunking,
          @write_path_in_schema,
          @file_encryption_properties : Encryption::FileEncryptionProperties? = nil,
        )
        end

        def self.default : WriterProperties
          builder.build
        end

        def self.builder : WriterPropertiesBuilder
          WriterPropertiesBuilder.new
        end

        def into_builder : WriterPropertiesBuilder
          WriterPropertiesBuilder.from_writer_properties(self)
        end

        # Returns data page size limit.
        def data_page_size_limit : Int32
          @default_column_properties.data_page_size_limit || DEFAULT_PAGE_SIZE
        end

        def column_data_page_size_limit(col : Schema::ColumnPath) : Int32
          resolve_data_page_size_limit(column_override(col), @default_column_properties)
        end

        def dictionary_page_size_limit : Int32
          @default_column_properties.dictionary_page_size_limit || DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT
        end

        def column_dictionary_page_size_limit(col : Schema::ColumnPath) : Int32
          resolve_dictionary_page_size_limit(column_override(col), @default_column_properties)
        end

        def bloom_filter_for_dictionary_encoded_chunks : Bool
          @bloom_filter_for_dictionary_encoded_chunks
        end

        # Returns `true` if offset index writing is disabled.
        def offset_index_disabled : Bool
          @offset_index_setting == OffsetIndexSetting::Disabled
        end

        def data_page_v2_compression_ratio_threshold : Float64
          @default_column_properties.data_page_v2_compression_ratio_threshold ||
            DEFAULT_DATA_PAGE_V2_COMPRESSION_RATIO_THRESHOLD
        end

        def column_data_page_v2_compression_ratio_threshold(col : Schema::ColumnPath) : Float64
          resolve_data_page_v2_compression_ratio_threshold(column_override(col), @default_column_properties)
        end

        # Returns encoding for a data page, when dictionary encoding is enabled.
        def dictionary_data_page_encoding : Encoding
          Encoding::RLE_DICTIONARY
        end

        # Returns encoding for a dictionary page, when dictionary encoding is enabled.
        def dictionary_page_encoding : Encoding
          Encoding::PLAIN
        end

        # Returns encoding for a column, if set.
        def encoding(col : Schema::ColumnPath) : Encoding?
          resolve_encoding(column_override(col), @default_column_properties)
        end

        def compression(col : Schema::ColumnPath) : Compression
          resolve_compression(column_override(col), @default_column_properties)
        end

        def dictionary_enabled(col : Schema::ColumnPath) : Bool
          resolve_dictionary_enabled(column_override(col), @default_column_properties)
        end

        def statistics_enabled(col : Schema::ColumnPath) : EnabledStatistics
          resolve_statistics_enabled(column_override(col), @default_column_properties)
        end

        def write_page_header_statistics(col : Schema::ColumnPath) : Bool
          resolve_write_page_header_statistics(column_override(col), @default_column_properties)
        end

        def bloom_filter_properties(col : Schema::ColumnPath) : BloomFilterProperties?
          resolve_bloom_filter_properties(column_override(col), @default_column_properties)
        end

        # Resolves every per-column writer setting for `col`.
        def resolve_column_properties(col : Schema::ColumnPath) : ResolvedColumnProperties
          column = column_override(col)
          default = @default_column_properties
          ResolvedColumnProperties.new(
            resolve_encoding(column, default),
            resolve_compression(column, default),
            resolve_dictionary_enabled(column, default),
            resolve_statistics_enabled(column, default),
            resolve_write_page_header_statistics(column, default),
            resolve_data_page_size_limit(column, default),
            resolve_dictionary_page_size_limit(column, default),
            resolve_data_page_v2_compression_ratio_threshold(column, default),
            resolve_bloom_filter_properties(column, default),
          )
        end

        private def column_override(col : Schema::ColumnPath) : ColumnProperties?
          @column_properties[col]?
        end

        private def resolve_encoding(column : ColumnProperties?, default : ColumnProperties) : Encoding?
          (column && column.encoding) || default.encoding
        end

        private def resolve_compression(column : ColumnProperties?, default : ColumnProperties) : Compression
          ((column && column.codec) || default.codec) || DEFAULT_COMPRESSION
        end

        private def resolve_dictionary_enabled(column : ColumnProperties?, default : ColumnProperties) : Bool
          value = column.try(&.dictionary_enabled)
          value = default.dictionary_enabled if value.nil?
          value.nil? ? DEFAULT_DICTIONARY_ENABLED : value
        end

        private def resolve_statistics_enabled(column : ColumnProperties?, default : ColumnProperties) : EnabledStatistics
          (column && column.statistics_enabled) || default.statistics_enabled || DEFAULT_STATISTICS_ENABLED
        end

        private def resolve_write_page_header_statistics(column : ColumnProperties?, default : ColumnProperties) : Bool
          value = (column && column.write_page_header_statistics) || default.write_page_header_statistics
          value.nil? ? DEFAULT_WRITE_PAGE_HEADER_STATISTICS : value
        end

        private def resolve_data_page_size_limit(column : ColumnProperties?, default : ColumnProperties) : Int32
          (column && column.data_page_size_limit) || default.data_page_size_limit || DEFAULT_PAGE_SIZE
        end

        private def resolve_dictionary_page_size_limit(column : ColumnProperties?, default : ColumnProperties) : Int32
          (column && column.dictionary_page_size_limit) || default.dictionary_page_size_limit || DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT
        end

        private def resolve_data_page_v2_compression_ratio_threshold(column : ColumnProperties?, default : ColumnProperties) : Float64
          (column && column.data_page_v2_compression_ratio_threshold) ||
            default.data_page_v2_compression_ratio_threshold ||
            DEFAULT_DATA_PAGE_V2_COMPRESSION_RATIO_THRESHOLD
        end

        private def resolve_bloom_filter_properties(column : ColumnProperties?, default : ColumnProperties) : BloomFilterProperties?
          if column && (bf = column.bloom_filter_properties)
            bf
          else
            default.bloom_filter_properties
          end
        end
      end

      # Builder for `WriterProperties`.
      class WriterPropertiesBuilder
        @column_properties : Hash(Schema::ColumnPath, ColumnProperties)
        @default_column_properties : ColumnProperties

        def self.from_writer_properties(props : WriterProperties) : WriterPropertiesBuilder
          new.load_from(props)
        end

        def load_from(props : WriterProperties) : WriterPropertiesBuilder
          @data_page_row_count_limit = props.data_page_row_count_limit
          @write_batch_size = props.write_batch_size
          @max_row_group_row_count = props.max_row_group_row_count
          @max_row_group_bytes = props.max_row_group_bytes
          @bloom_filter_position = props.bloom_filter_position
          @bloom_filter_for_dictionary_encoded_chunks = props.bloom_filter_for_dictionary_encoded_chunks
          @writer_version = props.writer_version
          @created_by = props.created_by
          @offset_index_disabled = props.offset_index_setting != OffsetIndexSetting::Enabled
          @key_value_metadata = props.key_value_metadata
          @default_column_properties = props.default_column_properties
          @column_properties = props.column_properties
          @sorting_columns = props.sorting_columns
          @column_index_truncate_length = props.column_index_truncate_length
          @statistics_truncate_length = props.statistics_truncate_length
          @coerce_types = props.coerce_types
          @write_row_group_number_distinct_values = props.write_row_group_number_distinct_values
          @content_defined_chunking = props.content_defined_chunking
          @write_path_in_schema = props.write_path_in_schema
          @file_encryption_properties = props.file_encryption_properties
          self
        end

        def initialize
          @data_page_row_count_limit = DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT
          @write_batch_size = DEFAULT_WRITE_BATCH_SIZE
          @max_row_group_row_count = DEFAULT_MAX_ROW_GROUP_ROW_COUNT
          @max_row_group_bytes = nil.as(Int32?)
          @bloom_filter_position = DEFAULT_BLOOM_FILTER_POSITION
          @bloom_filter_for_dictionary_encoded_chunks = DEFAULT_BLOOM_FILTER_FOR_DICTIONARY_ENCODED_CHUNKS
          @writer_version = DEFAULT_WRITER_VERSION
          @created_by = DEFAULT_CREATED_BY
          @offset_index_disabled = DEFAULT_OFFSET_INDEX_DISABLED
          @key_value_metadata = nil.as(Array(Metadata::KeyValue)?)
          @default_column_properties = ColumnProperties.new
          @column_properties = {} of Schema::ColumnPath => ColumnProperties
          @sorting_columns = nil.as(Array(Metadata::SortingColumn)?)
          @column_index_truncate_length = DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH
          @statistics_truncate_length = DEFAULT_STATISTICS_TRUNCATE_LENGTH
          @coerce_types = DEFAULT_COERCE_TYPES
          @write_row_group_number_distinct_values = DEFAULT_WRITE_ROW_GROUP_NUMBER_DISTINCT_VALUES
          @content_defined_chunking = nil.as(CdcOptions?)
          @write_path_in_schema = DEFAULT_WRITE_PATH_IN_SCHEMA
          @file_encryption_properties = nil.as(Encryption::FileEncryptionProperties?)
        end

        # Finalizes the configuration and returns immutable writer properties.
        def build : WriterProperties
          offset_index_setting =
            if @offset_index_disabled
              default_page_stats_enabled = @default_column_properties.statistics_enabled == EnabledStatistics::Page
              column_page_stats_enabled = @column_properties.values.any? do |props|
                props.statistics_enabled == EnabledStatistics::Page
              end
              if default_page_stats_enabled || column_page_stats_enabled
                OffsetIndexSetting::DisabledOverridden
              else
                OffsetIndexSetting::Disabled
              end
            else
              OffsetIndexSetting::Enabled
            end

          default_ndv = (@max_row_group_row_count || DEFAULT_MAX_ROW_GROUP_ROW_COUNT).to_u64
          @default_column_properties.resolve_bloom_filter_ndv(default_ndv)
          @column_properties.each_value(&.resolve_bloom_filter_ndv(default_ndv))

          WriterProperties.new(
            @data_page_row_count_limit,
            @write_batch_size,
            @max_row_group_row_count,
            @max_row_group_bytes,
            @bloom_filter_position,
            @bloom_filter_for_dictionary_encoded_chunks,
            @writer_version,
            @created_by,
            offset_index_setting,
            @key_value_metadata,
            @default_column_properties,
            @column_properties,
            @sorting_columns,
            @column_index_truncate_length,
            @statistics_truncate_length,
            @coerce_types,
            @write_row_group_number_distinct_values,
            @content_defined_chunking,
            @write_path_in_schema,
            @file_encryption_properties,
          )
        end

        def set_writer_version(value : WriterVersion) : WriterPropertiesBuilder
          @writer_version = value
          self
        end

        def set_data_page_row_count_limit(value : Int32) : WriterPropertiesBuilder
          raise "Cannot have a 0 data page row count limit" if value == 0
          @data_page_row_count_limit = value
          self
        end

        def set_write_batch_size(value : Int32) : WriterPropertiesBuilder
          raise "Cannot have a 0 write batch size" if value == 0
          @write_batch_size = value
          self
        end

        def set_max_row_group_row_count(value : Int32?) : WriterPropertiesBuilder
          raise "Cannot have a 0 max row group row count" if value == 0
          @max_row_group_row_count = value
          self
        end

        def set_max_row_group_bytes(value : Int32?) : WriterPropertiesBuilder
          raise "Cannot have a 0 max row group bytes" if value == 0
          @max_row_group_bytes = value
          self
        end

        def set_bloom_filter_position(value : BloomFilterPosition) : WriterPropertiesBuilder
          @bloom_filter_position = value
          self
        end

        def set_bloom_filter_for_dictionary_encoded_chunks(value : Bool) : WriterPropertiesBuilder
          @bloom_filter_for_dictionary_encoded_chunks = value
          self
        end

        def set_created_by(value : String) : WriterPropertiesBuilder
          @created_by = value
          self
        end

        def set_offset_index_disabled(value : Bool) : WriterPropertiesBuilder
          @offset_index_disabled = value
          self
        end

        def set_key_value_metadata(value : Array(Metadata::KeyValue)?) : WriterPropertiesBuilder
          @key_value_metadata = value
          self
        end

        def set_sorting_columns(value : Array(Metadata::SortingColumn)?) : WriterPropertiesBuilder
          @sorting_columns = value
          self
        end

        def set_column_index_truncate_length(value : Int32?) : WriterPropertiesBuilder
          if value && value <= 0
            raise "Cannot have a 0 column index truncate length. If you wish to disable min/max value truncation, set it to `None`."
          end
          @column_index_truncate_length = value
          self
        end

        def set_statistics_truncate_length(value : Int32?) : WriterPropertiesBuilder
          if value && value <= 0
            raise "Cannot have a 0 statistics truncate length. If you wish to disable min/max value truncation, set it to `None`."
          end
          @statistics_truncate_length = value
          self
        end

        def set_coerce_types(value : Bool) : WriterPropertiesBuilder
          @coerce_types = value
          self
        end

        def set_write_row_group_number_distinct_values(value : Bool) : WriterPropertiesBuilder
          @write_row_group_number_distinct_values = value
          self
        end

        def set_write_path_in_schema(value : Bool) : WriterPropertiesBuilder
          @write_path_in_schema = value
          self
        end

        # Sets the file encryption properties (upstream
        # `with_file_encryption_properties`).
        # ameba:disable Naming/AccessorMethodName
        def set_file_encryption_properties(value : Encryption::FileEncryptionProperties?) : WriterPropertiesBuilder
          @file_encryption_properties = value
          self
        end

        def set_content_defined_chunking(options : CdcOptions?) : WriterPropertiesBuilder
          if options
            raise "min_chunk_size must be positive" if options.min_chunk_size <= 0
            if options.max_chunk_size <= options.min_chunk_size
              raise "max_chunk_size (#{options.max_chunk_size}) must be greater than min_chunk_size (#{options.min_chunk_size})"
            end
          end
          @content_defined_chunking = options
          self
        end

        def set_data_page_v2_compression_ratio_threshold(value : Float64) : WriterPropertiesBuilder
          @default_column_properties.set_data_page_v2_compression_ratio_threshold(value)
          self
        end

        def set_encoding(value : Encoding) : WriterPropertiesBuilder
          @default_column_properties.set_encoding(value)
          self
        end

        def set_compression(value : Compression) : WriterPropertiesBuilder
          @default_column_properties.set_compression(value)
          self
        end

        def set_dictionary_enabled(value : Bool) : WriterPropertiesBuilder
          @default_column_properties.set_dictionary_enabled(value)
          self
        end

        def set_dictionary_page_size_limit(value : Int32) : WriterPropertiesBuilder
          @default_column_properties.set_dictionary_page_size_limit(value)
          self
        end

        def set_data_page_size_limit(value : Int32) : WriterPropertiesBuilder
          @default_column_properties.set_data_page_size_limit(value)
          self
        end

        def set_statistics_enabled(value : EnabledStatistics) : WriterPropertiesBuilder
          @default_column_properties.set_statistics_enabled(value)
          self
        end

        def set_write_page_header_statistics(value : Bool) : WriterPropertiesBuilder
          @default_column_properties.set_write_page_header_statistics(value)
          self
        end

        def set_bloom_filter_enabled(value : Bool) : WriterPropertiesBuilder
          @default_column_properties.set_bloom_filter_enabled(value)
          self
        end

        def set_bloom_filter_fpp(value : Float64) : WriterPropertiesBuilder
          @default_column_properties.set_bloom_filter_fpp(value)
          self
        end

        def set_bloom_filter_max_ndv(value : UInt64) : WriterPropertiesBuilder
          @default_column_properties.set_bloom_filter_ndv(value)
          self
        end

        def set_bloom_filter_ndv(value : UInt64) : WriterPropertiesBuilder
          set_bloom_filter_max_ndv(value)
        end

        def set_bloom_filter_properties(value : BloomFilterProperties) : WriterPropertiesBuilder
          @default_column_properties.set_bloom_filter_properties(value)
          self
        end

        def set_column_encoding(col : Schema::ColumnPath, value : Encoding) : WriterPropertiesBuilder
          get_mut_props(col).set_encoding(value)
          self
        end

        def set_column_compression(col : Schema::ColumnPath, value : Compression) : WriterPropertiesBuilder
          get_mut_props(col).set_compression(value)
          self
        end

        def set_column_dictionary_enabled(col : Schema::ColumnPath, value : Bool) : WriterPropertiesBuilder
          get_mut_props(col).set_dictionary_enabled(value)
          self
        end

        def set_column_dictionary_page_size_limit(col : Schema::ColumnPath, value : Int32) : WriterPropertiesBuilder
          get_mut_props(col).set_dictionary_page_size_limit(value)
          self
        end

        def set_column_data_page_size_limit(col : Schema::ColumnPath, value : Int32) : WriterPropertiesBuilder
          get_mut_props(col).set_data_page_size_limit(value)
          self
        end

        def set_column_statistics_enabled(col : Schema::ColumnPath, value : EnabledStatistics) : WriterPropertiesBuilder
          get_mut_props(col).set_statistics_enabled(value)
          self
        end

        def set_column_write_page_header_statistics(col : Schema::ColumnPath, value : Bool) : WriterPropertiesBuilder
          get_mut_props(col).set_write_page_header_statistics(value)
          self
        end

        def set_column_bloom_filter_enabled(col : Schema::ColumnPath, value : Bool) : WriterPropertiesBuilder
          get_mut_props(col).set_bloom_filter_enabled(value)
          self
        end

        def set_column_bloom_filter_fpp(col : Schema::ColumnPath, value : Float64) : WriterPropertiesBuilder
          get_mut_props(col).set_bloom_filter_fpp(value)
          self
        end

        def set_column_bloom_filter_max_ndv(col : Schema::ColumnPath, value : UInt64) : WriterPropertiesBuilder
          get_mut_props(col).set_bloom_filter_ndv(value)
          self
        end

        def set_column_bloom_filter_ndv(col : Schema::ColumnPath, value : UInt64) : WriterPropertiesBuilder
          set_column_bloom_filter_max_ndv(col, value)
        end

        def set_column_bloom_filter_properties(col : Schema::ColumnPath, value : BloomFilterProperties) : WriterPropertiesBuilder
          get_mut_props(col).set_bloom_filter_properties(value)
          self
        end

        def set_column_data_page_v2_compression_ratio_threshold(col : Schema::ColumnPath, value : Float64) : WriterPropertiesBuilder
          get_mut_props(col).set_data_page_v2_compression_ratio_threshold(value)
          self
        end

        private def get_mut_props(col : Schema::ColumnPath) : ColumnProperties
          @column_properties[col] ||= ColumnProperties.new
        end
      end

      # Configuration settings for reading parquet files.
      class ReaderProperties
        @codec_options : CodecOptions
        @read_bloom_filter : Bool
        @read_page_stats : Bool

        def initialize(@codec_options, @read_bloom_filter, @read_page_stats)
        end

        def self.builder : ReaderPropertiesBuilder
          ReaderPropertiesBuilder.with_defaults
        end

        def codec_options : CodecOptions
          @codec_options
        end

        def read_bloom_filter : Bool
          @read_bloom_filter
        end

        def read_page_stats : Bool
          @read_page_stats
        end
      end

      # Builder for `ReaderProperties`.
      class ReaderPropertiesBuilder
        DEFAULT_READ_BLOOM_FILTER = false
        DEFAULT_READ_PAGE_STATS   = false

        def self.with_defaults : ReaderPropertiesBuilder
          new
        end

        def initialize
          @codec_options_builder = CodecOptionsBuilder.default
          @read_bloom_filter = nil.as(Bool?)
          @read_page_stats = nil.as(Bool?)
        end

        def build : ReaderProperties
          read_bloom = @read_bloom_filter
          read_page = @read_page_stats
          ReaderProperties.new(
            @codec_options_builder.build,
            read_bloom.nil? ? DEFAULT_READ_BLOOM_FILTER : read_bloom,
            read_page.nil? ? DEFAULT_READ_PAGE_STATS : read_page,
          )
        end

        def set_backward_compatible_lz4(value : Bool) : ReaderPropertiesBuilder
          @codec_options_builder = @codec_options_builder.set_backward_compatible_lz4(value)
          self
        end

        def set_read_bloom_filter(value : Bool) : ReaderPropertiesBuilder
          @read_bloom_filter = value
          self
        end

        def set_read_page_statistics(value : Bool) : ReaderPropertiesBuilder
          @read_page_stats = value
          self
        end
      end
    end
  end
end
