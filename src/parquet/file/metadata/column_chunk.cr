require "../metadata"
require "../column_crypto_metadata"
require "../../geospatial"

# Ported from `parquet/src/file/metadata/mod.rs` and
# `parquet/src/file/metadata/thrift/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `KeyValue`, `PageEncodingStats`, `SortingColumn`, `SizeStatistics`,
# `BoundingBox`/`GeospatialStatistics` (thrift + conversion), and
# `ColumnChunkMetaData` (+ builder and thrift conversion). `RowGroupMetaData`,
# `ParquetMetaData`, and `FileMetaData` follow (see `plans/parity.md`).
#
# Divergences: the domain `Parquet::Geospatial::BoundingBox`/
# `GeospatialStatistics` are kept distinct from the thrift structs defined here
# (upstream writes impls next to the wire structs) and are converted with
# `convert_bounding_box`/`convert_geo_stats` (matching upstream
# `convert_bounding_box`/`convert_geo_stats`); `HeapSize` is not ported.
module Parquet
  module File
    module Metadata
      alias Int32List = Array(Int32)
      alias Int64List = Array(Int64)

      # A key-value pair for `FileMetaData`.
      define_thrift_struct(KeyValue) do
        required 1, key, String
        optional 2, value, String
      end

      # PageEncodingStats for a column chunk and data page.
      define_thrift_struct(PageEncodingStats) do
        required 1, page_type, PageType
        required 2, encoding, Encoding
        required 3, count, Int32
      end

      # Sort order within a RowGroup of a leaf column.
      define_thrift_struct(SortingColumn) do
        required 1, column_idx, Int32
        required 2, descending, Bool
        required 3, nulls_first, Bool
      end

      # Mirrors thrift `SizeStatistics`.
      define_thrift_struct(SizeStatistics) do
        optional 1, unencoded_byte_array_data_bytes, Int64
        optional 2, repetition_level_histogram, Int64List
        optional 3, definition_level_histogram, Int64List
      end

      # Mirrors thrift `BoundingBox`.
      define_thrift_struct(BoundingBox) do
        required 1, xmin, Float64
        required 2, xmax, Float64
        required 3, ymin, Float64
        required 4, ymax, Float64
        optional 5, zmin, Float64
        optional 6, zmax, Float64
        optional 7, mmin, Float64
        optional 8, mmax, Float64
      end

      # Mirrors thrift `GeospatialStatistics`.
      define_thrift_struct(GeospatialStatistics) do
        optional 1, bbox, BoundingBox
        optional 2, geospatial_types, Int32List
      end

      # Converts a thrift `GeospatialStatistics` into the domain type.
      def self.convert_geo_stats(stats : GeospatialStatistics) : ::Parquet::Geospatial::GeospatialStatistics
        bbox = stats.bbox.try { |value| convert_bounding_box(value) }
        geospatial_types = stats.geospatial_types
        geospatial_types = nil if geospatial_types.try(&.empty?)
        ::Parquet::Geospatial::GeospatialStatistics.new(bbox, geospatial_types)
      end

      # Converts a thrift `BoundingBox` into the domain type.
      #
      # A missing zmin/zmax (or mmin/mmax) pair leaves that dimension unset
      # instead of raising, matching upstream.
      def self.convert_bounding_box(bb : BoundingBox) : ::Parquet::Geospatial::BoundingBox
        out = ::Parquet::Geospatial::BoundingBox.new(bb.xmin, bb.xmax, bb.ymin, bb.ymax)

        if (zmin = bb.zmin) && (zmax = bb.zmax)
          out = out.with_zrange(zmin, zmax)
        end

        if (mmin = bb.mmin) && (mmax = bb.mmax)
          out = out.with_mrange(mmin, mmax)
        end

        out
      end

      # Converts a domain `BoundingBox` into the thrift struct.
      def self.bounding_box_to_thrift(bb : ::Parquet::Geospatial::BoundingBox) : BoundingBox
        BoundingBox.new(bb.xmin, bb.xmax, bb.ymin, bb.ymax, bb.zmin, bb.zmax, bb.mmin, bb.mmax)
      end

      # Converts domain `GeospatialStatistics` into the thrift struct.
      def self.geo_stats_to_thrift(stats : ::Parquet::Geospatial::GeospatialStatistics) : GeospatialStatistics
        bbox = stats.bounding_box.try { |value| bounding_box_to_thrift(value) }
        GeospatialStatistics.new(bbox, stats.geospatial_types)
      end

      # Internal representation of page encoding stats: either the full array or
      # a condensed bitmask of the encodings used for data pages.
      struct ParquetPageEncodingStats
        getter full : Array(PageEncodingStats)?
        getter mask : EncodingMask?

        def initialize(@full : Array(PageEncodingStats)?, @mask : EncodingMask?)
        end

        def self.full(value : Array(PageEncodingStats)) : ParquetPageEncodingStats
          new(value, nil)
        end

        def self.mask(value : EncodingMask) : ParquetPageEncodingStats
          new(nil, value)
        end
      end

      # Metadata for a column chunk.
      #
      # Divergence: upstream keeps these fields private and mutates them directly
      # from the reader; here they are `property` (public accessors).
      class ColumnChunkMetaData
        getter column_descr : Parquet::Schema::ColumnDescriptor
        property encodings : EncodingMask
        property file_path : String?
        property file_offset : Int64
        property num_values : Int64
        property compression : CompressionCodec
        property total_compressed_size : Int64
        property total_uncompressed_size : Int64
        property data_page_offset : Int64
        property index_page_offset : Int64?
        property dictionary_page_offset : Int64?
        property statistics : Statistics?
        property encoding_stats : ParquetPageEncodingStats?
        property bloom_filter_offset : Int64?
        property bloom_filter_length : Int32?
        property offset_index_offset : Int64?
        property offset_index_length : Int32?
        property column_index_offset : Int64?
        property column_index_length : Int32?
        property unencoded_byte_array_data_bytes : Int64?
        property repetition_level_histogram : LevelHistogram?
        property definition_level_histogram : LevelHistogram?
        property column_crypto_metadata : ::Parquet::File::ColumnCryptoMetaData?
        property encrypted_column_metadata : Bytes?
        # When true, the footer is plaintext; affects serialization when
        # `encrypted_column_metadata` is present (write-only).
        property plaintext_footer_mode : Bool
        property geo_statistics : ::Parquet::Geospatial::GeospatialStatistics?

        def initialize(@column_descr : Parquet::Schema::ColumnDescriptor)
          @encodings = EncodingMask.try_new(0)
          @file_path = nil
          @file_offset = 0_i64
          @num_values = 0_i64
          @compression = CompressionCodec::UNCOMPRESSED
          @total_compressed_size = 0_i64
          @total_uncompressed_size = 0_i64
          @data_page_offset = 0_i64
          @index_page_offset = nil
          @dictionary_page_offset = nil
          @statistics = nil
          @encoding_stats = nil
          @bloom_filter_offset = nil
          @bloom_filter_length = nil
          @offset_index_offset = nil
          @offset_index_length = nil
          @column_index_offset = nil
          @column_index_length = nil
          @unencoded_byte_array_data_bytes = nil
          @repetition_level_histogram = nil
          @definition_level_histogram = nil
          @column_crypto_metadata = nil
          @encrypted_column_metadata = nil
          @plaintext_footer_mode = false
          @geo_statistics = nil
        end

        # The encryption metadata for this column chunk, if any.
        def crypto_metadata : ::Parquet::File::ColumnCryptoMetaData?
          @column_crypto_metadata
        end

        # Returns a builder for column chunk metadata.
        # The `(start, length)` byte range of this column chunk, including its
        # dictionary page when present (upstream `byte_range`).
        def byte_range : Tuple(Int64, Int64)
          start = @dictionary_page_offset || @data_page_offset
          {start, @total_compressed_size}
        end

        def self.builder(column_descr : Parquet::Schema::ColumnDescriptor) : ColumnChunkMetaDataBuilder
          ColumnChunkMetaDataBuilder.new(column_descr)
        end

        def into_builder : ColumnChunkMetaDataBuilder
          ColumnChunkMetaDataBuilder.from(self)
        end

        # Type of this column (must be primitive).
        def column_type : Type
          @column_descr.physical_type
        end

        def column_path : Parquet::Schema::ColumnPath
          @column_descr.path
        end

        def compression_codec : CompressionCodec
          @compression
        end

        def compressed_size : Int64
          @total_compressed_size
        end

        def uncompressed_size : Int64
          @total_uncompressed_size
        end

        # All encodings used for this column.
        def encodings_list : Array(Encoding)
          @encodings.encodings
        end

        def encodings_mask : EncodingMask
          @encodings
        end

        # The full page encoding stats, if kept in full form.
        def page_encoding_stats : Array(PageEncodingStats)?
          @encoding_stats.try(&.full)
        end

        # The page encoding stats as a bitmask, if condensed.
        def page_encoding_stats_mask : EncodingMask?
          @encoding_stats.try(&.mask)
        end

        # Returns the range `[offset, offset+length)` for the column index.
        def column_index_range : Range(UInt64, UInt64)?
          offset = @column_index_offset
          length = @column_index_length
          return nil if offset.nil? || length.nil? || offset < 0 || length < 0

          o = offset.to_u64
          o...(o + length.to_u64)
        end

        # Returns the range `[offset, offset+length)` for the offset index.
        def offset_index_range : Range(UInt64, UInt64)?
          offset = @offset_index_offset
          length = @offset_index_length
          return nil if offset.nil? || length.nil? || offset < 0 || length < 0

          o = offset.to_u64
          o...(o + length.to_u64)
        end

        # ameba:disable Metrics/CyclomaticComplexity
        def ==(other : ColumnChunkMetaData) : Bool
          column_descr == other.column_descr &&
            encodings == other.encodings &&
            file_path == other.file_path &&
            file_offset == other.file_offset &&
            num_values == other.num_values &&
            compression == other.compression &&
            total_compressed_size == other.total_compressed_size &&
            total_uncompressed_size == other.total_uncompressed_size &&
            data_page_offset == other.data_page_offset &&
            index_page_offset == other.index_page_offset &&
            dictionary_page_offset == other.dictionary_page_offset &&
            statistics == other.statistics &&
            encoding_stats == other.encoding_stats &&
            bloom_filter_offset == other.bloom_filter_offset &&
            bloom_filter_length == other.bloom_filter_length &&
            offset_index_offset == other.offset_index_offset &&
            offset_index_length == other.offset_index_length &&
            column_index_offset == other.column_index_offset &&
            column_index_length == other.column_index_length &&
            unencoded_byte_array_data_bytes == other.unencoded_byte_array_data_bytes &&
            repetition_level_histogram == other.repetition_level_histogram &&
            definition_level_histogram == other.definition_level_histogram &&
            column_crypto_metadata == other.column_crypto_metadata &&
            encrypted_column_metadata == other.encrypted_column_metadata &&
            plaintext_footer_mode == other.plaintext_footer_mode &&
            geo_statistics == other.geo_statistics
        end

        def self.thrift_element_type : ::Parquet::Thrift::ElementType
          ::Parquet::Thrift::ElementType::Struct
        end

        # Serializes this as a thrift `ColumnChunk`.
        def write_thrift(prot : ::Parquet::Thrift::ThriftCompactOutputProtocol) : Nil
          last_field_id = 0_i16
          if fp = @file_path
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, fp, 1_i16, last_field_id)
          end
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, @file_offset, 2_i16, last_field_id)

          # Skip writing the plaintext meta_data when the footer is encrypted and
          # an encrypted blob is present (matches upstream).
          write_meta_data = @encrypted_column_metadata.nil? || @plaintext_footer_mode
          if write_meta_data
            prot.write_field_begin(::Parquet::Thrift::FieldType::Struct, 3_i16, last_field_id)
            Metadata.serialize_column_meta_data(self, prot)
            last_field_id = 3_i16
          end

          if v = @offset_index_offset
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, v, 4_i16, last_field_id)
          end
          if v = @offset_index_length
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, v, 5_i16, last_field_id)
          end
          if v = @column_index_offset
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, v, 6_i16, last_field_id)
          end
          if v = @column_index_length
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, v, 7_i16, last_field_id)
          end
          if crypto = @column_crypto_metadata
            last_field_id = ::Parquet::Thrift.write_thrift_field(prot, crypto, 8_i16, last_field_id)
          end
          if encrypted_meta = @encrypted_column_metadata
            ::Parquet::Thrift.write_thrift_field(prot, encrypted_meta, 9_i16, last_field_id)
          end

          prot.write_struct_end
        end
      end

      # Builder for `ColumnChunkMetaData`.
      class ColumnChunkMetaDataBuilder
        def initialize(@meta : ColumnChunkMetaData)
        end

        def self.new(column_descr : Parquet::Schema::ColumnDescriptor) : ColumnChunkMetaDataBuilder
          new(ColumnChunkMetaData.new(column_descr))
        end

        def self.from(meta : ColumnChunkMetaData) : ColumnChunkMetaDataBuilder
          new(meta)
        end

        # ameba:disable Naming/AccessorMethodName
        def set_encodings(encodings : Array(Encoding)) : self
          @meta.encodings = EncodingMask.new_from_encodings(encodings)
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_encodings_mask(encodings : EncodingMask) : self
          @meta.encodings = encodings
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_file_path(value : String) : self
          @meta.file_path = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_num_values(value : Int64) : self
          @meta.num_values = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_compression_codec(value : CompressionCodec) : self
          @meta.compression = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_total_compressed_size(value : Int64) : self
          @meta.total_compressed_size = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_total_uncompressed_size(value : Int64) : self
          @meta.total_uncompressed_size = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_data_page_offset(value : Int64) : self
          @meta.data_page_offset = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_dictionary_page_offset(value : Int64?) : self
          @meta.dictionary_page_offset = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_index_page_offset(value : Int64?) : self
          @meta.index_page_offset = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_statistics(value : Statistics) : self
          @meta.statistics = value
          self
        end

        def clear_statistics : self
          @meta.statistics = nil
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_page_encoding_stats(value : Array(PageEncodingStats)) : self
          @meta.encoding_stats = ParquetPageEncodingStats.full(value)
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_page_encoding_stats_mask(value : EncodingMask) : self
          @meta.encoding_stats = ParquetPageEncodingStats.mask(value)
          self
        end

        def clear_page_encoding_stats : self
          @meta.encoding_stats = nil
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_bloom_filter_offset(value : Int64?) : self
          @meta.bloom_filter_offset = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_bloom_filter_length(value : Int32?) : self
          @meta.bloom_filter_length = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_offset_index_offset(value : Int64?) : self
          @meta.offset_index_offset = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_offset_index_length(value : Int32?) : self
          @meta.offset_index_length = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_column_index_offset(value : Int64?) : self
          @meta.column_index_offset = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_column_index_length(value : Int32?) : self
          @meta.column_index_length = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_unencoded_byte_array_data_bytes(value : Int64?) : self
          @meta.unencoded_byte_array_data_bytes = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_repetition_level_histogram(value : LevelHistogram?) : self
          @meta.repetition_level_histogram = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_definition_level_histogram(value : LevelHistogram?) : self
          @meta.definition_level_histogram = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_column_crypto_metadata(value : ::Parquet::File::ColumnCryptoMetaData?) : self
          @meta.column_crypto_metadata = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_encrypted_column_metadata(value : Bytes?) : self
          @meta.encrypted_column_metadata = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_plaintext_footer_mode(value : Bool) : self
          @meta.plaintext_footer_mode = value
          self
        end

        # ameba:disable Naming/AccessorMethodName
        def set_geo_statistics(value : ::Parquet::Geospatial::GeospatialStatistics) : self
          @meta.geo_statistics = value
          self
        end

        def build : ColumnChunkMetaData
          @meta
        end
      end

      # Bit positions for required fields in the thrift `ColumnMetaData` struct.
      COL_META_TYPE             = 1_u16 << 1
      COL_META_ENCODINGS        = 1_u16 << 2
      COL_META_CODEC            = 1_u16 << 4
      COL_META_NUM_VALUES       = 1_u16 << 5
      COL_META_TOTAL_UNCOMP_SZ  = 1_u16 << 6
      COL_META_TOTAL_COMP_SZ    = 1_u16 << 7
      COL_META_DATA_PAGE_OFFSET = 1_u16 << 9
      COL_META_ALL_REQUIRED     = COL_META_TYPE | COL_META_ENCODINGS | COL_META_CODEC |
                                  COL_META_NUM_VALUES | COL_META_TOTAL_UNCOMP_SZ |
                                  COL_META_TOTAL_COMP_SZ | COL_META_DATA_PAGE_OFFSET

      def self.validate_column_metadata(mask : UInt16) : Nil
        return if mask == COL_META_ALL_REQUIRED

        raise general_err!("Required field encodings is missing") if (mask & COL_META_ENCODINGS) == 0
        raise general_err!("Required field codec is missing") if (mask & COL_META_CODEC) == 0
        raise general_err!("Required field num_values is missing") if (mask & COL_META_NUM_VALUES) == 0
        unless (mask & COL_META_TOTAL_UNCOMP_SZ) != 0
          raise general_err!("Required field total_uncompressed_size is missing")
        end
        unless (mask & COL_META_TOTAL_COMP_SZ) != 0
          raise general_err!("Required field total_compressed_size is missing")
        end
        unless (mask & COL_META_DATA_PAGE_OFFSET) != 0
          raise general_err!("Required field data_page_offset is missing")
        end
      end

      # Reads a thrift `list<PageEncodingStats>` and condenses it to a bitmask of
      # the encodings used for data pages.
      def self.read_encoding_stats_as_mask(prot : ::Parquet::Thrift::ThriftCompactInputProtocol) : EncodingMask
        mask = 0_i32
        list_ident = prot.read_list_begin
        ::Parquet::Thrift.validate_list_type(::Parquet::Thrift::ElementType::Struct, list_ident)
        list_ident.size.times do
          pes = PageEncodingStats.read_thrift(prot)
          case pes.page_type
          when PageType::DATA_PAGE, PageType::DATA_PAGE_V2
            mask |= 1_i32 << pes.encoding.value
          end
        end
        EncodingMask.try_new(mask)
      end

      # Decodes `ColumnMetaData` into `column`, returning a mask of seen fields.
      # ameba:disable Metrics/CyclomaticComplexity
      def self.read_column_metadata(
        prot : ::Parquet::Thrift::ThriftCompactInputProtocol,
        column : ColumnChunkMetaData,
        col_index : Int32,
        options : ParquetMetaDataOptions? = nil,
      ) : UInt16
        seen_mask = 0_u16
        last_field_id = 0_i16
        loop do
          field_ident = prot.read_field_begin(last_field_id)
          break if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop

          case field_ident.id
          when 1
            Type.read_thrift(prot)
            seen_mask |= COL_META_TYPE
          when 2
            column.encodings = EncodingMask.read_thrift(prot)
            seen_mask |= COL_META_ENCODINGS
          when 4
            column.compression = CompressionCodec.read_thrift(prot)
            seen_mask |= COL_META_CODEC
          when 5
            column.num_values = prot.read_i64
            seen_mask |= COL_META_NUM_VALUES
          when 6
            column.total_uncompressed_size = prot.read_i64
            seen_mask |= COL_META_TOTAL_UNCOMP_SZ
          when 7
            column.total_compressed_size = prot.read_i64
            seen_mask |= COL_META_TOTAL_COMP_SZ
          when 9
            column.data_page_offset = prot.read_i64
            seen_mask |= COL_META_DATA_PAGE_OFFSET
          when 10
            column.index_page_offset = prot.read_i64
          when 11
            column.dictionary_page_offset = prot.read_i64
          when 12
            if options && options.skip_column_stats(col_index)
              prot.skip(field_ident.field_type)
            else
              page_stats = Format::PageStatistics.read_thrift(prot)
              column.statistics = Statistics.from_thrift_page_stats(column.column_type, page_stats)
            end
          when 13
            if options && options.skip_encoding_stats(col_index)
              prot.skip(field_ident.field_type)
            elsif options && !options.encoding_stats_as_mask?
              column.encoding_stats = ParquetPageEncodingStats.full(::Parquet::Thrift.read_thrift_vec(prot, PageEncodingStats))
            else
              column.encoding_stats = ParquetPageEncodingStats.mask(read_encoding_stats_as_mask(prot))
            end
          when 14
            column.bloom_filter_offset = prot.read_i64
          when 15
            column.bloom_filter_length = prot.read_i32
          when 16
            if options && options.skip_size_stats(col_index)
              prot.skip(field_ident.field_type)
              last_field_id = field_ident.id
              next
            end
            size_stats = SizeStatistics.read_thrift(prot)
            column.unencoded_byte_array_data_bytes = size_stats.unencoded_byte_array_data_bytes
            column.repetition_level_histogram =
              size_stats.repetition_level_histogram.try { |hist| LevelHistogram.from(hist) }
            column.definition_level_histogram =
              size_stats.definition_level_histogram.try { |hist| LevelHistogram.from(hist) }
          when 17
            column.geo_statistics = convert_geo_stats(GeospatialStatistics.read_thrift(prot))
          else
            prot.skip(field_ident.field_type)
          end
          last_field_id = field_ident.id
        end
        seen_mask
      end

      # Decodes a thrift `ColumnChunk` into `ColumnChunkMetaData`.
      def self.read_column_chunk(
        prot : ::Parquet::Thrift::ThriftCompactInputProtocol,
        column_descr : Parquet::Schema::ColumnDescriptor,
        col_index : Int32,
        options : ParquetMetaDataOptions? = nil,
      ) : ColumnChunkMetaData
        column = ColumnChunkMetaData.new(column_descr)
        has_file_offset = false
        col_meta_mask = 0_u16

        last_field_id = 0_i16
        loop do
          field_ident = prot.read_field_begin(last_field_id)
          break if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop

          case field_ident.id
          when 1
            column.file_path = prot.read_string
          when 2
            column.file_offset = prot.read_i64
            has_file_offset = true
          when 3
            col_meta_mask = read_column_metadata(prot, column, col_index, options)
          when 4
            column.offset_index_offset = prot.read_i64
          when 5
            column.offset_index_length = prot.read_i32
          when 6
            column.column_index_offset = prot.read_i64
          when 7
            column.column_index_length = prot.read_i32
          when 8
            column.column_crypto_metadata = ::Parquet::File::ColumnCryptoMetaData.read_thrift(prot)
          when 9
            column.encrypted_column_metadata = prot.read_bytes
          else
            prot.skip(field_ident.field_type)
          end
          last_field_id = field_ident.id
        end

        raise general_err!("Required field file_offset is missing") unless has_file_offset

        # If encrypted, return early; the remaining metadata is populated after
        # the (encrypted) footer is decrypted.
        return column if column.encrypted_column_metadata

        validate_column_metadata(col_meta_mask)
        column
      end

      # Serializes the `ColumnMetaData` struct of a column chunk.
      def self.serialize_column_meta_data(
        column_chunk : ColumnChunkMetaData,
        prot : ::Parquet::Thrift::ThriftCompactOutputProtocol,
      ) : Nil
        last_field_id = ::Parquet::Thrift.write_thrift_field(prot, column_chunk.column_type, 1_i16, 0_i16)
        last_field_id = ::Parquet::Thrift.write_thrift_field(prot, column_chunk.encodings_list, 2_i16, last_field_id)
        last_field_id = ::Parquet::Thrift.write_thrift_field(prot, column_chunk.column_descr.path.parts, 3_i16, last_field_id)
        last_field_id = ::Parquet::Thrift.write_thrift_field(prot, column_chunk.compression, 4_i16, last_field_id)
        last_field_id = ::Parquet::Thrift.write_thrift_field(prot, column_chunk.num_values, 5_i16, last_field_id)
        last_field_id = ::Parquet::Thrift.write_thrift_field(prot, column_chunk.total_uncompressed_size, 6_i16, last_field_id)
        last_field_id = ::Parquet::Thrift.write_thrift_field(prot, column_chunk.total_compressed_size, 7_i16, last_field_id)
        last_field_id = ::Parquet::Thrift.write_thrift_field(prot, column_chunk.data_page_offset, 9_i16, last_field_id)

        if v = column_chunk.index_page_offset
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, v, 10_i16, last_field_id)
        end
        if v = column_chunk.dictionary_page_offset
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, v, 11_i16, last_field_id)
        end

        if stats = Statistics.page_stats_to_thrift(column_chunk.statistics)
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, stats, 12_i16, last_field_id)
        end
        if full = column_chunk.page_encoding_stats
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, full, 13_i16, last_field_id)
        end
        if v = column_chunk.bloom_filter_offset
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, v, 14_i16, last_field_id)
        end
        if v = column_chunk.bloom_filter_length
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, v, 15_i16, last_field_id)
        end

        if column_chunk.unencoded_byte_array_data_bytes || column_chunk.repetition_level_histogram || column_chunk.definition_level_histogram
          size_stats = SizeStatistics.new(
            column_chunk.unencoded_byte_array_data_bytes,
            column_chunk.repetition_level_histogram.try(&.into_inner),
            column_chunk.definition_level_histogram.try(&.into_inner),
          )
          last_field_id = ::Parquet::Thrift.write_thrift_field(prot, size_stats, 16_i16, last_field_id)
        end

        if geo_stats = column_chunk.geo_statistics
          ::Parquet::Thrift.write_thrift_field(prot, geo_stats_to_thrift(geo_stats), 17_i16, last_field_id)
        end

        prot.write_struct_end
      end
    end
  end
end
