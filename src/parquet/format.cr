# Ported from `parquet/src/file/metadata/thrift/mod.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Currently ports the page-header thrift structs and their
# `read_thrift_without_stats` variants. The metadata structs (`FileMetaData`,
# `RowGroup`, `ColumnChunk`, `Statistics`, ...) are ported with the file
# metadata feature (see `plans/parity.md`).
module Parquet
  module Format
    # Empty thrift struct `IndexPageHeader`.
    define_thrift_struct(IndexPageHeader) do
    end

    # Mirrors thrift struct `DictionaryPageHeader`.
    define_thrift_struct(DictionaryPageHeader) do
      required 1, num_values, Int32
      required 2, encoding, Encoding
      optional 3, is_sorted, Bool
    end

    # Statistics embedded in a page header.
    define_thrift_struct(PageStatistics) do
      optional 1, max, Bytes
      optional 2, min, Bytes
      optional 3, null_count, Int64
      optional 4, distinct_count, Int64
      optional 5, max_value, Bytes
      optional 6, min_value, Bytes
      optional 7, is_max_value_exact, Bool
      optional 8, is_min_value_exact, Bool
      optional 9, nan_count, Int64
    end

    # Mirrors thrift struct `DataPageHeader`.
    define_thrift_struct(DataPageHeader) do
      required 1, num_values, Int32
      required 2, encoding, Encoding
      required 3, definition_level_encoding, Encoding
      required 4, repetition_level_encoding, Encoding
      optional 5, statistics, PageStatistics
    end

    # Reader that skips decoding page statistics.
    def self.read_data_page_header_without_stats(
      prot : ::Parquet::Thrift::ThriftCompactInputProtocol,
    ) : DataPageHeader
      num_values = nil.as(Int32?)
      encoding = nil.as(Encoding?)
      definition_level_encoding = nil.as(Encoding?)
      repetition_level_encoding = nil.as(Encoding?)

      last_field_id = 0_i16
      loop do
        field_ident = prot.read_field_begin(last_field_id)
        break if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop

        case field_ident.id
        when 1 then num_values = prot.read_i32
        when 2 then encoding = Encoding.read_thrift(prot)
        when 3 then definition_level_encoding = Encoding.read_thrift(prot)
        when 4 then repetition_level_encoding = Encoding.read_thrift(prot)
        else        prot.skip(field_ident.field_type)
        end
        last_field_id = field_ident.id
      end

      raise general_err!("Required field num_values is missing") unless num_values
      raise general_err!("Required field encoding is missing") unless encoding
      unless definition_level_encoding
        raise general_err!("Required field definition_level_encoding is missing")
      end
      unless repetition_level_encoding
        raise general_err!("Required field repetition_level_encoding is missing")
      end

      DataPageHeader.new(num_values, encoding, definition_level_encoding, repetition_level_encoding, nil)
    end

    # Mirrors thrift struct `DataPageHeaderV2`.
    define_thrift_struct(DataPageHeaderV2) do
      required 1, num_values, Int32
      required 2, num_nulls, Int32
      required 3, num_rows, Int32
      required 4, encoding, Encoding
      required 5, definition_levels_byte_length, Int32
      required 6, repetition_levels_byte_length, Int32
      optional 7, is_compressed, Bool
      optional 8, statistics, PageStatistics
    end

    # Reader that skips decoding page statistics.
    # ameba:disable Metrics/CyclomaticComplexity
    def self.read_data_page_header_v2_without_stats(
      prot : ::Parquet::Thrift::ThriftCompactInputProtocol,
    ) : DataPageHeaderV2
      num_values = nil.as(Int32?)
      num_nulls = nil.as(Int32?)
      num_rows = nil.as(Int32?)
      encoding = nil.as(Encoding?)
      definition_levels_byte_length = nil.as(Int32?)
      repetition_levels_byte_length = nil.as(Int32?)
      is_compressed = nil.as(Bool?)

      last_field_id = 0_i16
      loop do
        field_ident = prot.read_field_begin(last_field_id)
        break if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop

        case field_ident.id
        when 1 then num_values = prot.read_i32
        when 2 then num_nulls = prot.read_i32
        when 3 then num_rows = prot.read_i32
        when 4 then encoding = Encoding.read_thrift(prot)
        when 5 then definition_levels_byte_length = prot.read_i32
        when 6 then repetition_levels_byte_length = prot.read_i32
        when 7 then is_compressed = field_ident.bool_val
        else        prot.skip(field_ident.field_type)
        end
        last_field_id = field_ident.id
      end

      raise general_err!("Required field num_values is missing") unless num_values
      raise general_err!("Required field num_nulls is missing") unless num_nulls
      raise general_err!("Required field num_rows is missing") unless num_rows
      raise general_err!("Required field encoding is missing") unless encoding
      unless definition_levels_byte_length
        raise general_err!("Required field definition_levels_byte_length is missing")
      end
      unless repetition_levels_byte_length
        raise general_err!("Required field repetition_levels_byte_length is missing")
      end

      DataPageHeaderV2.new(
        num_values, num_nulls, num_rows, encoding,
        definition_levels_byte_length, repetition_levels_byte_length,
        is_compressed, nil,
      )
    end

    # Mirrors thrift struct `PageHeader`.
    define_thrift_struct(PageHeader) do
      required 1, type, PageType
      required 2, uncompressed_page_size, Int32
      required 3, compressed_page_size, Int32
      optional 4, crc, Int32
      optional 5, data_page_header, DataPageHeader
      optional 6, index_page_header, IndexPageHeader
      optional 7, dictionary_page_header, DictionaryPageHeader
      optional 8, data_page_header_v2, DataPageHeaderV2
    end

    # Reader that skips reading page statistics.
    # ameba:disable Metrics/CyclomaticComplexity
    def self.read_page_header_without_stats(
      prot : ::Parquet::Thrift::ThriftCompactInputProtocol,
    ) : PageHeader
      page_type = nil.as(PageType?)
      uncompressed_page_size = nil.as(Int32?)
      compressed_page_size = nil.as(Int32?)
      crc = nil.as(Int32?)
      data_page_header = nil.as(DataPageHeader?)
      index_page_header = nil.as(IndexPageHeader?)
      dictionary_page_header = nil.as(DictionaryPageHeader?)
      data_page_header_v2 = nil.as(DataPageHeaderV2?)

      last_field_id = 0_i16
      loop do
        field_ident = prot.read_field_begin(last_field_id)
        break if field_ident.field_type == ::Parquet::Thrift::FieldType::Stop

        case field_ident.id
        when 1 then page_type = PageType.read_thrift(prot)
        when 2 then uncompressed_page_size = prot.read_i32
        when 3 then compressed_page_size = prot.read_i32
        when 4 then crc = prot.read_i32
        when 5 then data_page_header = Format.read_data_page_header_without_stats(prot)
        when 6 then index_page_header = IndexPageHeader.read_thrift(prot)
        when 7 then dictionary_page_header = DictionaryPageHeader.read_thrift(prot)
        when 8 then data_page_header_v2 = Format.read_data_page_header_v2_without_stats(prot)
        else        prot.skip(field_ident.field_type)
        end
        last_field_id = field_ident.id
      end

      raise general_err!("Required field type_ is missing") unless page_type
      unless uncompressed_page_size
        raise general_err!("Required field uncompressed_page_size is missing")
      end
      unless compressed_page_size
        raise general_err!("Required field compressed_page_size is missing")
      end

      PageHeader.new(
        page_type, uncompressed_page_size, compressed_page_size, crc,
        data_page_header, index_page_header, dictionary_page_header, data_page_header_v2,
      )
    end
  end
end
