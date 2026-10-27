# parquet/src/file/metadata/memory.rs and related HeapSize implementations,
# apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b.
module Parquet
  # Descendant allocations, excluding the receiver's inline/object storage.
  module HeapSize
    abstract def heap_size : Int64
  end

  # Native retained-byte estimates. Counts class storage, array/hash capacity,
  # strings and visible slice bytes; excludes allocator/GC overhead. Shared
  # descendants can be counted more than once, following upstream ownership.
  module MemorySize
    def self.retained(value : Nil | Number | Bool | Enum) : Int64
      0_i64
    end

    def self.retained(value : String) : Int64
      String::HEADER_SIZE.to_i64 + value.bytesize + 1
    end

    def self.retained(values : Slice(T)) : Int64 forall T
      values.size.to_i64 * sizeof(T) + values.sum(0_i64) { |value| retained(value) }
    end

    def self.retained(values : Array(T)) : Int64 forall T
      # Crystal has no public capacity query. These read-only layout queries
      # track the Crystal 1.22 container implementation (the minimum runtime).
      instance_sizeof(Array(T)).to_i64 + values.@capacity.to_i64 * sizeof(T) +
        values.sum(0_i64) { |value| retained(value) }
    end

    def self.retained(values : Hash(K, V)) : Int64 forall K, V
      buckets = values.@entries.null? ? 0_i64 : 1_i64 << values.@indices_size_pow2
      entries = buckets // 2 * sizeof(Hash::Entry(K, V))
      indices = values.@indices.null? ? 0_i64 : buckets * values.@indices_bytesize
      instance_sizeof(Hash(K, V)).to_i64 + entries + indices +
        values.sum(0_i64) { |key, value| retained(key) + retained(value) }
    end

    # Upstream only accounts for its concrete PageIndex implementation.
    def self.retained(value : File::Metadata::PageIndexProvider) : Int64
      value.is_a?(File::Metadata::PageIndex) ? value.allocation_size + value.heap_size : 0_i64
    end

    def self.retained(value : T) : Int64 forall T
      {% if T.union? %}
        case value
        {% for type in T.union_types %}
        when {{type}}
          retained(value.as({{type}}))
        {% end %}
        else
          0_i64
        end
      {% else %}
        value.allocation_size + value.heap_size
      {% end %}
    end

    # List owned descendants explicitly so schema/column back-references and
    # opaque application callbacks cannot introduce cycles or double counting.
    macro fields(*names)
      include ::Parquet::HeapSize

      def heap_size : Int64
        total = 0_i64
        {% for name in names %}
          total += ::Parquet::MemorySize.retained(@{{name.id}})
        {% end %}
        total
      end

      {% verbatim do %}
        def allocation_size : Int64
          {% if @type < Reference %}
            instance_sizeof({{@type}}).to_i64
          {% else %}
            0_i64
          {% end %}
        end
      {% end %}
    end
  end
end

abstract class Parquet::Schema::Type
  include Parquet::HeapSize

  abstract def allocation_size : Int64
end

class Parquet::Schema::PrimitiveType
  Parquet::MemorySize.fields basic_info
end

class Parquet::Schema::GroupType
  Parquet::MemorySize.fields basic_info, fields
end

struct Parquet::Schema::BasicTypeInfo
  # Logical types are allocated classes in this port, unlike Rust enum fields.
  Parquet::MemorySize.fields name, logical_type
end

struct Parquet::Schema::ColumnPath
  Parquet::MemorySize.fields parts
end

class Parquet::Schema::ColumnDescriptor
  # primitive_type is already counted by SchemaDescriptor's schema tree.
  Parquet::MemorySize.fields path
end

class Parquet::Schema::SchemaDescriptor
  Parquet::MemorySize.fields schema, leaves, leaf_to_base
end

struct Parquet::Int96
  Parquet::MemorySize.fields
end

struct Parquet::ByteArray
  Parquet::MemorySize.fields data
end

struct Parquet::FixedLenByteArray
  Parquet::MemorySize.fields value
end

struct Parquet::ColumnOrder
  Parquet::MemorySize.fields
end

struct Parquet::EncodingMask
  Parquet::MemorySize.fields
end

struct Parquet::ValueStatistics(T)
  Parquet::MemorySize.fields min, max
end

class Parquet::Statistics
  Parquet::MemorySize.fields value
end

module Parquet::File::Metadata
  class ParquetMetaData
    Parquet::MemorySize.fields file_metadata, row_groups, page_index, file_decryptor

    # Estimate for metadata cache budgets, including this object. Counts shared
    # allocations per reference except the known schema back-references, and
    # excludes custom page-index providers. Not a process/allocator measurement.
    def memory_size : Int64
      allocation_size + heap_size
    end
  end

  class FileMetaData
    Parquet::MemorySize.fields created_by, key_value_metadata, schema_descr,
      column_orders, encryption_algorithm, footer_signing_key_metadata
  end

  class RowGroupMetaData
    # schema_descr belongs to FileMetaData's ownership accounting.
    Parquet::MemorySize.fields columns, sorting_columns
  end

  class ColumnChunkMetaData
    # column_descr is already included in the file's schema descriptor.
    Parquet::MemorySize.fields encodings, file_path, compression, statistics,
      encoding_stats, unencoded_byte_array_data_bytes, repetition_level_histogram,
      definition_level_histogram, geo_statistics, column_crypto_metadata,
      encrypted_column_metadata
  end

  struct KeyValue
    Parquet::MemorySize.fields key, value
  end

  struct ParquetPageEncodingStats
    Parquet::MemorySize.fields full
  end

  struct PageEncodingStats
    Parquet::MemorySize.fields
  end

  struct SortingColumn
    Parquet::MemorySize.fields
  end

  struct LevelHistogram
    Parquet::MemorySize.fields inner
  end

  class PageIndex
    Parquet::MemorySize.fields column_indexes, offset_indexes
  end

  struct AesGcmV1
    Parquet::MemorySize.fields aad_prefix, aad_file_unique
  end

  struct AesGcmCtrV1
    Parquet::MemorySize.fields aad_prefix, aad_file_unique
  end

  struct EncryptionAlgorithm
    Parquet::MemorySize.fields aes_gcm_v1, aes_gcm_ctr_v1
  end
end

module Parquet::File::PageIndex
  struct ColumnIndex
    Parquet::MemorySize.fields null_pages, null_counts, nan_counts,
      definition_level_histograms, repetition_level_histograms
  end

  struct PrimitiveColumnIndex(T)
    Parquet::MemorySize.fields column_index, min_values, max_values
  end

  struct ByteArrayColumnIndex
    Parquet::MemorySize.fields column_index, min_bytes, min_offsets, max_bytes, max_offsets
  end

  struct ColumnIndexMetaData
    Parquet::MemorySize.fields inner
  end

  struct OffsetIndexMetaData
    Parquet::MemorySize.fields page_locations, unencoded_byte_array_data_bytes
  end

  struct PageLocation
    Parquet::MemorySize.fields
  end
end

module Parquet::File
  struct ColumnCryptoMetaData
    Parquet::MemorySize.fields encryption_with_column_key
  end

  struct EncryptionWithColumnKey
    Parquet::MemorySize.fields path_in_schema, key_metadata
  end
end

module Parquet::Geospatial
  struct BoundingBox
    Parquet::MemorySize.fields
  end

  class GeospatialStatistics
    Parquet::MemorySize.fields bounding_box, geospatial_types
  end
end

module Parquet::Encryption
  struct ExplicitDecryptionKeys
    Parquet::MemorySize.fields footer_key, column_keys
  end

  struct DecryptionKeys
    # Application-owned KeyRetriever state is opaque, as upstream.
    Parquet::MemorySize.fields explicit
  end

  class FileDecryptionProperties
    Parquet::MemorySize.fields keys, aad_prefix
  end

  abstract class BlockDecryptor
    # Unknown application cipher implementations have no inspectable storage.
    def heap_size : Int64
      0_i64
    end

    def allocation_size : Int64
      0_i64
    end
  end

  class GcmBlockDecryptor
    Parquet::MemorySize.fields key
  end

  class FileDecryptor
    Parquet::MemorySize.fields decryption_properties, footer_decryptor, file_aad
  end
end

abstract class Parquet::LogicalType
  include Parquet::HeapSize

  abstract def allocation_size : Int64
end

class Parquet::LogicalType::Map
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::List
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::Enum
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::Date
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::Json
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::Bson
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::Uuid
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::Float16
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::Unknown
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::File
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::String
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::UnknownField
  Parquet::MemorySize.fields
end

class Parquet::LogicalType::Decimal
  Parquet::MemorySize.fields value
end

class Parquet::LogicalType::Time
  Parquet::MemorySize.fields value
end

class Parquet::LogicalType::Timestamp
  Parquet::MemorySize.fields value
end

class Parquet::LogicalType::Integer
  Parquet::MemorySize.fields value
end

class Parquet::LogicalType::Variant
  Parquet::MemorySize.fields value
end

class Parquet::LogicalType::Geometry
  Parquet::MemorySize.fields value
end

class Parquet::LogicalType::Geography
  Parquet::MemorySize.fields value
end

struct Parquet::DecimalType
  Parquet::MemorySize.fields
end

struct Parquet::TimestampType
  Parquet::MemorySize.fields
end

struct Parquet::IntType
  Parquet::MemorySize.fields
end

struct Parquet::VariantType
  Parquet::MemorySize.fields
end

struct Parquet::EdgeInterpolationAlgorithm
  Parquet::MemorySize.fields
end

struct Parquet::GeometryType
  Parquet::MemorySize.fields crs
end

struct Parquet::GeographyType
  Parquet::MemorySize.fields crs
end
