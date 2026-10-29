# Owned-field Clone semantics from parquet/src/file/metadata/mod.rs and
# encryption/decrypt.rs (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
# Schema descriptors, page-index providers, decryption properties and block
# decryptors retain upstream's shared-reference semantics. All mutable owned
# arrays and byte buffers are copied without a lossy Thrift round trip.
module Parquet
  module MetadataSnapshot
    def self.copy(value : T) forall T
      {% if T.union? %}
        case value
        {% for member in T.union_types %}
        when {{member}}
          copy(value)
        {% end %}
        else
          raise "Unreachable metadata snapshot type"
        end
      {% elsif T < Value || T == String %}
        value
      {% else %}
        value.clone
      {% end %}
    end

    def self.copy(value : Bytes) : Bytes
      value.dup
    end

    def self.copy(value : Array(T)) : Array(T) forall T
      value.map { |item| copy(item) }
    end

    def self.copy(value : ByteArray) : ByteArray
      data = value.@data
      data ? ByteArray.new(data.dup) : ByteArray.new
    end

    def self.copy(value : FixedLenByteArray) : FixedLenByteArray
      FixedLenByteArray.new(copy(value.value))
    end

    def self.copy(value : ValueStatistics(T)) : ValueStatistics(T) forall T
      ValueStatistics(T).new(
        copy(value.min), copy(value.max), value.distinct_count, value.null_count,
        value.min_max_deprecated?, value.nan_count, value.max_value_exact?,
        value.min_value_exact?, value.min_max_backwards_compatible?
      )
    end

    def self.copy(value : File::Metadata::LevelHistogram) : File::Metadata::LevelHistogram
      File::Metadata::LevelHistogram.from(value.inner.dup)
    end

    def self.copy(value : File::Metadata::ParquetPageEncodingStats) : File::Metadata::ParquetPageEncodingStats
      File::Metadata::ParquetPageEncodingStats.new(copy(value.full), value.mask)
    end

    def self.copy(value : File::Metadata::EncryptionAlgorithm) : File::Metadata::EncryptionAlgorithm
      if params = value.aes_gcm_v1
        File::Metadata::EncryptionAlgorithm.aes_gcm_v1(
          File::Metadata::AesGcmV1.new(copy(params.aad_prefix), copy(params.aad_file_unique), params.supply_aad_prefix)
        )
      else
        params = value.aes_gcm_ctr_v1!
        File::Metadata::EncryptionAlgorithm.aes_gcm_ctr_v1(
          File::Metadata::AesGcmCtrV1.new(copy(params.aad_prefix), copy(params.aad_file_unique), params.supply_aad_prefix)
        )
      end
    end

    def self.copy(value : File::ColumnCryptoMetaData) : File::ColumnCryptoMetaData
      if params = value.encryption_with_column_key
        File::ColumnCryptoMetaData.encryption_with_column_key(
          File::EncryptionWithColumnKey.new(copy(params.path_in_schema), copy(params.key_metadata))
        )
      else
        value
      end
    end

    # Start with a value copy of scalar fields, then replace owned fields. An
    # explicit list prevents accidentally cloning upstream Arc-like references.
    macro clone_owned(*fields)
      def clone : self
        result = dup
        result.copy_owned_fields(self)
        result
      end

      protected def copy_owned_fields(other : self) : Nil
        {% for field in fields %}
          @{{field.id}} = ::Parquet::MetadataSnapshot.copy(other.@{{field.id}})
        {% end %}
      end
    end
  end

  class Statistics
    MetadataSnapshot.clone_owned value
  end

  module Geospatial
    class GeospatialStatistics
      MetadataSnapshot.clone_owned geospatial_types
    end
  end

  module Encryption
    class FileDecryptor
      MetadataSnapshot.clone_owned file_aad
    end
  end

  module File::Metadata
    class FileMetaData
      MetadataSnapshot.clone_owned key_value_metadata, column_orders,
        encryption_algorithm, footer_signing_key_metadata
    end

    class ParquetMetaData
      MetadataSnapshot.clone_owned file_metadata, row_groups, file_decryptor
    end

    class RowGroupMetaData
      MetadataSnapshot.clone_owned columns, sorting_columns
    end

    class ColumnChunkMetaData
      MetadataSnapshot.clone_owned statistics, encoding_stats,
        repetition_level_histogram, definition_level_histogram,
        column_crypto_metadata, encrypted_column_metadata, geo_statistics

      # Metadata stores only the codec, not the writer's compression level.
      # Named separately to preserve the existing native `compression` getter.
      def compression_config : Compression
        Compression.from_codec(@compression)
      end
    end

    class ColumnChunkMetaDataBuilder
      # ameba:disable Naming/AccessorMethodName
      def set_compression(value : Compression) : self
        set_compression_codec(value.codec)
      end
    end
  end
end
