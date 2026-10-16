require "../../encryption/encrypt"
require "../../encryption/modules"
require "./thrift/encryption"

# Ported from `parquet/src/file/metadata/writer.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `MetadataObjectWriter`, `ThriftMetadataWriter`, and
# `ParquetMetaDataWriter`, which serialize the page indexes (column index then
# offset index), back-fill their offsets on the column chunks, then write the
# thrift `FileMetaData` and the footer tail (encrypting the footer / column
# metadata / page indexes when a `FileEncryptor` is set).
#
# Divergences: encryption is modelled with a `FileEncryptor?` (no `Arc`); the
# Rust `FileMeta`/`WriteThrift` wrapper is inlined (the file metadata is
# serialized to a buffer before encryption); `apply_row_group_encryption`
# mutates the row groups in place and returns them (upstream returns a separate
# deep copy of the unencrypted metadata); row groups are otherwise shallow.
module Parquet
  module File
    module Metadata
      # Writes thrift metadata objects, optionally encrypting them.
      class MetadataObjectWriter
        def initialize(@file_encryptor : ::Parquet::Encryption::FileEncryptor? = nil)
        end

        # Sets the file encryptor to use.
        def with_file_encryptor(file_encryptor : ::Parquet::Encryption::FileEncryptor?) : self
          @file_encryptor = file_encryptor
          self
        end

        def self.write_thrift_object(object, sink : IO) : Nil
          protocol = ::Parquet::Thrift::ThriftCompactOutputProtocol.new(sink)
          object.write_thrift(protocol)
        end

        # Writes the `FileMetaData`, encrypting it (and possibly prefacing a
        # `FileCryptoMetaData`) when a file encryptor is configured.
        def write_file_metadata(
          file_metadata : FileMetaData,
          row_groups : Array(RowGroupMetaData),
          sink : IO,
          write_path_in_schema : Bool,
        ) : Nil
          encryptor = @file_encryptor
          if encryptor && encryptor.properties.encrypt_footer
            crypto_metadata = MetadataObjectWriter.file_crypto_metadata(encryptor)
            protocol = ::Parquet::Thrift::ThriftCompactOutputProtocol.new(sink)
            crypto_metadata.write_thrift(protocol)

            aad = ::Parquet::Encryption.create_footer_aad(encryptor.file_aad)
            buffer = serialize_file_metadata(file_metadata, row_groups, write_path_in_schema)
            sink.write(encryptor.footer_encryptor.encrypt(buffer, aad))
          elsif encryptor && !file_metadata.encryption_algorithm.nil?
            aad = ::Parquet::Encryption.create_footer_aad(encryptor.file_aad)
            buffer = serialize_file_metadata(file_metadata, row_groups, write_path_in_schema)
            sink.write(buffer)
            encrypted = encryptor.footer_encryptor.encrypt(buffer, aad)
            sink.write(encrypted[::Parquet::Encryption::SIZE_LEN, ::Parquet::Encryption::NONCE_LEN])
            sink.write(encrypted[encrypted.size - ::Parquet::Encryption::TAG_LEN, ::Parquet::Encryption::TAG_LEN])
          else
            Metadata.write_file_metadata(
              file_metadata, row_groups, ::Parquet::Thrift::ThriftCompactOutputProtocol.new(sink), write_path_in_schema
            )
          end
          nil
        end

        # Writes an offset index, encrypting it if its column is encrypted.
        def write_offset_index(
          offset_index : OffsetIndexMetaData,
          column_chunk : ColumnChunkMetaData,
          row_group_idx : Int32,
          column_idx : Int32,
          sink : IO,
        ) : Nil
          encryptor = @file_encryptor
          if encryptor
            write_thrift_object_with_encryption(
              offset_index, sink, encryptor, column_chunk,
              ::Parquet::Encryption::ModuleType::OffsetIndex, row_group_idx, column_idx
            )
          else
            MetadataObjectWriter.write_thrift_object(offset_index, sink)
          end
        end

        # Writes a column index, encrypting it if its column is encrypted.
        def write_column_index(
          column_index : ColumnIndexMetaData,
          column_chunk : ColumnChunkMetaData,
          row_group_idx : Int32,
          column_idx : Int32,
          sink : IO,
        ) : Bool
          encryptor = @file_encryptor
          if encryptor
            write_thrift_object_with_encryption(
              column_index, sink, encryptor, column_chunk,
              ::Parquet::Encryption::ModuleType::ColumnIndex, row_group_idx, column_idx
            )
          else
            MetadataObjectWriter.write_thrift_object(column_index, sink)
          end
          true
        end

        # Encrypts row-group column metadata when a file encryptor is configured.
        #
        # Returns the row groups to write (mutated in place) and, per the
        # divergence, `nil` for the unencrypted copy.
        def apply_row_group_encryption(
          row_groups : Array(RowGroupMetaData),
        ) : Tuple(Array(RowGroupMetaData), Array(RowGroupMetaData)?)
          encryptor = @file_encryptor
          return {row_groups, nil} if encryptor.nil?

          row_groups.each_with_index do |row_group, rg_idx|
            row_group.columns.each_with_index do |column, col_idx|
              encrypt_column_chunk(column, encryptor, rg_idx, col_idx)
            end
          end
          {row_groups, nil}
        end

        # The magic bytes identifying the file type.
        def file_magic : Bytes
          encryptor = @file_encryptor
          if encryptor && encryptor.properties.encrypt_footer
            Parquet::File::PARQUET_MAGIC_ENCRYPTED_FOOTER
          else
            Parquet::File::PARQUET_MAGIC
          end
        end

        # The encryption algorithm and footer key metadata for a plaintext footer.
        def plaintext_footer_crypto_metadata : Tuple(EncryptionAlgorithm?, Bytes?)
          encryptor = @file_encryptor
          if encryptor && !encryptor.properties.encrypt_footer
            return {
              MetadataObjectWriter.encryption_algorithm_from_encryptor(encryptor),
              encryptor.properties.footer_key_metadata,
            }
          end
          {nil, nil}
        end

        def self.encryption_algorithm_from_encryptor(
          file_encryptor : ::Parquet::Encryption::FileEncryptor,
        ) : EncryptionAlgorithm
          properties = file_encryptor.properties
          supply_aad_prefix = properties.aad_prefix.nil? ? nil : !properties.store_aad_prefix
          aad_prefix = properties.store_aad_prefix ? properties.aad_prefix : nil
          EncryptionAlgorithm.aes_gcm_v1(
            AesGcmV1.new(aad_prefix, file_encryptor.aad_file_unique, supply_aad_prefix)
          )
        end

        def self.file_crypto_metadata(
          file_encryptor : ::Parquet::Encryption::FileEncryptor,
        ) : FileCryptoMetaData
          FileCryptoMetaData.new(
            encryption_algorithm_from_encryptor(file_encryptor),
            file_encryptor.properties.footer_key_metadata,
          )
        end

        private def serialize_file_metadata(
          file_metadata : FileMetaData,
          row_groups : Array(RowGroupMetaData),
          write_path_in_schema : Bool,
        ) : Bytes
          io = IO::Memory.new
          Metadata.write_file_metadata(
            file_metadata, row_groups, ::Parquet::Thrift::ThriftCompactOutputProtocol.new(io), write_path_in_schema
          )
          io.to_slice.dup
        end

        private def write_thrift_object_with_encryption(
          object,
          sink : IO,
          file_encryptor : ::Parquet::Encryption::FileEncryptor,
          column_metadata : ColumnChunkMetaData,
          module_type : ::Parquet::Encryption::ModuleType,
          row_group_idx : Int32,
          column_idx : Int32,
        ) : Nil
          path = column_metadata.column_descr.path.parts
          column_path = path.size == 1 ? path[0] : path.join(".")

          if file_encryptor.column_encrypted?(column_path)
            aad = ::Parquet::Encryption.create_module_aad(
              file_encryptor.file_aad, module_type, row_group_idx, column_idx, nil
            )
            encryptor = file_encryptor.column_encryptor(column_path)
            buffer = IO::Memory.new
            MetadataObjectWriter.write_thrift_object(object, buffer)
            sink.write(encryptor.encrypt(buffer.to_slice.dup, aad))
          else
            MetadataObjectWriter.write_thrift_object(object, sink)
          end
        end

        private def encrypt_column_chunk(
          column_chunk : ColumnChunkMetaData,
          file_encryptor : ::Parquet::Encryption::FileEncryptor,
          row_group_idx : Int32,
          column_idx : Int32,
        ) : Nil
          crypto_metadata = column_chunk.crypto_metadata
          encryptor =
            if crypto_metadata.nil?
              nil
            elsif crypto_metadata.kind.encryption_with_footer_key?
              # Under uniform encryption the footer is already encrypted, so the
              # column chunk needs no further encryption -- unless the footer is
              # plaintext, in which case the column metadata is encrypted here.
              file_encryptor.properties.encrypt_footer ? nil : file_encryptor.footer_encryptor
            else
              key = crypto_metadata.encryption_with_column_key!
              file_encryptor.column_encryptor(key.path_in_schema.join("."))
            end

          return if encryptor.nil?

          aad = ::Parquet::Encryption.create_module_aad(
            file_encryptor.file_aad, ::Parquet::Encryption::ModuleType::ColumnMetaData,
            row_group_idx, column_idx, nil
          )
          io = IO::Memory.new
          Metadata.serialize_column_meta_data(column_chunk, ::Parquet::Thrift::ThriftCompactOutputProtocol.new(io))
          column_chunk.encrypted_column_metadata = encryptor.encrypt(io.to_slice.dup, aad)
          column_chunk.plaintext_footer_mode = !file_encryptor.properties.encrypt_footer
          nil
        end
      end

      # Writes `ParquetMetaData` structs to a thrift-encoded byte stream.
      class ThriftMetadataWriter
        def initialize(
          @buf : Parquet::File::TrackedWrite,
          @schema_descr : Parquet::Schema::SchemaDescriptor,
          @row_groups : Array(RowGroupMetaData),
          @created_by : String?,
          @writer_version : Int32,
          @write_path_in_schema : Bool,
        )
          @column_indexes = nil
          @offset_indexes = nil
          @key_value_metadata = nil
          @object_writer = MetadataObjectWriter.new
        end

        # Sets the file encryptor used to encrypt the footer / column metadata /
        # page indexes.
        def with_file_encryptor(file_encryptor : ::Parquet::Encryption::FileEncryptor?) : self
          @object_writer = @object_writer.with_file_encryptor(file_encryptor)
          self
        end

        # Serializes the column indexes and back-fills their offsets.
        def with_column_indexes(column_indexes : Array(Array(ColumnIndexMetaData?))) : self
          @column_indexes = column_indexes
          self
        end

        # Serializes the offset indexes and back-fills their offsets.
        def with_offset_indexes(offset_indexes : Array(Array(OffsetIndexMetaData?))) : self
          @offset_indexes = offset_indexes
          self
        end

        def with_key_value_metadata(key_value_metadata : Array(KeyValue)) : self
          @key_value_metadata = key_value_metadata
          self
        end

        # Assembles and writes the final metadata, returning it.
        def finish : ParquetMetaData
          num_rows = @row_groups.sum(&.num_rows)

          column_indexes = finalize_column_indexes
          offset_indexes = finalize_offset_indexes

          column_orders = @schema_descr.columns.map do |col|
            ColumnOrder.column_order_for_type(col.logical_type_ref, col.converted_type, col.physical_type)
          end

          encrypted_row_groups, _unencrypted = @object_writer.apply_row_group_encryption(@row_groups)
          encryption_algorithm, footer_signing_key_metadata = @object_writer.plaintext_footer_crypto_metadata

          file_metadata = FileMetaData.new(
            @writer_version, num_rows, @created_by, @key_value_metadata, @schema_descr, column_orders,
            encryption_algorithm, footer_signing_key_metadata
          )

          start_pos = @buf.bytes_written
          @object_writer.write_file_metadata(file_metadata, encrypted_row_groups, @buf, @write_path_in_schema)
          end_pos = @buf.bytes_written

          metadata_len = (end_pos - start_pos).to_u32!
          @buf.write_bytes(metadata_len, IO::ByteFormat::LittleEndian)
          @buf.write(@object_writer.file_magic)

          page_index = PageIndex.new(column_indexes, offset_indexes)
          ParquetMetaData.new(file_metadata, encrypted_row_groups, page_index)
        end

        private def finalize_column_indexes : Array(Array(ColumnIndexMetaData?))?
          column_indexes = @column_indexes
          @column_indexes = nil

          if column_indexes
            write_column_indexes(column_indexes)
          end

          return nil if column_indexes.nil? || column_indexes.all?(&.all?(Nil))

          column_indexes
        end

        private def finalize_offset_indexes : Array(Array(OffsetIndexMetaData?))?
          offset_indexes = @offset_indexes
          @offset_indexes = nil

          if offset_indexes
            write_offset_indexes(offset_indexes)
          end

          return nil if offset_indexes.nil? || offset_indexes.all?(&.all?(Nil))

          offset_indexes
        end

        private def write_column_indexes(column_indexes : Array(Array(ColumnIndexMetaData?))) : Nil
          @row_groups.each_with_index do |row_group, rg_idx|
            row_group.columns.each_with_index do |column_metadata, col_idx|
              index = column_indexes[rg_idx][col_idx]?
              next if index.nil?

              start_offset = @buf.bytes_written
              @object_writer.write_column_index(index, column_metadata, rg_idx, col_idx, @buf)
              end_offset = @buf.bytes_written
              column_metadata.column_index_offset = start_offset
              column_metadata.column_index_length = (end_offset - start_offset).to_i32!
            end
          end
        end

        private def write_offset_indexes(offset_indexes : Array(Array(OffsetIndexMetaData?))) : Nil
          @row_groups.each_with_index do |row_group, rg_idx|
            row_group.columns.each_with_index do |column_metadata, col_idx|
              index = offset_indexes[rg_idx][col_idx]?
              next if index.nil?

              start_offset = @buf.bytes_written
              @object_writer.write_offset_index(index, column_metadata, rg_idx, col_idx, @buf)
              end_offset = @buf.bytes_written
              column_metadata.offset_index_offset = start_offset
              column_metadata.offset_index_length = (end_offset - start_offset).to_i32!
            end
          end
        end
      end

      # Writes `ParquetMetaData` to a byte stream.
      #
      # The output is: optional column indexes, optional offset indexes, the
      # thrift `FileMetaData`, the 4-byte metadata length, and the magic
      # (`PAR1`, or `PARE` for an encrypted footer).
      class ParquetMetaDataWriter
        def initialize(@metadata : ParquetMetaData, @tracked : Parquet::File::TrackedWrite)
          @write_path_in_schema = true
          @file_encryptor = nil.as(::Parquet::Encryption::FileEncryptor?)
        end

        def self.new(buf : IO, metadata : ParquetMetaData) : ParquetMetaDataWriter
          new(metadata, Parquet::File::TrackedWrite.new(buf))
        end

        def self.new_with_tracked(buf : Parquet::File::TrackedWrite, metadata : ParquetMetaData) : ParquetMetaDataWriter
          new(metadata, buf)
        end

        # Sets whether to write the `path_in_schema` field of `ColumnMetaData`.
        def with_write_path_in_schema(val : Bool) : self
          @write_path_in_schema = val
          self
        end

        # Sets the file encryptor used to encrypt the footer / column metadata.
        def with_file_encryptor(file_encryptor : ::Parquet::Encryption::FileEncryptor?) : self
          @file_encryptor = file_encryptor
          self
        end

        # Writes the metadata to the buffer.
        def finish : Nil
          file_metadata = @metadata.file_metadata
          schema_descr = file_metadata.schema_descr
          row_groups = @metadata.row_groups.dup

          encoder = ThriftMetadataWriter.new(
            @tracked, schema_descr, row_groups, file_metadata.created_by,
            file_metadata.version, @write_path_in_schema
          )
          encoder = encoder.with_file_encryptor(@file_encryptor)

          if page_index = @metadata.page_index
            if page_index.is_a?(PageIndex)
              if column_indexes = page_index.column_indexes_raw
                encoder = encoder.with_column_indexes(column_indexes)
              end
              if offset_indexes = page_index.offset_indexes_raw
                encoder = encoder.with_offset_indexes(offset_indexes)
              end
            end
          end

          if key_value_metadata = file_metadata.key_value_metadata
            encoder = encoder.with_key_value_metadata(key_value_metadata)
          end

          encoder.finish
        end
      end
    end
  end
end
