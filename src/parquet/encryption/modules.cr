require "../errors"

# Ported from `parquet/src/encryption/modules.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the Parquet Modular Encryption AAD (additional authenticated data)
# builders: `ModuleType`, `create_footer_aad`, and `create_module_aad`.
#
# Divergences: `usize` indices are `Int32`; errors raise `Parquet::GeneralError`
# instead of returning `Result`; the upstream `_BloomFilterHeader`/
# `_BloomFilterBitset` (dead-code-marked) variants drop the leading underscore.
module Parquet
  module Encryption
    # The module a piece of encrypted data belongs to (the first AAD byte).
    enum ModuleType
      Footer               = 0
      ColumnMetaData       = 1
      DataPage             = 2
      DictionaryPage       = 3
      DataPageHeader       = 4
      DictionaryPageHeader = 5
      ColumnIndex          = 6
      OffsetIndex          = 7
      BloomFilterHeader    = 8
      BloomFilterBitset    = 9
    end

    # Builds the AAD for the file footer.
    def self.create_footer_aad(file_aad : Bytes) : Bytes
      create_module_aad(file_aad, ModuleType::Footer, 0, 0, nil)
    end

    # Builds the AAD for a module.
    #
    # For the footer only the file AAD plus the module byte are used. For data
    # pages (and dictionary page headers) the row-group, column, and page
    # ordinals are appended as little-endian `i16`; other modules append the
    # row-group and column ordinals only.
    def self.create_module_aad(
      file_aad : Bytes,
      module_type : ModuleType,
      row_group_idx : Int32,
      column_ordinal : Int32,
      page_ordinal : Int32?,
    ) : Bytes
      module_byte = module_type.value.to_u8

      if module_type.footer?
        return to_bytes(file_aad) + Bytes[module_byte]
      end

      if row_group_idx > Int16::MAX
        raise general_err!(
          "Encrypted parquet files can't have more than #{Int16::MAX} row groups: #{row_group_idx}"
        )
      end
      if column_ordinal > Int16::MAX
        raise general_err!(
          "Encrypted parquet files can't have more than #{Int16::MAX} columns: #{column_ordinal}"
        )
      end

      unless module_type.data_page_header? || module_type.data_page?
        return to_bytes(file_aad) +
          Bytes[module_byte] +
          i16_le(row_group_idx) +
          i16_le(column_ordinal)
      end

      page = page_ordinal
      raise general_err!("Page ordinal must be set for data pages") if page.nil?
      if page > Int16::MAX
        raise general_err!(
          "Encrypted parquet files can't have more than #{Int16::MAX} pages per column chunk: #{page}"
        )
      end

      to_bytes(file_aad) +
        Bytes[module_byte] +
        i16_le(row_group_idx) +
        i16_le(column_ordinal) +
        i16_le(page)
    end

    private def self.i16_le(value : Int32) : Bytes
      io = IO::Memory.new(2)
      io.write_bytes(value.to_i16, IO::ByteFormat::LittleEndian)
      io.to_slice.dup
    end

    private def self.to_bytes(file_aad : Bytes) : Bytes
      Bytes.new(file_aad.size) { |i| file_aad[i] }
    end
  end
end
