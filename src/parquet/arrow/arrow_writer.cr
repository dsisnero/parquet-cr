require "./schema"
require "../file/writer"
require "./arrow_writer/column"

# parquet/src/arrow/arrow_writer/mod.rs, arrow-rs 60.0.0 (ef1fa157...).
module Parquet::Arrow
  # Arrow-specific options; builders follow this port's mutable convention.
  class ArrowWriterOptions
    getter properties = File::Properties::WriterProperties.builder.build
    getter? skip_arrow_metadata = false
    getter schema_root : String?
    getter schema_descr : Schema::SchemaDescriptor?
    getter page_store_factory : Column::PageStoreFactory?

    def with_properties(@properties : File::Properties::WriterProperties) : self
      self
    end

    def with_skip_arrow_metadata(@skip_arrow_metadata : Bool) : self
      self
    end

    def with_schema_root(@schema_root : String) : self
      self
    end

    def with_parquet_schema(@schema_descr : Schema::SchemaDescriptor) : self
      self
    end

    def with_page_store_factory(@page_store_factory : Column::PageStoreFactory) : self
      self
    end
  end

  # Creates independent column encoders for a row group, including page encryption.
  class ArrowRowGroupWriterFactory
    @store_factory : Column::PageStoreFactory = Column::InMemoryPageStoreFactory.new

    def initialize(@file : File::SerializedFileWriter, @arrow_schema : ::Arrow::Schema)
    end

    def with_page_store_factory(factory : Column::PageStoreFactory) : self
      @store_factory = factory
      self
    end

    def create_column_writers(row_group_index : Int32) : Array(ArrowColumnWriter)
      @file.schema_descr.columns.map_with_index do |descriptor, index|
        store = @store_factory.create(Column::PageStoreArgs.new(index, descriptor))
        encryptor = Column::PageEncryptor.create_if_column_encrypted(@file.file_encryptor, row_group_index, index, descriptor.path.string)
        ArrowColumnWriter.new(descriptor, @file.properties, ArrowPageWriter.new(store, encryptor))
      end
    end
  end

  # Encodes Arrow record batches without retaining their unencoded arrays.
  class ArrowWriter
    getter arrow_schema : ::Arrow::Schema
    @writer : File::SerializedFileWriter
    @factory : ArrowRowGroupWriterFactory
    @columns : Array(ArrowColumnWriter)?
    @buffered_rows = 0
    @finished = false
    @chunkers : Array(Column::Chunker::ContentDefinedChunker)?

    def self.try_new(sink : IO, schema : ::Arrow::Schema, properties : File::Properties::WriterProperties? = nil) : ArrowWriter
      try_new_with_options(sink, schema, ArrowWriterOptions.new.with_properties(properties || File::Properties::WriterProperties.builder.build))
    end

    def self.try_new_with_options(sink : IO, schema : ::Arrow::Schema, options : ArrowWriterOptions) : ArrowWriter
      new(sink, schema, options)
    end

    def initialize(sink : IO, @arrow_schema, options : ArrowWriterOptions = ArrowWriterOptions.new)
      @properties = options.properties
      converter = ArrowSchemaConverter.new.with_coerce_types(@properties.coerce_types)
      converter.schema_root(options.schema_root || "arrow_schema")
      descriptor = options.schema_descr || converter.convert(@arrow_schema)
      Parquet::Arrow.add_encoded_arrow_schema_to_metadata(@arrow_schema, @properties) unless options.skip_arrow_metadata?
      @writer = File::SerializedFileWriter.new(sink, descriptor.root_schema_ptr, @properties)
      @factory = ArrowRowGroupWriterFactory.new(@writer, @arrow_schema)
      if factory = options.page_store_factory
        @factory.with_page_store_factory(factory)
      end
      @chunkers = @properties.content_defined_chunking.try do |config|
        descriptor.columns.map { |column| Column::Chunker::ContentDefinedChunker.new(column, config) }
      end
    end

    @properties : File::Properties::WriterProperties

    def write(batch : ::Arrow::RecordBatch) : Nil
      return if batch.num_rows == 0
      raise GeneralError.new("SerializedFileWriter already finished") if @finished
      remaining = batch
      loop do
        columns = @columns ||= @factory.create_column_writers(flushed_row_groups.size)
        count = remaining.num_rows
        if max_rows = @properties.max_row_group_row_count
          count = Math.min(count, max_rows - @buffered_rows)
        end
        if (max_bytes = @properties.max_row_group_bytes) && @buffered_rows > 0
          bytes = in_progress_size
          if bytes >= max_bytes
            flush
            next
          end
          average = bytes // @buffered_rows
          if average > 0
            fits = ((max_bytes - bytes) // average).to_i32
            if fits == 0
              flush
              next
            end
            count = Math.min(count, fits)
          end
        end
        part = remaining.slice(0, count)
        write_part(part, columns)
        @buffered_rows += count
        if @properties.max_row_group_row_count.try { |limit| @buffered_rows >= limit } || @properties.max_row_group_bytes.try { |limit| in_progress_size >= limit }
          flush
        end
        return if count == remaining.num_rows
        remaining = remaining.slice(count, remaining.num_rows - count)
      end
    end

    private def write_part(batch : ::Arrow::RecordBatch, columns : Array(ArrowColumnWriter)) : Nil
      unless batch.num_columns == @arrow_schema.fields.size
        raise ArrowError.new("Incompatible number of columns")
      end
      ordinal = 0
      @arrow_schema.fields.each_with_index do |field, index|
        Parquet::Arrow.compute_leaves(field, batch.column(index)).each do |leaf|
          column = columns[ordinal]? || raise ArrowError.new("Incompatible leaf column count")
          if chunkers = @chunkers
            chunks = Parquet::Arrow.arrow_chunks(chunkers[ordinal], leaf)
            chunks.each_with_index do |chunk, chunk_index|
              column.write(leaf.slice_for_chunk(chunk))
              column.add_data_page if chunk_index + 1 < chunks.size
            end
          else
            column.write(leaf)
          end
          ordinal += 1
        end
      end
      raise ArrowError.new("Incompatible leaf column count") unless ordinal == columns.size
    end

    def flush : Nil
      return unless columns = @columns
      @columns = nil
      @buffered_rows = 0
      row_group = @writer.next_row_group
      columns.each(&.close.append_to_row_group(row_group))
      row_group.close
      nil
    end

    def in_progress_rows : Int32
      @buffered_rows
    end

    def in_progress_size : Int64
      @columns.try(&.sum(0_i64, &.estimated_total_bytes)) || 0_i64
    end

    def memory_size : Int64
      @columns.try(&.sum(0_i64, &.memory_size)) || 0_i64
    end

    def bytes_written : Int64
      @writer.bytes_written
    end

    def flushed_row_groups : Array(File::Metadata::RowGroupMetaData)
      @writer.flushed_row_groups
    end

    def append_key_value_metadata(value : File::Metadata::KeyValue) : Nil
      @writer.append_key_value_metadata(value)
    end

    def write_all(bytes : Bytes) : Nil
      @writer.write_all(bytes)
    end

    def sync : Nil
      @writer.flush
    end

    def inner : IO
      @writer.inner
    end

    def inner_mut : IO
      @writer.inner_mut
    end

    def into_inner : IO
      flush
      @finished = true
      @writer.into_inner
    end

    def into_serialized_writer : Tuple(File::SerializedFileWriter, ArrowRowGroupWriterFactory)
      flush
      @finished = true
      {@writer, @factory}
    end

    def finish : File::Metadata::ParquetMetaData
      flush
      @finished = true
      @writer.finish
    end

    def close : File::Metadata::ParquetMetaData
      finish
    end
  end

  # CDC hashes the Arrow representation (dictionary keys and little-endian
  # native scalar bytes), preserving chunker state across row groups.
  def self.arrow_chunks(chunker : Column::Chunker::ContentDefinedChunker, leaf : ArrayLevels) : Array(Column::Chunker::CdcChunk)
    definitions = Column::Writer::LevelDataRef.from(leaf.def_levels)
    repetitions = Column::Writer::LevelDataRef.from(leaf.rep_levels)
    count = Math.max(definitions.len, repetitions.len)
    count = leaf.array.len if count == 0
    chunker.calculate(definitions, repetitions, count) do |state, index|
      state.roll(arrow_value_bytes(leaf.array, index))
    end
  end

  def self.arrow_value_bytes(array : ::Arrow::ArrayBase, index : Int32) : Bytes
    case array
    when ::Arrow::DictionaryArray(Int8), ::Arrow::DictionaryArray(Int16), ::Arrow::DictionaryArray(Int32), ::Arrow::DictionaryArray(Int64),
         ::Arrow::DictionaryArray(UInt8), ::Arrow::DictionaryArray(UInt16), ::Arrow::DictionaryArray(UInt32), ::Arrow::DictionaryArray(UInt64)
      arrow_value_bytes(array.keys, index)
    when ::Arrow::NullArray
      Bytes.empty
    when ::Arrow::BooleanArray
      Bytes[array.value(index) ? 1_u8 : 0_u8]
    when ::Arrow::PrimitiveArray(::Arrow::I256)
      array.value(index).to_le_bytes
    when ::Arrow::PrimitiveArray(::Arrow::Float16)
      bytes = Bytes.new(2)
      IO::ByteFormat::LittleEndian.encode(array.value(index).bits, bytes)
      bytes
    when ::Arrow::PrimitiveArray(Int8), ::Arrow::PrimitiveArray(Int16), ::Arrow::PrimitiveArray(Int32), ::Arrow::PrimitiveArray(Int64),
         ::Arrow::PrimitiveArray(UInt8), ::Arrow::PrimitiveArray(UInt16), ::Arrow::PrimitiveArray(UInt32), ::Arrow::PrimitiveArray(UInt64),
         ::Arrow::PrimitiveArray(Int128), ::Arrow::PrimitiveArray(Float32), ::Arrow::PrimitiveArray(Float64)
      scalar_bytes(array, index)
    else
      WriterValues.bytes(array, index)
    end
  end

  private def self.scalar_bytes(array : ::Arrow::PrimitiveArray(T), index : Int32) : Bytes forall T
    bytes = Bytes.new(sizeof(T))
    IO::ByteFormat::LittleEndian.encode(array.value(index), bytes)
    bytes
  end
end
