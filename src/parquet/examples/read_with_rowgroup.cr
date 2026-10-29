# Complete parquet/examples/read_with_rowgroup.rs workflow, apache/arrow-rs
# 60.0.0 (ef1fa157977633f0ba21aa921f9ef3d5c669235b). Reuses the native dense
# ChunkReader and single-reader PageIterator rather than duplicating them.
module Parquet::Examples::ReadWithRowGroup
  alias A = Parquet::Arrow
  alias M = Parquet::File::Metadata

  # Explicitly fetched projected chunks for one row group. The native base
  # supplies the RowGroups/ChunkReader adapters used by the array readers.
  class InMemoryRowGroup < A::InMemoryRowGroup
    def initialize(metadata : M::ParquetMetaData, row_group_idx : Int32, @mask : A::ProjectionMask)
      row = metadata.row_group(row_group_idx)
      chunks = Array(A::ColumnChunkData?).new(row.num_columns, nil)
      super(row.num_rows.to_i32, chunks, nil, row_group_idx, metadata)
    end

    def self.create(metadata : M::ParquetMetaData, row_group_idx : Int32, mask : A::ProjectionMask) : self
      new(metadata, row_group_idx, mask)
    end

    def row_group_metadata : M::RowGroupMetaData
      @metadata.row_group(@row_group_idx)
    end

    # Selection intentionally does not reduce IO in the pinned example. Every
    # call refetches full projected chunks, in leaf order, one range at a time.
    def async_fetch_data(reader : A::AsyncFileReader, _selection : A::RowSelection? = nil) : Nil
      chunks = @column_chunks
      @column_chunks = [] of A::ColumnChunkData?
      row_group_metadata.columns.each_with_index do |column, index|
        next unless @mask.leaf_included?(index)

        start, size = column.byte_range
        bytes = reader.get_bytes(start.to_u64...(start + size).to_u64)
        chunks[index] = A::ColumnChunkData::Dense.new(start.to_i32, bytes)
      end
      @column_chunks = chunks
    end

    # Build an independent decoder over fetched bytes. Schema conversion uses
    # no Arrow field hints, matching the example rather than IPC-aware builders.
    def build_reader(batch_size : Int32, selection : A::RowSelection? = nil) : A::ParquetRecordBatchReader
      levels = A.parquet_to_arrow_field_levels(row_group_metadata.schema_descr, @mask, nil)
      reader = A::ArrayReaderBuilder.new(self, batch_size).build_array_reader(levels.levels, @mask)
      plan = A::ReadPlanBuilder.new(batch_size).with_selection(selection).build
      A::ParquetRecordBatchReader.new(reader, plan)
    end

    # Source Clone owns metadata and the slot vector, sharing Arc chunk data.
    def clone : self
      mask = A::ProjectionMask.new(@mask.mask.try(&.dup))
      snapshot = self.class.create(@metadata.clone, @row_group_idx, mask)
      snapshot.replace_chunks(@column_chunks.dup)
      snapshot
    end

    protected def replace_chunks(chunks : Array(A::ColumnChunkData?)) : Nil
      @column_chunks = chunks
    end
  end

  # Read cached metadata once, then explicitly fetch/decode/print each row group.
  # AsyncFileReader methods may suspend the current Crystal fiber.
  def self.run(reader : A::AsyncFileReader, output : IO = STDOUT) : Nil
    metadata = reader.get_metadata
    metadata.num_row_groups.times do |index|
      row_group = InMemoryRowGroup.create(metadata.clone, index, A::ProjectionMask.all)
      row_group.async_fetch_data(reader)
      row_group.build_reader(1024).each do |batch|
        output.puts ::Arrow.pretty_format_batches([batch])
      end
    end
  end
end
