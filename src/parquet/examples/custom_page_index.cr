# Runnable selective-index workflow from parquet/examples/custom_page_index.rs
# (arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b). IO::Memory replaces
# the temporary file; the sample data, writer settings and fetch pattern match.
module Parquet::Examples::CustomPageIndex
  alias MD = Parquet::File::Metadata

  # Create the original six-column sample, with three 50-row groups.
  def self.create_sample_file : Bytes
    names = ["id", "value", "name", "score", "category", "amount"]
    fields = names.map do |name|
      data_type = name == "name" || name == "category" ? ::Arrow::DataType.utf8 : ::Arrow::DataType.int32
      ::Arrow::Field.new(name, data_type, false)
    end
    schema = ::Arrow::Schema.new(::Arrow::Fields.from(fields))
    properties = File::Properties::WriterProperties.builder
      .set_statistics_enabled(File::Properties::EnabledStatistics::Page)
      .set_data_page_size_limit(100).set_write_batch_size(10).build
    output = IO::Memory.new
    writer = Arrow::ArrowWriter.try_new(output, schema, properties)
    3.times do |group|
      5.times { |batch| writer.write(sample_batch(schema, group * 50 + batch * 10)) }
      writer.flush
    end
    writer.close
    output.to_slice.dup
  end

  private def self.sample_batch(schema : ::Arrow::Schema, offset : Int32) : ::Arrow::RecordBatch
    values = (offset...offset + 10).to_a
    arrays = [
      ::Arrow::PrimitiveArray(Int32).from_values(values),
      ::Arrow::PrimitiveArray(Int32).from_values(values.map { |value| value * 2 }),
      ::Arrow::StringArray.from_values(values.map { |value| "name#{value}" }),
      ::Arrow::PrimitiveArray(Int32).from_values(values.map { |value| value * 3 }),
      ::Arrow::StringArray.from_values(values.map { |value| value.even? ? "even" : "odd" }),
      ::Arrow::PrimitiveArray(Int32).from_values(values.map { |value| value * 4 }),
    ] of ::Arrow::ArrayBase
    ::Arrow::RecordBatch.try_new(schema, arrays)
  end

  # Compare all indexes with column statistics for (0,0) and page locations for
  # (0,0), (0,1), (0,4), as in the pinned example.
  def self.run(output : IO = STDOUT) : Nil
    bytes = create_sample_file
    output.puts "Sample file created with page indexes\n"
    output.puts "=== Example 1: Standard PageIndex (all columns) ==="
    full = read_metadata(bytes, MD::PageIndexPolicy::Required)
    output.puts "Number of row groups: #{full.num_row_groups}"
    output.puts "Number of columns: #{full.file_metadata.schema_descr.num_columns}\n"
    output.puts "== All indexes should be populated"
    print_page_index(full, 0, output)
    print_page_index(full, 1, output)

    output.puts "=== Example 2: Selective PageIndex (columns 0, 1, 4 only) ==="
    metadata = read_metadata(bytes, MD::PageIndexPolicy::Skip)
    provider = MD::OnDemandPageIndexProvider.new(metadata.clone, bytes)
    provider.fetch_column_index(0, 0)
    [0, 1, 4].each { |column| provider.fetch_offset_index(0, column) }
    selected = metadata.into_builder.set_page_index(provider).build
    output.puts "== Indexes should be partially populated"
    print_page_index(selected, 0, output)
    output.puts "== Indexes should be unpopulated"
    print_page_index(selected, 1, output)
  end

  private def self.read_metadata(bytes : Bytes, policy : MD::PageIndexPolicy) : MD::ParquetMetaData
    MD::ParquetMetaDataReader.new.with_page_index_policy(policy)
      .parse_and_finish(File::BytesChunkReader.new(bytes))
  end

  def self.print_page_index(metadata : MD::ParquetMetaData, row_group_idx : Int32, output : IO = STDOUT) : Nil
    page_index = metadata.page_index
    unless page_index
      output.puts "No page index in metadata"
      output.puts "Note: This example requires a file with page indexes."
      output.puts "Try using alltypes_tiny_pages.parquet or another file with page indexes."
      raise general_err!("no page index")
    end
    output.puts "\nIndexes for row group #{row_group_idx}:"
    metadata.file_metadata.schema_descr.num_columns.times do |column|
      output.puts "  Column #{column}: has offset idx #{!page_index.offset_index(row_group_idx, column).nil?}, has column idx #{!page_index.column_index(row_group_idx, column).nil?}"
    end
    output.puts
  end
end
