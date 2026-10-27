# Shared fixtures for arrow-rs 60.0.0 async-reader parity tests.
module AsyncReaderSpecs
  alias A = Parquet::Arrow
  alias M = Parquet::File::Metadata

  def self.fixture(rows = 12, bloom = false) : Bytes
    columns = [
      Arrow::PrimitiveArray(Int32).from_values((0...rows).to_a).as(Arrow::ArrayBase),
      Arrow::StringArray.from_nullable((0...rows).map { |index| index % 3 == 0 ? nil : "name_#{index}" }).as(Arrow::ArrayBase),
    ]
    schema = Arrow::Schema.new(Arrow::Fields.from([Arrow::Field.new("id", Arrow::DataType.int32, false), Arrow::Field.new("name", Arrow::DataType.utf8, true)]), Arrow::Metadata.from({"user" => "metadata"}))
    properties = Parquet::File::Properties::WriterProperties.builder.set_max_row_group_row_count(4)
      .set_write_batch_size(1).set_data_page_row_count_limit(2).set_dictionary_enabled(false).set_bloom_filter_enabled(bloom).build
    output = IO::Memory.new
    writer = A::ArrowWriter.try_new(output, schema, properties)
    writer.write(Arrow::RecordBatch.try_new(schema, columns)) unless rows == 0
    writer.close
    output.to_slice.dup
  end

  def self.file(bytes : Bytes) : Parquet::File::SerializedFileReader
    Parquet::File::SerializedFileReader.new(Parquet::File::BytesChunkReader.new(bytes))
  end

  def self.ids(batches : Array(Arrow::RecordBatch)) : Array(Int32?)
    batches.flat_map { |batch| batch.column(0).as(Arrow::PrimitiveArray(Int32)).to_a }
  end

  # Mirrors upstream TestReader: cached metadata, recording data range requests.
  class TestReader < A::AsyncFileReader
    getter requests = [] of Range(UInt64, UInt64)
    getter metadata_calls = 0
    property failure : Exception? = nil
    property? short_read = false

    def initialize(@bytes : Bytes)
    end

    def get_bytes(range : Range(UInt64, UInt64)) : Bytes
      @requests << range
      Fiber.yield
      if error = @failure
        raise error
      end
      bytes = Parquet::File::BytesChunkReader.new(@bytes).get_bytes(range.begin, (range.end - range.begin).to_i32)
      @short_read && !bytes.empty? ? bytes[0, bytes.size - 1] : bytes
    end

    def get_metadata(options : A::ArrowReaderOptions? = nil) : M::ParquetMetaData
      @metadata_calls += 1
      M::ParquetMetaDataReader.new.with_arrow_reader_options(options).parse_and_finish(Parquet::File::BytesChunkReader.new(@bytes))
    end
  end
end
