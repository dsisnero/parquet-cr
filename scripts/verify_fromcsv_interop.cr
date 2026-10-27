require "../src/parquet-cr"
require "file_utils"

# Reproducible comparison with the pinned arrow-rs importer and record reader:
# crystal run scripts/verify_fromcsv_interop.cr -- CRYSTAL_FROMCSV RUST_FROMCSV RUST_READ DIR
private def run_csv_tool(binary : String, args : Array(String), output : IO = STDOUT) : Nil
  status = Process.run(binary, args, output: output, error: STDERR)
  raise "#{binary} failed with #{status}" unless status.success?
end

private def encode_csv_input(bytes : Bytes, codec : String) : Bytes
  output = IO::Memory.new
  case codec
  when "UNCOMPRESSED" then bytes
  when "SNAPPY"
    Compress::Snappy::Writer.open(output, &.write(bytes))
    output.to_slice.dup
  when "GZIP"
    Compress::Gzip::Writer.open(output, &.write(bytes))
    output.to_slice.dup
  when "BROTLI" then Brotli.encode(bytes, quality: 2)
  when "LZ4"    then Parquet::Lz4.compress_frame(bytes)
  when "ZSTD"   then Zinc.compress(bytes, level: 1)
  else               raise "Unexpected codec: #{codec}"
  end
end

private def compare_csv_imports(crystal_binary : String, rust_binary : String, reader : String, directory : String, name : String, input : Bytes, schema : String, options : Array(String)) : Nil
  input_path = File.join(directory, "#{name}.csv")
  schema_path = File.join(directory, "#{name}.schema")
  File.write(input_path, input)
  File.write(schema_path, schema)
  records = [] of String
  {"crystal" => crystal_binary, "rust" => rust_binary}.each do |language, binary|
    output_path = File.join(directory, "#{name}-#{language}.parquet")
    run_csv_tool(binary, ["-s", schema_path, "-i", input_path, "-o", output_path] + options)
    json = IO::Memory.new
    run_csv_tool(reader, ["--json", output_path], json)
    File.write(File.join(directory, "#{name}-#{language}.json"), json.to_s)
    records << json.to_s
  end
  raise "Record mismatch for #{name}; compare files in #{directory}" unless records[0] == records[1]
end

raise "Expected CRYSTAL_FROMCSV RUST_FROMCSV RUST_READ DIR" unless ARGV.size == 4
crystal_binary, rust_binary, reader, directory = ARGV
FileUtils.mkdir_p(directory)
schema = "message csv_interop { required int32 id; optional binary name (STRING); optional boolean active; optional double score; optional int32 day (DATE); optional int64 amount (DECIMAL(12,2)); }"
# Exceeds the Snappy frame chunk size; includes CSV quotes, nulls and typed data.
csv = String.build do |text|
  text << "id,name,active,score,day,amount\r\n"
  (1...2000).each do |index|
    text << "#{index},\"name,#{index}\"\"quoted\",true,1.25,1970-01-02,12.34\r\n"
  end
  text << "2000,,,,,\r\n"
end
codecs = %w[UNCOMPRESSED SNAPPY GZIP BROTLI LZ4 ZSTD]
codecs.each do |codec|
  %w[1 2].each do |version|
    compare_csv_imports(crystal_binary, rust_binary, reader, directory, "#{codec}-v#{version}", encode_csv_input(csv.to_slice, codec), schema,
      ["-h", "-C", codec, "-c", codec, "-b", "37", "-m", "103", "-w", version, "--enable-bloom-filter", "true"])
  end
end
simple_schema = "message dialect { optional int32 id; optional binary name (STRING); }"
compare_csv_imports(crystal_binary, rust_binary, reader, directory, "tsv", "1\t\"a\tb\"\n2\t\"a\"\"b\"\n".to_slice, simple_schema, ["-f", "tsv", "-D", "false"])
compare_csv_imports(crystal_binary, rust_binary, reader, directory, "custom", "1;'a!'b'\r2;'c;d'\r".to_slice, simple_schema, ["-d", ";", "-e", "!", "-q", "'", "-r", "cr"])
%w[GZIP ZSTD LZ4].each do |codec|
  frames = IO::Memory.new
  frames.write(encode_csv_input("1,one\n".to_slice, codec))
  frames.write(encode_csv_input("2,two\n".to_slice, codec))
  compare_csv_imports(crystal_binary, rust_binary, reader, directory, "concatenated-#{codec}", frames.to_slice, simple_schema, ["-C", codec])
end
%w[UNCOMPRESSED SNAPPY LZ4].each do |codec|
  compare_csv_imports(crystal_binary, rust_binary, reader, directory, "empty-#{codec}", Bytes.empty, simple_schema, ["-C", codec])
end
empty_input = File.join(directory, "empty-invalid.csv")
File.write(empty_input, Bytes.empty)
%w[GZIP BROTLI ZSTD].each do |codec|
  {"crystal" => crystal_binary, "rust" => rust_binary}.each do |language, binary|
    errors = IO::Memory.new
    status = Process.run(binary, ["-s", File.join(directory, "empty-LZ4.schema"), "-i", empty_input,
                                  "-o", File.join(directory, "empty-#{codec}-#{language}.parquet"), "-C", codec], output: STDOUT, error: errors)
    File.write(File.join(directory, "empty-#{codec}-#{language}.error"), errors.to_s)
    unless status.exit_code == 1 && errors.to_s.includes?("Failed to read RecordBatch from CSV")
      raise "Unexpected empty #{codec} behavior for #{language}: #{status}, #{errors}"
    end
  end
end
puts "CSV interoperability passed: 6 input/output codecs, V1/V2, typed/null values, dialects, concatenated and empty streams; files in #{directory}"
