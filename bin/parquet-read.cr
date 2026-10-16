require "../src/parquet-cr"
require "option_parser"

# `parquet-read` entry point: reads records from a Parquet file (or stdin with
# `-`). Ported from `parquet/src/bin/parquet-read.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

file_name = ""
num_records = 0
json = false

OptionParser.parse do |parser|
  parser.banner = "Usage: parquet-read [options] <file>"
  parser.on("-n N", "--num-records N", "Number of records to read (0 = all)") { |v| num_records = v.to_i }
  parser.on("-j", "--json", "Print records as JSON lines") { json = true }
  parser.on("-h", "--help", "Print help") { puts parser; exit 0 }
  parser.unknown_args { |before, after| file_name = (before + after).first? || "" }
end

if file_name.empty?
  STDERR.puts "Usage: parquet-read [options] <file>"
  exit 1
end

if file_name == "-"
  Parquet::Bin::ParquetRead.run_stdin(STDOUT, STDIN, num_records, json)
else
  Parquet::Bin::ParquetRead.run(STDOUT, file_name, num_records, json)
end
