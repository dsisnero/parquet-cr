require "../src/parquet-cr"
require "option_parser"

# `parquet-schema` entry point: prints the schema and metadata of a Parquet
# file. Ported from `parquet/src/bin/parquet-schema.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

file_name = ""
verbose = false

OptionParser.parse do |parser|
  parser.banner = "Usage: parquet-schema [options] <file>"
  parser.on("-v", "--verbose", "Print full file metadata") { verbose = true }
  parser.on("-h", "--help", "Print help") { puts parser; exit 0 }
  parser.unknown_args { |before, after| file_name = (before + after).first? || "" }
end

if file_name.empty?
  STDERR.puts "Usage: parquet-schema [options] <file>"
  exit 1
end

Parquet::Bin::ParquetSchema.run(STDOUT, file_name, verbose)
