require "../src/parquet-cr"
require "option_parser"

# `parquet-show-bloom-filter` entry point: checks values against a column's
# bloom filter. Ported from `parquet/src/bin/parquet-show-bloom-filter.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

file_name = ""
column = ""
values = [] of String

OptionParser.parse do |parser|
  parser.banner = "Usage: parquet-show-bloom-filter --file-name FILE --column COLUMN --values VALUE [--values VALUE ...]"
  parser.on("--file-name FILE", "Path to the parquet file") { |v| file_name = v }
  parser.on("--column COLUMN", "Dot-separated column path to check") { |v| column = v }
  parser.on("--values VALUE", "Value to check (repeatable)") { |v| values << v }
  parser.on("-h", "--help", "Print help") { puts parser; exit 0 }
  parser.invalid_option { |flag| STDERR.puts "Invalid option: #{flag}"; exit 1 }
end

if file_name.empty? || column.empty? || values.empty?
  STDERR.puts "Usage: parquet-show-bloom-filter --file-name FILE --column COLUMN --values VALUE [--values VALUE ...]"
  exit 1
end

begin
  Parquet::Bin::ParquetShowBloomFilter.run(STDOUT, file_name, column, values)
rescue ex
  STDERR.puts ex.message
  exit 1
end
