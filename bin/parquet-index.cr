require "../src/parquet-cr"
require "option_parser"

# `parquet-index` entry point: prints the page index of a Parquet column chunk.
# Ported from `parquet/src/bin/parquet-index.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b). Divergence: `clap` is replaced by
# Crystal's `OptionParser`.

positional = [] of String
show_help = false

OptionParser.parse do |parser|
  parser.banner = "Usage: parquet-index <file> <column>"
  parser.on("-h", "--help", "Print help") { show_help = true }
  parser.unknown_args do |before, after|
    positional = before + after
  end
end

if show_help
  puts "Usage: parquet-index <file> <column>"
  exit 0
end

if positional.size != 2
  STDERR.puts "Usage: parquet-index <file> <column>"
  exit 1
end

begin
  Parquet::Bin::ParquetIndex.run(STDOUT, positional[0], positional[1])
rescue ex
  STDERR.puts ex.message
  exit 1
end
