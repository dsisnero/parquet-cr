require "option_parser"
require "../src/parquet"

samples = 3
smoke = false
filter = nil.as(String?)
columns = nil.as(Int32?)
groups = nil.as(Int32?)
rows = nil.as(Int32?)
page_rows = nil.as(Int32?)
parser = OptionParser.new do |options|
  options.banner = "Usage: parquet_round_trip [options]"
  options.on("--smoke", "Use 2 columns, 2 groups, 32 rows/group, 16 rows/page") { smoke = true }
  options.on("--case=TEXT", "Select case names containing TEXT") { |value| filter = value }
  options.on("--samples=N", "Warm samples after the first measured run (default 3)") { |value| samples = value.to_i }
  options.on("--columns=N", "Override columns per case") { |value| columns = value.to_i }
  options.on("--groups=N", "Override row groups") { |value| groups = value.to_i }
  options.on("--rows=N", "Rows/group; above 100, use a multiple of 100") { |value| rows = value.to_i }
  options.on("--page-rows=N", "Data page row limit") { |value| page_rows = value.to_i }
  options.on("-h", "--help", "Show options") { puts options; exit }
end

begin
  parser.parse
  cases = Parquet::Benchmarks::RoundTrip.cases.select do |item|
    query = filter
    query.nil? || item.name.includes?(query)
  end
  raise ArgumentError.new("No benchmark cases match") if cases.empty?
  cases = cases.map do |item|
    spec = smoke ? Parquet::Benchmarks::RoundTrip.smoke_spec(item.spec) : item.spec
    if value = columns
      spec = spec.with_num_columns(value)
    end
    if value = groups
      spec = spec.with_num_row_groups(value)
    end
    if value = rows
      spec = spec.with_rows_per_row_group(value)
    end
    if value = page_rows
      spec = spec.with_rows_per_page(value)
    end
    Parquet::Benchmarks::RoundTrip::Case.new(item.name, spec)
  end
  Parquet::Benchmarks::RoundTrip.run(cases, samples)
rescue error : Exception
  STDERR.puts(error.message)
  exit 1
end
