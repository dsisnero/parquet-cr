require "../src/parquet"

path = ARGV[0]? || File.join(__DIR__, "../vendor/arrow-rs/parquet-testing/data/alltypes_plain.parquet")
File.open(path) do |file|
  reader = Parquet::Arrow::AsyncSeekableReader.new(file)
  Parquet::Examples::ReadWithRowGroup.run(reader)
end
