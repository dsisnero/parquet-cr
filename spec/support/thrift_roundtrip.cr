# Shared helper mirroring `parquet_thrift::tests::test_roundtrip`
# (apache/arrow-rs 60.0.0).
module ThriftRoundTrip
  # Serialize `value` with the compact protocol and read it back.
  def self.roundtrip(value : T) : T forall T
    io = IO::Memory.new
    writer = Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
    Parquet::Thrift.write_thrift_value(writer, value)
    Parquet::Thrift.read_value(Parquet::Thrift::ThriftSliceInputProtocol.new(io.to_slice), T)
  end
end
