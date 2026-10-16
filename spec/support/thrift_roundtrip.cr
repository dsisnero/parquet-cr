# Shared helper mirroring `parquet_thrift::tests::test_roundtrip`
# (apache/arrow-rs 60.0.0).
module ThriftRoundTrip
  # Serialize `value` with the compact protocol and read it back.
  def self.roundtrip(value : T) : T forall T
    read(encode(value), T)
  end

  # Serialize `value` with the compact protocol.
  def self.encode(value : T) : Bytes forall T
    io = IO::Memory.new
    writer = Parquet::Thrift::ThriftCompactOutputProtocol.new(io)
    Parquet::Thrift.write_thrift_value(writer, value)
    io.to_slice.dup
  end

  # Read a value of type `T` from compact-protocol `bytes`.
  def self.read(bytes : Bytes, type : T.class) : T forall T
    Parquet::Thrift.read_value(Parquet::Thrift::ThriftSliceInputProtocol.new(bytes), T)
  end
end
