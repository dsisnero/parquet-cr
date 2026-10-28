require "../spec_helper"

# parquet_macros.rs thrift_union{,_with_unknown} and parquet_thrift.rs
# roundtrip contract, arrow-rs 60.0.0 ef1fa157977633f0ba21aa921f9ef3d5c669235b.
# The macros have no dedicated mixed-union test module; cases below characterize
# their pinned dispatch, skip, single-field and serialization behavior.
module MixedUnionSpecs
  define_thrift_struct Payload do
    required 1, count, Int32
    optional 2, enabled, Bool
  end

  define_thrift_union Strict do
    unit 1, Empty, empty
    variant 2, Payload, payload, Payload
    variant 17, Items, items, Array(Int32)
    variant 31, Text, text, String
    variant 32, Binary, binary, Bytes
    variant -2, Number, number, Int64
    variant 33, Flags, flags, Array(Bool)
  end

  define_thrift_union BooleanPayload do
    variant 1, Flag, flag, Bool
  end

  define_thrift_union_with_unknown Forward do
    unit 1, Empty, empty
    variant 2, Payload, payload, Payload
  end

  define_thrift_struct Envelope do
    required 1, choice, Strict
    required 2, choices, Array(Strict)
    required 3, sentinel, Int32
  end

  def self.protocols(bytes : Bytes, &)
    yield Parquet::Thrift::ThriftSliceInputProtocol.new(bytes)
    yield Parquet::Thrift::ThriftReadInputProtocol.new(IO::Memory.new(bytes))
  end
end

private alias UnionSpec = MixedUnionSpecs
private alias UnionT = Parquet::Thrift

describe "Generated mixed Thrift unions" do
  it "generates native unit/payload constructors, Kind and accessors" do
    unit = UnionSpec::Strict::EMPTY
    unit.should eq UnionSpec::Strict.empty
    unit.kind.should eq UnionSpec::Strict::Kind::Empty
    unit.kind.value.should eq 0
    unit.payload.should be_nil
    payload = UnionSpec::Payload.new(42, false)
    value = UnionSpec::Strict.payload(payload)
    value.kind.should eq UnionSpec::Strict::Kind::Payload
    value.kind.value.should eq 1
    value.payload.should eq payload
    value.payload!.should eq payload
    expect_raises(Parquet::GeneralError, "Strict is not PAYLOAD") { unit.payload! }
    UnionSpec::Strict.thrift_element_type.should eq UnionT::ElementType::Struct
  end

  it "matches exact compact bytes for unit and struct payload variants" do
    ThriftRoundTrip.encode(UnionSpec::Strict.empty).should eq Bytes[0x1c, 0, 0]
    value = UnionSpec::Strict.payload(UnionSpec::Payload.new(42, false))
    expected = Bytes[0x2c, 0x15, 84, 0x12, 0, 0]
    ThriftRoundTrip.encode(value).should eq expected
    UnionSpec.protocols(expected) { |prot| UnionSpec::Strict.read_thrift(prot).should eq value }
  end

  it "encodes list payloads and extended/negative field identifiers exactly" do
    value = UnionSpec::Strict.items([1, -2])
    expected = Bytes[0x09, 34, 0x25, 2, 3, 0]
    ThriftRoundTrip.encode(value).should eq expected
    UnionSpec.protocols(expected) { |prot| UnionSpec::Strict.read_thrift(prot).should eq value }
    value = UnionSpec::Strict.number(-123_i64)
    expected = Bytes[0x06, 3, 245, 1, 0]
    ThriftRoundTrip.encode(value).should eq expected
    UnionSpec.protocols(expected) { |prot| UnionSpec::Strict.read_thrift(prot).should eq value }
  end

  it "round trips owned text, arbitrary binary and Boolean list elements" do
    [UnionSpec::Strict.text("snow: ☃"), UnionSpec::Strict.binary(Bytes[0, 255, 128]),
     UnionSpec::Strict.flags([true, false, true]), UnionSpec::Strict.items([] of Int32)].each do |value|
      bytes = ThriftRoundTrip.encode(value)
      UnionSpec.protocols(bytes) { |prot| UnionSpec::Strict.read_thrift(prot).should eq value }
    end
  end

  it "composes generated unions as fields and list elements without cursor drift" do
    value = UnionSpec::Envelope.new(UnionSpec::Strict.empty,
      [UnionSpec::Strict.payload(UnionSpec::Payload.new(2, true)), UnionSpec::Strict.items([3, 4])], 999)
    bytes = ThriftRoundTrip.encode(value)
    UnionSpec.protocols(bytes) { |prot| UnionSpec::Envelope.read_thrift(prot).should eq value }
  end

  it "rejects empty unions in strict and forward modes" do
    UnionSpec.protocols(Bytes[0]) do |prot|
      expect_raises(Parquet::GeneralError, "Received empty union from remote Strict") { UnionSpec::Strict.read_thrift(prot) }
    end
    UnionSpec.protocols(Bytes[0]) do |prot|
      expect_raises(Parquet::GeneralError, "Received empty union from remote Forward") { UnionSpec::Forward.read_thrift(prot) }
    end
  end

  it "rejects unknown strict variants before attempting to consume their payload" do
    UnionSpec.protocols(Bytes[0x3c, 0, 0]) do |prot|
      error = expect_raises(Parquet::GeneralError) { UnionSpec::Strict.read_thrift(prot) }
      error.message.should eq "Parquet error: Unexpected Strict 3"
      prot.read_byte.should eq 0 # Body is untouched by strict failure.
    end
  end

  it "rejects multiple and duplicate fields with pinned diagnostics" do
    [Bytes[0x1c, 0, 0x1c, 0], Bytes[0x1c, 0, 0x0c, 2, 0]].each do |bytes|
      UnionSpec.protocols(bytes) do |prot|
        expect_raises(Parquet::GeneralError, "Received multiple fields for union from remote {}Strict") { UnionSpec::Strict.read_thrift(prot) }
      end
    end
    UnionSpec.protocols(Bytes[0x1c, 0, 0x1c, 0]) do |prot|
      expect_raises(Parquet::GeneralError, "Received multiple fields for union from remote {}Forward") { UnionSpec::Forward.read_thrift(prot) }
    end
  end

  it "requires unit structs to be empty and rejects truncated payloads" do
    UnionSpec.protocols(Bytes[0x1c, 0x15, 2, 0, 0]) do |prot|
      expect_raises(Parquet::GeneralError, "Empty struct has fields") { UnionSpec::Strict.read_thrift(prot) }
    end
    [Bytes[0x2c], Bytes[0x2c, 0x15], Bytes[0x1c, 0]].each do |bytes|
      UnionSpec.protocols(bytes) { |prot| expect_raises(Parquet::EOFError) { UnionSpec::Strict.read_thrift(prot) } }
    end
  end

  it "preserves pinned ID dispatch rather than validating a known field's wire tag" do
    # The macro dispatches by id and its declared payload type, as upstream.
    UnionSpec.protocols(Bytes[0x15, 0, 0]) { |prot| UnionSpec::Strict.read_thrift(prot).should eq UnionSpec::Strict.empty }
  end

  it "skips unknown nested structs, maps/lists and inline Boolean fields exactly once" do
    # Unknown field 7 contains a struct with one list<i32>, then a trailing byte
    # belongs to the caller, not the union. Also exercise map and bool skips.
    [Bytes[0x7c, 0x19, 0x25, 2, 4, 0, 0, 99],
     Bytes[0x7b, 1, 0x85, 1, 120, 2, 0, 99],
     Bytes[0x71, 0, 99]].each do |bytes|
      UnionSpec.protocols(bytes) do |prot|
        value = UnionSpec::Forward.read_thrift(prot)
        value.kind.should eq UnionSpec::Forward::Kind::Unknown
        value.unknown_field_id.should eq 7
        prot.read_byte.should eq 99
        expect_raises(Parquet::GeneralError, "Trying to write unknown variant") { ThriftRoundTrip.encode(value) }
      end
    end
  end

  it "rejects a second field even after skipping an unknown variant" do
    UnionSpec.protocols(Bytes[0x7c, 0, 0x1c, 0, 0]) do |prot|
      expect_raises(Parquet::GeneralError, "Received multiple fields for union from remote {}Forward") { UnionSpec::Forward.read_thrift(prot) }
    end
    UnionSpec.protocols(Bytes[0x7c, 0x15]) do |prot|
      expect_raises(Parquet::EOFError) { UnionSpec::Forward.read_thrift(prot) }
    end
  end

  it "validates owned UTF-8 payloads on both protocol implementations" do
    # Extended field id 31, one-byte invalid UTF-8 body, union stop.
    UnionSpec.protocols(Bytes[0x08, 62, 1, 255, 0]) do |prot|
      expect_raises(Parquet::GeneralError, "invalid utf8") { UnionSpec::Strict.read_thrift(prot) }
    end
  end

  it "preserves the pinned direct-Boolean payload limitation and false accessors" do
    # Rust __thrift_read_variant calls bool::read_thrift, which reads a value
    # byte even though WriteThriftField embeds bool in the header. A struct
    # field or Array(Bool) is the usable representation in the pinned release.
    [true, false].each do |flag|
      value = UnionSpec::BooleanPayload.flag(flag)
      value.flag!.should eq flag
      bytes = Bytes[flag ? 0x11_u8 : 0x12_u8, 0]
      ThriftRoundTrip.encode(value).should eq bytes
      UnionSpec.protocols(bytes) do |prot|
        expect_raises(Parquet::EOFError) { UnionSpec::BooleanPayload.read_thrift(prot) }
      end
    end
  end

  it "round trips known forward-compatible variants without creating unknown state" do
    [UnionSpec::Forward.empty, UnionSpec::Forward.payload(UnionSpec::Payload.new(-7, nil))].each do |value|
      UnionSpec.protocols(ThriftRoundTrip.encode(value)) do |prot|
        decoded = UnionSpec::Forward.read_thrift(prot)
        decoded.should eq value
        decoded.unknown_field_id.should be_nil
      end
    end
  end
end
