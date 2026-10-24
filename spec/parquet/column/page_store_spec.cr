require "../../spec_helper"

# Ported from parquet/src/column/page_store.rs tests (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
private alias PageKey = Parquet::Column::PageKey
private alias InMemoryPageStore = Parquet::Column::InMemoryPageStore

describe "column page store" do
  it "round trips blobs in handle order" do
    store = InMemoryPageStore.new
    first = store.put("hello".to_slice)
    second = store.put("world".to_slice)
    first.should_not eq second
    store.take(first).should eq "hello".to_slice
    store.take(second).should eq "world".to_slice
  end

  it "releases the slot on take" do
    store = InMemoryPageStore.new
    key = store.put("abc".to_slice)
    store.take(key).should eq "abc".to_slice
    store.take(key).should be_empty
  end

  it "errors on an invalid key" do
    store = InMemoryPageStore.new
    error = expect_raises(Parquet::GeneralError) { store.take(PageKey.new(99_u64)) }
    error.message.should eq "Parquet error: invalid page key 99"
  end

  it "reports resident bytes" do
    store = InMemoryPageStore.new
    store.memory_size.should eq 0
    first = store.put("hello".to_slice)
    second = store.put("!".to_slice)
    store.memory_size.should eq 6
    store.take(first)
    store.memory_size.should eq 1
    store.take(second)
    store.memory_size.should eq 0
  end
end

# Upstream's OffHeap stub checks the PageStore default without implementing a
# spilling backend. Writer integration belongs to the Arrow writer feature.
private class OffHeapPageStore < Parquet::Column::PageStore
  def put(value : Bytes) : PageKey
    PageKey.new(0_u64)
  end

  def take(key : PageKey) : Bytes
    Bytes.empty
  end
end

private class RecordingPageStoreFactory < Parquet::Column::PageStoreFactory
  getter seen : Parquet::Column::PageStoreArgs?

  def create(args : Parquet::Column::PageStoreArgs) : Parquet::Column::PageStore
    @seen = args
    OffHeapPageStore.new
  end
end

describe "page store extension contracts" do
  # Upstream `default_store_memory_size_is_zero`.
  it "defaults to zero resident bytes for an off-heap store" do
    store = OffHeapPageStore.new
    store.memory_size.should eq 0
    key = store.put("off heap".to_slice)
    key.get.should eq 0_u64
    store.take(key).should be_empty
    store.memory_size.should eq 0
  end

  # Characterization of PageKey's upstream new/get and derived value traits.
  it "preserves UInt64 keys, ordering, equality and hashing" do
    key = PageKey.new(UInt64::MAX)
    copy = key
    copy.get.should eq UInt64::MAX
    copy.should eq PageKey.new(UInt64::MAX)
    copy.hash.should eq PageKey.new(UInt64::MAX).hash
    keys = [key, PageKey.new(0_u64), PageKey.new(1_u64)]
    keys.sort.map(&.get).should eq [0_u64, 1_u64, UInt64::MAX]
    {PageKey.new(42_u64) => "blob"}[PageKey.new(42_u64)].should eq "blob"
  end

  it "passes the leaf column context to a custom factory" do
    type = Parquet::Schema::Type.primitive_type_builder("payload", Parquet::Type::BYTE_ARRAY)
      .with_repetition(Parquet::Repetition::OPTIONAL).build
    descriptor = Parquet::Schema::ColumnDescriptor.new(type, 1_i16, 0_i16, Parquet::Schema::ColumnPath.new("payload"))
    args = Parquet::Column::PageStoreArgs.new(3, descriptor)
    factory = RecordingPageStoreFactory.new
    store = factory.create(args)
    store.should be_a(OffHeapPageStore)
    seen = factory.seen
    raise "Factory did not receive context" unless seen
    seen.column_index.should eq 3
    seen.column_descriptor.same?(descriptor).should be_true
    seen.column_descriptor.physical_type.should eq Parquet::Type::BYTE_ARRAY
    seen.column_descriptor.max_def_level.should eq 1
    seen.column_descriptor.path.string.should eq "payload"
  end

  it "creates independent empty in-memory stores for each column chunk" do
    type = Parquet::Schema::Type.primitive_type_builder("id", Parquet::Type::INT32).build
    descriptor = Parquet::Schema::ColumnDescriptor.new(type, 0_i16, 0_i16, Parquet::Schema::ColumnPath.new("id"))
    args = Parquet::Column::PageStoreArgs.new(0, descriptor)
    factory = Parquet::Column::InMemoryPageStoreFactory.new
    first = factory.create(args)
    second = factory.create(args)
    first.same?(second).should be_false
    first.memory_size.should eq 0
    second.memory_size.should eq 0
    key = first.put("data".to_slice)
    second.memory_size.should eq 0
    expect_raises(Parquet::GeneralError) { second.take(key) }
    first.take(key).should eq "data".to_slice
  end

  it "supports empty blobs and out-of-order takes without recycling keys" do
    store = InMemoryPageStore.new
    empty = store.put(Bytes.empty)
    first = store.put("abc".to_slice)
    second = store.put("de".to_slice)
    store.take(second).should eq "de".to_slice
    store.memory_size.should eq 3
    store.take(second).should be_empty
    store.memory_size.should eq 3
    store.take(empty).should be_empty
    store.take(first).should eq "abc".to_slice
    store.memory_size.should eq 0
    next_key = store.put("f".to_slice)
    [empty.get, first.get, second.get, next_key.get].should eq [0_u64, 1_u64, 2_u64, 3_u64]
    store.memory_size.should eq 1
    store.take(next_key).should eq "f".to_slice
    store.memory_size.should eq 0
  end

  it "rejects oversized and just-past-the-end keys without altering resident bytes" do
    store = InMemoryPageStore.new
    valid = store.put("abc".to_slice)
    [1_u64, Int32::MAX.to_u64 + 1, UInt64::MAX].each do |raw|
      error = expect_raises(Parquet::GeneralError) { store.take(PageKey.new(raw)) }
      error.message.should eq "Parquet error: invalid page key #{raw}"
      store.memory_size.should eq 3
    end
    store.take(valid).should eq "abc".to_slice
  end
end
