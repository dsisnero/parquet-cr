require "../spec_helper"

# Ported from `parquet-variant/src/builder/object.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: `drop(builder)` is the explicit `builder.abandon`; `IndexMap`
# field iteration is exposed as `ObjectBuilder#fields` (`{id, offset}` tuples);
# `Result` errors are raised.

private alias VB = ParquetVariant::VariantBuilder
private alias OB = ParquetVariant::ObjectBuilder
private alias VB2 = ParquetVariant::ValueBuilder
private alias PS = ParquetVariant::ParentState
private alias V = ParquetVariant::Variant
private alias MD = ParquetVariant::VariantMetadata
private alias WMB = ParquetVariant::WritableMetadataBuilder
private alias ROMB = ParquetVariant::ReadOnlyMetadataBuilder
private alias Err = ParquetVariant::InvalidArgumentError

private DUPLICATE_ENTRY_METADATA = Bytes[
  0b0000_0001, # header: offset_size_minus_one=0, sorted=0, version=1
  2,           # dictionary_size
  0x00,
  0x01,
  0x02,
  'a'.ord.to_u8,
  'a'.ord.to_u8,
]

describe ParquetVariant::ObjectBuilder do
  # Upstream `test_object`.
  it "builds an object" do
    builder = VB.new
    builder
      .new_object
      .with_field("name", "John")
      .with_field("age", 42i8)
      .finish

    metadata, value = builder.finish
    metadata.empty?.should be_false
    value.empty?.should be_false
  end

  # Upstream `test_object_field_ordering`.
  it "orders object fields lexicographically" do
    builder = VB.new
    builder
      .new_object
      .with_field("zebra", "stripes")
      .with_field("apple", "red")
      .with_field("banana", "yellow")
      .finish

    _, value = builder.finish
    (value[0] & 0x03).should eq(ParquetVariant::VariantBasicType::Object.value)
    value[1].should eq(3)

    # apple(1), banana(2), zebra(0)
    field_ids = value[2, 3].to_a
    field_ids.should eq([1_u8, 2_u8, 0_u8])
  end

  # Upstream `test_duplicate_fields_in_object`.
  it "keeps the last value for duplicate fields" do
    builder = VB.new
    builder
      .new_object
      .with_field("name", "Ron Artest")
      .with_field("name", "Metta World Peace")
      .finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    obj = variant.as_object.not_nil!
    obj.len.should eq(1)
    obj.field(0).not_nil!.should eq(V.from("Metta World Peace"))
    obj.iter.should eq([{"name", V.from("Metta World Peace")}])
  end

  # Upstream `test_read_only_metadata_builder`.
  it "builds with a read-only metadata builder" do
    default_builder = WMB.new
    default_builder.upsert_field_name("name")
    default_builder.upsert_field_name("age")
    default_builder.upsert_field_name("active")
    default_builder.finish
    metadata_bytes = default_builder.into_inner

    metadata = MD.try_new(metadata_bytes)
    metadata_builder = ROMB.new(metadata)
    value_builder = VB2.new

    state = PS.variant(value_builder, metadata_builder)
    obj = OB.new(state, false)
    obj.insert("name", "Alice")
    obj.insert("age", 30i8)
    obj.insert("active", true)
    obj.finish

    value = value_builder.into_inner
    variant = V.try_new(metadata_bytes, value)
    obj = variant.as_object.not_nil!
    obj.get("name").should eq(V.from("Alice"))
    obj.get("age").should eq(V.from(30i8))
    obj.get("active").should eq(V.from(true))
  end

  # Upstream `test_read_only_metadata_builder_duplicate_dictionary_entries_are_detected`.
  it "detects duplicate dictionary entries" do
    metadata = MD.try_new(DUPLICATE_ENTRY_METADATA)
    metadata.sorted?.should be_false

    metadata_builder = ROMB.new(metadata)
    value_builder = VB2.new
    state = PS.variant(value_builder, metadata_builder)
    obj = OB.new(state, true)

    obj.insert(metadata.get(0), 1i8)
    err = expect_raises(Err) { obj.try_insert(metadata.get(1), 2i8) }
    err.message.not_nil!.should contain("Duplicate field name")
  end

  # Upstream `test_read_only_metadata_builder_duplicate_dictionary_entries_build_valid_object`.
  it "builds a valid object from duplicate dictionary entries" do
    metadata = MD.try_new(DUPLICATE_ENTRY_METADATA)

    metadata_builder = ROMB.new(metadata)
    value_builder = VB2.new
    state = PS.variant(value_builder, metadata_builder)
    obj = OB.new(state, false)

    obj.insert(metadata.get(1), 1i8)
    obj.insert("a", 2i8)
    obj.finish

    value = value_builder.into_inner
    variant = V.try_new(DUPLICATE_ENTRY_METADATA, value)
    obj = variant.as_object.not_nil!
    obj.len.should eq(1)
    obj.get("a").should eq(V.from(2i8))
  end

  # Upstream `test_append_object`.
  it "appends an object" do
    m1, v1 = make_object
    variant = V.new(m1, v1)

    builder = VB.new.with_metadata(MD.new(m1))
    builder.append_value(variant)
    metadata, value = builder.finish
    variant.should eq(V.new(metadata, value))
  end

  # Upstream `test_append_nested_object`.
  it "appends a nested object" do
    m1, v1 = make_nested_object
    variant = V.new(m1, v1)

    builder = VB.new.with_metadata(MD.new(m1))
    builder.append_value(variant)
    metadata, value = builder.finish
    variant.should eq(V.new(metadata, value))
  end

  # Upstream `test_nested_object`.
  it "builds a nested object" do
    builder = VB.new
    outer = builder.new_object
    inner = outer.new_object("c")
    inner.insert("b", "a")
    inner.finish
    outer.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    outer_object = variant.as_object.not_nil!
    outer_object.len.should eq(1)
    outer_object.field_name(0).should eq("c")

    inner_variant = outer_object.field(0).not_nil!
    inner_object = inner_variant.as_object.not_nil!
    inner_object.len.should eq(1)
    inner_object.field_name(0).should eq("b")
    inner_object.field(0).not_nil!.should eq(V.from("a"))
  end

  # Upstream `test_nested_object_with_duplicate_field_names_per_object`.
  it "allows duplicate field names across objects" do
    builder = VB.new
    outer = builder.new_object
    inner = outer.new_object("c")
    inner.insert("b", false)
    inner.insert("c", "a")
    inner.finish
    outer.insert("b", false)
    outer.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    outer_object = variant.as_object.not_nil!
    outer_object.len.should eq(2)
    outer_object.field_name(0).should eq("b")

    inner_variant = outer_object.field(1).not_nil!
    inner_object = inner_variant.as_object.not_nil!
    inner_object.len.should eq(2)
    inner_object.field_name(0).should eq("b")
    inner_object.field(0).not_nil!.should eq(V.from(false))
    inner_object.field_name(1).should eq("c")
    inner_object.field(1).not_nil!.should eq(V.from("a"))
  end

  # Upstream `test_nested_object_with_heterogeneous_fields`.
  it "builds a nested object with heterogeneous fields" do
    builder = VB.new
    outer = builder.new_object
    outer.insert("a", false)

    inner = outer.new_object("c")
    inner.insert("b", "a")
    inner_c = inner.new_object("c")
    inner_c.insert("aa", "bb")
    inner_c.finish
    inner_d = inner.new_object("d")
    inner_d.insert("cc", "dd")
    inner_d.finish
    inner.finish

    outer.insert("b", true)

    outer_d = outer.new_object("d")
    outer_d.insert("e", 1)
    f_list = outer_d.new_list("f")
    f_list.append_value(1)
    f_list.append_value(true)
    f_list.finish
    g_list = outer_d.new_list("g")
    g_list.append_value("tree")
    g_list.append_value(false)
    g_list.finish
    outer_d.finish

    outer.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    outer_object = variant.as_object.not_nil!
    outer_object.len.should eq(4)

    outer_object.field_name(0).should eq("a")
    outer_object.field(0).not_nil!.should eq(V.from(false))

    outer_object.field_name(2).should eq("c")
    inner_object = outer_object.field(2).not_nil!.as_object.not_nil!
    inner_object.len.should eq(3)
    inner_object.field_name(0).should eq("b")
    inner_object.field(0).not_nil!.should eq(V.from("a"))

    inner_c2 = inner_object.field(1).not_nil!.as_object.not_nil!
    inner_c2.len.should eq(1)
    inner_c2.field_name(0).should eq("aa")
    inner_c2.field(0).not_nil!.should eq(V.from("bb"))

    inner_d2 = inner_object.field(2).not_nil!.as_object.not_nil!
    inner_d2.len.should eq(1)
    inner_d2.field_name(0).should eq("cc")
    inner_d2.field(0).not_nil!.should eq(V.from("dd"))

    outer_object.field_name(1).should eq("b")
    outer_object.field(1).not_nil!.should eq(V.from(true))

    outer_d2 = outer_object.field(3).not_nil!.as_object.not_nil!
    outer_d2.len.should eq(3)
    outer_d2.field_name(0).should eq("e")
    outer_d2.field(0).not_nil!.should eq(V.from(1))
    outer_d2.field_name(1).should eq("f")

    f = outer_d2.field(1).not_nil!.as_list.not_nil!
    f.len.should eq(2)
    f.get(0).not_nil!.should eq(V.from(1))
    f.get(1).not_nil!.should eq(V.from(true))

    g = outer_d2.field(2).not_nil!.as_list.not_nil!
    g.len.should eq(2)
    g.get(0).not_nil!.should eq(V.from("tree"))
    g.get(1).not_nil!.should eq(V.from(false))
  end

  # Upstream `test_object_without_unique_field_validation`.
  it "keeps the last duplicate field without unique validation" do
    builder = VB.new
    obj = builder.new_object
    obj.insert("a", 1)
    obj.insert("a", 2)
    obj.finish

    builder = VB.new
    outer_list = builder.new_list
    inner_list = outer_list.new_list
    nested_obj = inner_list.new_object
    nested_obj.insert("x", 1)
    nested_obj.insert("x", 2)
    nested_obj.new_list("x").with_value(3).finish
    nested_obj.new_object("x").with_field("y", 4).finish
    nested_obj.finish
    inner_list.finish
    outer_list.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    outer_element = variant.get_list_element(0).not_nil!
    inner_element = outer_element.get_list_element(0).not_nil!
    outer_field = inner_element.get_object_field("x").not_nil!
    inner_field = outer_field.get_object_field("y").not_nil!
    inner_field.should eq(V.from(4))
  end

  # Upstream `test_object_with_unique_field_validation`.
  it "rejects duplicate fields with unique validation" do
    builder = VB.new.with_validate_unique_fields(true)

    obj0 = builder.new_object
    obj0.insert("a", 1)
    obj0.insert("b", 2)
    err = expect_raises(Err) { obj0.try_insert("a", 3) }
    err.message.should eq("Invalid argument error: Duplicate field name: a")
    obj0.abandon

    outer_list = builder.new_list
    inner_list = outer_list.new_list
    obj = inner_list.new_object
    obj.insert("x", 1)
    err = expect_raises(Err) { obj.try_insert("x", 2) }
    err.message.should eq("Invalid argument error: Duplicate field name: x")
    err = expect_raises(Err) { obj.new_list("x") }
    err.message.should eq("Invalid argument error: Duplicate field name: x")
    err = expect_raises(Err) { obj.new_object("x") }
    err.message.should eq("Invalid argument error: Duplicate field name: x")

    obj.abandon
    inner_list.finish
    outer_list.finish

    builder2 = VB.new.with_validate_unique_fields(true)
    list = builder2.new_list
    valid_obj = list.new_object
    valid_obj.insert("m", 1)
    valid_obj.insert("n", 2)
    valid_obj.finish
    list.finish
  end
end

# Object with fields inserted in reverse lexicographical order.
private def make_object : Tuple(Bytes, Bytes)
  builder = VB.new
  obj = builder.new_object
  obj.insert("b", true)
  obj.insert("a", false)
  obj.finish
  builder.finish
end

# Nested object with an inner object value.
private def make_nested_object : Tuple(Bytes, Bytes)
  builder = VB.new
  outer = builder.new_object
  inner = outer.new_object("b")
  inner.insert("a", "inner_value")
  inner.finish
  outer.finish
  builder.finish
end
