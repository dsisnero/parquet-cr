require "../spec_helper"

# Ported from `parquet-variant/src/builder/list.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: `Into<Variant>` uses the `VariantValue` union; `Variant::List`
# pattern matching uses `as_list`; `Result` errors are raised.

private alias VB = ParquetVariant::VariantBuilder
private alias LB = ParquetVariant::ListBuilder
private alias VB2 = ParquetVariant::ValueBuilder
private alias PS = ParquetVariant::ParentState
private alias V = ParquetVariant::Variant
private alias MD = ParquetVariant::VariantMetadata
private alias ROMB = ParquetVariant::ReadOnlyMetadataBuilder

describe ParquetVariant::ListBuilder do
  # Upstream `test_list`.
  it "builds a list" do
    builder = VB.new
    builder
      .new_list
      .with_value(1i8)
      .with_value(2i8)
      .with_value("test")
      .finish

    metadata, value = builder.finish
    metadata.empty?.should be_false
    value.empty?.should be_false

    variant = V.try_new(metadata, value)
    list = variant.as_list.not_nil!
    list.get(0).not_nil!.should eq(V.from(1i8))
    list.get(1).not_nil!.should eq(V.from(2i8))
    list.get(2).not_nil!.should eq(V.from("test"))
  end

  # Upstream `test_nested_list`.
  it "builds a nested list" do
    builder = VB.new
    outer = builder.new_list
    outer
      .new_list
      .with_value("a")
      .with_value("b")
      .with_value("c")
      .with_value("d")
      .finish
    outer.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    outer_list = variant.as_list.not_nil!
    outer_list.len.should eq(1)

    inner_variant = outer_list.get(0).not_nil!
    inner_list = inner_variant.as_list.not_nil!
    inner_list.iter.should eq([V.from("a"), V.from("b"), V.from("c"), V.from("d")])
  end

  # Upstream `test_super_nested_list`.
  it "builds a deeply nested list" do
    builder = VB.new
    list1 = builder.new_list
    list2 = list1.new_list
    list3 = list2.new_list
    list4 = list3.new_list
    list5 = list4.new_list
    list5.append_value(1)
    list5.finish
    list4.finish
    list3.finish
    list2.finish
    list1.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    l1 = variant.as_list.not_nil!
    l1.len.should eq(1)
    l2 = l1.get(0).not_nil!.as_list.not_nil!
    l2.len.should eq(1)
    l3 = l2.get(0).not_nil!.as_list.not_nil!
    l3.len.should eq(1)
    l4 = l3.get(0).not_nil!.as_list.not_nil!
    l4.len.should eq(1)
    l5 = l4.get(0).not_nil!.as_list.not_nil!
    l5.len.should eq(1)
    l5.get(0).not_nil!.should eq(V.from(1))
  end

  # Upstream `test_list_append_bytes_subset`.
  it "appends a subset of list elements via raw bytes" do
    builder = VB.new
    list = builder.new_list
    list.append_value("item1")
    list.append_value(42i32)
    list.append_value(true)
    list.append_value("item4")
    list.append_value(1.234f64)
    list.finish

    metadata1, value1 = builder.finish
    original_variant = V.try_new(metadata1, value1)
    original_list = original_variant.as_list.not_nil!

    metadata2 = MD.new(metadata1)
    metadata_builder = ROMB.new(metadata2)
    builder2 = VB2.new
    state = PS.variant(builder2, metadata_builder)
    list2 = LB.new(state, true)

    list2.append_value_bytes(original_list.get(0).not_nil!)
    list2.append_value("new_item")
    list2.append_value_bytes(original_list.get(2).not_nil!)
    list2.append_value(99i32)
    list2.append_value_bytes(original_list.get(4).not_nil!)
    list2.finish

    value2 = builder2.into_inner
    result_variant = V.try_new(metadata1, value2)
    result_list = result_variant.as_list.not_nil!

    result_list.len.should eq(5)
    result_list.get(0).not_nil!.as_string.should eq("item1")
    result_list.get(1).not_nil!.as_string.should eq("new_item")
    result_list.get(2).not_nil!.as_boolean.should be_true
    result_list.get(3).not_nil!.as_int32.should eq(99)
    result_list.get(4).not_nil!.as_f64.should eq(1.234)
  end

  # Upstream `test_append_list`.
  it "appends a list" do
    m1, v1 = make_list
    variant = V.new(m1, v1)
    builder = VB.new
    builder.append_value(variant)
    metadata, value = builder.finish
    variant.should eq(V.new(metadata, value))
  end

  # Upstream `test_append_nested_list`.
  it "appends a nested list" do
    m1, v1 = make_nested_list
    variant = V.new(m1, v1)
    builder = VB.new
    builder.append_value(variant)
    metadata, value = builder.finish
    variant.should eq(V.new(metadata, value))
  end

  # Upstream `test_object_list`.
  it "builds a list of objects" do
    builder = VB.new
    list_builder = builder.new_list
    list_builder.new_object.with_field("id", 1).with_field("type", "Cauliflower").finish
    list_builder.new_object.with_field("id", 2).with_field("type", "Beets").finish
    list_builder.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    list = variant.as_list.not_nil!
    list.len.should eq(2)

    obj1 = list.get(0).not_nil!.as_object.not_nil!
    obj1.iter.should eq([{"id", V.from(1)}, {"type", V.from("Cauliflower")}])

    obj2 = list.get(1).not_nil!.as_object.not_nil!
    obj2.iter.should eq([{"id", V.from(2)}, {"type", V.from("Beets")}])
  end

  # Upstream `test_object_list2`.
  it "builds a list of single-field objects" do
    builder = VB.new
    list_builder = builder.new_list
    list_builder.new_object.with_field("a", 1).finish
    list_builder.new_object.with_field("b", 2).finish
    list_builder.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    list = variant.as_list.not_nil!
    list.len.should eq(2)

    obj1 = list.get(0).not_nil!.as_object.not_nil!
    obj1.iter.should eq([{"a", V.from(1)}])
    obj2 = list.get(1).not_nil!.as_object.not_nil!
    obj2.iter.should eq([{"b", V.from(2)}])
  end

  # Upstream `test_hetergenous_list`.
  it "builds a heterogeneous list" do
    builder = VB.new
    list_builder = builder.new_list
    list_builder.append_value(1)

    object_builder = list_builder.new_object
    object_builder.insert("a", 1)
    object_builder.finish

    list_builder.append_value(2)

    object_builder = list_builder.new_object
    object_builder.insert("b", 2)
    object_builder.finish

    list_builder.append_value(3)
    list_builder.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    list = variant.as_list.not_nil!
    list.len.should eq(5)
    list.get(0).not_nil!.should eq(V.from(1))

    obj1 = list.get(1).not_nil!.as_object.not_nil!
    obj1.iter.should eq([{"a", V.from(1)}])

    list.get(2).not_nil!.should eq(V.from(2))

    obj2 = list.get(3).not_nil!.as_object.not_nil!
    obj2.iter.should eq([{"b", V.from(2)}])

    list.get(4).not_nil!.should eq(V.from(3))
  end

  # Upstream `test_nested_list_with_heterogeneous_fields_for_buffer_reuse`.
  it "reuses the parent buffer for nested heterogeneous lists" do
    builder = VB.new
    outer = builder.new_list
    outer.append_value("apple")
    outer.append_value(false)

    inner_list_builder = outer.new_list
    inner_obj = inner_list_builder.new_object
    inner_obj.insert("a", "b")
    inner_obj.insert("b", "c")
    inner_obj.finish
    inner_obj = inner_list_builder.new_object
    inner_obj.insert("c", "d")
    inner_obj.insert("d", "e")
    inner_obj.finish
    inner_list_builder.finish

    inner_list_builder = outer.new_list
    dbl = inner_list_builder.new_list
    dbl.append_value(1)
    dbl.append_value(true)
    dbl.finish
    dbl = inner_list_builder.new_list
    dbl.append_value("tree")
    dbl.append_value(false)
    dbl.finish
    inner_list_builder.finish

    outer.append_value(1)
    outer.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    outer_list = variant.as_list.not_nil!
    outer_list.len.should eq(5)

    outer_list.get(0).not_nil!.should eq(V.from("apple"))
    outer_list.get(1).not_nil!.should eq(V.from(false))
    outer_list.get(4).not_nil!.should eq(V.from(1))

    list1 = outer_list.get(2).not_nil!.as_list.not_nil!
    list1.len.should eq(2)
    list1_obj1 = list1.get(0).not_nil!.as_object.not_nil!
    list1_obj1.field_name(0).should eq("a")
    list1_obj1.field(0).not_nil!.should eq(V.from("b"))
    list1_obj1.field_name(1).should eq("b")
    list1_obj1.field(1).not_nil!.should eq(V.from("c"))

    list2 = outer_list.get(3).not_nil!.as_list.not_nil!
    list2.len.should eq(2)
    list2_list1 = list2.get(0).not_nil!.as_list.not_nil!
    list2_list1.len.should eq(2)
    list2_list1.get(0).not_nil!.should eq(V.from(1))
    list2_list1.get(1).not_nil!.should eq(V.from(true))
    list2_list2 = list2.get(1).not_nil!.as_list.not_nil!
    list2_list2.len.should eq(2)
    list2_list2.get(0).not_nil!.should eq(V.from("tree"))
    list2_list2.get(1).not_nil!.should eq(V.from(false))
  end
end

private def make_list : Tuple(Bytes, Bytes)
  builder = VB.new
  builder
    .new_list
    .with_value(1234)
    .with_value("a string value")
    .finish
  builder.finish
end

private def make_nested_list : Tuple(Bytes, Bytes)
  builder = VB.new
  list = builder.new_list
  list.new_list
    .with_value("the dog licked the oil")
    .with_value(4.3)
    .finish
  list.finish
  builder.finish
end
