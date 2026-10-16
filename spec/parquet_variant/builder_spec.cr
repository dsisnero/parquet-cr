require "../spec_helper"

# Ported from `parquet-variant/src/builder.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Adaptations: `drop(builder)` (deterministic rollback) is the explicit
# `builder.abandon`, since Crystal has no `Drop`; `VariantBuilder`/`ListBuilder`/
# `ObjectBuilder` `into` conversions use the `VariantValue` union; `Result`
# errors are raised (`expect_raises`).
# `test_variant_all_subtypes_debug` (exact Rust `Debug` formatting) lives in
# `variant_spec.cr` and is deferred.

private alias VB = ParquetVariant::VariantBuilder
private alias V = ParquetVariant::Variant
private alias MD = ParquetVariant::VariantMetadata
private alias Err = ParquetVariant::InvalidArgumentError

private def roundtrip(input : ParquetVariant::VariantValue, expected : V) : Nil
  builder = VB.new
  builder.append_value(input)
  metadata, value = builder.finish
  variant = V.try_new(metadata, value)
  variant.should eq(expected)
end

describe ParquetVariant::VariantBuilder do
  # Upstream `test_simple_usage`.
  it "round-trips simple values" do
    roundtrip(nil, V.from_nil)
    roundtrip(true, V.from(true))
    roundtrip(false, V.from(false))
    roundtrip(42i8, V.from(42i8))
    roundtrip(1234i16, V.from(1234i16))
    roundtrip(123456i32, V.from(123456i32))
    roundtrip(123456789i64, V.from(123456789i64))
    roundtrip(1.5f32, V.from(1.5f32))
    roundtrip(2.5f64, V.from(2.5f64))
    roundtrip("hello", V.from("hello"))

    long_string = "This is a very long string that exceeds the short string limit of 63 bytes and should be encoded as a regular string type instead of a short string"
    roundtrip(long_string, V.from(long_string))

    binary_data = "binary data".to_slice
    roundtrip(binary_data, V.from(binary_data))
  end

  # Upstream `test_try_finish_empty_builder_errors`.
  it "errors when finishing an empty builder" do
    expect_raises(Err, /empty/) { VB.new.try_finish }
  end

  # Upstream `test_finish_empty_builder_panics`.
  it "raises when finishing an empty builder" do
    expect_raises(Err, /empty/) { VB.new.finish }
  end

  # Upstream `test_try_append_value_after_value_errors`.
  it "errors when appending a second value" do
    builder = VB.new
    builder.append_value(1i32)
    expect_raises(Err, /only one is allowed/) { builder.try_append_value(2i32) }
  end

  # Upstream `test_try_append_value_bytes_after_value_errors`.
  it "errors when appending value bytes after a value" do
    builder = VB.new
    builder.append_value(1i32)
    expect_raises(Err, /only one is allowed/) { builder.append_value_bytes(2i32) }
  end

  # Upstream `test_try_new_list_after_value_errors`.
  it "errors when creating a list after a value" do
    builder = VB.new
    builder.append_value(1i32)
    expect_raises(Err, /only one is allowed/) { builder.new_list }
  end

  # Upstream `test_try_new_object_after_value_errors`.
  it "errors when creating an object after a value" do
    builder = VB.new
    builder.append_value(1i32)
    expect_raises(Err, /only one is allowed/) { builder.new_object }
  end

  # Upstream `test_append_value_after_value_panics`.
  it "raises when appending a second value (infallible)" do
    builder = VB.new
    builder.append_value(1i32)
    expect_raises(Err, /only one is allowed/) { builder.append_value(2i32) }
  end

  # Upstream `test_new_list_after_value_panics`.
  it "raises when creating a list after a value (infallible)" do
    builder = VB.new
    builder.append_value(1i32)
    expect_raises(Err, /only one is allowed/) { builder.new_list }
  end

  # Upstream `test_nested_object_with_lists`.
  it "builds nested objects with lists" do
    builder = VB.new
    outer_object_builder = builder.new_object
    inner_object_builder = outer_object_builder.new_object("door 1")
    inner_object_builder.new_list("items").with_value("apple").with_value(false).finish
    inner_object_builder.finish
    outer_object_builder.finish

    metadata, value = builder.finish
    variant = V.try_new(metadata, value)
    outer_object = variant.as_object.not_nil!

    outer_object.len.should eq(1)
    outer_object.field_name(0).should eq("door 1")

    inner_object_variant = outer_object.field(0).not_nil!
    inner_object = inner_object_variant.as_object.not_nil!
    inner_object.len.should eq(1)
    inner_object.field_name(0).should eq("items")

    items_variant = inner_object.field(0).not_nil!
    items_list = items_variant.as_list.not_nil!
    items_list.len.should eq(2)
    items_list.get(0).not_nil!.should eq(V.from("apple"))
    items_list.get(1).not_nil!.should eq(V.from(false))
  end

  # Upstream `test_sorted_dictionary`.
  it "tracks sorted dictionaries" do
    variant1 = VB.new.with_field_names(["b", "c", "d"])
    variant2 = VB.new
    variant2.add_field_name("b")
    variant2.add_field_name("c")
    variant2.add_field_name("d")

    variant1.metadata_builder.num_field_names.should eq(variant2.metadata_builder.num_field_names)
    3.times do |i|
      variant1.metadata_builder.field_name(i).should eq(variant2.metadata_builder.field_name(i))
    end

    variant1.metadata_builder.sorted?.should be_true
    variant2.metadata_builder.sorted?.should be_true

    variant2.add_field_name("a")
    variant2.metadata_builder.sorted?.should be_false

    expect_raises(Err, /empty/) { variant2.try_finish }

    variant1.append_value(false)
    m, v = variant1.finish
    V.try_new(m, v).should be_a(V::BooleanFalse)
    MD.try_new(m).sorted?.should be_true
  end

  # Upstream `test_object_sorted_dictionary`.
  it "builds sorted object dictionaries" do
    variant1 = VB.new.with_field_names(["a", "b", "c"])
    obj = variant1.new_object
    obj.insert("c", true)
    obj.insert("a", false)
    obj.insert("b", nil)

    field_ids = obj.fields.map { |(id, _)| id }
    field_ids.should eq([2_u32, 0_u32, 1_u32])

    obj.insert("d", 2)
    obj.finish

    metadata, value = variant1.finish
    variant = V.try_new(metadata, value)
    MD.try_new(metadata).sorted?.should be_true

    object = variant.as_object.not_nil!
    object.iter.map { |(name, _)| name }.should eq(["a", "b", "c", "d"])
  end

  # Upstream `test_object_not_sorted_dictionary`.
  it "builds unsorted object dictionaries" do
    variant1 = VB.new.with_field_names(["b", "c", "d"])
    obj = variant1.new_object
    obj.insert("c", true)
    obj.insert("d", false)
    obj.insert("b", nil)

    field_ids = obj.fields.map { |(id, _)| id }
    field_ids.should eq([1_u32, 2_u32, 0_u32])

    obj.insert("a", 2)
    obj.finish

    metadata, value = variant1.finish
    variant = V.try_new(metadata, value)
    MD.try_new(metadata).sorted?.should be_false

    object = variant.as_object.not_nil!
    object.iter.map { |(name, _)| name }.should eq(["a", "b", "c", "d"])
  end

  # Upstream `test_building_sorted_dictionary`.
  it "reports dictionary sort state while building" do
    builder = VB.new
    builder.metadata_builder.sorted?.should be_false
    builder.metadata_builder.num_field_names.should eq(0)

    builder.add_field_name("a")
    builder.metadata_builder.sorted?.should be_true
    builder.metadata_builder.num_field_names.should eq(1)

    builder = builder.with_field_names(["b", "c", "d"])
    builder.metadata_builder.sorted?.should be_true
    builder.metadata_builder.num_field_names.should eq(4)

    builder = builder.with_field_names(["z", "y"])
    builder.metadata_builder.sorted?.should be_false
    builder.metadata_builder.num_field_names.should eq(6)
  end

  # Upstream `test_variant_builder_to_list_builder_no_finish`.
  it "rolls back an unfinished top-level list" do
    builder = VB.new
    list_builder = builder.new_list
    list_builder.append_value("hi")
    list_builder.abandon

    builder.append_value(42i8)
    metadata, value = builder.finish
    metadata_obj = MD.try_new(metadata)
    metadata_obj.empty?.should be_true
    V.try_new_with_metadata(metadata_obj, value).should eq(V.from(42i8))
  end

  # Upstream `test_variant_builder_to_object_builder_no_finish`.
  it "rolls back an unfinished top-level object" do
    builder = VB.new
    object_builder = builder.new_object
    object_builder.insert("name", "unknown")
    object_builder.abandon

    builder.append_value(42i8)
    metadata, value = builder.finish
    metadata_obj = MD.try_new(metadata)
    metadata_obj.empty?.should be_true
    V.try_new_with_metadata(metadata_obj, value).should eq(V.from(42i8))
  end

  # Upstream `test_list_builder_to_list_builder_inner_no_finish`.
  it "rolls back an unfinished inner list" do
    builder = VB.new
    list_builder = builder.new_list
    list_builder.append_value(1i8)

    nested = list_builder.new_list
    nested.append_value("hi")
    nested.abandon

    list_builder.append_value(2i8)
    list_builder.finish

    metadata, value = builder.finish
    MD.try_new(metadata).empty?.should be_true
    variant = V.try_new_with_metadata(MD.try_new(metadata), value)
    list = variant.as_list.not_nil!
    list.len.should eq(2)
    list.get(0).not_nil!.should eq(V.from(1i8))
    list.get(1).not_nil!.should eq(V.from(2i8))
  end

  # Upstream `test_list_builder_to_list_builder_outer_no_finish`.
  it "rolls back an unfinished outer list" do
    builder = VB.new
    list_builder = builder.new_list
    list_builder.append_value(1i8)

    nested = list_builder.new_list
    nested.append_value("hi")
    nested.finish

    list_builder.abandon

    builder.append_value(2i8)
    metadata, value = builder.finish
    MD.try_new(metadata).empty?.should be_true
    V.try_new_with_metadata(MD.try_new(metadata), value).should eq(V.from(2i8))
  end

  # Upstream `test_list_builder_to_object_builder_inner_no_finish`.
  it "rolls back an unfinished inner object inside a list" do
    builder = VB.new
    list_builder = builder.new_list
    list_builder.append_value(1i8)

    nested = list_builder.new_object
    nested.insert("name", "unknown")
    nested.abandon

    list_builder.append_value(2i8)
    list_builder.finish

    metadata, value = builder.finish
    MD.try_new(metadata).empty?.should be_true
    variant = V.try_new_with_metadata(MD.try_new(metadata), value)
    list = variant.as_list.not_nil!
    list.len.should eq(2)
    list.get(0).not_nil!.should eq(V.from(1i8))
    list.get(1).not_nil!.should eq(V.from(2i8))
  end

  # Upstream `test_list_builder_to_object_builder_outer_no_finish`.
  it "rolls back an unfinished outer list containing a finished object" do
    builder = VB.new
    list_builder = builder.new_list
    list_builder.append_value(1i8)

    nested = list_builder.new_object
    nested.insert("name", "unknown")
    nested.finish

    list_builder.abandon

    builder.append_value(2i8)
    metadata, value = builder.finish
    MD.try_new(metadata).empty?.should be_true
    V.try_new_with_metadata(MD.try_new(metadata), value).should eq(V.from(2i8))
  end

  # Upstream `test_object_builder_to_list_builder_inner_no_finish`.
  it "rolls back an unfinished inner list inside an object" do
    builder = VB.new
    object_builder = builder.new_object
    object_builder.insert("first", 1i8)

    nested = object_builder.new_list("nested")
    nested.append_value("hi")
    nested.abandon

    object_builder.insert("second", 2i8)
    object_builder.finish

    metadata, value = builder.finish
    metadata_obj = MD.try_new(metadata)
    metadata_obj.len.should eq(2)
    metadata_obj[0].should eq("first")
    metadata_obj[1].should eq("second")

    variant = V.try_new_with_metadata(metadata_obj, value)
    obj = variant.as_object.not_nil!
    obj.len.should eq(2)
    obj.get("first").not_nil!.should eq(V.from(1i8))
    obj.get("second").not_nil!.should eq(V.from(2i8))
  end

  # Upstream `test_object_builder_to_list_builder_outer_no_finish`.
  it "rolls back an unfinished outer object containing a finished list" do
    builder = VB.new
    object_builder = builder.new_object
    object_builder.insert("first", 1i8)

    nested = object_builder.new_list("nested")
    nested.append_value("hi")
    nested.finish

    object_builder.abandon

    builder.append_value(2i8)
    metadata, value = builder.finish
    MD.try_new(metadata).empty?.should be_true
    V.try_new_with_metadata(MD.try_new(metadata), value).should eq(V.from(2i8))
  end

  # Upstream `test_object_builder_to_object_builder_inner_no_finish`.
  it "rolls back an unfinished inner object" do
    builder = VB.new
    object_builder = builder.new_object
    object_builder.insert("first", 1i8)

    nested = object_builder.new_object("nested")
    nested.insert("name", "unknown")
    nested.abandon

    object_builder.insert("second", 2i8)
    object_builder.finish

    metadata, value = builder.finish
    metadata_obj = MD.try_new(metadata)
    metadata_obj.len.should eq(2)
    metadata_obj[0].should eq("first")
    metadata_obj[1].should eq("second")

    variant = V.try_new_with_metadata(metadata_obj, value)
    obj = variant.as_object.not_nil!
    obj.len.should eq(2)
    obj.get("first").not_nil!.should eq(V.from(1i8))
    obj.get("second").not_nil!.should eq(V.from(2i8))
  end

  # Upstream `test_object_builder_to_object_builder_outer_no_finish`.
  it "rolls back an unfinished outer object containing a finished object" do
    builder = VB.new
    object_builder = builder.new_object
    object_builder.insert("first", 1i8)

    nested = object_builder.new_object("nested")
    nested.insert("name", "unknown")
    nested.finish

    object_builder.abandon

    builder.append_value(2i8)
    metadata, value = builder.finish
    MD.try_new(metadata).len.should eq(0)
    V.try_new_with_metadata(MD.try_new(metadata), value).should eq(V.from(2i8))
  end

  # Upstream `test_append_list_object_list_object`.
  it "builds deeply nested list/object structures with skips" do
    counter = 0
    take = ->(n : Int32) do
      result = [] of Int32
      n.times do
        result << counter
        counter += 1
      end
      result
    end

    builder = VB.new
    skip = 5
    list = builder.new_list
    take.call(4).each do |i|
      object = list.new_object
      take.call(4).each do |j|
        field_name = "field#{j}"
        field_list = object.new_list(field_name)
        take.call(3).each do |k|
          inner_object = field_list.new_object
          take.call(3).each do |m|
            inner_object.insert("field#{m}", m) if m % skip != 0
          end
          inner_object.finish if k % skip != 0
        end
        field_list.finish if j % skip != 0
      end
      object.finish if i % skip != 0
    end
    list.finish

    metadata, value = builder.finish
    v1 = V.try_new(metadata, value)

    builder2 = VB.new
    builder2.append_value(v1)
    metadata2, value2 = builder2.finish
    v2 = V.try_new(metadata2, value2)

    v1.to_s.should eq(v2.to_s)
  end

  # Upstream `test_append_variant_bytes_round_trip`.
  it "copies raw variant bytes" do
    builder = VB.new
    obj = builder.new_object
    obj.insert("name", "Alice")
    obj.insert("age", 30i32)
    scores = obj.new_list("scores")
    scores.append_value(95i32)
    scores.append_value(87i32)
    scores.append_value(92i32)
    scores.finish
    address = obj.new_object("address")
    address.insert("street", "123 Main St")
    address.insert("city", "Anytown")
    address.finish
    obj.finish

    metadata, value1 = builder.finish
    variant1 = V.try_new(metadata, value1)

    md = MD.new(metadata)
    metadata_builder = ParquetVariant::ReadOnlyMetadataBuilder.new(md)
    builder2 = ParquetVariant::ValueBuilder.new
    state = ParquetVariant::ParentState.variant(builder2, metadata_builder)
    ParquetVariant::ValueBuilder.append_variant_bytes(state, variant1)
    value2 = builder2.into_inner

    value1.should eq(value2)
  end

  # Upstream `test_object_insert_bytes_subset`.
  it "inserts a subset of object fields via raw bytes" do
    builder = VB.new.with_field_names(["new_field", "another_field"])
    obj = builder.new_object
    obj.insert("field1", "value1")
    obj.insert("field2", 42i32)
    obj.insert("field3", true)
    obj.insert("field4", "value4")
    obj.finish

    metadata1, value1 = builder.finish
    original_variant = V.try_new(metadata1, value1)
    original_obj = original_variant.as_object.not_nil!

    metadata2 = MD.new(metadata1)
    metadata_builder = ParquetVariant::ReadOnlyMetadataBuilder.new(metadata2)
    builder2 = ParquetVariant::ValueBuilder.new
    state = ParquetVariant::ParentState.variant(builder2, metadata_builder)
    obj2 = ParquetVariant::ObjectBuilder.new(state, true)

    obj2.insert_bytes("field1", original_obj.get("field1").not_nil!)
    obj2.insert("new_field", "new_value")
    obj2.insert_bytes("field3", original_obj.get("field3").not_nil!)
    obj2.insert("another_field", 99i32)
    obj2.insert_bytes("field2", original_obj.get("field2").not_nil!)
    obj2.finish

    value2 = builder2.into_inner
    result_variant = V.try_new(metadata1, value2)
    result_obj = result_variant.as_object.not_nil!

    result_obj.len.should eq(5)
    result_obj.get("field1").not_nil!.as_string.should eq("value1")
    result_obj.get("field2").not_nil!.as_int32.should eq(42)
    result_obj.get("field3").not_nil!.as_boolean.should be_true
    result_obj.get("new_field").not_nil!.as_string.should eq("new_value")
    result_obj.get("another_field").not_nil!.as_int32.should eq(99)
  end

  # Upstream `test_complex_nested_filtering_injection`.
  it "filters and injects into a nested structure via raw bytes" do
    builder = VB.new.with_field_names(
      ["active_count", "active_users", "computed_score", "processed_at", "status"]
    )

    root_obj = builder.new_object
    root_obj.insert("metadata", "original")

    users_list = root_obj.new_list("users")
    user1 = users_list.new_object
    user1.insert("id", 1i32)
    user1.insert("name", "Alice")
    user1.insert("active", true)
    user1.finish
    user2 = users_list.new_object
    user2.insert("id", 2i32)
    user2.insert("name", "Bob")
    user2.insert("active", false)
    user2.finish
    user3 = users_list.new_object
    user3.insert("id", 3i32)
    user3.insert("name", "Charlie")
    user3.insert("active", true)
    user3.finish
    users_list.finish

    root_obj.insert("total_count", 3i32)
    root_obj.finish

    metadata1, value1 = builder.finish
    original_variant = V.try_new(metadata1, value1)
    original_obj = original_variant.as_object.not_nil!
    original_users = original_obj.get("users").not_nil!.as_list.not_nil!

    metadata2 = MD.new(metadata1)
    metadata_builder = ParquetVariant::ReadOnlyMetadataBuilder.new(metadata2)
    builder2 = ParquetVariant::ValueBuilder.new
    state = ParquetVariant::ParentState.variant(builder2, metadata_builder)
    root_obj2 = ParquetVariant::ObjectBuilder.new(state, true)

    root_obj2.insert_bytes("metadata", original_obj.get("metadata").not_nil!)
    root_obj2.insert("processed_at", "2024-01-01T00:00:00Z")

    filtered_users = root_obj2.new_list("active_users")
    original_users.len.times do |i|
      user = original_users.get(i).not_nil!.as_object.not_nil!
      if user.get("active").not_nil!.as_boolean
        new_user = filtered_users.new_object
        new_user.insert_bytes("id", user.get("id").not_nil!)
        new_user.insert_bytes("name", user.get("name").not_nil!)
        user_id = user.get("id").not_nil!.as_int32.not_nil!
        new_user.insert("computed_score", user_id * 10)
        new_user.insert("status", "verified")
        new_user.finish
      end
    end
    system_user = filtered_users.new_object
    system_user.insert("id", 999i32)
    system_user.insert("name", "System User")
    system_user.insert("computed_score", 0i32)
    system_user.insert("status", "system")
    system_user.finish
    filtered_users.finish

    root_obj2.insert("active_count", 3i32)
    root_obj2.finish

    value2 = builder2.into_inner
    result_variant = V.try_new(metadata1, value2)
    result_obj = result_variant.as_object.not_nil!

    result_obj.get("metadata").not_nil!.as_string.should eq("original")
    result_obj.get("processed_at").not_nil!.as_string.should eq("2024-01-01T00:00:00Z")
    result_obj.get("active_count").not_nil!.as_int32.should eq(3)

    active_users = result_obj.get("active_users").not_nil!.as_list.not_nil!
    active_users.len.should eq(3)

    alice = active_users.get(0).not_nil!.as_object.not_nil!
    alice.get("id").not_nil!.as_int32.should eq(1)
    alice.get("name").not_nil!.as_string.should eq("Alice")
    alice.get("computed_score").not_nil!.as_int32.should eq(10)
    alice.get("status").not_nil!.as_string.should eq("verified")
    alice.get("active").should be_nil

    charlie = active_users.get(1).not_nil!.as_object.not_nil!
    charlie.get("id").not_nil!.as_int32.should eq(3)
    charlie.get("name").not_nil!.as_string.should eq("Charlie")
    charlie.get("computed_score").not_nil!.as_int32.should eq(30)
    charlie.get("status").not_nil!.as_string.should eq("verified")

    sys = active_users.get(2).not_nil!.as_object.not_nil!
    sys.get("id").not_nil!.as_int32.should eq(999)
    sys.get("name").not_nil!.as_string.should eq("System User")
    sys.get("computed_score").not_nil!.as_int32.should eq(0)
    sys.get("status").not_nil!.as_string.should eq("system")
  end
end
