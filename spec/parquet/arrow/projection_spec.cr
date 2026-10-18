require "../../spec_helper"

# Ported from `parquet/src/arrow/mod.rs` (`ProjectionMask`,
# `test_mask_from_column_names`, `test_projection_mask_union`,
# `test_projection_mask_intersect`, `parquet_column`) and
# `parquet/src/arrow/schema/primitive.rs` (`convert_primitive` + its tests)
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# `ProjectionMask::without_nested_types` and its tests are a follow-up
# (`push_decoder`/selection use it); everything else on the mask is here.
private alias PArrow = Parquet::Arrow
private alias PSchema = Parquet::Schema
private alias PType = Parquet::Type
private alias PLogical = Parquet::LogicalType
private alias PConverted = Parquet::ConvertedType

private def parse_parquet_schema(message_type : String) : PSchema::SchemaDescriptor
  PSchema::SchemaDescriptor.new(PSchema.parse_message_type(message_type))
end

private def leaf_mask(mask : PArrow::ProjectionMask) : Array(Bool)
  mask.mask.not_nil!
end

describe Parquet::Arrow::ProjectionMask do
  it "selects all columns by default" do
    mask = PArrow::ProjectionMask.all
    mask.mask.should be_nil
    0.upto(4) { |leaf| mask.leaf_included?(leaf).should be_true }
  end

  it "selects no columns" do
    mask = PArrow::ProjectionMask.none(3)
    leaf_mask(mask).should eq([false, false, false])
    mask.leaf_included?(0).should be_false
  end

  it "selects leaves by index" do
    schema = parse_parquet_schema("message s { REQUIRED INT32 a; REQUIRED INT32 b; REQUIRED INT32 c; }")

    mask = PArrow::ProjectionMask.leaves(schema, [0, 2])
    leaf_mask(mask).should eq([true, false, true])
  end

  it "selects roots by index" do
    schema = parse_parquet_schema(<<-SCHEMA)
      message s {
        OPTIONAL GROUP a (LIST) { REPEATED GROUP list { OPTIONAL INT32 element; } }
        REQUIRED INT32 b;
      }
      SCHEMA

    mask = PArrow::ProjectionMask.roots(schema, [1])
    leaf_mask(mask).should eq([false, true])
  end

  # Upstream `test_mask_from_column_names`.
  it "builds masks from column names" do
    schema = parse_parquet_schema(<<-SCHEMA)
      message test_schema {
        OPTIONAL group a (MAP) {
          REPEATED group key_value {
            REQUIRED BYTE_ARRAY key (UTF8);
            OPTIONAL group value (MAP) {
              REPEATED group key_value {
                REQUIRED INT32 key;
                REQUIRED BOOLEAN value;
              }
            }
          }
        }
        REQUIRED INT32 b;
        REQUIRED DOUBLE c;
      }
      SCHEMA

    leaf_mask(PArrow::ProjectionMask.columns(schema, ["foo", "bar"])).should eq([false, false, false, false, false])
    leaf_mask(PArrow::ProjectionMask.columns(schema, [] of String)).should eq([false, false, false, false, false])
    leaf_mask(PArrow::ProjectionMask.columns(schema, ["a", "c"])).should eq([true, true, true, false, true])
    leaf_mask(PArrow::ProjectionMask.columns(schema, ["a.key_value.key", "c"])).should eq([true, false, false, false, true])
    leaf_mask(PArrow::ProjectionMask.columns(schema, ["a.key_value.value", "b"])).should eq([false, true, true, true, false])

    schema = parse_parquet_schema(<<-SCHEMA)
      message test_schema {
        OPTIONAL group a (LIST) {
          REPEATED group list {
            OPTIONAL group element (LIST) {
              REPEATED group list {
                OPTIONAL group element (LIST) {
                  REPEATED group list {
                    OPTIONAL BYTE_ARRAY element (UTF8);
                  }
                }
              }
            }
          }
        }
        REQUIRED INT32 b;
      }
      SCHEMA

    leaf_mask(PArrow::ProjectionMask.columns(schema, ["a", "b"])).should eq([true, true])
    leaf_mask(PArrow::ProjectionMask.columns(schema, ["a.list.element", "b"])).should eq([true, true])
    leaf_mask(PArrow::ProjectionMask.columns(schema, ["a.list.element.list.element.list.element", "b"])).should eq([true, true])
    leaf_mask(PArrow::ProjectionMask.columns(schema, ["b"])).should eq([false, true])

    schema = parse_parquet_schema(<<-SCHEMA)
      message test_schema { OPTIONAL INT32 a; OPTIONAL INT32 b; OPTIONAL INT32 c; OPTIONAL INT32 d; OPTIONAL INT32 e; }
      SCHEMA

    leaf_mask(PArrow::ProjectionMask.columns(schema, ["a", "b"])).should eq([true, true, false, false, false])
    leaf_mask(PArrow::ProjectionMask.columns(schema, ["d", "b", "d"])).should eq([false, true, false, true, false])

    schema = parse_parquet_schema(<<-SCHEMA)
      message test_schema { OPTIONAL INT32 a; OPTIONAL INT32 b; OPTIONAL INT32 a; OPTIONAL INT32 d; OPTIONAL INT32 e; }
      SCHEMA

    leaf_mask(PArrow::ProjectionMask.columns(schema, ["a", "e"])).should eq([true, false, true, false, true])

    schema = parse_parquet_schema("message test_schema { OPTIONAL INT32 a; OPTIONAL INT32 aa; }")
    leaf_mask(PArrow::ProjectionMask.columns(schema, ["a"])).should eq([true, false])
  end

  # Upstream `test_projection_mask_union`.
  it "unions projection masks" do
    mask1 = PArrow::ProjectionMask.leaves_with_mask([true, false, true])
    mask2 = PArrow::ProjectionMask.leaves_with_mask([false, true, true])
    mask1.union(mask2)
    mask1.mask.should eq([true, true, true])

    mask1 = PArrow::ProjectionMask.all
    mask1.union(PArrow::ProjectionMask.leaves_with_mask([false, true, true]))
    mask1.mask.should be_nil

    mask1 = PArrow::ProjectionMask.leaves_with_mask([true, false, true])
    mask1.union(PArrow::ProjectionMask.all)
    mask1.mask.should be_nil

    mask1 = PArrow::ProjectionMask.all
    mask1.union(PArrow::ProjectionMask.all)
    mask1.mask.should be_nil
  end

  # Upstream `test_projection_mask_intersect`.
  it "intersects projection masks" do
    mask1 = PArrow::ProjectionMask.leaves_with_mask([true, false, true])
    mask2 = PArrow::ProjectionMask.leaves_with_mask([false, true, true])
    mask1.intersect(mask2)
    mask1.mask.should eq([false, false, true])

    mask1 = PArrow::ProjectionMask.all
    mask1.intersect(PArrow::ProjectionMask.leaves_with_mask([false, true, true]))
    mask1.mask.should eq([false, true, true])

    mask1 = PArrow::ProjectionMask.leaves_with_mask([true, false, true])
    mask1.intersect(PArrow::ProjectionMask.all)
    mask1.mask.should eq([true, false, true])

    mask1 = PArrow::ProjectionMask.all
    mask1.intersect(PArrow::ProjectionMask.all)
    mask1.mask.should be_nil
  end
end

describe Parquet::Arrow do
  # Upstream `parquet_column`: looks up a top-level, non-nested column by name.
  it "looks up parquet columns by arrow field name" do
    schema = parse_parquet_schema("message s { OPTIONAL INT32 a; REQUIRED INT32 b; }")
    arrow_schema = Arrow::Schema.new(Arrow::Fields.from([
      Arrow::Field.new("a", Arrow::DataType.int32, true),
      Arrow::Field.new("b", Arrow::DataType.int32, false),
    ]))

    index, field = PArrow.parquet_column(schema, arrow_schema, "b").not_nil!
    index.should eq(1)
    field.name.should eq("b")

    PArrow.parquet_column(schema, arrow_schema, "missing").should be_nil
  end

  it "rejects nested arrow fields in parquet_column" do
    schema = parse_parquet_schema("message s { OPTIONAL INT32 a; }")
    arrow_schema = Arrow::Schema.new(Arrow::Fields.from([
      Arrow::Field.new_list("a", Arrow::Field.new("element", Arrow::DataType.int32, true), true),
    ]))

    PArrow.parquet_column(schema, arrow_schema, "a").should be_nil
  end
end
