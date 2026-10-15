require "../../spec_helper"

# Ported from `parquet/src/schema/types.rs` `mod tests` (`test_schema_type_thrift_*`,
# `test_schema_from_thrift_*`) apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b.
describe Parquet::Schema do
  # Upstream `test_schema_type_thrift_conversion_err`.
  it "rejects serializing a non-group root schema" do
    schema = ST.primitive_type_builder("col", PPhys::INT32).build.as(ST)
    expect_raises(Parquet::ParquetError, "Root schema must be Group type") do
      Parquet::Schema.schema_to_buf(schema)
    end
  end

  # Upstream `test_schema_type_thrift_conversion`.
  it "round-trips a complex schema through thrift" do
    message_type = <<-SCHEMA
      message conversions {
        REQUIRED INT64 id;
        OPTIONAL FIXED_LEN_BYTE_ARRAY (2) f16 (FLOAT16);
        OPTIONAL group int_array_Array (LIST) {
          REPEATED group list {
            OPTIONAL group element (LIST) {
              REPEATED group list {
                OPTIONAL INT32 element;
              }
            }
          }
        }
        OPTIONAL group int_map (MAP) {
          REPEATED group map (MAP_KEY_VALUE) {
            REQUIRED BYTE_ARRAY key (UTF8);
            OPTIONAL INT32 value;
          }
        }
        OPTIONAL group int_Map_Array (LIST) {
          REPEATED group list {
            OPTIONAL group g (MAP) {
              REPEATED group map (MAP_KEY_VALUE) {
                REQUIRED BYTE_ARRAY key (UTF8);
                OPTIONAL group value {
                  OPTIONAL group H {
                    OPTIONAL group i (LIST) {
                      REPEATED group list {
                        OPTIONAL DOUBLE element;
                      }
                    }
                  }
                }
              }
            }
          }
        }
        OPTIONAL group nested_struct {
          OPTIONAL INT32 A;
          OPTIONAL group b (LIST) {
            REPEATED group list {
              REQUIRED FIXED_LEN_BYTE_ARRAY (16) element;
            }
          }
        }
      }
      SCHEMA

    expected = Parquet::Schema.parse_message_type(message_type)
    result = Parquet::Schema.roundtrip_schema(expected)
    result.should eq(expected)
  end

  # Upstream `test_schema_type_thrift_conversion_decimal`.
  it "round-trips decimal schemas through thrift" do
    message_type = <<-SCHEMA
      message decimals {
        OPTIONAL INT32 field0;
        OPTIONAL INT64 field1 (DECIMAL (18, 2));
        OPTIONAL FIXED_LEN_BYTE_ARRAY (16) field2 (DECIMAL (38, 18));
        OPTIONAL BYTE_ARRAY field3 (DECIMAL (9));
      }
      SCHEMA

    expected = Parquet::Schema.parse_message_type(message_type)
    result = Parquet::Schema.roundtrip_schema(expected)
    result.should eq(expected)
  end

  # Upstream `test_schema_from_thrift_with_num_children_set`.
  it "reads thrift schemas with num_children set to 0 for primitives" do
    message_type = <<-SCHEMA
      message schema {
        OPTIONAL BYTE_ARRAY id (UTF8);
        OPTIONAL BYTE_ARRAY name (UTF8);
        OPTIONAL BYTE_ARRAY message (UTF8);
        OPTIONAL INT32 type (UINT_8);
        OPTIONAL INT64 author_time (TIMESTAMP_MILLIS);
        OPTIONAL INT64 __index_level_0__;
      }
      SCHEMA

    expected = Parquet::Schema.parse_message_type(message_type)
    buf = Parquet::Schema.schema_to_buf(expected)
    thrift_schema = Parquet::Schema.buf_to_schema_list(buf)

    # Change all of None to Some(0)
    thrift_schema.each { |elem| elem.num_children = 0 if elem.num_children.nil? }

    result = Parquet::Schema.parquet_schema_from_array(thrift_schema)
    result.should eq(expected)
  end

  # Upstream `test_schema_from_thrift_root_has_repetition`.
  it "ignores a repetition level set on the root node" do
    message_type = <<-SCHEMA
      message schema {
        OPTIONAL BYTE_ARRAY a (UTF8);
        OPTIONAL INT32 b (UINT_8);
      }
      SCHEMA

    expected = Parquet::Schema.parse_message_type(message_type)
    buf = Parquet::Schema.schema_to_buf(expected)
    thrift_schema = Parquet::Schema.buf_to_schema_list(buf)
    thrift_schema[0].repetition_type = PRep::REQUIRED

    result = Parquet::Schema.parquet_schema_from_array(thrift_schema)
    result.should eq(expected)
  end

  # Upstream `test_schema_from_thrift_group_has_no_child`.
  it "handles an empty group schema" do
    expected = Parquet::Schema.parse_message_type("message schema {}")
    buf = Parquet::Schema.schema_to_buf(expected)
    thrift_schema = Parquet::Schema.buf_to_schema_list(buf)
    thrift_schema[0].repetition_type = PRep::REQUIRED

    result = Parquet::Schema.parquet_schema_from_array(thrift_schema)
    result.should eq(expected)
  end
end
