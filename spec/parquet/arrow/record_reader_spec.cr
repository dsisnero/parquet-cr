require "../../spec_helper"

# Ported from `parquet/src/arrow/record_reader/mod.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Uses the ported `ParquetTestPageUtil` builders (`DataPageBuilderImpl`,
# `InMemoryPageReader`). The upstream `SELECTIVE_PADDING` tests are ported at the
# end; `test_read_more_than_one_batch` builds a 5000-level page.
private alias RR = Parquet::Arrow::RecordReader

private DEFAULT_BATCH_SIZE = 1024

private alias ValuesBuffer = RR::ArrayValuesBuffer(Int32)
private alias Decoder = Parquet::Column::Reader::ColumnValueDecoderImpl(Int32)

private def record_reader(message_type : String, capacity : Int32 = DEFAULT_BATCH_SIZE) : RR::GenericRecordReader(ValuesBuffer, Decoder)
  desc = ParquetTestPageUtil.column_descriptor(message_type)
  RR.typed_record_reader(desc, capacity, 0)
end

private def page(desc : Parquet::Schema::ColumnDescriptor, num_values : Int32, v2 : Bool = true) : ParquetTestPageUtil::DataPageBuilderImpl
  ParquetTestPageUtil::DataPageBuilderImpl.new(desc, num_values.to_u32, v2)
end

private def masks(bytes : Bytes, size : Int32) : Array(Bool)
  Array(Bool).new(size) { |i| Parquet::Util::BitUtil.get_bit(bytes, i) }
end

describe Parquet::Arrow::RecordReader::GenericRecordReader do
  # Upstream `test_read_required_records`.
  it "reads required records across pages" do
    message_type = "message test_schema { REQUIRED INT32 leaf; }"
    desc = ParquetTestPageUtil.column_descriptor(message_type)
    reader = record_reader(message_type)

    builder = page(desc, 5)
    builder.add_values(Parquet::Encoding::PLAIN, [4, 7, 6, 3, 2])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.read_records(2).should eq(2)
    reader.num_records.should eq(2)
    reader.num_values.should eq(2)
    reader.read_records(3).should eq(3)
    reader.num_records.should eq(5)
    reader.num_values.should eq(5)

    builder = page(desc, 2)
    builder.add_values(Parquet::Encoding::PLAIN, [8, 9])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.read_records(10).should eq(2)
    reader.num_records.should eq(7)
    reader.num_values.should eq(7)

    reader.consume_record_data.values.should eq([4, 7, 6, 3, 2, 8, 9])
    reader.consume_def_levels.should be_nil
    reader.consume_bitmap.should be_nil
  end

  # Upstream `test_capacity_hint_preserved_across_fragmented_reads`.
  it "keeps the capacity hint across fragmented reads" do
    message_type = "message test_schema { REQUIRED INT32 leaf; }"
    desc = ParquetTestPageUtil.column_descriptor(message_type)
    reader = record_reader(message_type, capacity: 8)

    builder = page(desc, 4)
    builder.add_values(Parquet::Encoding::PLAIN, [1, 2, 3, 4])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.read_records(1).should eq(1)
    reader.read_records(1).should eq(1)

    data = reader.consume_record_data.values
    data.should eq([1, 2])
    data.size.should be <= 8 # capacity hint only pre-allocates
  end

  # Upstream `test_read_optional_records`.
  it "reads optional records with definition levels" do
    message_type = "message test_schema { OPTIONAL Group test_struct { OPTIONAL INT32 leaf; } }"
    desc = ParquetTestPageUtil.column_descriptor(message_type)
    reader = record_reader(message_type)

    builder = page(desc, 5)
    builder.add_def_levels(2_i16, [1_i16, 2_i16, 0_i16, 2_i16, 2_i16])
    builder.add_values(Parquet::Encoding::PLAIN, [7, 6, 3])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.read_records(2).should eq(2)
    reader.num_records.should eq(2)
    reader.num_values.should eq(2)
    reader.read_records(3).should eq(3)
    reader.num_records.should eq(5)
    reader.num_values.should eq(5)

    builder = page(desc, 2)
    builder.add_def_levels(2_i16, [0_i16, 2_i16])
    builder.add_values(Parquet::Encoding::PLAIN, [8])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.read_records(10).should eq(2)
    reader.num_records.should eq(7)
    reader.num_values.should eq(7)

    reader.consume_def_levels.should eq([1_i16, 2_i16, 0_i16, 2_i16, 2_i16, 0_i16, 2_i16])

    expected_valid = [false, true, false, true, true, false, true]
    masks(reader.consume_bitmap.not_nil!, 7).should eq(expected_valid)

    actual = reader.consume_record_data.values
    expected = [0, 7, 0, 6, 3, 0, 8]
    actual.size.should eq(expected.size)
    expected_valid.each_with_index do |valid, i|
      actual[i].should eq(expected[i]) if valid
    end
  end

  # Upstream `test_read_repeated_records`.
  it "reads repeated records with repetition levels" do
    message_type = "message test_schema { REPEATED Group test_struct { REPEATED INT32 leaf; } }"
    desc = ParquetTestPageUtil.column_descriptor(message_type)
    reader = record_reader(message_type)

    builder = page(desc, 7)
    builder.add_rep_levels(2_i16, [0_i16, 0_i16, 0_i16, 1_i16, 2_i16, 2_i16, 1_i16])
    builder.add_def_levels(2_i16, [2_i16, 0_i16, 1_i16, 2_i16, 2_i16, 2_i16, 2_i16])
    builder.add_values(Parquet::Encoding::PLAIN, [4, 7, 6, 3, 2])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.read_records(1).should eq(1)
    reader.num_records.should eq(1)
    reader.num_values.should eq(1)
    reader.read_records(3).should eq(2)
    reader.num_records.should eq(3)
    reader.num_values.should eq(7)

    builder = page(desc, 2)
    builder.add_rep_levels(2_i16, [0_i16, 2_i16])
    builder.add_def_levels(2_i16, [2_i16, 2_i16])
    builder.add_values(Parquet::Encoding::PLAIN, [8, 9])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.read_records(10).should eq(1)
    reader.num_records.should eq(4)
    reader.num_values.should eq(9)

    reader.consume_def_levels.should eq([2_i16, 0_i16, 1_i16, 2_i16, 2_i16, 2_i16, 2_i16, 2_i16, 2_i16])
    expected_valid = [true, false, false, true, true, true, true, true, true]
    masks(reader.consume_bitmap.not_nil!, 9).should eq(expected_valid)

    actual = reader.consume_record_data.values
    expected = [4, 0, 0, 7, 6, 3, 2, 8, 9]
    actual.size.should eq(expected.size)
    expected_valid.each_with_index do |valid, i|
      actual[i].should eq(expected[i]) if valid
    end
  end

  # Upstream `test_read_more_than_one_batch`.
  it "reads more than one batch" do
    message_type = "message test_schema { REPEATED INT32 leaf; }"
    desc = ParquetTestPageUtil.column_descriptor(message_type)
    reader = record_reader(message_type)

    rep_levels = Array(Int16).new(5000, 1_i16)
    1000.times { |idx| rep_levels[idx * 5] = 0_i16 }
    def_levels = Array(Int16).new(5000, 1_i16)
    values = Array(Int32).new(5000, 100)

    builder = page(desc, 5000)
    builder.add_rep_levels(1_i16, rep_levels)
    builder.add_def_levels(1_i16, def_levels)
    builder.add_values(Parquet::Encoding::PLAIN, values)
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.read_records(1000).should eq(1000)
    reader.num_records.should eq(1000)
    reader.num_values.should eq(5000)
  end

  # Upstream `test_row_group_boundary`.
  it "stops at row group boundaries" do
    message_type = "message test_schema { REPEATED Group test_struct { REPEATED INT32 leaf; } }"
    desc = ParquetTestPageUtil.column_descriptor(message_type)

    builder = page(desc, 7)
    builder.add_rep_levels(2_i16, [0_i16, 0_i16, 0_i16, 1_i16, 2_i16, 0_i16, 1_i16])
    builder.add_def_levels(2_i16, [1_i16, 0_i16, 1_i16, 2_i16, 2_i16, 1_i16, 2_i16])
    builder.add_values(Parquet::Encoding::PLAIN, [1, 2, 3])
    built = builder.consume

    reader = record_reader(message_type)
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([built]))
    reader.read_records(4).should eq(4)
    reader.num_records.should eq(4)
    reader.num_values.should eq(7)

    reader.read_records(4).should eq(0)
    reader.num_records.should eq(4)
    reader.num_values.should eq(7)

    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([built]))
    reader.read_records(4).should eq(4)
    reader.num_records.should eq(8)
    reader.num_values.should eq(14)
  end

  # Upstream `test_skip_required_records`.
  it "skips required records" do
    message_type = "message test_schema { REQUIRED INT32 leaf; }"
    desc = ParquetTestPageUtil.column_descriptor(message_type)
    reader = record_reader(message_type)

    builder = page(desc, 5)
    builder.add_values(Parquet::Encoding::PLAIN, [4, 7, 6, 3, 2])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.skip_records(2).should eq(2)
    reader.num_records.should eq(0)
    reader.num_values.should eq(0)
    reader.read_records(3).should eq(3)
    reader.num_records.should eq(3)
    reader.num_values.should eq(3)

    builder = page(desc, 2)
    builder.add_values(Parquet::Encoding::PLAIN, [8, 9])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.skip_records(10).should eq(2)
    reader.num_records.should eq(3)
    reader.num_values.should eq(3)
    reader.read_records(10).should eq(0)

    reader.consume_record_data.values.should eq([6, 3, 2])
    reader.consume_def_levels.should be_nil
    reader.consume_bitmap.should be_nil
  end

  # Upstream `test_selective_padding_consumes_full_bitmap`.
  it "consumes the compact bitmap under selective padding" do
    message_type = "message test_schema { OPTIONAL GROUP my_list (LIST) { REPEATED GROUP list { OPTIONAL INT32 element; } } }"
    desc = ParquetTestPageUtil.column_descriptor(message_type)
    reader = RR.typed_record_reader(desc, DEFAULT_BATCH_SIZE, 0)
    reader.set_padding_threshold(2_i16)

    builder = page(desc, 5)
    builder.add_rep_levels(1_i16, [0_i16, 1_i16, 0_i16, 0_i16, 1_i16])
    builder.add_def_levels(3_i16, [3_i16, 2_i16, 0_i16, 2_i16, 3_i16])
    builder.add_values(Parquet::Encoding::PLAIN, [10, 20])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.read_records(3).should eq(3)

    masks(reader.consume_compact_bitmap.not_nil!, 4).should eq([true, false, false, true])
    reader.consume_bitmap.should be_nil
  end

  # Upstream `test_selective_padding_reserves_compact_capacity`.
  it "only pads item-level entries under selective padding" do
    message_type = "message test_schema { OPTIONAL GROUP my_list (LIST) { REPEATED GROUP list { OPTIONAL INT32 element; } } }"
    desc = ParquetTestPageUtil.column_descriptor(message_type)
    reader = RR.typed_record_reader(desc, DEFAULT_BATCH_SIZE, 0)
    reader.set_padding_threshold(2_i16)

    def_levels = Array(Int16).new(30, 0_i16) + [3_i16, 2_i16]
    rep_levels = Array(Int16).new(def_levels.size, 0_i16)
    builder = page(desc, def_levels.size)
    builder.add_rep_levels(1_i16, rep_levels)
    builder.add_def_levels(3_i16, def_levels)
    builder.add_values(Parquet::Encoding::PLAIN, [10])
    reader.set_page_reader(ParquetTestPageUtil::InMemoryPageReader.new([builder.consume]))

    reader.read_records(128).should eq(def_levels.size)
    masks(reader.consume_compact_bitmap.not_nil!, 2).should eq([true, false])

    values = reader.consume_record_data
    values.size.should eq(2)
    values[0].should eq(10)
  end
end
