# Ported from `parquet/src/util/test_common/page_util.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Test-only helpers for building data pages (v1 or v2) in memory and reading
# them back through a `PageReader`/`PageIterator`.
#
# Divergences: `DataPageBuilder`'s trait is a single class (Crystal has no
# traits) and `add_values` takes the value type explicitly (`T.class`).
module ParquetTestPageUtil
  # Builds a data page (v1 or v2). Callers populate it in order:
  # rep levels, def levels, values (or indices), then `consume`.
  class DataPageBuilderImpl
    @encoding : Parquet::Encoding?
    @num_values : UInt32
    @buffer : Array(UInt8)
    @rep_levels_byte_len : UInt32
    @def_levels_byte_len : UInt32
    @datapage_v2 : Bool
    @type_width : Int32

    # `num_values` is the number of non-null values to put in the data page.
    def initialize(@descr : Parquet::Schema::ColumnDescriptor, @num_values : UInt32, @datapage_v2 : Bool)
      @encoding = nil
      @buffer = [] of UInt8
      @rep_levels_byte_len = 0_u32
      @def_levels_byte_len = 0_u32
      @type_width = @descr.type_length
    end

    def add_rep_levels(max_level : Int16, rep_levels : Array(Int16)) : Nil
      @num_values = rep_levels.size.to_u32
      @rep_levels_byte_len = add_levels(max_level, rep_levels)
    end

    def add_def_levels(max_level : Int16, def_levels : Array(Int16)) : Nil
      @num_values = def_levels.size.to_u32
      @def_levels_byte_len = add_levels(max_level, def_levels)
    end

    def add_values(encoding : Parquet::Encoding, values : Array(T)) : Nil forall T
      if @num_values < values.size.to_u32
        raise ArgumentError.new("num_values: #{@num_values}, values.size(): #{values.size}")
      end

      # A synthetic column descriptor for the encoder.
      typed = Parquet::Schema::Type.primitive_type_builder("t", Parquet::Encodings.physical_type(T))
        .with_length(@type_width)
        .build
      descriptor = Parquet::Schema::ColumnDescriptor.new(typed, 0_i16, 0_i16, Parquet::Schema::ColumnPath.from([] of String))

      @encoding = encoding
      encoder = Parquet::Encodings.get_encoder(T, encoding, descriptor)
      encoder.put(values)
      @buffer.concat(encoder.flush_buffer)
    end

    def add_indices(indices : Bytes) : Nil
      @encoding = Parquet::Encoding::RLE_DICTIONARY
      @buffer.concat(indices)
    end

    def consume : Parquet::Column::Page
      encoding = @encoding || raise general_err!("DataPageBuilder: encoding not set")
      buf = Bytes.new(@buffer.size) { |i| @buffer[i] }

      if @datapage_v2
        Parquet::Column::Page::DataPageV2.new(
          buf,
          @num_values,
          encoding,
          0_u32,
          @num_values,
          @def_levels_byte_len,
          @rep_levels_byte_len,
          false,
          nil
        )
      else
        Parquet::Column::Page::DataPage.new(
          buf,
          @num_values,
          encoding,
          Parquet::Encoding::RLE,
          Parquet::Encoding::RLE,
          nil
        )
      end
    end

    # Adds encoded levels, returning the number of encoded bytes.
    private def add_levels(max_level : Int16, levels : Array(Int16)) : UInt32
      return 0_u32 if max_level <= 0

      encoder = Parquet::Encodings::LevelEncoder.v1_streaming(max_level)
      encoder.put_with_observer(levels) { |_, _| }
      encoded = encoder.consume
      # The level encoder prefixes the encoded bytes with an i32 length.
      encoded_bytes = encoded[4, encoded.size - 4]
      if @datapage_v2
        # Data page v2 stores the length explicitly, so skip the prefix.
        @buffer.concat(encoded_bytes)
      else
        @buffer.concat(encoded)
      end
      encoded_bytes.size.to_u32
    end
  end

  # An in-memory page reader (upstream `InMemoryPageReader`).
  class InMemoryPageReader < Parquet::Column::PageReader
    def initialize(@pages : Array(Parquet::Column::Page), @position : Int32 = 0)
    end

    def get_next_page : Parquet::Column::Page?
      return nil if @position >= @pages.size

      page = @pages[@position]
      @position += 1
      page
    end

    def peek_next_page : Parquet::Column::PageMetadata?
      return nil if @position >= @pages.size

      case page = @pages[@position]
      when Parquet::Column::Page::DataPage
        Parquet::Column::PageMetadata.new(nil, page.num_values.to_i32, false)
      when Parquet::Column::Page::DataPageV2
        Parquet::Column::PageMetadata.new(page.num_rows.to_i32, page.num_values.to_i32, false)
      else
        Parquet::Column::PageMetadata.new(nil, nil, true)
      end
    end

    def skip_next_page : Nil
      @position += 1 if @position < @pages.size
      nil
    end
  end

  # An in-memory page iterator over several page readers (upstream
  # `InMemoryPageIterator`).
  class InMemoryPageIterator
    include Parquet::Column::PageIterator

    def initialize(@page_batches : Array(Array(Parquet::Column::Page)), @position : Int32 = 0)
    end

    def next : Parquet::Column::PageReader | Iterator::Stop
      return stop if @position >= @page_batches.size

      pages = @page_batches[@position]
      @position += 1
      InMemoryPageReader.new(pages).as(Parquet::Column::PageReader)
    end
  end

  # Builds a single-column `ColumnDescriptor` from a parquet message type.
  def self.column_descriptor(message_type : String) : Parquet::Schema::ColumnDescriptor
    schema = Parquet::Schema.parse_message_type(message_type)
    Parquet::Schema::SchemaDescriptor.new(schema).column(0)
  end
end
