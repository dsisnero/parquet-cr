require "../errors"
require "../basic"
require "../schema/types"
require "../file/reader"
require "../file/serialized_reader"
require "./api"
require "./triplet"

# Ported from `parquet/src/record/reader.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports `TreeBuilder`, `Reader`, `ReaderIter`, and `RowIter`.
# Divergences: Rust's payload-carrying `Reader` enum is an abstract class with
# the subclasses `PrimitiveReader`/`OptionReader`/`GroupReader`/
# `RepeatedReader`/`KeyValueReader`; the `Either` file-reader wrapper is an
# optional `FileReader`; `usize` is `Int32`; errors are raised (not `Result`).
module Parquet
  module Record
    DEFAULT_BATCH_SIZE = 1024

    # Tree builder for the `Reader` tree.
    class TreeBuilder
      def initialize(@batch_size : Int32 = DEFAULT_BATCH_SIZE)
      end

      # Sets the batch size for this tree builder.
      def with_batch_size(batch_size : Int32) : TreeBuilder
        @batch_size = batch_size
        self
      end

      # Creates a new root reader for the schema and row group.
      def build(descr : Schema::SchemaDescriptor, row_group_reader : File::RowGroupReader) : Reader
        readers = [] of Reader
        path = [] of String
        descr.root_schema.get_fields.each do |field|
          readers << reader_tree(field, path, 0_i16, 0_i16, row_group_reader)
        end
        Reader::GroupReader.new(nil, 0_i16, readers)
      end

      # Creates an iterator of `Row`s from the schema and row group.
      def as_iter(descr : Schema::SchemaDescriptor, row_group_reader : File::RowGroupReader) : ReaderIter
        ReaderIter.new(build(descr, row_group_reader), row_group_reader.metadata.num_rows.to_i32)
      end

      # Builds the reader tree for `field` recursively.
      private def reader_tree(
        field : Schema::Type,
        path : Array(String),
        curr_def_level : Int16,
        curr_rep_level : Int16,
        row_group_reader : File::RowGroupReader,
      ) : Reader
        basic = field.get_basic_info
        repetition = basic.repetition
        case repetition
        when Repetition::OPTIONAL
          curr_def_level += 1
        when Repetition::REPEATED
          curr_def_level += 1
          curr_rep_level += 1
        end

        path << field.name
        reader =
          if field.primitive?
            col_path = Schema::ColumnPath.new(path.dup)
            orig_index = find_column_index(row_group_reader, col_path)
            col_descr = row_group_reader.metadata.column(orig_index).column_descr
            col_reader = row_group_reader.get_column_reader(orig_index)
            column = TripletIter.new(col_descr, col_reader, @batch_size)
            primitive = Reader::PrimitiveReader.new(field, column)
            if repetition == Repetition::REPEATED
              Reader::RepeatedReader.new(field, curr_def_level - 1, curr_rep_level - 1, primitive)
            else
              primitive
            end
          else
            converted = basic.converted_type
            if converted == ConvertedType::LIST
              build_list(field, path, curr_def_level, curr_rep_level, row_group_reader)
            elsif converted == ConvertedType::MAP || converted == ConvertedType::MAP_KEY_VALUE
              build_map(field, path, curr_def_level, curr_rep_level, row_group_reader)
            elsif repetition == Repetition::REPEATED
              # A repeated field without LIST/MAP annotation is a required list
              # whose element type is the field itself. Upstream returns early
              # here (without re-wrapping in an option reader).
              required_field = Schema::Type.group_type_builder(field.name)
                .with_repetition(Repetition::REQUIRED)
                .with_converted_type(converted)
                .with_fields(field.get_fields)
                .build
              path.pop
              inner = reader_tree(required_field, path, curr_def_level, curr_rep_level, row_group_reader)
              return Reader::RepeatedReader.new(field, curr_def_level - 1, curr_rep_level - 1, inner)
            else
              readers = [] of Reader
              field.get_fields.each do |child|
                readers << reader_tree(child, path, curr_def_level, curr_rep_level, row_group_reader)
              end
              Reader::GroupReader.new(field, curr_def_level, readers)
            end
          end

        path.pop
        Reader.option(repetition, curr_def_level, reader)
      end

      private def build_list(
        field : Schema::Type,
        path : Array(String),
        curr_def_level : Int16,
        curr_rep_level : Int16,
        row_group_reader : File::RowGroupReader,
      ) : Reader
        fields = field.get_fields
        raise general_err!("Invalid list type") unless fields.size == 1

        repeated_field = fields[0]
        unless repeated_field.get_basic_info.repetition == Repetition::REPEATED
          raise general_err!("Invalid list type")
        end

        if Reader.element_type?(repeated_field)
          reader = reader_tree(repeated_field, path, curr_def_level, curr_rep_level, row_group_reader)
          Reader::RepeatedReader.new(field, curr_def_level, curr_rep_level, reader)
        else
          child_field = repeated_field.get_fields[0]
          path << repeated_field.name
          reader = reader_tree(child_field, path, curr_def_level + 1, curr_rep_level + 1, row_group_reader)
          path.pop
          Reader::RepeatedReader.new(field, curr_def_level, curr_rep_level, reader)
        end
      end

      private def build_map(
        field : Schema::Type,
        path : Array(String),
        curr_def_level : Int16,
        curr_rep_level : Int16,
        row_group_reader : File::RowGroupReader,
      ) : Reader
        fields = field.get_fields
        raise general_err!("Invalid map type") unless fields.size == 1

        key_value_type = fields[0]
        raise general_err!("Invalid map type") if key_value_type.primitive?
        unless key_value_type.get_basic_info.repetition == Repetition::REPEATED
          raise general_err!("Invalid map type")
        end

        path << key_value_type.name
        key_type = key_value_type.get_fields[0]
        unless key_type.primitive?
          raise general_err!("Map key type is expected to be a primitive type")
        end
        key_reader = reader_tree(key_type, path, curr_def_level + 1, curr_rep_level + 1, row_group_reader)

        if key_value_type.get_fields.size == 1
          path.pop
          Reader::RepeatedReader.new(field, curr_def_level, curr_rep_level, key_reader)
        else
          value_type = key_value_type.get_fields[1]
          value_reader = reader_tree(value_type, path, curr_def_level + 1, curr_rep_level + 1, row_group_reader)
          path.pop
          Reader::KeyValueReader.new(field, curr_def_level, curr_rep_level, key_reader, value_reader)
        end
      end

      private def find_column_index(row_group_reader : File::RowGroupReader, col_path : Schema::ColumnPath) : Int32
        row_group_reader.metadata.columns.each_with_index do |col, i|
          return i if col.column_path == col_path
        end
        raise general_err!("Path #{col_path} not found")
      end
    end

    # Reader tree for record assembly.
    abstract class Reader
      # Wraps `reader` in an option reader based on repetition.
      def self.option(repetition : Repetition, def_level : Int16, reader : Reader) : Reader
        if repetition == Repetition::OPTIONAL
          Reader::OptionReader.new(def_level - 1, reader)
        else
          reader
        end
      end

      # Returns `true` if the repeated type is an element type for a list.
      def self.element_type?(repeated_type : Schema::Type) : Bool
        return false if list_type?(repeated_type) || single_repeated_child?(repeated_type)

        repeated_type.primitive? ||
          (repeated_type.group? && repeated_type.get_fields.size > 1) ||
          repeated_type.name == "array" ||
          repeated_type.name.ends_with?("_tuple")
      end

      private def self.list_type?(type : Schema::Type) : Bool
        type.get_basic_info.converted_type == ConvertedType::LIST
      end

      private def self.single_repeated_child?(type : Schema::Type) : Bool
        fields = type.get_fields
        fields.size == 1 && fields[0].get_basic_info.repetition == Repetition::REPEATED
      end

      # Reads the current record as a `Row` (valid only for the root reader).
      def read : Row
        raise general_err!("Cannot call read() on #{self}")
      end

      # Reads the current record as a `Field`.
      abstract def read_field : Field
      # Returns the field name for the current reader.
      abstract def field_name : String
      # Returns the repetition for the current reader.
      abstract def repetition : Repetition
      # Returns `true` if the current reader has more values.
      abstract def has_next? : Bool
      # Returns the current definition level.
      abstract def current_def_level : Int16
      # Returns the current repetition level.
      abstract def current_rep_level : Int16
      # Advances the leaf columns for the current reader.
      abstract def advance_columns : Nil

      def to_s(io : IO) : Nil
        io << self.class.name.split("::").last
      end
    end

    # Primitive reader with type information and triplet iterator.
    class Reader::PrimitiveReader < Reader
      def initialize(@field : Schema::Type, @column : TripletIter)
      end

      def read_field : Field
        raise general_err!("Unexpected end of column data") unless @column.has_next?

        value = @column.current_value
        @column.read_next
        value
      end

      def field_name : String
        @field.name
      end

      def repetition : Repetition
        @field.get_basic_info.repetition
      end

      def has_next? : Bool
        @column.has_next?
      end

      def current_def_level : Int16
        @column.current_def_level
      end

      def current_rep_level : Int16
        @column.current_rep_level
      end

      def advance_columns : Nil
        @column.read_next
        nil
      end
    end

    # Optional reader with the definition level of a parent and a reader.
    class Reader::OptionReader < Reader
      def initialize(@def_level : Int16, @reader : Reader)
      end

      def read_field : Field
        raise general_err!("Unexpected end of column data") unless @reader.has_next?

        if @reader.current_def_level > @def_level
          @reader.read_field
        else
          @reader.advance_columns
          Field::Null.new
        end
      end

      def field_name : String
        @reader.field_name
      end

      def repetition : Repetition
        @reader.repetition
      end

      def has_next? : Bool
        @reader.has_next?
      end

      def current_def_level : Int16
        @reader.current_def_level
      end

      def current_rep_level : Int16
        @reader.current_rep_level
      end

      def advance_columns : Nil
        @reader.advance_columns
      end
    end

    # Group (struct) reader with an optional type, definition level and children.
    class Reader::GroupReader < Reader
      @readers : Array(Reader)

      def initialize(@field : Schema::Type?, @def_level : Int16, readers : Array(R)) forall R
        @readers = readers.map(&.as(Reader))
      end

      def read : Row
        fields = @readers.map { |reader| {reader.field_name, reader.read_field} }
        Row.new(fields)
      end

      def read_field : Field
        fields = @readers.map do |reader|
          if reader.repetition != Repetition::OPTIONAL || reader.current_def_level > @def_level
            {reader.field_name, reader.read_field}
          else
            reader.advance_columns
            {reader.field_name, Field::Null.new}
          end
        end
        Field::Group.new(Row.new(fields))
      end

      def field_name : String
        field = @field || raise general_err!("Field is None for group reader")
        field.name
      end

      def repetition : Repetition
        field = @field || raise general_err!("Field is None for group reader")
        field.get_basic_info.repetition
      end

      def has_next? : Bool
        first.has_next?
      end

      def current_def_level : Int16
        first.current_def_level
      end

      def current_rep_level : Int16
        first.current_rep_level
      end

      def advance_columns : Nil
        @readers.each(&.advance_columns)
        nil
      end

      private def first : Reader
        @readers.first? || raise general_err!("empty group reader")
      end
    end

    # Reader for repeated values, e.g. lists.
    class Reader::RepeatedReader < Reader
      def initialize(@field : Schema::Type, @def_level : Int16, @rep_level : Int16, @reader : Reader)
      end

      def read_field : Field
        raise general_err!("Unexpected end of column data") unless @reader.has_next?

        elements = [] of Field
        loop do
          if @reader.current_def_level > @def_level
            elements << @reader.read_field
          else
            @reader.advance_columns
            break
          end

          break if !@reader.has_next? || @reader.current_rep_level <= @rep_level
        end
        Field::ListInternal.new(Record.make_list(elements))
      end

      def field_name : String
        @field.name
      end

      def repetition : Repetition
        @field.get_basic_info.repetition
      end

      def has_next? : Bool
        @reader.has_next?
      end

      def current_def_level : Int16
        @reader.current_def_level
      end

      def current_rep_level : Int16
        @reader.current_rep_level
      end

      def advance_columns : Nil
        @reader.advance_columns
      end
    end

    # Reader of key-value pairs, e.g. maps.
    class Reader::KeyValueReader < Reader
      def initialize(
        @field : Schema::Type,
        @def_level : Int16,
        @rep_level : Int16,
        @keys : Reader,
        @values : Reader,
      )
      end

      def read_field : Field
        raise general_err!("Unexpected end of column data") unless @keys.has_next?

        pairs = [] of Tuple(Field, Field)
        loop do
          if @keys.current_def_level > @def_level
            pairs << {@keys.read_field, @values.read_field}
          else
            @keys.advance_columns
            @values.advance_columns
            break
          end

          break if !@keys.has_next? || @keys.current_rep_level <= @rep_level
        end

        Field::MapInternal.new(Record.make_map(pairs))
      end

      def field_name : String
        @field.name
      end

      def repetition : Repetition
        @field.get_basic_info.repetition
      end

      def has_next? : Bool
        @keys.has_next?
      end

      def current_def_level : Int16
        @keys.current_def_level
      end

      def current_rep_level : Int16
        @keys.current_rep_level
      end

      def advance_columns : Nil
        @keys.advance_columns
        @values.advance_columns
      end
    end

    # Internal iterator of `Row`s for a reader.
    class ReaderIter
      include Iterator(Row)

      def initialize(@root_reader : Reader, @records_left : Int32)
        @root_reader.advance_columns
      end

      def next : Row | Iterator::Stop
        if @records_left > 0
          @records_left -= 1
          @root_reader.read
        else
          stop
        end
      end
    end

    # Access Parquet data as an iterator of `Row`.
    class RowIter
      include Iterator(Row)

      def initialize(
        @descr : Schema::SchemaDescriptor,
        @tree_builder : TreeBuilder,
        @file_reader : File::FileReader?,
        @row_iter : ReaderIter?,
        @current_row_group : Int32,
        @num_row_groups : Int32,
      )
      end

      # Creates an iterator of `Row`s for all row groups in a file.
      def self.from_file(proj : Schema::Type?, reader : File::FileReader) : RowIter
        descr = get_proj_descr(proj, reader.metadata.file_metadata.schema_descr)
        new(descr, TreeBuilder.new, reader, nil, 0, reader.num_row_groups)
      end

      # Creates an iterator of `Row`s for a specific row group.
      def self.from_row_group(proj : Schema::Type?, reader : File::RowGroupReader) : RowIter
        descr = get_proj_descr(proj, reader.metadata.schema_descr)
        tree_builder = TreeBuilder.new
        row_iter = tree_builder.as_iter(descr, reader)
        new(descr, tree_builder, nil, row_iter, 0, 0)
      end

      # Creates an iterator of `Row`s from a file reader using the full schema.
      def self.from_file_into(reader : File::FileReader) : RowIter
        descr = reader.metadata.file_metadata.schema_descr
        new(descr, TreeBuilder.new, reader, nil, 0, reader.num_row_groups)
      end

      # Creates an iterator of `Row`s using a projection.
      def project(proj : Schema::Type?) : RowIter
        reader = @file_reader || raise general_err!("File reader is required to use projections")
        descr = RowIter.get_proj_descr(proj, reader.metadata.file_metadata.schema_descr)
        RowIter.new(descr, @tree_builder, reader, nil, 0, @num_row_groups)
      end

      # Sets the batch size for this row iter.
      def with_batch_size(batch_size : Int32) : RowIter
        @tree_builder = @tree_builder.with_batch_size(batch_size)
        self
      end

      def next : Row | Iterator::Stop
        row = @row_iter.try { |iter| iter.next.as?(Row) }

        while row.nil? && @current_row_group < @num_row_groups
          if file_reader = @file_reader
            row_group_reader = file_reader.get_row_group(@current_row_group)
            iter = @tree_builder.as_iter(@descr, row_group_reader)
            row = iter.next.as?(Row)
            @current_row_group += 1
            @row_iter = iter
          end
        end

        row.nil? ? stop : row
      end

      # Returns the schema descriptor for a projected schema.
      def self.get_proj_descr(proj : Schema::Type?, root_descr : Schema::SchemaDescriptor) : Schema::SchemaDescriptor
        if projection = proj
          unless root_descr.root_schema.check_contains(projection)
            raise general_err!("Root schema does not contain projection")
          end
          Schema::SchemaDescriptor.new(projection)
        else
          root_descr
        end
      end
    end
  end
end

module Parquet
  module File
    abstract class FileReader
      # Returns an iterator of `Record::Row`s over the file.
      def get_row_iter(projection : Schema::Type?) : Parquet::Record::RowIter
        Parquet::Record::RowIter.from_file(projection, self)
      end
    end

    abstract class RowGroupReader
      # Returns an iterator of `Record::Row`s over this row group.
      def get_row_iter(projection : Schema::Type?) : Parquet::Record::RowIter
        Parquet::Record::RowIter.from_row_group(projection, self)
      end
    end
  end
end
