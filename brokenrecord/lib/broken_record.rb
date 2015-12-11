require 'sqlite3'

module BrokenRecord
  class Table
    class DataError < StandardError
    end

    attr_accessor :name, :columns, :primary_key

    def initialize(params)
      @name = params[:name]
      @db = params[:db]
      @columns = {}

      table_data = parse_table
      table_data.each do |column|
        column_name = column[1].to_sym
        @columns[column_name] = {type: column[2]}
      end
    end

    def get_primary_key
      table_data = parse_table
      table_data.each do |column|
        column_name = column[1].to_sym
        if column.last == 1
          @primary_key = column_name
        end
      end
    end

    def rows
      a = []
      column_headers = @columns.keys.to_a
      rows_info = @db.execute("SELECT * FROM #{name}")
      rows_info.each do |row_info|
        a << Hash[column_headers.zip row_info]
      end
      return a
    end

    def new_row(params)
      column_headers = @columns.keys.join(", ")
      values = get_values(params).join(", ")
      @db.execute("INSERT INTO #{@name}(#{column_headers}) VALUES(#{values})")
    end

    def update(identifier, params)
      row_data = find(identifier)
      values = get_values(params)
      @db.execute("UPDATE #{@name}
                   SET #{params.keys[0]}=#{values[0]}
                   WHERE #{row_data.keys[0]}=#{row_data.values[0]}" )
    end

    def delete_row(identifier)
      row_data = find(identifier)
      @db.execute("DELETE FROM #{@name} WHERE #{row_data.keys[0]}=#{row_data.values[0]}")
    end

    def find(identifier)
      row_data = nil
      rows.each do |row|
        if row.values.include?(identifier.values[0])
          row_data = row
        end
        if row_data == nil
          raise DataError, "A row with #{identifier} cannot be found"
        else
          return row_data
        end
      end
    end

    def where(filter)
      row_data = []
      rows.each do |row|
        if row.values.include?(filter.values[0])
          row_data << row
        end
      end
      if row_data.empty?
        raise DataError, "Rows with #{filter} cannot be found"
      else
        return row_data
      end
    end

  private

    def parse_table
      @db.execute("PRAGMA table_info(#{@name})")
    end

    def pack_row_value(column_type, row_value)
      case column_type
      when "STRING"
        "'#{row_value}'"
      when "INTEGER"
        row_value
      end
    end

    def get_values(params)
      values = []
      params.each do |k, v|
        if @columns[k] == nil
          raise DataError, "There is no column name #{k}"
        else
          values << pack_row_value(@columns[k][:type], v)
        end
      end
      return values
    end

  end
end
