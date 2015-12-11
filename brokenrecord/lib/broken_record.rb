require 'sqlite3'

module BrokenRecord
  class Table

    attr_accessor :name, :columns, :primary_key

    def initialize(params)
      @name = params[:name]
      @db = params[:db]
      @columns = {}
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

    def parse_table
      @db.execute("PRAGMA table_info(#{@name})")
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

    def get_columns
      table_data = parse_table
      table_data.each do |column|
        column_name = column[1].to_sym
        @columns[column_name] = {type: column[2]}
      end
    end

    def new_row(params)
      column_headers = @columns.keys.join(", ")
      values = get_values(params).join(", ")
      @db.execute("INSERT INTO #{@name}(#{column_headers}) VALUES(#{values})")
    end

    def update(identifier, params)
      row_data = where(identifier)
      values = get_values(params)
      @db.execute("UPDATE #{@name}
                   SET #{params.keys[0]}=#{values[0]}
                   WHERE #{row_data.keys[0]}=#{row_data.values[0]}" )
    end

    def delete_row(identifier)
      row_data = where(identifier)
      @db.execute("DELETE FROM #{@name} WHERE #{row_data.keys[0]}=#{row_data.values[0]}")
    end

    def get_values(params)
      values = []
      params.each {|k, v| values << pack_row_value(@columns[k][:type], v)}
      return values
    end

    def where(identifier)
      row_data = nil
      rows.each do |row|
        if row.values.include?(identifier.values[0])
          row_data = row
        end
        return row_data
      end
    end

    def pack_row_value(column_type, row_value)
      case column_type
      when "STRING"
        "'#{row_value}'"
      when "INTEGER"
        row_value
      end
    end

  end
end
