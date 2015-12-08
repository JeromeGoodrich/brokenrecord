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
      raw_table_data = @db.execute("PRAGMA table_info(#{name})")
      raw_table_data.each do |column|
        column_name = column[1].to_sym
        if column.last == 1
          @primary_key = column_name
        end
      end
    end

    def rows
      @db.execute("SELECT * FROM #{name}")
    end

    def get_columns
      raw_table_data = @db.execute("PRAGMA table_info(#{name})")
      raw_table_data.each do |column|
        column_name = column[1].to_sym
        @columns[column_name] = {type: column[2]}
      end
    end

    def new_row(params)
      column_headers = @columns.keys.join(', ')
      #hard coding in values for now there is some tricky stuff I need to figure out
      @db.execute("INSERT INTO #{@name}(#{column_headers}) VALUES('1', 'row1')")
    end

  end
end
