require 'rspec'
require 'sqlite3'
require 'broken_record'


describe BrokenRecord::Table do

  let (:test_db) { SQLite3::Database.new(":memory:") }
  let (:table)  { BrokenRecord::Table.new({ name: "New_Table",db: test_db }) }
  let (:row1_info) { {id: 1, name: "Tester McTesterson"} }
  let (:row2_info) { {id: 2, name: "Jerome Goodrich"} }
  let (:row3_info) { {id: 3, name: "Tester McTesterson"}}

  before do
    test_db.execute("CREATE TABLE new_table(id INTEGER PRIMARY KEY, name STRING)")
  end

  it "determines a primary key for the table" do
    table.get_primary_key
    expect(table.primary_key).to eq(:id)
  end

  it "is able to get column info" do
    table.get_columns
    columns = table.columns

    expect(columns.count).to eq(2)
    expect(columns[:id][:type]).to eq("INTEGER")
    expect(columns[:name][:type]).to eq("STRING")
  end

  it "can create new rows and read their info" do
    table.get_columns

    table.new_row(row1_info)
    table.new_row(row2_info)

    expect(table.rows.count).to eq(2)
  end

  it "can read row data" do
    table.get_columns

    table.new_row(row1_info)
    table.new_row(row2_info)

    expect(table.rows.first[:id]).to eq(1)
    expect(table.rows.last[:name]).to eq("Jerome Goodrich")
  end

  it "can update row data" do
    table.get_columns
    new_info = {name: "Dr. Evil"}
    identifier = {:id => 1}
    table.new_row(row1_info)
    table.new_row(row2_info)

    table.update(identifier, new_info)

    expect(table.rows.count).to eq(2)
    expect(table.rows.first[:name]).to eq("Dr. Evil")
   end

  it "can delete a row" do
    table.get_columns
    table.new_row(row1_info)
    identifier = {name: "Tester McTesterson"}

    expect(table.rows.count).to eq(1)
    table.delete_row(identifier)

    expect(table.rows.count).to eq(0)
  end

  it "can find a row using a simple query" do
    table.get_columns
    identifier = {name: "Tester McTesterson"}
    table.new_row(row1_info)
    table.new_row(row2_info)

    expect(table.find(identifier)).to eq(row1_info)
  end

  it "can filter rows using a simple query" do
    table.get_columns
    filter = {name: "Tester McTesterson"}
    table.new_row(row1_info)
    table.new_row(row2_info)
    table.new_row(row3_info)


    expect(table.where(filter).count).to eq(2)
  end

end
