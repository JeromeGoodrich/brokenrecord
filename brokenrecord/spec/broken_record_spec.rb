require 'rspec'
require 'sqlite3'
require 'broken_record'


describe BrokenRecord::Table do

  let (:test_db) { SQLite3::Database.new(":memory:") }
  let (:table)  { BrokenRecord::Table.new({ name: "New_Table",db: test_db }) }

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

  it "it is able create new rows and get their info" do
    table.get_columns
    params = { id: "1", name: "row1"}

    table.new_row(params)

    expect(table.rows.count).to eq(1)

    #need to figure out how to make a hash out of the 2-ple that the SQLite code gives me
    #expect(table.rows.first[:id]).to eq("1")
    # expect(record[:name]).to eq("row1")
  end


end
