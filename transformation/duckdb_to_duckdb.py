import duckdb
import os

# Get the directory of the current script (repo root)
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct relative paths based on the script location
original_duckdb_file = os.path.join(script_dir, '../my_shopify.duckdb')
query_folder = os.path.join(script_dir, '../transformation/sql')
new_duckdb_file = os.path.join(script_dir, '../shopify/sources/shopify/analytics.duckdb')

# Check if the new DuckDB file exists, and delete if necessary
if os.path.exists(new_duckdb_file):
    os.remove(new_duckdb_file)

# Connect to the original DuckDB file (to read data) and the new DuckDB file (to store results)
original_conn = duckdb.connect(original_duckdb_file)
new_conn = duckdb.connect(new_duckdb_file)

# Get all SQL files in the query folder
query_files = [f for f in os.listdir(query_folder) if f.endswith('.sql')]

# Loop through each SQL query file and execute them on the original DuckDB file
for query_file in query_files:
    # Read SQL query from file
    with open(os.path.join(query_folder, query_file), 'r') as file:
        query = file.read()

    # Derive table name from file name (removing '.sql' extension)
    table_name = query_file.replace('.sql', '')

    # Run the query on the original DuckDB file and store the result in the new DuckDB file
    result_df = original_conn.execute(query).fetchdf()
    new_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM result_df")

# Close connections
original_conn.close()
new_conn.close()

print("Queries have been executed and results stored in the new DuckDB file.")
