import sqlite3
import duckdb
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.constants import SQLITE_PATH, WAREHOUSE_PATH

# Define the DuckDB and SQLite file paths
duckdb_file_path = WAREHOUSE_PATH
sqlite_file_path = SQLITE_PATH

def delete_existing_sqlite_file():
    # Ensure the parent directory exists
    os.makedirs(os.path.dirname(sqlite_file_path), exist_ok=True)

    # Check if the SQLite file exists
    if os.path.exists(sqlite_file_path):
        os.remove(sqlite_file_path)
        print(f"Existing SQLite file at {sqlite_file_path} has been deleted.")
    else:
        print(f"No existing SQLite file found at {sqlite_file_path}.")

def create_sqlite_schema():
    # Connect to the SQLite file
    sqlite_con = sqlite3.connect(sqlite_file_path)
    sqlite_cursor = sqlite_con.cursor()

    # Create a schema table in SQLite to hold metadata from DuckDB views
    create_table_query = """
    CREATE TABLE IF NOT EXISTS duckdb_schema (
        table_name TEXT,
        column_name TEXT,
        data_type TEXT,
        is_nullable TEXT,
        column_default TEXT
    );
    """
    sqlite_cursor.execute(create_table_query)
    sqlite_con.commit()
    sqlite_con.close()
    print("SQLite schema table created.")

def extract_pragma_to_sqlite():
    # Connect to DuckDB and SQLite
    duckdb_con = duckdb.connect(duckdb_file_path)
    sqlite_con = sqlite3.connect(sqlite_file_path)
    sqlite_cursor = sqlite_con.cursor()

    try:
        # Get the list of all views in the DuckDB database
        views_query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW';"
        views = duckdb_con.execute(views_query).fetchall()

        for view in views:
            view_name = view[0]

            # Run PRAGMA on each DuckDB view to get table info
            pragma_query = f"PRAGMA table_info({view_name});"
            pragma_result = duckdb_con.execute(pragma_query).fetchall()

            # Insert PRAGMA results into the SQLite table
            for column in pragma_result:
                column_name = column[1]
                data_type = column[2]
                is_nullable = 'YES' if column[3] == 0 else 'NO'
                column_default = column[4] or 'NULL'

                insert_query = """
                INSERT INTO duckdb_schema (table_name, column_name, data_type, is_nullable, column_default)
                VALUES (?, ?, ?, ?, ?);
                """
                sqlite_cursor.execute(insert_query, (view_name, column_name, data_type, is_nullable, column_default))

        sqlite_con.commit()
        print("PRAGMA information inserted into SQLite.")
    
    finally:
        # Close connections
        duckdb_con.close()
        sqlite_con.close()
        print("Connections to DuckDB and SQLite closed.")

if __name__ == "__main__":
    # Step 1: Check and delete existing SQLite file if present
    delete_existing_sqlite_file()

    # Step 2: Create the schema table in SQLite
    create_sqlite_schema()

    # Step 3: Extract DuckDB PRAGMA info and upload to SQLite
    extract_pragma_to_sqlite()
