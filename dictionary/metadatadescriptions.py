import sqlite3
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# import constants and our asset descriptions
from pipeline.constants import SQLITE_PATH
from pipeline.assets.assets_descriptions import table_descriptions

# Function to add descriptions for a specific table
def update_descriptions_for_table(conn, table_name, descriptions):
    cursor = conn.cursor()
    for column_name, description in descriptions.items():
        update_query = """
        UPDATE duckdb_schema
        SET description = ?
        WHERE table_name = ? AND column_name = ?;
        """
        cursor.execute(update_query, (description, table_name, column_name))
    conn.commit()

# Ensure the "description" column exists in duckdb_schema table
def ensure_description_column_exists(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER TABLE duckdb_schema ADD COLUMN description TEXT;")
    except sqlite3.OperationalError as e:
        print(f"Error (likely column exists): {e}")

# Main function to update descriptions for all tables
def main():
    # Path to your SQLite file
    sqlite_file_path = SQLITE_PATH

    # Connect to SQLite
    conn = sqlite3.connect(sqlite_file_path)

    # Ensure the "description" column exists in duckdb_schema table
    ensure_description_column_exists(conn)

    # Dynamically update descriptions for all tables
    for table_name, descriptions in table_descriptions.items():
        print(f"Updating descriptions for table: {table_name}")
        update_descriptions_for_table(conn, table_name, descriptions)

    # Close the connection
    conn.close()

    print("Descriptions updated for all tables.")

# Run the script
if __name__ == "__main__":
    main()
