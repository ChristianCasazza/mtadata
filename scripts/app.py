
from flask import Flask, render_template, jsonify, request
import sqlite3
import os
import sys
import os
# Add the parent directory to the module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.constants import SQLITE_PATH
# Correct path to the 'templates' folder relative to the 'scripts' directory
app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))

# Path to your SQLite file
sqlite_file_path = SQLITE_PATH

# Function to get column names, data types, and descriptions from a specific table
def get_table_metadata(table_name):
    conn = sqlite3.connect(sqlite_file_path)
    cursor = conn.cursor()
    
    query = """
    SELECT column_name, data_type, description
    FROM duckdb_schema
    WHERE table_name = ?;
    """
    
    cursor.execute(query, (table_name,))
    rows = cursor.fetchall()
    conn.close()
    
    # Structure the result as both human-readable and LLM-optimized
    metadata_human = []
    metadata_llm = f"Table: {table_name}\n"
    
    for row in rows:
        column_name, data_type, description = row
        # Human-readable version
        metadata_human.append({
            "column_name": column_name,
            "data_type": data_type,
            "description": description or "No description available"
        })
        
        # LLM-optimized version (minimal whitespace, compact format)
        metadata_llm += f"{column_name}: {data_type}, {description or 'No description'}; "
    
    return metadata_human, metadata_llm

# Main route to show the list of tables
@app.route('/')
def index():
    conn = sqlite3.connect(sqlite_file_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT table_name FROM duckdb_schema;")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return render_template('index.html', tables=tables)

# Route to get metadata for a specific table
@app.route('/table/<table_name>')
def table_metadata(table_name):
    metadata_human, metadata_llm = get_table_metadata(table_name)
    return jsonify({
        "human": metadata_human,
        "llm": metadata_llm
    })

if __name__ == '__main__':
    app.run(debug=False, port=5050)
