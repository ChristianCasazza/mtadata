import os

# Get the absolute path to the root of the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Relative path to the DuckDB file
LAKE_PATH = os.path.join(BASE_DIR, 'mta', 'mtastats', 'sources', 'mta', 'mtastats.duckdb')

# Relative path to the SQLite file at the root of the project
SQLITE_PATH = os.path.join(BASE_DIR, 'metadata.db')
