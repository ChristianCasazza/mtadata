import sys
import os

# Add the parent directory of 'mta' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline.constants import WAREHOUSE_PATH, DAGSTER_PATH

# Ensure forward slashes are used on Windows
warehouse_path = WAREHOUSE_PATH.replace("\\", "/")
dagster_path = DAGSTER_PATH.replace("\\", "/")

# For Windows, derive DAGSTER_HOME from DAGSTER_PATH
dagster_home = dagster_path

# Print WAREHOUSE_PATH on the first line
print(warehouse_path)
# Print DAGSTER_HOME on the second line
print(dagster_home)
