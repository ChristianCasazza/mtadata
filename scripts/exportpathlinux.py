#!/usr/bin/env python
import sys
import os

# Add the parent directory of 'mta' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline.constants import WAREHOUSE_PATH, DAGSTER_PATH

# Convert any backslashes to forward slashes (just in case, on Linux it's typically forward slash)
warehouse_path = WAREHOUSE_PATH.replace("\\", "/")
dagster_path = DAGSTER_PATH.replace("\\", "/")

# Derive DAGSTER_HOME from DAGSTER_PATH
dagster_home = dagster_path

# Print LAKE_PATH on the first line
print(warehouse_path)
# Print DAGSTER_HOME on the second line
print(dagster_home)
