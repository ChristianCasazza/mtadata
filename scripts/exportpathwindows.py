import sys
import os

# Add the parent directory of 'mta' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the LAKE_PATH constant
from mta.constants import LAKE_PATH

# Ensure forward slashes are used on Windows
lake_path = LAKE_PATH.replace("\\", "/")

# Print only the corrected path
print(lake_path)
