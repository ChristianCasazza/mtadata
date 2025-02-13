import sys
import os

# Add the parent directory of 'mta' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the LAKE_PATH constant
from mta.constants import LAKE_PATH

# Print the path exactly as it is (Linux/macOS uses forward slashes by default)
print(LAKE_PATH)
