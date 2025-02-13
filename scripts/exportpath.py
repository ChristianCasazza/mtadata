import sys
import os

# Add the parent directory of 'mta' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now you can import constants
from mta.constants import LAKE_PATH

# Check if the user provided "windows" as an argument
is_windows = len(sys.argv) > 1 and sys.argv[1].lower() == "windows"

# Ensure forward slashes are used if "windows" argument is provided
lake_path = LAKE_PATH.replace('\\', '/').replace('\\\\', '/') if is_windows else LAKE_PATH

# Print only the path (no export command)
print(lake_path)
