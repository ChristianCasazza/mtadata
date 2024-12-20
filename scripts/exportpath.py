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

# Generate the export command for the environment variable
if is_windows:
    # Windows command (for PowerShell)
    export_command = f'$env:LAKE_PATH="{lake_path}"'
else:
    # Linux/macOS command
    export_command = f'export LAKE_PATH="{lake_path}"'

# Print the command so it can be used in a shell script
print(export_command)
