import sys
import os

# Add the parent directory of 'mta' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now you can import constants
from mta.constants import LAKE_PATH

# Generate the export command for the LAKE_PATH environment variable
export_command = f'export LAKE_PATH="{LAKE_PATH}"'

# Print the command so it can be used in a shell script
print(export_command)
