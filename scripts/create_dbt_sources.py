import os
import yaml
import sys

# Add the root of the project to the system path to resolve imports from the mta module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline.datasets import MTA_ASSETS_NAMES
from pipeline.datasets import WEATHER_ASSETS_NAMES

# Combine MTA and Weather asset names
all_assets = MTA_ASSETS_NAMES + WEATHER_ASSETS_NAMES

# Create the structure for the sources.yml
sources_structure = {
    'version': 2,
    'sources': [
        {
            'name': 'main',
            'tables': [
                {
                    'name': asset_name,
                    'meta': {
                        'dagster': {
                            'asset_key': [asset_name]
                        }
                    }
                } for asset_name in all_assets
            ]
        }
    ]
}

# Define the path to the sources.yml file, updated for the new folder structure
output_file_path = os.path.join(os.path.dirname(__file__), '..', 'mta', 'transformations', 'dbt', 'models', 'sources.yml')

# Ensure the directory exists, create if it doesn't
os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

# Write the YAML structure to the file, replacing the file if it exists
with open(output_file_path, 'w') as file:
    yaml.dump(sources_structure, file, sort_keys=False)

print(f'sources.yml has been created/updated at {output_file_path}')
