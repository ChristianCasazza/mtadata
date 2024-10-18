import os
import yaml
from mta.assets.ingestion.mta_subway.mta_assets import MTA_ASSETS_NAMES
from mta.assets.ingestion.weather.weather_assets import WEATHER_ASSETS_NAMES

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

# Define the path to the sources.yml file
output_file_path = 'mta/transformations/dbt/models/sources.yml'

# Ensure the directory exists, create if it doesn't
os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

# Write the YAML structure to the file, replacing the file if it exists
with open(output_file_path, 'w') as file:
    yaml.dump(sources_structure, file, sort_keys=False)

print(f'sources.yml has been created/updated at {output_file_path}')
