import os
import requests
from dotenv import load_dotenv
from dagster import Config
from pydantic import Field

# Load environment variables
load_dotenv()

class SocrataAPIConfig(Config):
    SOCRATA_ENDPOINT: str = Field(..., description="Socrata API endpoint URL")
    order: str = Field(..., description="Field to order the API results by")
    limit: int = Field(50000, description="The maximum number of records to retrieve in one call")
    where: str = Field(None, description="Optional filter condition for the API call")
    offset: int = Field(0, description="Offset for paginated API calls")

class SocrataAPI:
    def __init__(self, config: SocrataAPIConfig):
        self.config = config
        self.api_token = os.getenv('SOCRATA_API_TOKEN')

    def fetch_data(self):
        headers = {'X-App-Token': self.api_token}

        # Build query parameters from config
        params = {
            '$limit': self.config.limit,
            '$offset': self.config.offset,
            '$order': self.config.order
        }
        if self.config.where:
            params['$where'] = self.config.where

        # Make the request
        response = requests.get(self.config.SOCRATA_ENDPOINT, params=params, headers=headers)
        response.raise_for_status()

        # Determine if the response is GeoJSON or regular JSON based on the endpoint extension
        if self.config.SOCRATA_ENDPOINT.endswith('.geojson'):
            return self._handle_geojson_response(response.json())
        else:
            return self._handle_json_response(response.json())

    def _handle_geojson_response(self, data):
        # GeoJSON-specific processing: extract features and properties
        features = data.get('features', [])
        processed_data = []

        for feature in features:
            # Extract only the 'properties' field from each feature
            properties = feature.get('properties', {})
            processed_data.append(properties)

        return processed_data

    def _handle_json_response(self, data):
        # Regular JSON processing: return the data as-is, assuming it's a list of records
        return data