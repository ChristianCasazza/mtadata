# mta/resources/socrata_resource.py

import requests
from dagster import ConfigurableResource, EnvVar
from typing import Any, Dict, List


class SocrataResource(ConfigurableResource):
    """
    A reusable, configurable resource for fetching data from Socrata.
    Reads 'api_token' from an environment variable: SOCRATA_API_TOKEN.
    """

    api_token: str = EnvVar("SOCRATA_API_TOKEN")

    def fetch_data(
        self,
        endpoint: str,
        query_params: Dict[str, Any],
    ) -> List[dict]:
        """
        Perform a GET request to a Socrata endpoint with the given query params.
        Returns a list of records (dicts).
        """
        headers = {"X-App-Token": self.api_token}
        resp = requests.get(endpoint, params=query_params, headers=headers)
        resp.raise_for_status()

        # If it's a .geojson endpoint, handle differently
        if endpoint.endswith(".geojson"):
            feats = resp.json().get("features", [])
            return [feat.get("properties", {}) for feat in feats]
        else:
            return resp.json()

