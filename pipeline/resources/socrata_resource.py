# pipeline/resources/socrata_resource.py

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
        Retries certain transient errors automatically.
        Returns a list of records (dicts).
        """

        # Create a retry strategy that will retry on certain connection or status errors
        retry_strategy = Retry(
            total=5,                # Max number of retries
            backoff_factor=1,       # Exponential backoff: 1s, 2s, 4s, etc.
            status_forcelist=[429, 500, 502, 503, 504],
            # Note: "allowed_methods" is only available in urllib3 >= 1.26. By default, it includes
            # GET, PUT, PATCH, DELETE, HEAD, OPTIONS. If you need to allow or restrict methods, do so here.
            allowed_methods=["GET"],
        )

        # Create a custom HTTP adapter with our retry strategy
        adapter = HTTPAdapter(max_retries=retry_strategy)

        # Build a session and mount the adapter
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        headers = {"X-App-Token": self.api_token}

        try:
            resp = session.get(endpoint, params=query_params, headers=headers)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            # You may want more granular error handling/logging here
            raise e
        finally:
            # Always close the session to free up resources
            session.close()

        # If it's a .geojson endpoint, handle differently
        if endpoint.endswith(".geojson"):
            feats = resp.json().get("features", [])
            return [feat.get("properties", {}) for feat in feats]
        else:
            return resp.json()
