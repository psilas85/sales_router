#sales_router/src/geocoding_engine/infrastructure/nominatim_client.py

import requests
import os


class NominatimClient:

    def __init__(self):

        self.url = os.getenv("NOMINATIM_LOCAL_URL")

    def geocode(self, address):

        params = {
            "q": address,
            "format": "json",
            "limit": 1
        }

        headers = {
            "User-Agent": "SalesRouter-Geocoder"
        }

        r = requests.get(
            f"{self.url}/search",
            params=params,
            headers=headers,
            timeout=5
        )

        if r.status_code != 200:
            return None

        data = r.json()

        if not data:
            return None

        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])

        return lat, lon