#sales_router/src/geocoding_engine/infrastructure/google_client.py

import requests
import os
from urllib.parse import quote


class GoogleClient:

    def __init__(self):

        self.key = os.getenv("GMAPS_API_KEY")

    def geocode(self, address):

        url = (
            "https://maps.googleapis.com/maps/api/geocode/json?"
            f"address={quote(address)}&key={self.key}"
        )

        r = requests.get(url, timeout=5)

        data = r.json()

        if data.get("status") != "OK":
            return None

        loc = data["results"][0]["geometry"]["location"]

        return loc["lat"], loc["lng"]