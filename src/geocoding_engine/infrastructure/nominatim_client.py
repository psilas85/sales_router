#sales_router/src/geocoding_engine/infrastructure/nominatim_client.py

import requests
import os
import time
from loguru import logger


class NominatimClient:

    def __init__(self):

        self.url = os.getenv("NOMINATIM_LOCAL_URL")
        self.timeout = 5
        self.max_retries = 2

        if not self.url:
            logger.warning("[NOMINATIM] URL não configurada")

    # ---------------------------------------------------------
    # GEOCODE
    # ---------------------------------------------------------
    def geocode(self, address):

        if not address:
            return None

        params = {
            "q": address,
            "format": "json",
            "limit": 1,
            "addressdetails": 1
        }

        headers = {
            "User-Agent": "SalesRouter-Geocoder"
        }

        for attempt in range(self.max_retries + 1):

            try:
                r = requests.get(
                    f"{self.url}/search",
                    params=params,
                    headers=headers,
                    timeout=self.timeout
                )

                if r.status_code != 200:
                    logger.warning(f"[NOMINATIM][HTTP_{r.status_code}] {address}")
                    continue

                data = r.json()

                if not data:
                    logger.info(f"[NOMINATIM][MISS] {address}")
                    return None

                item = data[0]

                lat = float(item["lat"])
                lon = float(item["lon"])

                tipo = item.get("type")
                addresstype = item.get("addresstype")

                logger.info(
                    f"[NOMINATIM][HIT] lat={lat} lon={lon} type={tipo} addresstype={addresstype}"
                )

                return lat, lon, tipo, addresstype

            except Exception as e:
                logger.warning(f"[NOMINATIM][ERRO] tentativa={attempt} {e}")
                time.sleep(0.5)

        return None