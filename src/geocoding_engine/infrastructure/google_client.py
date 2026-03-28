#sales_router/src/geocoding_engine/infrastructure/google_client.py

import requests
import os
import time
from urllib.parse import quote
from loguru import logger


class GoogleClient:

    def __init__(self):

        self.key = os.getenv("GMAPS_API_KEY")
        self.base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        self.timeout = 5
        self.max_retries = 2

        if not self.key:
            logger.warning("[GOOGLE] API KEY não configurada")

    # ---------------------------------------------------------
    # GEOCODE
    # ---------------------------------------------------------
    def geocode(self, address):

        if not address:
            return None

        url = f"{self.base_url}?address={quote(address)}&key={self.key}"

        for attempt in range(self.max_retries + 1):

            try:
                r = requests.get(url, timeout=self.timeout)

                if r.status_code != 200:
                    logger.warning(f"[GOOGLE][HTTP_{r.status_code}] {address}")
                    continue

                data = r.json()

                status = data.get("status")

                # -------------------------------------------------
                # OK
                # -------------------------------------------------
                if status == "OK":

                    result = data["results"][0]

                    loc = result["geometry"]["location"]
                    location_type = result["geometry"].get("location_type")

                    logger.info(
                        f"[GOOGLE][HIT] lat={loc['lat']} lon={loc['lng']} type={location_type}"
                    )

                    return loc["lat"], loc["lng"]

                # -------------------------------------------------
                # ZERO RESULTS (não é erro)
                # -------------------------------------------------
                if status == "ZERO_RESULTS":
                    logger.info(f"[GOOGLE][ZERO_RESULTS] {address}")
                    return None

                # -------------------------------------------------
                # RATE LIMIT
                # -------------------------------------------------
                if status == "OVER_QUERY_LIMIT":
                    logger.warning("[GOOGLE][RATE_LIMIT] aguardando...")
                    time.sleep(1)
                    continue

                # -------------------------------------------------
                # ERROS CRÍTICOS
                # -------------------------------------------------
                if status in ["REQUEST_DENIED", "INVALID_REQUEST"]:
                    logger.error(f"[GOOGLE][ERRO] {status} - {address}")
                    return None

                logger.warning(f"[GOOGLE][STATUS_{status}] {address}")

            except Exception as e:
                logger.warning(f"[GOOGLE][ERRO] tentativa={attempt} {e}")
                time.sleep(0.5)

        return None