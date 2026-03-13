#sales_router/src/geocoding_engine/application/geocode_addresses_use_case.py

import uuid
import time

from loguru import logger

from geocoding_engine.services.geolocation_service import GeolocationService
from geocoding_engine.infrastructure.database_reader import DatabaseReader
from geocoding_engine.infrastructure.database_writer import DatabaseWriter
from geocoding_engine.infrastructure.geocoding_history_repository import GeocodingHistoryRepository
from geocoding_engine.domain.address_normalizer import normalize_base, normalize_for_cache


class GeocodeAddressesUseCase:

    def __init__(self):

        self.reader = DatabaseReader()
        self.writer = DatabaseWriter(self.reader.conn)

        self.service = GeolocationService(
            reader=self.reader
        )

        self.history_repo = GeocodingHistoryRepository(self.reader.conn)


    def execute(self, addresses, tenant_id=1, origem="api"):

        start = time.time()

        request_id = str(uuid.uuid4())

        logger.info(
            f"[GEOCODE_START] request_id={request_id} total_enderecos={len(addresses)}"
        )

        results = []

        cache_hits = 0
        nominatim_hits = 0
        google_hits = 0
        falhas = 0

        resolved = {}

        total = len(addresses)

        for i, item in enumerate(addresses, start=1):

            # progresso a cada 100 registros
            if i % 100 == 0 or i == total:
                logger.info(
                    f"[GEOCODE_PROGRESS] {i}/{total} endereços processados"
                )

            endereco_base = normalize_base(item["address"])
            cache_key = normalize_for_cache(endereco_base)

            if cache_key in resolved:

                lat, lon, source = resolved[cache_key]

            else:

                lat, lon, source = self.service.geocode(item["address"])

                if lat and lon and source != "cache":

                    self.writer.salvar_cache(
                        cache_key,
                        lat,
                        lon,
                        source
                    )

                resolved[cache_key] = (lat, lon, source)

            if source == "cache":
                cache_hits += 1

            elif "nominatim" in source:
                nominatim_hits += 1

            elif source == "google":
                google_hits += 1

            if not lat:
                falhas += 1

            results.append({
                "id": item["id"],
                "lat": lat,
                "lon": lon,
                "source": source
            })

        tempo_ms = int((time.time() - start) * 1000)

        logger.info(
            f"[GEOCODE_END] request_id={request_id} "
            f"total={len(addresses)} "
            f"sucesso={len(addresses) - falhas} "
            f"falhas={falhas} "
            f"cache_hits={cache_hits} "
            f"nominatim_hits={nominatim_hits} "
            f"google_hits={google_hits} "
            f"tempo_ms={tempo_ms}"
        )

        self.history_repo.salvar(
            request_id=request_id,
            tenant_id=tenant_id,
            origem=origem,
            total=len(addresses),
            sucesso=len(addresses) - falhas,
            falhas=falhas,
            cache_hits=cache_hits,
            nominatim_hits=nominatim_hits,
            google_hits=google_hits,
            tempo_ms=tempo_ms
        )

        return {
            "request_id": request_id,
            "results": results
        }