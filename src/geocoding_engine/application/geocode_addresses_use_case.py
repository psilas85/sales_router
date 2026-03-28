#sales_router/src/geocoding_engine/application/geocode_addresses_use_case.py

import uuid
import time
from loguru import logger

from geocoding_engine.services.geolocation_service import GeolocationService
from geocoding_engine.infrastructure.database_reader import DatabaseReader
from geocoding_engine.infrastructure.database_writer import DatabaseWriter

from geocoding_engine.domain.address_normalizer import normalize_for_cache


class GeocodeAddressesUseCase:

    def __init__(self):
        self.reader = DatabaseReader()
        self.writer = DatabaseWriter(self.reader.conn)
        self.service = GeolocationService(reader=self.reader)

    # =====================================================
    # 🚀 EXECUTE
    # =====================================================
    def execute(self, addresses, tenant_id=1, origem="api"):

        start = time.time()
        request_id = str(uuid.uuid4())

        logger.info(f"[GEOCODE_V5_START] total={len(addresses)}")

        # -------------------------------------------------
        # 🔹 PRÉ-PROCESSAMENTO
        # -------------------------------------------------
        enriched = []

        for item in addresses:

            raw = item.get("address")
            cidade = item.get("cidade")
            uf = item.get("uf")
            cep = item.get("cep")

            if not raw:
                continue

            cache_key = normalize_for_cache(raw, cep=cep)

            enriched.append({
                "id": item["id"],
                "raw": raw,
                "cache_key": cache_key,
                "cidade": cidade,
                "uf": uf,
                "cep": cep
            })

        # -------------------------------------------------
        # 🔹 DEDUP (CORRIGIDO)
        # -------------------------------------------------
        unique_map = {}

        for e in enriched:
            dedup_key = e["cache_key"]   # 🔥 FIX
            unique_map[dedup_key] = e

        unique_list = [
            {**v, "dedup_key": k}
            for k, v in unique_map.items()
        ]

        logger.info(f"[DEDUP] {len(enriched)} → {len(unique_list)} únicos")

        # -------------------------------------------------
        # 🔹 RESULTADOS
        # -------------------------------------------------
        result_map = {}

        cache_hits = 0
        nominatim_hits = 0
        google_hits = 0
        falhas = 0

        # -------------------------------------------------
        # 🔹 CACHE EM LOTE
        # -------------------------------------------------
        cache_keys = [e["cache_key"] for e in unique_list]

        try:
            cache_map = self.reader.buscar_cache_em_lote(cache_keys)
        except Exception as e:
            logger.warning(f"[CACHE_LOTE][ERRO] {e}")
            cache_map = {}

        to_geocode = []

        for e in unique_list:

            cache_key = e["cache_key"]
            dedup_key = e["dedup_key"]

            if cache_key in cache_map:

                lat, lon = cache_map[cache_key]

                if lat is not None and lon is not None:
                    result_map[dedup_key] = (lat, lon, "cache")
                    cache_hits += 1
                    continue

            to_geocode.append(e)

        # -------------------------------------------------
        # 🔹 GEOCODING
        # -------------------------------------------------
        if to_geocode:

            payload = [
                {
                    "id": e["id"],
                    "address": e["raw"],
                    "cidade": e["cidade"],
                    "uf": e["uf"],
                    "cep": e["cep"],
                    "cache_key": e["cache_key"]
                }
                for e in to_geocode   # 🔥 FIX (faltava loop)
            ]

            res = self.service.geocode_batch_enriched(payload)

            if isinstance(res, dict):
                results_batch = res.get("results", [])
            elif isinstance(res, list):
                results_batch = res
            else:
                logger.error(f"[ERRO_BATCH_FORMAT] tipo inesperado: {type(res)}")
                results_batch = []

            if not isinstance(results_batch, list):
                logger.error(f"[ERRO_BATCH_FORMAT] {type(results_batch)}")
                results_batch = []

            for r in results_batch:

                dedup_key = r["cache_key"]  # 🔥 FIX

                lat = r["lat"]
                lon = r["lon"]
                source = r["source"]
                is_valid = r["valid"]

                logger.info(
                    f"[PIPE_RESULT] lat={lat} lon={lon} source={source} valid={is_valid} tipo_lat={type(lat)} tipo_lon={type(lon)}"
                )
                if is_valid and lat is not None and lon is not None:

                    result_map[dedup_key] = (lat, lon, source)

                    if source == "cache":
                        cache_hits += 1
                    elif "nominatim" in str(source):
                        nominatim_hits += 1
                    elif source == "google":
                        google_hits += 1

                    try:
                        self.writer.salvar_cache(
                            r["cache_key"],
                            lat,
                            lon,
                            source
                        )
                    except Exception as err:
                        logger.warning(f"[CACHE_SAVE_ERRO] {err}")

                else:
                    result_map[dedup_key] = (None, None, "falha")
                    falhas += 1

        # -------------------------------------------------
        # 🔹 EXPANSÃO (CORRIGIDO)
        # -------------------------------------------------
        results = []

        for e in enriched:

            dedup_key = e["cache_key"]  # 🔥 FIX

            lat, lon, source = result_map.get(
                dedup_key,
                (None, None, "falha")
            )

            results.append({
                "id": e["id"],
                "lat": lat,
                "lon": lon,
                "source": source
            })

        # -------------------------------------------------
        # 🔹 STATS
        # -------------------------------------------------
        total = len(addresses)
        tempo_ms = int((time.time() - start) * 1000)

        logger.info(
            f"[GEOCODE_V5_END] total={total} "
            f"cache={cache_hits} "
            f"nominatim={nominatim_hits} "
            f"google={google_hits} "
            f"falhas={falhas} "
            f"tempo_ms={tempo_ms}"
        )

        return {
            "request_id": request_id,
            "results": results,
            "stats": {
                "total": total,
                "cache_hits": cache_hits,
                "nominatim_hits": nominatim_hits,
                "google_hits": google_hits,
                "falhas": falhas,
                "tempo_ms": tempo_ms
            }
        }