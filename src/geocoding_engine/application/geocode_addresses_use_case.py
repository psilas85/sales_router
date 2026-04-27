#sales_router/src/geocoding_engine/application/geocode_addresses_use_case.py

import uuid
import time
import pandas as pd
from loguru import logger

from geocoding_engine.services.geolocation_service import GeolocationService
from geocoding_engine.infrastructure.database_reader import DatabaseReader
from geocoding_engine.infrastructure.database_writer import DatabaseWriter
from geocoding_engine.domain.cache_key_builder import build_cache_key
from geocoding_engine.domain.geo_validator import GeoValidator, validar_municipios_batch_fast


class GeocodeAddressesUseCase:

    def __init__(self):
        self.reader = DatabaseReader()
        self.writer = DatabaseWriter(self.reader.conn)
        self.service = GeolocationService(reader=self.reader)

    def execute(self, addresses, tenant_id=1, origem="api", persist_cache=True, validate_polygon=True):

        start = time.time()
        request_id = str(uuid.uuid4())

        logger.info(f"[GEOCODE_START] total={len(addresses)}")

        # =====================================================
        # 🔹 ENRICH + CHAVE PADRÃO
        # =====================================================
        enriched = []
        skipped_results = []

        for idx, item in enumerate(addresses):

            logradouro = item.get("logradouro") or item.get("endereco")
            numero = item.get("numero") or ""
            cidade = item.get("cidade")
            uf = item.get("uf")
            cep = item.get("cep")
            raw = item.get("address")

            if not logradouro or not cidade or not uf:
                skipped_results.append({
                    "id": item.get("id", idx),
                    "lat": None,
                    "lon": None,
                    "source": "invalid_input"
                })
                continue

            if not raw:
                partes_rua = " ".join(
                    p for p in [str(logradouro).strip(), str(numero).strip()] if p
                )
                bairro = item.get("bairro")
                bairro_part = f", {bairro}" if bairro else ""
                raw = f"{partes_rua}{bairro_part}, {cidade} - {uf}"

            cache_key = build_cache_key(
                logradouro,
                numero,
                cidade,
                uf,
            )

            enriched.append({
                "id": item["id"],
                "logradouro": logradouro,
                "numero": numero,
                "cidade": cidade,
                "uf": uf,
                "cep": cep,
                "raw": raw,
                "cache_key": cache_key
            })

        # =====================================================
        # 🔹 DEDUP
        # =====================================================
        unique_map = {}

        for e in enriched:
            unique_map[e["cache_key"]] = e

        unique_list = list(unique_map.values())

        logger.info(f"[DEDUP] {len(enriched)} → {len(unique_list)} únicos")

        # =====================================================
        # 🔹 CACHE LOTE
        # =====================================================
        cache_keys = [e["cache_key"] for e in unique_list]

        cache_map = self.reader.buscar_cache_em_lote(cache_keys) or {}

        logger.info(f"[CACHE_LOTE] encontrados={len(cache_map)}")

        # =====================================================
        # 🔹 APPLY CACHE
        # =====================================================
        result_map = {}
        to_geocode = []
        cache_candidates = []
        cache_candidate_keys = set()

        cache_hits = 0
        nominatim_hits = 0
        google_hits = 0
        falhas = len(skipped_results)

        for e in unique_list:

            key = e["cache_key"]

            if key in cache_map:

                lat, lon = cache_map[key]

                if lat is not None and lon is not None:
                    if GeoValidator.validar_ponto(lat, lon, e["cidade"], e["uf"]) == "ok":
                        cache_candidates.append({
                            "cache_key": key,
                            "id": e["id"],
                            "cidade": e["cidade"],
                            "uf": e["uf"],
                            "lat": lat,
                            "lon": lon,
                        })
                        cache_candidate_keys.add(key)
                        continue

            to_geocode.append(e)

        if cache_candidates:
            df_cache = pd.DataFrame(cache_candidates)
            df_cache = validar_municipios_batch_fast(df_cache, None)

            valid_cache_keys = set(
                df_cache.loc[df_cache["valido_municipio"], "cache_key"].astype(str)
            )

            for row in df_cache[df_cache["valido_municipio"]].itertuples(index=False):
                result_map[row.cache_key] = (row.lat, row.lon, "cache")
                cache_hits += 1

            for e in unique_list:
                key = e["cache_key"]
                if key in cache_candidate_keys and key not in valid_cache_keys:
                    to_geocode.append(e)

        logger.info(f"[CACHE] hits={cache_hits} miss={len(to_geocode)}")

        # =====================================================
        # 🔹 GEOCODING
        # =====================================================
        if to_geocode:

            # 🔥 mapa para lookup O(1)
            map_to_geocode = {e["cache_key"]: e for e in to_geocode}

            payload = [
                {
                    "id": e["id"],
                    "address": e["raw"],
                    "cidade": e["cidade"],
                    "uf": e["uf"],
                    "cep": e["cep"],
                    "cache_key": e["cache_key"]
                }
                for e in to_geocode
            ]

            results_batch = self.service.geocode_batch_enriched(
                payload,
                validate_polygon=validate_polygon,
            )

            for r in results_batch:

                key = r["cache_key"]

                lat = r["lat"]
                lon = r["lon"]
                source = r["source"]
                is_valid = r["valid"]

                if is_valid and lat is not None and lon is not None:

                    result_map[key] = (lat, lon, source)

                    if persist_cache:
                        try:
                            orig = map_to_geocode.get(key)

                            if orig and source != "fallback_cidade":

                                endereco_padrao = (
                                    f"{orig['logradouro']} {orig['numero']}, "
                                    f"{orig['cidade']} - {orig['uf']}"
                                ).replace(" ,", ",").strip()

                                self.writer.salvar_cache(
                                    logradouro=orig["logradouro"],
                                    numero=orig["numero"],
                                    cidade=orig["cidade"],
                                    uf=orig["uf"],
                                    endereco_original=endereco_padrao,
                                    lat=lat,
                                    lon=lon,
                                    origem=source
                                )

                        except Exception as e:
                            logger.warning(f"[CACHE_SAVE_FAIL] {e}")

                    if "nominatim" in str(source):
                        nominatim_hits += 1
                    elif source == "google":
                        google_hits += 1

                else:
                    result_map[key] = (None, None, "falha")
                    falhas += 1

        # =====================================================
        # 🔹 EXPANSÃO FINAL
        # =====================================================
        results = list(skipped_results)

        for e in enriched:

            key = e["cache_key"]

            lat, lon, source = result_map.get(
                key,
                (None, None, "falha")
            )

            results.append({
                "id": e["id"],
                "lat": lat,
                "lon": lon,
                "source": source
            })

        # =====================================================
        # 🔹 STATS
        # =====================================================
        total = len(addresses)
        tempo_ms = int((time.time() - start) * 1000)

        logger.info(
            f"[GEOCODE_END] total={total} "
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
