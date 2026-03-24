#sales_router/src/geocoding_engine/application/geocode_addresses_use_case.py

import uuid
import time

from loguru import logger

from geocoding_engine.services.geolocation_service import GeolocationService
from geocoding_engine.infrastructure.database_reader import DatabaseReader
from geocoding_engine.infrastructure.database_writer import DatabaseWriter
from geocoding_engine.infrastructure.geocoding_history_repository import GeocodingHistoryRepository

from geocoding_engine.domain.address_normalizer import (
    normalize_base,
    normalize_for_cache
)

from geocoding_engine.domain.geo_validator import GeoValidator


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

        total = len(addresses)

        # =====================================================
        # 🔥 PRÉ-PROCESSAMENTO
        # =====================================================
        enderecos = []
        meta_info = []

        for item in addresses:
            endereco = item.get("address")
            cidade = item.get("cidade")
            uf = item.get("uf")

            enderecos.append(endereco)
            meta_info.append({
                "id": item["id"],
                "cidade": cidade,
                "uf": uf,
                "endereco": endereco
            })

        # =====================================================
        # 🚀 GEOCODING EM LOTE
        # =====================================================
        resultados_batch = self.service.geocode_batch(enderecos)

        # =====================================================
        # 🔄 PROCESSAMENTO DOS RESULTADOS
        # =====================================================
        for i, meta in enumerate(meta_info, start=1):

            if i % 100 == 0 or i == total:
                logger.info(
                    f"[GEOCODE_PROGRESS] {i}/{total} endereços processados"
                )

            lat, lon, source = resultados_batch.get(i - 1, (None, None, "falha"))

            endereco = meta["endereco"]
            cidade = meta["cidade"]
            uf = meta["uf"]

            # -------------------------------------------------
            # VALIDAÇÃO GEOGRÁFICA (mantida)
            # -------------------------------------------------
            if lat is not None and lon is not None:

                status_geo = GeoValidator.validar_ponto(
                    lat,
                    lon,
                    cidade,
                    uf
                )

                if status_geo != "ok":

                    logger.warning(
                        f"[GEOCODE_INVALIDO] "
                        f"{endereco} → {lat},{lon} status={status_geo}"
                    )

                    lat = None
                    lon = None
                    source = "invalid_geo"

            # -------------------------------------------------
            # SALVAR CACHE (mantido)
            # -------------------------------------------------
            if lat and lon and source != "cache":

                endereco_base = normalize_base(endereco)
                cache_key = normalize_for_cache(endereco_base)

                self.writer.salvar_cache(
                    cache_key,
                    lat,
                    lon,
                    source
                )

            # -------------------------------------------------
            # CONTADORES
            # -------------------------------------------------
            if source == "cache":
                cache_hits += 1

            elif "nominatim" in str(source):
                nominatim_hits += 1

            elif source == "google":
                google_hits += 1

            if lat is None:
                falhas += 1

            # -------------------------------------------------
            # RESULTADO FINAL
            # -------------------------------------------------
            results.append({
                "id": meta["id"],
                "lat": lat,
                "lon": lon,
                "source": source
            })

        # =====================================================
        # FINALIZAÇÃO
        # =====================================================
        tempo_ms = int((time.time() - start) * 1000)
        sucesso = len(addresses) - falhas

        logger.info(
            f"[GEOCODE_END] request_id={request_id} "
            f"total={len(addresses)} "
            f"sucesso={sucesso} "
            f"falhas={falhas} "
            f"cache_hits={cache_hits} "
            f"nominatim_hits={nominatim_hits} "
            f"google_hits={google_hits} "
            f"tempo_ms={tempo_ms}"
        )

        # -------------------------------------------------
        # HISTÓRICO
        # -------------------------------------------------
        if origem != "subjob":
            self.history_repo.salvar(
                request_id=request_id,
                tenant_id=tenant_id,
                origem=origem,
                total=len(addresses),
                sucesso=sucesso,
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