# sales_router/src/routing_engine/visualization/map_use_case.py

import json

from routing_engine.visualization.map_repository import MapRepository
from routing_engine.visualization.geojson_builder import GeoJSONBuilder


class GenerateMapUseCase:

    def execute(self, json_path):

        repo = MapRepository()

        raw_data = repo.load_job_json(json_path)

        # ======================================================
        # 🔥 DEBUG RAW
        # ======================================================
        print("\n================ RAW_DATA SAMPLE ================")

        if isinstance(raw_data, list):
            for r in raw_data[:5]:
                print(r)
        else:
            print(raw_data)

        print("=================================================\n")

        # ======================================================
        # 🔥 GARANTE FORMATO LISTA
        # ======================================================
        if isinstance(raw_data, dict):
            raw_data = (
                raw_data.get("data")
                or raw_data.get("rotas")
                or raw_data.get("results")
                or []
            )

        if not isinstance(raw_data, list):
            print("⚠️ ERRO: raw_data não é lista")
            return {"type": "FeatureCollection", "features": []}

        # ======================================================
        # 🔥 AQUI É O FIX
        # ======================================================
        rotas = raw_data

        print(f"ROTAS RECEBIDAS: {len(rotas)}")

        # ======================================================
        # 🔥 BUILD GEOJSON DIRETO
        # ======================================================
        geojson = GeoJSONBuilder.build(rotas)

        # ======================================================
        # 🔥 DEBUG GEOJSON
        # ======================================================
        print("\n================ GEOJSON GERADO ================")
        print(json.dumps(geojson, indent=2))
        print("================================================\n")

        return geojson