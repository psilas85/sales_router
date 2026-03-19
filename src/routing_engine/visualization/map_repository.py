# sales_router/src/routing_engine/visualization/map_repository.py

# sales_router/src/routing_engine/visualization/map_repository.py

import json
import os


class MapRepository:

    def load_job_json(self, path):

        print(f"📂 Carregando JSON: {path}")

        if not os.path.exists(path):
            raise Exception("Arquivo de resultado não encontrado")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            print(f"📊 Registros carregados: {len(data)}")
        else:
            print("📊 JSON carregado (dict)")

        return data