#sales_router/src/geocoding_engine/visualization/map_repository.py

import json
import os


class MapRepository:

    def load_job_json(self, path):

        if not os.path.exists(path):
            raise Exception("Arquivo de resultado não encontrado")

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)