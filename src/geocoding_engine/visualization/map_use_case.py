#sales_router/src/geocoding_engine/visualization/map_use_case.py

from geocoding_engine.visualization.map_repository import MapRepository
from geocoding_engine.visualization.geojson_builder import GeoJSONBuilder


class GenerateMapUseCase:

    def execute(self, json_path):

        repo = MapRepository()

        records = repo.load_job_json(json_path)

        geojson = GeoJSONBuilder.build(records)

        return geojson