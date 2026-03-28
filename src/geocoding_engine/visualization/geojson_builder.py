#sales_router/src/geocoding_engine/visualization/geojson_builder.py

class GeoJSONBuilder:

    @staticmethod
    def build(records):

        features = []

        for r in records:

            if r.get("lat") is None or r.get("lon") is None:
                continue

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [r["lon"], r["lat"]]
                },
                "properties": {
                    "cidade": r.get("cidade"),
                    "setor": r.get("setor"),
                    "endereco": r.get("endereco")
                }
            })

        return {
            "type": "FeatureCollection",
            "features": features
        }