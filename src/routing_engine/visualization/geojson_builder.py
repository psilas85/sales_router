# sales_router/src/routing_engine/visualization/geojson_builder.py

class GeoJSONBuilder:

    @staticmethod
    def _get_color(rota_id: str):
        return "#{:06x}".format(abs(hash(rota_id)) % 0xFFFFFF)

    @staticmethod
    def build(rotas):

        features = []

        for rota in rotas:

            coords = rota.get("coords")

            if not coords or len(coords) < 2:
                continue

            # ======================================================
            # GEOMETRIA
            # ======================================================
            if isinstance(coords[0], dict) and "ordem" not in coords[0]:

                coords_final = [
                    [p["lon"], p["lat"]]
                    for p in coords
                    if p.get("lat") is not None and p.get("lon") is not None
                ]

                centro = coords[0]

            elif isinstance(coords[0], dict):

                coords_sorted = sorted(
                    coords,
                    key=lambda x: x.get("ordem", 0)
                )

                coords_final = [
                    [p["lon"], p["lat"]]
                    for p in coords_sorted
                    if p.get("lat") is not None and p.get("lon") is not None
                ]

                centro = coords_sorted[0]

            else:

                coords_final = [
                    [c[1], c[0]]
                    for c in coords
                    if c and len(c) == 2
                ]

                centro = {
                    "lat": coords[0][0],
                    "lon": coords[0][1]
                }

            if len(coords_final) < 2:
                continue

            cor = GeoJSONBuilder._get_color(rota.get("rota_id"))

            consultor = rota.get("cluster")  # 🔥 usa cluster como nome

            # ======================================================
            # LINHA
            # ======================================================
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coords_final
                },
                "properties": {
                    "rota_id": rota.get("rota_id"),
                    "cluster": rota.get("cluster"),
                    "veiculo": rota.get("veiculo"),
                    "color": cor
                }
            })

            # ======================================================
            # BASE
            # ======================================================
            if centro and centro.get("lat") and centro.get("lon"):

                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [centro["lon"], centro["lat"]]
                    },
                    "properties": {
                        "rota_id": rota.get("rota_id"),
                        "cluster": rota.get("cluster"),
                        "consultor": consultor,
                        "tipo": "centro",
                        "color": cor
                    }
                })

        print(f"FEATURES GERADAS: {len(features)}")

        return {
            "type": "FeatureCollection",
            "features": features
        }