import pandas as pd
import numpy as np

from clusterization_engine.domain.kmeans import LAT_ALIASES, LON_ALIASES, _find_column


def clusterizar_sweep(
    df: pd.DataFrame,
    max_pdv_cluster: int = 200,
    consultor_prefix: str = "Consultor",
    latitude_col: str | None = None,
    longitude_col: str | None = None,
) -> pd.DataFrame:
    if max_pdv_cluster < 1:
        raise ValueError("max_pdv_cluster deve ser maior ou igual a 1.")

    lat_col = _find_column(df, latitude_col, LAT_ALIASES)
    lon_col = _find_column(df, longitude_col, LON_ALIASES)
    if not lat_col or not lon_col:
        raise ValueError("O algoritmo sweep exige colunas de latitude e longitude.")

    df = df.copy()
    coords = df[[lat_col, lon_col]].apply(pd.to_numeric, errors="coerce")
    if coords.isna().any(axis=None):
        raise ValueError("Latitude e longitude devem estar preenchidas com valores numericos.")

    center_lat = coords[lat_col].mean()
    center_lon = coords[lon_col].mean()
    angles = np.arctan2(coords[lat_col] - center_lat, coords[lon_col] - center_lon)
    distances = np.sqrt((coords[lat_col] - center_lat) ** 2 + (coords[lon_col] - center_lon) ** 2)
    ordered_index = pd.DataFrame({"angle": angles, "distance": distances}).sort_values(
        ["angle", "distance"]
    ).index.tolist()
    assignments = {}
    for position, row_index in enumerate(ordered_index):
        assignments[row_index] = f"{consultor_prefix} {(position // max_pdv_cluster) + 1}"

    df["consultor"] = df.index.map(assignments)
    return df
