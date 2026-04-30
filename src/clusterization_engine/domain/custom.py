import pandas as pd
import numpy as np

from clusterization_engine.domain.kmeans import LAT_ALIASES, LON_ALIASES, _find_column, _haversine_km


def clusterizar_custom(
    df: pd.DataFrame,
    max_pdv_cluster: int = 200,
    consultor_prefix: str = "Consultor",
    latitude_col: str | None = None,
    longitude_col: str | None = None,
) -> pd.DataFrame:
    if max_pdv_cluster < 1:
        raise ValueError("max_pdv_cluster deve ser maior ou igual a 1.")

    df = df.copy()
    lat_col = _find_column(df, latitude_col, LAT_ALIASES)
    lon_col = _find_column(df, longitude_col, LON_ALIASES)
    if not lat_col or not lon_col or len(df) <= max_pdv_cluster:
        df["consultor"] = f"{consultor_prefix} 1"
        return df

    coords = df[[lat_col, lon_col]].apply(pd.to_numeric, errors="coerce")
    if coords.isna().any(axis=None):
        raise ValueError("Latitude e longitude devem estar preenchidas com valores numericos.")

    arr = coords.to_numpy(dtype=float)
    sums = np.zeros(len(arr))
    for i, point in enumerate(arr):
        sums[i] = sum(_haversine_km(point, other) for other in arr)

    medoid = arr[int(np.argmin(sums))]
    distances = np.array([_haversine_km(medoid, point) for point in arr])
    selected_positions = set(np.argsort(distances)[:max_pdv_cluster])
    df["consultor"] = [
        f"{consultor_prefix} 1" if pos in selected_positions else ""
        for pos in range(len(df))
    ]
    return df
