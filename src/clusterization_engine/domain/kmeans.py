import pandas as pd
import math
import numpy as np
from sklearn.cluster import KMeans
from sklearn.impute import SimpleImputer
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler


LAT_ALIASES = ("latitude", "lat", "lat_pdv", "lat_entrega")
LON_ALIASES = ("longitude", "lon", "lng", "long", "long_pdv", "lng_pdv")


def _find_column(df: pd.DataFrame, explicit: str | None, aliases: tuple[str, ...]) -> str | None:
    if explicit and explicit in df.columns:
        return explicit
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for alias in aliases:
        if alias in normalized:
            return normalized[alias]
    return None


def _feature_matrix(
    df: pd.DataFrame,
    latitude_col: str | None = None,
    longitude_col: str | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    lat_col = _find_column(df, latitude_col, LAT_ALIASES)
    lon_col = _find_column(df, longitude_col, LON_ALIASES)

    if lat_col and lon_col:
        features = df[[lat_col, lon_col]].copy()
        feature_names = [str(lat_col), str(lon_col)]
    else:
        features = df.select_dtypes(include="number").copy()
        feature_names = [str(col) for col in features.columns]

    if features.empty:
        raise ValueError(
            "Nao foi possivel encontrar latitude/longitude nem colunas numericas para clusterizar."
        )

    features = features.apply(pd.to_numeric, errors="coerce")
    if features.isna().all(axis=None):
        raise ValueError("As colunas usadas na clusterizacao nao possuem valores numericos validos.")

    return features, feature_names


def _haversine_km(p1, p2):
    from math import radians, sin, cos, sqrt, atan2

    lat1, lon1 = p1
    lat2, lon2 = p2
    radius_km = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * radius_km * atan2(sqrt(a), sqrt(1 - a))


def remover_outliers_geograficos(
    df: pd.DataFrame,
    latitude_col: str | None = None,
    longitude_col: str | None = None,
    z_thresh: float = 3.0,
) -> pd.DataFrame:
    lat_col = _find_column(df, latitude_col, LAT_ALIASES)
    lon_col = _find_column(df, longitude_col, LON_ALIASES)
    if not lat_col or not lon_col or len(df) < 5:
        return df

    coords = df[[lat_col, lon_col]].apply(pd.to_numeric, errors="coerce")
    valid_mask = ~coords.isna().any(axis=1)
    valid_coords = coords.loc[valid_mask].to_numpy(dtype=float)
    if len(valid_coords) < 5:
        return df

    coords_rad = np.radians(valid_coords)
    nn = NearestNeighbors(n_neighbors=min(6, len(coords_rad)), metric="haversine")
    nn.fit(coords_rad)
    dist, _ = nn.kneighbors(coords_rad)
    dist_min = dist[:, 1] * 6371.0
    limit = float(np.mean(dist_min) + z_thresh * np.std(dist_min))
    keep_valid = dist_min <= limit

    keep_mask = pd.Series(True, index=df.index)
    keep_mask.loc[coords.loc[valid_mask].index] = keep_valid
    return df.loc[keep_mask].copy()


def clusterizar_kmeans(
    df: pd.DataFrame,
    max_pdv_cluster: int = 200,
    freq: int = 1,
    k_forcado: int | None = None,
    consultor_prefix: str = "Consultor",
    latitude_col: str | None = None,
    longitude_col: str | None = None,
    random_state: int = 42,
) -> pd.DataFrame:
    n_clusters = int(k_forcado) if k_forcado else max(1, math.ceil(len(df) / max(max_pdv_cluster * max(freq, 1), 1)))
    if n_clusters < 1:
        raise ValueError("n_clusters deve ser maior ou igual a 1.")
    if len(df) < n_clusters:
        raise ValueError("n_clusters nao pode ser maior que a quantidade de linhas da planilha.")

    df = df.copy()
    features, _ = _feature_matrix(df, latitude_col=latitude_col, longitude_col=longitude_col)
    values = SimpleImputer(strategy="median").fit_transform(features)
    values = StandardScaler().fit_transform(values)
    labels = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10).fit_predict(values)

    # Segunda etapa semelhante ao balanceamento por capacidade: subdivide clusters grandes.
    next_label = 0
    balanced_labels = np.full(len(df), -1, dtype=int)
    for label in sorted(set(labels)):
        idx = np.where(labels == label)[0]
        if len(idx) <= max_pdv_cluster:
            balanced_labels[idx] = next_label
            next_label += 1
            continue

        k_sub = max(1, math.ceil(len(idx) / max_pdv_cluster))
        sub_values = values[idx]
        sub_labels = KMeans(n_clusters=k_sub, random_state=random_state, n_init=10).fit_predict(sub_values)
        for sub_label in sorted(set(sub_labels)):
            sub_idx = idx[sub_labels == sub_label]
            balanced_labels[sub_idx] = next_label
            next_label += 1

    labels = balanced_labels
    df["consultor"] = [f"{consultor_prefix} {label + 1}" for label in labels]
    return df
