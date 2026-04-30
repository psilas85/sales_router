# clusterization_engine/domain/consultor_nearest.py

from math import radians, sin, cos, sqrt, atan2
from typing import Any

import pandas as pd

from clusterization_engine.domain.kmeans import LAT_ALIASES, LON_ALIASES, _find_column


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


def clusterizar_consultor_nearest(
    df: pd.DataFrame,
    consultores: list[dict[str, Any]],
    max_pdv_por_consultor: int,
    permitir_excedente: bool = True,
    latitude_col: str | None = None,
    longitude_col: str | None = None,
) -> pd.DataFrame:
    """
    Assigns each PDV to the nearest consultor respecting capacity.

    consultores: list of dicts with keys: id, nome, lat, lon
    max_pdv_por_consultor: max PDVs per consultor in the cycle
    permitir_excedente: if True, overflows to nearest when all are full;
                        if False, raises ValueError listing unassigned PDVs
    """
    if not consultores:
        raise ValueError("Nenhum consultor selecionado para a setorizacao.")

    lat_col = _find_column(df, latitude_col, LAT_ALIASES)
    lon_col = _find_column(df, longitude_col, LON_ALIASES)

    if not lat_col or not lon_col:
        raise ValueError("Nao foi possivel encontrar colunas de latitude/longitude.")

    df = df.copy()
    coords = df[[lat_col, lon_col]].apply(pd.to_numeric, errors="coerce")

    capacity: dict[str, int] = {c["nome"]: max_pdv_por_consultor for c in consultores}
    assignees: list[str | None] = []
    excedentes: list[bool] = []
    unassigned_indices: list[int] = []

    for i, idx in enumerate(df.index):
        lat = coords.iloc[i][lat_col]
        lon = coords.iloc[i][lon_col]

        if pd.isna(lat) or pd.isna(lon):
            assignees.append(None)
            excedentes.append(False)
            continue

        sorted_consultores = sorted(
            consultores,
            key=lambda c: _haversine_km(lat, lon, c["lat"], c["lon"])
        )

        assigned: str | None = None
        is_excedente = False

        for c in sorted_consultores:
            if capacity[c["nome"]] > 0:
                assigned = c["nome"]
                capacity[c["nome"]] -= 1
                break

        if assigned is None:
            if permitir_excedente:
                assigned = sorted_consultores[0]["nome"]
                is_excedente = True
            else:
                unassigned_indices.append(idx)
                assigned = None

        assignees.append(assigned)
        excedentes.append(is_excedente)

    if unassigned_indices:
        raise ValueError(
            f"{len(unassigned_indices)} PDV(s) nao puderam ser atribuidos: todos os consultores "
            f"atingiram a capacidade maxima de {max_pdv_por_consultor} PDVs. "
            "Ative 'Permitir setor com excedente' ou selecione mais consultores."
        )

    df["consultor"] = assignees
    df["excedente"] = excedentes

    return df
