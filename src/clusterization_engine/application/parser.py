from pathlib import Path
from typing import BinaryIO

import pandas as pd


def parse_entregas_planilha(file: BinaryIO, sheet_name: str | int = 0) -> pd.DataFrame:
    """
    Le uma planilha XLSX de entregas e retorna um DataFrame.
    """
    df = pd.read_excel(file, sheet_name=sheet_name)
    df = df.dropna(how="all").reset_index(drop=True)
    if df.empty:
        raise ValueError("A planilha enviada nao possui linhas para clusterizar.")
    return df


def gerar_output_planilha(df: pd.DataFrame) -> bytes:
    """
    Gera o output XLSX da clusterizacao, incluindo a coluna 'consultor'.
    Retorna o conteúdo em bytes para download.
    """
    import io

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="clusterizacao")
    return output.getvalue()


def salvar_output_planilha(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="clusterizacao")
