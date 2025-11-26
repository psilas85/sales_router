#sales_router/src/pdv_preprocessing/cep_area_geocoding/domain/area_validator.py
import unicodedata
import pandas as pd


class AreaValidator:

    OBRIGATORIAS = ["cep", "bairro", "cidade", "uf"]

    def validar(self, df: pd.DataFrame):
        df.columns = df.columns.str.lower().str.strip()

        for col in self.OBRIGATORIAS:
            if col not in df.columns:
                raise ValueError(f"Coluna obrigatória ausente: {col}")

        # normalizar strings
        for col in ["bairro", "cidade", "uf"]:
            df[col] = df[col].astype(str).apply(self._limpar)

        # UF tem que ter 2 letras
        df = df[df["uf"].str.len() == 2]

        # NÃO remover as outras colunas!
        return df


    def _limpar(self, t: str):
        t = t.strip()
        t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode()
        return t.upper()
