# src/pdv_preprocessing/application/pdv_preprocessing_use_case.py

import pandas as pd
import logging
import unicodedata
import re
from pdv_preprocessing.entities.pdv_entity import PDV
from pdv_preprocessing.domain.pdv_validation_service import PDVValidationService
from pdv_preprocessing.domain.geolocation_service import GeolocationService


class PDVPreprocessingUseCase:
    """
    Caso de uso principal do pré-processamento de PDVs.
    Inclui normalização de colunas e valores, remoção de colunas desnecessárias
    e tratamento de formatações vindas de planilhas (como CNPJs em formato científico).
    """

    def __init__(self, reader, writer, tenant_id):
        self.reader = reader
        self.writer = writer
        self.tenant_id = tenant_id
        self.validator = PDVValidationService(db_reader=reader)
        self.geo_service = GeolocationService(reader, writer)

    # ============================================================
    # 🔹 Normalização de colunas e estrutura do DataFrame
    # ============================================================
    def normalizar_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Padroniza nomes de colunas (sem acento, minúsculas) e remove espaços extras."""
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .map(lambda x: unicodedata.normalize("NFKD", x)
                 .encode("ascii", errors="ignore")
                 .decode("utf-8"))
        )
        return df

    # ============================================================
    # 🔹 Limpeza de valores (CNPJs, CEPs, texto)
    # ============================================================
    def limpar_valores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Corrige formatos comuns vindos do Excel e padroniza UF/cidade."""

        # CNPJ — converte formato científico para string numérica limpa
        def normalizar_cnpj(valor):
            if pd.isna(valor) or str(valor).strip() == "":
                return None
            v = str(valor).strip()
            # trata formato científico: ex: 7,20293E+12 → 7202930000000
            if re.match(r"^\d+,\d+E\+\d+$", v):
                v = v.replace(",", ".")
            try:
                if "E+" in v or "e+" in v:
                    v = f"{float(v):.0f}"
            except Exception:
                pass
            return re.sub(r"[^0-9]", "", v)

        # Normaliza CNPJ e CEP
        df["cnpj"] = df["cnpj"].apply(normalizar_cnpj)
        if "cep" in df.columns:
            df["cep"] = df["cep"].astype(str).str.replace(r"[^0-9]", "", regex=True)

        # Padroniza textos básicos
        campos_texto = ["logradouro", "bairro", "cidade", "uf", "numero"]
        for c in campos_texto:
            if c in df.columns:
                df[c] = (
                    df[c].astype(str)
                    .str.strip()
                    .replace({"nan": "", "None": ""})
                )

        # ============================================
        # 🔠 Normalização definitiva de UF e CIDADE
        # ============================================
        estados_validos = {
            "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT",
            "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"
        }

        import unicodedata

        # UF — força maiúsculas e valida contra lista oficial
        if "uf" in df.columns:
            df["uf"] = df["uf"].str.upper().str.strip()
            uf_invalidas = df.loc[~df["uf"].isin(estados_validos) & df["uf"].ne(""), "uf"].unique()
            if len(uf_invalidas) > 0:
                logging.warning(f"⚠️ UFs inválidas detectadas: {', '.join(uf_invalidas)}")

        # Cidade — remove acentos e coloca tudo em maiúsculas
        if "cidade" in df.columns:
            df["cidade"] = df["cidade"].apply(
                lambda x: unicodedata.normalize("NFKD", str(x))
                .encode("ascii", errors="ignore")
                .decode("utf-8")
                .upper()
                .strip()
            )

        return df


    # ============================================================
    # 🔹 Seleciona apenas as colunas esperadas
    # ============================================================
    def filtrar_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mantém apenas as colunas relevantes, mesmo que o CSV traga colunas extras."""
        colunas_necessarias = ["cnpj", "logradouro", "numero", "bairro", "cidade", "uf", "cep"]
        colunas_presentes = [c for c in colunas_necessarias if c in df.columns]
        df = df[colunas_presentes].copy()
        return df

        # ============================================================
    # 🔹 Execução principal
    # ============================================================
    def execute(self, input_path: str, sep=";"):
        logging.info(f"📄 Lendo arquivo de entrada: {input_path}")

        # Carrega CSV
        df = pd.read_csv(input_path, sep=sep, dtype=str).fillna("")
        df = self.normalizar_colunas(df)
        df = self.limpar_valores(df)
        df = self.filtrar_colunas(df)

        # ✅ Ajuste: 'bairro' agora é opcional
        colunas_esperadas = ["cnpj", "logradouro", "numero", "cidade", "uf", "cep"]
        faltantes = [col for col in colunas_esperadas if col not in df.columns]
        if faltantes:
            raise ValueError(f"❌ Colunas obrigatórias ausentes: {', '.join(faltantes)}")

        # ℹ️ Log de auditoria sobre o campo 'bairro'
        if "bairro" in df.columns:
            total_sem_bairro = df["bairro"].eq("").sum()
            logging.info(f"ℹ️ {total_sem_bairro} PDV(s) sem bairro informado.")
        else:
            logging.info("ℹ️ Coluna 'bairro' não presente no arquivo (tratada como opcional).")

        # ============================================================
        # 🧩 Concatenação do endereço completo (bairro opcional)
        # ============================================================
        def montar_endereco(row):
            partes = [
                f"{row['logradouro'].strip()}, {row['numero'].strip()}",
            ]
            # Adiciona o bairro apenas se existir e não estiver vazio
            if 'bairro' in row and str(row['bairro']).strip():
                partes.append(row['bairro'].strip())
            partes.append(f"{row['cidade'].strip()} - {row['uf'].strip()}")
            partes.append(row["cep"].strip())
            return ", ".join(partes)

        df["pdv_endereco_completo"] = df.apply(montar_endereco, axis=1)

        # Validação de campos obrigatórios e duplicados
        df_validos, df_invalidos = self.validator.validar_dados(df, tenant_id=self.tenant_id)
        logging.info(f"✅ [{self.tenant_id}] {len(df_validos)} válidos / {len(df_invalidos)} inválidos")

        if df_validos.empty:
            logging.warning(f"⚠️ [{self.tenant_id}] Nenhum PDV válido para geolocalização.")
            return df_validos, df_invalidos

        # Busca coordenadas (cache → Nominatim → Google)
        coords = []
        for _, row in df_validos.iterrows():
            lat, lon, origem = self.geo_service.buscar_coordenadas(row["pdv_endereco_completo"], row["uf"])
            coords.append((lat, lon, origem))
        df_validos["pdv_lat"], df_validos["pdv_lon"], df_validos["status_geolocalizacao"] = zip(*coords)

        # Criação das entidades PDV
        campos_validos = PDV.__init__.__code__.co_varnames[1:]
        colunas_validas = [c for c in df_validos.columns if c in campos_validos]
        df_validos["tenant_id"] = self.tenant_id
        df_para_inserir = df_validos[colunas_validas + ["tenant_id"]]
        pdvs = [PDV(**row) for row in df_para_inserir.to_dict(orient="records")]

        # Inserção no banco
        self.writer.inserir_pdvs(pdvs)
        logging.info(f"💾 [{self.tenant_id}] {len(pdvs)} PDVs novos inseridos/atualizados com sucesso.")

        return df_validos, df_invalidos
