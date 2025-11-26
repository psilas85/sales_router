#sales_router/src/pdv_preprocessing/main_mkp_preprocessing.py

# ============================================================
# 📦 src/pdv_preprocessing/main_mkp_preprocessing.py (ASSÍNCRONO FINAL)
# ============================================================

import os
import argparse
import logging
import uuid
import json
import time
import pandas as pd
from dotenv import load_dotenv
from redis import Redis
from rq import Queue

from pdv_preprocessing.application.mkp_preprocessing_use_case import MKPPreprocessingUseCase
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from pdv_preprocessing.logs.logging_config import setup_logging

# novo master que orquestra job_geocode
from pdv_preprocessing.jobs.job_master_mkp import job_master_mkp


load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ------------------------------------------------------------
def detectar_separador(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig") as f:
        linha = f.readline()
        return ";" if ";" in linha else ","


# ------------------------------------------------------------
def carregar_dataframe_inteligente(path: str, sep: str = ";"):
    ext = os.path.splitext(path)[1].lower()

    if ext in [".xlsx", ".xls"]:
        logging.info("📄 Lendo XLSX — zeros à esquerda preservados")
        df = pd.read_excel(path, dtype=str, keep_default_na=False)
        return df.fillna("")

    logging.info("📄 Lendo CSV")
    df = pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8", keep_default_na=False)
    return df.fillna("")


# ------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Pré-processamento assíncrono de CEPs (Marketplace MKP)."
    )
    parser.add_argument("--tenant", required=True, help="Tenant ID (inteiro)")
    parser.add_argument("--arquivo", required=True, help="Caminho do arquivo (CSV/XLSX)")
    parser.add_argument("--descricao", required=True, help="Descrição do processamento")
    args = parser.parse_args()

    try:
        tenant_id = int(args.tenant)
    except ValueError:
        logging.error("❌ Tenant ID inválido. Deve ser inteiro.")
        return

    descricao = args.descricao.strip()[:60]
    input_id = str(uuid.uuid4())

    setup_logging(tenant_id)
    logging.info(f"🚀 Iniciando pré-processamento MKP (ASSÍNCRONO) | tenant={tenant_id}")
    logging.info(f"🆔 input_id={input_id}")
    logging.info(f"📝 Descrição: {descricao}")

    input_path = args.arquivo
    arquivo_nome = os.path.basename(input_path)

    if not os.path.exists(input_path):
        logging.error(f"❌ Arquivo não encontrado: {input_path}")
        return

    ext = os.path.splitext(input_path)[1].lower()
    sep = detectar_separador(input_path) if ext == ".csv" else None

    # ------------------------------------------------------------
    #  Conexão com banco
    # ------------------------------------------------------------
    try:
        db_reader = DatabaseReader()
        db_writer = DatabaseWriter()
    except Exception as e:
        logging.error(f"❌ Falha ao inicializar banco: {e}", exc_info=True)
        return

    try:
        # ============================================================
        # 1) Carregar e validar dados (SEM geocodificação)
        # ============================================================
        df = carregar_dataframe_inteligente(input_path, sep=sep)

        use_case = MKPPreprocessingUseCase(
            reader=None,
            writer=None,
            tenant_id=tenant_id,
            input_id=input_id,
            descricao=descricao
        )

        df_validos, df_invalidos, _ = use_case.execute_df(df)

        total_validos = len(df_validos)
        total_invalidos = len(df_invalidos)
        total_processados = total_validos + total_invalidos

        logging.info(f"📌 {total_validos} válidos | {total_invalidos} inválidos")

        # ============================================================
        # 2) Exportar inválidos se existirem
        # ============================================================
        arquivo_invalidos = None
        if total_invalidos > 0:
            os.makedirs("output/invalidos", exist_ok=True)
            arquivo_invalidos = f"output/invalidos/invalidos_mkp_{tenant_id}_{input_id}.csv"
            df_invalidos.to_csv(arquivo_invalidos, sep=";", index=False, encoding="utf-8-sig")

        # ============================================================
        # 3) Inserir apenas VÁLIDOS no marketplace_cep (lat/lon NULL)
        # ============================================================
        inseridos = db_writer.inserir_mkp_sem_geo(
            df_validos,
            tenant_id,
            input_id,
            descricao
        )
        logging.info(f"💾 {inseridos} registros inseridos no marketplace_cep (lat/lon NULL)")

        # ============================================================
        # 4) Registrar histórico inicial
        # ============================================================
        db_writer.salvar_historico_mkp_job(
            tenant_id=tenant_id,
            job_id=input_id,
            arquivo=arquivo_nome,
            status="processing",
            total_processados=total_processados,
            validos=total_validos,
            invalidos=total_invalidos,
            arquivo_invalidos=arquivo_invalidos,
            arquivo_validos=None,
            mensagem="Processamento iniciado. Novo job master acionado.",
            inseridos=inseridos,
            sobrescritos=0,
            descricao=descricao,
            input_id=input_id,
        )

        # ============================================================
        # 5) Disparar job_master_mkp
        # ============================================================
        queue_master = Queue("mkp_master", connection=Redis(host="redis", port=6379))
        queue_master.enqueue(job_master_mkp, tenant_id, input_id, descricao)

        logging.info("🧠 Novo job_master_mkp enfileirado com sucesso!")

        # ============================================================
        # 6) Retorno JSON para CLI
        # ============================================================
        print(json.dumps({
            "status": "processing",
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao,
            "arquivo": arquivo_nome,
            "total_processados": total_processados,
            "validos": total_validos,
            "invalidos": total_invalidos,
            "arquivo_invalidos": arquivo_invalidos,
            "msg": "Novo job master MKP enfileirado"
        }))

    except Exception as e:
        logging.error(f"❌ Erro inesperado no main MKP: {e}", exc_info=True)

        try:
            db_writer.salvar_historico_mkp_job(
                tenant_id=tenant_id,
                job_id=input_id,
                arquivo=arquivo_nome,
                status="error",
                total_processados=0,
                validos=0,
                invalidos=0,
                arquivo_invalidos=None,
                arquivo_validos=None,
                mensagem=str(e),
                inseridos=0,
                sobrescritos=0,
                descricao=descricao,
                input_id=input_id
            )
        except:
            pass

        print(json.dumps({
            "status": "error",
            "erro": str(e),
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao
        }))


if __name__ == "__main__":
    main()
