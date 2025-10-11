# src/pdv_preprocessing/main_pdv_preprocessing.py

import os
import argparse
import logging
import uuid
import time
import json
from dotenv import load_dotenv

from pdv_preprocessing.application.pdv_preprocessing_use_case import PDVPreprocessingUseCase
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from database.db_connection import get_connection


# ------------------------------------------------------------
# Configuração de logging e ambiente
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()


def detectar_separador(path: str) -> str:
    """Detecta automaticamente o separador CSV (',' ou ';')."""
    with open(path, "r", encoding="utf-8-sig") as f:
        linha = f.readline()
        return ";" if ";" in linha else ","


def salvar_invalidos(df_invalidos, pasta_base: str, job_id: str):
    """Salva registros inválidos em CSV na pasta /data/invalidos/."""
    try:
        if df_invalidos is None or df_invalidos.empty:
            return None

        pasta_invalidos = os.path.join(pasta_base, "invalidos")
        os.makedirs(pasta_invalidos, exist_ok=True)

        nome_arquivo = f"pdvs_invalidos_{job_id}.csv"
        caminho_saida = os.path.join(pasta_invalidos, nome_arquivo)

        df_invalidos.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8-sig")

        logging.warning(f"⚠️ {len(df_invalidos)} registro(s) inválido(s) salvo(s) em: {caminho_saida}")
        return caminho_saida
    except Exception as e:
        logging.error(f"❌ Erro ao salvar registros inválidos: {e}")
        return None


def main():
    # ------------------------------------------------------------
    # Argumentos CLI
    # ------------------------------------------------------------
    parser = argparse.ArgumentParser(description="Pré-processamento de PDVs (SalesRouter)")
    parser.add_argument("--tenant", help="Tenant ID (ou usa TENANT_ID do .env)")
    parser.add_argument("--arquivo", required=True, help="Caminho do CSV de entrada (ex: /app/data/pdvs_enderecos.csv)")
    parser.add_argument("--modo_forcar", action="store_true", help="Força reprocessamento mesmo se já existir cache")
    args = parser.parse_args()

    tenant_id = args.tenant or os.getenv("TENANT_ID")
    if not tenant_id:
        logging.error("❌ Tenant ID não informado via argumento ou variável de ambiente TENANT_ID.")
        return

    input_path = args.arquivo
    if not os.path.exists(input_path):
        logging.error(f"❌ Arquivo não encontrado: {input_path}")
        return

    sep = detectar_separador(input_path)
    job_id = str(uuid.uuid4())
    inicio_execucao = time.time()

    logging.info(f"🚀 Iniciando pré-processamento de PDVs - Job {job_id}")
    logging.info(f"📂 Lendo arquivo de entrada: {input_path} | Separador detectado: '{sep}'")

    # ------------------------------------------------------------
    # Conexão com o banco
    # ------------------------------------------------------------
    try:
        conn = get_connection()
        db_reader = DatabaseReader(conn)
        db_writer = DatabaseWriter(conn)
    except Exception as e:
        logging.error(f"❌ Falha ao conectar ao banco de dados: {e}")
        return

    # ------------------------------------------------------------
    # Execução do pipeline
    # ------------------------------------------------------------
    try:
        use_case = PDVPreprocessingUseCase(db_reader, db_writer, tenant_id)
        df_validos, df_invalidos = use_case.execute(input_path, sep)

        total_validos = len(df_validos) if df_validos is not None else 0
        total_invalidos = len(df_invalidos) if df_invalidos is not None else 0
        total = total_validos + total_invalidos

        # ------------------------------------------------------------
        # Salva inválidos (se houver)
        # ------------------------------------------------------------
        arquivo_invalidos = salvar_invalidos(df_invalidos, os.path.dirname(input_path), job_id)

        duracao = time.time() - inicio_execucao
        logging.info(f"✅ Processamento concluído com {total_validos} PDVs válidos.")
        logging.info(f"💾 Dados gravados no banco com sucesso.")
        logging.info(f"⏱️ Duração total: {duracao:.2f}s")
        logging.info("🏁 Fim do processamento.")

        # ------------------------------------------------------------
        # 📊 Saída JSON para integração via API Gateway
        # ------------------------------------------------------------
        print(json.dumps({
            "job_id": job_id,
            "tenant_id": tenant_id,
            "arquivo": os.path.basename(input_path),
            "total_processados": total,
            "validos": total_validos,
            "invalidos": total_invalidos,
            "arquivo_invalidos": arquivo_invalidos,
            "duracao_segundos": round(duracao, 2),
            "status": "done"
        }))

    except Exception as e:
        logging.error(f"❌ Erro inesperado durante o pré-processamento: {e}", exc_info=True)
        print(json.dumps({
            "status": "error",
            "erro": str(e),
            "job_id": job_id,
            "tenant_id": tenant_id
        }))


if __name__ == "__main__":
    main()
