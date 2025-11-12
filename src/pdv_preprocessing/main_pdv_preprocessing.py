# ============================================================
# 📦 src/pdv_preprocessing/main_pdv_preprocessing.py
# ============================================================

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
from pdv_preprocessing.utils.file_utils import detectar_separador, salvar_invalidos
from pdv_preprocessing.logs.logging_config import setup_logging
from database.db_connection import get_connection

# ============================================================
# 🌍 Inicialização de ambiente
# ============================================================
load_dotenv()
logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
    # ------------------------------------------------------------
    # 🎯 Argumentos CLI
    # ------------------------------------------------------------
    parser = argparse.ArgumentParser(
        description="Pré-processamento de PDVs (SalesRouter multi-tenant)"
    )
    parser.add_argument(
        "--tenant", required=True,
        help="Tenant ID (inteiro ou variável TENANT_ID do .env)"
    )
    parser.add_argument(
        "--arquivo", required=True,
        help="Caminho do CSV de entrada (ex: /app/data/pdvs_enderecos.csv)"
    )
    parser.add_argument(
        "--descricao", required=True,
        help="Descrição do processamento (máx. 60 caracteres)"
    )
    args = parser.parse_args()

    # ------------------------------------------------------------
    # 🔹 Inicialização de variáveis
    # ------------------------------------------------------------
    try:
        tenant_id = int(args.tenant or os.getenv("TENANT_ID"))
    except (TypeError, ValueError):
        logging.error("❌ Tenant ID inválido ou ausente.")
        return

    descricao = args.descricao.strip()[:60]
    input_id = str(uuid.uuid4())

    # ------------------------------------------------------------
    # 🧾 Logging e informações iniciais
    # ------------------------------------------------------------
    setup_logging(tenant_id)
    logging.info(f"🚀 Iniciando pré-processamento de PDVs | tenant={tenant_id}")
    logging.info(f"🆔 input_id={input_id}")
    logging.info(f"📝 Descrição: {descricao}")

    input_path = args.arquivo
    if not os.path.exists(input_path):
        logging.error(f"❌ Arquivo não encontrado: {input_path}")
        return

    sep = detectar_separador(input_path)
    inicio_execucao = time.time()

    # ------------------------------------------------------------
    # 🔗 Conexão com banco
    # ------------------------------------------------------------
    try:
        conn = get_connection()
        db_reader = DatabaseReader(conn)
        db_writer = DatabaseWriter(conn)
    except Exception as e:
        logging.error(f"❌ Falha ao conectar ao banco: {e}")
        return

    # ------------------------------------------------------------
    # 🚀 Execução principal
    # ------------------------------------------------------------
    try:
        use_case = PDVPreprocessingUseCase(
            db_reader,
            db_writer,
            tenant_id,
            input_id=input_id,
            descricao=descricao
        )

        df_validos, df_invalidos, inseridos = use_case.execute(
            input_path=input_path,
            sep=sep
        )


        total_validos = len(df_validos) if df_validos is not None else 0
        total_invalidos = len(df_invalidos) if df_invalidos is not None else 0
        total = total_validos + total_invalidos

        arquivo_invalidos = salvar_invalidos(df_invalidos, os.path.dirname(input_path), input_id)
        duracao = time.time() - inicio_execucao

        logging.info(f"✅ {total_validos} válidos / {total_invalidos} inválidos processados.")
        logging.info(f"💾 {inseridos} PDVs gravados no banco.")
        logging.info(f"⏱️ Duração total: {duracao:.2f}s")

        # --------------------------------------------------------
        # 🧾 Registro do histórico
        # --------------------------------------------------------
        db_writer.salvar_historico_pdv_job(
            tenant_id=tenant_id,
            input_id=input_id,
            descricao=descricao,
            arquivo=os.path.basename(input_path),
            status="done",
            total_processados=total,
            validos=total_validos,
            invalidos=total_invalidos,
            arquivo_invalidos=arquivo_invalidos,
            mensagem="✅ Pré-processamento de PDVs concluído com sucesso",
            inseridos=inseridos,
        )

        # --------------------------------------------------------
        # 📤 Saída JSON estruturada
        # --------------------------------------------------------
        resultado = {
            "status": "done",
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao,
            "arquivo": os.path.basename(input_path),
            "total_processados": total,
            "validos": total_validos,
            "invalidos": total_invalidos,
            "inseridos": inseridos,
            "arquivo_invalidos": arquivo_invalidos,
            "duracao_segundos": round(duracao, 2),
        }
        print(json.dumps(resultado, ensure_ascii=False))

    except Exception as e:
        logging.error(f"❌ Erro inesperado: {e}", exc_info=True)
        db_writer.salvar_historico_pdv_job(
            tenant_id=tenant_id,
            input_id=input_id,
            descricao=descricao,
            arquivo=os.path.basename(input_path),
            status="error",
            total_processados=0,
            validos=0,
            invalidos=0,
            arquivo_invalidos=None,
            mensagem=str(e),
            inseridos=0,
        )

        print(json.dumps({
            "status": "error",
            "erro": str(e),
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao
        }, ensure_ascii=False))


if __name__ == "__main__":
    main()
