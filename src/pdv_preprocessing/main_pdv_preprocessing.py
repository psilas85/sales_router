#sales_router/src/pdv_preprocessing/main_pdv_preprocessing.py

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

# ============================================================
# 🌍 Inicialização de ambiente
# ============================================================
load_dotenv()
logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(
        description="Pré-processamento de PDVs (SalesRouter multi-tenant)"
    )
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--arquivo", required=True)
    parser.add_argument("--descricao", required=True)
    parser.add_argument(
        "--usar_google",
        action="store_true",
        help="Ativa Google Maps na geolocalização (desativado por padrão)"
    )
    
    args = parser.parse_args()

    try:
        tenant_id = int(args.tenant)
    except Exception:
        logging.error("❌ Tenant ID inválido.")
        return

    descricao = args.descricao.strip()[:60]
    input_id = str(uuid.uuid4())
    input_path = args.arquivo

    # ============================================================
    # 🔹 NOVO — job_id fake para execução local (CLI)
    # ============================================================
    fake_job_id = uuid.uuid4()

    setup_logging(tenant_id)
    logging.info(f"🚀 Iniciando pré-processamento de PDVs | tenant={tenant_id}")
    logging.info(f"🆔 input_id={input_id}")
    logging.info(f"🆔 job_id={fake_job_id} (CLI)")

    if not os.path.exists(input_path):
        logging.error(f"❌ Arquivo não encontrado: {input_path}")
        return

    sep = detectar_separador(input_path)
    inicio_execucao = time.time()

    try:
        db_reader = DatabaseReader()
        db_writer = DatabaseWriter()
    except Exception as e:
        logging.error(f"❌ Falha ao inicializar DatabaseReader/Writer: {e}")
        return

    try:
        use_case = PDVPreprocessingUseCase(
            reader=db_reader,
            writer=db_writer,
            tenant_id=tenant_id,
            input_id=input_id,
            descricao=descricao,
            usar_google=args.usar_google
        )


        df_validos, df_invalidos, inseridos = use_case.execute(input_path, sep)

        total_validos = len(df_validos)
        total_invalidos = len(df_invalidos)
        total = total_validos + total_invalidos

        arquivo_invalidos = salvar_invalidos(df_invalidos, os.path.dirname(input_path), input_id)
        duracao = time.time() - inicio_execucao

        logging.info(f"✅ {total_validos} válidos / {total_invalidos} inválidos.")
        logging.info(f"💾 {inseridos} PDVs gravados.")
        logging.info(f"⏱️ {duracao:.2f}s")

        # ============================================================
        # 💾 Histórico — AGORA COM job_id válido
        # ============================================================
        db_writer.salvar_historico_pdv_job(
            tenant_id=tenant_id,
            job_id=fake_job_id,          # <── AQUI
            arquivo=os.path.basename(input_path),
            status="done",
            total_processados=total,
            validos=total_validos,
            invalidos=total_invalidos,
            arquivo_invalidos=arquivo_invalidos,
            mensagem="✓ Pré-processamento de PDVs concluído",
            inseridos=inseridos,
            sobrescritos=0,
            descricao=descricao,
            input_id=input_id,
        )

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
        logging.error(f"💥 Erro inesperado: {e}", exc_info=True)

        # ============================================================
        # 💾 Histórico de erro — também com job_id válido
        # ============================================================
        db_writer.salvar_historico_pdv_job(
            tenant_id=tenant_id,
            job_id=fake_job_id,     # <── AQUI TAMBÉM
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
            sobrescritos=0,
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

