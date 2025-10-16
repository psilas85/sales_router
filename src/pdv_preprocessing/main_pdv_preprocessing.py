# sales_router/src/pdv_preprocessing/main_pdv_preprocessing.py
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
from src.database.cleanup_service import limpar_dados_operacionais
from pdv_preprocessing.logs.logging_config import setup_logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()


def detectar_separador(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig") as f:
        linha = f.readline()
        return ";" if ";" in linha else ","


def salvar_invalidos(df_invalidos, pasta_base: str, job_id: str):
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
    parser = argparse.ArgumentParser(description="Pré-processamento de PDVs (SalesRouter multi-tenant)")
    parser.add_argument("--tenant", required=True, help="Tenant ID (inteiro ou variável TENANT_ID do .env)")
    parser.add_argument("--arquivo", required=True, help="Caminho do CSV de entrada (ex: /app/data/pdvs_enderecos.csv)")
    parser.add_argument("--modo_forcar", action="store_true", help="Força reprocessamento e limpa PDVs anteriores")
    args = parser.parse_args()

    try:
        tenant_id = int(args.tenant or os.getenv("TENANT_ID"))
    except (TypeError, ValueError):
        logging.error("❌ Tenant ID inválido ou ausente. Informe um número inteiro.")
        return

    # ============================================================
    # 🧾 Inicializa logging dedicado
    # ============================================================
    caminho_log = setup_logging(tenant_id)
    logging.info(f"🚀 Iniciando pré-processamento de PDVs (tenant={tenant_id})")

    # ============================================================
    # 🧹 LIMPEZA AUTOMÁTICA DE SIMULAÇÕES
    # ============================================================
    logging.info(f"🧹 Limpando simulações operacionais do tenant_id={tenant_id} antes do novo pré-processamento...")
    try:
        limpar_dados_operacionais("preprocessing", tenant_id=tenant_id)
    except Exception as e:
        logging.error(f"❌ Falha na limpeza automática: {e}")
        return

    input_path = args.arquivo
    if not os.path.exists(input_path):
        logging.error(f"❌ Arquivo não encontrado: {input_path}")
        return

    sep = detectar_separador(input_path)
    job_id = str(uuid.uuid4())
    inicio_execucao = time.time()

    logging.info(f"🚀 Iniciando pré-processamento de PDVs (tenant_id={tenant_id}) - Job {job_id}")
    logging.info(f"📂 Lendo arquivo de entrada: {input_path} | Separador detectado: '{sep}'")

    try:
        conn = get_connection()
        db_reader = DatabaseReader(conn)
        db_writer = DatabaseWriter(conn)
    except Exception as e:
        logging.error(f"❌ Falha ao conectar ao banco de dados: {e}")
        return

    # ============================================================
    # 🧹 LIMPEZA AUTOMÁTICA SE modo_forcar = True
    # ============================================================
    if args.modo_forcar:
        logging.warning(f"🧹 Modo forçar ativado — limpando PDVs existentes do tenant_id={tenant_id} ...")
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM pdvs WHERE tenant_id = %s;", (tenant_id,))
                conn.commit()
            logging.info(f"✅ Todos os PDVs anteriores do tenant_id={tenant_id} foram removidos.")
        except Exception as e:
            logging.error(f"❌ Erro ao limpar PDVs do tenant_id={tenant_id}: {e}")
            return

    try:
        use_case = PDVPreprocessingUseCase(db_reader, db_writer, tenant_id)
        df_validos, df_invalidos, inseridos, sobrescritos = use_case.execute(input_path, sep)

        total_validos = len(df_validos) if df_validos is not None else 0
        total_invalidos = len(df_invalidos) if df_invalidos is not None else 0
        total = total_validos + total_invalidos
        arquivo_invalidos = salvar_invalidos(df_invalidos, os.path.dirname(input_path), job_id)

        duracao = time.time() - inicio_execucao
        logging.info(f"✅ Processamento concluído com {total_validos} PDVs válidos.")
        logging.info(f"💾 Dados gravados no banco para tenant_id={tenant_id}.")
        logging.info(f"⏱️ Duração total: {duracao:.2f}s")

        # ============================================================
        # 📊 CONTAGEM FINAL DE PDVS NO BANCO
        # ============================================================
        try:
            with get_connection() as conn_final:
                with conn_final.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM pdvs WHERE tenant_id = %s;", (tenant_id,))
                    total_final = cur.fetchone()[0]
            logging.info(f"💾 Total final no banco para tenant_id={tenant_id}: {total_final} PDVs.")
        except Exception as e:
            logging.error(f"❌ Erro ao consultar total final de PDVs: {e}")
            total_final = None

        # ============================================================
        # 🧾 Registro de histórico
        # ============================================================
        try:
            db_writer.salvar_historico_pdv_job(
                tenant_id=tenant_id,
                job_id=job_id,
                arquivo=os.path.basename(input_path),
                status="done",
                total_processados=total,
                validos=total_validos,
                invalidos=total_invalidos,
                arquivo_invalidos=arquivo_invalidos,
                mensagem="✅ Pré-processamento de PDVs concluído com sucesso",
                inseridos=inseridos,
                sobrescritos=sobrescritos
            )
        except Exception as e:
            logging.error(f"❌ Falha ao salvar histórico do job: {e}")

        print(json.dumps({
            "status": "done",
            "job_id": job_id,
            "tenant_id": tenant_id,
            "arquivo": os.path.basename(input_path),
            "total_processados": total,
            "validos": total_validos,
            "invalidos": total_invalidos,
            "inseridos": inseridos,
            "sobrescritos": sobrescritos,
            "arquivo_invalidos": arquivo_invalidos,
            "duracao_segundos": round(duracao, 2),
            "total_final_banco": total_final
        }))

    except Exception as e:
        logging.error(f"❌ Erro inesperado: {e}", exc_info=True)
        try:
            db_writer.salvar_historico_pdv_job(
                tenant_id=tenant_id,
                job_id=job_id,
                arquivo=os.path.basename(input_path),
                status="error",
                total_processados=0,
                validos=0,
                invalidos=0,
                arquivo_invalidos=None,
                mensagem=str(e),
                inseridos=0,
                sobrescritos=0
            )
        except Exception as inner_e:
            logging.error(f"❌ Falha ao registrar histórico de erro: {inner_e}")

        print(json.dumps({
            "status": "error",
            "erro": str(e),
            "job_id": job_id,
            "tenant_id": tenant_id
        }))


if __name__ == "__main__":
    main()
