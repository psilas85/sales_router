#sales_router/src/pdv_preprocessing/infrastructure/database_writer.py

import logging
import time
from psycopg2.extras import execute_values


class DatabaseWriter:
    def __init__(self, conn):
        self.conn = conn

    # ============================================================
    # 💾 Inserção de PDVs (com input_id e descricao)
    # ============================================================
    def inserir_pdvs(self, lista_pdvs):
        if not lista_pdvs:
            logging.warning("⚠️ Nenhum PDV para inserir.")
            return 0

        inicio = time.time()
        cur = self.conn.cursor()

        # 🔹 Prepara valores para o INSERT
        valores = [
            (
                p.tenant_id, p.input_id, p.descricao, p.cnpj, p.logradouro, p.numero, p.bairro,
                p.cidade, p.uf, p.cep, p.pdv_endereco_completo,
                p.pdv_lat, p.pdv_lon, p.status_geolocalizacao
            )
            for p in lista_pdvs
        ]

        sql = """
            INSERT INTO pdvs (
                tenant_id, input_id, descricao, cnpj, logradouro, numero, bairro,
                cidade, uf, cep, pdv_endereco_completo,
                pdv_lat, pdv_lon, status_geolocalizacao
            )
            VALUES %s
            ON CONFLICT (tenant_id, input_id, cnpj)
            DO NOTHING;
        """

        try:
            execute_values(cur, sql, valores)
            self.conn.commit()
            inseridos = cur.rowcount or len(valores)
            dur = time.time() - inicio
            logging.info(f"💾 [PDV_DB] Inseridos {inseridos} PDVs ({dur:.2f}s)")
            return inseridos

        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao inserir PDVs: {e}", exc_info=True)
            return 0
        finally:
            cur.close()


    # ============================================================
    # 🗺️ Inserção no cache de endereços
    # ============================================================
    def inserir_localizacao(self, endereco: str, lat: float, lon: float):
        if not endereco or lat is None or lon is None:
            return
        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO enderecos_cache (endereco, lat, lon)
                VALUES (%s, %s, %s)
                ON CONFLICT (endereco) DO NOTHING;
            """, (endereco.strip().lower(), lat, lon))
            self.conn.commit()
            cur.close()
        except Exception as e:
            logging.error(f"❌ Erro ao inserir localização no cache: {e}")

    # ============================================================
    # 🧾 Registro de histórico de pré-processamento de PDVs
    # ============================================================
    def salvar_historico_pdv_job(
        self,
        tenant_id: int,
        arquivo: str,
        status: str,
        total_processados: int,
        validos: int,
        invalidos: int,
        arquivo_invalidos: str = None,
        mensagem: str = None,
        inseridos: int = 0,
        sobrescritos: int = 0,
        descricao: str = None,
        input_id: str = None,
        job_id: str = None,
    ):
        """Salva histórico de execução do pré-processamento de PDVs."""
        try:
            # 🔁 fallback automático: se job_id não for informado, usa o input_id
            job_id = job_id or input_id

            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO historico_pdv_jobs (
                    tenant_id,
                    job_id,
                    arquivo,
                    status,
                    total_processados,
                    validos,
                    invalidos,
                    arquivo_invalidos,
                    mensagem,
                    inseridos,
                    sobrescritos,
                    criado_em,
                    descricao,
                    input_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s);
            """, (
                tenant_id,
                job_id,
                arquivo,
                status,
                total_processados,
                validos,
                invalidos,
                arquivo_invalidos,
                mensagem,
                inseridos,
                sobrescritos,
                descricao,
                input_id
            ))
            self.conn.commit()
            cur.close()

            logging.info(
                f"🧾 Histórico salvo (tenant_id={tenant_id}, input_id={input_id}, "
                f"descricao='{descricao}', status={status})"
            )

        except Exception as e:
            self.conn.rollback()
            logging.error(
                f"❌ Erro ao salvar histórico (input_id={input_id}): {e}",
                exc_info=True
            )


