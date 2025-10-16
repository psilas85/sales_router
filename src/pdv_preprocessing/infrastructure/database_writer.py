import logging
import time
from psycopg2.extras import execute_values


class DatabaseWriter:
    def __init__(self, conn):
        self.conn = conn

    # ============================================================
    # 💾 Inserção / atualização de PDVs (com contagem e desempenho)
    # ============================================================
    def inserir_pdvs(self, lista_pdvs):
        if not lista_pdvs:
            logging.warning("⚠️ Nenhum PDV para inserir.")
            return 0, 0

        inicio = time.time()
        cur = self.conn.cursor()

        # 🔹 Prepara valores
        valores = [
            (
                p.tenant_id, p.cnpj, p.logradouro, p.numero, p.bairro, p.cidade,
                p.uf, p.cep, p.pdv_endereco_completo, p.pdv_lat, p.pdv_lon, p.status_geolocalizacao
            )
            for p in lista_pdvs
        ]

        # ✅ Corrigido: removido 'atualizado_em' do INSERT
        sql = """
            INSERT INTO pdvs (
                tenant_id, cnpj, logradouro, numero, bairro, cidade, uf, cep,
                pdv_endereco_completo, pdv_lat, pdv_lon, status_geolocalizacao
            )
            VALUES %s
            ON CONFLICT (tenant_id, cnpj)
            DO UPDATE SET
                logradouro = EXCLUDED.logradouro,
                numero = EXCLUDED.numero,
                bairro = EXCLUDED.bairro,
                cidade = EXCLUDED.cidade,
                uf = EXCLUDED.uf,
                cep = EXCLUDED.cep,
                pdv_endereco_completo = EXCLUDED.pdv_endereco_completo,
                pdv_lat = EXCLUDED.pdv_lat,
                pdv_lon = EXCLUDED.pdv_lon,
                status_geolocalizacao = EXCLUDED.status_geolocalizacao,
                atualizado_em = NOW()
            RETURNING xmax = 0 AS inserido;
        """

        try:
            execute_values(cur, sql, valores)
            resultados = cur.fetchall()
            self.conn.commit()

            # 🔹 Contagem de novos e sobrescritos
            inseridos = sum(1 for r in resultados if r[0])
            sobrescritos = len(resultados) - inseridos

            dur = time.time() - inicio
            logging.info(
                f"💾 [PDV_DB] {inseridos} novos / 🔁 {sobrescritos} sobrescritos "
                f"({dur:.2f}s)"
            )
            return inseridos, sobrescritos

        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao inserir/atualizar PDVs: {e}", exc_info=True)
            return 0, 0

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
        job_id: str,
        arquivo: str,
        status: str,
        total_processados: int,
        validos: int,
        invalidos: int,
        arquivo_invalidos: str = None,
        mensagem: str = None,
        inseridos: int = 0,
        sobrescritos: int = 0,
    ):
        """Salva histórico de execução do pré-processamento de PDVs."""
        try:
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
                    criado_em
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());
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
                sobrescritos
            ))
            self.conn.commit()
            cur.close()
            logging.info(
                f"🧾 Histórico salvo em historico_pdv_jobs "
                f"(job_id={job_id}, status={status}, inseridos={inseridos}, sobrescritos={sobrescritos})"
            )

        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao salvar histórico do job {job_id}: {e}", exc_info=True)
