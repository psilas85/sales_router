import logging
import time
from contextlib import closing
from functools import wraps
from typing import Optional, List
import psycopg2
from psycopg2.extras import execute_values


# ============================================================
# 🔁 Decorator de retry com backoff exponencial
# ============================================================
def retry_on_failure(max_retries=3, delay=1.0, backoff=2.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tentativa = 0
            while tentativa < max_retries:
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    tentativa += 1
                    logging.warning(f"⚠️ Erro de conexão na tentativa {tentativa}/{max_retries}: {e}")
                    time.sleep(delay * (backoff ** (tentativa - 1)))
                except Exception as e:
                    logging.error(f"❌ Erro inesperado em {func.__name__}: {e}", exc_info=True)
                    break
            logging.error(f"🚨 Falha após {max_retries} tentativas em {func.__name__}")
            return 0
        return wrapper
    return decorator


class DatabaseWriter:
    """Gerencia inserções e atualizações no banco PostgreSQL (PDVs, MKP e cache)."""

    def __init__(self, conn):
        self.conn = conn

    # ============================================================
    # 💾 Inserção de PDVs
    # ============================================================
    @retry_on_failure()
    def inserir_pdvs(self, lista_pdvs) -> int:
        if not lista_pdvs:
            logging.warning("⚠️ Nenhum PDV para inserir.")
            return 0

        inicio = time.time()
        valores = [
            (
                p.tenant_id, p.input_id, p.descricao, p.cnpj, p.logradouro, p.numero,
                p.bairro, p.cidade, p.uf, p.cep, p.pdv_endereco_completo,
                p.pdv_lat, p.pdv_lon, p.status_geolocalizacao, p.pdv_vendas
            )
            for p in lista_pdvs
        ]

        sql = """
            INSERT INTO pdvs (
                tenant_id, input_id, descricao, cnpj, logradouro, numero, bairro,
                cidade, uf, cep, pdv_endereco_completo,
                pdv_lat, pdv_lon, status_geolocalizacao, pdv_vendas
            )
            VALUES %s
            ON CONFLICT (tenant_id, input_id, cnpj)
            DO NOTHING;
        """

        try:
            with closing(self.conn.cursor()) as cur:
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

    # ============================================================
    # 🗺️ Inserção no cache de endereços (PDV e MKP unificado)
    # ============================================================
    @retry_on_failure()
    def salvar_cache(self, chave: str, lat: float, lon: float, tipo: str = "pdv") -> None:
        if not chave or lat is None or lon is None:
            return

        # Proteção contra coordenadas genéricas
        if abs(lat + 23.5506507) < 0.002 and abs(lon + 46.6333824) < 0.002:
            logging.debug(f"🧹 Coordenada genérica ignorada: {chave} ({lat}, {lon})")
            return

        try:
            with closing(self.conn.cursor()) as cur:
                if tipo.lower() == "mkp":
                    cur.execute(
                        """
                        INSERT INTO mkp_enderecos_cache (cep, lat, lon)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (cep) DO NOTHING;
                        """,
                        (str(chave).zfill(8), lat, lon),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO enderecos_cache (endereco, lat, lon)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (endereco) DO NOTHING;
                        """,
                        (chave.strip().lower(), lat, lon),
                    )
                self.conn.commit()
            logging.debug(f"💾 [CACHE_DB] Inserido no cache ({tipo.upper()}): {chave} → ({lat}, {lon})")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao salvar cache ({tipo}): {e}", exc_info=True)

    # ============================================================
    # 🗺️ Inserção individual em cache PDV
    # ============================================================
    @retry_on_failure()
    def inserir_localizacao(self, endereco: str, lat: float, lon: float) -> None:
        if not endereco or lat is None or lon is None:
            return
        try:
            with closing(self.conn.cursor()) as cur:
                cur.execute(
                    """
                    INSERT INTO enderecos_cache (endereco, lat, lon)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (endereco) DO NOTHING;
                    """,
                    (endereco.strip().lower(), lat, lon),
                )
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao inserir localização no cache: {e}", exc_info=True)

    # ============================================================
    # 🧾 Registro de histórico de execução
    # ============================================================
    @retry_on_failure()
    def salvar_historico_pdv_job(
        self,
        tenant_id: int,
        arquivo: str,
        status: str,
        total_processados: int,
        validos: int,
        invalidos: int,
        arquivo_invalidos: Optional[str] = None,
        mensagem: Optional[str] = None,
        inseridos: int = 0,
        sobrescritos: int = 0,
        descricao: Optional[str] = None,
        input_id: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> None:
        """Salva histórico de execução do pré-processamento de PDVs."""
        job_id = job_id or input_id
        try:
            with closing(self.conn.cursor()) as cur:
                cur.execute(
                    """
                    INSERT INTO historico_pdv_jobs (
                        tenant_id, job_id, arquivo, status, total_processados,
                        validos, invalidos, arquivo_invalidos, mensagem,
                        inseridos, sobrescritos, criado_em, descricao, input_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s);
                    """,
                    (
                        tenant_id, job_id, arquivo, status, total_processados,
                        validos, invalidos, arquivo_invalidos, mensagem,
                        inseridos, sobrescritos, descricao, input_id,
                    ),
                )
                self.conn.commit()
            logging.info(
                f"🧾 Histórico salvo (tenant={tenant_id}, input_id={input_id}, status={status})"
            )
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao salvar histórico (input_id={input_id}): {e}", exc_info=True)

    # ============================================================
    # 💾 Inserção de dados MKP agregados
    # ============================================================
    @retry_on_failure()
    def inserir_mkp(self, lista_mkp) -> int:
        if not lista_mkp:
            logging.warning("⚠️ Nenhum registro MKP para inserir.")
            return 0

        inicio = time.time()
        vistos = set()
        valores_unicos = []
        for m in lista_mkp:
            chave = (m.tenant_id, m.input_id, str(m.cep).zfill(8))
            if chave in vistos:
                continue
            vistos.add(chave)
            valores_unicos.append((
                m.tenant_id, m.input_id, m.descricao, m.cidade, m.uf, m.bairro,
                str(m.cep).zfill(8) if m.cep else None,
                m.clientes_total, m.clientes_target,
                getattr(m, "lat", None), getattr(m, "lon", None),
            ))

        sql = """
            INSERT INTO marketplace_cep (
                tenant_id, input_id, descricao, cidade, uf, bairro, cep,
                clientes_total, clientes_target, lat, lon
            )
            VALUES %s
            ON CONFLICT (tenant_id, input_id, cep)
            DO UPDATE SET
                clientes_total = EXCLUDED.clientes_total,
                clientes_target = EXCLUDED.clientes_target,
                lat = COALESCE(EXCLUDED.lat, marketplace_cep.lat),
                lon = COALESCE(EXCLUDED.lon, marketplace_cep.lon),
                atualizado_em = NOW();
        """

        try:
            with closing(self.conn.cursor()) as cur:
                execute_values(cur, sql, valores_unicos)
                self.conn.commit()
                inseridos = cur.rowcount or len(valores_unicos)
            dur = time.time() - inicio
            logging.info(f"💾 [MKP_DB] Inseridos/atualizados {inseridos} registros ({dur:.2f}s)")
            return inseridos
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao inserir/atualizar marketplace_cep: {e}", exc_info=True)
            return 0

    # ============================================================
    # 💾 Inserção individual no cache MKP
    # ============================================================
    @retry_on_failure()
    def inserir_localizacao_mkp(self, cep: str, lat: float, lon: float) -> None:
        if not cep or lat is None or lon is None:
            return
        try:
            with closing(self.conn.cursor()) as cur:
                cur.execute(
                    """
                    INSERT INTO mkp_enderecos_cache (cep, lat, lon)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (cep) DO NOTHING;
                    """,
                    (str(cep).zfill(8), lat, lon),
                )
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao inserir CEP no cache MKP: {e}", exc_info=True)

    # ============================================================
    # 🔄 Atualização de lat/lon de MKP via cache
    # ============================================================
    @retry_on_failure()
    def atualizar_lat_lon_por_cache(
        self,
        tenant_id: Optional[int] = None,
        uf: Optional[str] = None,
        input_id: Optional[str] = None,
    ) -> int:
        try:
            inicio = time.time()
            sql = """
                UPDATE marketplace_cep AS m
                SET lat = c.lat, lon = c.lon, atualizado_em = NOW()
                FROM mkp_enderecos_cache AS c
                WHERE m.cep = c.cep
                  AND (m.lat IS NULL OR m.lon IS NULL)
                  AND c.lat IS NOT NULL
                  AND c.lon IS NOT NULL
            """
            params = []
            if tenant_id:
                sql += " AND m.tenant_id = %s"; params.append(tenant_id)
            if uf:
                sql += " AND m.uf = %s"; params.append(uf)
            if input_id:
                sql += " AND m.input_id = %s"; params.append(input_id)

            with closing(self.conn.cursor()) as cur:
                cur.execute(sql, tuple(params))
                afetados = cur.rowcount
                self.conn.commit()
            dur = time.time() - inicio
            logging.info(f"🗺️ [MKP_DB] Atualizados {afetados} CEPs com coordenadas do cache ({dur:.2f}s)")
            return afetados
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao atualizar lat/lon a partir do cache: {e}", exc_info=True)
            return 0
