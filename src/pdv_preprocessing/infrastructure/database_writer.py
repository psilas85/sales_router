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

        valores = [
            (
                p.tenant_id, p.input_id, p.descricao, p.cnpj, p.logradouro, p.numero, p.bairro,
                p.cidade, p.uf, p.cep, p.pdv_endereco_completo,
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
    # 🗺️ Inserção no cache de endereços (PDV e MKP unificado)
    # ============================================================
    def salvar_cache(self, chave: str, lat: float, lon: float, tipo: str = "pdv"):
        """
        Insere coordenadas no cache adequado (enderecos_cache ou mkp_enderecos_cache).
        - tipo='pdv' → usa campo 'endereco'
        - tipo='mkp' → usa campo 'cep'
        Evita coordenadas genéricas e duplicadas.
        """
        if not chave or lat is None or lon is None:
            return

        # 🔎 Proteção contra coordenadas genéricas
        if abs(lat + 23.5506507) < 0.002 and abs(lon + 46.6333824) < 0.002:
            logging.debug(f"🧹 Coordenada genérica ignorada: {chave} ({lat}, {lon})")
            return

        try:
            cur = self.conn.cursor()

            if tipo.lower() == "mkp":
                sql = """
                    INSERT INTO mkp_enderecos_cache (cep, lat, lon)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (cep) DO NOTHING;
                """
                params = (str(chave).zfill(8), lat, lon)
            else:
                sql = """
                    INSERT INTO enderecos_cache (endereco, lat, lon)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (endereco) DO NOTHING;
                """
                params = (chave.strip().lower(), lat, lon)

            cur.execute(sql, params)
            self.conn.commit()
            cur.close()
            logging.debug(f"💾 [CACHE_DB] Inserido no cache ({tipo.upper()}): {chave} → ({lat}, {lon})")

        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao salvar cache ({tipo}): {e}", exc_info=True)
            
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


    # ============================================================
    # 💾 Inserção de dados MKP agregados por CEP
    # ============================================================
    def inserir_mkp(self, lista_mkp):
        if not lista_mkp:
            logging.warning("⚠️ Nenhum registro MKP para inserir.")
            return 0

        inicio = time.time()
        cur = self.conn.cursor()

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
                getattr(m, "lat", None), getattr(m, "lon", None)
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
        finally:
            cur.close()


    # ============================================================
    # 💾 Inserção no cache MKP (marketplace)
    # ============================================================
    def inserir_localizacao_mkp(self, cep: str, lat: float, lon: float):
        """
        Insere um novo CEP no cache MKP (único e global).
        Ignora se o CEP já existir.
        """
        if not cep or lat is None or lon is None:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO mkp_enderecos_cache (cep, lat, lon)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (cep) DO NOTHING;
                """, (str(cep).zfill(8), lat, lon))
                self.conn.commit()
        except Exception as e:
            logging.error(f"❌ Erro ao inserir CEP no cache MKP: {e}")

    # ============================================================
    # 🔄 Atualiza lat/lon de marketplace_cep com base no cache MKP
    # ============================================================
    def atualizar_lat_lon_por_cache(self, tenant_id: int = None, uf: str = None, input_id: str = None):
        try:
            inicio = time.time()
            cur = self.conn.cursor()

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

            cur.execute(sql, tuple(params))
            afetados = cur.rowcount
            self.conn.commit()
            cur.close()

            dur = time.time() - inicio
            logging.info(f"🗺️ [MKP_DB] Atualizados {afetados} CEPs com coordenadas do cache ({dur:.2f}s)")
            return afetados
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Erro ao atualizar lat/lon a partir do cache: {e}", exc_info=True)
            return 0

    