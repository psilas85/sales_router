#sales_router/src/pdv_preprocessing/cep_area_geocoding/infrastructure/database_writer.py
# ============================================================
# 📦 src/pdv_preprocessing/cep_area_geocoding/infrastructure/database_writer.py
# ============================================================

from database.db_connection import get_connection_context
from loguru import logger


class DatabaseWriter:

    # --------------------------------------------------------
    def salvar_cache_bairro(
        self, tenant_id, cep, bairro, cidade, uf,
        endereco_key, lat, lon, origem
    ):
        cep = str(cep).strip()
        bairro = (bairro or "").strip()
        cidade = (cidade or "").strip()
        uf = (uf or "").strip()
        endereco_key = (endereco_key or "").strip()
        origem = (origem or "").strip()

        if lat is None or lon is None:
            logger.error(f"❌ Tentativa de salvar lat/lon NULL | CEP={cep}")
            return

        with get_connection_context() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO cep_bairro_cache
                            (tenant_id, cep, bairro, cidade, uf, endereco_key,
                             lat, lon, origem)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tenant_id, cep)
                        DO UPDATE SET
                            bairro = EXCLUDED.bairro,
                            cidade = EXCLUDED.cidade,
                            uf = EXCLUDED.uf,
                            endereco_key = EXCLUDED.endereco_key,
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            origem = EXCLUDED.origem,
                            atualizado_em = NOW();
                    """, (
                        tenant_id, cep, bairro, cidade, uf,
                        endereco_key, lat, lon, origem
                    ))

                conn.commit()
                logger.debug(
                    f"💾 cache_write | CEP={cep} | origem={origem} "
                    f"| latlon=({lat},{lon})"
                )

            except Exception as e:
                logger.error(f"❌ Erro ao salvar cache CEP={cep}: {e}")
                conn.rollback()
                raise


    # --------------------------------------------------------
    def registrar_historico_geocoding(self, tenant_id, input_id, descricao, total):
        descricao = descricao or ""
        input_id = str(input_id)

        with get_connection_context() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO historico_cep_bairro
                            (tenant_id, input_id, descricao, total_processados)
                        VALUES (%s, %s, %s, %s);
                    """, (tenant_id, input_id, descricao, total))

                conn.commit()

                logger.info(
                    f"📝 histórico salvo | input_id={input_id} "
                    f"| total={total} | tenant={tenant_id}"
                )

            except Exception as e:
                logger.error(
                    f"❌ Erro ao salvar histórico (input_id={input_id}): {e}"
                )
                conn.rollback()
                raise


    # --------------------------------------------------------
    # 🔥 VERSÃO FINAL — SALVA clientes_total E clientes_target
    # --------------------------------------------------------
    def salvar_marketplace_cep(
        self,
        tenant_id,
        input_id,
        descricao,
        cep,
        bairro,
        cidade,
        uf,
        clientes_total,
        clientes_target,
        lat,
        lon,
        origem
    ):
        cep = str(cep).strip()
        bairro = (bairro or "").strip()
        cidade = (cidade or "").strip()
        uf = (uf or "").strip()
        origem = origem or ""
        descricao = descricao or ""

        # força numérico seguro
        try:
            clientes_total = int(clientes_total)
        except:
            clientes_total = 0

        try:
            clientes_target = int(clientes_target)
        except:
            clientes_target = 0

        with get_connection_context() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO marketplace_cep
                            (tenant_id, input_id, descricao, cidade, uf, bairro,
                             cep, clientes_total, clientes_target,
                             lat, lon, status_geolocalizacao)
                        VALUES (%s, %s, %s, %s, %s, %s,
                                %s, %s, %s,
                                %s, %s, %s)
                        ON CONFLICT (tenant_id, input_id, cep)
                        DO UPDATE SET
                            descricao = EXCLUDED.descricao,
                            cidade = EXCLUDED.cidade,
                            uf = EXCLUDED.uf,
                            bairro = EXCLUDED.bairro,
                            clientes_total = EXCLUDED.clientes_total,
                            clientes_target = EXCLUDED.clientes_target,
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            status_geolocalizacao = EXCLUDED.status_geolocalizacao,
                            atualizado_em = NOW();
                    """, (
                        tenant_id,
                        input_id,
                        descricao,
                        cidade,
                        uf,
                        bairro,
                        cep,
                        clientes_total,
                        clientes_target,
                        lat,
                        lon,
                        origem
                    ))

                conn.commit()

                logger.debug(
                    f"💾 marketplace_write | CEP={cep} | "
                    f"clientes_total={clientes_total} | clientes_target={clientes_target}"
                )

            except Exception as e:
                logger.error(f"❌ Erro ao salvar marketplace CEP={cep}: {e}")
                conn.rollback()
                raise
