#sales_router/src/pdv_preprocessing/infrastructure/database_writer.py

import logging
import time
from functools import wraps
from typing import Optional, List, Tuple
from psycopg2.extras import execute_values
import psycopg2

from pdv_preprocessing.infrastructure.database_reader import POOL
from pdv_preprocessing.domain.utils_geo import coordenada_generica
from pdv_preprocessing.domain.address_normalizer import normalize_for_cache
from pdv_preprocessing.entities.pdv_entity import PDV


from database.db_connection import get_connection_context

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
                    logging.warning(
                        f"⚠️ Erro de conexão ({func.__name__}) tentativa {tentativa}/{max_retries}: {e}"
                    )
                    time.sleep(delay * (backoff ** (tentativa - 1)))
                except Exception as e:
                    logging.error(f"❌ Erro inesperado em {func.__name__}: {e}", exc_info=True)
                    break

            logging.error(f"🚨 Falha após {max_retries} tentativas em {func.__name__}")
            return None
        return wrapper
    return decorator


class DatabaseWriter:
    def __init__(self):
        pass
    
    # ============================================================
    # 💾 Inserção de PDVs
    # ============================================================
    @retry_on_failure()
    def inserir_pdvs(self, lista_pdvs) -> int:
        if not lista_pdvs:
            return 0

        valores = [
            (
                p.tenant_id,
                p.input_id,
                p.descricao,
                p.cnpj,
                p.logradouro,
                p.numero,
                p.bairro,
                p.cidade,
                p.uf,
                p.cep,
                p.pdv_endereco_completo,
                p.endereco_cache_key,   # 👈 NOVO
                p.pdv_lat,
                p.pdv_lon,
                p.status_geolocalizacao,
                float(p.pdv_vendas) if p.pdv_vendas is not None else None,
            )
            for p in lista_pdvs
        ]

        sql = """
            INSERT INTO pdvs (
                tenant_id,
                input_id,
                descricao,
                cnpj,
                logradouro,
                numero,
                bairro,
                cidade,
                uf,
                cep,
                pdv_endereco_completo,
                endereco_cache_key,
                pdv_lat,
                pdv_lon,
                status_geolocalizacao,
                pdv_vendas
            )
            VALUES %s
            ON CONFLICT (tenant_id, input_id, cnpj)
            DO NOTHING;
        """

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, valores)
            conn.commit()
            return len(valores)

        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Erro ao inserir PDVs: {e}", exc_info=True)
            return 0

        finally:
            POOL.putconn(conn)

    # ============================================================
    # ❌ Excluir processamento completo (por input_id)
    # ============================================================
    @retry_on_failure()
    def excluir_processamento_por_input(
        self,
        tenant_id: int,
        input_id: str
    ) -> bool:
        """
        Exclui todos os PDVs e histórico vinculados a um input_id,
        desde que NÃO exista clusterização associada.
        """

        input_id = str(input_id)

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:

                # ----------------------------------------------------
                # 🔒 1. Verifica se já foi clusterizado
                # ----------------------------------------------------
                cur.execute(
                    """
                    SELECT 1
                    FROM cluster_run
                    WHERE tenant_id = %s
                    AND input_id = %s
                    LIMIT 1;
                    """,
                    (tenant_id, input_id)
                )

                if cur.fetchone():
                    logging.warning(
                        f"🚫 Exclusão bloqueada: input_id={input_id} "
                        f"já vinculado a clusterização (tenant={tenant_id})"
                    )
                    return False

                # ----------------------------------------------------
                # 🗑 2. Exclui PDVs
                # ----------------------------------------------------
                cur.execute(
                    """
                    DELETE FROM pdvs
                    WHERE tenant_id = %s
                    AND input_id = %s;
                    """,
                    (tenant_id, input_id)
                )
                pdvs_excluidos = cur.rowcount

                # ----------------------------------------------------
                # 🗑 3. Exclui histórico
                # ----------------------------------------------------
                cur.execute(
                    """
                    DELETE FROM historico_pdv_jobs
                    WHERE tenant_id = %s
                    AND input_id = %s;
                    """,
                    (tenant_id, input_id)
                )
                historico_excluido = cur.rowcount

            conn.commit()

            # ----------------------------------------------------
            # 📊 Logs
            # ----------------------------------------------------
            if pdvs_excluidos == 0 and historico_excluido == 0:
                logging.warning(
                    f"⚠️ Nenhum registro encontrado para exclusão "
                    f"(tenant={tenant_id}, input_id={input_id})"
                )
            else:
                logging.info(
                    f"🗑 Processamento excluído com sucesso "
                    f"(tenant={tenant_id}, input_id={input_id}) | "
                    f"PDVs removidos={pdvs_excluidos} | "
                    f"Histórico removido={historico_excluido}"
                )

            return True

        except Exception as e:
            conn.rollback()
            logging.error(
                f"❌ Erro ao excluir processamento "
                f"(tenant={tenant_id}, input_id={input_id}): {e}",
                exc_info=True
            )
            return False

        finally:
            POOL.putconn(conn)

    # ============================================================
    # 🗺️ Inserção no cache de endereços (PDV e MKP unificado)
    # ============================================================
    
    @retry_on_failure()
    def salvar_cache(
        self,
        endereco_cache: str,
        lat: float,
        lon: float,
        origem: str = "pipeline",
    ):
        """
        Cache thread-safe.
        Usa UPSERT para evitar race condition.
        - Normaliza endereço
        - Bloqueia coordenada genérica
        - NÃO sobrescreve origem = manual_edit
        """

        # --------------------------------------------------------
        # Validações básicas
        # --------------------------------------------------------
        if not endereco_cache or lat is None or lon is None:
            logging.warning(
                f"[CACHE][IGNORADO] endereco='{endereco_cache}' lat={lat} lon={lon}"
            )
            return

        if coordenada_generica(lat, lon):
            logging.warning(
                f"[CACHE][IGNORADO][GENERICA] endereco='{endereco_cache}' lat={lat} lon={lon}"
            )
            return

        # --------------------------------------------------------
        # Normalização ÚNICA (regra de ouro)
        # --------------------------------------------------------
        endereco_norm = normalize_for_cache(endereco_cache)

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO enderecos_cache (
                        endereco,
                        lat,
                        lon,
                        origem,
                        atualizado_em
                    )
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (endereco)
                    DO UPDATE SET
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        origem = EXCLUDED.origem,
                        atualizado_em = NOW()
                    WHERE enderecos_cache.origem IS DISTINCT FROM 'manual_edit';
                    """,
                    (
                        endereco_norm,
                        lat,
                        lon,
                        origem,
                    ),
                )

                logging.debug(
                    f"[CACHE][UPSERT] origem={origem} | "
                    f"endereco='{endereco_norm}' | "
                    f"lat={lat} lon={lon}"
                )

            conn.commit()

        except Exception as e:
            conn.rollback()
            logging.error(
                f"[CACHE][ERRO] endereco='{endereco_norm}' erro={e}",
                exc_info=True,
            )
            raise

        finally:
            POOL.putconn(conn)



    # ============================================================
    # 💾 ViaCEP Cache — Inserir ou atualizar 1 CEP
    # ============================================================
    @retry_on_failure()
    def salvar_viacep_cache(
        self,
        cep: str,
        logradouro: Optional[str],
        bairro: Optional[str],
        cidade: Optional[str],
        uf: Optional[str]
    ) -> None:

        if not cep:
            return

        cep = str(cep).replace("-", "").strip().zfill(8)

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO viacep_cache (
                        cep, logradouro, bairro, cidade, uf
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (cep)
                    DO UPDATE SET
                        logradouro = EXCLUDED.logradouro,
                        bairro     = EXCLUDED.bairro,
                        cidade     = EXCLUDED.cidade,
                        uf         = EXCLUDED.uf,
                        atualizado_em = NOW();
                    """,
                    (cep, logradouro, bairro, cidade, uf)
                )
            conn.commit()

        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Erro ao salvar viacep_cache para {cep}: {e}", exc_info=True)

        finally:
            POOL.putconn(conn)

    # ============================================================
    # 💾 ViaCEP Cache — Inserção em lote
    # ============================================================
    @retry_on_failure()
    def salvar_viacep_cache_em_lote(self, lista_dados: List[Tuple[str, str, str, str, str]]) -> int:
        """
        lista_dados = [(cep, logradouro, bairro, cidade, uf), ...]
        """
        if not lista_dados:
            return 0

        valores = [
            (str(cep).replace("-", "").strip().zfill(8), logradouro, bairro, cidade, uf)
            for (cep, logradouro, bairro, cidade, uf) in lista_dados
        ]

        sql = """
            INSERT INTO viacep_cache (
                cep, logradouro, bairro, cidade, uf
            )
            VALUES %s
            ON CONFLICT (cep)
            DO UPDATE SET
                logradouro = EXCLUDED.logradouro,
                bairro     = EXCLUDED.bairro,
                cidade     = EXCLUDED.cidade,
                uf         = EXCLUDED.uf,
                atualizado_em = NOW();
        """

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, valores)
            conn.commit()
            return len(valores)

        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Erro ao salvar lote ViaCEP cache: {e}", exc_info=True)
            return 0

        finally:
            POOL.putconn(conn)

    
    # ============================================================
    # 🧾 Registro de histórico de execução PDV
    # ============================================================
    @retry_on_failure()
    def salvar_historico_pdv_job(
        self,
        tenant_id: int,
        job_id: str,
        arquivo: str,
        status: str,
        total_processados: int = 0,
        validos: int = 0,
        invalidos: int = 0,
        inseridos: int = 0,
        sobrescritos: int = 0,
        arquivo_invalidos: Optional[str] = None,
        mensagem: Optional[str] = None,
        descricao: Optional[str] = None,
        input_id: Optional[str] = None,
    ) -> None:

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO historico_pdv_jobs (
                        tenant_id, job_id, arquivo, status,
                        total_processados, validos, invalidos,
                        inseridos, sobrescritos,
                        arquivo_invalidos, mensagem, descricao,
                        input_id, criado_em
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, NOW()
                    )
                    ON CONFLICT (tenant_id, job_id)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        total_processados = EXCLUDED.total_processados,
                        validos = EXCLUDED.validos,
                        invalidos = EXCLUDED.invalidos,
                        inseridos = EXCLUDED.inseridos,
                        sobrescritos = EXCLUDED.sobrescritos,
                        arquivo_invalidos = EXCLUDED.arquivo_invalidos,
                        mensagem = EXCLUDED.mensagem,
                        descricao = EXCLUDED.descricao,
                        input_id = EXCLUDED.input_id,
                        atualizado_em = NOW();

                    """,
                    (
                        tenant_id,
                        str(job_id),
                        arquivo,
                        status,
                        total_processados,
                        validos,
                        invalidos,
                        inseridos,
                        sobrescritos,
                        arquivo_invalidos,
                        mensagem,
                        descricao,
                        str(input_id) if input_id else None,
                    ),
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Erro ao salvar histórico PDV: {e}", exc_info=True)
        finally:
            POOL.putconn(conn)

    @retry_on_failure()
    def salvar_historico_mkp_job(
        self,
        tenant_id: int,
        job_id: str | None,
        arquivo: str,
        status: str,
        total_processados: int,
        validos: int,
        invalidos: int,
        arquivo_invalidos: Optional[str],
        arquivo_validos: Optional[str],
        mensagem: str,
        inseridos: int,
        sobrescritos: int,
        descricao: str,
        input_id: str,
    ):
        # 🔒 GARANTIA ABSOLUTA
        if not job_id:
            job_id = str(uuid.uuid4())

        query = """
            INSERT INTO historico_mkp_jobs (
                tenant_id, job_id, arquivo, status,
                total_processados, validos, invalidos,
                arquivo_invalidos, arquivo_validos,
                mensagem, inseridos, sobrescritos,
                descricao, input_id, criado_em
            )
            VALUES (%s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, NOW());
        """

        params = (
            tenant_id, job_id, arquivo, status,
            total_processados, validos, invalidos,
            arquivo_invalidos, arquivo_validos,
            mensagem, inseridos, sobrescritos,
            descricao, input_id,
        )

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Erro ao salvar histórico MKP: {e}", exc_info=True)
        finally:
            POOL.putconn(conn)


    
    @retry_on_failure()
    def marcar_falhas_mkp(self, tenant_id: int, input_id: str) -> int:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE marketplace_cep
                    SET status_geolocalizacao = 'geo_fail',
                        atualizado_em = NOW()
                    WHERE tenant_id = %s
                    AND input_id = %s
                    AND status_geolocalizacao = 'pending'
                    AND (lat IS NULL OR lon IS NULL);
                    """,
                    (tenant_id, input_id)
                )
                return cur.rowcount


    @retry_on_failure()
    def atualizar_marketplace_coord(
        self,
        mkp_id: str,
        lat: float,
        lon: float,
        status: str,
        tenant_id: int,
        input_id: str,
    ):
        if not mkp_id:
            logging.warning("⚠️ atualizar_marketplace_coord chamado sem mkp_id")
            return

        # proteção → nunca sobrescrever lat/lon inválidos (exceto geo_fail)
        if status != "geo_fail":
            if lat is None or lon is None or coordenada_generica(lat, lon):
                logging.warning(
                    f"⚠️ Ignorando update inválido para MKP_ID {mkp_id} — lat/lon inválidos"
                )
                return

        sql = """
            UPDATE marketplace_cep
            SET lat = %s,
                lon = %s,
                status_geolocalizacao = %s,
                atualizado_em = NOW()
            WHERE tenant_id = %s
            AND input_id = %s
            AND mkp_id = %s
        """

        params = (
            lat,
            lon,
            status,
            tenant_id,
            str(input_id),
            str(mkp_id),
        )

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)

                if cur.rowcount == 0:
                    logging.warning(
                        f"⚠️ Nenhuma linha atualizada em marketplace_cep "
                        f"(mkp_id={mkp_id}, tenant={tenant_id}, input_id={input_id})"
                    )

            conn.commit()

        except Exception as e:
            conn.rollback()
            logging.error(
                f"❌ Erro ao atualizar marketplace_cep (mkp_id={mkp_id}): {e}",
                exc_info=True,
            )

        finally:
            POOL.putconn(conn)

    @retry_on_failure()
    def inserir_mkp_sem_geo(
        self,
        df_validos,
        tenant_id: int,
        input_id: str,
        descricao: str
    ) -> int:

        if df_validos is None or df_validos.empty:
            return 0

        registros = {}

        for _, row in df_validos.iterrows():
            mkp_id = row.get("mkp_id")
            if not mkp_id:
                continue  # blindagem

            cep = str(row.get("cep", "")).replace("-", "").zfill(8)

            chave = (tenant_id, input_id, mkp_id)

            registros[chave] = (
                tenant_id,
                str(input_id),
                str(mkp_id),  # ← CONVERTE AQUI
                descricao.strip()[:60],
                row.get("cidade", "").strip().upper(),
                row.get("uf", "").strip().upper(),
                (str(row.get("bairro") or "")).strip().upper(),
                cep,
                int(row.get("clientes_total", 0)),
                int(row.get("clientes_target", 0)),
                None,
                None,
                "pending"
            )


        valores = list(registros.values())
        if not valores:
            return 0

        sql = """
            INSERT INTO marketplace_cep (
                tenant_id,
                input_id,
                mkp_id,
                descricao,
                cidade,
                uf,
                bairro,
                cep,
                clientes_total,
                clientes_target,
                lat,
                lon,
                status_geolocalizacao
            )
            VALUES %s
            ON CONFLICT (tenant_id, input_id, mkp_id)
            DO UPDATE SET
                descricao = EXCLUDED.descricao,
                cidade = EXCLUDED.cidade,
                uf = EXCLUDED.uf,
                bairro = EXCLUDED.bairro,
                clientes_total = EXCLUDED.clientes_total,
                clientes_target = EXCLUDED.clientes_target,
                lat = NULL,
                lon = NULL,
                status_geolocalizacao = 'pending',
                atualizado_em = NOW();

                    """

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, valores)
            conn.commit()
            return len(valores)

        except Exception as e:
            conn.rollback()
            logging.error("❌ Erro ao inserir MKP sem geo", exc_info=True)
            return 0

        finally:
            POOL.putconn(conn)



    # ============================================================
    # 🗺️ Salvar resultado da geocodificação (padronizado)
    # ============================================================
    @retry_on_failure()
    def salvar_geocode(
        self,
        mkp_id: str,
        lat: float,
        lon: float,
        origem: str,
        tenant_id: int,
        input_id: str,
    ):
        status = "geo_fail" if origem == "geo_fail" else "ok"

        if status != "geo_fail":
            if lat is None or lon is None or coordenada_generica(lat, lon):
                logging.warning(
                    f"⚠️ salvar_geocode ignorado (lat/lon inválidos) mkp_id={mkp_id}"
                )
                return

        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE marketplace_cep
                    SET lat = %s,
                        lon = %s,
                        status_geolocalizacao = %s,
                        atualizado_em = NOW()
                    WHERE tenant_id = %s
                    AND input_id = %s
                    AND mkp_id = %s;
                    """,
                    (lat, lon, status, tenant_id, input_id, mkp_id),
                )

                if cur.rowcount == 0:
                    logging.warning(
                        f"⚠️ salvar_geocode não atualizou nenhuma linha "
                        f"(mkp_id={mkp_id}, tenant={tenant_id}, input={input_id})"
                    )

    # ============================================================
    # 🔄 Buscar endereço no cache com base em lat/lon
    # ============================================================
    @retry_on_failure()
    def buscar_endereco_por_coordenada(self, lat: float, lon: float) -> Optional[str]:
        """
        Busca no cache (enderecos_cache) o endereço correspondente à coordenada.
        Retorna o endereço original (string) ou None caso não exista.
        """

        if lat is None or lon is None:
            return None

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT endereco
                    FROM enderecos_cache
                    WHERE abs(lat - %s) < 0.000001
                        AND abs(lon - %s) < 0.000001
                    LIMIT 1;
                    """,
                    (lat, lon),
                )
                row = cur.fetchone()

            if row:
                return row[0]

            return None

        except Exception as e:
            logging.error(f"❌ Erro ao buscar endereço por coordenada: {e}", exc_info=True)
            return None

        finally:
            POOL.putconn(conn)

    # ============================================================
    # 📝 Atualizar endereço completo do PDV
    # ============================================================
    @retry_on_failure()
    def atualizar_endereco_pdv(self, pdv_id: int, novo_endereco: str) -> bool:
        """
        Atualiza pdv_endereco_completo no banco para o PDV informado.
        """

        if not novo_endereco:
            return False

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pdvs
                    SET pdv_endereco_completo = %s,
                        atualizado_em = NOW()
                    WHERE id = %s
                    """,
                    (novo_endereco, pdv_id),
                )
            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Erro ao atualizar endereço do PDV: {e}", exc_info=True)
            return False

        finally:
            POOL.putconn(conn)

    
    # ============================================================
    # 🔍 Buscar coordenadas no cache com base NO ENDEREÇO NORMALIZADO
    # ============================================================
    @retry_on_failure()
    def buscar_por_endereco(self, endereco_completo: str) -> Optional[Tuple[float, float]]:
        """
        Busca coordenadas no cache a partir do endereço COMPLETO.
        ⚠️ Método legado. Evitar uso em novos fluxos.
        """

        if not endereco_completo:
            return None

        # Normaliza usando a mesma regra do pipeline
        endereco_norm = normalize_for_cache(endereco_completo)

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT lat, lon
                    FROM enderecos_cache
                    WHERE endereco = %s
                    LIMIT 1;
                    """,
                    (endereco_norm,)
                )
                row = cur.fetchone()
                return (row[0], row[1]) if row else None

        except Exception as e:
            logging.error(f"❌ Erro ao buscar_por_endereco: {e}", exc_info=True)
            return None

        finally:
            POOL.putconn(conn)



    # ============================================================
    # ✏️ Atualizar lat/lon do PDV (edição manual)
    # ============================================================
    @retry_on_failure()
    def atualizar_lat_lon_pdv(
        self,
        pdv_id: int,
        lat: float,
        lon: float,
        tenant_id: int,
    ) -> bool:
        """
        Atualiza APENAS lat/lon do PDV.
        Uso exclusivo para edição manual.
        Protegido por tenant_id.
        """

        # --------------------------------------------------------
        # Validações mínimas
        # --------------------------------------------------------
        if lat is None or lon is None:
            logging.warning("⚠️ atualizar_lat_lon_pdv chamado com lat/lon nulos.")
            return False

        if coordenada_generica(lat, lon):
            logging.warning(
                f"⚠️ Coordenada genérica ignorada para PDV {pdv_id}: lat={lat}, lon={lon}"
            )
            return False

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pdvs
                    SET
                        pdv_lat = %s,
                        pdv_lon = %s,
                        status_geolocalizacao = 'manual_edit',
                        atualizado_em = NOW()
                    WHERE id = %s
                    AND tenant_id = %s
                    """,
                    (lat, lon, pdv_id, tenant_id),
                )

            conn.commit()

            if cur.rowcount == 0:
                logging.warning(
                    f"⚠️ Nenhum PDV atualizado (id={pdv_id}, tenant_id={tenant_id})."
                )
                return False

            logging.info(
                f"📝 PDV {pdv_id} (tenant={tenant_id}) atualizado manualmente → "
                f"lat={lat}, lon={lon}"
            )
            return True

        except Exception as e:
            conn.rollback()
            logging.error(
                f"❌ Erro ao atualizar_lat_lon_pdv "
                f"(pdv_id={pdv_id}, tenant_id={tenant_id}): {e}",
                exc_info=True,
            )
            return False

        finally:
            POOL.putconn(conn)


    # ============================================================
    # ✏️ Atualizar lat/lon no cache usando o ENDEREÇO NORMALIZADO
    # ============================================================
    @retry_on_failure()
    def atualizar_cache_por_endereco(
        self,
        endereco_completo: str,
        nova_lat: float,
        nova_lon: float
    ) -> bool:
        """
        Atualiza o cache (enderecos_cache) para o endereço COMPLETO informado.
        Usa a chave normalizada, igual ao pipeline.
        """

        if not endereco_completo or nova_lat is None or nova_lon is None:
            logging.warning("⚠️ atualizar_cache_por_endereco chamado com dados inválidos.")
            return False

        if coordenada_generica(nova_lat, nova_lon):
            logging.warning(
                f"⚠️ Coordenada suspeita ignorada ao atualizar cache: {endereco_completo}"
            )
            return False

        endereco_norm = normalize_for_cache(endereco_completo)

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE enderecos_cache
                    SET
                        lat = %s,
                        lon = %s,
                        origem = 'manual_edit',
                        atualizado_em = NOW()
                    WHERE endereco = %s
                    """,
                    (nova_lat, nova_lon, endereco_norm)
                )

            conn.commit()

            if cur.rowcount > 0:
                logging.info(
                    f"📝 Cache atualizado (manual_edit) | '{endereco_norm}' "
                    f"→ {nova_lat}, {nova_lon}"
                )
                return True

            logging.warning(
                f"⚠️ Cache não encontrado para '{endereco_norm}'. Criando registro manual_edit."
            )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO enderecos_cache (endereco, lat, lon, origem)
                    VALUES (%s, %s, %s, 'manual_edit')
                    ON CONFLICT (endereco)
                    DO UPDATE SET
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        origem = 'manual_edit',
                        atualizado_em = NOW()
                    """,
                    (endereco_norm, nova_lat, nova_lon)
                )

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logging.error(
                f"❌ Erro ao atualizar cache por endereço: {e}",
                exc_info=True
            )
            return False

        finally:
            POOL.putconn(conn)


    # ============================================================
    # ❌ Excluir PDV (com proteção por tenant_id)
    # ============================================================
    @retry_on_failure()
    def excluir_pdv(self, pdv_id: int, tenant_id: int) -> bool:
        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM pdvs
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (pdv_id, tenant_id)
                )
            conn.commit()
            return cur.rowcount > 0

        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Erro ao excluir PDV: {e}", exc_info=True)
            return False

        finally:
            POOL.putconn(conn)

    @retry_on_failure()
    def atualizar_pdv_completo(self, pdv: PDV) -> bool:
        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pdvs
                    SET
                        logradouro = %s,
                        numero = %s,
                        bairro = %s,
                        cidade = %s,
                        uf = %s,
                        cep = %s,
                        pdv_lat = %s,
                        pdv_lon = %s,
                        pdv_endereco_completo = %s,
                        status_geolocalizacao = %s,
                        atualizado_em = NOW()
                    WHERE tenant_id = %s AND cnpj = %s
                    """,
                    (
                        pdv.logradouro,
                        pdv.numero,
                        pdv.bairro,
                        pdv.cidade,
                        pdv.uf,
                        pdv.cep,
                        pdv.pdv_lat,
                        pdv.pdv_lon,
                        pdv.pdv_endereco_completo,
                        pdv.status_geolocalizacao,
                        pdv.tenant_id,
                        pdv.cnpj,
                    )
                )
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Erro ao atualizar PDV: {e}", exc_info=True)
            return False
        finally:
            POOL.putconn(conn)

    # ============================================================
    # ✏️ Atualizar lat/lon no cache usando CHAVE CANÔNICA (CORRIGIDO)
    # ============================================================
    @retry_on_failure()
    def atualizar_cache_por_chave(self, cache_key: str, nova_lat: float, nova_lon: float) -> bool:
        if not cache_key or nova_lat is None or nova_lon is None:
            return False

        if coordenada_generica(nova_lat, nova_lon):
            return False

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE enderecos_cache
                    SET
                        lat = %s,
                        lon = %s,
                        origem = 'manual_edit',
                        atualizado_em = NOW()
                    WHERE endereco = %s
                    """,
                    (nova_lat, nova_lon, cache_key),
                )

                if cur.rowcount == 0:
                    logging.warning(
                        f"⚠️ Cache não encontrado para chave '{cache_key}', inserindo manual_edit"
                    )

                    cur.execute(
                        """
                        INSERT INTO enderecos_cache (endereco, lat, lon, origem, atualizado_em)
                        VALUES (%s, %s, %s, 'manual_edit', NOW())
                        ON CONFLICT (endereco)
                        DO UPDATE SET
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            origem = 'manual_edit',
                            atualizado_em = NOW()
                        """,
                        (cache_key, nova_lat, nova_lon),
                    )

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logging.error(f"Erro ao atualizar cache: {e}", exc_info=True)
            return False

        finally:
            POOL.putconn(conn)




    @retry_on_failure()
    def buscar_cache_key_pdv(self, pdv_id: int, tenant_id: int) -> str | None:
        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT endereco_cache_key
                    FROM pdvs
                    WHERE id = %s
                    AND tenant_id = %s
                    """,
                    (pdv_id, tenant_id),
                )
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            POOL.putconn(conn)

    