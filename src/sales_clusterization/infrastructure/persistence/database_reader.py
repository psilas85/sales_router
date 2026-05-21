#sales_router/src/sales_clusterization/infrastructure/persistence/database_reader.py

# ============================================================
# 📦 src/sales_clusterization/infrastructure/persistence/database_reader.py
# ============================================================

from typing import List, Optional
import numpy as np
from src.database.db_connection import get_connection
from src.sales_clusterization.domain.entities import PDV
from loguru import logger


_SCHEMAS_VALIDOS = ("public", "operacional")


def carregar_pdvs(
    tenant_id: int,
    input_id: str,
    uf=None,  # str | List[str] | None
    cidade: Optional[str] = None,
    schema: str = "public",
) -> List[PDV]:
    # schema da pipeline: 'public' (Simulação) ou 'operacional' (Execução
    # Operacional). Em operacional, a conexão roda com search_path
    # operacional, public → `pdvs` resolve em operacional.pdvs.
    if schema not in _SCHEMAS_VALIDOS:
        raise ValueError(f"schema inválido: {schema!r}")

    base_query = """
        SELECT id, cnpj, bairro, cidade, uf, pdv_lat, pdv_lon
        FROM pdvs
        WHERE tenant_id = %s
          AND input_id = %s
    """
    params = [tenant_id, input_id]

    # uf aceita str (1 UF), lista (>=2 UFs) ou None (todas).
    # Lista vazia equivale a None — não filtra.
    if isinstance(uf, (list, tuple)):
        ufs_norm = [str(u).strip().upper() for u in uf if str(u).strip()]
        if len(ufs_norm) == 1:
            base_query += " AND UPPER(uf) = %s"
            params.append(ufs_norm[0])
        elif len(ufs_norm) > 1:
            base_query += " AND UPPER(uf) = ANY(%s)"
            params.append(ufs_norm)
    elif uf:
        base_query += " AND UPPER(uf) = UPPER(%s)"
        params.append(uf)
    if cidade:
        base_query += " AND UPPER(cidade) = UPPER(%s)"
        params.append(cidade)

    base_query += ";"

    with get_connection() as conn:
        if schema != "public":
            with conn.cursor() as _c:
                _c.execute(f"SET search_path TO {schema}, public")
        with conn.cursor() as cur:
            cur.execute(base_query, tuple(params))
            rows = cur.fetchall()

    if not rows:
        logger.warning(
            f"⚠️ Nenhum PDV encontrado | tenant={tenant_id}, input_id={input_id}, "
            f"UF={uf or 'todas'}, cidade={cidade or 'todas'}"
        )
        return []

    pdvs_limp = []
    invalidos = 0
    duplicadas = 0
    coords_vistos = set()

    for row in rows:
        try:
            _id, cnpj, bairro, cidade_, uf_, lat, lon = row

            # --- VALIDAÇÃO CRÍTICA ---
            if lat is None or lon is None:
                invalidos += 1
                continue

            if np.isnan(lat) or np.isnan(lon):
                invalidos += 1
                continue

            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                invalidos += 1
                continue

            # Duplicatas (apenas log)
            chave = (round(lat, 6), round(lon, 6))
            if chave in coords_vistos:
                duplicadas += 1
            coords_vistos.add(chave)

            # --- CRIA OBJETO PDV ---
            pdv = PDV(
                id=_id,
                cnpj=cnpj,
                nome=None,
                cidade=cidade_,
                uf=uf_,
                lat=float(lat),
                lon=float(lon),
            )
            pdvs_limp.append(pdv)

        except Exception as e:
            invalidos += 1
            logger.warning(f"⚠️ Erro ao processar PDV id={row[0] if row else 'N/A'}: {e}")
            continue

    # --- INDEXAÇÃO CRUCIAL PARA SWEEP E BALANCEADO ---
    for idx, p in enumerate(pdvs_limp):
        p.original_index = idx

    uf_label = (
        ",".join(uf) if isinstance(uf, (list, tuple)) and uf else (uf or "todas")
    )
    logger.info(
        f"📦 {len(pdvs_limp)} PDVs carregados | tenant={tenant_id} | input_id={input_id} | "
        f"UF={uf_label} | cidade={cidade or 'todas'} | 🧹 {invalidos} inválidos | ⚠️ {duplicadas} duplicadas"
    )

    return pdvs_limp



def get_cidades_por_uf(tenant_id: int, uf: str, input_id: str) -> list[str]:
    """
    Retorna lista de cidades com PDVs válidos (com lat/lon) na UF e input_id informados.
    """
    query = """
        SELECT DISTINCT cidade
        FROM pdvs
        WHERE tenant_id = %s
          AND input_id = %s
          AND UPPER(uf) = UPPER(%s)
          AND pdv_lat IS NOT NULL
          AND pdv_lon IS NOT NULL
        ORDER BY cidade;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, input_id, uf))
            rows = cur.fetchall()

    cidades = [r[0] for r in rows] if rows else []
    logger.info(
        f"🌎 {len(cidades)} cidades encontradas | tenant={tenant_id} | UF={uf} | input_id={input_id}"
    )
    return cidades



def carregar_clusters(tenant_id: int, run_id: int):
        """
        Lê os clusters principais (centros) gerados pela clusterização.
        Retorna lista de dicionários com centro_lat/lon e id do cluster.
        """
        sql = """
            SELECT 
                id AS cluster_id,
                cluster_label,
                centro_lat,
                centro_lon,
                n_pdvs
            FROM cluster_setor
            WHERE tenant_id = %s AND run_id = %s
            ORDER BY cluster_label;
        """

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, run_id))
                rows = cur.fetchall()

        clusters = [
            {
                "cluster_id": r[0],
                "cluster_label": r[1],
                "centro_lat": float(r[2]),
                "centro_lon": float(r[3]),
                "n_pdvs": int(r[4]),
            }
            for r in rows
        ]

        logger.info(f"📍 {len(clusters)} clusters carregados (tenant={tenant_id}, run_id={run_id})")
        return clusters


def carregar_pdvs_por_clusters(tenant_id: int, run_id: int):
        """
        Retorna lista de PDVs agrupados por cluster_id.
        Campos: pdv_id, cluster_id, lat, lon, cidade, uf
        """
        sql = """
            SELECT 
                pdv_id,
                cluster_id,
                lat,
                lon,
                cidade,
                uf
            FROM cluster_setor_pdv
            WHERE tenant_id = %s AND run_id = %s
            AND lat IS NOT NULL AND lon IS NOT NULL;
        """

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, run_id))
                rows = cur.fetchall()

        pdvs = [
            {
                "pdv_id": int(r[0]),
                "cluster_id": int(r[1]),
                "lat": float(r[2]),
                "lon": float(r[3]),
                "cidade": r[4],
                "uf": r[5],
            }
            for r in rows
        ]

        logger.info(f"🧩 {len(pdvs)} PDVs carregados (tenant={tenant_id}, run_id={run_id})")
        return pdvs




# ============================================================
# 🔄 Compatibilidade para uso em cluster_cep_use_case
# ============================================================

class DatabaseReader:
    """
    Wrapper compatível para chamadas padrão de leitura.
    Encapsula as funções existentes neste módulo.
    """
    def __init__(self, conn):
        self.conn = conn

    def buscar_marketplace_ceps(self, tenant_id: int, uf: str, input_id: str, cidade: str = None):
        """
        Retorna lista de CEPs georreferenciados do marketplace.
        Filtros:
        - tenant_id (obrigatório)
        - uf (obrigatório)
        - input_id (obrigatório)
        - cidade (opcional)
        """
        cur = self.conn.cursor()

        sql = """
            SELECT cep, lat, lon, clientes_total, clientes_target
            FROM marketplace_cep
            WHERE tenant_id = %s
            AND uf = %s
            AND input_id = %s
            AND lat IS NOT NULL
            AND lon IS NOT NULL
        """
        params = [tenant_id, uf, input_id]

        if cidade:
            sql += " AND UPPER(cidade) = UPPER(%s)"
            params.append(cidade)

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        cur.close()
        return rows

    def buscar_ceps_pdv(self, tenant_id: int, input_id: str, uf: str, cidade: str = None):
        """
        Retorna CEPs vindos dos PDVs com coordenadas.
        Formato compatível com clusterização de CEP:
        cep, lat, lon, clientes_total, clientes_target
        """
        cur = self.conn.cursor()

        sql = """
            SELECT 
                cep,
                pdv_lat AS lat,
                pdv_lon AS lon,
                1 AS clientes_total,
                1 AS clientes_target
            FROM pdvs
            WHERE tenant_id = %s
            AND input_id = %s
            AND UPPER(uf) = UPPER(%s)
            AND cep IS NOT NULL
            AND cep <> ''
            AND pdv_lat IS NOT NULL
            AND pdv_lon IS NOT NULL
        """

        params = [tenant_id, input_id, uf]

        if cidade:
            sql += " AND UPPER(cidade) = UPPER(%s)"
            params.append(cidade)

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        cur.close()
        return rows

    def buscar_ceps(
        self,
        usar_marketplace: bool,
        tenant_id: int,
        input_id: str,
        uf: str,
        cidade: str = None
    ):
        """
        Método unificado.
        - Se usar_marketplace=True → marketplace_cep
        - Caso contrário → CEPs de PDVs
        """
        if usar_marketplace:
            return self.buscar_marketplace_ceps(
                tenant_id=tenant_id,
                uf=uf,
                input_id=input_id,
                cidade=cidade
            )
        else:
            return self.buscar_ceps_pdv(
                tenant_id=tenant_id,
                input_id=input_id,
                uf=uf,
                cidade=cidade
            )


    # ============================================================
    # 🔍 Busca coordenadas no cache de endereços
    # ============================================================
    def buscar_localizacao(self, endereco: str):
        """
        Busca coordenadas no cache (enderecos_cache) pelo campo 'endereco'.
        Retorna (lat, lon) se encontrado, senão None.
        """
        if not endereco:
            return None

        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT lat, lon
                FROM enderecos_cache
                WHERE endereco = %s
                LIMIT 1;
            """, (endereco,))
            row = cur.fetchone()
            cur.close()

            if row and row[0] is not None and row[1] is not None:
                return row  # (lat, lon)
            return None

        except Exception as e:
            import logging
            logging.warning(f"⚠️ Erro ao consultar cache de endereços: {e}")
            return None

    def buscar_endereco_cache(self, endereco: str):
        if not endereco:
            return None

        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT endereco, lat, lon, origem
                FROM enderecos_cache
                WHERE endereco = %s
                LIMIT 1;
            """, (endereco,))
            row = cur.fetchone()
            cur.close()

            if row:
                return {
                    "endereco": row[0],
                    "lat": row[1],
                    "lon": row[2],
                    "origem": row[3],
                }
            return None

        except Exception as e:
            import logging
            logging.warning(f"⚠️ Erro ao consultar cache de endereços: {e}")
            return None

