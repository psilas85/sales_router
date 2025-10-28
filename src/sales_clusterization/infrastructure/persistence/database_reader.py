#sales_router/src/sales_clusterization/infrastructure/persistence/database_reader.py

# ============================================================
# 📦 src/sales_clusterization/infrastructure/persistence/database_reader.py
# ============================================================

from typing import List, Optional
import numpy as np
from src.database.db_connection import get_connection
from src.sales_clusterization.domain.entities import PDV
from loguru import logger


def carregar_pdvs(
    tenant_id: int,
    input_id: str,
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
) -> List[PDV]:
    """
    Lê os PDVs válidos da base vinculada ao tenant e ao input_id informado.
    - Filtra PDVs pela base específica (input_id)
    - Mantém duplicatas legítimas (não descarta coordenadas iguais)
    - Apenas registra duplicatas no log
    """

    base_query = """
        SELECT id, cnpj, bairro, cidade, uf, pdv_lat, pdv_lon
        FROM pdvs
        WHERE tenant_id = %s
          AND input_id = %s
    """
    params = [tenant_id, input_id]

    if uf:
        base_query += " AND UPPER(uf) = UPPER(%s)"
        params.append(uf)
    if cidade:
        base_query += " AND UPPER(cidade) = UPPER(%s)"
        params.append(cidade)

    base_query += ";"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(base_query, tuple(params))
            rows = cur.fetchall()

    if not rows:
        logger.warning(
            f"⚠️ Nenhum PDV encontrado para tenant={tenant_id}, input_id={input_id}, "
            f"UF={uf or 'todas'}, cidade={cidade or 'todas'}"
        )
        return []

    pdvs_limp = []
    invalidos = 0
    coords_vistos = set()
    duplicadas = 0

    for row in rows:
        try:
            _id, cnpj, bairro, cidade_, uf_, lat, lon = row

            # Ignora coordenadas inválidas
            if lat in (None, 0) or lon in (None, 0):
                invalidos += 1
                continue
            if np.isnan(lat) or np.isnan(lon) or np.isinf(lat) or np.isinf(lon):
                invalidos += 1
                continue

            # Detecta duplicatas sem excluir
            chave = (round(lat, 6), round(lon, 6))
            if chave in coords_vistos:
                duplicadas += 1
                logger.debug(
                    f"⚠️ Coordenadas duplicadas detectadas "
                    f"(tenant={tenant_id}, cidade={cidade_}, uf={uf_}): lat={lat:.6f}, lon={lon:.6f}"
                )
            coords_vistos.add(chave)

            pdvs_limp.append(
                PDV(
                    id=_id,
                    cnpj=cnpj,
                    nome=None,
                    bairro=bairro,
                    cidade=cidade_,
                    uf=uf_,
                    lat=float(lat),
                    lon=float(lon),
                )
            )
        except Exception as e:
            invalidos += 1
            logger.warning(f"⚠️ Erro ao processar linha de PDV (id={row[0] if row else 'N/A'}): {e}")
            continue

    logger.info(
        f"📦 {len(pdvs_limp)} PDVs carregados | tenant={tenant_id} | input_id={input_id} | "
        f"UF={uf or 'todas'} | cidade={cidade or 'todas'} | 🧹 {invalidos} inválidos | ⚠️ {duplicadas} duplicadas"
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


