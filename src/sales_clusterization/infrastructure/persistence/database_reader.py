from typing import List, Optional
import numpy as np
from src.database.db_connection import get_connection
from src.sales_clusterization.domain.entities import PDV
from loguru import logger


def carregar_pdvs(tenant_id: int, uf: Optional[str] = None, cidade: Optional[str] = None) -> List[PDV]:
    """
    Lê os PDVs válidos do tenant informado.
    - Se cidade for None, carrega todos os PDVs da UF (misturando cidades próximas).
    - Mantém duplicatas legítimas (sem descartar PDVs com mesma coordenada).
    - Apenas registra no log se encontrar coordenadas idênticas.
    """
    base_query = """
        SELECT id, cnpj, bairro, cidade, uf, pdv_lat, pdv_lon
        FROM pdvs
        WHERE tenant_id = %s
    """
    params = [tenant_id]

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
        logger.warning(f"⚠️ Nenhum PDV encontrado para tenant={tenant_id}, UF={uf}, cidade={cidade}")
        return []

    pdvs_limp = []
    invalidos = 0
    coords_vistos = set()
    duplicadas = 0

    for row in rows:
        try:
            _id, cnpj, bairro, cidade_, uf_, lat, lon = row

            # Ignora valores totalmente inválidos
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
                    f"⚠️ Coordenadas duplicadas detectadas (tenant={tenant_id}, cidade={cidade_}, uf={uf_}): "
                    f"lat={lat:.6f}, lon={lon:.6f}"
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
        f"📦 {len(pdvs_limp)} PDVs carregados para tenant={tenant_id}, "
        f"UF={uf or 'todas'}, cidade={cidade or 'todas'} | "
        f"🧹 {invalidos} inválidos | ⚠️ {duplicadas} coordenadas duplicadas detectadas"
    )

    return pdvs_limp


def get_cidades_por_uf(tenant_id: int, uf: str) -> list[str]:
    """
    Retorna lista única de cidades com PDVs válidos (com lat/lon) na UF informada.
    Usado apenas para logs ou estatísticas.
    """
    query = """
        SELECT DISTINCT cidade
        FROM pdvs
        WHERE tenant_id = %s
          AND UPPER(uf) = UPPER(%s)
          AND pdv_lat IS NOT NULL
          AND pdv_lon IS NOT NULL
        ORDER BY cidade;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, uf))
            rows = cur.fetchall()

    cidades = [r[0] for r in rows] if rows else []
    logger.info(f"🌎 {len(cidades)} cidades encontradas para tenant={tenant_id}, UF={uf}")
    return cidades
