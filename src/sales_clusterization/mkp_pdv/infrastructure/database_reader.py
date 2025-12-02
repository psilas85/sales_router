#sales_router/src/sales_clusterization/mkp_pdv/infrastructure/database_reader.py

# sales_router/src/sales_clusterization/mkp_pdv/infrastructure/database_reader.py

from loguru import logger
from database.db_connection import get_connection_context


class MKPPDVReader:

    @staticmethod
    def carregar_pdvs(tenant_id: int, uf: str, cidade: str | None, input_id: str):
        """
        Carrega PDVs já geocodificados da tabela 'pdvs'.
        """
        logger.info(
            f"📥 Carregando PDVs (tenant={tenant_id}, UF={uf}, cidade={cidade}, input_id={input_id})"
        )

        query = """
            SELECT
                id,
                cnpj,
                pdv_lat,
                pdv_lon,
                pdv_vendas,
                cidade,
                bairro
            FROM pdvs
            WHERE tenant_id = %s
              AND uf = %s
              AND input_id = %s
              AND pdv_lat IS NOT NULL
              AND pdv_lon IS NOT NULL
        """

        params = [tenant_id, uf, input_id]

        if cidade:
            query += " AND cidade = %s"
            params.append(cidade)

        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        logger.info(f"📦 {len(rows)} PDVs carregados para atribuição.")

        pdvs = []
        for r in rows:
            pdvs.append({
                "pdv_id": r[0],
                "cnpj": r[1],
                "lat": float(r[2]),
                "lon": float(r[3]),
                "vendas": float(r[4]) if r[4] else 0.0,
                "cidade": r[5],
                "bairro": r[6],
            })

        return pdvs


    @staticmethod
    def carregar_clusters_ativos(tenant_id, uf, cidade, input_id):
        """
        Carrega PDVs da tabela mkp_cluster_pdv no modo 'ativa'.
        """
        query = """
            SELECT
                pdv_id,
                cnpj,
                lat,
                lon,
                cluster_id,
                cluster_lat,
                cluster_lon,
                cluster_bairro,
                distancia_km,
                tempo_min
            FROM mkp_cluster_pdv
            WHERE tenant_id = %s
              AND input_id = %s
              AND modo_clusterizacao = 'ativa'
        """

        params = [tenant_id, input_id]

        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        pdvs = []
        for r in rows:
            pdvs.append({
                "pdv_id": r[0],
                "cnpj": r[1],
                "lat": float(r[2]),
                "lon": float(r[3]),
                "cluster_id": int(r[4]),
                "cluster_lat": float(r[5]),
                "cluster_lon": float(r[6]),
                "cluster_bairro": r[7],
                "dist_km": float(r[8]),
                "tempo_min": float(r[9]),
            })

        return pdvs



# ============================================================
# NOVA CLASSE — SUPORTE À CLUSTERIZAÇÃO BALANCEADA
# ============================================================

class MKPPDVClusterReader:

    @staticmethod
    def carregar_centros_da_clusterizacao(tenant_id, clusterization_id):
        """
        Retorna os centros associados à clusterização ATIVA ou BALANCEADA.

        - Se existir registro em mkp_cluster_centros → retorno COMPLETO (preferido)
        - Se não existir → fallback para dados da tabela mkp_cluster_pdv (legacy)
        """

        # 1) Tenta carregar centros completos (ATIVA ou BALANCEADA)
        sql_centros = """
            SELECT 
                cluster_id,
                centro_bandeira,
                centro_cliente,
                centro_cnpj,
                centro_bairro,
                cluster_lat AS lat,
                cluster_lon AS lon,
                cluster_endereco AS endereco
            FROM mkp_cluster_centros
            WHERE tenant_id = %s
            AND clusterization_id = %s
            ORDER BY cluster_id;
        """


        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_centros, (tenant_id, clusterization_id))
                rows = cur.fetchall()

        centros = []

        if rows:
            # ✔️ Retorno COMPLETO — COM BAIRRO
            for r in rows:
                centros.append({
                    "cluster_id": int(r[0]),
                    "bandeira": r[1],
                    "cliente": r[2],
                    "cnpj": r[3],
                    "bairro": r[4],       # 👈 NOVO
                    "lat": float(r[5]),
                    "lon": float(r[6]),
                    "endereco": r[7],
                })

            return centros

        # 2) Fallback legacy (ATIVA antiga)
        sql_legacy = """
            SELECT DISTINCT 
                cluster_id,
                cluster_lat AS lat,
                cluster_lon AS lon,
                cluster_bairro AS bairro
            FROM mkp_cluster_pdv
            WHERE tenant_id = %s
            AND clusterization_id = %s;
        """

        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_legacy, (tenant_id, clusterization_id))
                rows = cur.fetchall()

        for r in rows:
            centros.append({
                "cluster_id": int(r[0]),
                "bandeira": None,
                "cliente": None,
                "cnpj": None,
                "lat": float(r[1]),
                "lon": float(r[2]),
                "endereco": None,
                "bairro": r[3] or "",   # ← mantém bairro
            })

        return centros



    @staticmethod
    def carregar_pdvs_da_clusterizacao(tenant_id, clusterization_id):
        """
        Carrega todos os PDVs clusterizados na etapa ativa.
        Agora inclui pdv_vendas.
        """
        sql = """
            SELECT
                pdv_id,
                cnpj,
                lat,
                lon,
                pdv_vendas,
                cluster_id,
                cluster_lat,
                cluster_lon,
                cluster_bairro,
                distancia_km,
                tempo_min
            FROM mkp_cluster_pdv
            WHERE tenant_id = %s
            AND clusterization_id = %s;
        """

        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, clusterization_id))
                rows = cur.fetchall()

        lista = []
        for r in rows:
            lista.append({
                "pdv_id": r[0],
                "cnpj": r[1],
                "lat": float(r[2]),
                "lon": float(r[3]),
                "vendas": float(r[4]) if r[4] is not None else 0.0,
                "cluster_id": int(r[5]),
                "cluster_lat": float(r[6]),
                "cluster_lon": float(r[7]),
                "cluster_bairro": r[8],
                "dist_km": float(r[9]),
                "tempo_min": float(r[10]),
            })

        return lista

    @staticmethod
    def carregar_centros_salvos(tenant_id, clusterization_id):
        sql = """
            SELECT *
            FROM mkp_cluster_centros
            WHERE tenant_id = %s
            AND clusterization_id = %s
            ORDER BY cluster_id;
        """

        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id, clusterization_id))
            rows = cur.fetchall()

        conn.close()
        return rows
