# sales_router/src/sales_clusterization/mkp_pdv/infrastructure/database_writer.py

# sales_router/src/sales_clusterization/mkp_pdv/infrastructure/database_writer.py

from loguru import logger
from database.db_connection import get_connection_context

class MKPPDVWriter:

    @staticmethod
    def inserir_pdv_clusters(lista, tenant_id, input_id, clusterization_id, modo):

        logger.info(f"💾 Gravando {len(lista)} registros em mkp_cluster_pdv (modo={modo})")

        query = """
        INSERT INTO mkp_cluster_pdv (
            tenant_id,
            input_id,
            clusterization_id,
            modo_clusterizacao,
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
        )
        VALUES %s
        """

        values = []
        for r in lista:
            values.append((
                tenant_id,
                input_id,
                clusterization_id,
                modo,
                r["pdv_id"],
                r.get("cnpj"),
                float(r["lat"]),
                float(r["lon"]),
                float(r.get("vendas", 0.0)),
                int(r["cluster_id"]),
                float(r["cluster_lat"]),
                float(r["cluster_lon"]),
                r.get("cluster_bairro") or "",
                float(r.get("dist_km", 0.0)),
                float(r.get("tempo_min", 0.0)),
            ))

        with get_connection_context() as conn:
            with conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(cur, query, values)
            conn.commit()

        logger.success("✅ Gravação de clusters concluída.")

    @staticmethod
    def inserir_centros(centros, tenant_id, input_id, clusterization_id):

        query = """
        INSERT INTO mkp_cluster_centros (
            tenant_id, 
            input_id, 
            clusterization_id, 
            cluster_id,
            centro_bandeira, 
            centro_cliente, 
            centro_cnpj,
            centro_bairro,
            cluster_lat, 
            cluster_lon, 
            cluster_endereco
        )
        VALUES %s
        """

        values = [
            (
                tenant_id,
                input_id,
                clusterization_id,
                c["cluster_id"],
                c.get("bandeira"),
                c.get("cliente"),
                c.get("cnpj"),
                c.get("bairro"),            # 👈 NOVO: SALVANDO O BAIRRO
                float(c["lat"]) if c["lat"] else None,
                float(c["lon"]) if c["lon"] else None,
                c.get("endereco")
            )
            for c in centros
        ]

        with get_connection_context() as conn:
            from psycopg2.extras import execute_values
            with conn.cursor() as cur:
                execute_values(cur, query, values)
            conn.commit()

        logger.success(f"💾 {len(values)} centros salvos em mkp_cluster_centros.")

