#sales_router/src/authentication/infrastructure/tenant_repository.py

from database.db_connection import get_connection

class TenantRepository:
    def create_table(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tenant (
                id SERIAL PRIMARY KEY,
                razao_social VARCHAR(255) NOT NULL,
                nome_fantasia VARCHAR(255),
                cnpj VARCHAR(18) UNIQUE NOT NULL,
                email_adm VARCHAR(255) NOT NULL,
                is_master BOOLEAN DEFAULT FALSE,
                criado_em TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        cur.close()
        conn.close()

    def create(self, tenant):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tenant (razao_social, nome_fantasia, cnpj, email_adm, is_master)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
        """, (tenant.razao_social, tenant.nome_fantasia, tenant.cnpj, tenant.email_adm, tenant.is_master))
        tenant.id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return tenant
