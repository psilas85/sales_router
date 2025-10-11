# sales_router/src/authentication/infrastructure/tenant_repository.py

from database.db_connection import get_connection
from authentication.entities.tenant import Tenant


class TenantRepository:
    # =====================================================
    # 🧩 Estrutura da Tabela
    # =====================================================
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

    # =====================================================
    # 🧩 CRUD
    # =====================================================
    def create(self, tenant: Tenant):
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

    def list_all(self):
        """Retorna todos os tenants como entidades Tenant."""
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, razao_social, nome_fantasia, cnpj, email_adm, is_master
            FROM tenant
            ORDER BY id;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return [
            Tenant(
                id=row[0],
                razao_social=row[1],
                nome_fantasia=row[2],
                cnpj=row[3],
                email_adm=row[4],
                is_master=row[5]
            )
            for row in rows
        ]

    def get_by_cnpj(self, cnpj: str):
        """Busca um tenant específico pelo CNPJ."""
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, razao_social, nome_fantasia, cnpj, email_adm, is_master
            FROM tenant
            WHERE cnpj = %s;
        """, (cnpj,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return None

        return Tenant(
            id=row[0],
            razao_social=row[1],
            nome_fantasia=row[2],
            cnpj=row[3],
            email_adm=row[4],
            is_master=row[5]
        )
