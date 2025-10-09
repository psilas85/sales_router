#sales_router/src/authentication/infrastructure/user_respository.py

from database.db_connection import get_connection

class UserRepository:
    def create_table(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS usuario (
                id SERIAL PRIMARY KEY,
                tenant_id INT NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
                nome VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                senha_hash VARCHAR(255) NOT NULL,
                role VARCHAR(50) DEFAULT 'operacional',
                ativo BOOLEAN DEFAULT TRUE,
                criado_em TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        cur.close()
        conn.close()

    def create(self, user):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO usuario (tenant_id, nome, email, senha_hash, role, ativo)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (user.tenant_id, user.nome, user.email, user.senha_hash, user.role, user.ativo))
        user.id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return user

    def find_by_email(self, email):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, tenant_id, nome, email, senha_hash, role, ativo FROM usuario WHERE email = %s", (email,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row

