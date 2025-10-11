# sales_router/src/authentication/infrastructure/user_repository.py

from database.db_connection import get_connection
from authentication.entities.user import User


class UserRepository:
    # =====================================================
    # 🔧 Estrutura da Tabela
    # =====================================================
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

    # =====================================================
    # 🧩 CRUD
    # =====================================================
    def create(self, user: User):
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

    def find_by_email(self, email: str):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, tenant_id, nome, email, senha_hash, role, ativo
            FROM usuario
            WHERE email = %s;
        """, (email,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return None

        return User(
            id=row[0],
            tenant_id=row[1],
            nome=row[2],
            email=row[3],
            senha_hash=row[4],
            role=row[5],
            ativo=row[6]
        )

    def list_all(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, tenant_id, nome, email, senha_hash, role, ativo FROM usuario;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            User(
                id=row[0],
                tenant_id=row[1],
                nome=row[2],
                email=row[3],
                senha_hash=row[4],
                role=row[5],
                ativo=row[6]
            )
            for row in rows
        ]

    def list_by_tenant(self, tenant_id: int):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, tenant_id, nome, email, senha_hash, role, ativo
            FROM usuario WHERE tenant_id = %s;
        """, (tenant_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            User(
                id=row[0],
                tenant_id=row[1],
                nome=row[2],
                email=row[3],
                senha_hash=row[4],
                role=row[5],
                ativo=row[6]
            )
            for row in rows
        ]

    def deactivate(self, user_id: int):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("UPDATE usuario SET ativo = FALSE WHERE id = %s RETURNING id, nome, email, role, ativo;", (user_id,))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if not row:
            return None

        return User(
            id=row[0],
            nome=row[1],
            email=row[2],
            role=row[3],
            ativo=row[4],
            tenant_id=None,
            senha_hash=None
        )

    def update(self, user: User):
        """Atualiza todos os campos editáveis do usuário (ex: senha)."""
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE usuario
            SET nome = %s,
                email = %s,
                senha_hash = %s,
                role = %s,
                ativo = %s
            WHERE id = %s;
        """, (user.nome, user.email, user.senha_hash, user.role, user.ativo, user.id))
        conn.commit()
        cur.close()
        conn.close()
        return user
