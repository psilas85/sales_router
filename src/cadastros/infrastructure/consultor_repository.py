# sales_router/src/cadastros/infrastructure/consultor_repository.py

from typing import List, Optional
from uuid import UUID
import uuid

from cadastros.entities.consultor_entity import Consultor
from database.db_connection import get_connection


class ConsultorRepository:

    def _ensure_ativo_column(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE consultores
                ADD COLUMN IF NOT EXISTS ativo BOOLEAN
                """
            )
            cur.execute(
                """
                UPDATE consultores
                   SET ativo = TRUE
                 WHERE ativo IS NULL
                """
            )
            cur.execute(
                """
                ALTER TABLE consultores
                ALTER COLUMN ativo SET DEFAULT FALSE
                """
            )
            cur.execute(
                """
                ALTER TABLE consultores
                ALTER COLUMN ativo SET NOT NULL
                """
            )
        conn.commit()

    def criar(self, consultor: Consultor) -> Consultor:
        conn = get_connection()
        self._ensure_ativo_column(conn)
        cur = conn.cursor()

        consultor_id = consultor.id or uuid.uuid4()

        query = """
            INSERT INTO consultores (
                id,
                tenant_id,
                ativo,
                setor,
                consultor,
                cpf,
                logradouro,
                numero,
                complemento,
                bairro,
                cidade,
                uf,
                cep,
                celular,
                email,
                lat,
                lon,
                criado_em,
                atualizado_em
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            RETURNING id, tenant_id, ativo, setor, consultor, cpf, logradouro, numero, complemento,
                      bairro, cidade, uf, cep, celular, email, lat, lon, criado_em, atualizado_em
        """

        cur.execute(
            query,
            (
                str(consultor_id),
                consultor.tenant_id,
                consultor.ativo,
                consultor.setor,
                consultor.consultor,
                consultor.cpf,
                consultor.logradouro,
                consultor.numero,
                consultor.complemento,
                consultor.bairro,
                consultor.cidade,
                consultor.uf,
                consultor.cep,
                consultor.celular,
                consultor.email,
                consultor.lat,
                consultor.lon,
            ),
        )

        row = cur.fetchone()
        conn.commit()

        cur.close()
        conn.close()

        return self._row_to_entity(row)

    def listar(self, tenant_id: int) -> List[Consultor]:
        conn = get_connection()
        self._ensure_ativo_column(conn)
        cur = conn.cursor()

        query = """
                 SELECT id, tenant_id, ativo, setor, consultor, cpf, logradouro, numero, complemento,
                     bairro, cidade, uf, cep, celular, email, lat, lon, criado_em, atualizado_em
            FROM consultores
            WHERE tenant_id = %s
            ORDER BY consultor
        """

        cur.execute(query, (tenant_id,))
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return [self._row_to_entity(row) for row in rows]

    def buscar_por_id(self, consultor_id: UUID, tenant_id: int) -> Optional[Consultor]:
        conn = get_connection()
        self._ensure_ativo_column(conn)
        cur = conn.cursor()

        query = """
                 SELECT id, tenant_id, ativo, setor, consultor, cpf, logradouro, numero, complemento,
                     bairro, cidade, uf, cep, celular, email, lat, lon, criado_em, atualizado_em
            FROM consultores
            WHERE id = %s
              AND tenant_id = %s
        """

        cur.execute(query, (str(consultor_id), tenant_id))
        row = cur.fetchone()

        cur.close()
        conn.close()

        return self._row_to_entity(row) if row else None

    def atualizar(self, consultor: Consultor) -> Optional[Consultor]:
        conn = get_connection()
        self._ensure_ativo_column(conn)
        cur = conn.cursor()

        query = """
            UPDATE consultores
               SET ativo = %s,
                   setor = %s,
                   consultor = %s,
                   cpf = %s,
                   logradouro = %s,
                   numero = %s,
                   complemento = %s,
                   bairro = %s,
                   cidade = %s,
                   uf = %s,
                   cep = %s,
                   celular = %s,
                   email = %s,
                   lat = %s,
                   lon = %s,
                   atualizado_em = NOW()
             WHERE id = %s
               AND tenant_id = %s
         RETURNING id, tenant_id, ativo, setor, consultor, cpf, logradouro, numero, complemento,
                   bairro, cidade, uf, cep, celular, email, lat, lon, criado_em, atualizado_em
        """

        cur.execute(
            query,
            (
                consultor.ativo,
                consultor.setor,
                consultor.consultor,
                consultor.cpf,
                consultor.logradouro,
                consultor.numero,
                consultor.complemento,
                consultor.bairro,
                consultor.cidade,
                consultor.uf,
                consultor.cep,
                consultor.celular,
                consultor.email,
                consultor.lat,
                consultor.lon,
                str(consultor.id),
                consultor.tenant_id,
            ),
        )

        row = cur.fetchone()
        conn.commit()

        cur.close()
        conn.close()

        return self._row_to_entity(row) if row else None

    def excluir(self, consultor_id: UUID, tenant_id: int) -> bool:
        conn = get_connection()
        self._ensure_ativo_column(conn)
        cur = conn.cursor()

        query = """
            DELETE FROM consultores
            WHERE id = %s
              AND tenant_id = %s
        """

        cur.execute(query, (str(consultor_id), tenant_id))
        deleted = cur.rowcount > 0

        conn.commit()

        cur.close()
        conn.close()

        return deleted

    def _row_to_entity(self, row) -> Consultor:
        return Consultor(
            id=row[0],
            tenant_id=row[1],
            ativo=row[2],
            setor=row[3],
            consultor=row[4],
            cpf=row[5],
            logradouro=row[6],
            numero=row[7],
            complemento=row[8],
            bairro=row[9],
            cidade=row[10],
            uf=row[11],
            cep=row[12],
            celular=row[13],
            email=row[14],
            lat=row[15],
            lon=row[16],
            criado_em=row[17],
            atualizado_em=row[18],
        )