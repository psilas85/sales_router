#sales_router/src/cadastros/infrastructure/consultor_repository.py

from datetime import datetime
from typing import List, Optional
from uuid import UUID
import uuid

from cadastros.entities.consultor_entity import Consultor
from database.db_connection import get_connection


class ConsultorRepository:
    def criar(self, consultor: Consultor) -> Consultor:
        conn = get_connection()
        cur = conn.cursor()

        consultor_id = consultor.id or uuid.uuid4()

        query = """
            INSERT INTO consultores (
                id,
                tenant_id,
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
                criado_em,
                atualizado_em
            )
            VALUES (
                %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            RETURNING id, tenant_id, setor, consultor, cpf, logradouro, numero, complemento,
                      bairro, cidade, uf, cep, celular, email, criado_em, atualizado_em
        """

        cur.execute(
            query,
            (
                str(consultor_id),
                consultor.tenant_id,
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
            ),
        )

        row = cur.fetchone()
        conn.commit()

        cur.close()
        conn.close()

        return self._row_to_entity(row)

    def listar(self, tenant_id: UUID) -> List[Consultor]:
        conn = get_connection()
        cur = conn.cursor()

        query = """
            SELECT id, tenant_id, setor, consultor, cpf, logradouro, numero, complemento,
                   bairro, cidade, uf, cep, celular, email, criado_em, atualizado_em
            FROM consultores
            WHERE tenant_id = %s
            ORDER BY consultor
        """
        cur.execute(query, (str(tenant_id),))
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return [self._row_to_entity(row) for row in rows]

    def buscar_por_id(self, consultor_id: UUID, tenant_id: UUID) -> Optional[Consultor]:
        conn = get_connection()
        cur = conn.cursor()

        query = """
            SELECT id, tenant_id, setor, consultor, cpf, logradouro, numero, complemento,
                   bairro, cidade, uf, cep, celular, email, criado_em, atualizado_em
            FROM consultores
            WHERE id = %s
              AND tenant_id = %s
        """
        cur.execute(query, (str(consultor_id), str(tenant_id)))
        row = cur.fetchone()

        cur.close()
        conn.close()

        return self._row_to_entity(row) if row else None

    def atualizar(self, consultor: Consultor) -> Optional[Consultor]:
        conn = get_connection()
        cur = conn.cursor()

        query = """
            UPDATE consultores
               SET setor = %s,
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
                   atualizado_em = NOW()
             WHERE id = %s
               AND tenant_id = %s
         RETURNING id, tenant_id, setor, consultor, cpf, logradouro, numero, complemento,
                   bairro, cidade, uf, cep, celular, email, criado_em, atualizado_em
        """

        cur.execute(
            query,
            (
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
                str(consultor.id),
                str(consultor.tenant_id),
            ),
        )

        row = cur.fetchone()
        conn.commit()

        cur.close()
        conn.close()

        return self._row_to_entity(row) if row else None

    def excluir(self, consultor_id: UUID, tenant_id: UUID) -> bool:
        conn = get_connection()
        cur = conn.cursor()

        query = """
            DELETE FROM consultores
            WHERE id = %s
              AND tenant_id = %s
        """
        cur.execute(query, (str(consultor_id), str(tenant_id)))
        deleted = cur.rowcount > 0
        conn.commit()

        cur.close()
        conn.close()

        return deleted

    def _row_to_entity(self, row) -> Consultor:
        return Consultor(
            id=row[0],
            tenant_id=row[1],
            setor=row[2],
            consultor=row[3],
            cpf=row[4],
            logradouro=row[5],
            numero=row[6],
            complemento=row[7],
            bairro=row[8],
            cidade=row[9],
            uf=row[10],
            cep=row[11],
            celular=row[12],
            email=row[13],
            criado_em=row[14],
            atualizado_em=row[15],
        )