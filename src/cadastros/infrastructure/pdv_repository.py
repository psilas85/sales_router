# sales_router/src/cadastros/infrastructure/pdv_repository.py

import logging
from typing import List, Optional, Tuple
from uuid import UUID
import uuid

import psycopg2

from cadastros.entities.pdv_entity import CadastroPDV
from database.db_connection import get_connection


class CnpjDuplicadoError(Exception):
    """CNPJ já cadastrado para o tenant — viola a unicidade (tenant_id, cnpj)."""

    def __init__(self, cnpj: str):
        self.cnpj = cnpj
        super().__init__(f"CNPJ {cnpj} já cadastrado neste tenant.")


SELECT_COLUMNS = """
    id, tenant_id, ativo, cnpj, razao_social, nome_fantasia,
    logradouro, numero, bairro, cidade, uf, cep,
    pdv_lat, pdv_lon, status_geolocalizacao, pdv_vendas,
    janela_atendimento_inicio, janela_atendimento_fim,
    tempo_atendimento_min, is_estrategico, origem,
    criado_em, atualizado_em, revisao_pendente
"""


class CadastroPDVRepository:
    # ============================================================
    # Lazy migration — idempotente, segura para reexecução.
    # ============================================================
    def _ensure_table(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS cadastro_pdvs (
                    id UUID PRIMARY KEY,
                    tenant_id INTEGER NOT NULL,
                    ativo BOOLEAN NOT NULL DEFAULT TRUE,
                    cnpj TEXT NOT NULL,
                    razao_social TEXT,
                    nome_fantasia TEXT,
                    logradouro TEXT NOT NULL,
                    numero TEXT NOT NULL,
                    bairro TEXT NOT NULL,
                    cidade TEXT NOT NULL,
                    uf TEXT NOT NULL,
                    cep TEXT NOT NULL,
                    pdv_lat DOUBLE PRECISION,
                    pdv_lon DOUBLE PRECISION,
                    status_geolocalizacao TEXT,
                    pdv_vendas NUMERIC,
                    janela_atendimento_inicio INTEGER,
                    janela_atendimento_fim INTEGER,
                    tempo_atendimento_min DOUBLE PRECISION,
                    is_estrategico BOOLEAN,
                    origem TEXT NOT NULL DEFAULT 'manual',
                    criado_em TIMESTAMP NOT NULL DEFAULT NOW(),
                    atualizado_em TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                ALTER TABLE cadastro_pdvs
                    ADD COLUMN IF NOT EXISTS revisao_pendente
                    BOOLEAN NOT NULL DEFAULT FALSE;
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cadastro_pdvs_tenant
                    ON cadastro_pdvs(tenant_id);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cadastro_pdvs_tenant_ativo
                    ON cadastro_pdvs(tenant_id, ativo);
                """
            )
            # Unicidade de CNPJ por tenant — garantida no banco. Substitui o
            # antigo índice comum idx_cadastro_pdvs_tenant_cnpj. Em savepoint:
            # se houver CNPJs duplicados legados, o índice não é criado mas as
            # demais migrações seguem (o app loga e continua funcionando).
            cur.execute("SAVEPOINT sp_uq_cnpj;")
            try:
                cur.execute("DROP INDEX IF EXISTS idx_cadastro_pdvs_tenant_cnpj;")
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_cadastro_pdvs_tenant_cnpj
                        ON cadastro_pdvs(tenant_id, cnpj);
                    """
                )
                cur.execute("RELEASE SAVEPOINT sp_uq_cnpj;")
            except Exception as exc:
                cur.execute("ROLLBACK TO SAVEPOINT sp_uq_cnpj;")
                logging.warning(
                    "[CADASTRO_PDV] índice único (tenant_id, cnpj) não criado "
                    "— provável CNPJ duplicado legado: %s",
                    exc,
                )
        conn.commit()

    # ============================================================
    # Criar
    # ============================================================
    def criar(self, pdv: CadastroPDV) -> CadastroPDV:
        conn = get_connection()
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            pdv_id = pdv.id or uuid.uuid4()
            cur.execute(
                f"""
                INSERT INTO cadastro_pdvs (
                    id, tenant_id, ativo, cnpj, razao_social, nome_fantasia,
                    logradouro, numero, bairro, cidade, uf, cep,
                    pdv_lat, pdv_lon, status_geolocalizacao, pdv_vendas,
                    janela_atendimento_inicio, janela_atendimento_fim,
                    tempo_atendimento_min, is_estrategico, origem, revisao_pendente,
                    criado_em, atualizado_em
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    NOW(), NOW()
                )
                ON CONFLICT (tenant_id, cnpj) DO NOTHING
                RETURNING {SELECT_COLUMNS}
                """,
                (
                    str(pdv_id), pdv.tenant_id, pdv.ativo,
                    pdv.cnpj, pdv.razao_social, pdv.nome_fantasia,
                    pdv.logradouro, pdv.numero, pdv.bairro, pdv.cidade, pdv.uf, pdv.cep,
                    pdv.pdv_lat, pdv.pdv_lon, pdv.status_geolocalizacao, pdv.pdv_vendas,
                    pdv.janela_atendimento_inicio, pdv.janela_atendimento_fim,
                    pdv.tempo_atendimento_min, pdv.is_estrategico, pdv.origem,
                    pdv.revisao_pendente,
                ),
            )
            row = cur.fetchone()
            # ON CONFLICT DO NOTHING — sem linha = CNPJ já existe no tenant.
            if row is None:
                conn.rollback()
                raise CnpjDuplicadoError(pdv.cnpj)
            conn.commit()
            return self._row_to_entity(row)
        finally:
            conn.close()

    # ============================================================
    # Listar com paginação + filtros
    # ============================================================
    def listar(
        self,
        tenant_id: int,
        *,
        ativo: Optional[bool] = True,
        situacao: Optional[str] = None,
        uf: Optional[str] = None,
        ufs: Optional[list] = None,
        cidade: Optional[str] = None,
        cidades: Optional[list] = None,
        busca: Optional[str] = None,
        is_estrategico: Optional[bool] = None,
        com_coordenadas: Optional[bool] = None,
        criado_de=None,
        criado_ate=None,
        atualizado_de=None,
        atualizado_ate=None,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[CadastroPDV], int]:
        conn = get_connection()
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            where = ["tenant_id = %s"]
            params: list = [tenant_id]

            # situacao (3 estados) tem precedência sobre o filtro `ativo` legado.
            if situacao == "ativo":
                where.append("ativo = TRUE AND revisao_pendente = FALSE")
            elif situacao == "revisar":
                where.append("ativo = TRUE AND revisao_pendente = TRUE")
            elif situacao == "inativo":
                where.append("ativo = FALSE")
            elif ativo is not None:
                where.append("ativo = %s")
                params.append(ativo)

            if uf:
                where.append("uf = %s")
                params.append(uf.strip().upper()[:2])

            if ufs:
                where.append("uf = ANY(%s)")
                params.append([u.strip().upper()[:2] for u in ufs if u])

            if cidade:
                where.append("cidade ILIKE %s")
                params.append(f"%{cidade.strip()}%")

            if cidades:
                # Match exato — cidades já normalizadas (UPPER/sem acento).
                where.append("cidade = ANY(%s)")
                params.append([c for c in cidades if c])

            if is_estrategico is not None:
                where.append("is_estrategico IS %s" % ("TRUE" if is_estrategico else "FALSE"))

            if com_coordenadas is True:
                where.append("pdv_lat IS NOT NULL AND pdv_lon IS NOT NULL")
            elif com_coordenadas is False:
                where.append("(pdv_lat IS NULL OR pdv_lon IS NULL)")

            if busca:
                where.append(
                    "(cnpj ILIKE %s OR razao_social ILIKE %s OR nome_fantasia ILIKE %s)"
                )
                like = f"%{busca.strip()}%"
                params.extend([like, like, like])

            if criado_de:
                where.append("criado_em::date >= %s")
                params.append(criado_de)
            if criado_ate:
                where.append("criado_em::date <= %s")
                params.append(criado_ate)
            if atualizado_de:
                where.append("atualizado_em::date >= %s")
                params.append(atualizado_de)
            if atualizado_ate:
                where.append("atualizado_em::date <= %s")
                params.append(atualizado_ate)

            where_sql = " AND ".join(where)

            cur.execute(
                f"SELECT COUNT(*) FROM cadastro_pdvs WHERE {where_sql}",
                tuple(params),
            )
            total = int(cur.fetchone()[0] or 0)

            cur.execute(
                f"""
                SELECT {SELECT_COLUMNS}
                FROM cadastro_pdvs
                WHERE {where_sql}
                ORDER BY criado_em DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params) + (int(limit), int(offset)),
            )
            rows = cur.fetchall()
            return [self._row_to_entity(r) for r in rows], total
        finally:
            conn.close()

    # ============================================================
    # Buscar por ID
    # ============================================================
    def buscar_por_id(
        self, pdv_id: UUID, tenant_id: int
    ) -> Optional[CadastroPDV]:
        conn = get_connection()
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT {SELECT_COLUMNS}
                FROM cadastro_pdvs
                WHERE id = %s AND tenant_id = %s
                """,
                (str(pdv_id), tenant_id),
            )
            row = cur.fetchone()
            return self._row_to_entity(row) if row else None
        finally:
            conn.close()

    # ============================================================
    # Buscar por CNPJ (unicidade por tenant)
    # ============================================================
    def buscar_por_cnpj(
        self, tenant_id: int, cnpj: str
    ) -> Optional[CadastroPDV]:
        conn = get_connection()
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT {SELECT_COLUMNS}
                FROM cadastro_pdvs
                WHERE tenant_id = %s AND cnpj = %s
                LIMIT 1
                """,
                (tenant_id, cnpj),
            )
            row = cur.fetchone()
            return self._row_to_entity(row) if row else None
        finally:
            conn.close()

    # ============================================================
    # Atualizar
    # ============================================================
    def atualizar(self, pdv: CadastroPDV) -> Optional[CadastroPDV]:
        conn = get_connection()
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            try:
                cur.execute(
                    f"""
                    UPDATE cadastro_pdvs
                    SET
                        ativo = %s,
                        cnpj = %s,
                        razao_social = %s,
                        nome_fantasia = %s,
                        logradouro = %s,
                        numero = %s,
                        bairro = %s,
                        cidade = %s,
                        uf = %s,
                        cep = %s,
                        pdv_lat = %s,
                        pdv_lon = %s,
                        status_geolocalizacao = %s,
                        pdv_vendas = %s,
                        janela_atendimento_inicio = %s,
                        janela_atendimento_fim = %s,
                        tempo_atendimento_min = %s,
                        is_estrategico = %s,
                        revisao_pendente = %s,
                        atualizado_em = NOW()
                    WHERE id = %s AND tenant_id = %s
                    RETURNING {SELECT_COLUMNS}
                    """,
                    (
                        pdv.ativo,
                        pdv.cnpj, pdv.razao_social, pdv.nome_fantasia,
                        pdv.logradouro, pdv.numero, pdv.bairro,
                        pdv.cidade, pdv.uf, pdv.cep,
                        pdv.pdv_lat, pdv.pdv_lon, pdv.status_geolocalizacao,
                        pdv.pdv_vendas,
                        pdv.janela_atendimento_inicio, pdv.janela_atendimento_fim,
                        pdv.tempo_atendimento_min, pdv.is_estrategico,
                        pdv.revisao_pendente,
                        str(pdv.id), pdv.tenant_id,
                    ),
                )
            except psycopg2.errors.UniqueViolation:
                # CNPJ alterado para um já usado por outro PDV do tenant.
                conn.rollback()
                raise CnpjDuplicadoError(pdv.cnpj)
            row = cur.fetchone()
            conn.commit()
            return self._row_to_entity(row) if row else None
        finally:
            conn.close()

    # ============================================================
    # Soft delete (marca ativo=false, mantém histórico)
    # ============================================================
    def excluir(self, pdv_id: UUID, tenant_id: int) -> bool:
        conn = get_connection()
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE cadastro_pdvs
                SET ativo = FALSE, atualizado_em = NOW()
                WHERE id = %s AND tenant_id = %s AND ativo = TRUE
                """,
                (str(pdv_id), tenant_id),
            )
            ok = cur.rowcount > 0
            conn.commit()
            return ok
        finally:
            conn.close()

    # ============================================================
    # Row → entity
    # ============================================================
    def _row_to_entity(self, row) -> CadastroPDV:
        return CadastroPDV(
            id=row[0],
            tenant_id=row[1],
            ativo=row[2],
            cnpj=row[3],
            razao_social=row[4],
            nome_fantasia=row[5],
            logradouro=row[6],
            numero=row[7],
            bairro=row[8],
            cidade=row[9],
            uf=row[10],
            cep=row[11],
            pdv_lat=float(row[12]) if row[12] is not None else None,
            pdv_lon=float(row[13]) if row[13] is not None else None,
            status_geolocalizacao=row[14],
            pdv_vendas=float(row[15]) if row[15] is not None else None,
            janela_atendimento_inicio=row[16],
            janela_atendimento_fim=row[17],
            tempo_atendimento_min=float(row[18]) if row[18] is not None else None,
            is_estrategico=row[19],
            origem=row[20],
            criado_em=row[21],
            atualizado_em=row[22],
            revisao_pendente=bool(row[23]) if len(row) > 23 else False,
        )
