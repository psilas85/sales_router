# routing_engine/infrastructure/agenda_repository.py

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date
from typing import Optional

from routing_engine.infrastructure.database_connection import get_db_connection


# ─────────────────────────────────────────────
# SCHEMA BOOTSTRAP
# ─────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS agenda (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       INTEGER NOT NULL,
    job_id          VARCHAR NOT NULL,
    nome            VARCHAR NOT NULL,
    data_inicio     DATE NOT NULL,
    data_fim        DATE NOT NULL,
    dias_uteis      INTEGER NOT NULL,
    criado_em       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agenda_rota (
    id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agenda_id                   UUID NOT NULL REFERENCES agenda(id) ON DELETE CASCADE,
    tenant_id                   INTEGER NOT NULL,
    consultor                   VARCHAR NOT NULL,
    rota_id                     VARCHAR NOT NULL,
    data                        DATE NOT NULL,
    distancia_km                DOUBLE PRECISION,
    tempo_min                   DOUBLE PRECISION,
    qtd_pdvs                    INTEGER,
    data_alterada_manualmente   BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (agenda_id, consultor, data)
);

CREATE TABLE IF NOT EXISTS agenda_visita (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agenda_rota_id  UUID NOT NULL REFERENCES agenda_rota(id) ON DELETE CASCADE,
    agenda_id       UUID NOT NULL,
    tenant_id       INTEGER NOT NULL,
    consultor       VARCHAR NOT NULL,
    sequencia       INTEGER NOT NULL,
    cnpj            VARCHAR,
    nome_fantasia   VARCHAR,
    cidade          VARCHAR,
    uf              VARCHAR,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION
);
"""


def ensure_schema() -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# DATACLASSES
# ─────────────────────────────────────────────

@dataclass
class AgendaRotaRow:
    consultor: str
    rota_id: str
    data: date
    distancia_km: Optional[float]
    tempo_min: Optional[float]
    qtd_pdvs: Optional[int]
    pdvs: list["AgendaVisitaRow"]


@dataclass
class AgendaVisitaRow:
    sequencia: int
    cnpj: Optional[str]
    nome_fantasia: Optional[str]
    cidade: Optional[str]
    uf: Optional[str]
    lat: Optional[float]
    lon: Optional[float]


# ─────────────────────────────────────────────
# WRITE
# ─────────────────────────────────────────────

def criar_agenda(
    tenant_id: int,
    job_id: str,
    nome: str,
    data_inicio: date,
    data_fim: date,
    dias_uteis: int,
    rotas: list[AgendaRotaRow],
) -> str:
    ensure_schema()
    conn = get_db_connection()
    try:
        agenda_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agenda (id, tenant_id, job_id, nome, data_inicio, data_fim, dias_uteis)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (agenda_id, tenant_id, job_id, nome, data_inicio, data_fim, dias_uteis),
            )

            for rota in rotas:
                rota_id_db = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO agenda_rota
                        (id, agenda_id, tenant_id, consultor, rota_id, data,
                         distancia_km, tempo_min, qtd_pdvs)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        rota_id_db, agenda_id, tenant_id,
                        rota.consultor, rota.rota_id, rota.data,
                        rota.distancia_km, rota.tempo_min, rota.qtd_pdvs,
                    ),
                )
                for pdv in rota.pdvs:
                    cur.execute(
                        """
                        INSERT INTO agenda_visita
                            (agenda_rota_id, agenda_id, tenant_id, consultor,
                             sequencia, cnpj, nome_fantasia, cidade, uf, lat, lon)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            rota_id_db, agenda_id, tenant_id, rota.consultor,
                            pdv.sequencia, pdv.cnpj, pdv.nome_fantasia,
                            pdv.cidade, pdv.uf, pdv.lat, pdv.lon,
                        ),
                    )
        conn.commit()
        return agenda_id
    finally:
        conn.close()


# ─────────────────────────────────────────────
# READ
# ─────────────────────────────────────────────

def buscar_agenda(agenda_id: str, tenant_id: int) -> Optional[dict]:
    ensure_schema()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, tenant_id, job_id, nome, data_inicio, data_fim, dias_uteis, criado_em
                FROM agenda WHERE id = %s AND tenant_id = %s
                """,
                (agenda_id, tenant_id),
            )
            row = cur.fetchone()
            if not row:
                return None

            agenda = {
                "id": str(row[0]),
                "tenant_id": row[1],
                "job_id": row[2],
                "nome": row[3],
                "data_inicio": row[4].isoformat(),
                "data_fim": row[5].isoformat(),
                "dias_uteis": row[6],
                "criado_em": row[7].isoformat(),
                "rotas": [],
            }

            cur.execute(
                """
                SELECT r.id, r.consultor, r.rota_id, r.data, r.distancia_km,
                       r.tempo_min, r.qtd_pdvs, r.data_alterada_manualmente
                FROM agenda_rota r
                WHERE r.agenda_id = %s AND r.tenant_id = %s
                ORDER BY r.consultor, r.data
                """,
                (agenda_id, tenant_id),
            )
            rotas = cur.fetchall()

            for rota in rotas:
                rota_id_db = str(rota[0])
                cur.execute(
                    """
                    SELECT sequencia, cnpj, nome_fantasia, cidade, uf, lat, lon
                    FROM agenda_visita
                    WHERE agenda_rota_id = %s
                    ORDER BY sequencia
                    """,
                    (rota_id_db,),
                )
                visitas = [
                    {
                        "sequencia": v[0], "cnpj": v[1], "nome_fantasia": v[2],
                        "cidade": v[3], "uf": v[4], "lat": v[5], "lon": v[6],
                    }
                    for v in cur.fetchall()
                ]
                agenda["rotas"].append({
                    "id": rota_id_db,
                    "consultor": rota[1],
                    "rota_id": rota[2],
                    "data": rota[3].isoformat(),
                    "distancia_km": rota[4],
                    "tempo_min": rota[5],
                    "qtd_pdvs": rota[6],
                    "data_alterada_manualmente": rota[7],
                    "visitas": visitas,
                })

        return agenda
    finally:
        conn.close()


# ─────────────────────────────────────────────
# PATCH DATE
# ─────────────────────────────────────────────

def atualizar_data_rota(
    rota_id: str,
    tenant_id: int,
    nova_data: date,
) -> Optional[dict]:
    ensure_schema()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agenda_rota
                SET data = %s, data_alterada_manualmente = TRUE
                WHERE id = %s AND tenant_id = %s
                RETURNING id, consultor, rota_id, data, data_alterada_manualmente,
                          distancia_km, tempo_min, qtd_pdvs
                """,
                (nova_data, rota_id, tenant_id),
            )
            row = cur.fetchone()
            if not row:
                return None
        conn.commit()
        return {
            "id": str(row[0]),
            "consultor": row[1],
            "rota_id": row[2],
            "data": row[3].isoformat(),
            "data_alterada_manualmente": row[4],
            "distancia_km": row[5],
            "tempo_min": row[6],
            "qtd_pdvs": row[7],
        }
    finally:
        conn.close()


def listar_agendas(tenant_id: int) -> list[dict]:
    ensure_schema()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    a.id, a.nome, a.job_id, a.data_inicio, a.data_fim,
                    a.dias_uteis, a.criado_em,
                    COUNT(DISTINCT r.id)       AS total_rotas,
                    COUNT(DISTINCT r.consultor) AS total_consultores
                FROM agenda a
                LEFT JOIN agenda_rota r ON r.agenda_id = a.id AND r.tenant_id = a.tenant_id
                WHERE a.tenant_id = %s
                GROUP BY a.id
                ORDER BY a.criado_em DESC
                """,
                (tenant_id,),
            )
            rows = cur.fetchall()
        return [
            {
                "id": str(r[0]),
                "nome": r[1],
                "job_id": r[2],
                "data_inicio": r[3].isoformat(),
                "data_fim": r[4].isoformat(),
                "dias_uteis": r[5],
                "criado_em": r[6].isoformat(),
                "total_rotas": r[7],
                "total_consultores": r[8],
            }
            for r in rows
        ]
    finally:
        conn.close()


def datas_ocupadas_consultor(agenda_id: str, consultor: str, tenant_id: int) -> list[str]:
    """Returns ISO dates already assigned to a consultant in an agenda."""
    ensure_schema()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT data FROM agenda_rota
                WHERE agenda_id = %s AND consultor = %s AND tenant_id = %s
                ORDER BY data
                """,
                (agenda_id, consultor, tenant_id),
            )
            return [row[0].isoformat() for row in cur.fetchall()]
    finally:
        conn.close()
