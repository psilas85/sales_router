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
    ativo           BOOLEAN NOT NULL DEFAULT TRUE,
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
    lon             DOUBLE PRECISION,
    logradouro      VARCHAR,
    numero          VARCHAR,
    bairro          VARCHAR,
    cep             VARCHAR,
    razao_social    VARCHAR,
    status          VARCHAR(20) NOT NULL DEFAULT 'a_realizar',
    data_realizacao DATE,
    data_prevista   DATE
);
"""

# Idempotent migrations for tables that already exist without the new columns
_MIGRATIONS = """
ALTER TABLE agenda_visita ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'a_realizar';
ALTER TABLE agenda_visita ADD COLUMN IF NOT EXISTS data_realizacao DATE;
ALTER TABLE agenda_visita ADD COLUMN IF NOT EXISTS data_prevista DATE;
ALTER TABLE agenda_visita ADD COLUMN IF NOT EXISTS logradouro VARCHAR;
ALTER TABLE agenda_visita ADD COLUMN IF NOT EXISTS numero VARCHAR;
ALTER TABLE agenda_visita ADD COLUMN IF NOT EXISTS bairro VARCHAR;
ALTER TABLE agenda_visita ADD COLUMN IF NOT EXISTS cep VARCHAR;
ALTER TABLE agenda_visita ADD COLUMN IF NOT EXISTS razao_social VARCHAR;
ALTER TABLE agenda ADD COLUMN IF NOT EXISTS ativo BOOLEAN NOT NULL DEFAULT TRUE;
"""


def ensure_schema() -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
            cur.execute(_MIGRATIONS)
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
    logradouro: Optional[str] = None
    numero: Optional[str] = None
    bairro: Optional[str] = None
    cep: Optional[str] = None
    razao_social: Optional[str] = None


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
                             sequencia, cnpj, nome_fantasia, cidade, uf, lat, lon,
                             logradouro, numero, bairro, cep, razao_social)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            rota_id_db, agenda_id, tenant_id, rota.consultor,
                            pdv.sequencia, pdv.cnpj, pdv.nome_fantasia,
                            pdv.cidade, pdv.uf, pdv.lat, pdv.lon,
                            pdv.logradouro, pdv.numero, pdv.bairro, pdv.cep, pdv.razao_social,
                        ),
                    )
        conn.commit()
        return agenda_id
    finally:
        conn.close()


# ─────────────────────────────────────────────
# READ — agenda + rotas + visitas (nested)
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
# READ — flat visita list with effective status
# ─────────────────────────────────────────────

def _effective_status(stored_status: str, data_efetiva: date, today: date) -> str:
    """Computes display status from stored value and effective planned date."""
    if stored_status in ("realizada", "cancelada"):
        return stored_status
    return "vencida" if data_efetiva < today else "a_realizar"


def listar_visitas_flat(agenda_id: str, tenant_id: int) -> list[dict]:
    """Returns all PDVs as a flat list. Status is effective: 'vencida' when a_realizar + date past."""
    ensure_schema()
    today = date.today()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    v.id, v.agenda_rota_id, v.consultor, v.sequencia,
                    v.cnpj, v.nome_fantasia, v.cidade, v.uf,
                    v.status, v.data_realizacao, v.data_prevista,
                    r.rota_id, r.data AS data_rota
                FROM agenda_visita v
                JOIN agenda_rota r ON r.id = v.agenda_rota_id
                WHERE v.agenda_id = %s AND v.tenant_id = %s
                ORDER BY v.consultor, r.data, v.sequencia
                """,
                (agenda_id, tenant_id),
            )
            rows = cur.fetchall()

        result = []
        for row in rows:
            stored_status = row[8]
            data_prevista: Optional[date] = row[10]
            data_rota: date = row[12]
            data_efetiva = data_prevista if data_prevista else data_rota

            result.append({
                "id": str(row[0]),
                "agenda_rota_id": str(row[1]),
                "consultor": row[2],
                "sequencia": row[3],
                "cnpj": row[4],
                "nome_fantasia": row[5],
                "cidade": row[6],
                "uf": row[7],
                "status": _effective_status(stored_status, data_efetiva, today),
                "data_realizacao": row[9].isoformat() if row[9] else None,
                "data_prevista": data_prevista.isoformat() if data_prevista else None,
                "rota_id": row[11],
                "data_rota": data_rota.isoformat(),
                "data_efetiva": data_efetiva.isoformat(),
            })
        return result
    finally:
        conn.close()


# ─────────────────────────────────────────────
# LIST agendas
# ─────────────────────────────────────────────

def listar_agendas(tenant_id: int) -> list[dict]:
    ensure_schema()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    a.id, a.nome, a.job_id, a.data_inicio, a.data_fim,
                    a.dias_uteis, a.criado_em, a.ativo,
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
                "ativo": r[7],
                "total_rotas": r[8],
                "total_consultores": r[9],
            }
            for r in rows
        ]
    finally:
        conn.close()


# ─────────────────────────────────────────────
# READ — roteiro por período (para disparo WA)
# ─────────────────────────────────────────────

def buscar_roteiro_por_data(
    agenda_id: str,
    tenant_id: int,
    data_inicio: date,
    data_fim: date,
) -> list[dict]:
    """
    Returns visits grouped by consultor → date → ordered visits.
    Also joins consultores table to get celular.
    """
    ensure_schema()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ar.consultor,
                    ar.data          AS rota_data,
                    ar.distancia_km,
                    av.sequencia,
                    av.nome_fantasia,
                    av.razao_social,
                    av.cnpj,
                    av.logradouro,
                    av.numero,
                    av.bairro,
                    av.cidade,
                    av.uf,
                    av.lat,
                    av.lon,
                    av.status,
                    c.celular
                FROM agenda_rota ar
                JOIN agenda_visita av ON av.agenda_rota_id = ar.id
                LEFT JOIN consultores c
                    ON UPPER(TRIM(c.consultor)) = UPPER(TRIM(ar.consultor))
                    AND c.tenant_id = ar.tenant_id
                WHERE ar.agenda_id = %s
                  AND ar.tenant_id = %s
                  AND ar.data BETWEEN %s AND %s
                ORDER BY ar.consultor, ar.data, av.sequencia
                """,
                (agenda_id, tenant_id, data_inicio, data_fim),
            )
            rows = cur.fetchall()

        # group: consultor → { celular, datas: { date → { distancia_km, visitas[] } } }
        resultado: dict[str, dict] = {}
        for row in rows:
            (
                consultor, rota_data, distancia_km,
                sequencia, nome_fantasia, razao_social, cnpj,
                logradouro, numero, bairro, cidade, uf,
                lat, lon, status, celular,
            ) = row

            if consultor not in resultado:
                resultado[consultor] = {"celular": celular, "datas": {}}

            if rota_data not in resultado[consultor]["datas"]:
                resultado[consultor]["datas"][rota_data] = {
                    "distancia_km": distancia_km,
                    "visitas": [],
                }

            resultado[consultor]["datas"][rota_data]["visitas"].append({
                "sequencia": sequencia,
                "nome_fantasia": nome_fantasia,
                "razao_social": razao_social,
                "cnpj": cnpj,
                "logradouro": logradouro,
                "numero": numero,
                "bairro": bairro,
                "cidade": cidade,
                "uf": uf,
                "lat": lat,
                "lon": lon,
                "status": status,
            })

        return resultado
    finally:
        conn.close()


# ─────────────────────────────────────────────
# PATCH — agenda ativo
# ─────────────────────────────────────────────

def toggle_agenda_ativo(agenda_id: str, tenant_id: int, ativo: bool) -> Optional[dict]:
    ensure_schema()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agenda
                SET ativo = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING id, nome, ativo
                """,
                (ativo, agenda_id, tenant_id),
            )
            row = cur.fetchone()
            if not row:
                return None
        conn.commit()
        return {"id": str(row[0]), "nome": row[1], "ativo": row[2]}
    finally:
        conn.close()


# ─────────────────────────────────────────────
# PATCH — rota date
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


# ─────────────────────────────────────────────
# PATCH — visita status (single)
# ─────────────────────────────────────────────

def atualizar_status_visita(
    visita_id: str,
    tenant_id: int,
    status: Optional[str] = None,
    data_realizacao: Optional[date] = None,
    data_prevista: Optional[date] = None,
    limpar_data_prevista: bool = False,
) -> Optional[dict]:
    """Updates any combination of status, data_realizacao, and data_prevista for a single visita."""
    ensure_schema()
    today = date.today()

    set_parts: list[str] = []
    params: list = []

    if status is not None:
        set_parts.append("status = %s")
        params.append(status)
        # Clear realization date when un-doing a visit
        if status in ("a_realizar", "cancelada"):
            set_parts.append("data_realizacao = NULL")
    if data_realizacao is not None:
        set_parts.append("data_realizacao = %s")
        params.append(data_realizacao)
    if limpar_data_prevista:
        set_parts.append("data_prevista = NULL")
    elif data_prevista is not None:
        set_parts.append("data_prevista = %s")
        params.append(data_prevista)

    if not set_parts:
        return None

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE agenda_visita
                SET {", ".join(set_parts)}
                WHERE id = %s AND tenant_id = %s
                RETURNING id, agenda_rota_id, consultor, sequencia,
                          cnpj, nome_fantasia, cidade, uf,
                          status, data_realizacao, data_prevista
                """,
                [*params, visita_id, tenant_id],
            )
            row = cur.fetchone()
            if not row:
                return None
            cur.execute(
                "SELECT rota_id, data FROM agenda_rota WHERE id = %s",
                (str(row[1]),),
            )
            rota_row = cur.fetchone()
        conn.commit()

        if not rota_row:
            return None

        data_prevista_val: Optional[date] = row[10]
        data_rota_val: date = rota_row[1]
        data_efetiva = data_prevista_val if data_prevista_val else data_rota_val

        return {
            "id": str(row[0]),
            "agenda_rota_id": str(row[1]),
            "consultor": row[2],
            "sequencia": row[3],
            "cnpj": row[4],
            "nome_fantasia": row[5],
            "cidade": row[6],
            "uf": row[7],
            "status": _effective_status(row[8], data_efetiva, today),
            "data_realizacao": row[9].isoformat() if row[9] else None,
            "data_prevista": data_prevista_val.isoformat() if data_prevista_val else None,
            "rota_id": rota_row[0],
            "data_rota": data_rota_val.isoformat(),
            "data_efetiva": data_efetiva.isoformat(),
        }
    finally:
        conn.close()


# ─────────────────────────────────────────────
# PATCH — visita status / date (bulk)
# ─────────────────────────────────────────────

def atualizar_status_visitas_lote(
    agenda_id: str,
    tenant_id: int,
    visita_ids: list[str],
    status: Optional[str] = None,
    data_realizacao: Optional[date] = None,
    data_prevista: Optional[date] = None,
    limpar_data_prevista: bool = False,
) -> int:
    """Bulk-updates status/dates for multiple visitas. Returns number of rows updated."""
    if not visita_ids:
        return 0

    set_parts: list[str] = []
    params: list = []

    if status is not None:
        set_parts.append("status = %s")
        params.append(status)
        if status in ("a_realizar", "cancelada"):
            set_parts.append("data_realizacao = NULL")
    if data_realizacao is not None:
        set_parts.append("data_realizacao = %s")
        params.append(data_realizacao)
    if limpar_data_prevista:
        set_parts.append("data_prevista = NULL")
    elif data_prevista is not None:
        set_parts.append("data_prevista = %s")
        params.append(data_prevista)

    if not set_parts:
        return 0

    ensure_schema()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE agenda_visita
                SET {", ".join(set_parts)}
                WHERE id = ANY(%s::uuid[])
                  AND agenda_id = %s
                  AND tenant_id = %s
                """,
                [*params, visita_ids, agenda_id, tenant_id],
            )
            updated = cur.rowcount
        conn.commit()
        return updated
    finally:
        conn.close()


# ─────────────────────────────────────────────
# READ — backlog export (non-realized visits)
# ─────────────────────────────────────────────

def listar_backlog_para_export(agenda_id: str, tenant_id: int) -> list[dict]:
    """Returns all non-realized visits ready for routing re-upload."""
    ensure_schema()
    today = date.today()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    v.cnpj, v.razao_social, v.nome_fantasia,
                    v.logradouro, v.numero, v.bairro,
                    v.cidade, v.uf, v.cep,
                    v.consultor, v.lat, v.lon,
                    v.status, v.data_prevista,
                    r.data AS data_rota
                FROM agenda_visita v
                JOIN agenda_rota r ON r.id = v.agenda_rota_id
                WHERE v.agenda_id = %s AND v.tenant_id = %s
                  AND v.status != 'realizada'
                ORDER BY v.consultor, COALESCE(v.data_prevista, r.data), v.sequencia
                """,
                (agenda_id, tenant_id),
            )
            rows = cur.fetchall()

        result = []
        for row in rows:
            stored_status = row[12]
            data_prevista: Optional[date] = row[13]
            data_rota: date = row[14]
            data_efetiva = data_prevista if data_prevista else data_rota
            effective_status = _effective_status(stored_status, data_efetiva, today)

            result.append({
                "cnpj": row[0],
                "razao_social": row[1],
                "nome_fantasia": row[2],
                "logradouro": row[3],
                "numero": row[4],
                "bairro": row[5],
                "cidade": row[6],
                "uf": row[7],
                "cep": row[8],
                "consultor": row[9],
                "lat": row[10],
                "lon": row[11],
                "status": effective_status,
                "data_planejada_visita": data_efetiva.isoformat(),
            })
        return result
    finally:
        conn.close()


# ─────────────────────────────────────────────
# READ — occupied dates per consultant
# ─────────────────────────────────────────────

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
