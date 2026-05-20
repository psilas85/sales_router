# sales_router/src/cadastros/application/pdv_use_case.py

import os
import re
import unicodedata
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Tuple
from uuid import UUID

import jwt
import requests
from loguru import logger

from cadastros.entities.pdv_entity import CadastroPDV
from cadastros.infrastructure.pdv_repository import CadastroPDVRepository


# ============================================================
# Normalizadores
# ============================================================

def _norm_cidade(valor: Optional[str]) -> Optional[str]:
    if not valor:
        return valor
    sem_acento = unicodedata.normalize("NFD", valor)
    sem_acento = "".join(c for c in sem_acento if unicodedata.category(c) != "Mn")
    return " ".join(sem_acento.upper().split())


def _norm_uf(valor: Optional[str]) -> Optional[str]:
    if not valor:
        return valor
    return valor.strip().upper()[:2]


def _norm_cnpj(valor: Optional[str]) -> str:
    if not valor:
        return ""
    digitos = re.sub(r"\D", "", str(valor))
    # Excel zera o leading zero quando a coluna é numérica — repõe o zero
    # de CNPJ (14 dígitos). CPF (11) é mantido como está.
    if 11 < len(digitos) < 14:
        digitos = digitos.zfill(14)
    return digitos


def _norm_cep(valor: Optional[str]) -> str:
    if not valor:
        return ""
    return re.sub(r"\D", "", str(valor))


# ============================================================
# Geocoding via geocoding_engine (síncrono, single-PDV)
# ============================================================

GEOCODING_URL = os.getenv(
    "GEOCODING_ENGINE_URL", "http://geocoding_engine:8007/api/v1"
).rstrip("/")
GEOCODING_TIMEOUT = int(os.getenv("GEOCODING_ENGINE_TIMEOUT", "30"))

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")


def _service_token() -> Optional[str]:
    """JWT de serviço para autenticar no geocoding_engine — mesmo segredo
    dos tenants. O engine apenas decodifica o token; qualquer JWT válido
    assinado com JWT_SECRET_KEY é aceito."""
    if not JWT_SECRET_KEY:
        return None
    payload = {
        "user_id": "cadastros-service",
        "tenant_id": 0,
        "role": "service",
        "email": "service@salesrouter",
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def _auth_headers() -> dict:
    token = _service_token()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _endereco_log(pdv: CadastroPDV) -> str:
    return f"{pdv.logradouro} {pdv.numero}, {pdv.bairro}, {pdv.cidade}-{pdv.uf}"


def _geocode_single(pdv: CadastroPDV) -> Optional[dict]:
    """Chama POST /geocode do engine. Retorna {lat, lon, status} ou None
    se a chamada falhar (sem quebrar a criação do PDV).
    """
    payload = {
        "id": 1,
        "logradouro": pdv.logradouro,
        "numero": pdv.numero,
        "bairro": pdv.bairro,
        "cidade": pdv.cidade,
        "uf": pdv.uf,
        "cep": pdv.cep,
    }
    logger.info(f"[CADASTRO_PDV][GEOCODE] chamando engine | {_endereco_log(pdv)}")
    try:
        resp = requests.post(
            f"{GEOCODING_URL}/geocode",
            json=payload,
            headers=_auth_headers(),
            timeout=GEOCODING_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        logger.info(
            f"[CADASTRO_PDV][GEOCODE] engine respondeu | "
            f"lat={data.get('lat')} lon={data.get('lon')} status={data.get('status')}"
        )
        return data
    except Exception as e:
        logger.warning(f"[CADASTRO_PDV][GEOCODE] falhou: {e}")
        return None


def _upsert_cache_endereco(pdv: CadastroPDV) -> None:
    """Propaga a coordenada confirmada/corrigida manualmente do PDV para o
    cache de endereços do geocoding_engine. A chave canônica é montada no
    engine (mesmo build_cache_key das consultas) — aqui só enviamos o
    endereço cru. Fire-and-forget: nunca quebra o salvamento do PDV."""
    if pdv.pdv_lat is None or pdv.pdv_lon is None:
        return
    if not (pdv.logradouro and pdv.numero and pdv.cidade and pdv.uf):
        return
    try:
        resp = requests.post(
            f"{GEOCODING_URL}/cache/upsert",
            json={
                "logradouro": pdv.logradouro,
                "numero": pdv.numero,
                "cidade": pdv.cidade,
                "uf": pdv.uf,
                "lat": pdv.pdv_lat,
                "lon": pdv.pdv_lon,
                "origem": "correcao_manual",
            },
            headers=_auth_headers(),
            timeout=GEOCODING_TIMEOUT,
        )
        resp.raise_for_status()
        logger.info(
            f"[CADASTRO_PDV][CACHE] endereço propagado ao cache | "
            f"{_endereco_log(pdv)} -> ({pdv.pdv_lat}, {pdv.pdv_lon})"
        )
    except Exception as e:
        logger.warning(f"[CADASTRO_PDV][CACHE] upsert falhou: {e}")


# ============================================================
# Use case
# ============================================================

class CadastroPDVUseCase:
    def __init__(self):
        self.repository = CadastroPDVRepository()

    def _normalizar(self, pdv: CadastroPDV) -> CadastroPDV:
        pdv.cnpj = _norm_cnpj(pdv.cnpj)
        pdv.cep = _norm_cep(pdv.cep)
        pdv.cidade = _norm_cidade(pdv.cidade) or pdv.cidade
        pdv.uf = _norm_uf(pdv.uf) or pdv.uf
        pdv.logradouro = (pdv.logradouro or "").strip()
        pdv.numero = (pdv.numero or "").strip()
        pdv.bairro = (pdv.bairro or "").strip()
        if pdv.razao_social:
            pdv.razao_social = pdv.razao_social.strip()
        if pdv.nome_fantasia:
            pdv.nome_fantasia = pdv.nome_fantasia.strip()
        return pdv

    # --------------------------------------------------------
    # Criar
    # --------------------------------------------------------
    def criar(
        self,
        pdv: CadastroPDV,
        *,
        geocode: bool = True,
    ) -> CadastroPDV:
        pdv = self._normalizar(pdv)
        # lat/lon já preenchidos no payload = coordenada confirmada por humano.
        coords_manuais = pdv.pdv_lat is not None and pdv.pdv_lon is not None
        logger.info(
            f"[CADASTRO_PDV][CRIAR] cnpj={pdv.cnpj} {pdv.cidade}-{pdv.uf} "
            f"tenant={pdv.tenant_id}"
        )

        # Geocoding automático se lat/lon não vierem preenchidos.
        if geocode and (pdv.pdv_lat is None or pdv.pdv_lon is None):
            r = _geocode_single(pdv)
            if r and r.get("lat") and r.get("lon"):
                pdv.pdv_lat = float(r["lat"])
                pdv.pdv_lon = float(r["lon"])
                pdv.status_geolocalizacao = r.get("status") or "geocoded"
                logger.info(
                    f"[CADASTRO_PDV][CRIAR] geocodificado "
                    f"lat={pdv.pdv_lat} lon={pdv.pdv_lon} "
                    f"status={pdv.status_geolocalizacao}"
                )
            else:
                pdv.status_geolocalizacao = "nao_encontrado"
                logger.warning(
                    "[CADASTRO_PDV][CRIAR] geocoding sem resultado — "
                    "status=nao_encontrado"
                )
        elif not geocode:
            logger.info("[CADASTRO_PDV][CRIAR] geocoding desligado (geocode=false)")
        else:
            logger.info(
                f"[CADASTRO_PDV][CRIAR] geocoding dispensado — lat/lon informados "
                f"(lat={pdv.pdv_lat} lon={pdv.pdv_lon})"
            )

        criado = self.repository.criar(pdv)
        logger.info(f"[CADASTRO_PDV][CRIAR] persistido id={criado.id}")
        # Coordenada informada manualmente alimenta o cache de endereços.
        if coords_manuais:
            _upsert_cache_endereco(criado)
        return criado

    # --------------------------------------------------------
    # Listar (paginado + filtros)
    # --------------------------------------------------------
    def listar(
        self,
        tenant_id: int,
        *,
        ativo: Optional[bool] = True,
        situacao: Optional[str] = None,
        uf: Optional[str] = None,
        cidade: Optional[str] = None,
        busca: Optional[str] = None,
        is_estrategico: Optional[bool] = None,
        com_coordenadas: Optional[bool] = None,
        criado_de: Optional[date] = None,
        criado_ate: Optional[date] = None,
        atualizado_de: Optional[date] = None,
        atualizado_ate: Optional[date] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[CadastroPDV], int]:
        return self.repository.listar(
            tenant_id,
            ativo=ativo,
            situacao=situacao,
            uf=uf,
            # Normaliza igual ao cadastro (UPPER + sem acento) para casar
            # com o valor armazenado, já que o filtro usa ILIKE.
            cidade=_norm_cidade(cidade) if cidade else None,
            busca=busca,
            is_estrategico=is_estrategico,
            com_coordenadas=com_coordenadas,
            criado_de=criado_de,
            criado_ate=criado_ate,
            atualizado_de=atualizado_de,
            atualizado_ate=atualizado_ate,
            limit=limit,
            offset=offset,
        )

    # --------------------------------------------------------
    # Buscar por ID
    # --------------------------------------------------------
    def buscar_por_id(self, pdv_id: UUID, tenant_id: int) -> Optional[CadastroPDV]:
        return self.repository.buscar_por_id(pdv_id, tenant_id)

    # --------------------------------------------------------
    # Atualizar
    # --------------------------------------------------------
    def atualizar(
        self,
        pdv: CadastroPDV,
        *,
        regeocode_se_endereco_mudou: bool = True,
        endereco_anterior: Optional[CadastroPDV] = None,
    ) -> Optional[CadastroPDV]:
        pdv = self._normalizar(pdv)
        # lat/lon no payload = coordenada confirmada/corrigida por humano.
        coords_manuais = pdv.pdv_lat is not None and pdv.pdv_lon is not None
        logger.info(
            f"[CADASTRO_PDV][ATUALIZAR] id={pdv.id} cnpj={pdv.cnpj} ativo={pdv.ativo}"
        )

        # Re-geocodifica se: endereço mudou E user não informou lat/lon
        # manualmente nesta atualização. Compara com o estado anterior.
        if (
            regeocode_se_endereco_mudou
            and endereco_anterior is not None
            and (pdv.pdv_lat is None or pdv.pdv_lon is None)
        ):
            mudou = (
                pdv.logradouro != endereco_anterior.logradouro
                or pdv.numero != endereco_anterior.numero
                or pdv.bairro != endereco_anterior.bairro
                or pdv.cidade != endereco_anterior.cidade
                or pdv.uf != endereco_anterior.uf
                or pdv.cep != endereco_anterior.cep
            )
            if mudou:
                logger.info(
                    "[CADASTRO_PDV][ATUALIZAR] endereço alterado — re-geocodificando"
                )
                r = _geocode_single(pdv)
                if r and r.get("lat") and r.get("lon"):
                    pdv.pdv_lat = float(r["lat"])
                    pdv.pdv_lon = float(r["lon"])
                    pdv.status_geolocalizacao = r.get("status") or "geocoded"
                    logger.info(
                        f"[CADASTRO_PDV][ATUALIZAR] geocodificado "
                        f"lat={pdv.pdv_lat} lon={pdv.pdv_lon} "
                        f"status={pdv.status_geolocalizacao}"
                    )
                else:
                    pdv.status_geolocalizacao = "nao_encontrado"
                    logger.warning(
                        "[CADASTRO_PDV][ATUALIZAR] geocoding sem resultado — "
                        "status=nao_encontrado"
                    )
            else:
                # Endereço igual → mantém coord anterior.
                pdv.pdv_lat = endereco_anterior.pdv_lat
                pdv.pdv_lon = endereco_anterior.pdv_lon
                pdv.status_geolocalizacao = endereco_anterior.status_geolocalizacao
                logger.info(
                    "[CADASTRO_PDV][ATUALIZAR] endereço inalterado — "
                    "coordenadas mantidas"
                )
        else:
            logger.info(
                "[CADASTRO_PDV][ATUALIZAR] geocoding dispensado — lat/lon "
                "informados manualmente ou re-geocoding desligado"
            )

        # Resolve a pendência de revisão assim que o PDV passa a ter
        # coordenadas (cidade corrigida + geocodificada com sucesso).
        if pdv.pdv_lat is not None and pdv.pdv_lon is not None and pdv.revisao_pendente:
            pdv.revisao_pendente = False
            logger.info(
                f"[CADASTRO_PDV][ATUALIZAR] id={pdv.id} revisão resolvida "
                "(PDV agora geocodificado)"
            )

        atualizado = self.repository.atualizar(pdv)
        if atualizado:
            logger.info(f"[CADASTRO_PDV][ATUALIZAR] persistido id={atualizado.id}")
            # Coordenada corrigida manualmente alimenta o cache de endereços.
            if coords_manuais:
                _upsert_cache_endereco(atualizado)
        else:
            logger.warning(
                f"[CADASTRO_PDV][ATUALIZAR] id={pdv.id} não encontrado para atualização"
            )
        return atualizado

    # --------------------------------------------------------
    # Excluir (soft delete)
    # --------------------------------------------------------
    def excluir(self, pdv_id: UUID, tenant_id: int) -> bool:
        ok = self.repository.excluir(pdv_id, tenant_id)
        if ok:
            logger.info(
                f"[CADASTRO_PDV][DESATIVAR] id={pdv_id} desativado (soft delete)"
            )
        else:
            logger.warning(
                f"[CADASTRO_PDV][DESATIVAR] id={pdv_id} não encontrado ou já desativado"
            )
        return ok
