#sales_router/src/pdv_preprocessing/infrastructure/cep_resolver.py

# ============================================================
# 📮 Resolução de CEP → cidade / UF
# - cache-first (tabela viacep_cache)
# - fallback HTTP: ViaCEP → BrasilAPI
# - grava no cache os CEPs resolvidos via HTTP
# Uso: fallback de divergência cidade×UF no pré-processamento de PDVs.
# ============================================================

import re
import time

import requests
from loguru import logger


VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
HTTP_TIMEOUT = 6


def _normalizar_cep(cep: str) -> str | None:
    digitos = re.sub(r"[^0-9]", "", str(cep or ""))
    return digitos.zfill(8) if len(digitos) in (5, 8) else None


class CepResolver:
    """
    resolver(cep) / resolver_em_lote(ceps) -> {cep, logradouro, bairro, cidade, uf}
    Retorna None / ausente quando o CEP não pode ser resolvido.
    """

    def __init__(self, reader=None, writer=None):
        self.reader = reader
        self.writer = writer

    # --------------------------------------------------------
    # HTTP — ViaCEP
    # --------------------------------------------------------
    def _via_viacep(self, cep: str) -> dict | None:
        try:
            resp = requests.get(VIACEP_URL.format(cep=cep), timeout=HTTP_TIMEOUT)
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not isinstance(data, dict) or data.get("erro"):
                return None
            cidade = (data.get("localidade") or "").strip()
            uf = (data.get("uf") or "").strip().upper()
            if not cidade or len(uf) != 2:
                return None
            return {
                "cep": cep,
                "logradouro": (data.get("logradouro") or "").strip(),
                "bairro": (data.get("bairro") or "").strip(),
                "cidade": cidade,
                "uf": uf,
            }
        except Exception as e:
            logger.warning(f"[CEP_RESOLVER][VIACEP][ERRO] cep={cep} erro={e}")
            return None

    # --------------------------------------------------------
    # HTTP — BrasilAPI (fallback)
    # --------------------------------------------------------
    def _via_brasilapi(self, cep: str) -> dict | None:
        try:
            resp = requests.get(BRASILAPI_URL.format(cep=cep), timeout=HTTP_TIMEOUT)
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not isinstance(data, dict):
                return None
            cidade = (data.get("city") or "").strip()
            uf = (data.get("state") or "").strip().upper()
            if not cidade or len(uf) != 2:
                return None
            return {
                "cep": cep,
                "logradouro": (data.get("street") or "").strip(),
                "bairro": (data.get("neighborhood") or "").strip(),
                "cidade": cidade,
                "uf": uf,
            }
        except Exception as e:
            logger.warning(f"[CEP_RESOLVER][BRASILAPI][ERRO] cep={cep} erro={e}")
            return None

    # --------------------------------------------------------
    # Resolve 1 CEP (cache → HTTP)
    # --------------------------------------------------------
    def resolver(self, cep: str) -> dict | None:
        cep_norm = _normalizar_cep(cep)
        if not cep_norm:
            return None

        # 1) cache
        if self.reader is not None:
            try:
                cached = self.reader.buscar_viacep_cache(cep_norm)
                if cached and cached.get("cidade") and cached.get("uf"):
                    return {
                        "cep": cep_norm,
                        "logradouro": cached.get("logradouro") or "",
                        "bairro": cached.get("bairro") or "",
                        "cidade": cached["cidade"],
                        "uf": cached["uf"],
                    }
            except Exception as e:
                logger.warning(f"[CEP_RESOLVER][CACHE][ERRO] cep={cep_norm} erro={e}")

        # 2) HTTP: ViaCEP → BrasilAPI
        info = self._via_viacep(cep_norm) or self._via_brasilapi(cep_norm)
        if not info:
            return None

        # 3) grava no cache
        if self.writer is not None:
            try:
                self.writer.salvar_viacep_cache(
                    cep=cep_norm,
                    logradouro=info.get("logradouro"),
                    bairro=info.get("bairro"),
                    cidade=info.get("cidade"),
                    uf=info.get("uf"),
                )
            except Exception as e:
                logger.warning(f"[CEP_RESOLVER][CACHE_SAVE][ERRO] cep={cep_norm} erro={e}")

        return info

    # --------------------------------------------------------
    # Resolve vários CEPs (cache em lote → HTTP para o resto)
    # --------------------------------------------------------
    def resolver_em_lote(self, ceps) -> dict[str, dict]:
        normalizados = {
            c for c in (_normalizar_cep(cep) for cep in ceps) if c
        }
        if not normalizados:
            return {}

        resultados: dict[str, dict] = {}

        # 1) cache em lote
        if self.reader is not None:
            try:
                cache = self.reader.buscar_viacep_cache_em_lote(list(normalizados))
                for cep_norm, row in (cache or {}).items():
                    if row and row.get("cidade") and row.get("uf"):
                        resultados[cep_norm] = {
                            "cep": cep_norm,
                            "logradouro": row.get("logradouro") or "",
                            "bairro": row.get("bairro") or "",
                            "cidade": row["cidade"],
                            "uf": row["uf"],
                        }
            except Exception as e:
                logger.warning(f"[CEP_RESOLVER][CACHE_LOTE][ERRO] erro={e}")

        # 2) HTTP para os que faltam
        faltantes = normalizados - set(resultados)
        for cep_norm in faltantes:
            info = self._via_viacep(cep_norm) or self._via_brasilapi(cep_norm)
            if info:
                resultados[cep_norm] = info
                if self.writer is not None:
                    try:
                        self.writer.salvar_viacep_cache(
                            cep=cep_norm,
                            logradouro=info.get("logradouro"),
                            bairro=info.get("bairro"),
                            cidade=info.get("cidade"),
                            uf=info.get("uf"),
                        )
                    except Exception as e:
                        logger.warning(
                            f"[CEP_RESOLVER][CACHE_SAVE][ERRO] cep={cep_norm} erro={e}"
                        )
            # cortesia com as APIs públicas em lotes grandes
            time.sleep(0.05)

        return resultados
