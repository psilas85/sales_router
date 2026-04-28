import os
import time
from typing import Callable, Dict, Iterable, Tuple

import jwt
import requests
from loguru import logger


STATUS_FALHA_INTEGRACAO = "falha_integracao_geocoding"


class GeocodingEngineClient:
    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        timeout: int | None = None,
    ):
        self.base_url = (base_url or os.getenv("GEOCODING_ENGINE_URL") or "").rstrip("/")
        self.timeout = timeout or int(os.getenv("GEOCODING_ENGINE_TIMEOUT", "60"))
        self.static_token = token or os.getenv("GEOCODING_ENGINE_TOKEN")
        self.poll_interval = float(os.getenv("GEOCODING_ENGINE_POLL_INTERVAL", "2"))
        self.job_timeout = int(os.getenv("GEOCODING_ENGINE_JOB_TIMEOUT", "3600"))

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def _build_service_token(self) -> str | None:
        secret = os.getenv("JWT_SECRET_KEY")
        if not secret:
            return None

        now = int(time.time())
        payload = {
            "sub": "pdv_preprocessing",
            "service": "pdv_preprocessing",
            "iat": now,
            "exp": now + 300,
        }

        algorithm = os.getenv("JWT_ALGORITHM", "HS256")
        return jwt.encode(payload, secret, algorithm=algorithm)

    def _headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        token = self.static_token or self._build_service_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _validate_result_rows(self, results: list, expected_ids: set[int], context: str) -> None:
        seen_ids: set[int] = set()

        for result in results:
            try:
                result_id = int(result["id"])
            except Exception:
                logger.warning(f"[GEOCODING_ENGINE][RESULTADO_SEM_ID][{context}] {result}")
                continue

            if result_id not in expected_ids:
                logger.warning(
                    f"[GEOCODING_ENGINE][RESULTADO_INESPERADO][{context}] id={result_id}"
                )
                continue

            if result_id in seen_ids:
                logger.warning(
                    f"[GEOCODING_ENGINE][RESULTADO_DUPLICADO][{context}] id={result_id}"
                )
                continue

            seen_ids.add(result_id)

    def geocode_pdv_batch(
        self,
        items: Iterable[dict],
    ) -> Dict[int, Tuple[float | None, float | None, str]]:
        if not self.enabled:
            raise RuntimeError("GEOCODING_ENGINE_URL não configurada")

        payload_items = list(items)
        if not payload_items:
            return {}

        url = f"{self.base_url}/geocode/batch"

        response = requests.post(
            url,
            json={"addresses": payload_items},
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()

        data = response.json()
        results = data.get("results") or []
        mapped: Dict[int, Tuple[float | None, float | None, str]] = {}
        expected_ids = {int(item["id"]) for item in payload_items}

        self._validate_result_rows(results, expected_ids, "batch")

        for result in results:
            try:
                idx = int(result["id"])
            except Exception:
                logger.warning(f"[GEOCODING_ENGINE][RESULTADO_IGNORADO] {result}")
                continue

            if idx not in expected_ids or idx in mapped:
                continue

            mapped[idx] = (
                result.get("lat"),
                result.get("lon"),
                result.get("source") or "falha",
            )

        missing_ids = expected_ids - set(mapped)
        for missing_id in missing_ids:
            mapped[missing_id] = (None, None, STATUS_FALHA_INTEGRACAO)

        if missing_ids:
            logger.warning(
                f"[GEOCODING_ENGINE][RESULTADOS_FALTANTES][batch] total={len(missing_ids)}"
            )

        stats = data.get("stats") or {}
        logger.info(
            "[GEOCODING_ENGINE][BATCH_OK] "
            f"enviados={len(payload_items)} retornados={len(mapped)} stats={stats}"
        )

        return mapped

    def geocode_pdv_batch_job(
        self,
        items: Iterable[dict],
        on_progress: Callable[[int, str | None], None] | None = None,
    ) -> Dict[int, Tuple[float | None, float | None, str]]:
        if not self.enabled:
            raise RuntimeError("GEOCODING_ENGINE_URL não configurada")

        payload_items = list(items)
        if not payload_items:
            return {}

        id_by_position = {
            pos: int(item["id"])
            for pos, item in enumerate(payload_items)
        }

        payload_for_engine = []
        for pos, item in enumerate(payload_items):
            payload_for_engine.append({
                **item,
                "id": pos,
            })

        create_response = requests.post(
            f"{self.base_url}/geocode/batch/jobs",
            json={"addresses": payload_for_engine},
            headers=self._headers(),
            timeout=self.timeout,
        )
        create_response.raise_for_status()

        job_id = create_response.json().get("job_id")
        if not job_id:
            raise RuntimeError("geocoding_engine não retornou job_id")

        logger.info(
            f"[GEOCODING_ENGINE][JOB_CREATED] job_id={job_id} total={len(payload_items)}"
        )

        deadline = time.time() + self.job_timeout

        while time.time() < deadline:
            status_response = requests.get(
                f"{self.base_url}/job/{job_id}",
                headers=self._headers(),
                timeout=self.timeout,
            )
            status_response.raise_for_status()

            status_data = status_response.json()
            status = status_data.get("status")
            progress = int(status_data.get("progress", 0) or 0)
            step = status_data.get("step")

            if status == "finished":
                if on_progress:
                    on_progress(100, step or "Geocodificacao concluida")
                break

            if status == "failed":
                raise RuntimeError(
                    f"geocoding_engine job falhou: {status_data.get('error')}"
                )

            if on_progress:
                on_progress(progress, step)

            logger.info(
                "[GEOCODING_ENGINE][JOB_WAIT] "
                f"job_id={job_id} status={status} "
                f"progress={progress} "
                f"step={step or ''}"
            )
            time.sleep(self.poll_interval)
        else:
            raise TimeoutError(
                f"timeout aguardando geocoding_engine job_id={job_id}"
            )

        result_response = requests.get(
            f"{self.base_url}/job/{job_id}/batch-result",
            headers=self._headers(),
            timeout=self.timeout,
        )
        result_response.raise_for_status()

        data = result_response.json()
        mapped: Dict[int, Tuple[float | None, float | None, str]] = {}
        expected_positions = set(id_by_position)

        self._validate_result_rows(data.get("results") or [], expected_positions, "batch_job")

        for result in data.get("results") or []:
            try:
                pos = int(result["id"])
                original_id = id_by_position[pos]
            except Exception:
                logger.warning(f"[GEOCODING_ENGINE][RESULTADO_IGNORADO] {result}")
                continue

            if pos not in expected_positions or original_id in mapped:
                continue

            mapped[original_id] = (
                result.get("lat"),
                result.get("lon"),
                result.get("source") or "falha",
            )

        missing_positions = expected_positions - {
            pos for pos, original_id in id_by_position.items() if original_id in mapped
        }
        for pos in missing_positions:
            mapped[id_by_position[pos]] = (None, None, STATUS_FALHA_INTEGRACAO)

        if missing_positions:
            logger.warning(
                f"[GEOCODING_ENGINE][RESULTADOS_FALTANTES][batch_job] total={len(missing_positions)}"
            )

        logger.info(
            "[GEOCODING_ENGINE][JOB_OK] "
            f"job_id={job_id} enviados={len(payload_items)} retornados={len(mapped)} "
            f"stats={data.get('stats') or {}}"
        )

        return mapped
