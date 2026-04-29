import os
import time

import jwt
import requests


class RoutingEngineClient:
    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        timeout: int | None = None,
    ):
        self.base_url = (
            base_url
            or os.getenv("ROUTING_ENGINE_URL")
            or "http://routing_engine:8008/api/v1"
        ).rstrip("/")
        self.timeout = timeout or int(os.getenv("ROUTING_ENGINE_TIMEOUT", "60"))
        self.static_token = token or os.getenv("ROUTING_ENGINE_TOKEN")
        self.poll_interval = float(os.getenv("ROUTING_ENGINE_POLL_INTERVAL", "1"))
        self.job_timeout = int(os.getenv("ROUTING_ENGINE_JOB_TIMEOUT", "3600"))

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def _build_service_token(self) -> str | None:
        secret = os.getenv("JWT_SECRET_KEY")
        if not secret:
            return None

        now = int(time.time())
        payload = {
            "sub": "sales_routing",
            "service": "sales_routing",
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

    def start_prepared_groups_job(self, payload: dict) -> dict:
        response = requests.post(
            f"{self.base_url}/internal/prepared-groups/balanced-routing",
            json=payload,
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def get_job_status(self, job_id: str) -> dict:
        response = requests.get(
            f"{self.base_url}/job/{job_id}",
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def execute_prepared_groups_job(self, payload: dict, on_progress=None) -> dict:
        create_data = self.start_prepared_groups_job(payload)
        job_id = create_data.get("job_id")

        if not job_id:
            raise RuntimeError("routing_engine não retornou job_id")

        deadline = time.time() + self.job_timeout

        while time.time() < deadline:
            status_data = self.get_job_status(job_id)
            status = status_data.get("status")
            progress = int(status_data.get("progress", 0) or 0)
            step = status_data.get("step")

            if on_progress:
                on_progress(progress, step)

            if status == "finished":
                result = status_data.get("result") or {}
                result["engine_job_id"] = job_id
                return result

            if status == "failed":
                raise RuntimeError(
                    f"routing_engine job falhou: {status_data.get('error') or 'erro desconhecido'}"
                )

            time.sleep(self.poll_interval)

        raise TimeoutError(f"timeout aguardando routing_engine job_id={job_id}")