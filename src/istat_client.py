"""Client per le API SDMX di ISTAT.

ISTAT impone un limite di 5 richieste/minuto per IP; superarlo comporta un
blocco di 1-2 giorni. Questo client applica tre protezioni:

1. Rate limiting cross-processo tramite un file di stato con lock (necessario
   perché i task Airflow possono girare in processi/worker separati).
2. Cache su disco delle risposte grezze, per non ri-scaricare mai per errore
   durante lo sviluppo o al rilancio di un DAG.
3. Retry con backoff esponenziale su errori 429/5xx.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path

import requests
from filelock import FileLock

from src.config import (
    ISTAT_MAX_REQUESTS_PER_MINUTE,
    ISTAT_SDMX_BASE_URL,
    RATE_LIMITER_STATE_FILE,
    RAW_DATA_DIR,
)

logger = logging.getLogger(__name__)

_CACHE_DIR = RAW_DATA_DIR / "cache"
_MAX_RETRIES = 5
_BASE_BACKOFF_SECONDS = 5


class IstatApiError(RuntimeError):
    """Errore non recuperabile dall'API ISTAT (dopo i retry disponibili)."""


class RateLimiter:
    """Limitatore di frequenza persistito su file, condiviso tra processi."""

    def __init__(self, max_per_minute: int, state_file: Path):
        self.max_per_minute = max_per_minute
        self.state_file = state_file
        self.lock_file = state_file.with_suffix(".lock")
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

    def wait_for_slot(self) -> None:
        while True:
            with FileLock(str(self.lock_file)):
                now = time.time()
                timestamps = self._read_timestamps()
                timestamps = [t for t in timestamps if now - t < 60]

                if len(timestamps) < self.max_per_minute:
                    timestamps.append(now)
                    self._write_timestamps(timestamps)
                    return

                sleep_seconds = 60 - (now - timestamps[0]) + 0.5

            logger.info("Rate limit ISTAT raggiunto, attendo %.1fs", sleep_seconds)
            time.sleep(max(sleep_seconds, 0.5))

    def _read_timestamps(self) -> list[float]:
        if not self.state_file.exists():
            return []
        try:
            return json.loads(self.state_file.read_text())
        except (json.JSONDecodeError, OSError):
            return []

    def _write_timestamps(self, timestamps: list[float]) -> None:
        self.state_file.write_text(json.dumps(timestamps))


class IstatClient:
    def __init__(
        self,
        base_url: str = ISTAT_SDMX_BASE_URL,
        cache_dir: Path = _CACHE_DIR,
        max_requests_per_minute: int = ISTAT_MAX_REQUESTS_PER_MINUTE,
    ):
        self.base_url = base_url.rstrip("/")
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limiter = RateLimiter(max_requests_per_minute, RATE_LIMITER_STATE_FILE)
        self.session = requests.Session()

    def get_dataflows(self, agency_id: str = "IT1", use_cache: bool = True) -> str:
        """Scarica il catalogo completo dei dataflow (XML SDMX)."""
        url = f"{self.base_url}/dataflow/{agency_id}"
        return self._get(url, accept="application/xml", use_cache=use_cache, cache_key=f"dataflow_{agency_id}")

    def get_data_csv(
        self,
        flow_ref: str,
        key: str = "",
        params: dict | None = None,
        use_cache: bool = True,
    ) -> str:
        """Scarica i dati di un dataflow in formato SDMX-CSV.

        `key` è la chiave posizionale SDMX (es. "M.IT.15_64.1.1..." a seconda
        delle dimensioni del dataflow) — vuota per prendere tutta la serie.
        """
        url = f"{self.base_url}/data/{flow_ref}/{key}"
        cache_key = self._cache_key(flow_ref, key, params)
        return self._get(
            url,
            accept="application/vnd.sdmx.data+csv;version=1.0.0",
            params=params,
            use_cache=use_cache,
            cache_key=cache_key,
        )

    def _cache_key(self, flow_ref: str, key: str, params: dict | None) -> str:
        raw = f"{flow_ref}|{key}|{json.dumps(params or {}, sort_keys=True)}"
        digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
        safe_flow = flow_ref.replace("/", "_")
        return f"{safe_flow}_{digest}"

    def _get(
        self,
        url: str,
        accept: str,
        params: dict | None = None,
        use_cache: bool = True,
        cache_key: str | None = None,
    ) -> str:
        cache_file = self.cache_dir / f"{cache_key}.raw" if cache_key else None

        if use_cache and cache_file and cache_file.exists():
            logger.debug("Cache hit: %s", cache_file.name)
            return cache_file.read_text(encoding="utf-8")

        response_text = self._request_with_retry(url, accept, params)

        if cache_file:
            cache_file.write_text(response_text, encoding="utf-8")

        return response_text

    def _request_with_retry(self, url: str, accept: str, params: dict | None) -> str:
        last_error: Exception | None = None

        for attempt in range(1, _MAX_RETRIES + 1):
            self.rate_limiter.wait_for_slot()

            try:
                resp = self.session.get(
                    url, params=params, headers={"Accept": accept}, timeout=30
                )
            except requests.RequestException as exc:
                last_error = exc
                logger.warning("Errore di rete (tentativo %d/%d): %s", attempt, _MAX_RETRIES, exc)
            else:
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code == 429:
                    logger.warning(
                        "HTTP 429 da ISTAT (tentativo %d/%d): rallento ulteriormente", attempt, _MAX_RETRIES
                    )
                elif resp.status_code >= 500:
                    logger.warning(
                        "Errore server ISTAT %d (tentativo %d/%d)", resp.status_code, attempt, _MAX_RETRIES
                    )
                else:
                    raise IstatApiError(
                        f"Richiesta fallita ({resp.status_code}) per {url}: {resp.text[:300]}"
                    )
                last_error = IstatApiError(f"HTTP {resp.status_code} per {url}")

            backoff = _BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            time.sleep(backoff)

        raise IstatApiError(f"Impossibile completare la richiesta a {url} dopo {_MAX_RETRIES} tentativi") from last_error
