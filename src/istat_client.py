"""Wrapper attorno a `istatapi` che aggiunge le due protezioni indispensabili
che la libreria non ha di serie:

1. Rate limiting cross-processo (ISTAT impone 5 richieste/minuto per IP, con
   blocco di 1-2 giorni se superato). Una singola `istatapi.discovery.DataSet(...)`
   genera già 3 richieste HTTP interne (catalogo completo, struttura del
   dataflow, valori disponibili); qui intercettiamo *ogni* richiesta HTTP che
   istatapi fa, tramite un monkeypatch di `ISTAT._request`, e la seriamo
   dietro un limitatore persistito su file (condiviso anche tra i task
   Airflow, che possono girare in processi separati).
2. Cache su disco del catalogo completo dei dataflow: istatapi lo riscarica
   per intero ad ogni `DataSet(...)`, il che è inutile e spreca la quota di
   richieste. Qui lo scarichiamo una volta sola e lo teniamo in cache.

Il catalogo che costruiamo qui, a differenza di quello nativo di istatapi,
include anche il nome in italiano di ogni dataflow (istatapi tiene solo
l'inglese), per poter cercare con parole chiave italiane in `src/discover.py`.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import pandas as pd
from filelock import FileLock

from src.config import (
    ISTAT_MAX_REQUESTS_PER_MINUTE,
    RATE_LIMITER_STATE_FILE,
    RAW_DATA_DIR,
)

logger = logging.getLogger(__name__)

_CACHE_DIR = RAW_DATA_DIR / "cache"
_CATALOG_CACHE_FILE = _CACHE_DIR / "dataflow_catalog.csv"


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


rate_limiter = RateLimiter(ISTAT_MAX_REQUESTS_PER_MINUTE, RATE_LIMITER_STATE_FILE)

_patched = False


def _install_rate_limit_patch() -> None:
    """Fa passare ogni richiesta HTTP di istatapi dal rate limiter.

    Idempotente: chiamabile più volte (es. da più moduli che importano questo
    file) senza applicare il patch più di una volta.
    """
    global _patched
    if _patched:
        return

    from istatapi.base import ISTAT

    original_request = ISTAT._request

    def rate_limited_request(self, path, **kwargs):
        rate_limiter.wait_for_slot()
        return original_request(self, path, **kwargs)

    ISTAT._request = rate_limited_request
    _patched = True


def _parse_catalog_with_italian_names(response) -> list[dict]:
    """Come `istatapi.discovery.parse_dataflows`, ma tiene anche il nome
    italiano di ogni dataflow (istatapi tiene solo quello inglese)."""
    from istatapi.utils import make_tree, strip_ns

    tree = make_tree(response)
    strip_ns(tree)
    root = tree.root

    dataflows = []
    for dataflow in root.iter("Dataflow"):
        structure_refs = [ref.get("id") for ref in dataflow.iter("Ref")]
        names = {name.get("{http://www.w3.org/XML/1998/namespace}lang"): name.text for name in dataflow.findall("Name")}

        dataflows.append(
            {
                "df_id": dataflow.get("id"),
                "version": dataflow.get("version"),
                "df_description": names.get("en", ""),
                "df_description_it": names.get("it", ""),
                "df_structure_id": structure_refs[0] if structure_refs else None,
            }
        )

    return dataflows


def _fetch_catalog_live() -> pd.DataFrame:
    from istatapi.base import ISTAT

    _install_rate_limit_patch()
    client = ISTAT()
    response = client._request("dataflow/IT1")
    return pd.DataFrame(_parse_catalog_with_italian_names(response))


def get_dataflow_catalog(use_cache: bool = True) -> pd.DataFrame:
    """Catalogo completo dei dataflow ISTAT, con cache su disco.

    Interroga l'API una sola volta (invece che ad ogni `DataSet(...)` come
    farebbe istatapi di default) e installa un monkeypatch su
    `istatapi.discovery.all_available` così che ogni `DataSet(...)`
    costruito dopo questa chiamata riusi questo stesso catalogo in cache.
    """
    if use_cache and _CATALOG_CACHE_FILE.exists():
        catalog = pd.read_csv(_CATALOG_CACHE_FILE)
    else:
        catalog = _fetch_catalog_live()
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        catalog.to_csv(_CATALOG_CACHE_FILE, index=False)

    import istatapi.discovery as _discovery

    _discovery.all_available = lambda dataframe=True: catalog if dataframe else catalog.to_dict(orient="records")

    return catalog


_install_rate_limit_patch()
