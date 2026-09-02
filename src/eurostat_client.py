"""Client minimale per le API SDMX di Eurostat.

Eurostat non impone un limite di richieste rigido come ISTAT (niente rischio
di blocco IP per giorni), quindi qui non serve il rate limiter cross-processo
di `src/istat_client.py` — resta comunque una cache su disco, per non
riscaricare inutilmente gli stessi dati ad ogni run.
"""

from __future__ import annotations

import hashlib
import io
import logging

import pandas as pd
import requests

from src.config import EUROSTAT_BASE_URL, RAW_DATA_DIR

logger = logging.getLogger(__name__)

_CACHE_DIR = RAW_DATA_DIR / "cache_eurostat"


def fetch_data(dataset: str, key: str, start_period: int = 2015, use_cache: bool = True) -> pd.DataFrame:
    """Scarica una serie SDMX-CSV da Eurostat per l'intera dimensione `geo`
    (chiave con l'ultima posizione vuota), così una sola chiamata copre tutte
    le regioni NUTS invece di una richiesta per regione."""
    cache_key = hashlib.sha256(f"{dataset}|{key}|{start_period}".encode()).hexdigest()[:16]
    cache_file = _CACHE_DIR / f"{dataset}_{cache_key}.csv"

    if use_cache and cache_file.exists():
        logger.debug("Cache hit: %s", cache_file.name)
        return pd.read_csv(cache_file)

    url = f"{EUROSTAT_BASE_URL}/data/{dataset}/{key}"
    logger.info("Scarico da Eurostat: dataset=%s key=%s", dataset, key)
    response = requests.get(
        url,
        params={"format": "SDMX-CSV", "startPeriod": start_period},
        timeout=30,
    )
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text))

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache_file, index=False)

    return df
