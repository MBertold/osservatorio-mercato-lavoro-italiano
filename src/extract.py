"""Estrazione dati dai dataflow ISTAT configurati verso data/raw/<indicatore>.csv,
tramite `istatapi` (rate limiting e cache applicati da src/istat_client.py)."""

from __future__ import annotations

import logging

from src.config import DATAFLOWS, RAW_DATA_DIR
from src.istat_client import get_dataflow_catalog

logger = logging.getLogger(__name__)


def extract_indicatore(nome: str, use_cache: bool = True):
    identifier = DATAFLOWS.get(nome)

    if not identifier:
        logger.warning(
            "Salto '%s': nessun identificatore configurato in src/config.py. "
            "Cercalo con `python -m src.discover <parola chiave>`.",
            nome,
        )
        return None

    get_dataflow_catalog(use_cache=use_cache)  # scarica/usa cache prima di costruire il DataSet

    from istatapi.discovery import DataSet
    from istatapi.retrieval import get_data

    logger.info("Estraggo indicatore '%s' (identificatore=%s)", nome, identifier)
    dataset = DataSet(identifier)
    df = get_data(dataset)

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DATA_DIR / f"{nome}.csv"
    df.to_csv(out_path, index=False)
    logger.info("Salvato %s (%d righe, colonne: %s)", out_path, len(df), list(df.columns))
    return df


def extract_all() -> dict:
    return {nome: extract_indicatore(nome) for nome in DATAFLOWS}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_all()
