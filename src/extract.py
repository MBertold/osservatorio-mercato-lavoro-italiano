"""Estrazione dati dai dataflow ISTAT configurati verso data/raw/<indicatore>.csv."""

from __future__ import annotations

import io
import logging

import pandas as pd

from src.config import DATAFLOWS, RAW_DATA_DIR
from src.istat_client import IstatClient

logger = logging.getLogger(__name__)


def extract_indicatore(nome: str, use_cache: bool = True) -> "pd.DataFrame | None":
    cfg = DATAFLOWS[nome]

    if not cfg.get("verificato", False):
        logger.warning(
            "Salto '%s': flow_ref non ancora confermato (%s). "
            "Esegui `python -m src.discover` per confermarlo in config.py.",
            nome,
            cfg["flow_ref"],
        )
        return None

    client = IstatClient()
    logger.info("Estraggo indicatore '%s' (flow_ref=%s)", nome, cfg["flow_ref"])
    csv_text = client.get_data_csv(cfg["flow_ref"], use_cache=use_cache)
    df = pd.read_csv(io.StringIO(csv_text))

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DATA_DIR / f"{nome}.csv"
    df.to_csv(out_path, index=False)
    logger.info("Salvato %s (%d righe, colonne: %s)", out_path, len(df), list(df.columns))
    return df


def extract_all() -> dict[str, "pd.DataFrame | None"]:
    return {nome: extract_indicatore(nome) for nome in DATAFLOWS}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_all()
