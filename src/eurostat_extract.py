"""Estrazione degli indicatori sul mercato del lavoro da Eurostat.
Scrive in `data/raw/<indicatore>.csv`, con uno schema di colonne
(geo/Territorio/sex/age/TIME_PERIOD/OBS_VALUE) letto direttamente da
`src/transform.py`.

`tasso_inattivita` non è pubblicato da Eurostat come tasso a sé: viene
derivato da `tasso_attivita` con l'identità 100 - tasso_attivita.
"""

from __future__ import annotations

import logging

import pandas as pd

from src.config import EUROSTAT_DATAFLOWS, NUTS2_TO_REGIONE, RAW_DATA_DIR
from src.eurostat_client import fetch_data

logger = logging.getLogger(__name__)

_DIRECT_INDICATORI = ["tasso_disoccupazione", "tasso_occupazione", "tasso_attivita", "neet"]


def _aggregate_by_territorio(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra alle sole regioni italiane NUTS2 e accorpa Bolzano+Trento in
    'Trentino-Alto Adige' (media semplice, approssimazione ragionevole per
    un progetto di portfolio — andrebbe pesata per popolazione per un uso
    in produzione)."""
    df = df[df["geo"].isin(NUTS2_TO_REGIONE.keys())].copy()
    df["Territorio"] = df["geo"].map(NUTS2_TO_REGIONE)

    dim_cols = [c for c in df.columns if c not in ("geo", "Territorio", "OBS_VALUE", "OBS_FLAG", "CONF_STATUS", "LAST UPDATE", "DATAFLOW")]
    grouped = df.groupby(["Territorio"] + dim_cols, as_index=False)["OBS_VALUE"].mean()

    # transform.py si aspetta sia un codice territorio (TERRITORIO_CANDIDATES)
    # sia un'etichetta leggibile (TERRITORIO_LABEL_CANDIDATES): qui non
    # abbiamo un codice NUTS2 unico dopo l'accorpamento Bolzano+Trento, quindi
    # usiamo il nome regione anche come "codice" (comunque univoco e stabile).
    grouped["geo"] = grouped["Territorio"]
    return grouped


def extract_indicatore(nome: str, use_cache: bool = True) -> pd.DataFrame | None:
    if nome == "tasso_inattivita":
        attivita = extract_indicatore("tasso_attivita", use_cache=use_cache)
        if attivita is None:
            return None
        inattivita = attivita.copy()
        inattivita["OBS_VALUE"] = 100 - inattivita["OBS_VALUE"]
        _save(nome, inattivita)
        return inattivita

    cfg = EUROSTAT_DATAFLOWS.get(nome)
    if cfg is None:
        logger.warning("Nessun dataset Eurostat configurato per '%s'", nome)
        return None

    logger.info("Estraggo (Eurostat) '%s': dataset=%s", nome, cfg["dataset"])
    raw = fetch_data(cfg["dataset"], cfg["key"], use_cache=use_cache)
    df = _aggregate_by_territorio(raw)
    _save(nome, df)
    return df


def _save(nome: str, df: pd.DataFrame) -> None:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DATA_DIR / f"{nome}.csv"
    df.to_csv(out_path, index=False)
    logger.info("Salvato %s (%d righe, colonne: %s)", out_path, len(df), list(df.columns))


def extract_all() -> dict[str, pd.DataFrame | None]:
    results = {nome: extract_indicatore(nome) for nome in _DIRECT_INDICATORI}
    results["tasso_inattivita"] = extract_indicatore("tasso_inattivita")
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_all()
