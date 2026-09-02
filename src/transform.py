"""Trasformazione bronze -> silver -> gold su Postgres.

I dataflow SDMX (Eurostat) non hanno sempre gli stessi nomi di colonna: qui
si usa un piccolo resolver a lista di candidati per restare tolleranti a
variazioni tra dataflow diversi, loggando le colonne trovate al primo
utilizzo così eventuali disallineamenti sono facili da individuare.
"""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import INDICATORI, RAW_DATA_DIR, warehouse_connection_string

logger = logging.getLogger(__name__)

TERRITORIO_CANDIDATES = ["geo"]
TERRITORIO_LABEL_CANDIDATES = ["Territorio"]
TIME_CANDIDATES = ["TIME_PERIOD"]
SESSO_CANDIDATES = ["sex"]
ETA_CANDIDATES = ["age"]
VALUE_CANDIDATES = ["OBS_VALUE"]

# Mappatura regione -> macro-area, usata in dashboard per il confronto Nord/Centro/Sud.
RIPARTIZIONE_PER_REGIONE = {
    "Piemonte": "Nord", "Valle d'Aosta": "Nord", "Liguria": "Nord", "Lombardia": "Nord",
    "Trentino-Alto Adige": "Nord", "Veneto": "Nord", "Friuli-Venezia Giulia": "Nord", "Emilia-Romagna": "Nord",
    "Toscana": "Centro", "Umbria": "Centro", "Marche": "Centro", "Lazio": "Centro",
    "Abruzzo": "Sud", "Molise": "Sud", "Campania": "Sud", "Puglia": "Sud",
    "Basilicata": "Sud", "Calabria": "Sud", "Sicilia": "Sud", "Sardegna": "Sud",
}


def get_engine() -> Engine:
    return create_engine(warehouse_connection_string())


def ensure_schemas(engine: Engine) -> None:
    with engine.begin() as conn:
        for schema in ("bronze", "silver", "gold"):
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))


def _pick_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    if required:
        raise KeyError(f"Nessuna colonna tra {candidates} trovata. Colonne disponibili: {list(df.columns)}")
    return None


def _read_raw(nome: str) -> pd.DataFrame | None:
    path = RAW_DATA_DIR / f"{nome}.csv"
    if not path.exists():
        logger.warning("Nessun file grezzo per '%s' (%s): salto.", nome, path)
        return None
    return pd.read_csv(path)


def load_bronze(engine: Engine) -> None:
    for nome in INDICATORI:
        df = _read_raw(nome)
        if df is None:
            continue
        df.to_sql(nome, engine, schema="bronze", if_exists="replace", index=False)
        logger.info("bronze.%s caricata (%d righe)", nome, len(df))


def normalize_indicator_df(df: pd.DataFrame, nome: str) -> pd.DataFrame:
    """Trasforma il CSV grezzo (schema SDMX, variabile da dataflow a dataflow)
    nello schema long comune usato dal layer silver. Funzione pura, senza I/O,
    per poter essere testata con dataframe di esempio."""
    col_territorio = _pick_column(df, TERRITORIO_CANDIDATES)
    col_territorio_label = _pick_column(df, TERRITORIO_LABEL_CANDIDATES, required=False)
    col_time = _pick_column(df, TIME_CANDIDATES)
    col_value = _pick_column(df, VALUE_CANDIDATES)
    col_sesso = _pick_column(df, SESSO_CANDIDATES, required=False)
    col_eta = _pick_column(df, ETA_CANDIDATES, required=False)

    return pd.DataFrame(
        {
            "indicatore": nome,
            "cod_territorio": df[col_territorio].astype(str),
            "territorio": df[col_territorio_label].astype(str) if col_territorio_label else df[col_territorio].astype(str),
            "periodo": df[col_time].astype(str),
            "sesso": df[col_sesso].astype(str) if col_sesso else "totale",
            "classe_eta": df[col_eta].astype(str) if col_eta else "totale",
            "valore": pd.to_numeric(df[col_value], errors="coerce"),
        }
    )


def build_silver(engine: Engine) -> pd.DataFrame:
    frames = []

    for nome in INDICATORI:
        df = _read_raw(nome)
        if df is None:
            continue
        frames.append(normalize_indicator_df(df, nome))

    if not frames:
        raise RuntimeError("Nessun dato grezzo disponibile: esegui prima l'estrazione (src.eurostat_extract).")

    silver = pd.concat(frames, ignore_index=True).dropna(subset=["valore"])
    silver.to_sql("stg_mercato_lavoro", engine, schema="silver", if_exists="replace", index=False)
    logger.info("silver.stg_mercato_lavoro caricata (%d righe)", len(silver))
    return silver


def _parse_anno(periodo: str) -> int | None:
    digits = "".join(ch for ch in periodo[:4] if ch.isdigit())
    return int(digits) if len(digits) == 4 else None


def build_gold(engine: Engine, silver: pd.DataFrame) -> None:
    dim_territorio = silver[["cod_territorio", "territorio"]].drop_duplicates().reset_index(drop=True)
    dim_territorio["ripartizione"] = dim_territorio["territorio"].map(RIPARTIZIONE_PER_REGIONE).fillna("N/D")
    dim_territorio.to_sql("dim_territorio", engine, schema="gold", if_exists="replace", index=False)

    dim_tempo = silver[["periodo"]].drop_duplicates().reset_index(drop=True)
    dim_tempo["anno"] = dim_tempo["periodo"].map(_parse_anno)
    dim_tempo.to_sql("dim_tempo", engine, schema="gold", if_exists="replace", index=False)

    fact = silver[["indicatore", "cod_territorio", "periodo", "sesso", "classe_eta", "valore"]].copy()
    fact.to_sql("fact_mercato_lavoro", engine, schema="gold", if_exists="replace", index=False)

    logger.info(
        "gold: dim_territorio=%d righe, dim_tempo=%d righe, fact_mercato_lavoro=%d righe",
        len(dim_territorio), len(dim_tempo), len(fact),
    )


def run_full_transform() -> dict[str, int]:
    engine = get_engine()
    ensure_schemas(engine)
    load_bronze(engine)
    silver = build_silver(engine)
    build_gold(engine, silver)
    return {"silver_rows": len(silver)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_full_transform()
