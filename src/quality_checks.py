"""Controlli di qualità sul layer gold, eseguiti prima di considerare il DAG riuscito.

Ogni check solleva AssertionError con un messaggio chiaro: nel DAG Airflow
questo fa fallire il task in modo visibile nella UI, invece di lasciar
passare dati silenziosamente corrotti a valle (dashboard, modelli).
"""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def check_table_not_empty(engine: Engine, schema: str, table: str) -> None:
    count = pd.read_sql(f"SELECT COUNT(*) AS n FROM {schema}.{table}", engine)["n"].iloc[0]
    assert count > 0, f"{schema}.{table} è vuota (0 righe)"
    logger.info("OK: %s.%s ha %d righe", schema, table, count)


def check_no_null_keys(engine: Engine) -> None:
    df = pd.read_sql(
        "SELECT * FROM gold.fact_mercato_lavoro "
        "WHERE cod_territorio IS NULL OR periodo IS NULL OR indicatore IS NULL",
        engine,
    )
    assert df.empty, f"Trovate {len(df)} righe con chiavi nulle in gold.fact_mercato_lavoro"
    logger.info("OK: nessuna chiave nulla in gold.fact_mercato_lavoro")


def check_value_ranges(engine: Engine) -> None:
    df = pd.read_sql(
        "SELECT * FROM gold.fact_mercato_lavoro WHERE valore < 0 OR valore > 100",
        engine,
    )
    assert df.empty, (
        f"Trovate {len(df)} righe con valore fuori range [0,100] "
        f"(indicatori attesi sono tutti tassi percentuali)"
    )
    logger.info("OK: tutti i valori sono nel range [0,100]")


def check_no_duplicate_facts(engine: Engine) -> None:
    df = pd.read_sql(
        "SELECT indicatore, cod_territorio, periodo, sesso, classe_eta, COUNT(*) AS n "
        "FROM gold.fact_mercato_lavoro "
        "GROUP BY indicatore, cod_territorio, periodo, sesso, classe_eta "
        "HAVING COUNT(*) > 1",
        engine,
    )
    assert df.empty, f"Trovate {len(df)} combinazioni di chiave duplicate in gold.fact_mercato_lavoro"
    logger.info("OK: nessun duplicato sulla chiave naturale del fact")


def run_quality_checks(engine: Engine) -> None:
    check_table_not_empty(engine, "gold", "fact_mercato_lavoro")
    check_table_not_empty(engine, "gold", "dim_territorio")
    check_table_not_empty(engine, "gold", "dim_tempo")
    check_no_null_keys(engine)
    check_value_ranges(engine)
    check_no_duplicate_facts(engine)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from src.transform import get_engine

    run_quality_checks(get_engine())
