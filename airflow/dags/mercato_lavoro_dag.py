"""DAG: pipeline mercato del lavoro (extract -> bronze -> silver/gold -> quality).

Estrazione con due canali intercambiabili, scelti dalla variabile d'ambiente
`DATA_SOURCE` (default "eurostat"):

- "eurostat": dati regionali italiani (NUTS2) via Eurostat (`src/eurostat_extract.py`).
  Verificato end-to-end con dati reali — è il canale di default.
- "istat": dati direttamente da ISTAT via `istatapi` (`src/extract.py`). Richiede
  una rete che riesca a raggiungere esploradati.istat.it, mai verificata
  nell'ambiente in cui questo progetto è stato sviluppato (vedi README).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

sys.path.insert(0, "/opt/airflow/project")

from airflow.decorators import dag, task

DEFAULT_ARGS = {"owner": "data-eng", "retries": 1}


def _data_source() -> str:
    return os.environ.get("DATA_SOURCE", "eurostat")


@dag(
    dag_id="mercato_lavoro_istat",
    description="Estrazione mercato del lavoro (Eurostat o ISTAT) -> Postgres bronze/silver/gold -> quality check",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["istat", "eurostat", "mercato-lavoro"],
)
def mercato_lavoro_pipeline():
    @task
    def discover_dataflows() -> None:
        """Sanity check di connettività (solo per il canale ISTAT: scarica/usa
        cache del catalogo SDMX; per Eurostat è un no-op, i dataset sono già
        noti e verificati in config.py)."""
        if _data_source() == "istat":
            from src.istat_client import get_dataflow_catalog

            get_dataflow_catalog()

    @task
    def extract_indicatore(nome: str) -> str:
        if _data_source() == "istat":
            from src.extract import extract_indicatore as _extract
        else:
            from src.eurostat_extract import extract_indicatore as _extract

        _extract(nome)
        return nome

    @task
    def load_bronze(_nomi: list[str]) -> None:
        from src.transform import ensure_schemas, get_engine
        from src.transform import load_bronze as _load_bronze

        engine = get_engine()
        ensure_schemas(engine)
        _load_bronze(engine)

    @task
    def transform_silver_gold() -> None:
        from src.transform import build_gold, build_silver, get_engine

        engine = get_engine()
        silver = build_silver(engine)
        build_gold(engine, silver)

    @task
    def quality_checks() -> None:
        from src.quality_checks import run_quality_checks
        from src.transform import get_engine

        run_quality_checks(get_engine())

    from src.config import DATAFLOWS

    discovery = discover_dataflows()
    extracted = extract_indicatore.expand(nome=list(DATAFLOWS.keys()))
    bronze = load_bronze(extracted)
    silver_gold = transform_silver_gold()
    checks = quality_checks()

    discovery >> extracted
    bronze >> silver_gold >> checks


mercato_lavoro_pipeline()
