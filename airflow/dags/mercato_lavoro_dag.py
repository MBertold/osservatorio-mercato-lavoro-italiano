"""DAG: pipeline mercato del lavoro italiano (extract -> bronze -> silver/gold -> quality).

Estrazione da Eurostat (dati regionali NUTS2, `src/eurostat_extract.py`),
verificata end-to-end con dati reali.
"""

from __future__ import annotations

import sys
from datetime import datetime

sys.path.insert(0, "/opt/airflow/project")

from airflow.decorators import dag, task

DEFAULT_ARGS = {"owner": "data-eng", "retries": 1}


@dag(
    dag_id="mercato_lavoro_istat",
    description="Estrazione Eurostat (mercato del lavoro) -> Postgres bronze/silver/gold -> quality check",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["eurostat", "mercato-lavoro"],
)
def mercato_lavoro_pipeline():
    @task
    def extract_indicatore(nome: str) -> str:
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

    from src.config import INDICATORI

    extracted = extract_indicatore.expand(nome=INDICATORI)
    bronze = load_bronze(extracted)
    silver_gold = transform_silver_gold()
    checks = quality_checks()

    bronze >> silver_gold >> checks


mercato_lavoro_pipeline()
