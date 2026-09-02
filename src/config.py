"""Configurazione condivisa: connessione al warehouse, percorsi cache, dataflow ISTAT."""

import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = Path(os.environ.get("ISTAT_RAW_DIR", ROOT_DIR / "data" / "raw"))
RATE_LIMITER_STATE_FILE = Path(
    os.environ.get("ISTAT_RATE_LIMITER_FILE", RAW_DATA_DIR / ".rate_limiter_state.json")
)

ISTAT_MAX_REQUESTS_PER_MINUTE = 4  # sotto il limite ISTAT di 5/min: un blocco dura 1-2 giorni

WAREHOUSE_HOST = os.environ.get("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = os.environ.get("WAREHOUSE_PORT", "5432")
WAREHOUSE_DB = os.environ.get("WAREHOUSE_DB", "warehouse")
WAREHOUSE_USER = os.environ.get("POSTGRES_USER", "istat")
WAREHOUSE_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "changeme")


def warehouse_connection_string() -> str:
    return (
        f"postgresql+psycopg2://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}"
        f"@{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"
    )


# Dataflow ISTAT di interesse per il mercato del lavoro.
#
# Ogni voce è l'"identificatore" che si passa a `istatapi.discovery.DataSet(...)`:
# può essere il nome della struttura dati SDMX (es. "DCCV_TAXOCCU") oppure il
# suo id numerico di catalogo (es. "151_914") — istatapi risolve da solo il
# resto scaricando il catalogo completo, quindi non serve più conoscere in
# anticipo il codice numerico. Un identificatore sbagliato fa fallire (in modo
# esplicito) solo il task di estrazione di quell'indicatore, non l'intero DAG.
#
# - tasso_disoccupazione: identificatore noto da fonte pubblica (doc. istatapi).
# - tasso_occupazione: "DCCV_TAXOCCU", indicato direttamente dall'utente
#   (provato con l'URL .../SDMXWS/rest/data/DCCV_TAXOCCU/).
# - tasso_inattivita: candidato non confermato (trovato per ricerca in
#   documentazione di terzi) — verificalo con `python -m src.discover inattività`.
# - neet: nessun candidato trovato — vedi `python -m src.discover neet`.
DATAFLOWS = {
    "tasso_disoccupazione": "DCCV_TAXDISOCCU1",
    "tasso_occupazione": "DCCV_TAXOCCU",
    "tasso_inattivita": "DCCV_TASSAT",  # non confermato, vedi nota sopra
    "neet": None,  # ancora da trovare
}
