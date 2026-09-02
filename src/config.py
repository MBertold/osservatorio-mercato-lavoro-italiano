"""Configurazione condivisa: connessione al warehouse, percorsi cache, dataflow ISTAT."""

import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = Path(os.environ.get("ISTAT_RAW_DIR", ROOT_DIR / "data" / "raw"))
RATE_LIMITER_STATE_FILE = Path(
    os.environ.get("ISTAT_RATE_LIMITER_FILE", RAW_DATA_DIR / ".rate_limiter_state.json")
)

ISTAT_SDMX_BASE_URL = "https://esploradati.istat.it/SDMXWS/rest"
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
# IMPORTANTE su "verificato": True significa "confermato interrogando dal vivo
# il catalogo SDMX ISTAT" (con `python -m src.discover ...`, eseguito da una rete
# che riesce a raggiungere esploradati.istat.it — questo ambiente di sviluppo
# non ci riesce, vedi README). extract.py salta ogni indicatore con
# verificato=False, quindi finché non lo confermi manualmente non verrà scaricato.
#
# - tasso_disoccupazione: verificato dal vivo (fonte: documentazione pubblica
#   del pacchetto istatapi, poi confermabile via discover.py).
# - tasso_occupazione, tasso_inattivita: flow_ref *candidati*, trovati per
#   ricerca in documentazione di terzi (datiistat.it/alldataflows), MAI
#   verificati via API in questo ambiente. Probabile che siano corretti (stessa
#   numerazione della famiglia di dataflow del mercato del lavoro), ma vanno
#   confermati con discover.py prima di fidarsene: se corrispondono, cambia
#   semplicemente `verificato` a True.
# - neet: nessun candidato trovato nemmeno in documentazione di terzi — va
#   cercato da zero con discover.py.
DATAFLOWS = {
    "tasso_disoccupazione": {
        "flow_ref": "151_914",
        "structure_id": "DCCV_TAXDISOCCU1",
        "descrizione": "Tasso di disoccupazione",
        "verificato": True,
    },
    "tasso_occupazione": {
        "flow_ref": "150_915",  # candidato non verificato via API, vedi nota sopra
        "structure_id": "DCCV_TAXOCCU1",
        "descrizione": "Tasso di occupazione",
        "verificato": False,
    },
    "tasso_inattivita": {
        "flow_ref": "152_913",  # candidato non verificato via API, vedi nota sopra
        "structure_id": "DCCV_TASSAT",
        "descrizione": "Tasso di inattività",
        "verificato": False,
    },
    "neet": {
        "flow_ref": "TODO_DA_CONFERMARE",
        "structure_id": "DCCV_NEET",
        "descrizione": "Giovani NEET (15-29 anni)",
        "verificato": False,
    },
}
