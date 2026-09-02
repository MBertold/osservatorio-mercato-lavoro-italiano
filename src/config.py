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


# Canale alternativo: stessi 4 indicatori, dati regionali italiani (NUTS2)
# presi da Eurostat invece che direttamente da ISTAT. Ogni dataset e chiave
# SDMX sotto è stato interrogato dal vivo e verificato (a differenza dei
# candidati ISTAT sopra). L'inattività non è pubblicata da Eurostat come
# tasso a sé: si ricava come 100 - tasso di attività (identità standard
# delle statistiche sul lavoro).
EUROSTAT_BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"

EUROSTAT_DATAFLOWS = {
    "tasso_disoccupazione": {
        "dataset": "lfst_r_lfu3rt",
        "key": "A.TOTAL.T.Y15-74.PC.",  # freq.isced11.sex.age.unit.geo
        "descrizione": "Unemployment rate, 15-74 anni",
    },
    "tasso_occupazione": {
        "dataset": "lfst_r_lfe2emprt",
        "key": "A.PC.T.Y20-64.",  # freq.unit.sex.age.geo
        "descrizione": "Employment rate, 20-64 anni",
    },
    "tasso_attivita": {
        "dataset": "lfst_r_lfp2actrt",
        "key": "A.PC.T.Y15-64.",  # freq.unit.sex.age.geo
        "descrizione": "Activity rate, 15-64 anni (usato per derivare tasso_inattivita = 100 - questo)",
    },
    "neet": {
        "dataset": "edat_lfse_22",
        "key": "A.T.Y15-29.NO_FE_NO_NFE.NEMP.PC.",  # freq.sex.age.training.wstatus.unit.geo
        "descrizione": "NEET rate, 15-29 anni",
    },
}

# NUTS2 -> nome regione italiana (nomenclatura NUTS 2021). Bolzano e Trento
# sono due unità NUTS2 separate ma un'unica regione amministrativa italiana
# (Trentino-Alto Adige): vengono accorpate in fase di estrazione.
NUTS2_TO_REGIONE = {
    "ITC1": "Piemonte",
    "ITC2": "Valle d'Aosta",
    "ITC3": "Liguria",
    "ITC4": "Lombardia",
    "ITH1": "Trentino-Alto Adige",  # Bolzano
    "ITH2": "Trentino-Alto Adige",  # Trento
    "ITH3": "Veneto",
    "ITH4": "Friuli-Venezia Giulia",
    "ITH5": "Emilia-Romagna",
    "ITI1": "Toscana",
    "ITI2": "Umbria",
    "ITI3": "Marche",
    "ITI4": "Lazio",
    "ITF1": "Abruzzo",
    "ITF2": "Molise",
    "ITF3": "Campania",
    "ITF4": "Puglia",
    "ITF5": "Basilicata",
    "ITF6": "Calabria",
    "ITG1": "Sicilia",
    "ITG2": "Sardegna",
}
