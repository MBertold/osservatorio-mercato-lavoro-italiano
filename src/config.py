"""Configurazione condivisa: connessione al warehouse, percorsi cache, dataflow Eurostat."""

import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = Path(os.environ.get("RAW_DATA_DIR", ROOT_DIR / "data" / "raw"))

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


# Dati regionali italiani (NUTS2) sul mercato del lavoro, presi da Eurostat.
# Ogni dataset e chiave SDMX è stato interrogato dal vivo e verificato.
# L'inattività non è pubblicata da Eurostat come tasso a sé: si ricava come
# 100 - tasso di attività (identità standard delle statistiche sul lavoro).
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

# I 4 indicatori "ufficiali" della pipeline (bronze/silver/gold). "tasso_attivita"
# non compare qui: è solo un intermedio usato per derivare tasso_inattivita,
# non un indicatore autonomo da portare fino al layer gold.
INDICATORI = ["tasso_disoccupazione", "tasso_occupazione", "tasso_inattivita", "neet"]

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
