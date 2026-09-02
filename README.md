# Osservatorio Mercato del Lavoro Italiano

Progetto end-to-end su dati aperti sul mercato del lavoro italiano che copre in un'unica
pipeline coerente tre profili: **data engineering** (estrazione, orchestrazione, storage),
**data analysis** (dashboard) e **data science** (forecasting, clustering).

```
Airflow DAG (extract → bronze → silver/gold → quality check)
        │  Postgres (bronze / silver / gold)
        ├──> Streamlit dashboard (KPI, mappa, serie storiche, confronto Nord/Centro/Sud)
        └──> Notebook Jupyter (forecasting ARIMA, clustering K-means)
```

Tutto containerizzato con Docker Compose.

## Fonte dati: Eurostat

I dati regionali italiani (20 regioni, nomenclatura NUTS2) vengono da **Eurostat**, non
direttamente da ISTAT: durante lo sviluppo, `esploradati.istat.it`/`sdmx.istat.it` non si sono
mai rivelati raggiungibili in modo affidabile (connessione rifiutata, timeout, o un loop di
redirect lato server, a seconda del percorso tentato), mentre l'API Eurostat è risultata
pienamente funzionante. Eurostat pubblica gli stessi indicatori con la stessa metodologia
armonizzata Labour Force Survey, quindi è un canale legittimo, non solo un ripiego — ed è stato
verificato end-to-end con dati reali (vedi "Limiti noti" per i dettagli).

Dataset usati (`src/config.py`):

| Indicatore | Dataset Eurostat | Note |
|---|---|---|
| Tasso di disoccupazione | `lfst_r_lfu3rt` | 15-74 anni |
| Tasso di occupazione | `lfst_r_lfe2emprt` | 20-64 anni |
| Tasso di inattività | `lfst_r_lfp2actrt` | derivato: 100 - tasso di attività (15-64 anni) |
| NEET | `edat_lfse_22` | 15-29 anni, non occupati e non in formazione |

## Perché questo stack

- **Postgres** invece di un file DuckDB: con più container che leggono/scrivono in parallelo
  (Airflow + dashboard) serve un database con vera concorrenza.
- **Airflow con LocalExecutor**: orchestrazione realistica ma senza la complessità
  operativa di Celery/Redis, adatta a girare su una singola macchina. Il task di estrazione usa
  il *dynamic task mapping* di Airflow (un'istanza per indicatore, in parallelo).
- **Cache su disco** (`src/eurostat_client.py`): le risposte SDMX-CSV vengono salvate su disco,
  così non si ri-scarica mai per errore durante lo sviluppo o al rilancio di un DAG.
- **Bolzano e Trento accorpate**: sono due unità NUTS2 separate ma un'unica regione
  amministrativa italiana (Trentino-Alto Adige) — vengono unite con una media semplice in
  `src/eurostat_extract.py`.

## Struttura del repo

```
docker-compose.yml          postgres, airflow-init/webserver/scheduler, dashboard
airflow/
  Dockerfile                 estende apache/airflow con le dipendenze in più
  dags/mercato_lavoro_dag.py DAG TaskFlow: extract -> bronze -> silver/gold -> quality
src/
  config.py                   connessione DB + configurazione dei dataset Eurostat
  eurostat_client.py           client SDMX Eurostat con cache su disco
  eurostat_extract.py          estrazione regionale (NUTS2) verso data/raw/<indicatore>.csv
  transform.py                 bronze -> silver -> gold (schema a stella) su Postgres
  quality_checks.py            controlli di qualità sul layer gold
dashboard/app.py              dashboard Streamlit (legge gold via Postgres)
analysis/                     notebook di forecasting e clustering
tests/                        unit test (pytest) sulla logica di trasformazione ed estrazione
```

## Avvio rapido

```bash
cp .env.example .env
# genera una Fernet key per Airflow e incollala in .env:
python -c "import secrets, base64; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```

Modifica `.env` con le password che preferisci, poi:

```bash
docker compose build
docker compose up -d postgres
docker compose up airflow-init      # una tantum: crea schema Airflow + utente admin
docker compose up -d airflow-webserver airflow-scheduler dashboard
```

Servizi esposti (porte non standard per evitare conflitti con altri stack Postgres/Airflow/Streamlit
eventualmente già in esecuzione sulla stessa macchina — modificale in `docker-compose.yml` se preferisci
le porte di default):

| Servizio        | URL                     | Credenziali              |
|-----------------|-------------------------|---------------------------|
| Airflow UI      | http://localhost:8081   | da `.env` (default admin/admin) |
| Dashboard       | http://localhost:8502   | —                          |
| Postgres        | localhost:5433           | da `.env`                 |

## Eseguire la pipeline

Nella UI di Airflow (http://localhost:8081), attiva e lancia manualmente il DAG
`mercato_lavoro_istat`. Task: `extract_indicatore (un'istanza per indicatore, in parallelo) →
load_bronze → transform_silver_gold → quality_checks`.

Se il task `quality_checks` fallisce, il problema è nei dati (es. valori fuori range) ed è
visibile nei log del task — comportamento voluto: preferiamo un DAG rosso a dati silenziosamente
corrotti in dashboard.

## Dashboard e notebook

Dopo un run riuscito del DAG:

- Dashboard: http://localhost:8502 — KPI, mappa coropletica per regione, serie storiche,
  confronto Nord/Centro/Sud, focus NEET.
- Notebook (`analysis/01_forecasting.ipynb`, `analysis/02_clustering.ipynb`): da eseguire
  con `requirements-dev.txt` installato, puntando a Postgres su `localhost:5433`.

## Test

```bash
pip install -r requirements-dev.txt
pytest tests/
```

I test coprono la logica pura di trasformazione (`src/transform.py`) e l'aggregazione regionale
(`src/eurostat_extract.py`), senza dipendere da rete o database.

## Limiti noti / prossimi passi

- L'accorpamento Bolzano+Trento in "Trentino-Alto Adige" usa una media semplice tra le due
  province — andrebbe pesata per popolazione per un uso reale (qui è un'approssimazione
  accettabile per un progetto di portfolio).
- Le serie sono annuali (2015-2025, 11 punti per regione): abbastanza per un backtest ma poco
  per un forecasting ad alta confidenza — vedi la nota nel notebook di forecasting.
- Stretch goal possibili: CeleryExecutor per Airflow, dbt per la trasformazione silver→gold,
  deploy della dashboard su un servizio cloud, alerting su fallimento DAG, dati a livello
  provinciale (NUTS3) invece che regionale.
