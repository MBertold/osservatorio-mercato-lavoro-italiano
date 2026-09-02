# Osservatorio Mercato del Lavoro Italiano

Progetto end-to-end su dati aperti ISTAT (API SDMX) che copre in un'unica pipeline coerente
tre profili: **data engineering** (estrazione, orchestrazione, storage), **data analysis**
(dashboard) e **data science** (forecasting, clustering).

```
Airflow DAG (extract → bronze → silver/gold → quality check)
        │  Postgres (bronze / silver / gold)
        ├──> Streamlit dashboard (KPI, mappa, serie storiche, confronto Nord/Centro/Sud)
        └──> Notebook Jupyter (forecasting SARIMAX, clustering K-means)
```

Tutto containerizzato con Docker Compose.

## Perché questo stack

- **Postgres** invece di un file DuckDB: con più container che leggono/scrivono in parallelo
  (Airflow + dashboard) serve un database con vera concorrenza.
- **Airflow con LocalExecutor**: orchestrazione realistica ma senza la complessità
  operativa di Celery/Redis, adatta a girare su una singola macchina.
- **Rate limiter cross-processo su file** (`src/istat_client.py`): ISTAT impone un limite
  di **5 richieste/minuto per IP**, con blocco di 1-2 giorni se superato. Airflow può eseguire
  i task di estrazione in parallelo (dynamic task mapping su `extract_indicatore`), quindi il
  rate limiting va condiviso tra processi, non tenuto in memoria in un singolo processo: per
  questo lo stato è persistito su un file JSON protetto da lock (`filelock`).
- **Cache su disco delle risposte grezze**: nessuna richiesta viene mai ripetuta per errore.

## Struttura del repo

```
docker-compose.yml          postgres, airflow-init/webserver/scheduler, dashboard
airflow/
  Dockerfile                 estende apache/airflow con le dipendenze in più
  dags/mercato_lavoro_dag.py DAG TaskFlow: discover -> extract -> bronze -> silver/gold -> quality
src/
  config.py                   connessione DB + configurazione dei dataflow ISTAT
  istat_client.py              client SDMX: rate limiting, cache, retry/backoff
  discover.py                   script di discovery per confermare i flow_ref
  extract.py                    estrazione verso data/raw/<indicatore>.csv
  transform.py                  bronze -> silver -> gold (schema a stella) su Postgres
  quality_checks.py             controlli di qualità sul layer gold
dashboard/app.py              dashboard Streamlit (legge gold via Postgres)
analysis/                     notebook di forecasting e clustering
tests/                        unit test (pytest) sulla logica di trasformazione
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

## Primo utilizzo: confermare i dataflow ISTAT

Solo `tasso_disoccupazione` è già configurato con un `flow_ref` verificato in
`src/config.py`. Gli altri (`tasso_occupazione`, `tasso_inattivita`, `neet`) vanno confermati
una volta, eseguendo (fuori da Airflow, per non consumare la quota di 5 richieste/minuto durante
lo sviluppo):

```bash
python -m venv .venv && source .venv/bin/activate   # o .venv\Scripts\activate su Windows
pip install -r requirements-dev.txt
python -m src.discover occupazione disoccupazione inattività neet
```

Copia i `flow_ref` trovati in `src/config.py` (campo `verificato: True`), poi rilancia il DAG.

## Eseguire la pipeline

Nella UI di Airflow (http://localhost:8081), attiva e lancia manualmente il DAG
`mercato_lavoro_istat`. Task: `discover_dataflows → extract_indicatore (per ciascun indicatore,
in dynamic task mapping) → load_bronze → transform_silver_gold → quality_checks`.

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

I test coprono la logica pura di trasformazione (`src/transform.py`) e il rate limiter
(`src/istat_client.py`), senza dipendere da rete o database.

## Limiti noti / prossimi passi

- Solo il tasso di disoccupazione ha il `flow_ref` confermato: vedi sopra per completare gli altri.
- Nessun ambiente di sviluppo di questo progetto ha accesso diretto a `esploradati.istat.it`
  (rete esterna bloccata): il primo run reale del DAG va verificato sulla propria macchina.
- Stretch goal possibili: CeleryExecutor per Airflow, dbt per la trasformazione silver→gold,
  deploy della dashboard su un servizio cloud, alerting su fallimento DAG.
