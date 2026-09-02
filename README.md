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
- **Estrazione via [`istatapi`](https://github.com/Attol8/istatapi)** invece di un client SDMX
  scritto da zero: risolve da sola l'identificatore di un dataflow (basta il nome della
  struttura dati, es. `DCCV_TAXOCCU`, senza dover conoscere in anticipo il codice numerico
  di catalogo) e restituisce direttamente un DataFrame pandas.
- **Rate limiter cross-processo su file** (`src/istat_client.py`): ISTAT impone un limite
  di **5 richieste/minuto per IP**, con blocco di 1-2 giorni se superato — cosa che `istatapi`
  di per sé non rispetta (una singola query può generare 3-4 richieste HTTP interne). Qui
  intercettiamo *ogni* richiesta HTTP di istatapi con un monkeypatch e la seriamo dietro un
  limitatore persistito su file JSON (`filelock`), condiviso anche tra i task Airflow che
  possono girare in processi separati.
- **Cache del catalogo dataflow**: istatapi lo riscaricherebbe per intero ad ogni query;
  qui lo scarichiamo una volta sola e lo teniamo in cache su disco.

## Struttura del repo

```
docker-compose.yml          postgres, airflow-init/webserver/scheduler, dashboard
airflow/
  Dockerfile                 estende apache/airflow con le dipendenze in più
  dags/mercato_lavoro_dag.py DAG TaskFlow: discover -> extract -> bronze -> silver/gold -> quality
src/
  config.py                   connessione DB + configurazione dei dataflow ISTAT
  istat_client.py              wrapper su istatapi: rate limiting, cache del catalogo
  discover.py                   script di discovery per trovare gli identificatori
  extract.py                    estrazione (via istatapi) verso data/raw/<indicatore>.csv
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

## Primo utilizzo: trovare gli identificatori mancanti

`src/config.py` contiene, per ogni indicatore, l'identificatore da passare a `istatapi`
(il nome della struttura dati SDMX, es. `DCCV_TAXOCCU`). Stato attuale:

- `tasso_disoccupazione`, `tasso_occupazione`: identificatori noti (il secondo indicato
  direttamente dall'utente del progetto).
- `tasso_inattivita`: candidato non confermato, trovato in documentazione di terzi.
- `neet`: nessun candidato — va cercato da zero.

Un identificatore sbagliato fa fallire (con un errore chiaro) solo il task Airflow di
quell'indicatore, non l'intero DAG — quindi non è indispensabile confermare tutto prima di
lanciare la pipeline la prima volta. Per cercare/confermare un identificatore (fuori da
Airflow, per non consumare la quota di 5 richieste/minuto — serve una rete che riesca a
raggiungere `esploradati.istat.it`, vedi limiti noti più sotto):

```bash
python -m venv .venv && source .venv/bin/activate   # o .venv\Scripts\activate su Windows
pip install -r requirements-dev.txt
python -m src.discover occupazione disoccupazione inattività neet unemployment employment
```

Cerca sia in italiano che in inglese (il catalogo ISTAT ha entrambe le descrizioni). Copia
il `df_structure_id` che stampa in `src/config.py`.

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

- `tasso_inattivita` è un identificatore candidato non confermato, `neet` è ancora da trovare:
  vedi sopra.
- L'ambiente in cui questo progetto è stato sviluppato non ha mai raggiunto
  `esploradati.istat.it`/`sdmx.istat.it` in modo affidabile (a seconda del percorso: connessione
  rifiutata, timeout, o un loop di redirect lato server) — il primo run reale del DAG va quindi
  verificato sulla propria macchina/rete, idealmente controllando prima con un browser vero che
  `https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1` risponda.
- Stretch goal possibili: CeleryExecutor per Airflow, dbt per la trasformazione silver→gold,
  deploy della dashboard su un servizio cloud, alerting su fallimento DAG.
