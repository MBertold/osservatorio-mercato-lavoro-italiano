"""Dashboard Streamlit: Osservatorio Mercato del Lavoro Italiano (layer gold)."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from sqlalchemy import create_engine

from src.config import warehouse_connection_string

st.set_page_config(page_title="Osservatorio Mercato del Lavoro", layout="wide")

ITALY_REGIONS_GEOJSON_URL = (
    "https://raw.githubusercontent.com/openpolis/geojson-italy/master/geojson/limits_IT_regions.geojson"
)


@st.cache_resource
def get_engine():
    return create_engine(warehouse_connection_string())


@st.cache_data(ttl=600)
def load_fact() -> pd.DataFrame:
    engine = get_engine()
    fact = pd.read_sql("SELECT * FROM gold.fact_mercato_lavoro", engine)
    dim_territorio = pd.read_sql("SELECT * FROM gold.dim_territorio", engine)
    dim_tempo = pd.read_sql("SELECT * FROM gold.dim_tempo", engine)
    df = fact.merge(dim_territorio, on="cod_territorio", how="left").merge(dim_tempo, on="periodo", how="left")
    return df


@st.cache_data(ttl=3600)
def load_regions_geojson() -> dict | None:
    try:
        resp = requests.get(ITALY_REGIONS_GEOJSON_URL, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException:
        return None


st.title("Osservatorio Mercato del Lavoro Italiano")
st.caption("Fonte: ISTAT (API SDMX) — pipeline Airflow su Postgres, layer gold")

try:
    df = load_fact()
except Exception as exc:  # noqa: BLE001 - mostriamo l'errore invece di un traceback grezzo
    st.error(
        "Impossibile leggere il layer gold dal warehouse. "
        "Assicurati che il DAG Airflow `mercato_lavoro_istat` abbia completato almeno un run.\n\n"
        f"Dettaglio: {exc}"
    )
    st.stop()

if df.empty:
    st.warning("Il layer gold è vuoto: esegui il DAG Airflow prima di usare la dashboard.")
    st.stop()

indicatori_disponibili = sorted(df["indicatore"].unique())

with st.sidebar:
    st.header("Filtri")
    indicatore = st.selectbox("Indicatore", indicatori_disponibili)
    sessi = sorted(df["sesso"].unique())
    sesso = st.selectbox("Sesso", sessi, index=sessi.index("totale") if "totale" in sessi else 0)
    classi_eta = sorted(df["classe_eta"].unique())
    classe_eta = st.selectbox("Classe di età", classi_eta, index=classi_eta.index("totale") if "totale" in classi_eta else 0)
    regioni = sorted(df["territorio"].dropna().unique())
    regioni_selezionate = st.multiselect("Regioni (vuoto = tutte)", regioni)

filtered = df[(df["indicatore"] == indicatore) & (df["sesso"] == sesso) & (df["classe_eta"] == classe_eta)]
if regioni_selezionate:
    filtered = filtered[filtered["territorio"].isin(regioni_selezionate)]

filtered = filtered.sort_values("periodo")

col1, col2, col3 = st.columns(3)
ultimo_periodo = filtered["periodo"].max()
valore_nazionale = filtered.loc[filtered["periodo"] == ultimo_periodo, "valore"].mean()
col1.metric(f"Valore medio più recente ({ultimo_periodo})", f"{valore_nazionale:.1f}%" if pd.notna(valore_nazionale) else "N/D")

periodi_ordinati = sorted(filtered["periodo"].unique())
if len(periodi_ordinati) > 1:
    penultimo = periodi_ordinati[-2]
    valore_precedente = filtered.loc[filtered["periodo"] == penultimo, "valore"].mean()
    delta = valore_nazionale - valore_precedente if pd.notna(valore_precedente) else None
    col2.metric("Variazione vs periodo precedente", f"{delta:+.1f} p.p." if delta is not None else "N/D")
else:
    col2.metric("Variazione vs periodo precedente", "N/D")

col3.metric("Regioni coperte", filtered["territorio"].nunique())

tab_mappa, tab_serie, tab_confronto, tab_neet = st.tabs(
    ["Mappa regionale", "Serie storica", "Confronto Nord/Centro/Sud", "Focus NEET"]
)

with tab_mappa:
    ultimo = filtered[filtered["periodo"] == ultimo_periodo]
    geojson = load_regions_geojson()
    if geojson is not None and not ultimo.empty:
        fig = px.choropleth(
            ultimo,
            geojson=geojson,
            locations="territorio",
            featureidkey="properties.reg_name",
            color="valore",
            color_continuous_scale="Blues",
            scope="europe",
            title=f"{indicatore} — {ultimo_periodo}",
        )
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("GeoJSON regioni non disponibile (offline?): mostro un grafico a barre in alternativa.")
        st.bar_chart(ultimo.set_index("territorio")["valore"])

with tab_serie:
    fig = px.line(
        filtered.sort_values(["territorio", "periodo"]), x="periodo", y="valore", color="territorio",
        title=f"{indicatore} nel tempo",
    )
    st.plotly_chart(fig, use_container_width=True)

with tab_confronto:
    confronto = (
        filtered[filtered["ripartizione"].isin(["Nord", "Centro", "Sud"])]
        .groupby(["periodo", "ripartizione"], as_index=False)["valore"]
        .mean()
    )
    fig = px.line(confronto, x="periodo", y="valore", color="ripartizione", title=f"{indicatore}: Nord vs Centro vs Sud")
    st.plotly_chart(fig, use_container_width=True)

with tab_neet:
    if "neet" in indicatori_disponibili:
        neet_df = df[df["indicatore"] == "neet"].sort_values("periodo")
        fig = px.line(neet_df, x="periodo", y="valore", color="territorio", title="NEET nel tempo")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Indicatore NEET non ancora presente nel warehouse: esegui prima il DAG Airflow.")
