"""Script di discovery: cerca nel catalogo SDMX ISTAT per parola chiave
(italiano o inglese), per trovare l'identificatore da mettere in config.py.

Uso:
    python -m src.discover occupazione unemployment inattività neet

Va eseguito una volta, fuori da Airflow, per non consumare inutilmente la
quota di 5 richieste/minuto imposta da ISTAT durante lo sviluppo del DAG.
Richiede una rete che riesca a raggiungere esploradati.istat.it (vedi README).
"""

from __future__ import annotations

import sys

from src.istat_client import get_dataflow_catalog


def search(keywords: list[str]) -> None:
    print("Scarico (o leggo da cache) il catalogo completo dei dataflow ISTAT...")
    catalog = get_dataflow_catalog()
    print(f"Trovati {len(catalog)} dataflow totali nel catalogo.\n")

    keywords_lower = [k.lower() for k in keywords]

    for _, row in catalog.iterrows():
        haystack = f"{row.get('df_description', '')} {row.get('df_description_it', '')}".lower()
        if any(k in haystack for k in keywords_lower):
            print(f"df_id={row['df_id']}  df_structure_id={row['df_structure_id']}  version={row['version']}")
            print(f"    [en] {row.get('df_description', '')}")
            print(f"    [it] {row.get('df_description_it', '')}")
            print(f"    -> in config.py puoi usare come identificatore: \"{row['df_structure_id']}\" (o \"{row['df_id']}\")")
            print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    search(sys.argv[1:])
