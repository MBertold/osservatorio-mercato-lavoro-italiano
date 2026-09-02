"""Script di discovery: interroga il catalogo SDMX ISTAT e cerca i dataflow
per parola chiave, per confermare i flow_ref esatti da mettere in config.py.

Uso:
    python -m src.discover occupazione disoccupazione inattivita neet

Va eseguito una volta, fuori da Airflow, per non consumare inutilmente la
quota di 5 richieste/minuto imposta da ISTAT durante lo sviluppo del DAG.
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET

from src.istat_client import IstatClient


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _find_all(elem: ET.Element, local_name: str):
    return [child for child in elem.iter() if _local_name(child.tag) == local_name]


def parse_dataflows(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    results = []

    for dataflow in _find_all(root, "Dataflow"):
        names = {}
        for name_el in _find_all(dataflow, "Name"):
            lang = name_el.attrib.get("{http://www.w3.org/XML/1998/namespace}lang", "?")
            names[lang] = (name_el.text or "").strip()

        structure_id = None
        for ref in _find_all(dataflow, "Ref"):
            if ref.attrib.get("class") == "DataStructure":
                structure_id = ref.attrib.get("id")
                break

        results.append(
            {
                "flow_ref": dataflow.attrib.get("id"),
                "agency_id": dataflow.attrib.get("agencyID"),
                "version": dataflow.attrib.get("version"),
                "structure_id": structure_id,
                "names": names,
            }
        )

    return results


def search(keywords: list[str]) -> None:
    client = IstatClient()
    print("Scarico (o leggo da cache) il catalogo completo dei dataflow ISTAT...")
    xml_text = client.get_dataflows()
    dataflows = parse_dataflows(xml_text)
    print(f"Trovati {len(dataflows)} dataflow totali nel catalogo.\n")

    keywords_lower = [k.lower() for k in keywords]

    for df in dataflows:
        all_names = " ".join(df["names"].values()).lower()
        if any(k in all_names for k in keywords_lower):
            print(f"flow_ref={df['flow_ref']}  structure_id={df['structure_id']}  version={df['version']}")
            for lang, name in df["names"].items():
                print(f"    [{lang}] {name}")
            print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    logging_keywords = sys.argv[1:]
    search(logging_keywords)
