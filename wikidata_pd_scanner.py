#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script amélioré : interroge Wikidata et inclut une colonne `genres` (libellés) dans les CSV.
Filtres appliqués ensuite pour déterminer les candidats par pays (EU70, Mexico100, US_pub).
"""
from datetime import datetime
import os
import requests
import pandas as pd
import yaml

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {"Accept": "application/sparql-results+json", "User-Agent": "PD-Scanner/1.0 (veille)"}

# -- QIDs ciblés pour les genres (romance, aventure, science-fiction, enfants, illustrés, policier, espionnage)
GENRE_QIDS = [
    "Q48290",   # romance novel (romance)
    "Q188473",  # adventure fiction (aventure)
    "Q24925",   # science fiction
    "Q340169",  # children's literature
    "Q173281",  # picture book / illustrated book
    "Q828130",  # detective fiction
    "Q208016",  # spy fiction
]

# -- QIDs pour types (novel, literary work, children's book)
TYPE_QIDS = [
    "Q8261",     # novel
    "Q7725634",  # literary work
    "Q3331189",  # children's book
]

def load_config(path="config/pd_rules.yaml"):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    return {}

def build_sparql():
    genre_values = " ".join(f"wd:{q}" for q in GENRE_QIDS)
    type_values = " ".join(f"wd:{q}" for q in TYPE_QIDS)
    q = f"""
    SELECT ?work ?workLabel ?author ?authorLabel ?death ?pubYear ?langLabel ?wp (GROUP_CONCAT(DISTINCT ?genreLabel; separator="|") AS ?genres)
    WHERE {{
      {{ VALUES ?g {{ {genre_values} }} ?work wdt:P136 ?g. }}
      UNION
      {{ VALUES ?t {{ {type_values} }} ?work wdt:P31 ?t. }}
      ?work wdt:P50 ?author.
      OPTIONAL {{ ?author wdt:P570 ?death. }}
      OPTIONAL {{ ?work wdt:P577 ?pubDate. BIND(YEAR(?pubDate) AS ?pubYear) }}
      OPTIONAL {{ ?work wdt:P407 ?lang. }}
      OPTIONAL {{
        ?wp_schema schema:about ?work ;
                   schema:isPartOf/wikibase:wikiGroup "wikipedia" ;
                   schema:name ?wp.
      }}
      OPTIONAL {{ ?work wdt:P136 ?genre. }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],fr,en" }}
    }}
    GROUP BY ?work ?workLabel ?author ?authorLabel ?death ?pubYear ?langLabel ?wp
    LIMIT 10000
    """
    return q

def run_sparql(query):
    resp = requests.get(WIKIDATA_ENDPOINT, params={"query": query}, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json().get("results", {}).get("bindings", [])

def get_val(row, key):
    v = row.get(key)
    return v.get("value") if v else None

def extract_year_from_iso(date_literal):
    if not date_literal:
        return None
    try:
        return int(date_literal[:4])
    except Exception:
        return None

def main():
    cfg = load_config()
    Y = int(cfg.get("current_year", datetime.utcnow().year))
    eu_cutoff = Y - 71
    mx_cutoff = Y - 101
    # US rule: publication year threshold (historical fixed for entry en 2025)
    us_pub_cutoff = 1929 if Y >= 2025 else 1929

    print(f"Année courante utilisée: {Y} ; EU cutoff: {eu_cutoff} ; MX cutoff: {mx_cutoff} ; US pub cutoff: {us_pub_cutoff}")
    query = build_sparql()
    rows = run_sparql(query)
    print(f"Résultats bruts Wikidata: {len(rows)} lignes")

    data = []
    for r in rows:
        title = get_val(r, "workLabel")
        author = get_val(r, "authorLabel")
        author_qid = get_val(r, "author")
        death_raw = get_val(r, "death")
        death_year = extract_year_from_iso(death_raw)
        pubYear = get_val(r, "pubYear")
        lang = get_val(r, "langLabel")
        wp = get_val(r, "wp")
        genres_raw = get_val(r, "genres")  # string with '|' separator or None
        genres = [g for g in (genres_raw.split("|") if genres_raw else []) if g]

        regions = []
        if death_year is not None:
            if death_year <= eu_cutoff:
                regions.append("EU70")
            if death_year <= mx_cutoff:
                regions.append("Mexico100")
        if pubYear:
            try:
                if int(pubYear) <= us_pub_cutoff:
                    regions.append("US_pub")
            except Exception:
                pass

        data.append({
            "title": title,
            "author": author,
            "author_qid": author_qid,
            "author_death_year": death_year,
            "publication_year": pubYear,
            "language": lang,
            "wikipedia_page": wp,
            "genres": ",".join(genres) if genres else "",
            "regions": ",".join(sorted(set(regions))) if regions else "",
        })

    df = pd.DataFrame(data)
    # découpage par région
    df_eu = df[df["regions"].str.contains("EU70", na=False)].copy()
    df_mx = df[df["regions"].str.contains("Mexico100", na=False)].copy()
    df_us = df[df["regions"].str.contains("US_pub", na=False)].copy()
    # dossier de sortie
    today = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = os.path.join("output", today)
    os.makedirs(out_dir, exist_ok=True)
    df_eu.to_csv(os.path.join(out_dir, "EU70_candidates.csv"), index=False)
    df_mx.to_csv(os.path.join(out_dir, "Mexico100_candidates.csv"), index=False)
    df_us.to_csv(os.path.join(out_dir, "US_pub_candidates.csv"), index=False)
    # global
    if not df_eu.empty or not df_mx.empty or not df_us.empty:
        df_all = pd.concat([df_eu.assign(region="EU70"), df_mx.assign(region="Mexico100"), df_us.assign(region="US_pub")], ignore_index=True)
        df_all = df_all.drop_duplicates(subset=["title", "author"])
        df_all.to_csv(os.path.join(out_dir, "ALL_candidates.csv"), index=False)
    else:
        # save full df as fallback
        df.to_csv(os.path.join(out_dir, "ALL_candidates.csv"), index=False)

    print(f"Export CSV dans: {out_dir} ; total lignes exportées: {len(df)}")

if __name__ == '__main__':
    main()
