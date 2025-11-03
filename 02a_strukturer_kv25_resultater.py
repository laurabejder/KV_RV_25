import pandas as pd
import os
import glob
import json
import datetime
from pathlib import Path

from helper_functions import kombiner_resultater

from_path = "data/raw/"
to_path = "data/struktureret/"

# KV25 - Valgresultater
def get_kv_resultater(from_path=from_path, to_path=to_path, *_unused):
    files = kombiner_resultater(from_path, to_path, "kv", "valgresultater")
    partier, kandidater = [], []

    for file in files:
        try:
            with open(file, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue
        
        # define base structure for each entry (what columns do we want in the data)
        base = {
            "kommune": data.get("Kommune"),
            "kommune_kode": data.get("Kommunekode"),
            "afstemningsområde": data.get("Afstemningsområde"),
            "afstemningsområde_dagi_id": data.get("AfstemningsområdeDagiId"),
            "frigivelsestidspunkt": data.get("FrigivelsesTidspunktUTC"),
            "godkendelsestidspunkt": data.get("GodkendelsesTidspunktUTC"),
            "resultat_art": data.get("Resultatart"),
            "total_gyldige_stemmer": data.get("GyldigeStemmer"),
            "total_afgivne_stemmer": data.get("AfgivneStemmer"),
        }

        # if there are no results, add a placeholder entry
        if data.get("Resultatart") == "IngenResultater":
            partier.append({
                **base, "parti": None, "stemmer": 0, "listestemmer": 0,
                "difference_forrige_valg": 0
            })
            continue
        
        # if there are results present, iterate through parties and candidates
        for parti in data.get("Kandidatlister", []):
            partier.append({
                **base,
                "parti": parti.get("Navn"),
                "stemmer": parti.get("Stemmer", 0),
                "listestemmer": parti.get("Listestemmer", 0),
                "difference_forrige_valg": parti.get("StemmerDifferenceFraForrigeValg", 0),
            })

            for kandidat in (parti.get("Kandidater") or []):
                kandidater.append({
                    **base,
                    "parti": parti.get("Navn"),
                    "kandidat": kandidat.get("Stemmeseddelnavn"),
                    "stemmer": kandidat.get("Stemmer", 0),
                })

    return partier, kandidater

kv_partier, kv_kandidater = get_kv_resultater(from_path, to_path, "kv", "valgresultater")

df_kv_partier = pd.DataFrame(kv_partier)
df_kv_kandidater = pd.DataFrame(kv_kandidater)

# Convert datetime columns (dd-mm-yyyy hh:mm:ss), coercing invalid/missing values
for df in (df_kv_partier, df_kv_kandidater):
    for col in ("frigivelsestidspunkt", "godkendelsestidspunkt"):
        if col in df:
            df[col] = pd.to_datetime(df[col], format="%d-%m-%Y %H:%M:%S", errors="coerce")

outdir = Path(to_path) / "kv"
outdir.mkdir(parents=True, exist_ok=True)
df_kv_partier.to_csv(outdir / "kv25_resultater_partier.csv", index=False)
df_kv_kandidater.to_csv(outdir / "kv25_resultater_kandidater.csv", index=False)