import pandas as pd
import os
import glob
import json
import datetime
from pathlib import Path

from helper_functions import kombiner_resultater
from config import FROM_PATH, TO_PATH, KOMMUNE_INFO, FOLDERS


# load kommune info for region mapping
with open(KOMMUNE_INFO, "r", encoding="utf-8") as f:
    kommune_info = json.load(f)

# RV25 - Valgresultater
def get_rv_resultater(from_path=FROM_PATH, to_path=TO_PATH, folders=FOLDERS, kommune_info=kommune_info, *_unused):
    files = kombiner_resultater(from_path, to_path, "rv", folders[0])  # "valgresultater"
    partier, kandidater = [], []

    for file in files:
        try:
            with open(file, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue
        
        # find the region that corresponds with the kommune code in kommuner.json
        kommune_kode = str(data.get("Kommunekode"))
        region = None
        for kommune in kommune_info:
            if kommune.get("kommune_id") == kommune_kode:
                region = kommune.get("region")
                break


        # define base structure for each entry (what columns do we want in the data)
        base = {
            "region": region,
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
                "parti_id": parti.get("KandidatlisteId"),
                "parti_bogstav": parti.get("Bogstavbetegnelse"),
                "stemmer": parti.get("Stemmer", 0),
                "listestemmer": parti.get("Listestemmer", 0),
                "difference_forrige_valg": parti.get("StemmerDifferenceFraForrigeValg", 0),
            })

            for kandidat in (parti.get("Kandidater") or []):
                kandidater.append({
                    **base,
                    "kandidat": kandidat.get("Stemmeseddelnavn"),
                    'kandidat_id': kandidat.get("Id"),
                    "parti": parti.get("Navn"),
                    "parti_id": parti.get("KandidatlisteId"),
                    "parti_bogstav": parti.get("Bogstavbetegnelse"),
                    "stemmer": kandidat.get("Stemmer", 0),
                })

    return partier, kandidater

partier, kandidater = get_rv_resultater(FROM_PATH, TO_PATH, FOLDERS, kommune_info, "rv", "valgresultater")

df_partier = pd.DataFrame(partier)
df_kandidater = pd.DataFrame(kandidater)

# Convert datetime columns (dd-mm-yyyy hh:mm:ss), coercing invalid/missing values
for df in (df_partier, df_kandidater):
    for col in ("frigivelsestidspunkt", "godkendelsestidspunkt"):
        if col in df:
            df[col] = pd.to_datetime(df[col], format="%d-%m-%Y %H:%M:%S", errors="coerce")

outdir = Path(TO_PATH) / "rv"
outdir.mkdir(parents=True, exist_ok=True)
df_partier.to_csv(outdir / "rv25_resultater_partier.csv", index=False)
df_kandidater.to_csv(outdir / "rv25_resultater_kandidater.csv", index=False)