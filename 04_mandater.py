import pandas as pd
import os
import glob
import json
from pathlib import Path

from config import FROM_PATH, TO_PATH, FOLDERS

def get_mandater(from_path=FROM_PATH, to_path=TO_PATH, folder=FOLDERS, valg="kv"):
    mandat_folder = folder[2]  # "verifikation/mandatfordeling"
    filer = glob.glob(os.path.join(from_path, valg, mandat_folder, "*.json"))
    
    mandater = []

    for file in filer:
        try:
            with open(file, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue
       
       
        if valg == "rv":
            base = {
                "valgart": data.get("Valgart"),
                "region": data.get("Region"),
                "region_dagi_id": data.get("RegionDagiId"),
                "resultat_art": data.get("Resultatart"),
                "frigivelsestidspunkt": data.get("FrigivelsesTidspunktUTC"),
            }
        else:  # valg == "kv"
            base = {
                "valgart": data.get("Valgart"),
                "kommune": data.get("Kommune"),
                "kommune_kode": data.get("Kommunekode"),
                "resultat_art": data.get("Resultatart"),
                "frigivelsestidspunkt": data.get("FrigivelsesTidspunktUTC"),
            }


        # the mandater is either in the key "Personlige Mandater" or "Listemandater"
        for mandat_type in ["PersonligeMandater", "ListeMandater"]:
            for mandat in data.get(mandat_type, []):
                mandater.append({
                    **base,
                    "nummer" : mandat.get("Nummer"),
                    "mandat_type": mandat_type,
                    "kandidat": mandat.get("Stemmeseddelnavn", None),
                    "kandidat_id": mandat.get("KandidatId", None),
                    "parti": mandat.get("KandidatlisteNavn"),
                    "parti_id": mandat.get("KandidatlisteId"),
                })
    return mandater

kv_mandater = get_mandater(FROM_PATH, TO_PATH, FOLDERS, "kv")
rv_mandater = get_mandater(FROM_PATH, TO_PATH, FOLDERS, "rv")

df_kv_mandater = pd.DataFrame(kv_mandater)
df_rv_mandater = pd.DataFrame(rv_mandater)

outdir_kv = Path(TO_PATH) / "kv"
outdir_kv.mkdir(parents=True, exist_ok=True)
df_kv_mandater.to_csv(outdir_kv / "kv25_mandater.csv", index=False)

outdir_rv = Path(TO_PATH) / "rv"
outdir_rv.mkdir(parents=True, exist_ok=True)
df_rv_mandater.to_csv(outdir_rv / "rv25_mandater.csv", index=False)


