import pandas as pd
import json

kv_parti_resultater = pd.read_csv("data/struktureret/kv/kv25_resultater_partier.csv")

for kommune_dagi in kv_parti_resultater['kommune_kode'].unique():
    kommunedata = kv_parti_resultater[kv_parti_resultater['kommune_kode'] == kommune_dagi]
    kommunenavn = kommunedata['kommune'].values[0].replace(" Kommune", "")

    # find the corresponding files in valgresultater/
    kom_geo = pd.read_csv(f"data/struktureret/kv/valgresultater/kommune/{kommune_dagi}_{kommunenavn.lower()}_kommune.csv")
    afst_geo = pd.read_csv(f"data/struktureret/kv/valgresultater/afstemningssteder/{kommune_dagi}_{kommunenavn.lower()}_afstemningsområde.csv")

    grouped = kommunedata.groupby('parti').agg({'stemmer':'sum'}).reset_index()
    print(grouped)

    kommunedata['parti_procent'] = round(kommunedata['stemmer'] / kommunedata['total_gyldige_stemmer'] * 100,1)

    # turn from long to wide format
    kommunedata_wide = kommunedata.pivot_table(index=['kommune', 'kommune_kode','afstemningsområde_dagi_id','afstemningsområde','resultat_art'], columns='parti', values='parti_procent').reset_index()
    print(kommunedata_wide)



