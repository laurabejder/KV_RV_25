import pandas as pd
import os
import glob
import json
import datetime

from helper_functions import kombiner_resultater
from helper_functions import strip_kommune

# Paths
from_path = "data/raw/"
to_path = "data/struktureret/"

# KV25 - Valgresultater
def get_kv_resultater(from_path, to_path, valg, data_type):
    all_files = kombiner_resultater(from_path, to_path, "kv", "valgresultater")
    partier_resultater = []
    kandidat_resultater = []

    for file in all_files:
        # get the parti results
        data = json.load(open(file, 'r', encoding='utf-8'))
        try:
            for parti in data['Kandidatlister']:
                partier_resultater.append({
                    'kommune': data['Kommune'],
                    'kommune_kode': data['Kommunekode'],
                    'afstemningsområde': data['Afstemningsområde'],
                    'afstemningsområde_dagi_id': data['AfstemningsområdeDagiId'],
                    'godkendelsesdato': data['GodkendelsesDatoUTC'],
                    'frigivelsestidspunkt': data['FrigivelsesTidspunktUTC'],
                    'parti': parti['Navn'],
                    'stemmer': parti['Stemmer'],
                    'listestemmer': parti['Listestemmer'],
                    'difference_forrige_valg' : parti['StemmerDifferenceFraForrigeValg']
                })

                for kandidat in parti['Kandidater']:

                    kandidat_resultater.append({
                        'kommune': data['Kommune'],
                        'kommune_kode': data['Kommunekode'],
                        'afstemningsområde': data['Afstemningsområde'],
                        'afstemningsområde_dagi_id': data['AfstemningsområdeDagiId'],
                        'godkendelsesdato': data['GodkendelsesDatoUTC'],
                        'frigivelsestidspunkt': data['FrigivelsesTidspunktUTC'],
                        'parti': parti['Navn'],
                        'kandidat': kandidat['Stemmeseddelnavn'],
                        'stemmer': kandidat['Stemmer']
                    })
        except Exception as e:
            print(f"Fejl ved læsning af {file}: {e}")
    return partier_resultater, kandidat_resultater

kv_resultater = get_kv_resultater(from_path, to_path, "kv", "valgresultater")

df_kv_partier = pd.DataFrame(kv_resultater[0])
df_kv_kandidater = pd.DataFrame(kv_resultater[1])


# fix the datetime columns in dd-mm-yyyy hh:mm:ss format
df_kv_partier['godkendelsesdato'] = pd.to_datetime(df_kv_partier['godkendelsesdato'], format='%d-%m-%Y %H:%M:%S')
df_kv_partier['frigivelsestidspunkt'] = pd.to_datetime(df_kv_partier['frigivelsestidspunkt'], format='%d-%m-%Y %H:%M:%S')
df_kv_kandidater['godkendelsesdato'] = pd.to_datetime(df_kv_kandidater['godkendelsesdato'], format='%d-%m-%Y %H:%M:%S')
df_kv_kandidater['frigivelsestidspunkt'] = pd.to_datetime(df_kv_kandidater['frigivelsestidspunkt'], format='%d-%m-%Y %H:%M:%S')

df_kv_partier.to_csv("data/struktureret/kv25_resultater_partier.csv", index=False)
df_kv_kandidater.to_csv("data/struktureret/kv25_resultater_kandidater.csv", index=False)
