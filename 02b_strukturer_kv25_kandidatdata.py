import pandas as pd
import os
import glob
import json
import datetime

from helper_functions import kombiner_resultater, strip_kommune

# Paths
from_path = "data/raw/"
to_path = "data/struktureret/"

def get_kv_kandidatdata(from_path, to_path, valg, data_type):
    all_files = kombiner_resultater(from_path, to_path, "kv", "kandidat-data")
    kandidat_data = []

    for file in all_files:
        data = json.load(open(file, 'r', encoding='utf-8'))
        try:
            for kandidater in data['Kandidatlister']:
                for kandidat in kandidater['Kandidater']:
                    kandidat_data.append({
                        'kommune': data['Kommune'],
                        'kommune_dagi_id': data['KommuneDagiId'],
                        'frigivelsestidspunkt': data['FrigivelsesTidspunktUTC'],
                        'opdateringstidspunkt': data['OpdateringsTidspunktUTC'],
                        'parti_stemmeseddelsplacering': kandidater['Stemmeseddelsplacering'],
                        'parti_navn': kandidater['Navn'],
                        'parti_bogstav': kandidater['Bogstavbetegnelse'],
                        'parti_opstillingsform': kandidater['Opstillingsform'],
                        'kandidat_navn': kandidat['Navn'],
                        'kandidat_stemmeseddelnavn': kandidat['Stemmeseddelnavn'],
                        'kandidat_stilling': kandidat['Stilling']
                    })
        except Exception as e:  
            print(f"Fejl ved læsning af {file}: {e}")
    
    return kandidat_data

kandidat_data = get_kv_kandidatdata(from_path, to_path, "kv", "kandidat-data")
df_kandidat_data = pd.DataFrame(kandidat_data)

df_kandidat_data['opdateringstidspunkt'] = pd.to_datetime(df_kandidat_data['opdateringstidspunkt'], format='%d-%m-%Y %H:%M:%S')
df_kandidat_data['frigivelsestidspunkt'] = pd.to_datetime(df_kandidat_data['frigivelsestidspunkt'], format='%d-%m-%Y %H:%M:%S')

df_kandidat_data.to_csv("data/struktureret/kv25_kandidat_data.csv", index=False)
