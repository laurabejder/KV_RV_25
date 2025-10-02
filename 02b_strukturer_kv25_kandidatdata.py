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
    valgforbund_data = []
    kandidat_data = []

    for file in all_files:
        data = json.load(open(file, 'r', encoding='utf-8'))
        try:
        
            for valgforbund in data['Valgforbund']:
                valgforbund_data.append({
                    'kommune': data['Kommune'],
                    'kommune_dagi_id': data['KommuneDagiId'],
                    'frigivelsestidspunkt': data['FrigivelsesTidspunktUTC'],
                    'opdateringstidspunkt': data['OpdateringsTidspunktUTC'],
                    'valgforbund_navn': valgforbund['Navn'],
                    'kandidatliste_id': valgforbund['KandidatlisteId']
                })
        except Exception as e:
            print(f"Fejl ved læsning af valgforbund i {file}: {e}")

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
                        'kandidatliste_id': kandidater['KandidatlisteId'],
                        'kandidat_navn': kandidat['Navn'],
                        'kandidat_stemmeseddelnavn': kandidat['Stemmeseddelnavn'],
                        'kandidat_stilling': kandidat['Stilling']
                    })
        except Exception as e:  
            print(f"Fejl ved læsning af {file}: {e}")
    
    return kandidat_data, valgforbund_data

data = get_kv_kandidatdata(from_path, to_path, "kv", "kandidat-data")
df_kandidat_data = pd.DataFrame(data[0])
df_valgforbund_data = pd.DataFrame(data[1])

def convert_to_datetime(date_str):
    try:
        return datetime.datetime.strptime(date_str, '%d-%m-%Y %H:%M:%S')
    except ValueError:
        return pd.NaT


df_kandidat_data['opdateringstidspunkt'] = df_kandidat_data['opdateringstidspunkt'].apply(convert_to_datetime)
df_kandidat_data['frigivelsestidspunkt'] = df_kandidat_data['frigivelsestidspunkt'].apply(convert_to_datetime)
df_valgforbund_data['opdateringstidspunkt'] = df_valgforbund_data['opdateringstidspunkt'].apply(convert_to_datetime)
df_valgforbund_data['frigivelsestidspunkt'] = df_valgforbund_data['frigivelsestidspunkt'].apply(convert_to_datetime)

df_kandidat_data.to_csv("data/struktureret/kv25_kandidat_data.csv", index=False)
df_valgforbund_data.to_csv("data/struktureret/kv25_valgforbund_data.csv", index=False)
