import pandas as pd
import glob
import json
import datetime

from helper_functions import kombiner_resultater

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
                        'kandidat_id': kandidat['Id'],
                        'kandidat_navn': kandidat['Navn'],
                        'kandidat_stemmeseddelnavn': kandidat['Stemmeseddelnavn'],
                        'kandidat_stilling': kandidat['Stilling'],
                        'kandidat_adresse': kandidat['BopaelPaaStemmeseddel'],
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
    
# loop over the column kandidatliste_id in the df_valgforbund_data and print each element in the list
for index, row in df_valgforbund_data.iterrows():
    df_valgforbund_data.at[index, 'valgforbund_partier'] = ""
    df_valgforbund_data.at[index, 'valgforbund_partibogstav'] = ""
    if isinstance(row['kandidatliste_id'], list):
        df_valgforbund_data.at[index, 'valgforbund_partier'] = []
        df_valgforbund_data.at[index, 'valgforbund_partibogstav'] = []
        for id in row['kandidatliste_id']:
            if id in df_kandidat_data['kandidatliste_id'].values:
                parti_bogstav = df_kandidat_data[df_kandidat_data['kandidatliste_id'] == id]['parti_bogstav'].values[0]
                parti_navn = df_kandidat_data[df_kandidat_data['kandidatliste_id'] == id]['parti_navn'].values[0]
                df_valgforbund_data.at[index, 'valgforbund_partier'].append(parti_navn)
                df_valgforbund_data.at[index, 'valgforbund_partibogstav'].append(parti_bogstav)
            else:
                print(f"ID: {id} not found in df_kandidat_data")
    else:
        print("No list available")

def apply_functions(df, file):
    df['opdateringstidspunkt'] = df['opdateringstidspunkt'].apply(convert_to_datetime)
    df['frigivelsestidspunkt'] = df['frigivelsestidspunkt'].apply(convert_to_datetime)
    df.to_csv("data/struktureret/kv/kandidat-info/"+file, index=False)

apply_functions(df_kandidat_data, "kv25_kandidat_data.csv")
apply_functions(df_valgforbund_data, "kv25_valgforbund_data.csv")