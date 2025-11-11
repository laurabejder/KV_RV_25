import os
from dotenv import load_dotenv
import requests
import pandas as pd
import json
import copy

urls = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRQUadygm9cUwREReC2MSBMsRPSBR42KKwKI_od_qSY65cVLk-ud8xcJhfQ9q_XYfbSJJ64OmyeQEg_/pub?output=csv")

metadata = json.load(open("dw_design.json", "r", encoding="utf-8"))
load_dotenv()  # this will load variables from .env into os.environ

## LOAD IN THE DW TOKEN
DW_TOKEN = os.getenv("DW_TOKEN")
if DW_TOKEN is None:
    raise ValueError("DW_TOKEN not found in .env file")

## DEFINE THE FUNCTIONS TO CREATE THE CHARTS, TABLES AND MAPS

def create_tables(geo, data_url):
    print(f"Creating chart for {geo} using data: {data_url}")

    url = "https://api.datawrapper.de/v3/charts"
    headers = {
        "Authorization": f"Bearer {DW_TOKEN}"
        # (requests sets Content-Type automatically when using json=)
    }

    # Deep copy so we never mutate the original file object
    m = copy.deepcopy(metadata["stemme-table-metadata"])

    # --- Normalize to the pure metadata block ---
    # Case 1: File stores full chart JSON: {"chart": {"metadata": {...}}}
    if isinstance(m, dict) and "chart" in m and isinstance(m["chart"], dict) and "metadata" in m["chart"]:
        m = m["chart"]["metadata"]

    # Case 2: File already stores the metadata object: {"visualize": ..., "describe": ..., maybe "data": ...}
    # Ensure "data" exists
    if "data" not in m or not isinstance(m["data"], dict):
        m["data"] = {}

    # Inject external-data settings inside metadata.data (optional but harmless),
    # while also using top-level externalData which the API expects.
    m["data"]["upload-method"] = "external-data"
    m["data"]["use-datawrapper-cdn"] = True
    # You can also mirror the URL inside metadata if your theme expects it:
    m["data"]["external-data"] = data_url

    payload = {
        "title": f"Her er {geo} største stemmeslugere",
        "type": "tables",
        "folderId": "363303",
        "language": "da-DK",
        "metadata": m,
        # Datawrapper API accepts externalData as a top-level field:
        "externalData": data_url
    }

    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if not r.ok:
        raise RuntimeError(f"Datawrapper error {r.status_code}: {r.text}")
    return r.json()


def create_columns(geo, data_url):
    print(f"Creating column chart for {geo} using data: {data_url}")

    url = "https://api.datawrapper.de/v3/charts"
    headers = {
        "Authorization": f"Bearer {DW_TOKEN}"
    }
    m = copy.deepcopy(metadata["column-metadata"])

    # --- Normalize metadata structure ---
    if isinstance(m, dict) and "chart" in m and "metadata" in m["chart"]:
        m = m["chart"]["metadata"]

    if "data" not in m or not isinstance(m["data"], dict):
        m["data"] = {}

    # Add Datawrapper external-data settings
    m["data"]["upload-method"] = "external-data"
    m["data"]["use-datawrapper-cdn"] = True
    m["data"]["external-data"] = data_url

    # Build request payload
    payload = {
        "title": f"Sådan stemte {geo} ved valget",
        "type": "column-chart",
        "folderId": "363302",
        "language": "da-DK",
        "metadata": m,
        "externalData": data_url
    }

    response = requests.post(url, headers=headers, json=payload, timeout=60)

    if not response.ok:
        raise RuntimeError(f"Datawrapper error {response.status_code}: {response.text}")

    print("Chart created:", response.json().get("id"))
    return response.json()

def create_maps(geo):

    url = "https://api.datawrapper.de/v3/charts"
    headers = {
        "Authorization": "Bearer " + DW_TOKEN,
        "Content-Type": "application/json"
    }
    data = {
        "title": "",
        "type": "d3-maps-choropleth",
        "folderId": "363304",
        "metadata" : metadata["map-metadata"],  
        'language': 'da-DK',
    }

    response = requests.post(url, headers=headers, json=data)
    return response

# loop over the dataframe and create the charts
for index, row in urls.iterrows():
    if pd.isna(row['geo']) or pd.isna(row['status_tabel']) or pd.isna(row['parti_søjle']):
        print(f"Skipping row {index} due to missing data")
        continue
    create_tables(row['geo'], row['stemme_tabel'])
    create_columns(row['geo'], row['parti_søjle'])
    # map_response = create_maps(row)
    # map_id = map_response.json()['id']
    # row['map_id'] = map_id
    # kommuner.at[index, 'map_id'] = map_id  # Save to DataFrame
    # add_topojson_to_map(row, map_id)


    # url = f"https://api.datawrapper.de/v3/charts/{map_id}"
    # headers = {
    #     "Authorization": f"Bearer {DW_TOKEN}"
    # }

    # response = requests.get(url, headers=headers)
    # response.raise_for_status()

    # # Pretty print the full metadata
    # chart = response.json()
    # print(json.dumps(chart["metadata"], indent=2))
    

