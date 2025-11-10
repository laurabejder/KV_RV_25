import os
from dotenv import load_dotenv
import requests
import pandas as pd
import json

metadata = json.load(open("dw_design.json", "r", encoding="utf-8"))
print(metadata["column-metadata"])

load_dotenv()  # this will load variables from .env into os.environ

## LOAD IN THE DW TOKEN
DW_TOKEN = os.getenv("DW_TOKEN")
if DW_TOKEN is None:
    raise ValueError("DW_TOKEN not found in .env file")

## LOAD IN THE DATA AND TOPOJSONS
kommuner = pd.read_json("data/kommuner.json")
topojsons = [f for f in os.listdir("data/shapes/") if f.endswith(".topojson")]


## DEFINE THE FUNCTIONS TO CREATE THE CHARTS, TABLES AND MAPS

for col in ["table_id", "chart_id", "map_id"]:
    kommuner[col] = kommuner[col].astype("object")

def create_tables(row):
    kommune_name = row['kommune_navn']

    url = "https://api.datawrapper.de/v3/charts"
    headers = {
        "Authorization": "Bearer " + DW_TOKEN,
        "Content-Type": "application/json"
    }
    data = {
        "title": "Sådan stemte " + kommune_name + " Kommune",
        "type": "tables",
        "folderId": "355509",
        "metadata" : metadata["stemme-table-metadata"],
        'language': 'da-DK',
        "externalData": None,
    }

    response = requests.post(url, headers=headers, json=data)
    return response

def create_charts(row):
    kommune_name = row['kommune_navn']

    url = "https://api.datawrapper.de/v3/charts"
    headers = {
        "Authorization": "Bearer " + DW_TOKEN,
        "Content-Type": "application/json"
    }
    data = {
        "title": "Sådan stemte " + kommune_name + " Kommune",
        "type": "column-chart",
        "folderId": "355517",
        "metadata" : metadata["column-metadata"],  
        'language': 'da-DK',
        'externalData': None,
        }

    response = requests.post(url, headers=headers, json=data)
    return response

# def create_maps(row):
#     kommune_name = row['kommune_navn']

#     url = "https://api.datawrapper.de/v3/charts"
#     headers = {
#         "Authorization": "Bearer " + DW_TOKEN,
#         "Content-Type": "application/json"
#     }
#     data = {
#         "title": "Sådan stemte " + kommune_name + " Kommune",
#         "type": "d3-maps-choropleth",
#         "folderId": "355525",
#         "metadata" : {
#             "describe": {
#                 "intro": "Her kan du se, hvordan der blev stemt i " + kommune_name + " Kommune ved kommunalvalget den 18. november 2025.",
#                 "source-name": "Valgdata fra valg.dk",
#                 "source-url": "https://www.valg.dk",
#                 "byline": "Laura Bejder Jensen"}
#         },
#         'language': 'da-DK'
#     }

#     response = requests.post(url, headers=headers, json=data)
#     return response

# def add_topojson_to_map(row, map_id):
#     kommune_name = row['kommune_navn']
    
#     # 1. Load the TopoJSON file
#     file_name = kommune_name.lower().replace(" ", "_") + "_afstemningsområder.topojson"
#     file_path = f"data/shapes/{file_name}"
    
#     with open(file_path, "r", encoding="utf-8") as f:
#         topojson_data = f.read()

#     # 2. Upload the TopoJSON asset
#     asset_url = f"https://api.datawrapper.de/v3/charts/{map_id}/assets/{map_id}.map.json"
#     asset_headers = {
#         "Authorization": f"Bearer {DW_TOKEN}",
#         "Content-Type": "application/json"
#     }
    
#     upload_response = requests.put(asset_url, headers=asset_headers, data=topojson_data)
#     upload_response.raise_for_status()

#     # 3. PATCH metadata to tell Datawrapper to use the uploaded map
#     patch_url = f"https://api.datawrapper.de/v3/charts/{map_id}"
#     patch_headers = {
#         "Authorization": f"Bearer {DW_TOKEN}",
#         "Content-Type": "application/json"
#     }

#     # overwrite all visualize fields and remove basemap
#     patch_payload = {
#         "metadata": {
#             "visualize": {
#                 "map-type-set": "locator-map-custom",
#                 "map-file": f"{map_id}.map.json",
#                 "map-key": "dagi_id",
#             }
#         }
#     }

#     patch_response = requests.patch(
#         f"https://api.datawrapper.de/v3/charts/{map_id}",
#         headers={
#             "Authorization": f"Bearer {DW_TOKEN}",
#             "Content-Type": "application/json"
#         },
#         json=patch_payload
# )
#     patch_response.raise_for_status()
    # publish_url = f"https://api.datawrapper.de/v3/charts/{map_id}/publish"
    # publish_headers = {
    #     "Authorization": f"Bearer {DW_TOKEN}"
    # }

    # publish_response = requests.post(publish_url, headers=publish_headers)
    # publish_response.raise_for_status()


    # return {
    #     "upload": upload_response.status_code,
    #     "patch": patch_response.status_code,
    #     "publish": publish_response.status_code
    # }






for index, row in kommuner[:3].iterrows():
    print("Hi")
    # create_tables(row)
    # create_charts(row)
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
    

