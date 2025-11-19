# import requests
# import json
# import os
# from dotenv import load_dotenv


# if DW_TOKEN is None:
#     raise ValueError("DW_TOKEN not found in .env file")

# dw_charts = json.load(open("dw_charts.json", "r", encoding="utf-8"))


# print("Publishing charts...")

# for chart in dw_charts.values():
#     for values in chart.values():
#         if isinstance(values, dict) and "id" in values:
#             print(f"Publishing chart with id {values['id']}")
#             chart_id = values['id']


#             url = f"https://api.datawrapper.de/v3/charts/{chart_id}/publish"

#             payload = { "callWebhooks": True }
#             headers = {
#                 "Authorization": f"Bearer {DW_TOKEN}",
#                 "accept": "*/*",
#                 "content-type": "application/json"
#             }

#             response = requests.post(url, json=payload, headers=headers)