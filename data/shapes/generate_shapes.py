import os
import json
import re
from collections import defaultdict

in_path = "/Users/bejder/Library/CloudStorage/OneDrive-AlrowMedia/Skrivebord/geojson/afstemningsomraader.geojson"
out_dir = "data/shapes"
os.makedirs(out_dir, exist_ok=True)

with open(in_path, "r", encoding="utf-8") as f:
    afstemningsområder = json.load(f)

groups = defaultdict(list)

def slugify(name: str) -> str:
    # simple slug: lowercase, spaces->_, remove non-word chars except _
    s = name.lower().strip().replace(" ", "_")
    s = re.sub(r"[^\w_]+", "", s)
    return s

for feature in afstemningsområder["features"]:
    props = feature.get("properties", {})
    kommune_id_raw = props.get("kommunekode")
    kommunenavn = props.get("kommunenavn", "")
    if not kommune_id_raw or not kommunenavn:
        continue

    # --- remove leading zeros ---
    kommune_id = str(int(kommune_id_raw))

    kommunenavn_slug = slugify(kommunenavn)
    groups[(kommune_id, kommunenavn_slug)].append(feature)

# write one file per kommune
for (kommune_id, kommunenavn_slug), feats in groups.items():
    fc = {"type": "FeatureCollection", "features": feats}
    out_path = os.path.join(out_dir, f"{kommune_id}_{kommunenavn_slug}_afstemningsomraader.geojson")
    print(f"Saving {kommune_id} - {kommunenavn_slug} ({len(feats)} features)")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)
