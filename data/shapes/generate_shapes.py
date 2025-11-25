import os
import json
import re
from collections import defaultdict
from topojson import Topology  # pip install topojson


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

# write one file per kommune (GeoJSON + TopoJSON)
for (kommune_id, kommunenavn_slug), feats in groups.items():
    fc = {"type": "FeatureCollection", "features": feats}
    base_name = f"{kommune_id}_{kommunenavn_slug}_afstemningsomraader"

    # save GeoJSON
    geojson_path = os.path.join(out_dir, f"{base_name}.geojson")
    print(f"Saving {kommune_id} - {kommunenavn_slug} ({len(feats)} features)")
    with open(geojson_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)

    # save TopoJSON
    topo = Topology(fc)
    topojson_path = os.path.join(out_dir, f"{base_name}.topojson")
    with open(topojson_path, "w", encoding="utf-8") as f:
        json.dump(topo.to_dict(), f, ensure_ascii=False, indent=2)






### Do the same for regioner

for feature in afstemningsområder["features"]:
    props = feature.get("properties", {})
    region_id_raw = props.get("regionskode")
    regionsnavn = props.get("regionsnavn", "")
    if not region_id_raw or not regionsnavn:
        continue

    if regionsnavn in ['Region Hovedstaden', 'Region Sjælland']:
        regionsnavn = "Region Østdanmark"
        region_id_raw = "1085"  # FIND DET RIGTIGE ID

    # --- remove leading zeros ---
    region_id = str(int(region_id_raw))

    regionsnavn_slug = slugify(regionsnavn)
    groups[(region_id, regionsnavn_slug)].append(feature)

# write one file per kommune (GeoJSON + TopoJSON)
for (region_id, regionsnavn_slug), feats in groups.items():
    fc = {"type": "FeatureCollection", "features": feats}
    base_name = f"{region_id}_{regionsnavn_slug}_afstemningsomraader"

    # save GeoJSON
    geojson_path = os.path.join(out_dir, f"{base_name}.geojson")
    print(f"Saving {region_id} - {regionsnavn_slug} ({len(feats)} features)")
    with open(geojson_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)

    # save TopoJSON
    topo = Topology(fc)
    topojson_path = os.path.join(out_dir, f"{base_name}.topojson")
    with open(topojson_path, "w", encoding="utf-8") as f:
        json.dump(topo.to_dict(), f, ensure_ascii=False, indent=2)