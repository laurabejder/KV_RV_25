import pandas as pd
import json
from config import PARTIER_INFO, REGIONS_FPS

#### SETUP ####

# Load in the files with the structured data
rv_parti_resultater = pd.read_csv("data/struktureret/rv/rv25_resultater_partier.csv")
rv_parti_resultater.drop_duplicates(inplace=True)

# Load the files in the config file
partier_info = json.load(open(PARTIER_INFO, "r", encoding="utf-8"))
regions_fps = pd.read_csv(REGIONS_FPS)

# Define base paths
base_path = "data/struktureret/rv/valgresultater"
region_path = base_path + "/region"
afstem_path = base_path + "/afstemningssteder"

regioner = ["Østdanmark", "Midtjylland", "Nordjylland", "Syddanmark"]

###

# Loop through each region
for region in rv_parti_resultater["region"].unique():
    regionsdata = rv_parti_resultater.query("region == @region").copy()
    regionsnavn = regionsdata["region"].iat[0].replace("Region ", "")
    regionsnavn_lower = regionsnavn.lower()

    reg_geo = pd.read_csv(region_path + f"/{regionsnavn_lower}.csv")
    afst_geo = pd.read_csv(afstem_path + f"/{regionsnavn_lower}_afstemningsområde.csv")

    afst_geo = afst_geo[[
        "dagi_id","navn","nummer","afstemningssted_navn","kommune_id",
        "opstillingskreds_nummer","opstillingskreds_dagi_id","afstemningssted_adresse",
        'region'
    ]]

    bogstav_to_navn = {p["listebogstav"]: p["navn"] for p in partier_info}
    bogstav_to_bogstav = {p["listebogstav"]: p["bogstav"] for p in partier_info}
    regionsdata["parti"] = regionsdata["parti_bogstav"].map(bogstav_to_navn).fillna(regionsdata["parti_bogstav"])
    regionsdata["bogstav"] = regionsdata["parti_bogstav"].map(bogstav_to_bogstav).fillna(regionsdata["parti_bogstav"])

    gyldige_total = (regionsdata.groupby("afstemningsområde_dagi_id")["total_gyldige_stemmer"].max().sum())

    parti_sum = (
        regionsdata.groupby(["parti",'bogstav','parti_bogstav'], as_index=False)["stemmer"].sum()
        .assign(
            procent_25=lambda x: x["stemmer"] / gyldige_total * 100,
            region_navn=regionsnavn,
        )
        .rename(columns={"stemmer": "stemmer_25", "parti": "partier","parti_bogstav":"listebogstav"})
        [ ["region_navn","partier", "bogstav","listebogstav", "procent_25","stemmer_25"] ]
    )

    df_stacked = (
        pd.concat([reg_geo, parti_sum], ignore_index=True)
        .dropna(subset=["partier", "procent_25"])
    )
    df_stacked = df_stacked.drop_duplicates(subset= ["region","listebogstav","partier"], keep='last')

    # save the file back with the new results
    df_stacked.to_csv(region_path + f"/{regionsnavn_lower}.csv", index=False)


    ########## --- Afstemningsområde-level results ---  #############

    regionsdata["parti_procent"] = regionsdata["stemmer"] / regionsdata["total_gyldige_stemmer"] * 100
    regionsdata_wide = (
        regionsdata.pivot_table(
            index=["region","afstemningsområde_dagi_id","afstemningsområde","resultat_art"],
            columns="parti", values="parti_procent"
        ).reset_index()
    )

    def get_biggest_party(row):
        party_cols = regionsdata_wide.columns.difference(
            ["region","afstemningsområde_dagi_id","afstemningsområde","resultat_art"]
        )
        biggest_party = row[party_cols].idxmax()
        return biggest_party
    regionsdata_wide["største_parti"] = regionsdata_wide.apply(get_biggest_party, axis=1)
    
    afst_geo = afst_geo.merge(
        regionsdata_wide, left_on="dagi_id", right_on="afstemningsområde_dagi_id", how="left"
    )

        # drop unnecessary columns and rename
    afst_geo = (
        afst_geo
        .drop(columns=["region_x"])
        .rename(columns={"region_y": "region"})
    )

    # reorder columns, putting these first
    first_cols = [
        "dagi_id", "navn", "nummer", "afstemningssted_navn", "region",
        "opstillingskreds_nummer", "opstillingskreds_dagi_id", "afstemningssted_adresse",
        "resultat_art"
    ]
    afst_geo = afst_geo[first_cols + [c for c in afst_geo.columns if c not in first_cols]]
    
    afst_geo.to_csv(afstem_path + f"/{regionsnavn_lower}_afstemningsområde.csv", index=False)
