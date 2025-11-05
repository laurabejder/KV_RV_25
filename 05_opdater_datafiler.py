import pandas as pd
import json

# Load in the files with the structured data
kv_parti_resultater = pd.read_csv("data/struktureret/kv/kv25_resultater_partier.csv")
# And the file with party information
partier_info = json.load(open("data/partier.json", "r", encoding="utf-8"))

borgmestre = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vSyAqdHmvVJX2xvsb0PbIwNcrEOu40HKV6ljA2mnYgpqB-4IbaplSBhCZNFiC6IaGvhNIG_mP6KKrk3/pub?gid=0&single=true&output=csv")

# Define base paths
base_path = "data/struktureret/kv/valgresultater"
kommune_path = base_path + "/kommune"
afstem_path = base_path + "/afstemningssteder"

# Loop through each kommune
for kommune_id in kv_parti_resultater["kommune_kode"].unique():
    kommunedata = kv_parti_resultater.query("kommune_kode == @kommune_id").copy()
    kommunenavn = kommunedata["kommune"].iat[0].replace(" Kommune", "")
    kommunenavn_lower = kommunenavn.lower()

    # Load the empty files that we will map the results onto
    kom_geo = pd.read_csv(kommune_path + f"/{kommune_id}_{kommunenavn_lower}_kommune.csv")
    afst_geo = pd.read_csv(afstem_path + f"/{kommune_id}_{kommunenavn_lower}_afstemningsområde.csv")

    # We use different letters and names for the parties in our coverage, so we standardize these here 
    bogstav_to_navn = {p["listebogstav"]: p["navn"] for p in partier_info}
    bogstav_to_bogstav = {p["listebogstav"]: p["bogstav"] for p in partier_info}
    kommunedata["parti"] = kommunedata["parti_bogstav"].map(bogstav_to_navn).fillna(kommunedata["parti_bogstav"])
    kommunedata["bogstav"] = kommunedata["parti_bogstav"].map(bogstav_to_bogstav).fillna(kommunedata["parti_bogstav"])

    # --- Kommune-level results ---
    gyldige_total = kommunedata.drop_duplicates("afstemningsområde_dagi_id")["total_gyldige_stemmer"].sum() # get the total number of gyldige stemmer in the kommune
    parti_sum = (
        kommunedata.groupby(["parti",'bogstav','parti_bogstav'], as_index=False)["stemmer"].sum()
        .assign(
            procent_25=lambda x: x["stemmer"] / gyldige_total * 100,
            kommune_id=kommune_id,
            kommune_navn=kommunenavn,
            kommune_dagi_id=kom_geo["kommune_dagi_id"].iat[0],
        )
        .rename(columns={"stemmer": "stemmer_25", "parti": "partier","parti_bogstav":"listebogstav"})
        [ ["kommune_id","kommune_dagi_id","kommune_navn","partier", "bogstav","listebogstav", "procent_25","stemmer_25"] ]
    )

    df_stacked = (
        pd.concat([kom_geo, parti_sum], ignore_index=True)
        .dropna(subset=["partier", "procent_25"])
    )

    # save the file back with the new results
    df_stacked.to_csv(kommune_path + f"/{kommune_id}_{kommunenavn_lower}_kommune.csv", index=False)

    # --- Afstemningsområde-level results ---
    kommunedata["parti_procent"] = kommunedata["stemmer"] / kommunedata["total_gyldige_stemmer"] * 100
    kommunedata_wide = (
        kommunedata.pivot_table(
            index=["kommune","kommune_kode","afstemningsområde_dagi_id","afstemningsområde","resultat_art"],
            columns="parti", values="parti_procent"
        ).reset_index()
    )

    # find the biggest party per afstemningsområde
    def get_biggest_party(row):
        party_cols = kommunedata_wide.columns.difference(
            ["kommune","kommune_kode","afstemningsområde_dagi_id","afstemningsområde","resultat_art"]
        )
        biggest_party = row[party_cols].idxmax()
        return biggest_party
    kommunedata_wide["største_parti"] = kommunedata_wide.apply(get_biggest_party, axis=1)

    afst_geo = afst_geo.merge(
        kommunedata_wide, left_on="dagi_id", right_on="afstemningsområde_dagi_id", how="left"
    )

    afst_geo = afst_geo.drop(columns=["største_parti_x","afstemningsområde_dagi_id","afstemningsområde","afstemningsområde",'kommune','kommune_kode'])
    #rename største_parti_y to største_parti
    afst_geo = afst_geo.rename(columns={"største_parti_y": "største_parti"})

    # set the order so that these columns come first: dagi_id,navn,nummer,afstemningssted_navn,kommune_id,opstillingskreds_nummer,opstillingskreds_dagi_id,afstemningssted_adresse,kommune_navn,kommune_dagi_id,største_parti
    cols = afst_geo.columns.tolist()
    first_cols = ["dagi_id","navn","nummer","afstemningssted_navn","kommune_id","opstillingskreds_nummer","opstillingskreds_dagi_id","afstemningssted_adresse","kommune_navn","kommune_dagi_id","største_parti","resultat_art"]
    new_order = first_cols + [col for col in cols if col not in first_cols]
    afst_geo = afst_geo[new_order]

    # save the file back with the new results
    afst_geo.to_csv(afstem_path + f"/{kommune_id}_{kommunenavn_lower}_afstemningsområde.csv", index=False)
    # --- Status for the kommune ---
    #create a new dataframe with four columns: kommune_id, kommune_navn, andel_af_afstemningssteder_talt, borgmester
    # load in the status file for the kommune
    summary_df = pd.read_csv(base_path + f"/status/{kommune_id}_{kommunenavn_lower}_status.csv")

    # and update the values in the columns "andel_af_afstemningssteder_talt" and "borgmester"

    # find the share of afstemningssteder where resultat_art is "Fintælling" or "ForeløbigtResultat"
    done_share = afst_geo[afst_geo["resultat_art"].isin(["Fintælling", "ForeløbigtResultat"])].shape[0] / afst_geo.shape[0]
    summary_df["andel_af_afstemningssteder_talt"] = done_share * 100

    # find the borgmester party from the borgmestre dataframe
    if kommune_id in borgmestre["kommune_kode"].values:
        borgmester = borgmestre.loc[borgmestre["kommune_kode"] == kommune_id, "borgmester"].iat[0]
        summary_df["borgmester"] = borgmester
    else:
        summary_df["borgmester"] = "Ikke afgjort"
    # save the updated summary file
    summary_df.to_csv(base_path + f"/status/{kommune_id}_{kommunenavn_lower}_status.csv", index=False) 

    