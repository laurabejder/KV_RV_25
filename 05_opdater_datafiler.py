import pandas as pd
import json
# Load main results file
kv_parti_resultater = pd.read_csv("data/struktureret/kv/kv25_resultater_partier.csv")

# Define base paths
base_path = "data/struktureret/kv/valgresultater"
kommune_path = base_path + "/kommune"
afstem_path = base_path + "/afstemningssteder"

# Loop through each kommune
for kommune_id in kv_parti_resultater["kommune_kode"].unique():
    kommunedata = kv_parti_resultater.query("kommune_kode == @kommune_id")
    print(kommunedata.columns.tolist())
    kommunenavn = kommunedata["kommune"].iat[0].replace(" Kommune", "")
    kommunenavn_lower = kommunenavn.lower()

    print(f"Processing kommune {kommune_id} ({kommunenavn})")

    # Load kommune and afstemningsområde data
    kom_geo = pd.read_csv(kommune_path + f"/{kommune_id}_{kommunenavn_lower}_kommune.csv")
    afst_geo = pd.read_csv(afstem_path + f"/{kommune_id}_{kommunenavn_lower}_afstemningsområde.csv")

    # --- Kommune-level results ---
    gyldige_total = kommunedata.drop_duplicates("afstemningsområde_dagi_id")["total_gyldige_stemmer"].sum()
    parti_sum = (
        kommunedata.groupby("parti", as_index=False)["stemmer"].sum()
        .assign(
            procent_25=lambda x: x["stemmer"] / gyldige_total * 100,
            kommune_id=kommune_id,
            kommune_navn=kommunenavn,
            kommune_dagi_id=kom_geo["kommune_dagi_id"].iat[0],
        )
        .rename(columns={"stemmer": "stemmer_25", "parti": "partier"})
        [ ["kommune_id","kommune_dagi_id","kommune_navn","partier","procent_25","stemmer_25"] ]
    )

    df_stacked = (
        pd.concat([kom_geo, parti_sum], ignore_index=True)
        .dropna(subset=["partier", "procent_25"])
    )

    print(df_stacked)

    # --- Afstemningsområde-level results ---
    kommunedata["parti_procent"] = kommunedata["stemmer"] / kommunedata["total_gyldige_stemmer"] * 100
    kommunedata_wide = (
        kommunedata.pivot_table(
            index=["kommune","kommune_kode","afstemningsområde_dagi_id","afstemningsområde","resultat_art"],
            columns="parti", values="parti_procent"
        ).reset_index()
    )

    afst_geo = afst_geo.merge(
        kommunedata_wide, left_on="dagi_id", right_on="afstemningsområde_dagi_id", how="left"
    )
    print(afst_geo)
# kv_parti_resultater = pd.read_csv("data/struktureret/kv/kv25_resultater_partier.csv")

# for kommune_id in kv_parti_resultater['kommune_kode'].unique():
#     kommunedata = kv_parti_resultater[kv_parti_resultater['kommune_kode'] == kommune_id]
#     kommunenavn = kommunedata['kommune'].values[0].replace(" Kommune", "")

#     print(kommune_id)

#     # find the corresponding files in valgresultater/
#     kom_geo = pd.read_csv(f"data/struktureret/kv/valgresultater/kommune/{kommune_id}_{kommunenavn.lower()}_kommune.csv")
#     afst_geo = pd.read_csv(f"data/struktureret/kv/valgresultater/afstemningssteder/{kommune_id}_{kommunenavn.lower()}_afstemningsområde.csv")

#     # First get the results per afstemningsområde per kommune
#     parti_sum = kommunedata.groupby("parti", as_index=False)["stemmer"].sum()
#     gyldige_total = kommunedata.drop_duplicates("afstemningsområde_dagi_id")["total_gyldige_stemmer"].sum()
#     parti_sum["procent_25"] = parti_sum["stemmer"] / gyldige_total * 100
#     parti_sum['kommune_id'] = kommune_id
#     parti_sum['kommune_navn'] = kommunenavn
#     parti_sum['kommune_dagi_id'] = kom_geo['kommune_dagi_id'].values[0]
#     parti_sum = parti_sum.rename(columns={'stemmer': 'stemmer_25', 'parti': 'partier'})
#     #change the order of the columns
#     parti_sum = parti_sum[['kommune_id', 'kommune_dagi_id', 'kommune_navn', 'partier', 'procent_25', 'stemmer_25']]
#     df_stacked = pd.concat([kom_geo, parti_sum], ignore_index=True)
#     # drop rows where partier is NaN
#     df_stacked = df_stacked.dropna(subset=['partier','procent_25'])

#     print(df_stacked)


#     # First get the results per afstemningsområde per kommune
#     kommunedata['parti_procent'] = kommunedata['stemmer'] / kommunedata['total_gyldige_stemmer'] * 100
#     kommunedata_wide = kommunedata.pivot_table(index=['kommune', 'kommune_kode','afstemningsområde_dagi_id','afstemningsområde','resultat_art'], columns='parti', values='parti_procent').reset_index() # turn from long to wide format
#     afst_geo = afst_geo.merge(kommunedata_wide, left_on='dagi_id', right_on='afstemningsområde_dagi_id', how='left') # if there are results, join them on the afst_geo data
#     #afst_geo.to_csv(f"data/struktureret/kv/valgresultater/afstemningssteder/{kommune_dagi}_{kommunenavn.lower()}_afstemningsområde.csv", index=False)

