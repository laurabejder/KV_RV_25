import json
from pathlib import Path
import pandas as pd
from config import PARTIER_INFO, BORGMESTRE

# ----------------------------
# Filstier og load af datafiler
# ----------------------------

BASE_PATH = Path("data/struktureret/kv/valgresultater")
KOMMUNE_DIR = BASE_PATH / "kommune"
AFSTEM_DIR = BASE_PATH / "afstemningssteder"
NATIONAL_DIR = BASE_PATH / "nationalt"

# Hent valgresultaterne for KV25 på kandidniveau
kv25_resultater_kandidater = (
    pd.read_csv("data/struktureret/kv/kv25_resultater_kandidater.csv")
    .drop_duplicates()
    .reset_index(drop=True)
)

# Hent valgresultaterne for kV25 på partiniveau
kv25_resultater_partier = (
    pd.read_csv("data/struktureret/kv/kv25_resultater_partier.csv")
    .drop_duplicates()
    .reset_index(drop=True)
)

# Hent valgresultaterne for KV21 på partiniveau
kv21_resultater_partier = pd.read_csv("data/21_resultater/kv21_parti_resultater.csv")

# Load filen med partiinformation, så vi senere kan standardisere partinavne og -bogstaver
with open(PARTIER_INFO, "r", encoding="utf-8") as f:
    partier_info = json.load(f)

# Hent borgmestre fra google sheets for opdatering af statusfiler
borgmestre = pd.read_csv(BORGMESTRE)

# ----------------------------
# Funktion til at standardisere partinavne
# ----------------------------

def _standardize_party_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Map to Altinget party names/letters based on config."""
    bogstav_to_navn = {p["listebogstav"]: p["navn"] for p in partier_info}
    bogstav_to_bogstav = {p["listebogstav"]: p["bogstav"] for p in partier_info}

    df["parti"] = df["parti_bogstav"].map(bogstav_to_navn).fillna(df["parti_bogstav"])
    df["bogstav"] = df["parti_bogstav"].map(bogstav_to_bogstav).fillna(df["parti_bogstav"])
    return df

# ----------------------------
# Centrale funktioner
# ----------------------------

# Funktionen udregner hvor mange procent af stemmerne, hvert parti har fået i kommunen, og merger med resultaterne fra 2021
def get_overall_percentages(
    data: pd.DataFrame,
    kom: pd.DataFrame,
    kommune_id: int | str,
    kommunenavn: str,
    kommunenavn_lower: str,
    resultater_21_partier: pd.DataFrame,
    kommune_dir: Path,
) -> None:
    """Compute kommune-level percentages, merge 2021, and write CSV."""
    # Få det samlede antal gyldige stemmer i kommunen til beregning af procenter (hvert valgsted tæller kun én gang)
    gyldige_total = (
        data.groupby("afstemningsområde_dagi_id")["total_gyldige_stemmer"]
        .max()
        .sum()
    )

    # Udregn hvor mange procent af stemmerne, hvert parti har fået i kommunen
    parti_sum = (
        data.groupby(["parti", "bogstav", "parti_bogstav"], as_index=False)["stemmer"]
        .sum()
        .assign(procent_25=lambda x: x["stemmer"] / gyldige_total * 100)
        .rename(
            columns={
                "stemmer": "stemmer_25",
                "parti": "partier",
                "parti_bogstav": "listebogstav",
            }
        )[["partier", "bogstav", "listebogstav", "procent_25"]]
    )

    # Join resultaterne med filen for kommunen 
    df = (
        pd.concat([kom, parti_sum], ignore_index=True)
        .dropna(subset=["partier", "procent_25"])
        .drop_duplicates(subset=["bogstav", "listebogstav", "partier"], keep="last")
    )

    # Hvis filen allerede har 2021 resultater, så spring merge over
    if "procent_21" in df.columns and df["procent_21"].notna().any():
        print("2021 results already present for", kommunenavn)
    else:
        # For en sikkerheds skyld, fjern gamle 2021 kolonner hvis de findes
        df = df.drop(columns=["procent_21", "stemmer_21"], errors="ignore")

        resultater_21 = (
            resultater_21_partier
            .query("kommune_id == @kommune_id")[["partier", "bogstav", "procent_21"]]
            .drop_duplicates(subset=["partier", "bogstav"])
        )

        # Merge 2021 resultaterne ind i dataframe
        df = df.merge(resultater_21, on=["partier", "bogstav"], how="left")
        print("merged 2021 results for", kommunenavn)

    # Ændr kolonne rækkefølge og drop ubrugte kolonner
    first_cols = ["bogstav", "procent_25", "procent_21"]
    df = df[first_cols + [c for c in df.columns if c not in first_cols]]
    df = df.drop(
        columns=["stemmer_25", "stemmer_21", "kommune_id", "kommune_dagi_id", "kommune_navn"],
        errors="ignore",
    )

    # Gem filen
    out_path = kommune_dir / f"{kommune_id}_{kommunenavn_lower}_kommune.csv"
    df.to_csv(out_path, index=False)

# Funktionen udregner procenter per afstemningsområde og finder største parti
def get_afstemningsområde_percentages(
    data: pd.DataFrame,
    afst: pd.DataFrame,
    kommune_id: int | str,
    kommunenavn_lower: str,
    afstem_dir: Path,
) -> None:
    """Compute per-polling-district percentages, largest party, and write CSV."""
    data = data.copy()
    data["parti_procent"] = data["stemmer"] / data["total_gyldige_stemmer"] * 100 # udregn partiernes procent per afstemningsområde

    # Pivot så hver række er et afstemningsområde, og hver kolonne et parti
    wide = (
        data.pivot_table(
            index=[
                "kommune",
                "kommune_kode",
                "afstemningsområde_dagi_id",
                "afstemningsområde",
                "resultat_art",
            ],
            columns="parti",
            values="parti_procent",
        )
        .reset_index()
    )

    # Find det største parti per afstemningsområde
    non_party_cols = [
        "kommune",
        "kommune_kode",
        "afstemningsområde_dagi_id",
        "afstemningsområde",
        "resultat_art",
    ]
    party_cols = wide.columns.difference(non_party_cols)

    def _biggest_party(row: pd.Series) -> str:
        return row[party_cols].idxmax()

    wide["største_parti"] = wide.apply(_biggest_party, axis=1)

    # Merge med afstemningssteds-info
    afst = afst[
        [
            "dagi_id",
            "navn",
            "nummer",
            "afstemningssted_navn",
            "kommune_id",
            "opstillingskreds_nummer",
            "opstillingskreds_dagi_id",
            "afstemningssted_adresse",
            "kommune_navn",
            "kommune_dagi_id",
        ]
    ].merge(
        wide,
        left_on="dagi_id",
        right_on="afstemningsområde_dagi_id",
        how="left",
    )

    # Drop unødvendige kolonner og ændr kolonne rækkefølge
    afst = afst.drop(
        columns=["afstemningsområde_dagi_id", "afstemningsområde", "kommune", "kommune_kode"],
        errors="ignore",
    )

    first_cols = [
        "dagi_id",
        "navn",
        "nummer",
        "afstemningssted_navn",
        "kommune_id",
        "opstillingskreds_nummer",
        "opstillingskreds_dagi_id",
        "afstemningssted_adresse",
        "kommune_navn",
        "kommune_dagi_id",
        "resultat_art",
    ]
    afst = afst[first_cols + [c for c in afst.columns if c not in first_cols]]

    # Gem filen
    out_path = afstem_dir / f"{kommune_id}_{kommunenavn_lower}_afstemningsområde.csv"
    afst.to_csv(out_path, index=False)

# Funktionen kombinerer data fra kombit og vores håndholdte borgmesterdata og opdaterer statusfilen
def get_status(
    kommune_id: int | str,
    kommunenavn_lower: str,
    borgmestre_df: pd.DataFrame,
    afst: pd.DataFrame,
    base_path: Path,
) -> None:
    """Update status CSV with counted share and borgmester."""
    status_path = base_path / "status" / f"{kommune_id}_{kommunenavn_lower}_status.csv"
    summary_df = pd.read_csv(status_path)

    # Udregn andelen af afstemningssteder, der er optalt
    done_mask = afst["resultat_art"].isin(["Fintælling", "ForeløbigOptælling"])
    done_share = f"{done_mask.sum()} ud af {len(afst)}"
    summary_df["Optalte valgsteder"] = done_share

    # Find borgmesteren for kommunen, hvis det er afgjort
    if kommune_id in borgmestre_df["kommune_kode"].values:
        borgmester = borgmestre_df.loc[
            borgmestre_df["kommune_kode"] == kommune_id, "borgmester"
        ].iat[0]
        summary_df["Borgmester"] = borgmester
    else:
        summary_df["Borgmester"] = "Ikke afgjort"

    # Drop unødvendige kolonner og gem filen
    summary_df = summary_df[["Optalte valgsteder", "Borgmester"]]
    summary_df.to_csv(status_path, index=False)

# Funktionen udregner kandidaternes personlige stemmetal per kommune og nationalt
def get_stemmetal(stemmer, base_path: Path) -> None:
    stemmer = stemmer.groupby(['kandidat','parti','parti_bogstav','kommune','kommune_kode']).stemmer.sum().reset_index()    
    stemmer['parti'] = stemmer['parti_bogstav'].map({p['listebogstav']:p['navn'] for p in partier_info}).fillna(stemmer['parti_bogstav'])
    stemmer['bogstav'] = stemmer['parti_bogstav'].map({p['listebogstav']:p['bogstav'] for p in partier_info}).fillna(stemmer['parti_bogstav'])
    stemmer = stemmer[['kandidat','parti','kommune','stemmer', 'kommune_kode']]

    stemmer.sort_values(by=['stemmer'], ascending=False, inplace=True)

    # Gem resultater per kommune
    for kommune in stemmer['kommune'].unique():
        kommune_stemmer = stemmer[stemmer['kommune'] == kommune]
        kommune_id = kommune_stemmer['kommune_kode'].iat[0]
        kommunenavn = kommune_stemmer['kommune'].iat[0]
        kommunenavn_lower = kommunenavn.lower()
        kommune_stemmer = kommune_stemmer.drop(columns=['kommune','kommune_kode'])
        out_path = base_path / f"kandidater/{kommune_id}_{kommunenavn_lower}_stemmetal_kandidater.csv"
        kommune_stemmer.to_csv(out_path, index=False)

    #drop kommune id
    stemmer = stemmer.drop(columns=['kommune_kode'])
    
    # Og gem nationalt
    out_path = base_path / f"nationalt/stemmetal_kandidater.csv"
    stemmer.to_csv(out_path, index=False)

# ----------------------------
# Main loop
# ----------------------------

# Loop over resultaterne fra kommunerne og opdater datafilerne
for kommune_id in kv25_resultater_partier["kommune_kode"].unique():
    data = kv25_resultater_partier.query("kommune_kode == @kommune_id").copy()
    kommunenavn = data["kommune"].iat[0].replace(" Kommune", "")
    kommunenavn_lower = kommunenavn.lower()

    # Load filerne, der ligger til grund for visualiseringerne
    kommune_niveau = pd.read_csv(KOMMUNE_DIR / f"{kommune_id}_{kommunenavn_lower}_kommune.csv")
    afstemningssted_niveau = pd.read_csv(AFSTEM_DIR / f"{kommune_id}_{kommunenavn_lower}_afstemningsområde.csv")

    # Standardiser partinavne og -bogstaver til vores format
    data_std = _standardize_party_labels(data)

    # Kør funktionerne og opdater vores datafiler
    get_overall_percentages(
        data=data_std,
        kom=kommune_niveau,
        kommune_id=kommune_id,
        kommunenavn=kommunenavn,
        kommunenavn_lower=kommunenavn_lower,
        resultater_21_partier=kv21_resultater_partier,
        kommune_dir=KOMMUNE_DIR,
    )

    get_afstemningsområde_percentages(
        data=data_std,
        afst=afstemningssted_niveau,
        kommune_id=kommune_id,
        kommunenavn_lower=kommunenavn_lower,
        afstem_dir=AFSTEM_DIR,
    )

    get_status(
        kommune_id=kommune_id,
        kommunenavn_lower=kommunenavn_lower,
        borgmestre_df=borgmestre,
        afst=afstemningssted_niveau,
        base_path=BASE_PATH,
    )

    get_stemmetal(
        stemmer = kv25_resultater_kandidater,
        base_path=BASE_PATH
    )

    print(f"Updated data files for {kommunenavn} ({kommune_id})")



# map listebogstav -> bogstav once
bogstav_map = {p["listebogstav"]: p["bogstav"] for p in partier_info}

totals = kv25_resultater_partier.groupby("kommune")["stemmer"].sum() # kommune totals (gyldige stemmer)


# aggregate, add bogstav, compute %, find biggest party per kommune, pivot wide
nat_resultater = (
    kv25_resultater_partier
      .assign(bogstav=lambda d: d["parti_bogstav"].map(bogstav_map).fillna(d["parti_bogstav"]))
      .groupby(["kommune_kode","kommune", "parti", "bogstav"], as_index=False)["stemmer"].sum()
      .assign(
          kommune_gyldige_stemmer=lambda d: d["kommune"].map(totals),
          procent_25=lambda d: d["stemmer"] / d["kommune_gyldige_stemmer"] * 100,
      )
)

største = (
    nat_resultater
      .sort_values(["kommune", "procent_25"], ascending=[True, False])
      .drop_duplicates("kommune")[["kommune", "parti"]]
      .rename(columns={"parti": "største_parti"})
)

nat_resultater = (
    nat_resultater
      .merge(største, on="kommune", how="left")
      .pivot(index=['kommune_kode',"kommune", "største_parti"], columns="bogstav", values="procent_25")
      .reset_index()
)

# make sure to strip kommune of " Kommune" suffix
nat_resultater["kommune"] = nat_resultater["kommune"].str.replace(" Kommune", "", regex=False)

# save file
out_path = NATIONAL_DIR / "nationalt_kommuner_parti_procenter.csv"
nat_resultater.to_csv(out_path, index=False)

# now get the percent per party across the whole country
national_totals = (
    kv25_resultater_partier
      .groupby(["parti", "parti_bogstav"], as_index=False)["stemmer"].sum()
      .assign(
          total_stemmer=lambda d: d["stemmer"].sum(),
          procent_25=lambda d: d["stemmer"] / d["total_stemmer"] * 100,
      )
      .rename(columns={"stemmer": "stemmer_25"})
      [["parti", "parti_bogstav", "stemmer_25", "procent_25"]]
)

# get the 2021 results too
kv21_national = (
    kv21_resultater_partier
      .groupby(["partier", "listebogstav"], as_index=False)["stemmer_21"].sum()
      .assign(
          total_stemmer=lambda d: d["stemmer_21"].sum(),
          procent_21=lambda d: d["stemmer_21"] / d["total_stemmer"] * 100,
      )
      [["partier", "listebogstav", "stemmer_21", "procent_21"]]
)

national_totals = (
    national_totals
      .merge(
          kv21_national,
          left_on=["parti", "parti_bogstav"],
          right_on=["partier", "listebogstav"],
          how="left",
      )
      .drop(columns=["partier", "listebogstav","stemmer_25","stemmer_21"])
)

national_totals["bogstav"] = national_totals["parti_bogstav"].map(bogstav_map).fillna(national_totals["parti_bogstav"]) # get the bogstavs too
national_totals = national_totals[["bogstav", "parti", "procent_25", "procent_21"]] # reorder columns

# save file
out_path = NATIONAL_DIR / "nationalt_partier.csv"
national_totals.to_csv(out_path, index=False)                                                                                    