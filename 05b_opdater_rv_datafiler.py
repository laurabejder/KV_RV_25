import json
from pathlib import Path
import pandas as pd
from config import PARTIER_INFO, REGIONS_FPS

# ----------------------------
# Filstier og load af datafiler
# ----------------------------

BASE_PATH = Path("data/struktureret/rv/valgresultater")
REGION_DIR = BASE_PATH / "region"
AFSTEM_DIR = BASE_PATH / "afstemningssteder"
NATIONAL_DIR = BASE_PATH / "nationalt"

# Hent valgresultaterne for RV25 på kandidniveau
rv25_resultater_kandidater = (
    pd.read_csv("data/struktureret/rv/rv25_resultater_kandidater.csv")
    .drop_duplicates()
    .reset_index(drop=True)
)

# Hent valgresultaterne for RV25 på partiniveau
rv25_resultater_partier = (
    pd.read_csv("data/struktureret/rv/rv25_resultater_partier.csv")
    .drop_duplicates()
    .reset_index(drop=True)
)

# Hent valgresultaterne for RV21 på partiniveau
rv21_resultater_partier = pd.read_csv("data/21_resultater/rv21_parti_resultater.csv")
rv21_resultater_partier["region"] = rv21_resultater_partier["region"].str.replace("Region ", "")

# Load filen med partiinformation, så vi senere kan standardisere partinavne og -bogstaver
with open(PARTIER_INFO, "r", encoding="utf-8") as f:
    partier_info = json.load(f)

# Hent regionsforpersoner fra google sheets for opdatering af statusfiler
regionsforpersoner = pd.read_csv(REGIONS_FPS)

# Og definer navnene på regionerne i 2025
regioner = ["Østdanmark", "Midtjylland", "Nordjylland", "Syddanmark"]

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

# Funktionen udregner hvor mange procent af stemmerne, hvert parti har fået i regionen, og merger med resultaterne fra 2021
def get_overall_percentages(
    data: pd.DataFrame,
    reg: pd.DataFrame,
    regionnavn: str,
    regionnavn_lower: str,
    resultater_21_partier: pd.DataFrame,
    region_dir: Path,
) -> None:
    """Compute kommune-level percentages, merge 2021, and write CSV."""
    # Få det samlede antal gyldige stemmer i regionen til beregning af procenter (hvert valgsted tæller kun én gang)
    gyldige_total = (
        data.groupby("afstemningsområde_dagi_id")["total_gyldige_stemmer"]
        .max()
        .sum()
    )

    # Udregn hvor mange procent af stemmerne, hvert parti har fået i regionen
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
        pd.concat([reg, parti_sum], ignore_index=True)
        .dropna(subset=["partier", "procent_25"])
        .drop_duplicates(subset=["bogstav", "listebogstav", "partier"], keep="last")
    )

    # Hvis filen allerede har 2021 resultater, så spring merge over
    if regionnavn == "Østdanmark":
        print("Skipping 2021 merge for", regionnavn)
    elif "procent_21" in df.columns and df["procent_21"].notna().any():
        print("2021 results already present for", regionnavn)
    else:
        # For en sikkerheds skyld, fjern gamle 2021 kolonner, hvis de findes
        df = df.drop(columns=["procent_21", "stemmer_21"], errors="ignore")
        resultater_21 = (
            resultater_21_partier
            .query("region == @region")[["partier", "bogstav", "procent_21"]]
            .drop_duplicates(subset=["partier", "bogstav"])
        )

        # Merge 2021 resultaterne ind i dataframe
        df = df.merge(resultater_21, on=["partier", "bogstav"], how="left")
        print("merged 2021 results for", regionnavn)

    # Ændr kolonne rækkefølge og drop ubrugte kolonner
    first_cols = ["bogstav", "procent_25", "procent_21"]
    df = df[first_cols + [c for c in df.columns if c not in first_cols]]
    df = df.drop(
        columns=["stemmer_25", "stemmer_21","region_navn"],
        errors="ignore",
    )

    # Gem filen
    out_path = region_dir / f"{regionnavn_lower}.csv"
    df.to_csv(out_path, index=False)

# Funktionen udregner procenter per afstemningsområde og finder største parti
def get_afstemningsområde_percentages(
    data: pd.DataFrame,
    afst: pd.DataFrame,
    regionnavn: str,
    regionnavn_lower: str,
    afstem_dir: Path,
) -> None:
    """Compute per-polling-district percentages, largest party, and write CSV."""
    data = data.copy()
    data["parti_procent"] = data["stemmer"] / data["total_gyldige_stemmer"] * 100 # udregn partiernes procent per afstemningsområde

    # Pivot så hver række er et afstemningsområde, og hver kolonne et parti
    wide = (
        data.pivot_table(
            index=[
                "region",
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
        "region",
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
            "region",
            "dagi_id",
            "navn",
            "nummer",
            "afstemningssted_navn",
            "opstillingskreds_nummer",
            "opstillingskreds_dagi_id",
            "afstemningssted_adresse",
        ]
    ].merge(
        wide.drop(columns=["region"]),
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
        "region",
        "navn",
        "nummer",
        "afstemningssted_navn",
        "opstillingskreds_nummer",
        "opstillingskreds_dagi_id",
        "afstemningssted_adresse",
        "resultat_art",
    ]
    afst = afst[first_cols + [c for c in afst.columns if c not in first_cols]]

    # Gem filen
    out_path = afstem_dir / f"{regionnavn_lower}_afstemningsområde.csv"
    afst.to_csv(out_path, index=False)

# Funktionen kombinerer data fra kombit og vores håndholdte regionsforpersondata og opdaterer statusfilen
def get_status(
    regionnavn_lower: str,
    regionsforpersoner: pd.DataFrame,
    afst: pd.DataFrame,
    base_path: Path,
) -> None:
    """Update status CSV with counted share and borgmester."""
    status_path = base_path / "status" / f"{regionnavn_lower}_status.csv"
    summary_df = pd.read_csv(status_path)

    # Udregn andelen af afstemningssteder, der er optalt
    done_mask = afst["resultat_art"].isin(["Fintælling", "ForeløbigOptælling"])
    done_share = f"{done_mask.sum()} ud af {len(afst)}"
    summary_df["Optalte afstemningssteder"] = done_share

    # Find regionsforpersonen for regionen, hvis det er afgjort
    if region in regionsforpersoner["region"].values:
        regionsforperson = regionsforpersoner.loc[
            regionsforpersoner["region"] == region, "regionsforperson"
        ].iat[0]
        summary_df["Regionsforperson"] = regionsforperson
    else:
        summary_df["Regionsforperson"] = "Ikke afgjort"

    # Drop unødvendige kolonner og gem filen
    summary_df = summary_df[["Optalte afstemningssteder", "Regionsforperson"]]
    summary_df.to_csv(status_path, index=False)

# Funktionen udregner kandidaternes personlige stemmetal per region og nationalt
def get_stemmetal(stemmer, base_path: Path) -> None:
    stemmer = stemmer.groupby(['kandidat','parti','parti_bogstav','region']).stemmer.sum().reset_index()  # grupper og sum stemmer per kandidat per region  
    stemmer['parti'] = stemmer['parti_bogstav'].map({p['listebogstav']:p['navn'] for p in partier_info}).fillna(stemmer['parti_bogstav']) # standardiser partinavne
    stemmer['bogstav'] = stemmer['parti_bogstav'].map({p['listebogstav']:p['bogstav'] for p in partier_info}).fillna(stemmer['parti_bogstav']) # standardiser partibogstaver
    stemmer = stemmer[['kandidat','parti','region','stemmer']]
    stemmer.sort_values(by=['stemmer'], ascending=False, inplace=True) # sorter efter antal stemmer

    # Gem resultater per region
    for region in stemmer['region'].unique():
        region_stemmer = stemmer[stemmer['region'] == region]
        regionnavn = region_stemmer['region'].iat[0]
        regionnavn_lower = regionnavn.lower()
        region_stemmer = region_stemmer.drop(columns=['region'])
        out_path = base_path / f"kandidater/{regionnavn_lower}_stemmetal_kandidater.csv"
        region_stemmer.to_csv(out_path, index=False)


    # Og gem nationalt
    out_path = base_path / f"nationalt/stemmetal_kandidater.csv"
    stemmer.to_csv(out_path, index=False)

# ----------------------------
# Main loop
# ----------------------------

# Loop over resultaterne fra regionerne og opdater datafilerne
for region in rv25_resultater_partier["region"].unique():
    data = rv25_resultater_partier.query("region == @region").copy()
    regionnavn = data["region"].iat[0].replace("Region ", "")
    regionnavn_lower = regionnavn.lower()

    # Load filerne, der ligger til grund for visualiseringerne
    region_niveau = pd.read_csv(REGION_DIR / f"{regionnavn_lower}.csv")
    afstemningssted_niveau = pd.read_csv(AFSTEM_DIR / f"{regionnavn_lower}_afstemningsområde.csv")

    # Standardiser partinavne og -bogstaver til vores format
    data_std = _standardize_party_labels(data)

    # Kør funktionerne og opdater vores datafiler
    get_overall_percentages(
        data=data_std,
        reg=region_niveau,
        regionnavn=regionnavn,
        regionnavn_lower=regionnavn_lower,
        resultater_21_partier=rv21_resultater_partier,
        region_dir=REGION_DIR,
    )

    get_afstemningsområde_percentages(
        data=data_std,
        afst=afstemningssted_niveau,
        regionnavn=regionnavn,
        regionnavn_lower=regionnavn_lower,
        afstem_dir=AFSTEM_DIR,
    )

    get_status(
        regionnavn_lower=regionnavn_lower,
        regionsforpersoner=regionsforpersoner,
        afst=afstemningssted_niveau,
        base_path=BASE_PATH,
    )

    get_stemmetal(
        stemmer = rv25_resultater_kandidater,
        base_path=BASE_PATH
    )

    print(f"Updated data files for {region}")



# map listebogstav -> bogstav once
bogstav_map = {p["listebogstav"]: p["bogstav"] for p in partier_info}

totals = rv25_resultater_partier.groupby("region")["stemmer"].sum() # kommune totals (gyldige stemmer)

# aggregate, add bogstav, compute %, find biggest party per kommune, pivot wide
nat_resultater = (
    rv25_resultater_partier
      .assign(bogstav=lambda d: d["parti_bogstav"].map(bogstav_map).fillna(d["parti_bogstav"]))
      .groupby(["region", "parti", "bogstav"], as_index=False)["stemmer"].sum()
      .assign(
          kommune_gyldige_stemmer=lambda d: d["region"].map(totals),
          procent_25=lambda d: d["stemmer"] / d["kommune_gyldige_stemmer"] * 100,
      )
)

største = (
    nat_resultater
      .sort_values(["region", "procent_25"], ascending=[True, False])
      .drop_duplicates("region")[["region", "parti"]]
      .rename(columns={"parti": "største_parti"})
)

nat_resultater = (
    nat_resultater
      .merge(største, on="region", how="left")
      .pivot(index=["region", "største_parti"], columns="bogstav", values="procent_25")
      .reset_index()
)

# prepend "Region " to region names
nat_resultater["region"] = nat_resultater["region"].apply(lambda x: f"Region {x}")

# save file
out_path = NATIONAL_DIR / "nationalt_kommuner_parti_procenter.csv"
nat_resultater.to_csv(out_path, index=False, sep=";")

# now get the percent per party across the whole country
national_totals = (
    rv25_resultater_partier
      .groupby(["parti", "parti_bogstav"], as_index=False)["stemmer"].sum()
      .assign(
          total_stemmer=lambda d: d["stemmer"].sum(),
          procent_25=lambda d: d["stemmer"] / d["total_stemmer"] * 100,
      )
      .rename(columns={"stemmer": "stemmer_25"})
      [["parti", "parti_bogstav", "stemmer_25", "procent_25"]]
)

# get the 2021 results too
rv21_national = (
    rv21_resultater_partier
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
          rv21_national,
          left_on=["parti", "parti_bogstav"],
          right_on=["partier", "listebogstav"],
          how="left",
      )
      .drop(columns=["partier", "listebogstav","stemmer_25","stemmer_21"])
)

national_totals["bogstav"] = national_totals["parti_bogstav"].map(bogstav_map).fillna(national_totals["parti_bogstav"]) # get the bogstavs too
national_totals = national_totals[["bogstav", "parti", "procent_25", "procent_21"]] # reorder columns

# group parties with less than 0.5 percent into "Andre"

minor_parties_mask = national_totals["procent_25"] < 0.5
andre_row = pd.DataFrame({
    "bogstav": ["Andre"],
    "parti": ["Andre"],
    "procent_25": [national_totals.loc[minor_parties_mask, "procent_25"].sum()],
    "procent_21": [national_totals.loc[minor_parties_mask, "procent_21"].sum()],
})

national_totals = pd.concat([
    national_totals.loc[~minor_parties_mask],
    andre_row
], ignore_index=True)

# replace 0 with NaN
national_totals["procent_21"] = national_totals["procent_21"].replace(0, pd.NA)

# save file
out_path = NATIONAL_DIR / "nationalt_partier.csv"
national_totals.to_csv(out_path, index=False, sep=";")  