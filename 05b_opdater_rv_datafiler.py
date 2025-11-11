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

rv25_resultater_partier = (
    pd.read_csv("data/struktureret/rv/rv25_resultater_partier.csv")
    .drop_duplicates()
    .reset_index(drop=True)
)

rv21_resultater_partier = pd.read_csv("data/21_resultater/rv21_parti_resultater.csv")
rv21_resultater_partier["region"] = rv21_resultater_partier["region"].str.replace("Region ", "")


with open(PARTIER_INFO, "r", encoding="utf-8") as f:
    partier_info = json.load(f)

regionsforpersoner = pd.read_csv(REGIONS_FPS)
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

def get_overall_percentages(
    data: pd.DataFrame,
    reg: pd.DataFrame,
    regionnavn: str,
    regionnavn_lower: str,
    resultater_21_partier: pd.DataFrame,
    region_dir: Path,
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
        # For en sikkerheds skyld, fjern gamle 2021 kolonner hvis de findes
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


def get_afstemningsområde_percentages(
    data: pd.DataFrame,
    afst: pd.DataFrame,
    regionnavn: str,
    regionnavn_lower: str,
    afstem_dir: Path,
) -> None:
    """Compute per-polling-district percentages, largest party, and write CSV."""
    data = data.copy()
    data["parti_procent"] = data["stemmer"] / data["total_gyldige_stemmer"] * 100

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

    # Determine largest party per row
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

    # Keep and merge geo columns
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

    # Drop unnecessary columns & reorder
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

    # Write file
    out_path = afstem_dir / f"{regionnavn_lower}_afstemningsområde.csv"
    afst.to_csv(out_path, index=False)


def get_status(
    regionnavn_lower: str,
    regionsforpersoner: pd.DataFrame,
    afst: pd.DataFrame,
    base_path: Path,
) -> None:
    """Update status CSV with counted share and borgmester."""
    status_path = base_path / "status" / f"{regionnavn_lower}_status.csv"
    summary_df = pd.read_csv(status_path)

    # Share of polling places with counted results
    done_mask = afst["resultat_art"].isin(["Fintælling", "ForeløbigtResultat"])
    done_share = done_mask.sum() / len(afst)
    summary_df["Procent optalte afstemningssteder"] = done_share * 100

    # Borgmester
    if region in regionsforpersoner["region"].values:
        regionsforperson = regionsforpersoner.loc[
            regionsforpersoner["region"] == region, "regionsforperson"
        ].iat[0]
        summary_df["Regionsforperson"] = regionsforperson
    else:
        summary_df["Regionsforperson"] = "Ikke afgjort"

    # Keep only required columns and write back
    summary_df = summary_df[["Procent optalte afstemningssteder", "Regionsforperson"]]
    summary_df.to_csv(status_path, index=False)


# ----------------------------
# Main loop
# ----------------------------

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

    print(f"Updated data files for {region}")