import pandas as pd
import os
import glob
############# Load data #############

kv_path = "data/struktureret/kv/valgresultater/nationalt/nationalt_kommuner_parti_procenter.csv"
kv_valgsted_path = "data/struktureret/kv/valgresultater/afstemningssteder/"
rv_path = "data/struktureret/rv/valgresultater/nationalt/nationalt_kommuner_parti_procenter.csv"
rv_valgsted_path = "data/struktureret/rv/valgresultater/afstemningssteder/"

national_kv = pd.read_csv(kv_path, sep=";")
national_rv = pd.read_csv(rv_path, sep=";")

############# Color maps #############

largest_party_colors = {
    "Socialdemokratiet": "#F00B2F",
    "Venstre": "#0781DD",
    "Dansk Folkeparti": "#F6BA00",
    "Enhedslisten": "#FF7400",
    "Liberal Alliance": "#48CEF3",
    "SF": "#F257A9",
    "Radikale": "#662690",
    "Konservative": "#06691E",
    "Alternativet": "#3CE63D",
    "Kristendemokraterne": "#8B8474",
    "Nye Borgerlige": "#004E62",
    "Moderaterne": "#911995",
    "Frie Grønne": "#eecbc6",
    "Danmarksdemokraterne": "#0075c9",
}

party_colors = {
    "S":  "#F00B2F",
    "V":  "#0781DD",
    "DF": "#F6BA00",
    "EL": "#FF7400",
    "LA": "#48CEF3",
    "SF": "#F257A9",
    "R":  "#662690",
    "K":  "#06691E",
    "ALT": "#3CE63D",
    "KD": "#8B8474",
    "NB": "#004E62",
    "M":  "#911995",
    "FG": "#eecbc6",
    "DD": "#0075c9",
    # add more if needed: "SP", "T", etc.
}

default_color = "#494949"

non_party_columns = ["region","kommune_kode", "kommune", "største_parti", "pop_up",
                     "dagi_id","navn","nummer","afstemningssted_navn","kommune_id",
                     "opstillingskreds_nummer","opstillingskreds_dagi_id",
                     "afstemningssted_adresse","kommune_navn","kommune_dagi_id","resultat_art"]


############# Generic function to add popups to a dataframe #############

def add_popups(
               df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()


    if "kommune" in df.columns:
        geo = "kommune"
    elif "afstemningssted_navn" in df.columns:
        geo = "afstemningssted_navn"
    else:
        geo = "region"

    # Determine party columns for this dataframe
    party_columns = [c for c in df.columns if c not in non_party_columns]

    # Force party columns to numeric (strings -> float; bad values -> NaN)
    df[party_columns] = df[party_columns].apply(pd.to_numeric, errors="coerce")

    def make_popup(row):
        largest = row["største_parti"]
        valg = row[geo]

        # if valg is afstemningssted_navn add på valgested before the name
        if geo == "afstemningssted_navn":
            valg = f"på valgstedet {valg}"
        else: 
            valg = f"i {valg}"

        header_color = largest_party_colors.get(largest, default_color)

        # Header line
        header = (
            f"<b style='color:{header_color}; font-size:1.5em;margin-bottom: 10px'>{largest}</b><br> "
            f"blev størst {valg}<br>"
        )

        rows = []
        for party in party_columns:
            pct = row[party]
            if pd.isna(pct):
                continue

            pct = float(pct)
            color = party_colors.get(party, default_color)
            bar_width = int(1.4 * pct)  # kept from your original code (unused but harmless)

            # fixed-width label cell (so S: and SP: line up)
            label_span = (
                f"<span style='display:inline-block; width:30px; font-size:1em;"
                f"vertical-align:middle; margin-left: 4px'>{party}</span>"
            )

            # bar cell
            bar_span = (
                f"<span style='display:inline-block; "
                f"width:0.3em; height:1.2em; vertical-align:middle;"
                f"background:{color};'></span>"
            )

            # percentage cell, fixed width & right-aligned
            pct_span = (
                f"<span style='display:inline-block; width:50px; "
                f"text-align:left; font-size:1em; vertical-align:middle'>{pct:.1f}%</span>"
            )

            line = bar_span + label_span + pct_span
            rows.append((pct, line))

        # sort by percentage descending
        rows.sort(key=lambda x: x[0], reverse=True)

        body = "<br>".join(line for _, line in rows)
        return header + body

    df["pop_up"] = df.apply(make_popup, axis=1)
    return df


############# Run on both dataframes #############
national_kv = add_popups(national_kv)
national_rv = add_popups(national_rv)

# Save for Datawrapper (overwrite original files – adjust if you want new filenames)
national_kv.to_csv(kv_path, index=False, sep=";")
national_rv.to_csv(rv_path, index=False, sep=";")


for path in [kv_valgsted_path, rv_valgsted_path]:
    print(f"Processing files in {path}")
    try:
        file_pattern = os.path.join(path, "*.csv")
        all_files = glob.glob(file_pattern)

        for file in all_files:
            print(f"Processing file {file}")
            df = pd.read_csv(file, sep=";")
            df = add_popups(df)
            df.to_csv(file, index=False, sep=";")
    except Exception as e:
        print(f"Error processing files in {path}: {e}")