import pandas as pd

############# Load data #############

kv_path = "data/struktureret/kv/valgresultater/nationalt/nationalt_kommuner_parti_procenter.csv"
rv_path = "data/struktureret/rv/valgresultater/nationalt/nationalt_kommuner_parti_procenter.csv"

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

non_party_columns = ["region","kommune_kode", "kommune", "største_parti", "pop_up"]


############# Generic function to add popups to a dataframe #############

def add_popups(
               df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()


    geo = "kommune" if "kommune" in df.columns else "region"

    # Determine party columns for this dataframe
    party_columns = [c for c in df.columns if c not in non_party_columns]

    # Force party columns to numeric (strings -> float; bad values -> NaN)
    df[party_columns] = df[party_columns].apply(pd.to_numeric, errors="coerce")

    def make_popup(row):
        largest = row["største_parti"]
        valg = row[geo]

        print(f"Creating popup for {valg} with largest party {largest}")
        header_color = largest_party_colors.get(largest, default_color)

        # Header line
        header = (
            f"<b style='color:{header_color}; font-size:1.5em;margin-bottom: 10px'>{largest}</b><br> "
            f"blev størst i {valg}<br>"
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
                f"<span style='display:inline-block; width:20px; font-size:1em;"
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


# ############# Color maps #############

# largest_party_colors = {
#     "Socialdemokratiet": "#F00B2F",
#     "Venstre": "#0781DD",
#     "Dansk Folkeparti": "#F6BA00",
#     "Enhedslisten": "#FF7400",
#     "Liberal Alliance": "#48CEF3",
#     "SF": "#F257A9",
#     "Radikale": "#662690",
#     "Konservative": "#06691E",
#     "Alternativet": "#3CE63D",
#     "Kristendemokraterne": "#8B8474",
#     "Nye Borgerlige": "#004E62",
#     "Moderaterne": "#911995",
#     "Frie Grønne": "#eecbc6",
#     "Danmarksdemokraterne": "#0075c9",
# }

# party_colors = {
#     "S":  "#F00B2F",
#     "V":  "#0781DD",
#     "DF": "#F6BA00",
#     "EL": "#FF7400",
#     "LA": "#48CEF3",
#     "SF": "#F257A9",
#     "R":  "#662690",
#     "K":  "#06691E",
#     "ALT": "#3CE63D",
#     "KD": "#8B8474",
#     "NB": "#004E62",
#     "M":  "#911995",
#     "FG": "#eecbc6",
#     "DD": "#0075c9",
#     # add more if needed: "SP", "T", etc.
# }

# default_color = "#494949"

# non_party_columns = ["kommune_kode", "kommune", "største_parti"]
# party_columns = [c for c in national_kv.columns if c not in non_party_columns]

# # 1) Force party columns to numeric (strings -> float; bad values -> NaN)
# national_kv[party_columns] = national_kv[party_columns].apply(
#     pd.to_numeric, errors="coerce"
# )

# ############# Function to build one popup (per row) #############

# def make_popup(row):
#     largest = row["største_parti"]
#     kommune = row["kommune"]
#     header_color = largest_party_colors.get(largest, default_color)

#     # Header line
#     header = (
#         f"<b style='color:{header_color}; font-size:1.5em'>{largest}</b><br> "
#         f"blev størst i {kommune} Kommune<br><br>"
#     )

#     rows = []
#     for party in party_columns:
#         pct = row[party]
#         if pd.isna(pct):
#             continue

#         pct = float(pct)
#         color = party_colors.get(party, default_color)
#         bar_width = int(1.4 * pct)

#         # fixed-width label cell (so S: and SP: line up)
#         label_span = (
#             f"<span style='display:inline-block; width:20px; font-size:1em;vertical-align:middle; margin-left: 4px'>{party}</span>"
#         )

#         # bar cell
#         bar_span = (
#             f"<span style='display:inline-block; "
#             f"width:0.3em; height:1.2em; vertical-align:middle;"
#             f"background:{color};'></span>"
#         )

#         # percentage cell, fixed width & right-aligned
#         pct_span = (
#             f"<span style='display:inline-block; width:50px; "
#             f"text-align:left; font-size:1em; vertical-align:middle'>{pct:.1f}%</span>"
#         )

#         line = bar_span + label_span + pct_span
#         rows.append((pct, line))

#     # sort by percentage descending
#     rows.sort(key=lambda x: x[0], reverse=True)

#     body = "<br>".join(line for _, line in rows)

#     return header + body

# ############# Generate pop-ups and store in column #############

# national_kv["pop_up"] = national_kv.apply(make_popup, axis=1)

# # Save for Datawrapper
# national_kv.to_csv("data/struktureret/kv/valgresultater/nationalt/nationalt_kommuner_parti_procenter.csv", index=False, sep=";")