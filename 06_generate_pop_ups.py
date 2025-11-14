# import pandas as pd



# ############# Variables for pop-ups ###############

# # største_parti_tekst = '<big><big><big><b style="color:{{ '+ COLOR + '}}">%REGION_VALUE%</b></big></big> blev størst i {{ ' + kommune + '}} Kommune</big>'

# # partier_bars = '<hr><table>' + parti_bars + '</table>'

# # bar = '<tr><td>' + parti_bogstav + '</td><td><div style="width:{{ 1.4*' + parti_procent + '}}px; height:14px; background-color:' + parti_color + '; color:white; padding:2px 0px 0px 0px; vertical-align:bottom; font-weight:bold; display:inline-block;"></div><div style="width:{{ 140-(1.4*' + parti_procent + ') }}px; height:14px; background-color:#ffffff; color: ' + parti_color + '; vertical-align:center; padding:4px 4px 0px 4px; font-weight:bold; display:inline-block;">{{ ROUND(' + parti_procent + ',1) }}% </div></td></tr><tr>'


# ############# Load data #############

# national_kv = pd.read_csv("data/struktureret/kv/valgresultater/nationalt/nationalt_kommuner_parti_procenter.csv")
# print(national_kv.head())

# for index, row in national_kv.iterrows():
#     if row['største_parti'] == "Socialdemokratiet":
#         farve = "#F00B2F"
#     elif row['største_parti'] == "Venstre":
#         farve = "#0781DD"
#     elif row['største_parti'] == "Dansk Folkeparti":
#         farve = "#F6BA00"
#     elif row['største_parti'] == "Enhedslisten":
#         farve = "#FF7400"
#     elif row['største_parti'] == "Liberal Alliance":
#         farve = "#48CEF3"
#     elif row['største_parti'] == "SF":
#         farve = "#F257A9"
#     elif row['største_parti'] == "Radikale":
#         farve = "#662690"
#     elif row['største_parti'] == "Konservative":
#         farve = "#06691E"
#     elif row['største_parti'] == "Alternativet":
#         farve = "#3CE63D"
#     elif row['største_parti'] == "Kristendemokraterne":
#         farve = "#8B8474"
#     elif row['største_parti'] == "Nye Borgerlige":
#         farve = "#004E62"
#     elif row['største_parti'] == "Moderaterne":
#         farve = "#911995"
#     elif row['største_parti'] == "Frie Grønne":
#         farve = "#eecbc6"
#     elif row['største_parti'] == "Danmarksdemokraterne":
#         farve = " #0075c9"
#     else: 
#         farve = "#494949"
#     største_parti1 = '<big><big><big><b style="color:' + farve + '">{{ strste_parti }}</b></big></big> blev størst i {{ kommune }} Kommune</big>'
    
#     non_party_columns = ['kommune_kode', 'kommune', 'største_parti']

#     for party in national_kv.columns:
#         if party not in non_party_columns:
#             if party == "S":
#                 party_color = "#F00B2F"
#             elif party == "V":
#                 party_color = "#0781DD"
#             elif party == "DF":
#                 party_color = "#F6BA00"
#             elif party == "EL":
#                 party_color = "#FF7400"
#             elif party == "LA":
#                 party_color = "#48CEF3"
#             elif party == "SF":
#                 party_color = "#F257A9"
#             elif party == "R":
#                 party_color = "#662690"
#             elif party == "K":
#                 party_color = "#06691E"
#             elif party == "ALT":
#                 party_color = "#3CE63D"
#             elif party == "KD":
#                 party_color = "#8B8474"
#             elif party == "NB":
#                 party_color = "#004E62"
#             elif party == "M":
#                 party_color = "#911995"
#             elif party == "FG":
#                 party_color = "#eecbc6"
#             elif party == "DD":
#                 party_color = " #0075c9"
#             else:
#                 party_color = "#494949"

#             bar = '<tr><td>' + party + '</td><td><div style="width:{{ 1.4*' + party.lower() + '}}px; height:14px; background-color:' + party_color + '; color:white; padding:2px 0px 0px 0px; vertical-align:bottom; font-weight:bold; display:inline-block;"></div><div style="width:{{ 140-(1.4*' + party.lower() + ') }}px; height:14px; background-color:#ffffff; color: ' + party_color + '; vertical-align:center; padding:4px 4px 0px 4px; font-weight:bold; display:inline-block;">{{ ROUND(' + party.lower() + ',1) }}% </div></td></tr><tr>'
#             if index == 0:
#                 parti_bars = bar
#             else:
#                 parti_bars += bar



#     pop_up_content = største_parti1 + '<hr><table>' + parti_bars + '</table>'

# print(pop_up_content)


# ############# Generate pop-ups #############

import pandas as pd

############# Load data #############

national_kv = pd.read_csv(
    "data/struktureret/kv/valgresultater/nationalt/nationalt_kommuner_parti_procenter.csv"
)

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

non_party_columns = ["kommune_kode", "kommune", "største_parti"]
party_columns = [c for c in national_kv.columns if c not in non_party_columns]

# 1) Force party columns to numeric (strings -> float; bad values -> NaN)
national_kv[party_columns] = national_kv[party_columns].apply(
    pd.to_numeric, errors="coerce"
)

############# Function to build one popup (per row) #############

def make_popup(row):
    largest = row["største_parti"]
    kommune = row["kommune"]
    header_color = largest_party_colors.get(largest, default_color)

    # Header line
    header = (
        f"<b style='color:{header_color}; font-size:1.5em'>{largest}</b><br> "
        f"blev størst i {kommune} Kommune<br><br>"
    )

    rows = []
    for party in party_columns:
        pct = row[party]
        if pd.isna(pct):
            continue

        pct = float(pct)
        color = party_colors.get(party, default_color)
        bar_width = int(1.4 * pct)

        # fixed-width label cell (so S: and SP: line up)
        label_span = (
            f"<span style='display:inline-block; width:28px; font-size:1em'>{party}</span>"
        )

        # bar cell
        bar_span = (
            f"<span style='display:inline-block; "
            f"width:0.3em; height:1em; vertical-align:middle"
            f"background:{color};'></span>"
        )

        # percentage cell, fixed width & right-aligned
        pct_span = (
            f"<span style='color: {color}; display:inline-block; width:50px; "
            f"text-align:left; font-size:1em;>{pct:.1f}%</span>"
        )

        line = bar_span + label_span + pct_span
        rows.append((pct, line))

    # sort by percentage descending
    rows.sort(key=lambda x: x[0], reverse=True)

    body = "<br>".join(line for _, line in rows)

    return header + body

############# Generate pop-ups and store in column #############

national_kv["pop_up"] = national_kv.apply(make_popup, axis=1)

# Save for Datawrapper
national_kv.to_csv("data/struktureret/kv/valgresultater/nationalt/nationalt_kommuner_parti_procenter1.csv", index=False, sep=";")