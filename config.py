FROM_PATH = "data/raw/"
TO_PATH = "data/struktureret/"

KOMMUNE_INFO = "data/kommuner.json"
PARTIER_INFO = "data/partier.json"
BORGMESTRE = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSyAqdHmvVJX2xvsb0PbIwNcrEOu40HKV6ljA2mnYgpqB-4IbaplSBhCZNFiC6IaGvhNIG_mP6KKrk3/pub?gid=0&single=true&output=csv"
REGIONS_FPS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSyAqdHmvVJX2xvsb0PbIwNcrEOu40HKV6ljA2mnYgpqB-4IbaplSBhCZNFiC6IaGvhNIG_mP6KKrk3/pub?gid=774356730&single=true&output=csv" 

# SFTP login information
HOST = "data.valg.dk"
PORT = 22
USERNAME = "Valg"
PASSWORD = "Valg"

# SFTP paths for KV25 and RV25 data
KV_REMOTE_PATH = "/data/kommunalvalg-134-18-11-2025" # SFTP path for KV25 data
RV_REMOTE_PATH = "/data/regionsrådsvalg-134-18-11-2025" # SFTP path for RV25 data
FOLDERS = ["valgresultater", "kandidat-data", "verifikation/mandatfordeling", "verifikation/valgdeltagelse"] # Folders to download for KV25 and RV25 from SFTP server