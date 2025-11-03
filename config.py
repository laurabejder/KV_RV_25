FROM_PATH = "data/raw/"
TO_PATH = "data/struktureret/"

KOMMUNE_INFO = "data/kommuner.json"

# SFTP login information
HOST = "data.valg.dk"
PORT = 22
USERNAME = "Valg"
PASSWORD = "Valg"

# SFTP paths for KV25 and RV25 data
KV_REMOTE_PATH = "/data/kommunalvalg-134-18-11-2025" # SFTP path for KV25 data
RV_REMOTE_PATH = "/data/regionsrådsvalg-134-18-11-2025" # SFTP path for RV25 data
FOLDERS = ["verifikation/valgresultater", "kandidat-data", "verifikation/mandatfordeling", "verifikation/valgdeltagelse"] # Folders to download for KV25 and RV25 from SFTP server