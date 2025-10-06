import paramiko
import os

# FTP login information
host = "data.valg.dk"
port = 22
username = "Valg"
password = "Valg"

# Forbindelse til FTP-server
transport = paramiko.Transport((host, port))
transport.connect(username=username, password=password)
sftp = paramiko.SFTPClient.from_transport(transport)

# Funktioner til download af filer og mapper
def download_files(sftp, remote_dir, local_dir, folder_name):
    files = sftp.listdir(remote_dir+"/"+folder_name)

    for file in files:   # download each file
        remote_file_path = remote_dir + "/" + folder_name + "/" + file
        local_file_path = os.path.join(local_dir, folder_name, file)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        sftp.get(remote_file_path, local_file_path)

def download_folders(folders):
    for folder in folders:
        download_files(sftp, remote_path, local_path, folder)

# Download KV25 data
remote_path = "/data/kommunalvalg-134-18-11-2025"
local_path = "data/raw/kv"
folders = ["verifikation/valgresultater", "kandidat-data", "verifikation/mandatfordeling", "verifikation/valgdeltagelse"]
download_folders(folders)

# Download RV25 data
remote_path = "/data/regionsrådsvalg-134-18-11-2025/verifikation"
local_path = "data/raw/rv"
folders = ["verifikation/valgresultater", "verifikation/mandatfordeling", "verifikation/valgdeltagelse"]
download_folders(folders)

# Luk forbindelsen
sftp.close()
transport.close()