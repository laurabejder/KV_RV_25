import paramiko
import os
import re

from config import FROM_PATH, TO_PATH, HOST, PORT, USERNAME, PASSWORD, FOLDERS, KV_REMOTE_PATH, RV_REMOTE_PATH

# Forbindelse til FTP-server
transport = paramiko.Transport((HOST, PORT))
transport.connect(username=USERNAME, password=PASSWORD)
sftp = paramiko.SFTPClient.from_transport(transport)

# Funktioner til download af filer og mapper
def download_files(sftp, remote_dir, local_dir, folder_name):
    files = sftp.listdir(remote_dir+"/"+folder_name)

    for file in files:   # download each file
        new_file = re.sub(r'-\d{12}(?=\.)', '', file)
        remote_file_path = remote_dir + "/" + folder_name + "/" + file
        local_file_path = os.path.join(local_dir, folder_name, new_file)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        sftp.get(remote_file_path, local_file_path)

def download_folders(folders):
    for folder in folders:
        download_files(sftp, remote_path, local_path, folder)

# Download RV25 data
remote_path = RV_REMOTE_PATH
local_path = FROM_PATH + "rv"
folders = FOLDERS
download_folders(folders)

# Download KV25 data
remote_path = KV_REMOTE_PATH
local_path = FROM_PATH + "kv"
folders = FOLDERS
download_folders(folders)

# Luk forbindelsen
sftp.close()
transport.close()