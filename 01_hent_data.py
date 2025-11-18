import paramiko
import os
import re
import time
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from config import FROM_PATH, TO_PATH, HOST, PORT, USERNAME, PASSWORD, FOLDERS, KV_REMOTE_PATH, RV_REMOTE_PATH

# Lock for thread-safe printing
print_lock = Lock()

def safe_print(*args, **kwargs):
    """Thread-safe print function."""
    with print_lock:
        print(*args, **kwargs)

def create_sftp_connection():
    """Create a new SFTP connection for a thread."""
    transport = paramiko.Transport((HOST, PORT))
    transport.connect(username=USERNAME, password=PASSWORD)
    return paramiko.SFTPClient.from_transport(transport), transport

# Funktioner til download af filer og mapper
def find_latest_timestamped_file(sftp, remote_dir, base_filename):
    """
    Find the file with the latest timestamp that matches the base filename pattern.
    
    Args:
        sftp: SFTP client connection
        remote_dir: Remote directory path
        base_filename: Base filename without timestamp (e.g., 'file.json' from 'file-202501011200.json')
    
    Returns:
        Full filename with latest timestamp, or None if not found
    """
    try:
        # Extract base name and extension
        base_name, ext = os.path.splitext(base_filename)
        # Pattern to match: base_name-YYYYMMDDHHMM.ext
        pattern = re.compile(rf'^{re.escape(base_name)}-(\d{{12}}){re.escape(ext)}$')
        
        # List all files in directory
        all_files = sftp.listdir(remote_dir)
        
        # Find all matching files with timestamps
        matching_files = []
        for file in all_files:
            match = pattern.match(file)
            if match:
                timestamp = match.group(1)
                matching_files.append((file, timestamp))
        
        if not matching_files:
            return None
        
        # Sort by timestamp (descending) and return the latest
        matching_files.sort(key=lambda x: x[1], reverse=True)
        return matching_files[0][0]
        
    except Exception as e:
        safe_print(f"Warning: Could not find latest timestamped file for {base_filename}: {e}")
        return None

def wait_for_file_stability(sftp, remote_file_path, max_wait_attempts=5, check_interval=1):
    """
    Wait for a file to become stable (size and timestamp don't change).
    
    Args:
        sftp: SFTP client connection
        remote_file_path: Path to remote file
        max_wait_attempts: Maximum number of stability checks
        check_interval: Seconds between checks
    
    Returns:
        True if file appears stable, False if still changing after max attempts
    """
    try:
        stat1 = sftp.stat(remote_file_path)
        time.sleep(check_interval)
        
        for check in range(max_wait_attempts):
            stat2 = sftp.stat(remote_file_path)
            
            # Check if both size and timestamp are stable
            size_stable = stat1.st_size == stat2.st_size
            timestamp_stable = stat1.st_mtime == stat2.st_mtime
            
            if size_stable and timestamp_stable:
                # File appears stable
                if check > 0:
                    safe_print(f"File {os.path.basename(remote_file_path)} stabilized after {check + 1} checks")
                return True
            
            # File is still changing
            if not size_stable:
                safe_print(f"File {os.path.basename(remote_file_path)} size changing: {stat1.st_size} -> {stat2.st_size}")
            if not timestamp_stable:
                safe_print(f"File {os.path.basename(remote_file_path)} timestamp changing: {stat1.st_mtime} -> {stat2.st_mtime}")
            
            stat1 = stat2
            time.sleep(check_interval)
        
        safe_print(f"File {os.path.basename(remote_file_path)} still changing after {max_wait_attempts} checks, proceeding anyway...")
        return False
        
    except Exception as e:
        safe_print(f"Warning: Could not check file stability for {os.path.basename(remote_file_path)}: {e}")
        return True  # Assume stable if we can't check

def download_file_with_retry(remote_file_path, local_file_path, remote_dir, max_retries=5, retry_delay=3, stability_check=True):
    """
    Download a file with retry logic that handles files changing during download
    and filename timestamps being updated. Creates its own SFTP connection for thread safety.
    If downloading a new version fails, keeps the old file if it exists.
    
    Args:
        remote_file_path: Path to remote file (may contain timestamp)
        local_file_path: Path to save local file (without timestamp)
        remote_dir: Remote directory path (for finding latest timestamped version)
        max_retries: Maximum number of retry attempts
        retry_delay: Base delay in seconds between retries (increases with attempts)
        stability_check: If True, wait for file stability before downloading
    """
    sftp = None
    transport = None
    
    # Check if old file exists before we start
    old_file_exists = os.path.exists(local_file_path)
    original_remote_path = remote_file_path
    is_trying_new_version = False
    
    try:
        sftp, transport = create_sftp_connection()
        current_remote_path = remote_file_path
        base_filename = os.path.basename(local_file_path)
        
        for attempt in range(max_retries):
            try:
                # Check if file exists, if not try to find latest timestamped version
                try:
                    sftp.stat(current_remote_path)
                except FileNotFoundError:
                    # File doesn't exist - try to find latest timestamped version
                    latest_file = find_latest_timestamped_file(sftp, remote_dir, base_filename)
                    if latest_file:
                        new_path = os.path.join(remote_dir, latest_file)
                        if new_path != original_remote_path:
                            is_trying_new_version = True
                        current_remote_path = new_path
                        safe_print(f"File {os.path.basename(remote_file_path)} not found, using latest version: {os.path.basename(current_remote_path)}")
                    else:
                        # No new version found, if old file exists, keep it
                        if old_file_exists:
                            safe_print(f"File {os.path.basename(remote_file_path)} not found and no newer version available, keeping existing file")
                            return True
                        raise FileNotFoundError(f"File {os.path.basename(remote_file_path)} not found and no timestamped version available")
                
                # Wait for file stability before attempting download
                if stability_check:
                    if not wait_for_file_stability(sftp, current_remote_path):
                        # If file is still changing, wait longer before retry
                        wait_time = retry_delay * (attempt + 1)
                        safe_print(f"Waiting {wait_time}s for file to stabilize before retry...")
                        time.sleep(wait_time)
                        continue
                
                # Get file stats before download to detect changes during download
                stat_before = sftp.stat(current_remote_path)
                
                # Backup old file if it exists and we're trying a new version
                backup_path = None
                if is_trying_new_version and old_file_exists and os.path.exists(local_file_path):
                    backup_path = local_file_path + ".backup"
                    try:
                        shutil.copy2(local_file_path, backup_path)
                    except Exception:
                        backup_path = None
                
                # Download the file
                sftp.get(current_remote_path, local_file_path)
                
                # Check if file changed during download by comparing stats
                try:
                    stat_after = sftp.stat(current_remote_path)
                    if stat_before.st_mtime != stat_after.st_mtime or stat_before.st_size != stat_after.st_size:
                        # File changed during download - restore backup if exists, otherwise delete partial
                        if backup_path and os.path.exists(backup_path):
                            try:
                                shutil.move(backup_path, local_file_path)
                                safe_print(f"File changed during download, restored previous version")
                            except Exception:
                                if os.path.exists(local_file_path):
                                    os.remove(local_file_path)
                        elif os.path.exists(local_file_path):
                            os.remove(local_file_path)
                        wait_time = retry_delay * (attempt + 1)
                        safe_print(f"File {os.path.basename(current_remote_path)} changed during download (timestamp: {stat_before.st_mtime} -> {stat_after.st_mtime}), retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                except FileNotFoundError:
                    # File was renamed/deleted during download - restore backup if exists
                    if backup_path and os.path.exists(backup_path):
                        try:
                            shutil.move(backup_path, local_file_path)
                            safe_print(f"File was renamed during download, restored previous version")
                        except Exception:
                            if os.path.exists(local_file_path):
                                os.remove(local_file_path)
                    elif os.path.exists(local_file_path):
                        os.remove(local_file_path)
                    
                    # Find latest version
                    latest_file = find_latest_timestamped_file(sftp, remote_dir, base_filename)
                    if latest_file:
                        current_remote_path = os.path.join(remote_dir, latest_file)
                        wait_time = retry_delay * (attempt + 1)
                        safe_print(f"File was renamed during download, found new version: {os.path.basename(current_remote_path)}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                except Exception:
                    # If we can't check after download, assume it's fine
                    pass
                
                # Successfully downloaded - remove backup if it exists
                if backup_path and os.path.exists(backup_path):
                    try:
                        os.remove(backup_path)
                    except Exception:
                        pass
                
                safe_print(f"✓ Downloaded {os.path.basename(current_remote_path)}")
                return True
                
            except FileNotFoundError as e:
                # File not found - try to find latest timestamped version
                if attempt < max_retries - 1:
                    latest_file = find_latest_timestamped_file(sftp, remote_dir, base_filename)
                    if latest_file:
                        new_path = os.path.join(remote_dir, latest_file)
                        if new_path != original_remote_path:
                            is_trying_new_version = True
                        current_remote_path = new_path
                        wait_time = retry_delay * (attempt + 1)
                        safe_print(f"File {os.path.basename(remote_file_path)} not found, found newer version: {os.path.basename(current_remote_path)}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        # No new version found - keep old file if it exists
                        if old_file_exists:
                            safe_print(f"File {os.path.basename(remote_file_path)} not found and no newer version available, keeping existing file")
                            return True
                        wait_time = retry_delay * (attempt + 1)
                        safe_print(f"Attempt {attempt + 1}/{max_retries} failed for {os.path.basename(remote_file_path)}: {e}")
                        safe_print(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                else:
                    # Final attempt failed - keep old file if it exists and we were trying a new version
                    if is_trying_new_version and old_file_exists and os.path.exists(local_file_path):
                        safe_print(f"✗ Failed to download new version {os.path.basename(remote_file_path)} after {max_retries} attempts, keeping existing file")
                        return True
                    safe_print(f"✗ Failed to download {os.path.basename(remote_file_path)} after {max_retries} attempts: {e}")
                    return False
                    
            except Exception as e:
                # On error, restore backup if we were trying a new version
                if is_trying_new_version:
                    backup_path = local_file_path + ".backup"
                    if backup_path and os.path.exists(backup_path):
                        try:
                            if os.path.exists(local_file_path):
                                os.remove(local_file_path)
                            shutil.move(backup_path, local_file_path)
                            safe_print(f"Error downloading new version, restored previous version")
                        except Exception:
                            pass
                
                # Clean up partial download on error (only if no backup exists)
                if os.path.exists(local_file_path):
                    backup_path = local_file_path + ".backup"
                    if not (is_trying_new_version and os.path.exists(backup_path)):
                        try:
                            os.remove(local_file_path)
                        except:
                            pass
                
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)  # Exponential backoff
                    safe_print(f"Attempt {attempt + 1}/{max_retries} failed for {os.path.basename(current_remote_path)}: {e}")
                    safe_print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    # Final attempt failed - keep old file if it exists and we were trying a new version
                    if is_trying_new_version and old_file_exists:
                        backup_path = local_file_path + ".backup"
                        if backup_path and os.path.exists(backup_path):
                            try:
                                if os.path.exists(local_file_path):
                                    os.remove(local_file_path)
                                shutil.move(backup_path, local_file_path)
                                safe_print(f"✗ Failed to download new version after {max_retries} attempts, restored previous version")
                                return True
                            except Exception:
                                pass
                        elif os.path.exists(local_file_path):
                            # Keep the existing file
                            safe_print(f"✗ Failed to download new version after {max_retries} attempts, keeping existing file")
                            return True
                    safe_print(f"✗ Failed to download {os.path.basename(current_remote_path)} after {max_retries} attempts: {e}")
                    return False
        return False
    finally:
        # Clean up any remaining backup files
        backup_path = local_file_path + ".backup"
        if os.path.exists(backup_path):
            try:
                os.remove(backup_path)
            except Exception:
                pass
        
        if sftp:
            sftp.close()
        if transport:
            transport.close()

def download_files(remote_dir, local_dir, folder_name, max_workers=4):
    """
    Download files in parallel (up to max_workers at a time).
    
    Args:
        remote_dir: Remote directory path
        local_dir: Local directory path
        folder_name: Folder name to download
        max_workers: Maximum number of parallel downloads (default: 4)
    """
    folder_path = remote_dir + "/" + folder_name
    
    # Create initial connection just to list files
    sftp, transport = create_sftp_connection()
    try:
        files = sftp.listdir(folder_path)
    finally:
        sftp.close()
        transport.close()
    
    # Prepare download tasks
    download_tasks = []
    for file in files:
        new_file = re.sub(r'-\d{12}(?=\.)', '', file)
        remote_file_path = folder_path + "/" + file
        local_file_path = os.path.join(local_dir, folder_name, new_file)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        download_tasks.append((remote_file_path, local_file_path))
    
    # Download files in parallel
    safe_print(f"Downloading {len(download_tasks)} files from {folder_name} (max {max_workers} parallel)...")
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_file = {
            executor.submit(download_file_with_retry, remote_path, local_path, folder_path): (remote_path, local_path)
            for remote_path, local_path in download_tasks
        }
        
        # Process completed downloads
        for future in as_completed(future_to_file):
            remote_path, local_path = future_to_file[future]
            try:
                if future.result():
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                safe_print(f"✗ Exception downloading {os.path.basename(remote_path)}: {e}")
                failed += 1
    
    safe_print(f"Completed {folder_name}: {successful} successful, {failed} failed")
    return successful, failed

def download_folders(folders, max_workers=4):
    """
    Download folders with parallel file downloads.
    
    Args:
        folders: List of folder names to download
        max_workers: Maximum number of parallel downloads per folder (default: 4)
    """
    for folder in folders:
        download_files(remote_path, local_path, folder, max_workers=max_workers)

# Download RV25 data
remote_path = RV_REMOTE_PATH
local_path = FROM_PATH + "rv"
folders = FOLDERS
safe_print("=" * 60)
safe_print("Downloading RV25 data from:", remote_path)
safe_print("=" * 60)
download_folders(folders, max_workers=8)
safe_print("✓ Completed RV25 data download.\n")

# Download KV25 data
remote_path = KV_REMOTE_PATH
local_path = FROM_PATH + "kv"
folders = FOLDERS
safe_print("=" * 60)
safe_print("Downloading KV25 data from:", remote_path)
safe_print("=" * 60)
download_folders(folders, max_workers=8)
safe_print("✓ Completed KV25 data download.")