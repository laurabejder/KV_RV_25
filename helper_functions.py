import os, glob

def kombiner_resultater(from_path, to_path, valg, data_type):
    os.makedirs(to_path, exist_ok=True) # Opret output-mappen, hvis den ikke findes
    
    file_pattern = os.path.join(from_path, valg, 'verifikation', data_type, "*.json") # Find alle json-filer i den angivne mappe
    all_files = glob.glob(file_pattern)

    return all_files 