from datetime import datetime, timedelta
import json
from google.cloud import storage
import os
from tqdm import tqdm
import psutil

# Constants
BUCKET_NAME = 'cs510-spring24-project1-bucket'
FOLDER_NAME = 'stopevents_data'
BUCKET_CREDENTIALS_FILE = 'cs510-project1-6c1b06b5846a.json'
LOCAL_DOWNLOAD_FOLDER = 'downloaded_stopevents_jsons'

TESTING = False


def log_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    print(f"Memory used: {mem_info.rss / (1024 * 1024)} MB")


def generate_date_named_files(start_date, end_date, prefix="TriMet_StopEvents__", suffix=".json"):
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        formatted_date = current_date.strftime('%Y-%m-%d')
        file_name = f"{prefix}{formatted_date}{suffix}"
        date_list.append(file_name)
        current_date += timedelta(days=1)
    return date_list


def download_json_files_from_bucket(file_names):
    client = storage.Client.from_service_account_json(BUCKET_CREDENTIALS_FILE)
    bucket = client.bucket(BUCKET_NAME)
    pbar = tqdm(file_names, desc="Downloading JSON files")

    if not os.path.exists(LOCAL_DOWNLOAD_FOLDER):
        os.makedirs(LOCAL_DOWNLOAD_FOLDER)

    for file_name in pbar:
        local_file_path = os.path.join(LOCAL_DOWNLOAD_FOLDER, file_name)

        # Check if the file already exists locally
        if not os.path.exists(local_file_path):
            try:
                blob = bucket.blob(f"{FOLDER_NAME}/{file_name}")
                if blob.exists():
                    blob.download_to_filename(local_file_path)
                    pbar.set_description(f"Downloaded {file_name}")
                else:
                    pbar.set_description(f"{file_name} does not exist in bucket")
            except Exception as e:
                print(f"Failed to download {file_name}: {e}")
                continue
        else:
            pbar.set_description(f"{file_name} already exists locally")


if __name__ == "__main__":
    try:
        log_memory_usage()  # Log memory usage at the start
        if TESTING:
            file_names = ["TriMet_StopEvents__2024-05-13.json"]
        else:
            start_date = datetime(2024, 5, 13)
            end_date = datetime.now()
            file_names = generate_date_named_files(start_date, end_date)

        download_json_files_from_bucket(file_names)
        log_memory_usage()  # Log memory usage at the end
    except Exception as e:
        print(f"Script terminated with exception: {e}")

