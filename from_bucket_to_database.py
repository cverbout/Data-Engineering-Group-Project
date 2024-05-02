from datetime import datetime, timedelta
import json
from google.cloud import storage
import os
import database_uploader as db_up
from tqdm import tqdm

# Constants
BUCKET_NAME = 'cs510-spring24-project1-bucket'
FOLDER_NAME = 'data_via_direct_download'
BUCKET_CREDENTIALS_FILE = 'cs510-project1-6c1b06b5846a.json'
LOCAL_JSON_RECORD = 'uploaded_json_records.txt'

TESTING = True


def generate_date_named_files(start_date, end_date, prefix="TriMet__", suffix=".json"):
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

    for file_name in pbar:
        blob = bucket.blob(f"{FOLDER_NAME}/{file_name}")
        if blob.exists():
            json_content = blob.download_as_string()
            local_file_name = file_name
            with open(local_file_name, 'wb') as file:
                file.write(json_content)
            pbar.set_description(f"Downloaded {file_name}")
            yield local_file_name
        else:
            pbar.set_description(f"{file_name} does not exist")


def impute_gps_coordinates(events):
    for i, event in enumerate(events):
        if event['GPS_LONGITUDE'] is None or event['GPS_LATITUDE'] is None:
            # Find previous and next events with valid GPS data
            prev_event = None
            next_event = None
            for j in range(i-1, -1, -1):
                if events[j]['GPS_LONGITUDE'] is not None:
                    prev_event = events[j]
                    break
            for k in range(i+1, len(events)):
                if events[k]['GPS_LONGITUDE'] is not None:
                    next_event = events[k]
                    break

            if prev_event and next_event:
                # Linear interpolation
                time_ratio = (event['ACT_TIME'] - prev_event['ACT_TIME']) / (next_event['ACT_TIME'] - prev_event['ACT_TIME'])
                event['GPS_LONGITUDE'] = prev_event['GPS_LONGITUDE'] + time_ratio * (next_event['GPS_LONGITUDE'] - prev_event['GPS_LONGITUDE'])
                event['GPS_LATITUDE'] = prev_event['GPS_LATITUDE'] + time_ratio * (next_event['GPS_LATITUDE'] - prev_event['GPS_LATITUDE'])


def upload_json_to_database(json_files):
    uploaded_files = set()
    if os.path.exists(LOCAL_JSON_RECORD):
        with open(LOCAL_JSON_RECORD, 'r') as file:
            uploaded_files = set(file.read().splitlines())

    pbar = tqdm(json_files, desc="Uploading JSON data")
    
    for json_file in pbar:
        if json_file not in uploaded_files:
            with open(json_file, 'r') as file:
                data = json.load(file)
                for vehicle_id, events in data.items():
                    impute_gps_coordinates(events)
                    try:
                        db_up.insert_breadcrumb(events)
                    except Exception as e:
                        print(f"Error processing {json_file} for vehicle {vehicle_id}: {e}")
            with open(LOCAL_JSON_RECORD, 'a') as file:
                file.write(json_file + '\n')
            pbar.set_description(f"Uploaded {json_file}")
        else:
            pbar.set_description(f"Skipped {json_file} (already uploaded)")


if __name__ == "__main__":
    if TESTING:
        file_names = ["TriMet__2024-04-11.json"]
    else:
        start_date = datetime(2024, 4, 12)
        end_date = datetime.now()
        file_names = generate_date_named_files(start_date, end_date)

    json_files = download_json_files_from_bucket(file_names)
    upload_json_to_database(json_files)

