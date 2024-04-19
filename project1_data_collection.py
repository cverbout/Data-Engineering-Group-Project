import requests
import pandas as pd
from datetime import datetime
import json
import time
from tqdm import tqdm
from google.cloud import storage



def get_vehicle_ids():
    doc_key = "10VKMye65LhbEgMLld5Ol3lOocWUwCaEgnPVgFQf9em0"
    url = f"https://docs.google.com/spreadsheets/d/{doc_key}/export?format=csv"
    response = requests.get(url)
    csv_data = response.content
    with open("vehicle_ids_sheet.csv", "wb") as file:
        file.write(csv_data)
    vehicle_ids = pd.read_csv("vehicle_ids_sheet.csv")['Doodle'].tolist()
    return vehicle_ids

def save_to_gcs(data, filename):
    bucket_name = 'cs510-spring24-project1-bucket'
    folder_name = "data_via_direct_download"
    bucket_key = 'cs510-project1-6c1b06b5846a.json'
    client = storage.Client.from_service_account_json(bucket_key)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{folder_name}/{filename}")
    blob.upload_from_string(json.dumps(data), content_type='application/json')
    print(f"Data saved successfully to GCS with filename {filename}")

def save_trimet_doodle_data():
    vehicle_ids = get_vehicle_ids()
    all_breadcrumbs = {}

    for i, vehicle_id in enumerate(tqdm(vehicle_ids, desc="Processing vehicle IDs")):
        index_str = f"[{i+1:03}/{len(vehicle_ids):03}]"
        success = False
        attempts = 0
        while not success and attempts < 5:
            url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
            response = requests.get(url)
            attempts += 1
            if response.status_code == 200:
                all_breadcrumbs[vehicle_id] = response.json()
                # print(f"{index_str} Successfully fetched data for vehicle ID {vehicle_id}")
                success = True
            time.sleep(1)
        if not success:
            all_breadcrumbs[vehicle_id] = []
            # print(f"{index_str} Failed to get data after 5 attempts for vehicle ID {vehicle_id}")

    # Sort by index (vehicle ids)
    all_breadcrumbs = dict(sorted(all_breadcrumbs.items()))

    today_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"TriMet__{today_date}.json"
    save_to_gcs(all_breadcrumbs, filename)

if __name__ == "__main__":
    save_trimet_doodle_data()

