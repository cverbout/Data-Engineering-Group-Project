from google.cloud import storage
import json
from tqdm import tqdm

# Constants
BUCKET_NAME = 'cs510-spring24-project1-bucket'
FOLDER_NAME = 'data_via_direct_download'
BUCKET_CREDENTIALS_FILE = 'cs510-project1-6c1b06b5846a.json'

def count_breadcrumbs_in_files(file_names):
    """Count breadcrumbs in specified JSON files from GCP bucket."""
    client = storage.Client.from_service_account_json(BUCKET_CREDENTIALS_FILE)
    bucket = client.bucket(BUCKET_NAME)
    breadcrumb_counts = {}

    # Initialize tqdm progress bar
    pbar = tqdm(file_names, desc="Counting breadcrumbs in files")
    
    for file_name in pbar:
        blob = bucket.blob(f"{FOLDER_NAME}/{file_name}")
        if blob.exists():
            json_content = blob.download_as_string()
            data = json.loads(json_content)
            breadcrumb_count = sum(len(events) for events in data.values())  # Summing events for each vehicle_id
            breadcrumb_counts[file_name] = breadcrumb_count
            pbar.set_description(f"Counted {breadcrumb_count} breadcrumbs in {file_name}")
        else:
            pbar.set_description(f"{file_name} does not exist")
            breadcrumb_counts[file_name] = None

    return breadcrumb_counts

def generate_date_named_files(start_date, end_date, prefix="TriMet__", suffix=".json"):
    from datetime import datetime, timedelta
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        formatted_date = current_date.strftime('%Y-%m-%d')
        file_name = f"{prefix}{formatted_date}{suffix}"
        date_list.append(file_name)
        current_date += timedelta(days=1)
    return date_list


if __name__ == "__main__":
    from datetime import datetime
    # Adjust the start date to include April 11, 2024
    start_date = datetime(2024, 4, 11)
    end_date = datetime.now()  # or any specific date you want to end at
    file_names = generate_date_named_files(start_date, end_date)

    # Count breadcrumbs
    breadcrumb_counts = count_breadcrumbs_in_files(file_names)

    # Print results
    for file_name, count in breadcrumb_counts.items():
        if count is not None:
            print(f"{file_name}: {count} breadcrumbs")
        else:
            print(f"{file_name}: File does not exist")

