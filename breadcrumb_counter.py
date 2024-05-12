import json
from tqdm import tqdm
from pathlib import Path

# Directory where JSON files are stored
FOLDER_PATH = 'downloaded_jsons'

def count_breadcrumbs_in_files(file_names):
    """Count breadcrumbs in specified JSON files from the local directory."""
    breadcrumb_counts = {}
    total_breadcrumbs = 0

    pbar = tqdm(file_names, desc="Counting breadcrumbs in files")
    
    for file_name in pbar:
        file_path = Path(FOLDER_PATH) / file_name
        if file_path.exists():
            with open(file_path, 'r') as file:
                data = json.load(file)
            breadcrumb_count = sum(len(events) for events in data.values())
            breadcrumb_counts[file_name] = breadcrumb_count
            total_breadcrumbs += breadcrumb_count
            pbar.set_description(f"Counted {breadcrumb_count} breadcrumbs in {file_name}")
        else:
            pbar.set_description(f"{file_name} does not exist")
            breadcrumb_counts[file_name] = None

    return breadcrumb_counts, total_breadcrumbs

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
    start_date = datetime(2024, 4, 11)
    end_date = datetime.now()
    file_names = generate_date_named_files(start_date, end_date)

    breadcrumb_counts, total_breadcrumbs = count_breadcrumbs_in_files(file_names)

    for file_name, count in breadcrumb_counts.items():
        if count is not None:
            print(f"{file_name}: {count} breadcrumbs")
        else:
            print(f"{file_name}: File does not exist")

    print(f"\nTotal breadcrumbs counted: {total_breadcrumbs}")

