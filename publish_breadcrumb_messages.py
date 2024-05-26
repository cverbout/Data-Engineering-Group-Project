import requests
import pandas as pd
from datetime import datetime
from google.cloud import pubsub_v1
import json
import time
from tqdm import tqdm
from assertions import validate_data 
from transformations import calculate_speed, decode_timestamp


def get_vehicle_ids():
    doc_key = "10VKMye65LhbEgMLld5Ol3lOocWUwCaEgnPVgFQf9em0"
    url = f"https://docs.google.com/spreadsheets/d/{doc_key}/export?format=csv"
    response = requests.get(url)
    csv_data = response.content
    with open("vehicle_ids_sheet.csv", "wb") as file:
        file.write(csv_data)
    vehicle_ids = pd.read_csv("vehicle_ids_sheet.csv")['Doodle'].tolist()
    return vehicle_ids

def publish_breadcrumbs():
    project_id = "cs510-project1"
    topic_id = "cs510-spring24-topic"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    vehicle_ids = get_vehicle_ids()

    for vehicle_id in tqdm(vehicle_ids, desc="Processing vehicle IDs"):
        url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
        response = requests.get(url)
        if response.status_code == 200:
            breadcrumbs = response.json()
            previous_breadcrumb = None
            for breadcrumb in breadcrumbs:
                if previous_breadcrumb is not None:
                    speed = calculate_speed(previous_breadcrumb, breadcrumb)
                    breadcrumb['SPEED'] = speed
                else:
                    breadcrumb['SPEED'] = None

                timestamp = decode_timestamp(breadcrumb) 
                breadcrumb['TIMESTAMP'] = timestamp
                validate_data(breadcrumb)
                data_str = json.dumps(breadcrumb)
                data_bytes = data_str.encode('utf-8')
                publisher.publish(topic_path, data_bytes)
                previous_breadcrumb = breadcrumb
                #print statement for testing
                # print(breadcrumb) 
        time.sleep(1)  # Respectful delay to avoid rate limiting

if __name__ == "__main__":
    publish_breadcrumbs()
