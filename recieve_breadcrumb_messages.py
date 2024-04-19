import google.cloud.pubsub_v1 as pubsub_v1
from google.cloud import storage
import json
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
import logging
import sys
from datetime import datetime

# Inline argument which gives receiver a title for logging
instance_id = sys.argv[1] if len(sys.argv) > 1 else "CronJob Receiver"

project_id = "cs510-project1"
subscription_id = "cs510-spring24-sub"
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client)
cloud_logger = logging.getLogger('cloudLogger')
cloud_logger.setLevel(logging.INFO)
cloud_logger.addHandler(handler)

# Prepare storage client
storage_client = storage.Client()
bucket = storage_client.bucket('cs510-spring24-project1-bucket')

# Temporary storage for messages
messages = []

def callback(message):
    message_data = json.loads(message.data.decode('utf-8'))
    messages.append(message_data)
    message.ack()  # Acknowledge the message
    
    if len(messages) % 1000 == 0:
        print(f"{instance_id}: Processed {len(messages)} messages.", end='\r', flush=True)


def sort_and_store_messages():
    if not messages:
        cloud_logger.info("No messages to process.")
        return

    # Group messages by 'VEHICLE_ID'
    grouped_messages = {}
    for message in messages:
        vehicle_id = message['VEHICLE_ID']
        if vehicle_id not in grouped_messages:
            grouped_messages[vehicle_id] = []
        grouped_messages[vehicle_id].append(message)

    # Sort each group by 'ACT_TIME'
    for vehicle_id in grouped_messages:
        grouped_messages[vehicle_id].sort(key=lambda x: x['ACT_TIME'])

    # Sort vehicle IDs to ensure keys are in order
    sorted_grouped_messages = {k: grouped_messages[k] for k in sorted(grouped_messages)}

    # Generate a filename with the current date
    today_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"TriMet__{today_date}.json"
    folder_name = "data_via_topic"

    # Save all messages as a single JSON object indexed by sorted 'VEHICLE_ID'
    blob = bucket.blob(f"{folder_name}/{filename}")
    blob.upload_from_string(json.dumps(sorted_grouped_messages), content_type='application/json')
    cloud_logger.info(f"All messages processed and saved to GCS. Filename: {filename}, Total vehicles processed: {len(sorted_grouped_messages)}.")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..")

try:
    streaming_pull_future.result(timeout=150)  # Extended timeout to ensure all messages are received
except TimeoutError:
    streaming_pull_future.cancel()
    sort_and_store_messages()

