import json
import os
from datetime import datetime, timedelta
from tqdm import tqdm
import psutil
import gc  # Import garbage collection module

# Constants
INPUT_FOLDER = 'downloaded_jsons'
OUTPUT_FOLDER = 'cleaned_jsons'
TESTING = False

def log_memory_usage():
    """Logs the current memory usage of the script."""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    print(f"Memory used: {mem_info.rss / (1024 * 1024)} MB")

def parse_date(opd_date):
    """Parses the OPD_DATE string into a datetime object."""
    return datetime.strptime(opd_date, '%d%b%Y:%H:%M:%S')

def impute_gps_coordinates(events):
    """Imputes missing GPS coordinates using linear interpolation."""
    for i, event in enumerate(events):
        if event['GPS_LONGITUDE'] is None or event['GPS_LATITUDE'] is None:
            prev_event = None
            next_event = None
            for j in range(i - 1, -1, -1):
                if events[j]['GPS_LONGITUDE'] is not None and events[j]['GPS_LATITUDE'] is not None:
                    prev_event = events[j]
                    break
            for k in range(i + 1, len(events)):
                if events[k]['GPS_LONGITUDE'] is not None and events[k]['GPS_LATITUDE'] is not None:
                    next_event = events[k]
                    break

            if prev_event and next_event:
                time_ratio = (event['ACT_TIME'] - prev_event['ACT_TIME']) / (next_event['ACT_TIME'] - prev_event['ACT_TIME'])
                event['GPS_LONGITUDE'] = prev_event['GPS_LONGITUDE'] + time_ratio * (next_event['GPS_LONGITUDE'] - prev_event['GPS_LONGITUDE'])
                event['GPS_LATITUDE'] = prev_event['GPS_LATITUDE'] + time_ratio * (next_event['GPS_LATITUDE'] - prev_event['GPS_LATITUDE'])
            elif prev_event:
                event['GPS_LONGITUDE'] = prev_event['GPS_LONGITUDE']
                event['GPS_LATITUDE'] = prev_event['GPS_LATITUDE']
            elif next_event:
                event['GPS_LONGITUDE'] = next_event['GPS_LONGITUDE']
                event['GPS_LATITUDE'] = next_event['GPS_LATITUDE']
            else:
                event['GPS_LONGITUDE'] = 0
                event['GPS_LATITUDE'] = 0

def compute_speeds(events):
    """Computes speeds for the breadcrumbs."""
    for i in range(1, len(events)):
        delta_distance = events[i]['METERS'] - events[i-1]['METERS']
        delta_time = events[i]['ACT_TIME'] - events[i-1]['ACT_TIME']
        if delta_time > 0:
            events[i]['SPEED'] = round((delta_distance / delta_time) * 3.6, 2)  # Speed in km/h
        else:
            events[i]['SPEED'] = 0
    if len(events) > 0:
        events[0]['SPEED'] = 0

def convert_breadcrumb(event, common_trip_id):
    """Converts event data into the format for the breadcrumb table."""
    base_time = parse_date(event["OPD_DATE"])
    full_time = base_time + timedelta(seconds=event["ACT_TIME"])
    return {
        "tstamp": full_time.isoformat(),
        "latitude": event["GPS_LATITUDE"],
        "longitude": event["GPS_LONGITUDE"],
        "speed": event["SPEED"],
        "trip_id": common_trip_id  # Use the common trip_id
    }

def convert_trip(vehicle_id, event):
    """Converts event data into the format for the trip table."""
    base_time = parse_date(event["OPD_DATE"])
    weekday = base_time.weekday()

    if weekday < 5:
        service_key = "Weekday"
    elif weekday == 5:
        service_key = "Saturday"
    else:
        service_key = "Sunday"

    return {
        "trip_id": event["EVENT_NO_TRIP"],
        "route_id": 1,
        "vehicle_id": vehicle_id,
        "service_key": service_key,
        "direction": True
    }

def clean_json_files():
    """Cleans JSON files and saves cleaned data to disk."""
    if not os.path.exists(INPUT_FOLDER) or not os.path.isdir(INPUT_FOLDER):
        print(f"Input directory {INPUT_FOLDER} does not exist.")
        return

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    already_processed = {f for f in os.listdir(OUTPUT_FOLDER) if f.endswith('.json')}
    json_files = [f for f in os.listdir(INPUT_FOLDER) if f.endswith('.json')]
    pbar = tqdm(json_files, desc="Cleaning JSON files", total=len(json_files))

    first_trip_printed = False
    first_breadcrumb_printed = False

    for file_name in pbar:
        with open(os.path.join(INPUT_FOLDER, file_name), 'r') as file:
            data = json.load(file)

        for vehicle_id, events in data.items():
            vehicle_id = int(vehicle_id)
            if not events:
                continue

            try:
                opd_date = parse_date(events[0]['OPD_DATE'])
                date_str = opd_date.strftime('%Y-%m-%d')
                output_file_name = f"{date_str}__{vehicle_id}.json"
            except IndexError:
                print(f"No events available in {file_name} for vehicle {vehicle_id} to determine OPD_DATE.")
                continue

            if output_file_name in already_processed:
                pbar.set_description(f"Skipped {output_file_name} (already processed)")
                continue

            impute_gps_coordinates(events)
            compute_speeds(events)

            # Use the first event's trip_id as the common trip_id
            common_trip_id = events[0]["EVENT_NO_TRIP"]

            breadcrumbs = [convert_breadcrumb(e, common_trip_id) for e in events]
            if not breadcrumbs:
                print(f"No valid breadcrumb data for {file_name}, vehicle {vehicle_id}.")
                continue

            trip_info = convert_trip(vehicle_id, events[0])

            with open(os.path.join(OUTPUT_FOLDER, output_file_name), 'w') as out_file:
                json.dump({'trip_info': trip_info, 'breadcrumbs': breadcrumbs}, out_file, indent=4)
                pbar.set_description(f"Saved cleaned data to {output_file_name}")

            if not first_trip_printed:
                trip_sql = f"INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction) VALUES ({trip_info['trip_id']}, {trip_info['route_id']}, {trip_info['vehicle_id']}, '{trip_info['service_key']}', {trip_info['direction']});"
                print(trip_sql)
                first_trip_printed = True

            if breadcrumbs and not first_breadcrumb_printed:
                breadcrumb = breadcrumbs[0]
                breadcrumb_sql = f"INSERT INTO breadcrumb (tstamp, latitude, longitude, speed, trip_id) VALUES ('{breadcrumb['tstamp']}', {breadcrumb['latitude']}, {breadcrumb['longitude']}, {breadcrumb['speed']}, {breadcrumb['trip_id']});"
                print(breadcrumb_sql)
                first_breadcrumb_printed = True

            del events
            gc.collect()

        if TESTING:
            break

if __name__ == "__main__":
    try:
        log_memory_usage()
        clean_json_files()
        log_memory_usage()
    except Exception as e:
        print(f"Script terminated with exception: {e}")

