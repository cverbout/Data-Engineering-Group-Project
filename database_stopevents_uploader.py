import json
import os
import io
import psycopg2
from psycopg2 import pool
from tqdm import tqdm

# Initialize the connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10,
    dbname="project-DB",
    user="chase",
    password="karla",
    host="34.145.104.158",
    sslmode="require"
)

# Global variable for testing mode
TESTING = False

def connect_db():
    return connection_pool.getconn()

def close_db(conn):
    connection_pool.putconn(conn)

def insert_trip(trip_name):
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO stopevents_trips (trip_name)
                VALUES (%s)
                RETURNING trip_id
            """
            cursor.execute(query, (trip_name,))
            trip_id = cursor.fetchone()[0]
            conn.commit()
            if TESTING:
                print(f"Executed SQL: {cursor.query.decode()}")  # Print the SQL query
            return trip_id
    except psycopg2.Error as e:
        print(f"Error inserting trip (Trip Name: {trip_name}): {e}")
        return None
    finally:
        close_db(conn)

def insert_stopevents_details(details, trip_id):
    conn = connect_db()
    if not TESTING:
        # Use copy_from for bulk insertion
        try:
            buffer = io.StringIO()
            for detail in details:
                buffer.write(
                    f"{detail['vehicle_number']},{detail['leave_time']},{detail['train']},{detail['route_number']},{detail['direction']},{detail['service_key']},"
                    f"{detail['trip_number']},{detail['stop_time']},{detail['arrive_time']},{detail['dwell']},{detail['location_id']},{detail['door']},{detail['lift']},"
                    f"{detail['ons']},{detail['offs']},{detail['estimated_load']},{detail['maximum_speed']},{detail['train_mileage']},{detail['pattern_distance']},"
                    f"{detail['location_distance']},{detail['x_coordinate']},{detail['y_coordinate']},{detail['data_source']},{detail['schedule_status']},{trip_id}\n"
                )
            buffer.seek(0)
            with conn.cursor() as cursor:
                cursor.copy_from(buffer, 'stopevents_details', sep=',', columns=(
                    'vehicle_number', 'leave_time', 'train', 'route_number', 'direction', 'service_key',
                    'trip_number', 'stop_time', 'arrive_time', 'dwell', 'location_id', 'door', 'lift', 'ons',
                    'offs', 'estimated_load', 'maximum_speed', 'train_mileage', 'pattern_distance',
                    'location_distance', 'x_coordinate', 'y_coordinate', 'data_source',
                    'schedule_status', 'trip_id'
                ))
                conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Database error (Trip ID: {trip_id}): {e}")
            return False
        finally:
            close_db(conn)
    else:
        # Fallback to the slower method if TESTING is True
        try:
            with conn.cursor() as cursor:
                for detail in details:
                    query = """
                        INSERT INTO stopevents_details (
                            vehicle_number, leave_time, train, route_number, direction, service_key,
                            trip_number, stop_time, arrive_time, dwell, location_id, door, lift, ons,
                            offs, estimated_load, maximum_speed, train_mileage, pattern_distance,
                            location_distance, x_coordinate, y_coordinate, data_source,
                            schedule_status, trip_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    cursor.execute(query, (
                        str(detail['vehicle_number']),
                        int(detail['leave_time']),
                        str(detail['train']),
                        str(detail['route_number']),
                        int(detail['direction']),
                        str(detail['service_key']),
                        str(detail['trip_number']),
                        int(detail['stop_time']),
                        int(detail['arrive_time']),
                        int(detail['dwell']),
                        str(detail['location_id']),
                        int(detail['door']),
                        int(detail['lift']),
                        int(detail['ons']),
                        int(detail['offs']),
                        int(detail['estimated_load']),
                        float(detail['maximum_speed']),
                        float(detail['train_mileage']),
                        int(detail['pattern_distance']),
                        int(detail['location_distance']),
                        float(detail['x_coordinate']),
                        float(detail['y_coordinate']),
                        str(detail['data_source']),
                        int(detail['schedule_status']),
                        trip_id
                    ))
                    if TESTING:
                        print(f"Executed SQL: {cursor.query.decode()}")  # Print the SQL query
                conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Database error (Trip ID: {trip_id}): {e}")
            return False
        finally:
            close_db(conn)

def update_history(filepath):
    with open('upload_stopevents_history.txt', 'a') as history_file:
        history_file.write(filepath + '\n')

def load_history():
    try:
        with open('upload_stopevents_history.txt', 'r') as history_file:
            return set(history_file.read().splitlines())
    except FileNotFoundError:
        return set()

def process_json_files(directory):
    uploaded_files = load_history()
    files = os.listdir(directory)
    if TESTING:
        files = files[:1]  # Only process the first file for testing

    for filename in tqdm(files, desc="Processing JSON files"):
        if filename.endswith('.json'):
            filepath = os.path.join(directory, filename)
            if filepath not in uploaded_files:
                with open(filepath, 'r') as file:
                    data = json.load(file)

                    for vehicle_number, trip_data in data.items():
                        if isinstance(trip_data, str):
                            try:
                                trip_data = json.loads(trip_data)
                            except json.JSONDecodeError:
                                print(f"Failed to decode trip_data: {trip_data[:100]}")
                                continue

                        if isinstance(trip_data, list):
                            if TESTING:
                                trip_data = trip_data[:1]  # Only process the first entry in TESTING mode

                            for trip in trip_data:
                                trip_name = trip.get('trip')
                                details = trip.get('data', [])

                                if trip_name and isinstance(details, list):
                                    trip_id = insert_trip(trip_name)
                                    if trip_id is not None:
                                        if TESTING:
                                            details = details[:1]
                                        
                                        details_success = insert_stopevents_details(details, trip_id)
                                        if details_success:
                                            update_history(filepath)
                                        else:
                                            print(f"Failed to insert stopevents details for file: {filename}")
                                            break  # Exit if any insert fails to prevent further processing
                                    else:
                                        print(f"Failed to insert trip for file: {filename}")
                                        break  # Exit if any insert fails to prevent further processing
                                else:
                                    print(f"Unexpected structure in trip_data: {trip}")
                        else:
                            print(f"Unexpected data type for trip_data after attempt to parse: {type(trip_data)}")
                    if TESTING:
                        break  # Stop processing further vehicles in TESTING mode

def main():
    process_json_files('downloaded_stopevents_jsons')

if __name__ == "__main__":
    main()

