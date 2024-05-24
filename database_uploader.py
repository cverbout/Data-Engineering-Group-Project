import json
import os
import io
import psycopg2
from psycopg2 import pool
from datetime import datetime
from tqdm import tqdm

# Initialize the connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10,
    dbname="project-DB",
    user="will",
    password="karlasgremlins",
    host="34.145.104.158",
    sslmode="require"
)

# Global variable for testing mode
TESTING = False

def connect_db():
    return connection_pool.getconn()

def close_db(conn):
    connection_pool.putconn(conn)

def insert_trip(trip_info):
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            # Debugging output
            #print(f"Attempting to insert trip with Trip ID: {trip_info['trip_id']}")
            
            # Insert a new trip, ignore if it exists
            cursor.execute(
                """
                INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (trip_id) DO NOTHING
                """,
                (trip_info['trip_id'], trip_info['route_id'], trip_info['vehicle_id'],
                 trip_info['service_key'], trip_info['direction'])
            )
            conn.commit()
        return True
    except psycopg2.Error as e:
        print(f"Error inserting trip (Trip ID: {trip_info['trip_id']}): {e}")
        return False
    finally:
        close_db(conn)

def insert_breadcrumbs(breadcrumbs, trip_id):
    conn = connect_db()
    try:
        buffer = io.StringIO()
        for breadcrumb in breadcrumbs:
            buffer.write(f"{breadcrumb['tstamp']},{breadcrumb['latitude']},{breadcrumb['longitude']},{breadcrumb['speed']},{trip_id}\n")

        buffer.seek(0)

        # Debugging output
        #print(f"Attempting to insert breadcrumbs for Trip ID: {trip_id}")
        
        with conn.cursor() as cursor:
            cursor.copy_from(buffer, 'breadcrumb', sep=',', columns=('tstamp', 'latitude', 'longitude', 'speed', 'trip_id'))
            conn.commit()
        return True
    except psycopg2.Error as e:
        print(f"Database error (Trip ID: {trip_id}): {e}")
        return False
    finally:
        close_db(conn)

def update_history(filepath):
    with open('upload_history.txt', 'a') as history_file:
        history_file.write(filepath + '\n')

def load_history():
    try:
        with open('upload_history.txt', 'r') as history_file:
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

                    trip_info = data['trip_info']
                    breadcrumbs = data['breadcrumbs']

                    # Ensure consistent trip_id across trip_info and breadcrumbs
                    consistent_trip_id = all(breadcrumb['trip_id'] == trip_info['trip_id'] for breadcrumb in breadcrumbs)
                    if not consistent_trip_id:
                        print(f"Inconsistent trip_ids found in file: {filename}")
                        continue

                    # Insert the trip first to satisfy foreign key constraints
                    trip_success = insert_trip(trip_info)

                    # Insert breadcrumbs if trip insertion was successful
                    if trip_success:
                        breadcrumbs_success = insert_breadcrumbs(breadcrumbs, trip_info['trip_id'])
                        if breadcrumbs_success:
                            # Update the history record only if both inserts are successful
                            update_history(filepath)
                        else:
                            print(f"Failed to insert breadcrumbs for file: {filename}")
                    else:
                        print(f"Failed to insert trip for file: {filename}")

def main():
    # Assuming your cleaned JSON files are in the 'cleaned_jsons' directory
    process_json_files('cleaned_jsons')

if __name__ == "__main__":
    main()

