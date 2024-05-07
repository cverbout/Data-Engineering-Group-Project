import psycopg2
from psycopg2 import extras
from psycopg2 import pool
from datetime import datetime

# Initialize the connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10,
    dbname="project-DB",
    user="will",
    password="karlasgremlins",
    host="34.145.104.158",
    sslmode="require"
)

def connect_db():
    return connection_pool.getconn()

def close_db(conn):
    connection_pool.putconn(conn)

def ensure_trip_exists(trip_id, vehicle_id):
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            # Check if trip_id exists
            cursor.execute("SELECT trip_id FROM trip WHERE trip_id = %s", (trip_id,))
            if cursor.fetchone() is None:
                # Insert a new trip if not exists
                cursor.execute(
                    "INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction) VALUES (%s, 1, %s, 'Weekday', true)",
                    (trip_id, vehicle_id)
                )
                conn.commit()
    except psycopg2.Error as e:
        print(f"Error checking/inserting trip: {e}")
    finally:
        close_db(conn)

def parse_date(opd_date):
    # Example format '29DEC2022:00:00:00'
    return datetime.strptime(opd_date, '%d%b%Y:%H:%M:%S')

def insert_breadcrumb(events):
    """Insert multiple breadcrumb records from a list of event dictionaries."""
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            # Prepare data for bulk insertion
            records = []
            for event in events:
                trip_id = event['EVENT_NO_TRIP']
                vehicle_id = event['VEHICLE_ID']
                tstamp = parse_date(event['OPD_DATE'])
                latitude = event['GPS_LATITUDE']
                longitude = event['GPS_LONGITUDE']
                speed = 0  # Assuming a default speed if not provided

                # Ensure the trip exists before inserting breadcrumb
                ensure_trip_exists(trip_id, vehicle_id)

                records.append((tstamp, latitude, longitude, speed, trip_id))

            # Insert all records at once if records are not empty
            if records:
                query = """
                INSERT INTO breadcrumb (tstamp, latitude, longitude, speed, trip_id)
                VALUES %s
                """
                extras.execute_values(cursor, query, records)
                conn.commit()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        close_db(conn)

