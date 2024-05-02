import psycopg2
from psycopg2 import extras
from datetime import datetime

def connect_db():
    return psycopg2.connect(
        dbname="project-DB",
        user="will",
        password="karlasgremlins",  # Replace with your actual password
        host="34.145.104.158",    # Use the IP of your Cloud SQL instance
        sslmode="require"         # Use SSL mode for security
    )

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
        conn.close()

def parse_date(opd_date):
    # Example format '29DEC2022:00:00:00'
    return datetime.strptime(opd_date, '%d%b%Y:%H:%M:%S')


def insert_breadcrumb(events):
    """Insert multiple breadcrumb records from a list of event dictionaries."""
    conn = connect_db()
    try:
        with conn.cursor() as cursor:
            for event in events:
                trip_id = event['EVENT_NO_TRIP']
                vehicle_id = event['VEHICLE_ID']
                tstamp = parse_date(event['OPD_DATE'])
                latitude = event['GPS_LATITUDE']
                longitude = event['GPS_LONGITUDE']
                speed = 0  # Assuming a default speed if not provided

                # Ensure the trip exists before inserting breadcrumb
                ensure_trip_exists(trip_id, vehicle_id)

                query = """
                INSERT INTO breadcrumb (tstamp, latitude, longitude, speed, trip_id)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(query, (tstamp, latitude, longitude, speed, trip_id))
            conn.commit()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()

