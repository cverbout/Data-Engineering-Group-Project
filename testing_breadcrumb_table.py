import psycopg2

# Database connection parameters
host = "34.145.104.158"  # Use the IP of your Cloud SQL instance
dbname = "project-DB"
user = "will"
password = "karlasgremlins"
sslmode = "require"  # Use SSL mode for security

# SQL query
query = "SELECT * FROM breadcrumb;"

# Connect to the PostgreSQL database
try:
    # Set up a connection to the database
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        sslmode=sslmode
    )
    
    # Create a cursor object
    cur = conn.cursor()
    
    # Execute the query
    cur.execute(query)
    
    # Fetch and print all rows from the query
    rows = cur.fetchall()
    for row in rows:
        print(row)
    
    # Close the cursor and connection
    cur.close()
    conn.close()

except psycopg2.Error as e:
    print(f"Error: {e}")

