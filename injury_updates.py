import os
import requests
# Ensure get_db_connection and normalize_name are imported from utils
# It is critical that utils.py's get_db_connection now returns a psycopg2 connection
from utils import normalize_name, get_db_connection

import logging
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("injury_updates")


# Define the API URL for injury data
api_url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries"

def fetch_injury_data():
    """
    Fetches MLB injury data from the ESPN API.
    """
    try:
        logger.info("Fetching injury data from ESPN API...")
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch injury data: {e}")
        return None

def update_database(data):
    logger.info("Updating database with injury data...")
    """
    Updates the 'injuries' table in the database with fetched injury data.
    """
    if not data:
        print("No injury data to update.")
        return

    # Use the utility function to get the database connection
    # This connection object is expected to be a psycopg2 connection
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Drop the table if it exists
        # This is generally fine for temporary or frequently updated tables like injuries
        cursor.execute("DROP TABLE IF EXISTS injuries")

        # Create the 'injuries' table
        # Changed INTEGER PRIMARY KEY AUTOINCREMENT to SERIAL PRIMARY KEY for PostgreSQL
        cursor.execute("""CREATE TABLE IF NOT EXISTS injuries (
                id SERIAL PRIMARY KEY, -- PostgreSQL auto-incrementing primary key
                player_name TEXT,
                team TEXT,
                status TEXT,
                description TEXT,
                long_description TEXT,
                return_estimate TEXT
            )
        """)

        # Clear old data before updating (redundant if table is dropped, but good for other scenarios)
        # If you remove DROP TABLE, keep this. If you keep DROP TABLE, this is optional.
        # Given DROP TABLE, this line can be removed as the table is fresh.
        # cursor.execute("DELETE FROM injuries")

        for team in data.get("injuries", []):
            team_name = team.get("team", {}).get("name", "Unknown Team")
            for injury in team.get("injuries", []):
                player_name = normalize_name(injury.get("athlete", {}).get("displayName", "Unknown Player"))
                status = injury.get("status", "Unknown Status")
                description = injury.get("shortComment", "No details available")
                long_description = injury.get("longComment", "No long description available")
                return_estimate = injury.get("details", {}).get("returnDate", "No estimated return date")

                # The %s placeholders are correct for psycopg2
                cursor.execute("""INSERT INTO injuries (player_name, team, status, description, long_description, return_estimate)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (player_name, team_name, status, description, long_description, return_estimate))

        conn.commit() # Commit changes to the database
        print("Injury data updated successfully.")

    except Exception as e:
        logger.error(f"An error occurred during database update: {e}")
        conn.rollback() # Rollback in case of error
    finally:
        cursor.close()
        conn.close() # Always close the connection

def main():
    """
    Main function to fetch injury data and update the database.
    """
    print("Fetching injury data...")
    data = fetch_injury_data()
    if data:
        print("Updating database with injury data...")
        update_database(data)
        print("Database updated with injury information.")
    else:
        print("Could not fetch injury data. Database not updated.")

if __name__ == '__main__':
    main()