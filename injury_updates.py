import sqlite3
import requests
from utils import normalize_name, DATABASE_FILE  # Import the normalization function


api_url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries"

def fetch_injury_data():
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch injury data")
        return None

def update_database(data):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS injuries")  # Drop the table if it exists

    cursor.execute("""CREATE TABLE IF NOT EXISTS injuries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT,
            team TEXT,
            status TEXT,
            description TEXT,
            long_description TEXT,
            return_estimate TEXT
        )
    """)
    
    cursor.execute("DELETE FROM injuries")  # Clear old data before updating
    
    for team in data.get("injuries", []):
        team_name = team.get("team", {}).get("name", "Unknown Team")
        for injury in team.get("injuries", []):
            player_name = normalize_name(injury.get("athlete", {}).get("displayName", "Unknown Player"))  # Normalize here
            status = injury.get("status", "Unknown Status")
            description = injury.get("shortComment", "No details available")
            long_description = injury.get("longComment", "No long description available")
            return_estimate = injury.get("details", {}).get("returnDate", "No estimated return date")
            
            cursor.execute("""INSERT INTO injuries (player_name, team, status, description, long_description, return_estimate)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (player_name, team_name, status, description, long_description, return_estimate))
    
    conn.commit()
    conn.close()

def main():
    data = fetch_injury_data()
    if data:
        update_database(data)
        print("Database updated with injury data.")
    else:
        print("No data to update.")

if __name__ == "__main__":
    main()