import os
from utils import get_db_connection  # Updated to use the new connection function

import pandas as pd

#   'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5, 'E': 90, 'ESE': 112.5,
#   'SE': 135, 'SSE': 157.5, 'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
#   'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5

# google maps lat/lon for MLB stadiums verification then screen shot for 
# orientation of field using https://protractoronline.com/ home plate through second base
# 0 degrees is due north, 90 is due east, 180 is due south, and 270 is due west

STADIUM_DATA_VERIFIED = {
    4: ("Guaranteed Rate Field", 41.8299, -87.6338, 125, 0, "America/Chicago"),
    7: ("Kauffman Stadium", 39.0510, -94.4800, 45, 0, "America/Chicago"),
    14: ("Rogers Centre", 43.6414, -79.3892, 110, 1, "America/Toronto"),
    15: ("Chase Field", 33.4455, -112.0667, 45, 1, "America/Phoenix"),
    22: ("Dodger Stadium", 34.0739, -118.2400, 30, 0, "America/Los_Angeles"),
    680: ("T-Mobile Park", 47.5914, -122.3325, 90, 1, "America/Los_Angeles"),
    2392: ("Daikin Park", 29.7572, -95.3551, 25, 1, "America/Chicago"),
    2602: ("Great American Ball Park", 39.0975, -84.5072, 125, 0, "America/New_York"),
    2680: ("Petco Park", 32.7076, -117.1570, 0, 0, "America/Los_Angeles"),
    2889: ("Busch Stadium", 38.6226, -90.1928, 65, 0, "America/Chicago"),
    3309: ("Nationals Park", 38.8730, -77.0074, 30, 0, "America/New_York"),
    3313: ("Yankee Stadium", 40.8296, -73.9262, 75, 0, "America/New_York"),
    4169: ("loanDepot park", 25.7781, -80.2197, 40, 1, "America/New_York"),
    5325: ("Globe Life Field", 32.7473, -97.0847, 105, 1, "America/Chicago"),
    2523: ("George M. Steinbrenner Field", 27.9803, -82.5067, 60, 0, "America/New_York"),
    32: ("American Family Field", 43.0280, -87.9712, 45, 1, "America/Chicago"),
    2681: ("Citizens Bank Park", 39.9061, -75.1665, 10, 0, "America/New_York"),
    3312: ("Target Field", 44.9817, -93.2775, 90, 0, "America/Chicago"),
    2: ("Oriole Park at Camden Yards", 39.2839, -76.6217, 30, 0, "America/New_York"),
    2529: ("Sutter Health Park", 38.5798, -121.5148, 30, 0, "America/Los_Angeles"),
    2394: ("Comerica Park", 42.3390, -83.0485, 150, 0, "America/Detroit"),
    3289: ("Citi Field", 40.7571, -73.8458, 15, 0, "America/New_York"),
    2395: ("Oracle Park", 37.7786, -122.3893, 85, 0, "America/Los_Angeles"),
    1: ("Angel Stadium", 33.8003, -117.8827, 45, 0, "America/Los_Angeles"),
    19: ("Coors Field", 39.7559, -104.9942, 0, 0, "America/Denver"),
    3: ("Fenway Park", 42.3467, -71.0972, 45, 0, "America/New_York"),
    31: ("PNC Park", 40.4469, -80.0057, 115, 0, "America/New_York"),
    17: ("Wrigley Field", 41.9484, -87.6553, 40, 0, "America/Chicago"),
    4705: ("Truist Park", 33.8908, -84.4682, 155, 0, "America/New_York"),
    5: ("Progressive Field", 41.4962, -81.6852, 0, 0, "America/New_York"),
}
# Manually sourced lat/lon, orientation, and dome info for MLB stadiums
#STADIUM_DATA = {
#    13: ("Oakland Coliseum", 37.7516, -122.2005, 55, 0),
#    15: ("Tropicana Field", 27.7683, -82.6534, 100, 1),
#}

def insert_regular_season_stadiums(conn):
    """Insert stadium data from STADIUM_DATA into the PostgreSQL database."""
    
    cur = conn.cursor()

    # Drop and recreate table (PostgreSQL syntax)
    cur.execute("DROP TABLE IF EXISTS stadiums CASCADE")
    
    # Create the Stadiums table with PostgreSQL syntax
    cur.execute('''CREATE TABLE stadiums (
                     id INTEGER PRIMARY KEY, 
                     name TEXT, 
                     lat REAL, 
                     lon REAL, 
                     orientation REAL, 
                     is_dome INTEGER, 
                     timezone TEXT
                   )''')

    # Insert stadium data using PostgreSQL syntax
    for stadium_id, stadium_info in STADIUM_DATA_VERIFIED.items():
        name, lat, lon, orientation, is_dome, timezone = stadium_info
        cur.execute("""
            INSERT INTO stadiums (id, name, lat, lon, orientation, is_dome, timezone) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                orientation = EXCLUDED.orientation,
                is_dome = EXCLUDED.is_dome,
                timezone = EXCLUDED.timezone
        """, (stadium_id, name, lat, lon, orientation, is_dome, timezone))
    
    conn.commit()
    print(f"✅ {len(STADIUM_DATA_VERIFIED)} stadiums inserted into the database!")

# --- Load Park Factors from CSV ---
def load_park_factors_from_csv(conn, csv_path='park_data.csv'):
    """Load park factors from a CSV file into the database."""
    df = pd.read_csv(csv_path)
    factor_types = ['Park Factor', 'wOBACon', 'xwOBACon', 'BACON', 'xBACON', 'HardHit', 
                    'R', 'OBP', 'H', '1B', '2B', '3B', 'HR', 'BB', 'SO']
    
    cur = conn.cursor()
    
    # Drop and recreate table (PostgreSQL syntax)
    cur.execute("DROP TABLE IF EXISTS park_factors CASCADE")
    cur.execute('''CREATE TABLE park_factors (
                     id SERIAL PRIMARY KEY, 
                     stadium_id INTEGER, 
                     factor_type TEXT, 
                     value REAL,
                     FOREIGN KEY (stadium_id) REFERENCES stadiums(id)
                   )''')
    
    for _, row in df.iterrows():
        venue = row['Venue']
        # Use PostgreSQL parameter syntax (%s instead of ?)
        cur.execute("SELECT id FROM stadiums WHERE name ILIKE %s", (f"%{venue}%",))
        stadium_result = cur.fetchone()
        
        if stadium_result:
            stadium_id = stadium_result[0]
            for factor_type in factor_types:
                value = row[factor_type]
                cur.execute("INSERT INTO park_factors (stadium_id, factor_type, value) VALUES (%s, %s, %s)",
                          (stadium_id, factor_type, value))
    
    conn.commit()
    print(f"Park factors updated from {csv_path}")


def main():
    # Run the one-time update
    conn = get_db_connection()
    try:
        insert_regular_season_stadiums(conn)

        park_data_csv = os.path.join("data", 'park_data.csv')
        load_park_factors_from_csv(conn, park_data_csv)
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()