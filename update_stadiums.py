import sqlite3

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
    2392: ("Minute Maid Park", 29.7572, -95.3551, 25, 1, "America/Chicago"),
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

DB_PATH = "mlb_sorare.db"  # Change if needed

def insert_regular_season_stadiums(db_path):
    """Insert stadium data from STADIUM_DATA into the SQLite database."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("drop table if exists Stadiums")
    # Ensure the Stadiums table exists
    c.execute('''CREATE TABLE IF NOT EXISTS Stadiums 
                 (id INTEGER PRIMARY KEY, name TEXT, lat REAL, lon REAL, orientation REAL, is_dome INTEGER, timezone TEXT)''')

    for stadium_id, stadium_info in STADIUM_DATA_VERIFIED.items():
        name, lat, lon, orientation, is_dome, timezone = stadium_info
        c.execute("""
            INSERT OR REPLACE INTO Stadiums (id, name, lat, lon, orientation, is_dome, timezone) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (stadium_id, name, lat, lon, orientation, is_dome, timezone))

    conn.commit()
    conn.close()
    print(f"✅ {len(STADIUM_DATA_VERIFIED)} stadiums inserted into the database!")

# Run the one-time update
insert_regular_season_stadiums(DB_PATH)