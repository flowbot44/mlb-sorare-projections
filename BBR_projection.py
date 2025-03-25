import pandas as pd
import sqlite3
from utils import normalize_name

# Connect to SQLite database (creates 'mlb_sorare.db' if it doesn't exist)
conn = sqlite3.connect('mlb_sorare.db')

# Drop existing tables to ensure fresh data
conn.execute('DROP TABLE IF EXISTS hitters_full_season')
conn.execute('DROP TABLE IF EXISTS pitchers_full_season')
conn.execute('DROP TABLE IF EXISTS hitters_per_game')
conn.execute('DROP TABLE IF EXISTS pitchers_per_unit')

# Create table for full-season hitter projections
conn.execute('''CREATE TABLE hitters_full_season (
    Rk INTEGER,
    Name TEXT,
    Age INTEGER,
    B TEXT,
    PA INTEGER,
    AB INTEGER,
    R INTEGER,
    H INTEGER,
    "2B" INTEGER,
    "3B" INTEGER,
    HR INTEGER,
    RBI INTEGER,
    SB INTEGER,
    CS INTEGER,
    BB INTEGER,
    SO INTEGER,
    BA FLOAT,
    OBP FLOAT,
    SLG FLOAT,
    OPS FLOAT,
    TB INTEGER,
    GDP INTEGER,
    HBP INTEGER,
    SH INTEGER,
    SF INTEGER,
    IBB INTEGER,
    Rel TEXT,
    "Name-additional" TEXT PRIMARY KEY
);''')

# Create table for full-season pitcher projections
conn.execute('''CREATE TABLE pitchers_full_season (
    Rk INTEGER,
    Name TEXT,
    Age INTEGER,
    T TEXT,
    W INTEGER,
    L INTEGER,
    "W-L%" FLOAT,
    ERA FLOAT,
    SV INTEGER,
    IP FLOAT,
    H INTEGER,
    R INTEGER,
    ER INTEGER,
    HR INTEGER,
    BB INTEGER,
    IBB INTEGER,
    SO INTEGER,
    HBP INTEGER,
    BK INTEGER,
    WP INTEGER,
    BF INTEGER,
    WHIP FLOAT,
    H9 FLOAT,
    HR9 FLOAT,
    BB9 FLOAT,
    SO9 FLOAT,
    "SO/W" FLOAT,
    Rel TEXT,
    "Name-additional" TEXT PRIMARY KEY
);''')

# Read the CSV files and normalize names
hitters = pd.read_csv('2025_bbr_hitter_projections.csv')
hitters['Name'] = hitters['Name'].apply(normalize_name)  # Normalize hitter names

pitchers = pd.read_csv('2025_bbr_pitching_projections.csv')
pitchers['Name'] = pitchers['Name'].apply(normalize_name)  # Normalize pitcher names

# Insert full-season data into the database
hitters.to_sql('hitters_full_season', conn, if_exists='replace', index=False)
pitchers.to_sql('pitchers_full_season', conn, if_exists='replace', index=False)

# Retrieve the full-season data for proration
hitters_full = pd.read_sql('SELECT * FROM hitters_full_season', conn)
pitchers_full = pd.read_sql('SELECT * FROM pitchers_full_season', conn)

# Define proration function for hitters (per game)
def prorate_hitter(row, games_per_season=162):
    """Prorate hitter stats on a per-game basis."""
    if row['PA'] == 0:
        for stat in ['PA', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'SB', 'CS', 'BB', 'SO', 'TB', 'GDP', 'HBP', 'SH', 'SF', 'IBB']:
            row[stat] = 0.0
    else:
        for stat in ['PA', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'SB', 'CS', 'BB', 'SO', 'TB', 'GDP', 'HBP', 'SH', 'SF', 'IBB']:
            row[stat] = row[stat] / games_per_season
    # Rate stats (BA, OBP, SLG, OPS) remain unchanged
    return row

# Define proration function for pitchers
def prorate_pitcher(row):
    """Prorate pitcher stats based on role: per inning for RP, per start for SP."""
    if row['IP'] == 0:
        for stat in ['IP', 'H', 'R', 'ER', 'HR', 'BB', 'IBB', 'SO', 'HBP', 'BK', 'WP', 'BF', 'W', 'L', 'SV']:
            row[stat] = 0.0
    else:
        ip = row['IP']
        if ip > 0:
            for stat in ['IP', 'H', 'R', 'ER', 'HR', 'BB', 'IBB', 'SO', 'HBP', 'BK', 'WP', 'BF', 'SV']:
                row[stat] = row[stat] / ip
    # Rate stats (ERA, WHIP, H9, etc.) remain unchanged
    return row

# Generate per-unit projections
hitters_per_game = hitters_full.apply(prorate_hitter, axis=1)
pitchers_per_unit = pitchers_full.apply(prorate_pitcher, axis=1)

# Create tables for per-unit projections
conn.execute('''CREATE TABLE hitters_per_game (
    Rk INTEGER,
    Name TEXT,
    Age INTEGER,
    B TEXT,
    PA FLOAT,
    AB FLOAT,
    R FLOAT,
    H FLOAT,
    "2B" FLOAT,
    "3B" FLOAT,
    HR FLOAT,
    RBI FLOAT,
    SB FLOAT,
    CS FLOAT,
    BB FLOAT,
    SO FLOAT,
    BA FLOAT,
    OBP FLOAT,
    SLG FLOAT,
    OPS FLOAT,
    TB FLOAT,
    GDP FLOAT,
    HBP FLOAT,
    SH FLOAT,
    SF FLOAT,
    IBB FLOAT,
    Rel TEXT,
    "Name-additional" TEXT PRIMARY KEY
);''')

conn.execute('''CREATE TABLE pitchers_per_unit (
    Rk INTEGER,
    Name TEXT,
    Age INTEGER,
    T TEXT,
    W FLOAT,
    L FLOAT,
    "W-L%" FLOAT,
    ERA FLOAT,
    SV FLOAT,
    IP FLOAT,
    H FLOAT,
    R FLOAT,
    ER FLOAT,
    HR FLOAT,
    BB FLOAT,
    IBB FLOAT,
    SO FLOAT,
    HBP FLOAT,
    BK FLOAT,
    WP FLOAT,
    BF FLOAT,
    WHIP FLOAT,
    H9 FLOAT,
    HR9 FLOAT,
    BB9 FLOAT,
    SO9 FLOAT,
    "SO/W" FLOAT,
    Rel TEXT,
    "Name-additional" TEXT PRIMARY KEY
);''')

# Insert per-unit projections into the database
hitters_per_game.to_sql('hitters_per_game', conn, if_exists='replace', index=False)
pitchers_per_unit.to_sql('pitchers_per_unit', conn, if_exists='replace', index=False)

# Close the database connection
conn.close()

print("Full-season and per-unit projections (per game for hitters/SP, per inning for RP) have been freshly imported and generated in 'mlb_sorare.db' with normalized names.")