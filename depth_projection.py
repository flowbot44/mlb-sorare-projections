import pandas as pd
import sqlite3
import os
from utils import normalize_name, DATABASE_FILE
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Debugging info
logging.info(f"Current working directory: {os.getcwd()}")
logging.info(f"DATABASE_FILE path: {DATABASE_FILE}")
logging.info(f"Directory portion: {os.path.dirname(DATABASE_FILE)}")

# Check directory permissions
if os.path.dirname(DATABASE_FILE):
    if not os.path.exists(os.path.dirname(DATABASE_FILE)):
        logging.info(f"Directory does not exist, creating: {os.path.dirname(DATABASE_FILE)}")
        os.makedirs(os.path.dirname(DATABASE_FILE), exist_ok=True)
    
    test_permissions = os.access(os.path.dirname(DATABASE_FILE), os.W_OK)
    logging.info(f"Directory is writable: {test_permissions}")

# Use environment variables with defaults
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Construct file paths
hitter_file = os.path.join(DATA_DIR, 'batter.csv')
pitcher_file = os.path.join(DATA_DIR, 'pitcher.csv')

# Create the database connection - IMPORTANT: Don't close it immediately
logging.info(f"Attempting to connect to database at: {DATABASE_FILE}")
try:
    conn = sqlite3.connect(DATABASE_FILE)
    logging.info("Successfully connected to the database.")
    
    # Function to check and display CSV column names
    def check_csv_columns(filepath):
        if not os.path.exists(filepath):
            print(f"ERROR: File {filepath} does not exist!")
            return None
        
        # Read the first few rows to inspect
        try:
            df = pd.read_csv(filepath, nrows=1)
            return df.columns.tolist()
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            return None

    # Check CSV files and their columns
    hitter_columns = check_csv_columns(hitter_file)
    pitcher_columns = check_csv_columns(pitcher_file)

    if not hitter_columns or not pitcher_columns:
        print("Exiting due to file issues.")
        exit(1)

    # Determine the name column for hitters
    hitter_name_col = None
    for possible_name in ['fName', 'Name', 'name', 'PLAYERNAME', 'PlayerName', 'Player', 'player']:
        if possible_name in hitter_columns:
            hitter_name_col = possible_name
            print(f"Using '{hitter_name_col}' as the hitter name column")
            break

    # Determine the name column for pitchers
    pitcher_name_col = None
    for possible_name in ['tName', 'Name', 'name', 'PLAYERNAME', 'PlayerName', 'Player', 'player']:
        if possible_name in pitcher_columns:
            pitcher_name_col = possible_name
            print(f"Using '{pitcher_name_col}' as the pitcher name column")
            break

    if not hitter_name_col or not pitcher_name_col:
        print("ERROR: Could not identify name columns in CSV files.")
        exit(1)

    # Read the CSV files
    print(f"Reading {hitter_file}...")
    hitters = pd.read_csv(hitter_file)
    print(f"Reading {pitcher_file}...")
    pitchers = pd.read_csv(pitcher_file)

    # Normalize player names
    hitters[hitter_name_col] = hitters[hitter_name_col].apply(normalize_name)
    pitchers[pitcher_name_col] = pitchers[pitcher_name_col].apply(normalize_name)

    # Drop existing tables to ensure fresh data
    conn.execute('DROP TABLE IF EXISTS hitters_full_season')
    conn.execute('DROP TABLE IF EXISTS pitchers_full_season')
    conn.execute('DROP TABLE IF EXISTS hitters_per_game')
    conn.execute('DROP TABLE IF EXISTS pitchers_per_game')

    # Create and populate tables with the data as is
    print("Creating full season tables...")
    hitters.to_sql('hitters_full_season', conn, if_exists='replace', index=False)
    pitchers.to_sql('pitchers_full_season', conn, if_exists='replace', index=False)

    # Check for required columns for calculations
    required_hitter_cols = ['G', 'R', 'RBI', '1B', '2B', '3B', 'HR', 'BB', 'SO', 'SB', 'CS', 'HBP']
    required_pitcher_cols = ['G', 'IP', 'SO', 'H', 'ER', 'BB', 'HBP', 'W', 'R', 'SV', 'HLD']

    # Map column names for calculations if needed
    # For example, if 'K' is used instead of 'SO' in the data
    hitter_col_map = {}
    pitcher_col_map = {}

    for req_col in required_hitter_cols:
        if req_col not in hitters.columns:
            # Try to find alternate column names
            if req_col == 'SO' and 'K' in hitters.columns:
                hitter_col_map['SO'] = 'K'
            elif req_col == '1B' and '1B' not in hitters.columns and 'H' in hitters.columns and '2B' in hitters.columns and '3B' in hitters.columns and 'HR' in hitters.columns:
                # Calculate 1B if not present but can be derived
                hitters['1B'] = hitters['H'] - hitters['2B'] - hitters['3B'] - hitters['HR']
            else:
                print(f"WARNING: Missing required hitter column: {req_col}")

    for req_col in required_pitcher_cols:
        if req_col not in pitchers.columns:
            # Try to find alternate column names
            if req_col == 'SO' and 'K' in pitchers.columns:
                pitcher_col_map['SO'] = 'K'
            elif req_col == 'SV' and 'S' in pitchers.columns:
                pitcher_col_map['SV'] = 'S'
            else:
                print(f"WARNING: Missing required pitcher column: {req_col}")

    # Function to safely get a column value with fallbacks
    def safe_get_col(row, col_name, col_map=None):
        if col_name in row.index:
            return row[col_name]
        elif col_map and col_name in col_map and col_map[col_name] in row.index:
            return row[col_map[col_name]]
        return 0  # Default to 0 if column not found

    # Define proration function for hitters (per game)
    def prorate_hitter(row):
        """Prorate hitter stats on a per-game basis."""
        games = safe_get_col(row, 'G')
        
        result = {
            hitter_name_col: row[hitter_name_col],
            'G': games
        }
        
        # Try to get MLBAMID if available
        if 'MLBAMID' in row.index:
            result['MLBAMID'] = row['MLBAMID']
        elif 'mlbamid' in row.index:
            result['MLBAMID'] = row['mlbamid']
        else:
            result['MLBAMID'] = 0  # Default if missing
        
        if games == 0:
            # Set all stats to 0 if games is 0
            for stat in ['R', 'RBI', '1B', '2B', '3B', 'HR', 'BB', 'SO', 'SB', 'CS', 'HBP']:
                result[f'{stat}_per_game'] = 0.0
        else:
            # Calculate per game stats
            for stat in ['R', 'RBI', '1B', '2B', '3B', 'HR', 'BB', 'SO', 'SB', 'CS', 'HBP']:
                value = safe_get_col(row, stat, hitter_col_map)
                # Map 'SO' in data to 'K' in desired output if needed
                if stat == 'SO':
                    result['K_per_game'] = value / games
                else:
                    result[f'{stat}_per_game'] = value / games
        
        return pd.Series(result)

    # Define proration function for pitchers (per game)
    def prorate_pitcher(row):
        """Prorate pitcher stats on a per-game basis."""
        games = safe_get_col(row, 'G')
        
        result = {
            pitcher_name_col: row[pitcher_name_col],
            'G': games
        }
        
        # Try to get MLBAMID if available
        if 'MLBAMID' in row.index:
            result['MLBAMID'] = row['MLBAMID']
        elif 'mlbamid' in row.index:
            result['MLBAMID'] = row['mlbamid']
        else:
            result['MLBAMID'] = 0  # Default if missing
        
        if games == 0:
            # Set all stats to 0 if games is 0
            for stat in ['IP', 'SO', 'H', 'ER', 'BB', 'HBP', 'W', 'R', 'SV', 'HLD']:
                result[f'{stat}_per_game'] = 0.0
        else:
            # Calculate per game stats
            for stat in ['IP', 'H', 'ER', 'BB', 'HBP', 'W', 'R', 'HLD']:
                value = safe_get_col(row, stat, pitcher_col_map)
                result[f'{stat}_per_game'] = value / games
            
            # Map 'SO' in data to 'K' in desired output
            so_value = safe_get_col(row, 'SO', pitcher_col_map)
            result['K_per_game'] = so_value / games
            
            # Map 'SV' to 'S' in desired output
            sv_value = safe_get_col(row, 'SV', pitcher_col_map)
            result['S_per_game'] = sv_value / games
        
        return pd.Series(result)

    print("Calculating per-game stats...")
    # Generate per-game projections
    hitters_per_game = hitters.apply(prorate_hitter, axis=1)
    pitchers_per_game = pitchers.apply(prorate_pitcher, axis=1)

    # Create tables with the calculated per-game stats
    print("Creating per-game tables...")
    hitters_per_game.to_sql('hitters_per_game', conn, if_exists='replace', index=False)
    pitchers_per_game.to_sql('pitchers_per_game', conn, if_exists='replace', index=False)
    
    print("Full-season and per-game projections have been freshly imported and generated in 'mlb_sorare.db'.")

except sqlite3.OperationalError as e:
    logging.error(f"Error opening database: {e}")
    raise
finally:
    # Close the connection only at the end after all operations
    if 'conn' in locals() and conn:
        conn.close()