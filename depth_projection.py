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
hitter_vs_rhp_file = os.path.join(DATA_DIR, 'batter_vs_rhp.csv')
hitter_vs_lhp_file = os.path.join(DATA_DIR, 'batter_vs_lhp.csv')

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

# Function to determine name column
def determine_name_column(columns, file_type="hitter"):
    name_col = None
    possible_names = ['fName', 'Name', 'name', 'PLAYERNAME', 'PlayerName', 'Player', 'player']
    if file_type == "pitcher":
        possible_names = ['tName'] + possible_names
    
    for possible_name in possible_names:
        if possible_name in columns:
            name_col = possible_name
            print(f"Using '{name_col}' as the {file_type} name column")
            break
    
    return name_col

# Function to safely get a column value with fallbacks
def safe_get_col(row, col_name, col_map=None):
    if col_name in row.index:
        return row[col_name]
    elif col_map and col_name in col_map and col_map[col_name] in row.index:
        return row[col_map[col_name]]
    return 0  # Default to 0 if column not found

# Define proration function for hitters (per game)
def prorate_hitter(row, name_col,  col_map=None):
    actual_games = safe_get_col(row, 'G')
    
    result = {
        name_col: row[name_col],
        'G': actual_games,  # Keep track of actual projected games
        
    }
    
    # Try to get MLBAMID if available
    if 'MLBAMID' in row.index:
        result['MLBAMID'] = row['MLBAMID']
    elif 'mlbamid' in row.index:
        result['MLBAMID'] = row['mlbamid']
    else:
        result['MLBAMID'] = 0  # Default if missing
    
    if actual_games == 0:
        # Set all stats to 0 if actual games is 0
        for stat in ['R', 'RBI', '1B', '2B', '3B', 'HR', 'BB', 'SO', 'SB', 'CS', 'HBP']:
            result[f'{stat}_per_game'] = 0.0
    else:
        # Calculate per game stats using games
        for stat in ['R', 'RBI', '1B', '2B', '3B', 'HR', 'BB', 'SO', 'SB', 'CS', 'HBP']:
            value = safe_get_col(row, stat, col_map)
            # First get the full projection by extrapolating from actual games to max games
            # Then get the per-game value based on max games
            if stat == 'SO':
                result['K_per_game'] = value / actual_games
            else:
                result[f'{stat}_per_game'] = value / actual_games
    
    return pd.Series(result)

# Define proration function for pitchers (per game)
def prorate_pitcher(row, name_col, col_map=None):
    """Prorate pitcher stats on a per-game basis."""
    games = safe_get_col(row, 'G')
    
    result = {
        name_col: row[name_col],
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
            value = safe_get_col(row, stat, col_map)
            result[f'{stat}_per_game'] = value / games
        
        # Map 'SO' in data to 'K' in desired output
        so_value = safe_get_col(row, 'SO', col_map)
        result['K_per_game'] = so_value / games
        
        # Map 'SV' to 'S' in desired output
        sv_value = safe_get_col(row, 'SV', col_map)
        result['S_per_game'] = sv_value / games
    
    return pd.Series(result)

# Process a dataset and create tables
def process_dataset(df, name_col, table_prefix, conn, col_map=None, is_pitcher=False):
    """Process a dataset and create both full season and per-game tables"""
    # Normalize player names
    df[name_col] = df[name_col].apply(normalize_name)
    
    # Drop existing tables to ensure fresh data
    conn.execute(f'DROP TABLE IF EXISTS {table_prefix}_full_season')
    conn.execute(f'DROP TABLE IF EXISTS {table_prefix}_per_game')
    
    # Create full season table
    print(f"Creating {table_prefix}_full_season table...")
    df.to_sql(f'{table_prefix}_full_season', conn, if_exists='replace', index=False)
    
    # Generate per-game projections
    print(f"Calculating per-game stats for {table_prefix}...")
    
    if is_pitcher:
        per_game_df = df.apply(lambda row: prorate_pitcher(row, name_col, col_map), axis=1)
    else:     
        per_game_df = df.apply(lambda row: prorate_hitter(row, name_col, col_map), axis=1)
    
    # Create per-game table
    print(f"Creating {table_prefix}_per_game table...")
    per_game_df.to_sql(f'{table_prefix}_per_game', conn, if_exists='replace', index=False)
    
    return (df, per_game_df)

# Create the database connection
logging.info(f"Attempting to connect to database at: {DATABASE_FILE}")
conn = None
try:
    conn = sqlite3.connect(DATABASE_FILE)
    logging.info("Successfully connected to the database.")
    
    # Process standard hitters dataset
    print("\n=== PROCESSING STANDARD HITTERS DATASET ===")
    hitter_columns = check_csv_columns(hitter_file)
    if not hitter_columns:
        print("Error with hitter file, exiting.")
        exit(1)
    
    hitter_name_col = determine_name_column(hitter_columns, "hitter")
    if not hitter_name_col:
        print("ERROR: Could not identify name column in hitter CSV file.")
        exit(1)
        
    print(f"Reading {hitter_file}...")
    hitters = pd.read_csv(hitter_file)
    
    # Map column names for calculations if needed
    hitter_col_map = {}
    # Check for required columns and map alternates
    required_hitter_cols = ['G', 'R', 'RBI', '1B', '2B', '3B', 'HR', 'BB', 'SO', 'SB', 'CS', 'HBP']
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
                
    # Process the standard hitters dataset
    hitters_result = process_dataset(hitters, hitter_name_col, "hitters", conn, hitter_col_map)
    
    # Process standard pitchers dataset
    print("\n=== PROCESSING STANDARD PITCHERS DATASET ===")
    pitcher_columns = check_csv_columns(pitcher_file)
    if not pitcher_columns:
        print("Error with pitcher file, exiting.")
        exit(1)
    
    pitcher_name_col = determine_name_column(pitcher_columns, "pitcher")
    if not pitcher_name_col:
        print("ERROR: Could not identify name column in pitcher CSV file.")
        exit(1)
        
    print(f"Reading {pitcher_file}...")
    pitchers = pd.read_csv(pitcher_file)
    
    # Map column names for pitchers if needed
    pitcher_col_map = {}
    # Check for required columns and map alternates
    required_pitcher_cols = ['G', 'IP', 'SO', 'H', 'ER', 'BB', 'HBP', 'W', 'R', 'SV', 'HLD']
    for req_col in required_pitcher_cols:
        if req_col not in pitchers.columns:
            # Try to find alternate column names
            if req_col == 'SO' and 'K' in pitchers.columns:
                pitcher_col_map['SO'] = 'K'
            elif req_col == 'SV' and 'S' in pitchers.columns:
                pitcher_col_map['SV'] = 'S'
            else:
                print(f"WARNING: Missing required pitcher column: {req_col}")
                
    # Process the standard pitchers dataset
    pitchers_result = process_dataset(pitchers, pitcher_name_col, "pitchers", conn, pitcher_col_map, is_pitcher=True)
    
    # Process hitters vs RHP dataset
    print("\n=== PROCESSING HITTERS VS RHP DATASET ===")
    if os.path.exists(hitter_vs_rhp_file):
        hitter_vs_rhp_columns = check_csv_columns(hitter_vs_rhp_file)
        if not hitter_vs_rhp_columns:
            print("Error with hitter vs RHP file, skipping.")
        else:
            hitter_vs_rhp_name_col = determine_name_column(hitter_vs_rhp_columns, "hitter")
            if not hitter_vs_rhp_name_col:
                print("ERROR: Could not identify name column in hitter vs RHP CSV file, skipping.")
            else:
                print(f"Reading {hitter_vs_rhp_file}...")
                hitters_vs_rhp = pd.read_csv(hitter_vs_rhp_file)
                
                # Map column names for hitters vs RHP if needed
                hitter_vs_rhp_col_map = {}
                # Check for required columns and map alternates
                for req_col in required_hitter_cols:
                    if req_col not in hitters_vs_rhp.columns:
                        # Try to find alternate column names
                        if req_col == 'SO' and 'K' in hitters_vs_rhp.columns:
                            hitter_vs_rhp_col_map['SO'] = 'K'
                        elif req_col == '1B' and '1B' not in hitters_vs_rhp.columns and 'H' in hitters_vs_rhp.columns and '2B' in hitters_vs_rhp.columns and '3B' in hitters_vs_rhp.columns and 'HR' in hitters_vs_rhp.columns:
                            # Calculate 1B if not present but can be derived
                            hitters_vs_rhp['1B'] = hitters_vs_rhp['H'] - hitters_vs_rhp['2B'] - hitters_vs_rhp['3B'] - hitters_vs_rhp['HR']
                        else:
                            print(f"WARNING: Missing required hitter vs RHP column: {req_col}")
                
                # Process the hitters vs RHP dataset
                hitters_vs_rhp_result = process_dataset(hitters_vs_rhp, hitter_vs_rhp_name_col, "hitters_vs_rhp", conn, hitter_vs_rhp_col_map)
    else:
        print(f"Hitter vs RHP file not found: {hitter_vs_rhp_file}")
    
    # Process hitters vs LHP dataset
    print("\n=== PROCESSING HITTERS VS LHP DATASET ===")
    if os.path.exists(hitter_vs_lhp_file):
        hitter_vs_lhp_columns = check_csv_columns(hitter_vs_lhp_file)
        if not hitter_vs_lhp_columns:
            print("Error with hitter vs LHP file, skipping.")
        else:
            hitter_vs_lhp_name_col = determine_name_column(hitter_vs_lhp_columns, "hitter")
            if not hitter_vs_lhp_name_col:
                print("ERROR: Could not identify name column in hitter vs LHP CSV file, skipping.")
            else:
                print(f"Reading {hitter_vs_lhp_file}...")
                hitters_vs_lhp = pd.read_csv(hitter_vs_lhp_file)
                
                # Map column names for hitters vs LHP if needed
                hitter_vs_lhp_col_map = {}
                # Check for required columns and map alternates
                for req_col in required_hitter_cols:
                    if req_col not in hitters_vs_lhp.columns:
                        # Try to find alternate column names
                        if req_col == 'SO' and 'K' in hitters_vs_lhp.columns:
                            hitter_vs_lhp_col_map['SO'] = 'K'
                        elif req_col == '1B' and '1B' not in hitters_vs_lhp.columns and 'H' in hitters_vs_lhp.columns and '2B' in hitters_vs_lhp.columns and '3B' in hitters_vs_lhp.columns and 'HR' in hitters_vs_lhp.columns:
                            # Calculate 1B if not present but can be derived
                            hitters_vs_lhp['1B'] = hitters_vs_lhp['H'] - hitters_vs_lhp['2B'] - hitters_vs_lhp['3B'] - hitters_vs_lhp['HR']
                        else:
                            print(f"WARNING: Missing required hitter vs LHP column: {req_col}")
                
                # Process the hitters vs LHP dataset
                hitters_vs_lhp_result = process_dataset(hitters_vs_lhp, hitter_vs_lhp_name_col, "hitters_vs_lhp", conn, hitter_vs_lhp_col_map)
    else:
        print(f"Hitter vs LHP file not found: {hitter_vs_lhp_file}")
    
    print("\nAll projections have been freshly imported and generated in 'mlb_sorare.db'.")

except sqlite3.OperationalError as e:
    logging.error(f"Error opening database: {e}")
    raise
finally:
    # Close the connection only at the end after all operations
    if 'conn' in locals() and conn:
        conn.close()