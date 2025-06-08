import numpy as np
import pandas as pd
import os
from utils import get_sqlalchemy_engine, normalize_name, DATA_DIR
import logging
from sqlalchemy import text
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("depth_projection")

# Construct file paths
hitter_file = os.path.join(DATA_DIR, 'batter.csv')
pitcher_file = os.path.join(DATA_DIR, 'pitcher.csv')
hitter_vs_rhp_file = os.path.join(DATA_DIR, 'batter_vs_rhp.csv')
hitter_vs_lhp_file = os.path.join(DATA_DIR, 'batter_vs_lhp.csv')

# Function to check and display CSV column names
def check_csv_columns(filepath):
    if not os.path.exists(filepath):
        logger.error(f"ERROR: File {filepath} does not exist!")
        return None
    
    try:
        df = pd.read_csv(filepath, nrows=1)
        return df.columns.tolist()
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
        return None

# Function to determine name column
def determine_name_column(columns, file_type="hitter"):
    name_col = None
    possible_names = ['fname', 'name', 'playername', 'player']
    if file_type == "pitcher":
        possible_names = ['tname'] + possible_names
    
    for possible_name in possible_names:
        if possible_name in [col.lower() for col in columns]:
            name_col = possible_name
            logger.info(f"Using '{name_col}' as the {file_type} name column")
            break
    
    return name_col

# Vectorized proration function for hitters
def prorate_hitters_vectorized(df, name_col, col_map=None):
    """Vectorized version of hitter proration for better performance"""
    result_df = pd.DataFrame()
    
    # Apply column mapping first if provided
    work_df = df.copy()
    if col_map:
        work_df = work_df.rename(columns={v: k for k, v in col_map.items()})
    
    # Basic columns
    result_df['name'] = work_df[name_col]
    result_df['g'] = work_df['g'].fillna(0)
    result_df['mlbamid'] = work_df.get('mlbamid', '0').astype(str)
    
    # Vectorized per-game calculations
    games = work_df['g'].fillna(0)
    
    per_game_stats = ['r', 'rbi', 'singles', 'doubles', 'triples', 'hr', 'bb', 'so', 'sb', 'cs', 'hbp']
    for stat in per_game_stats:
        stat_values = work_df.get(stat, 0).fillna(0)
        if stat == 'so':
            result_df['k_per_game'] = np.where(games > 0, stat_values / games, 0.0)
        else:
            result_df[f'{stat}_per_game'] = np.where(games > 0, stat_values / games, 0.0)
    
    return result_df

# Vectorized proration function for pitchers
# Enhanced vectorized proration function for pitchers with hybrid role handling
def prorate_pitchers_vectorized(df, name_col, col_map=None):
    """Vectorized version of pitcher proration with hybrid starter/reliever handling"""
    result_df = pd.DataFrame()
    
    # Apply column mapping first if provided
    work_df = df.copy()
    if col_map:
        work_df = work_df.rename(columns={v: k for k, v in col_map.items()})
    
    # Basic columns
    result_df['name'] = work_df[name_col]
    result_df['g'] = work_df['g'].fillna(0)
    result_df['gs'] = work_df.get('gs', 0).fillna(0)  # Games started
    result_df['mlbamid'] = work_df.get('mlbamid', '0').astype(str)
    
    # Calculate relief appearances
    games = work_df['g'].fillna(0)
    games_started = work_df.get('gs', 0).fillna(0)
    relief_appearances = games - games_started
    
    # Identify pitcher types
    is_pure_starter = (games_started > 0) & (relief_appearances == 0)
    is_pure_reliever = (games_started == 0) & (relief_appearances > 0)
    is_hybrid = (games_started > 0) & (relief_appearances > 0)
    
    # Constants for estimation (based on league averages)
    TYPICAL_STARTER_IP = 5.5
    TYPICAL_RELIEVER_IP = 1.0
    
    # Process innings pitched with role-based logic
    ip_values = work_df.get('ip', 0).fillna(0)
    
    # For hybrid pitchers, estimate separate rates
    starter_ip_rate = np.zeros(len(work_df))
    reliever_ip_rate = np.zeros(len(work_df))
    
    # Pure starters
    starter_ip_rate[is_pure_starter] = np.where(
        games_started[is_pure_starter] > 0,
        ip_values[is_pure_starter] / games_started[is_pure_starter],
        0
    )
    
    # Pure relievers  
    reliever_ip_rate[is_pure_reliever] = np.where(
        relief_appearances[is_pure_reliever] > 0,
        ip_values[is_pure_reliever] / relief_appearances[is_pure_reliever],
        0
    )
    
    # Hybrid pitchers - estimate based on total IP and role distribution
    for idx in np.where(is_hybrid)[0]:
        total_ip = ip_values.iloc[idx]
        starts = games_started.iloc[idx]
        relief_games = relief_appearances.iloc[idx]
        
        if total_ip > 0 and starts > 0 and relief_games > 0:
            # Use system of equations to estimate rates
            # We know: starts * starter_rate + relief * reliever_rate = total_ip
            # We assume starter_rate is typically higher than reliever_rate
            
            # Method 1: Proportional to typical rates
            total_expected_ip = starts * TYPICAL_STARTER_IP + relief_games * TYPICAL_RELIEVER_IP
            if total_expected_ip > 0:
                scaling_factor = total_ip / total_expected_ip
                estimated_starter_rate = TYPICAL_STARTER_IP * scaling_factor
                estimated_reliever_rate = TYPICAL_RELIEVER_IP * scaling_factor
            else:
                # Fallback to simple average
                estimated_starter_rate = total_ip / (starts + relief_games)
                estimated_reliever_rate = estimated_starter_rate
            
            # Cap starter rate at reasonable maximum (9 innings) and minimum
            estimated_starter_rate = np.clip(estimated_starter_rate, 0.5, 9.0)
            estimated_reliever_rate = np.clip(estimated_reliever_rate, 0.1, 4.0)
            
            starter_ip_rate[idx] = estimated_starter_rate
            reliever_ip_rate[idx] = estimated_reliever_rate
    
    # Store the rates
    result_df['ip_per_start'] = starter_ip_rate
    result_df['ip_per_relief'] = reliever_ip_rate
    
    # Calculate overall IP per game for backward compatibility
    result_df['ip_per_game'] = np.where(
        games > 0,
        (games_started * starter_ip_rate + relief_appearances * reliever_ip_rate) / games,
        0.0
    )
    
        # Handle other pitcher stats with role-aware calculations
    pitcher_stats = ['h', 'er', 'bb', 'hbp', 'w', 'r', 'hld']
    
    for stat in pitcher_stats:
        stat_values = work_df.get(stat, 0).fillna(0)
        
        # Calculate per-game stats for all pitchers (this is what we'll use primarily)
        result_df[f'{stat}_per_game'] = np.where(games > 0, stat_values / games, 0.0)
        
        # For hybrid pitchers, also calculate separate rates based on IP proportion
        for idx in np.where(is_hybrid)[0]:
            if games_started.iloc[idx] > 0 and relief_appearances.iloc[idx] > 0:
                total_stat = stat_values.iloc[idx]
                starts = games_started.iloc[idx]
                relief_games = relief_appearances.iloc[idx]
                starter_ip = starter_ip_rate[idx]
                reliever_ip = reliever_ip_rate[idx]
                
                # Distribute stats proportionally to IP within each role
                if starter_ip > 0 and reliever_ip > 0:
                    # Estimate what portion of stats come from starts vs relief
                    total_starter_ip = starts * starter_ip
                    total_reliever_ip = relief_games * reliever_ip
                    total_ip = total_starter_ip + total_reliever_ip
                    
                    if total_ip > 0:
                        # Distribute total stats based on IP contribution
                        starter_stat_share = total_starter_ip / total_ip
                        reliever_stat_share = total_reliever_ip / total_ip
                        
                        estimated_stat_per_start = (total_stat * starter_stat_share) / starts if starts > 0 else 0
                        estimated_stat_per_relief = (total_stat * reliever_stat_share) / relief_games if relief_games > 0 else 0
                        
                        result_df.loc[idx, f'{stat}_per_start'] = estimated_stat_per_start
                        result_df.loc[idx, f'{stat}_per_relief'] = estimated_stat_per_relief
    
    # Special handling for strikeouts
    so_values = work_df.get('so', 0).fillna(0)
    result_df['k_per_game'] = np.where(games > 0, so_values / games, 0.0)
    
    # Saves (typically only for relievers)
    sv_values = work_df.get('sv', 0).fillna(0)
    result_df['s_per_game'] = np.where(games > 0, sv_values / games, 0.0)
    
    # Add role classification for reference
    result_df['pitcher_role'] = 'unknown'
    result_df.loc[is_pure_starter, 'pitcher_role'] = 'starter'
    result_df.loc[is_pure_reliever, 'pitcher_role'] = 'reliever'
    result_df.loc[is_hybrid, 'pitcher_role'] = 'hybrid'
    
    # Add percentage of games as starter for hybrid pitchers
    result_df['start_percentage'] = np.where(games > 0, games_started / games, 0.0)
    
    return result_df

# Function to filter split datasets to only include players from main dataset
def filter_split_dataset(split_df, main_df, split_name_col, main_name_col):
    """Filter split dataset to only include players that exist in the main dataset"""
    # Normalize both name columns for comparison
    main_names = set(main_df[main_name_col].apply(normalize_name))
    split_df_normalized = split_df.copy()
    split_df_normalized[split_name_col] = split_df_normalized[split_name_col].apply(normalize_name)
    
    # Filter to only include players in main dataset
    filtered_df = split_df_normalized[split_df_normalized[split_name_col].isin(main_names)]
    
    original_count = len(split_df)
    filtered_count = len(filtered_df)
    logger.info(f"Filtered dataset from {original_count} to {filtered_count} players ({original_count - filtered_count} removed)")
    
    return filtered_df

# Process a dataset and create tables
def process_dataset(df, name_col, table_prefix, conn, col_map=None, is_pitcher=False):
    df[name_col] = df[name_col].apply(normalize_name)
    df.columns = df.columns.str.lower()
    
    # Apply column mapping to rename columns if col_map is provided
    if col_map:
        df = df.rename(columns={v: k for k, v in col_map.items()})  # Reverse the mapping (e.g., '1b' -> 'singles')
    
    
        
    # Convert mlbamid to string
    if 'xmlbamid' in df.columns:
        df['mlbamid'] = df['xmlbamid'].astype(str)
    
    try:
        with conn.begin():  # Transaction is managed here
            conn.execute(text(f'DROP TABLE IF EXISTS {table_prefix}_full_season'))
            conn.execute(text(f'DROP TABLE IF EXISTS {table_prefix}_per_game'))
            logger.info(f"Dropped tables {table_prefix}_full_season and {table_prefix}_per_game")
    except Exception as e:
        logger.error(f"Error dropping tables: {e}")
        raise
    
    try:
        logger.info(f"Creating {table_prefix}_full_season table...")
        df.to_sql(f'{table_prefix}_full_season', conn.engine, if_exists='replace', index=False,  method='multi', chunksize=500)
        logger.info(f"✅ {table_prefix}_full_season' inserted.")
    except Exception as e:
        logger.error(f"❌ Failed to write {table_prefix}_full_season': {str(e)}") 

    if is_pitcher:
        per_game_df = prorate_pitchers_vectorized(df, name_col, col_map)
    else:     
        per_game_df = prorate_hitters_vectorized(df, name_col, col_map)
    
    per_game_df.columns = per_game_df.columns.str.lower()
    # Convert mlbamid to string in per-game DataFrame
    if 'mlbamid' in per_game_df.columns:
        per_game_df['mlbamid'] = per_game_df['mlbamid'].astype(str)
    try:
        logger.info(f"Creating {table_prefix}_per_game table...")
        per_game_df.to_sql(f'{table_prefix}_per_game', conn.engine, if_exists='replace', index=False, method='multi', chunksize=500)
        logger.info(f"✅ {table_prefix}_per_game' inserted.")
    except Exception as e:
        logger.error(f"❌ Failed to write {table_prefix}_per_game': {str(e)}")    
    return (df, per_game_df)

# Create the database connection
conn = None
try:
    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    if conn is None:
        raise Exception("Failed to create database connection.")
    logger.info("Successfully connected to the database.")
    
    # Process standard hitters dataset FIRST to get the list of valid batters
    logger.info("=== PROCESSING STANDARD HITTERS DATASET ===")
    hitter_columns = check_csv_columns(hitter_file)
    if not hitter_columns:
        logger.error("Error with hitter file, exiting.")
        exit(1)
    
    hitter_name_col = determine_name_column(hitter_columns, "hitter")
    if not hitter_name_col:
        logger.error("ERROR: Could not identify name column in hitter CSV file.")
        exit(1)
        
    logger.info(f"Reading {hitter_file}...")
    hitters = pd.read_csv(hitter_file)
    hitters.columns = hitters.columns.str.lower()  # Convert CSV columns to lowercase
    
    # In the main script, update the hitter processing section
    hitter_col_map = {}
    required_hitter_cols = ['g', 'r', 'rbi', 'singles', 'doubles', 'triples', 'hr', 'bb', 'so', 'sb', 'cs', 'hbp']
    for req_col in required_hitter_cols:
        if req_col not in hitters.columns:
            if req_col == 'so' and 'k' in hitters.columns:
                hitter_col_map['so'] = 'k'
            elif req_col == 'singles' and '1b' in hitters.columns:
                hitter_col_map['singles'] = '1b'  # Map '1B' to 'single'
            elif req_col == 'doubles' and '2b' in hitters.columns:
                hitter_col_map['doubles'] = '2b'  # Map '2B' to 'double'
            elif req_col == 'triples' and '3b' in hitters.columns:
                hitter_col_map['triples'] = '3b'  # Map '3B' to 'triple'
            else:
                logger.info(f"WARNING: Missing required hitter column: {req_col}")
                
    hitters_result = process_dataset(hitters, hitter_name_col, "hitters", conn, hitter_col_map)
    
    # Process standard pitchers dataset
    logger.info("=== PROCESSING STANDARD PITCHERS DATASET ===")
    pitcher_columns = check_csv_columns(pitcher_file)
    if not pitcher_columns:
        logger.error("Error with pitcher file, exiting.")
        exit(1)
    
    pitcher_name_col = determine_name_column(pitcher_columns, "pitcher")
    if not pitcher_name_col:
        logger.error("ERROR: Could not identify name column in pitcher CSV file.")
        exit(1)
        
    logger.info(f"Reading {pitcher_file}...")
    pitchers = pd.read_csv(pitcher_file)
    pitchers.columns = pitchers.columns.str.lower()  # Convert CSV columns to lowercase
    
    pitcher_col_map = {}
    required_pitcher_cols = ['g', 'gs', 'ip', 'so', 'h', 'er', 'bb', 'hbp', 'w', 'r', 'sv', 'hld']
    for req_col in required_pitcher_cols:
        if req_col not in pitchers.columns:
            if req_col == 'so' and 'k' in pitchers.columns:
                pitcher_col_map['so'] = 'k'
            elif req_col == 'sv' and 's' in pitchers.columns:
                pitcher_col_map['sv'] = 's'
            else:
                logger.info(f"WARNING: Missing required pitcher column: {req_col}")
                
    pitchers_result = process_dataset(pitchers, pitcher_name_col, "pitchers", conn, pitcher_col_map, is_pitcher=True)
    
    # Process hitters vs RHP dataset (FILTERED)
    logger.info("=== PROCESSING HITTERS VS RHP DATASET ===")
    if os.path.exists(hitter_vs_rhp_file):
        hitter_vs_rhp_columns = check_csv_columns(hitter_vs_rhp_file)
        if not hitter_vs_rhp_columns:
            logger.error("Error with hitter vs RHP file, skipping.")
        else:
            hitter_vs_rhp_name_col = determine_name_column(hitter_vs_rhp_columns, "hitter")
            if not hitter_vs_rhp_name_col:
                logger.error("ERROR: Could not identify name column in hitter vs RHP CSV file, skipping.")
            else:
                logger.info(f"Reading {hitter_vs_rhp_file}...")
                hitters_vs_rhp_raw = pd.read_csv(hitter_vs_rhp_file)
                hitters_vs_rhp_raw.columns = hitters_vs_rhp_raw.columns.str.lower()  # Convert CSV columns to lowercase
                
                # FILTER: Only keep batters that exist in the main batters.csv
                logger.info("Filtering hitters vs RHP to only include batters from main dataset...")
                hitters_vs_rhp = filter_split_dataset(hitters_vs_rhp_raw, hitters, hitter_vs_rhp_name_col, hitter_name_col)
                
                hitter_vs_rhp_col_map = {}
                for req_col in required_hitter_cols:
                    if req_col not in hitters_vs_rhp.columns:
                        if req_col == 'so' and 'k' in hitters_vs_rhp.columns:
                            hitter_vs_rhp_col_map['so'] = 'k'
                        elif req_col == 'singles' and '1b' in hitters_vs_rhp.columns:
                            hitter_vs_rhp_col_map['singles'] = '1b'
                        elif req_col == 'doubles' and '2b' in hitters_vs_rhp.columns:
                            hitter_vs_rhp_col_map['doubles'] = '2b'
                        elif req_col == 'triples' and '3b' in hitters_vs_rhp.columns:
                            hitter_vs_rhp_col_map['triples'] = '3b'
                        else:
                            logger.error(f"WARNING: Missing required hitter vs RHP column: {req_col}")
                
                hitters_vs_rhp_result = process_dataset(hitters_vs_rhp, hitter_vs_rhp_name_col, "hitters_vs_rhp", conn, hitter_vs_rhp_col_map)
    else:
        logger.error(f"Hitter vs RHP file not found: {hitter_vs_rhp_file}")
    
    # Process hitters vs LHP dataset (FILTERED)
    logger.info("=== PROCESSING HITTERS VS LHP DATASET ===")
    if os.path.exists(hitter_vs_lhp_file):
        hitter_vs_lhp_columns = check_csv_columns(hitter_vs_lhp_file)
        if not hitter_vs_lhp_columns:
            logger.error("Error with hitter vs LHP file, skipping.")
        else:
            hitter_vs_lhp_name_col = determine_name_column(hitter_vs_lhp_columns, "hitter")
            if not hitter_vs_lhp_name_col:
                logger.error("ERROR: Could not identify name column in hitter vs LHP CSV file, skipping.")
            else:
                logger.info(f"Reading {hitter_vs_lhp_file}...")
                hitters_vs_lhp_raw = pd.read_csv(hitter_vs_lhp_file)
                hitters_vs_lhp_raw.columns = hitters_vs_lhp_raw.columns.str.lower()  # Convert CSV columns to lowercase
                
                # FILTER: Only keep batters that exist in the main batters.csv
                logger.info("Filtering hitters vs LHP to only include batters from main dataset...")
                hitters_vs_lhp = filter_split_dataset(hitters_vs_lhp_raw, hitters, hitter_vs_lhp_name_col, hitter_name_col)
                
                hitter_vs_lhp_col_map = {}
                for req_col in required_hitter_cols:
                    if req_col not in hitters_vs_lhp.columns:
                        if req_col == 'so' and 'k' in hitters_vs_lhp.columns:
                            hitter_vs_lhp_col_map['so'] = 'k'
                        elif req_col == 'singles' and '1b' in hitters_vs_lhp.columns:
                            hitter_vs_lhp_col_map['singles'] = '1b'
                        elif req_col == 'doubles' and '2b' in hitters_vs_lhp.columns:
                            hitter_vs_lhp_col_map['doubles'] = '2b'
                        elif req_col == 'triples' and '3b' in hitters_vs_lhp.columns:
                            hitter_vs_lhp_col_map['triples'] = '3b'
                        else:
                            logger.info(f"WARNING: Missing required hitter vs LHP column: {req_col}")
                
                hitters_vs_lhp_result = process_dataset(hitters_vs_lhp, hitter_vs_lhp_name_col, "hitters_vs_lhp", conn, hitter_vs_lhp_col_map)
    else:
        logger.info(f"Hitter vs LHP file not found: {hitter_vs_lhp_file}")
    
    logger.info("All projections have been freshly imported and generated in 'mlb_sorare.db'.")

except Exception as e:
    logger.error(f"An error occurred: {e}")
    if conn:
        conn.rollback()
finally:
    if 'conn' in locals() and conn:
        conn.close()