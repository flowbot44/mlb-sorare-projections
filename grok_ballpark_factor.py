import os
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from collections import defaultdict
from utils import (
    normalize_name, 
    determine_game_week, 
    
    get_platoon_start_side_by_mlbamid,
    get_db_connection,
    determine_daily_game_week
)
from ballpark_weather import (
    get_wind_effect,
    fetch_weather_and_store,
    get_temp_adjustment
)
import logging
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("grok_ballpark_factor")

SCORING_MATRIX = {
    'hitting': {'R': 3, 'RBI': 3, '1B': 2, '2B': 5, '3B': 8, 'HR': 10, 'BB': 2, 'K': -1, 'SB': 5, 'CS': -1, 'HBP': 2},
    'pitching': {'IP': 3, 'K': 2, 'H': -0.5, 'ER': -2, 'BB': -1, 'HBP': -1, 'W': 5, 'RA': 5, 'S': 10, 'HLD': 5 }
}
INJURY_STATUSES_OUT = ('Out', '10-Day-IL', '15-Day-IL', '60-Day-IL','suspension')
DAY_TO_DAY_STATUS = 'Day-To-Day'
DAY_TO_DAY_REDUCTION = 0.5

# --- Database Initialization ---
def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # Create or update tables - PostgreSQL syntax
    c.execute('''CREATE TABLE IF NOT EXISTS stadiums 
                 (id INTEGER PRIMARY KEY, name TEXT, lat REAL, lon REAL, orientation REAL, is_dome INTEGER)''')
    c.execute('DROP TABLE IF EXISTS games CASCADE')
    c.execute('''CREATE TABLE IF NOT EXISTS games 
                 (id INTEGER PRIMARY KEY, date TEXT, time TEXT, stadium_id INTEGER, 
                  home_team_id INTEGER, away_team_id INTEGER,
                  home_probable_pitcher_id TEXT, away_probable_pitcher_id TEXT, 
                  wind_effect_label TEXT, local_date TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS weather_forecasts 
                 (id SERIAL PRIMARY KEY, game_id INTEGER, 
                  wind_dir REAL, wind_speed REAL, temp REAL, rain REAL, timestamp TEXT)''')
    
    c.execute('DROP TABLE IF EXISTS adjusted_projections CASCADE')
    c.execute('''CREATE TABLE IF NOT EXISTS adjusted_projections 
                (id SERIAL PRIMARY KEY, player_name TEXT, mlbam_id TEXT,
                 game_id INTEGER, game_date TEXT, sorare_score REAL, team_id INTEGER, game_week TEXT)''')
    
    c.execute('DROP TABLE IF EXISTS player_teams CASCADE')
    c.execute('''CREATE TABLE IF NOT EXISTS player_teams 
             (id SERIAL PRIMARY KEY, player_id TEXT, 
              player_name TEXT, team_id INTEGER, mlbam_id TEXT)''')
    
    # Create new table for player handedness
    c.execute('''CREATE TABLE IF NOT EXISTS player_handedness 
                 (id SERIAL PRIMARY KEY, 
                  player_id TEXT, 
                  mlbam_id TEXT,
                  player_name TEXT, 
                  bats TEXT, 
                  throws TEXT,
                  last_updated TEXT)''')
 
    c.execute('''
        CREATE TABLE IF NOT EXISTS platoon_players (
            id SERIAL PRIMARY KEY, -- Changed from INTEGER PRIMARY KEY AUTOINCREMENT
            name TEXT NOT NULL,
            mlbam_id INTEGER NOT NULL UNIQUE, -- Added UNIQUE constraint
            starts_vs TEXT CHECK(starts_vs IN ('R', 'L'))
        )
    ''')

        # Ensure unique constraint for upsert logic
    c.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'unique_player_game'
            ) THEN
                ALTER TABLE adjusted_projections
                ADD CONSTRAINT unique_player_game UNIQUE (mlbam_id, game_id);
            END IF;
        END
        $$;
    """)

    
    # 2. stadiums (id is already PRIMARY KEY, but ensure unique)
    c.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'unique_stadium_id'
            ) THEN
                ALTER TABLE stadiums
                ADD CONSTRAINT unique_stadium_id UNIQUE (id);
            END IF;
        END
        $$;
    """)

    # 3. games (id is already PRIMARY KEY, but ensure unique)
    c.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'unique_game_id'
            ) THEN
                ALTER TABLE games
                ADD CONSTRAINT unique_game_id UNIQUE (id);
            END IF;
        END
        $$;
    """)

    # 4. player_teams (ensure unique mlbam_id per team)
    c.execute("""
        DELETE FROM player_teams a
        USING player_teams b
        WHERE a.ctid < b.ctid
          AND a.mlbam_id = b.mlbam_id
          AND a.team_id = b.team_id
    """)
    c.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'unique_player_team'
            ) THEN
                ALTER TABLE player_teams
                ADD CONSTRAINT unique_player_team UNIQUE (mlbam_id, team_id);
            END IF;
        END
        $$;
    """)

    # 5. player_handedness (ensure unique mlbam_id)
    c.execute("""
        DELETE FROM player_handedness a
        USING player_handedness b
        WHERE a.ctid < b.ctid
          AND a.mlbam_id = b.mlbam_id
    """)
    c.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'unique_player_handedness'
            ) THEN
                ALTER TABLE player_handedness
                ADD CONSTRAINT unique_player_handedness UNIQUE (mlbam_id);
            END IF;
        END
        $$;
    """)

    # Add indexes for performance
    c.execute("CREATE INDEX IF NOT EXISTS idx_player_teams_mlbam_id ON player_teams (mlbam_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_player_teams_team_id ON player_teams (team_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_games_local_date ON games (local_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_weather_forecasts_game_id ON weather_forecasts (game_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_park_factors_stadium_id ON park_factors (stadium_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_handedness_mlbam_id ON player_handedness (mlbam_id)")


    logger.info("Database initialized successfully.")
    
    conn.commit()
    return conn



# --- Schedule Functions ---
def get_schedule(conn, start_date, end_date):
    if not isinstance(start_date, str):
        start_date = start_date.strftime('%Y-%m-%d')
    if not isinstance(end_date, str):
        end_date = end_date.strftime('%Y-%m-%d')
    
    url = f"https://statsapi.mlb.com/api/v1/schedule?startDate={start_date}&endDate={end_date}&sportId=1&hydrate=probablePitcher"
    response = requests.get(url)
    data = response.json()
    c = conn.cursor()

    game_week_id = f"{start_date}_to_{end_date}"
    
    for date_data in data.get('dates', []):
        for game in date_data.get('games', []):
            game_id = game['gamePk']
            
            # Use officialDate as the local_date - this is MLB's official game date
            # regardless of time zone or when the game actually starts in UTC
            local_date = game['officialDate']
            
            # Split but preserve timezone information
            game_date_str = game['gameDate']
            date_parts = game_date_str.split('T')
            game_date = date_parts[0]  # This is the UTC date
            
            # Keep the full time including timezone indicator if present
            time_part = date_parts[1]
            if '.' in time_part:  # Handle milliseconds
                time_part = time_part.split('.')[0]
            
            # Check if timezone indicator exists and preserve it
            if time_part.endswith('Z'):
                game_time = time_part  # Keep the Z to indicate UTC
            else:
                # If no Z, but has timezone offset like +00:00
                for tzchar in ['+', '-']:
                    if tzchar in time_part:
                        tz_parts = time_part.split(tzchar)
                        game_time = f"{tz_parts[0]}Z"  # Simplify to UTC for storage
                        break
                else:
                    # No timezone indicator found, assume UTC
                    game_time = f"{time_part}Z"
            
            stadium_id = game['venue']['id']
            home_team_id = game['teams']['home']['team']['id']
            away_team_id = game['teams']['away']['team']['id']
            home_pitcher = game['teams']['home'].get('probablePitcher', {}).get('id', None)
            away_pitcher = game['teams']['away'].get('probablePitcher', {}).get('id', None)
            
            # PostgreSQL uses ON CONFLICT instead of INSERT OR IGNORE
            c.execute("""
                INSERT INTO stadiums (id, name) VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (stadium_id, game['venue']['name']))
            
            # Updated to include local_date column
            c.execute("""
                INSERT INTO games 
                (id, date, time, stadium_id, home_team_id, away_team_id, 
                 home_probable_pitcher_id, away_probable_pitcher_id, local_date) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    date = EXCLUDED.date,
                    time = EXCLUDED.time,
                    stadium_id = EXCLUDED.stadium_id,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    home_probable_pitcher_id = EXCLUDED.home_probable_pitcher_id,
                    away_probable_pitcher_id = EXCLUDED.away_probable_pitcher_id,
                    local_date = EXCLUDED.local_date
            """, (game_id, game_date, game_time, stadium_id, home_team_id, away_team_id,
                  str(home_pitcher) if home_pitcher else None, 
                  str(away_pitcher) if away_pitcher else None,
                  local_date))
    
    conn.commit()
    return game_week_id

def populate_player_teams(conn, start_date, end_date, update_rosters=False):
    c = conn.cursor()
    if not update_rosters:
        c.execute("SELECT COUNT(*) FROM player_teams")
        existing_count = c.fetchone()[0]
        if existing_count > 0:
            logger.info("Using cached roster data.")
            return
        logger.info("No cached roster data found; fetching rosters...")

    logger.info("Updating roster data...")
    c.execute("DELETE FROM player_teams")

    # Get all unique team IDs in one query
    c.execute("""
        SELECT DISTINCT team_id FROM (
            SELECT home_team_id AS team_id FROM games WHERE local_date BETWEEN %s AND %s
            UNION
            SELECT away_team_id AS team_id FROM games WHERE local_date BETWEEN %s AND %s
        ) AS all_teams
    """, (start_date, end_date, start_date, end_date))
    teams = [row[0] for row in c.fetchall()]

    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    player_teams_data = []
    handedness_data = []

    for team_id in teams:
        url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active&hydrate=person"
        try:
            response = requests.get(url)
            roster_data = response.json()
            for player in roster_data.get('roster', []):
                player_id = str(player['person']['id'])
                player_name = normalize_name(player['person']['fullName'])
                player_teams_data.append((player_id, player_name, team_id, player_id))

                if 'person' in player:
                    person_data = player['person']
                    bats = person_data.get('batSide', {}).get('code', 'Unknown')
                    throws = person_data.get('pitchHand', {}).get('code', 'Unknown')
                    handedness_data.append((player_id, player_id, player_name, bats, throws, current_date))
        except Exception as e:
            logger.error(f"Error fetching roster for team {team_id}: {e}")

    # Bulk insert player_teams
    if player_teams_data:
        args_str = ",".join(c.mogrify("(%s,%s,%s,%s)", row).decode() for row in player_teams_data)
        c.execute(f"""
            INSERT INTO player_teams (player_id, player_name, team_id, mlbam_id)
            VALUES {args_str}
            ON CONFLICT DO NOTHING
        """)

    # Bulk upsert handedness (Postgres 9.5+)
    if handedness_data:
        for row in handedness_data:
            c.execute("""
                INSERT INTO player_handedness
                (player_id, mlbam_id, player_name, bats, throws, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (mlbam_id) DO UPDATE
                SET bats = EXCLUDED.bats, throws = EXCLUDED.throws, last_updated = EXCLUDED.last_updated
            """, row)

    conn.commit()
    add_projected_starting_pitchers(conn, start_date, end_date)

# --- Adjustment Functions ---
def adjust_stats(stats, park_factors, is_dome, orientation, wind_dir, wind_speed, temp, is_pitcher=False):
    """Apply park and weather adjustments to player stats."""
    adjusted_stats = {}
    for stat, value in stats.items():
        park_adjustment = (1 / park_factors.get(stat, 1.0) if is_pitcher and stat in ['H', 'ER', 'BB', 'HR']
                           else park_factors.get(stat, 1.0))
        weather_factor = (1.0 if is_dome or stat != 'HR' 
                          else get_wind_effect(orientation or 0, wind_dir or 0, wind_speed or 0) * get_temp_adjustment(temp or 70))
        adjusted_stats[stat] = value * park_adjustment * weather_factor
    return adjusted_stats

def calculate_sorare_hitter_score(stats, scoring_matrix):
    score = 0
    score += stats.get('1B', 0) * scoring_matrix['hitting'].get('1B', 0)
    score += stats.get('2B', 0) * scoring_matrix['hitting'].get('2B', 0)
    score += stats.get('3B', 0) * scoring_matrix['hitting'].get('3B', 0)
    score += stats.get('HR', 0) * scoring_matrix['hitting'].get('HR', 0)
    score += stats.get('R', 0) * scoring_matrix['hitting'].get('R', 0)
    score += stats.get('RBI', 0) * scoring_matrix['hitting'].get('RBI', 0)
    score += stats.get('BB', 0) * scoring_matrix['hitting'].get('BB', 0)
    score += stats.get('K', 0) * scoring_matrix['hitting'].get('K', 0)
    score += stats.get('SB', 0) * scoring_matrix['hitting'].get('SB', 0)
    score += stats.get('CS', 0) * scoring_matrix['hitting'].get('CS', 0)
    score += stats.get('HBP', 0) * scoring_matrix['hitting'].get('HBP', 0)
    return score

def calculate_sorare_pitcher_score(stats, scoring_matrix):
    score = 0
    score += stats.get('IP', 0) * scoring_matrix['pitching'].get('IP', 0)
    score += stats.get('SO', 0) * scoring_matrix['pitching'].get('K', 0)
    score += stats.get('H', 0) * scoring_matrix['pitching'].get('H', 0)
    score += stats.get('ER', 0) * scoring_matrix['pitching'].get('ER', 0)
    score += stats.get('BB', 0) * scoring_matrix['pitching'].get('BB', 0)
    score += stats.get('HBP', 0) * scoring_matrix['pitching'].get('HBP', 0)
    score += stats.get('W', 0) * scoring_matrix['pitching'].get('W', 0)
    score += stats.get('HLD', 0) * scoring_matrix['pitching'].get('HLD', 0)
    score += stats.get('S', 0) * scoring_matrix['pitching'].get('S', 0)
    score += stats.get('RA', 0) * scoring_matrix['pitching'].get('RA', 0)
    return score

def adjust_score_for_injury(base_score, injury_status, return_estimate, game_date):
    """Adjust the Sorare score based on injury status and return estimate."""
    if not return_estimate or return_estimate == 'No estimated return date':
        return_estimate_date = None
    else:
        try:
            # Ensure game_date and return_estimate_date are date objects for comparison
            return_estimate_date = datetime.strptime(return_estimate, '%Y-%m-%d').date()
            if isinstance(game_date, datetime):
                game_date = game_date.date()
        except ValueError:
            logger.error(f"Warning: Invalid return date format '{return_estimate}', treating as None")
            return_estimate_date = None

    if injury_status in INJURY_STATUSES_OUT and (not return_estimate_date or game_date <= return_estimate_date):
        return 0.0
    if injury_status == DAY_TO_DAY_STATUS and return_estimate_date and game_date <= return_estimate_date:
        return base_score * DAY_TO_DAY_REDUCTION
    return base_score

def process_hitter(
    conn,
    game_data,
    hitter_data,
    injuries,
    game_week_id,
    stadium_weather_map,
    park_factors_map,
    platoon_map,
    handedness_map,
    pitcher_names,
    projections
):
    game_id, game_date, time, stadium_id, home_team_id, away_team_id, local_date = game_data
    game_date_obj = datetime.strptime(local_date, '%Y-%m-%d').date()

    player_name = normalize_name(hitter_data.get("name"))
    mlbam_id = hitter_data.get("mlbamid")
    if not player_name:
        logger.info(f"Warning: Null Name for hitter in game {game_id}")
        return

    player_team_id = hitter_data.get("teamid")
    if not player_team_id or player_team_id not in (home_team_id, away_team_id):
        return

    unique_player_key = mlbam_id if mlbam_id else f"{player_name}_{player_team_id}"

    # Use pre-fetched stadium and park factors
    if stadium_weather_map is None:
        stadium_weather_map = {}
    if stadium_weather_map is None:
        stadium_weather_map = {}
    stadium_data = stadium_weather_map.get(stadium_id, (0, 0, 0, 0, 70))
    park_factors = park_factors_map.get(stadium_id, {
        'R': 1.0, 'RBI': 1.0, '1B': 1.0, '2B': 1.0, '3B': 1.0, 'HR': 1.0,
        'BB': 1.0, 'K': 1.0, 'SB': 1.0, 'CS': 1.0, 'HBP': 1.0
    })
    is_dome, orientation, wind_dir, wind_speed, temp = stadium_data

    # Determine opposing pitcher and handedness
    opposing_pitcher_id = pitcher_names.get(away_team_id if player_team_id == home_team_id else home_team_id)
    pitcher_handedness = handedness_map.get(opposing_pitcher_id, {}).get('throws', 'Unknown')

    platoon_matchup = platoon_map.get(mlbam_id)

    # Select appropriate hitter stats table based on pitcher handedness
    base_stats = {
        'R': hitter_data.get('r_per_game', 0),
        'RBI': hitter_data.get('rbi_per_game', 0),
        '1B': hitter_data.get('singles_per_game', 0),
        '2B': hitter_data.get('doubles_per_game', 0),
        '3B': hitter_data.get('triples_per_game', 0),
        'HR': hitter_data.get('hr_per_game', 0),
        'BB': hitter_data.get('bb_per_game', 0),
        'K': hitter_data.get('k_per_game', 0),
        'SB': hitter_data.get('sb_per_game', 0),
        'CS': hitter_data.get('cs_per_game', 0),
        'HBP': hitter_data.get('hbp_per_game', 0)
    }

    if pitcher_handedness == 'L':
        # Use vs LHP stats if available
        base_stats.update({
            'R': hitter_data.get('r_per_game_vs_lhp', base_stats['R']),
            'RBI': hitter_data.get('rbi_per_game_vs_lhp', base_stats['RBI']),
            '1B': hitter_data.get('singles_per_game_vs_lhp', base_stats['1B']),
            '2B': hitter_data.get('doubles_per_game_vs_lhp', base_stats['2B']),
            '3B': hitter_data.get('triples_per_game_vs_lhp', base_stats['3B']),
            'HR': hitter_data.get('hr_per_game_vs_lhp', base_stats['HR']),
            'BB': hitter_data.get('bb_per_game_vs_lhp', base_stats['BB']),
            'K': hitter_data.get('k_per_game_vs_lhp', base_stats['K']),
            'SB': hitter_data.get('sb_per_game_vs_lhp', base_stats['SB']),
            'CS': hitter_data.get('cs_per_game_vs_lhp', base_stats['CS']),
            'HBP': hitter_data.get('hbp_per_game_vs_lhp', base_stats['HBP'])
        })
    elif pitcher_handedness == 'R':
        # Use vs RHP stats if available
        base_stats.update({
            'R': hitter_data.get('r_per_game_vs_rhp', base_stats['R']),
            'RBI': hitter_data.get('rbi_per_game_vs_rhp', base_stats['RBI']),
            '1B': hitter_data.get('singles_per_game_vs_rhp', base_stats['1B']),
            '2B': hitter_data.get('doubles_per_game_vs_rhp', base_stats['2B']),
            '3B': hitter_data.get('triples_per_game_vs_rhp', base_stats['3B']),
            'HR': hitter_data.get('hr_per_game_vs_rhp', base_stats['HR']),
            'BB': hitter_data.get('bb_per_game_vs_rhp', base_stats['BB']),
            'K': hitter_data.get('k_per_game_vs_rhp', base_stats['K']),
            'SB': hitter_data.get('sb_per_game_vs_rhp', base_stats['SB']),
            'CS': hitter_data.get('cs_per_game_vs_rhp', base_stats['CS']),
            'HBP': hitter_data.get('hbp_per_game_vs_rhp', base_stats['HBP'])
        })

    # Platoon adjustment
    platoon_adjustment = 1.0
    if platoon_matchup and pitcher_handedness and pitcher_handedness != platoon_matchup:
        platoon_adjustment = 0.25
        base_stats = {stat: value * platoon_adjustment for stat, value in base_stats.items()}
        logger.info(f"Applying platoon adjustment for {player_name}: {platoon_adjustment}x due to starter throws {pitcher_handedness} and batter starts vs {platoon_matchup} game {game_id}")

    # Apply park/weather adjustments
    adjusted_stats = adjust_stats(base_stats, park_factors, is_dome, orientation, wind_dir, wind_speed, temp)
    base_score = calculate_sorare_hitter_score(adjusted_stats, SCORING_MATRIX)
    fip_adjusted_score = apply_fip_adjustment(conn, game_id, player_team_id, base_score)
    handedness_adjusted_score = apply_handedness_matchup_adjustment(
        conn, game_id, mlbam_id, is_pitcher=False, base_score=fip_adjusted_score)
    injury_data = injuries.get(unique_player_key, injuries.get(player_name, {'status': 'Active', 'return_estimate': None}))
    final_score = adjust_score_for_injury(handedness_adjusted_score, injury_data['status'], injury_data['return_estimate'], game_date_obj)

    # Append to projections list
    projections.append((player_name, mlbam_id, game_id, local_date, final_score, game_week_id, player_team_id))

def process_pitcher(
    conn, 
    game_data, 
    pitcher_data, 
    injuries, 
    game_week_id, 
    is_starter=False, 
    stadium_weather_map=None,
    park_factors_map=None,
    projections=None
):
    if projections is None:
        projections = []
    game_id, game_date, time, stadium_id, home_team_id, away_team_id, local_date = game_data
    game_date_obj = datetime.strptime(local_date, '%Y-%m-%d').date()

    player_name = normalize_name(pitcher_data.get("name"))
    mlbam_id = pitcher_data.get("mlbamid")
    if not player_name:
        logger.info(f"Warning: Null Name for pitcher in game {game_id}")
        return

    player_team_id = pitcher_data.get("teamid")
    if not player_team_id or player_team_id not in (home_team_id, away_team_id):
        return

    unique_player_key = mlbam_id if mlbam_id else f"{player_name}_{player_team_id}"
    innings_per_game = pitcher_data.get('ip_per_game', 0)
    is_generally_starter = innings_per_game > 2.0

    # If pitcher is generally a starter but not starting, set score to 0
    if is_generally_starter and not is_starter:
        projections.append((player_name, mlbam_id, game_id, local_date, 0.0, game_week_id, player_team_id))
        return

    # Use pre-fetched stadium and park factors
    if stadium_weather_map is None:
        stadium_weather_map = {}
    if park_factors_map is None:
        park_factors_map = {}
    stadium_data = stadium_weather_map.get(stadium_id, (0, 0, 0, 0, 70))
    park_factors = park_factors_map.get(stadium_id, {
        'IP': 1.0, 'SO': 1.0, 'H': 1.0, 'ER': 1.0, 'BB': 1.0, 'HBP': 1.0, 'W': 1.0, 'SV': 1.0
    })
    is_dome, orientation, wind_dir, wind_speed, temp = stadium_data

    base_stats = {
        'IP': innings_per_game,
        'SO': pitcher_data.get('k_per_game', 0),
        'H': pitcher_data.get('h_per_game', 0),
        'ER': pitcher_data.get('er_per_game', 0),
        'BB': pitcher_data.get('bb_per_game', 0),
        'HBP': pitcher_data.get('hbp_per_game', 0),
        'W': pitcher_data.get('w_per_game', 0),
        'HLD': pitcher_data.get('hld_per_game', 0),
        'S': pitcher_data.get('s_per_game', 0),
        'RA': not is_starter
    }

    adjusted_stats = adjust_stats(base_stats, park_factors, is_dome, orientation, wind_dir, wind_speed, temp, is_pitcher=True)
    base_score = calculate_sorare_pitcher_score(adjusted_stats, SCORING_MATRIX)

    if not is_starter:
        base_score *= 0.4
    else:
        base_score = apply_handedness_matchup_adjustment(
            conn, game_id, mlbam_id, is_pitcher=True, base_score=base_score
        )

    injury_data = injuries.get(unique_player_key, injuries.get(player_name, {'status': 'Active', 'return_estimate': None}))
    final_score = adjust_score_for_injury(base_score, injury_data['status'], injury_data['return_estimate'], game_date_obj)

    # Append to projections list
    projections.append((player_name, mlbam_id, game_id, local_date, final_score, game_week_id, player_team_id))

def add_projected_starting_pitchers(conn, start_date, end_date):
    """
    Adds projected starting pitchers to the player_teams table even if they're not on active rosters yet.
    This will allow the system to use their existing projections from pitchers_per_game.
    Also captures their handedness information.
    """
    logger.info("Adding projected starting pitchers to player_teams...")
    c = conn.cursor()
    
    # Get games in the date range
    c.execute("""
        SELECT id, date, home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id 
        FROM games WHERE local_date BETWEEN %s AND %s
    """, (start_date, end_date))
    games = c.fetchall()
    
    pitcher_count = 0
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for game in games:
        game_id, game_date, home_team_id, away_team_id, home_pitcher_id, away_pitcher_id = game
        
        # Process home and away pitchers
        for pitcher_id, team_id in [(home_pitcher_id, home_team_id), (away_pitcher_id, away_team_id)]:
            if pitcher_id and pitcher_id != 'None':
                # Check if the pitcher is already in player_teams
                c.execute("SELECT COUNT(*) FROM player_teams WHERE player_id = %s", (pitcher_id,))
                existing = c.fetchone()[0]
                
                if existing == 0:
                    # Pitcher not in player_teams, fetch their details from MLB API with hydrate=person
                    try:
                        url = f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}?hydrate=person"
                        response = requests.get(url)
                        player_data = response.json()
                        
                        if 'people' in player_data and len(player_data['people']) > 0:
                            player = player_data['people'][0]
                            player_name = normalize_name(player['fullName'])
                            
                            # Check if this player exists in pitchers_per_game
                            c.execute("""
                                SELECT COUNT(*) FROM pitchers_per_game 
                                WHERE mlbamid = %s
                            """, (pitcher_id,))
                            pitcher_exists = c.fetchone()[0]
                            
                            if pitcher_exists > 0:
                                # Add to the player_teams table to connect them to their stats
                                c.execute("""
                                    INSERT INTO player_teams (player_id, player_name, team_id, mlbam_id)
                                    VALUES (%s, %s, %s, %s)
                                """, (str(pitcher_id), player_name, team_id, str(pitcher_id)))
                                
                                # Extract handedness data
                                bats = player.get('batSide', {}).get('code', 'Unknown')
                                throws = player.get('pitchHand', {}).get('code', 'Unknown')
                                
                                # Check if player already exists in handedness table
                                c.execute("SELECT id FROM player_handedness WHERE mlbam_id = %s", (pitcher_id,))
                                existing_hand = c.fetchone()
                                
                                if existing_hand:
                                    # Update existing record
                                    c.execute("""
                                        UPDATE player_handedness 
                                        SET bats = %s, throws = %s, last_updated = %s
                                        WHERE mlbam_id = %s
                                    """, (bats, throws, current_date, pitcher_id))
                                else:
                                    # Insert new record
                                    c.execute("""
                                        INSERT INTO player_handedness 
                                        (player_id, mlbam_id, player_name, bats, throws, last_updated)
                                        VALUES (%s, %s, %s, %s, %s, %s)
                                    """, (str(pitcher_id), str(pitcher_id), player_name, bats, throws, current_date))
                                
                                pitcher_count += 1
                                logger.info(f"Added projected starter: {player_name} (ID: {pitcher_id}) to team {team_id} - Throws: {throws}, Bats: {bats}")
                    except Exception as e:
                        logger.error(f"Error fetching pitcher {pitcher_id} data: {e}")
    
    conn.commit()
    logger.info(f"Added {pitcher_count} projected starting pitchers to player_teams")

# --- Updates to the main functions ---
def calculate_adjustments(conn, start_date, end_date, game_week_id):
    if not isinstance(start_date, str):
        start_date = start_date.strftime('%Y-%m-%d')
    if not isinstance(end_date, str):
        end_date = end_date.strftime('%Y-%m-%d')
    
    with conn.cursor() as c:
        # Prefetch all necessary data
        c.execute("SELECT mlbam_id, bats, throws FROM player_handedness")
        handedness_map = {row[0]: {'bats': row[1], 'throws': row[2]} for row in c.fetchall()}

        c.execute("SELECT mlbam_id, team_id FROM player_teams")
        team_map = {row[0]: row[1] for row in c.fetchall()}

        c.execute("SELECT stadium_id, factor_type, value FROM park_factors")
        park_factors_map = {}
        for stadium_id, factor_type, value in c.fetchall():
            park_factors_map.setdefault(stadium_id, {})[factor_type] = value / 100

        c.execute("""
            SELECT s.id, s.is_dome, s.orientation, w.wind_dir, w.wind_speed, w.temp
            FROM stadiums s
            LEFT JOIN weather_forecasts w ON w.game_id = s.id
        """)
        stadium_weather_map = {row[0]: row[1:] for row in c.fetchall()}

        c.execute("SELECT mlbam_id, starts_vs FROM platoon_players")
        platoon_map = {row[0]: row[1] for row in c.fetchall()}

        c.execute("""
            SELECT i.player_name, i.status, i.return_estimate, pt.team_id
            FROM injuries i
            LEFT JOIN player_teams pt ON i.player_name = pt.player_name
        """)
        injuries = {}
        for player_name, status, return_estimate, team_id in c.fetchall():
            injuries[player_name] = {'status': status, 'return_estimate': return_estimate}
            if team_id:
                injuries[f"{player_name}_{team_id}"] = {'status': status, 'return_estimate': return_estimate}

        # Fetch pitcher names for probable pitchers
        c.execute("""
            SELECT player_id, player_name FROM player_teams
            WHERE player_id IN (
                SELECT home_probable_pitcher_id FROM games WHERE local_date BETWEEN %s AND %s
                UNION
                SELECT away_probable_pitcher_id FROM games WHERE local_date BETWEEN %s AND %s
            )
        """, (start_date, end_date, start_date, end_date))
        pitcher_names = {row[0]: normalize_name(row[1]) for row in c.fetchall()}

        # Fetch games
        c.execute("""
            SELECT id, date, time, stadium_id, home_team_id, away_team_id, 
                   home_probable_pitcher_id, away_probable_pitcher_id, local_date
            FROM games WHERE local_date BETWEEN %s AND %s
        """, (start_date, end_date))
        games = c.fetchall()
        
        logger.info(f"Found {len(games)} games for projection processing between {start_date} and {end_date}")

        # Fetch all hitters and pitchers
        c.execute("""
            SELECT h.*, pt.team_id as teamid, pt.player_id 
            FROM hitters_per_game h
            LEFT JOIN player_teams pt ON h.mlbamid = pt.mlbam_id
            WHERE pt.team_id IN (
                SELECT home_team_id FROM games WHERE local_date BETWEEN %s AND %s
                UNION
                SELECT away_team_id FROM games WHERE local_date BETWEEN %s AND %s
            )
        """, (start_date, end_date, start_date, end_date))
        all_hitters = c.fetchall()
        hitter_columns = [col[0] for col in c.description]

        c.execute("""
            SELECT p.*, pt.team_id as teamid, pt.player_id 
            FROM pitchers_per_game p
            LEFT JOIN player_teams pt ON p.mlbamid = pt.mlbam_id
            WHERE pt.team_id IN (
                SELECT home_team_id FROM games WHERE local_date BETWEEN %s AND %s
                UNION
                SELECT away_team_id FROM games WHERE local_date BETWEEN %s AND %s
            )
        """, (start_date, end_date, start_date, end_date))
        all_pitchers = c.fetchall()
        pitcher_columns = [col[0] for col in c.description]

        # Group hitters and pitchers by team
        hitters_by_team = defaultdict(list)
        for hitter in all_hitters:
            hitter_dict = {hitter_columns[i]: hitter[i] for i in range(len(hitter_columns))}
            hitters_by_team[hitter_dict['teamid']].append(hitter_dict)

        pitchers_by_team = defaultdict(list)
        for pitcher in all_pitchers:
            pitcher_dict = {pitcher_columns[i]: pitcher[i] for i in range(len(pitcher_columns))}
            pitchers_by_team[pitcher_dict['teamid']].append(pitcher_dict)

        # Process games and collect projections
        projections = []
        for game in games:
            game_id, game_date, time, stadium_id, home_team_id, away_team_id, home_pitcher_id, away_pitcher_id, local_date = game
            game_data = game[:6] + (local_date,)

            # Process hitters
            for team_id in (home_team_id, away_team_id):
                for hitter_dict in hitters_by_team.get(team_id, []):
                    #if hitter_dict.get("mlbamid") == "660271":  # Skip Ohtani as hitter if needed
                    #    continue
                    process_hitter(
                        conn, game_data, hitter_dict, injuries, game_week_id,
                        stadium_weather_map, park_factors_map, platoon_map, handedness_map,
                        pitcher_names, projections
                    )

            # Process pitchers
            for team_id in (home_team_id, away_team_id):
                for pitcher_dict in pitchers_by_team.get(team_id, []):
                    if pitcher_dict.get("mlbamid") == "660271":  # Skip Ohtani as pitcher
                        continue
                    is_starter = pitcher_dict.get("mlbamid") in (home_pitcher_id, away_pitcher_id)
                    process_pitcher(
                        conn, game_data, pitcher_dict, injuries, game_week_id, is_starter,
                        stadium_weather_map, park_factors_map, projections
                    )

        # Bulk upsert projections
        if projections:
            args_str = ",".join(c.mogrify("(%s,%s,%s,%s,%s,%s,%s)", proj).decode() for proj in projections)
            c.execute(f"""
                INSERT INTO adjusted_projections 
                (player_name, mlbam_id, game_id, game_date, sorare_score, game_week, team_id)
                VALUES {args_str}
                ON CONFLICT (mlbam_id, game_id) DO UPDATE
                SET sorare_score = EXCLUDED.sorare_score, game_date = EXCLUDED.game_date
            """)

        conn.commit()
        logger.info("✅ Committed adjusted_projections to database.")


# --- Main Function ---
def main(update_rosters=False, specified_date=None, daily=False):
    try:
        # If no date is specified, use the current date in local timezone
        if specified_date is None:
            # Get current date in local timezone rather than UTC to ensure
            # correct determination of game week
            current_date = datetime.now().date()
        else:
            current_date = specified_date

        game_week_id = determine_game_week(current_date)  # Use the utils function
        start_date, end_date = game_week_id.split('_to_')  # Split the string for use
        if daily:
            game_week_id = determine_daily_game_week(current_date)  # Use the utils function
            if not isinstance(current_date, str):
                start_date = current_date.strftime('%Y-%m-%d')
                end_date = start_date
            

        
        logger.info(f"Processing game week: {start_date} to {end_date}")
        conn = init_db()
        
        get_schedule(conn, start_date, end_date)  # Still returns the same string
        fetch_weather_and_store(conn, start_date, end_date)
        populate_player_teams(conn, start_date, end_date, update_rosters=update_rosters)
        calculate_adjustments(conn, start_date, end_date, game_week_id)
        conn.close()
        logger.info(f"Projections adjusted for game week: {start_date} to {end_date}")
        logger.info(f"Game week ID: {game_week_id}")
    except Exception as e:
        logger.error(f"Error in populate projections function: {e}")

if __name__ == "__main__":
    main()

def get_player_handedness(conn, mlbam_id=None, player_name=None):
    """
    Retrieves handedness information for a player by MLBAM ID or name.
    
    Args:
        conn (psycopg2.Connection): Database connection
        mlbam_id (str, optional): Player's MLBAM ID
        player_name (str, optional): Player's name
        
    Returns:
        dict: Player handedness data containing 'bats' and 'throws' values
    """
    c = conn.cursor()
    
    if mlbam_id:
        c.execute("""
            SELECT bats, throws FROM player_handedness
            WHERE mlbam_id = %s
        """, (str(mlbam_id),))
        result = c.fetchone()
    elif player_name:
        normalized_name = normalize_name(player_name)
        c.execute("""
            SELECT bats, throws FROM player_handedness
            WHERE player_name = %s
        """, (normalized_name,))
        result = c.fetchone()
    else:
        return {'bats': 'Unknown', 'throws': 'Unknown'}
    
    if result:
        return {'bats': result[0], 'throws': result[1]}
    else:
        return {'bats': 'Unknown', 'throws': 'Unknown'}
    
def apply_handedness_matchup_adjustment(conn, game_id, player_mlbam_id, is_pitcher, base_score):
    """
    Applies a matchup adjustment based on batter-pitcher handedness matchup.
    
    Args:
        conn (psycopg2.Connection): Database connection
        game_id (int): Game ID
        player_mlbam_id (str): Player's MLBAM ID
        is_pitcher (bool): Whether the player is a pitcher
        base_score (float): Base score to adjust
        
    Returns:
        float: Adjusted score based on the handedness matchup
    """
    c = conn.cursor()
    
    # Get game information
    c.execute("""
        SELECT home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id 
        FROM games WHERE id = %s
    """, (game_id,))
    game_data = c.fetchone()
    
    if not game_data or not player_mlbam_id:
        return base_score
    
    home_team_id, away_team_id, home_pitcher_id, away_pitcher_id = game_data
    
    # Get player team and handedness
    c.execute("""
        SELECT team_id FROM player_teams WHERE mlbam_id = %s
    """, (player_mlbam_id,))
    player_data = c.fetchone()
    
    if not player_data:
        return base_score
    
    player_team_id = player_data[0]
    player_handedness = get_player_handedness(conn, mlbam_id=player_mlbam_id)
    
    # For pitchers
    if is_pitcher:
        # Determine if this is a home or away pitcher
        if player_team_id == home_team_id:
            opponent_team_id = away_team_id
        else:
            opponent_team_id = home_team_id
            
        # Get opposing team's batter handedness distribution
        batter_counts = {"L": 0, "R": 0, "S": 0}
        c.execute("""
            SELECT ph.mlbam_id, ph.bats 
            FROM player_handedness ph
            JOIN player_teams pt ON ph.mlbam_id = pt.mlbam_id
            WHERE pt.team_id = %s
        """, (opponent_team_id,))
        batters = c.fetchall()
        
        for _, bats in batters:
            if bats in batter_counts:
                batter_counts[bats] += 1
            else:
                batter_counts["Unknown"] = batter_counts.get("Unknown", 0) + 1
        
        # Calculate advantage based on pitcher's throwing hand and opponent's batting profile
        throws = player_handedness.get('throws', 'Unknown')
        if throws == 'L':
            # Left-handed pitchers typically fare better against left-handed batters
            lefty_ratio = batter_counts.get('L', 0) / max(sum(batter_counts.values()), 1)
            # Adjust score based on matchup quality
            if lefty_ratio > 0.4:  # Team has lots of lefty batters
                return base_score * 1.15
            elif lefty_ratio < 0.2:  # Team has few lefty batters
                return base_score * 0.95
        elif throws == 'R':
            # Right-handed pitchers typically fare better against right-handed batters
            righty_ratio = batter_counts.get('R', 0) / max(sum(batter_counts.values()), 1)
            # Adjust score based on matchup quality
            if righty_ratio > 0.7:  # Team has lots of righty batters
                return base_score * 1.05
            elif righty_ratio < 0.5:  # Team has few righty batters
                return base_score * 0.95
    
    # For batters
    else:
        # no need to apply handedness adjustment for batters as it is already handled with per_game stats
        return base_score
        
    return base_score

def apply_fip_adjustment(conn, game_id, hitter_team_id, base_score):
    """
    Applies a FIP adjustment to a base score based on the opposing pitcher's FIP.

    Args:
        conn (psycopg2.Connection): The database connection.
        game_id (int): The ID of the game.
        hitter_team_id (int): The ID of the hitting team.
        base_score (float): The base score to adjust.

    Returns:
        float: The adjusted score.
    """
    c = conn.cursor()
    # Identify opposing pitcher MLBAMID
    c.execute(
        "SELECT home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id FROM games WHERE id = %s",
        (game_id,)
    )
    result = c.fetchone()

    if not result:
        return base_score

    home_team_id, away_team_id, home_pitcher_id, away_pitcher_id = result
     # Determine which pitcher the hitter faces
    if hitter_team_id == home_team_id:
        opposing_pitcher_id = away_pitcher_id
    elif hitter_team_id == away_team_id:
        opposing_pitcher_id = home_pitcher_id
    else:
        return base_score  # Team mismatch

    if opposing_pitcher_id is None:
        return base_score  # No probable pitcher

    c.execute("SELECT fip FROM pitchers_full_season WHERE mlbamid = %s", (str(opposing_pitcher_id),))
    result = c.fetchone()

    if not result:
        return base_score  # No FIP available
    
    fip = result[0]
    if fip < 3.20:
        multiplier = 0.80
    elif fip < 3.50:
        multiplier = 0.90
    elif fip < 3.80:
        multiplier = 0.95
    elif fip < 4.20:
        multiplier = 1.00
    elif fip < 4.40:
        multiplier = 1.05
    elif fip < 4.70:
        multiplier = 1.10
    elif fip < 5.00:
        multiplier = 1.15
    else:
        multiplier = 1.20

    return base_score * multiplier