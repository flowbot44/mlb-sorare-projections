import os
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import pytz
from utils import (
    normalize_name, 
    determine_game_week, 
    get_wind_effect,
    get_wind_effect_label,
    get_temp_adjustment,
    wind_dir_to_degrees,
    get_platoon_start_side_by_mlbamid,
    get_db_connection,
    determine_daily_game_week
)
import math
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
    
    teams = set()
    c.execute("SELECT home_team_id, away_team_id FROM games WHERE local_date BETWEEN %s AND %s",
              (start_date, end_date))
    games = c.fetchall()
    for home_team_id, away_team_id in games:
        teams.add(home_team_id)
        teams.add(away_team_id)
    
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for team_id in teams:
        # Use hydrate=person to get detailed player information including handedness
        url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active&hydrate=person"
        try:
            response = requests.get(url)
            roster_data = response.json()
            for player in roster_data.get('roster', []):
                player_id = str(player['person']['id'])  # This is the MLBAMID
                player_name = normalize_name(player['person']['fullName'])
                
                # Insert into player_teams table
                c.execute("""
                    INSERT INTO player_teams (player_id, player_name, team_id, mlbam_id) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (player_id, player_name, team_id, player_id))
                
                # Extract handedness data
                if 'person' in player:
                    person_data = player['person']
                    bats = person_data.get('batSide', {}).get('code', 'Unknown')
                    throws = person_data.get('pitchHand', {}).get('code', 'Unknown')
                    
                    # Check if player already exists in handedness table
                    c.execute("SELECT id FROM player_handedness WHERE mlbam_id = %s", (player_id,))
                    existing = c.fetchone()
                    
                    if existing:
                        # Update existing record
                        c.execute("""
                            UPDATE player_handedness 
                            SET bats = %s, throws = %s, last_updated = %s
                            WHERE mlbam_id = %s
                        """, (bats, throws, current_date, player_id))
                    else:
                        # Insert new record
                        c.execute("""
                            INSERT INTO player_handedness 
                            (player_id, mlbam_id, player_name, bats, throws, last_updated)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (player_id, player_id, player_name, bats, throws, current_date))
        except Exception as e:
            logger.error(f"Error fetching roster for team {team_id}: {e}")
    
    conn.commit()
    add_projected_starting_pitchers(conn, start_date, end_date)

# --- Weather Functions ---
def get_weather_nws(lat, lon, forecast_time):
    """
    Fetches weather data from the National Weather Service API, averaging conditions over a 3-hour game period.
    Returns average wind, temperature and precipitation probability for the entire game duration.
    """
    try:
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            logger.info(f"Invalid coordinates: lat={lat}, lon={lon}")
            return None
        
        # Ensure the forecast_time has a timezone (should be UTC)
        if forecast_time.tzinfo is None:
            forecast_time = forecast_time.replace(tzinfo=pytz.utc)
            
        # Define game duration (3 hours)
        game_end_time = forecast_time + timedelta(hours=3)

        points_url = f"https://api.weather.gov/points/{lat},{lon}"
        try:
            points_response = requests.get(points_url)
            points_response.raise_for_status()
            points_data = points_response.json()

            if 'properties' not in points_data or 'forecastHourly' not in points_data['properties']:
                logger.info(f"Invalid points data structure: {points_data}")
                return None

            forecast_hourly_url = points_data['properties']['forecastHourly']
            forecast_response = requests.get(forecast_hourly_url)
            forecast_response.raise_for_status()
            forecast_data = forecast_response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 503 and "ForecastMissingData" in e.response.text:
                # Handle missing forecast data specifically
                error_details = e.response.json() if e.response.text else {"detail": "Unknown error"}
                logger.error(f"NWS API missing forecast data: {error_details.get('detail', 'No details provided')}")
                logger.error(f"This is normal for dates far in the future or certain regions.")
                return {
                    'temp': 70,  # Default temperature
                    'wind_speed': 5,  # Light wind
                    'wind_dir': 0,  # North
                    'rain': 0,  # No rain
                }
            raise  # Re-raise for other HTTP errors

        if 'properties' not in forecast_data or 'periods' not in forecast_data['properties']:
            logger.info(f"Invalid forecast data structure: {forecast_data}")
            return None

        periods = forecast_data['properties']['periods']
        if not periods:
            logger.info("No forecast periods available.")
            return None

        # Collect all forecasts within the game time window
        relevant_forecasts = []
        for period in periods:
            try:
                # Parse times from API ensuring proper timezone handling
                start_time = datetime.fromisoformat(period['startTime'].replace('Z', '+00:00'))
                end_time = datetime.fromisoformat(period['endTime'].replace('Z', '+00:00'))
                
                # Check if this period overlaps with the game time
                if (start_time <= game_end_time and end_time >= forecast_time):
                    # Calculate the overlap duration for weighted averaging
                    overlap_start = max(start_time, forecast_time)
                    overlap_end = min(end_time, game_end_time)
                    overlap_hours = (overlap_end - overlap_start).total_seconds() / 3600
                    
                    try:
                        wind_speed = int(period['windSpeed'].split()[0])
                    except (ValueError, IndexError, KeyError):
                        wind_speed = 0
                        
                    forecast = {
                        'temp': period.get('temperature', 70),
                        'wind_speed': wind_speed,
                        'wind_dir': wind_dir_to_degrees(period.get('windDirection', 'N')),
                        'rain': period.get('probabilityOfPrecipitation', {}).get('value', 0),
                        'weight': overlap_hours  # Weight by hours of overlap
                    }
                    relevant_forecasts.append(forecast)
            except (KeyError, ValueError) as e:
                logger.error(f"Error parsing period times: {e} in period: {period}")
                continue

        if not relevant_forecasts:
            logger.info(f"No forecast periods found overlapping with game time {forecast_time} to {game_end_time}")
            return None
            
        # Calculate weighted averages
        total_weight = sum(f['weight'] for f in relevant_forecasts)
        if total_weight == 0:
            return None
            
        avg_temp = sum(f['temp'] * f['weight'] for f in relevant_forecasts) / total_weight
        avg_rain = sum(f['rain'] * f['weight'] for f in relevant_forecasts) / total_weight
        
        # For wind speed and direction, we need to handle vector averaging
        # Convert to vectors and then back to speed/direction
        wind_x = sum(f['wind_speed'] * math.cos(math.radians(f['wind_dir'])) * f['weight'] for f in relevant_forecasts) / total_weight
        wind_y = sum(f['wind_speed'] * math.sin(math.radians(f['wind_dir'])) * f['weight'] for f in relevant_forecasts) / total_weight
        
        avg_wind_speed = math.sqrt(wind_x**2 + wind_y**2)
        avg_wind_dir = math.degrees(math.atan2(wind_y, wind_x)) % 360
        
        # Construct and return the averaged weather data
        weather = {
            'temp': round(avg_temp),
            'wind_speed': round(avg_wind_speed),
            'wind_dir': round(avg_wind_dir),
            'rain': round(avg_rain),
        }
        
        return weather
        
    except requests.exceptions.RequestException as e:
        logger.error(f"NWS API Request Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response content: {e.response.text[:500]}")  # Print first 500 chars of response
        return None
    except Exception as e:
        logger.error(f"Unexpected error in get_weather_nws: {e}")
        return None

def fetch_weather_and_store(conn, start_date, end_date):
    c = conn.cursor()
    
    # First, check if the timestamp column exists in weather_forecasts table
    c.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'weather_forecasts' AND column_name = 'timestamp'
    """)
    if not c.fetchone():
        # The timestamp column doesn't exist, so add it
        c.execute("ALTER TABLE weather_forecasts ADD COLUMN timestamp TEXT")
        conn.commit()
        logger.info("Added timestamp column to weather_forecasts table")
    
    # Use local_date for filtering instead of date
    c.execute("""
        SELECT id, date, time, stadium_id FROM games 
        WHERE local_date BETWEEN %s AND %s
    """, (start_date, end_date))
    games = c.fetchall()
    
    logger.info(f"Found {len(games)} games for weather processing between {start_date} and {end_date}")
    
    # Always use pytz.utc for current_time to ensure proper timezone handling
    current_time = datetime.now(pytz.utc)
    skipped_past_games = 0
    updated_forecasts = 0
    
    # Cache timeout in hours
    cache_timeout_hours = 1

    for game in games:
        game_id, date, time, stadium_id = game
        
        # Check if this game already has weather data and when it was last updated
        c.execute("""
            SELECT wind_dir, wind_speed, temp, rain, timestamp 
            FROM weather_forecasts 
            WHERE game_id = %s
        """, (game_id,))
        weather_data = c.fetchone()
        
        needs_update = True
        
        if weather_data:
            # Check if the timestamp is within our cache window
            if weather_data[4]:  # If timestamp exists
                try:
                    # Parse timestamp and ensure it has UTC timezone
                    last_update = datetime.strptime(weather_data[4], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.utc)
                    time_diff = current_time - last_update
                    
                    if time_diff.total_seconds() < cache_timeout_hours * 3600:
                        needs_update = False
                        continue  # Skip this game, forecast is recent enough
                    else:
                        logger.info(f"Weather forecast for game {game_id} is {time_diff.total_seconds() / 3600:.1f} hours old, refreshing...")
                except (ValueError, TypeError):
                    # If timestamp is invalid, update the forecast
                    logger.error(f"Invalid timestamp for game {game_id}, refreshing forecast...")
        
        c.execute("SELECT lat, lon, is_dome, orientation FROM stadiums WHERE id = %s", (stadium_id,))
        stadium = c.fetchone()
        if not stadium or stadium[2]:  # Skip if dome or no stadium data
            continue
            
        lat, lon, is_dome, orientation = stadium
        if lat is None or lon is None:
            logger.info(f"Skipping game https://baseballsavant.mlb.com/preview?game_pk={game_id} due to missing stadium {stadium_id} coordinates.")
            continue

        # Parse game time to UTC
        # We should now expect times to have the Z indicator since we preserved it in get_schedule
        try:
            if time.endswith('Z'):
                # Time already has UTC indicator
                utc_time = datetime.fromisoformat(f"{date}T{time[:-1]}+00:00")
            else:
                # Fallback for any times without Z - assume Eastern Time as before
                local_tz = pytz.timezone('America/New_York')
                local_time = datetime.strptime(f"{date}T{time}", "%Y-%m-%dT%H:%M:%S")
                local_time = local_tz.localize(local_time, is_dst=None)  # Handle DST properly
                utc_time = local_time.astimezone(pytz.utc)
                logger.info(f"Warning: Game {game_id} has no timezone indicator. Assuming Eastern Time.")
        except ValueError as e:
            logger.error(f"Error parsing time for game {game_id}: {e}")
            continue

        # Skip games that have already started or occurred in the past
        if utc_time <= current_time:
            skipped_past_games += 1
            logger.info(f"⚠️ Skipping forecast for game {game_id} that has already started or occurred ({utc_time.strftime('%Y-%m-%d %H:%M:%S')})")
            continue
            
        # Skip games too far in the future (> 7 days)
        if utc_time > current_time + timedelta(days=7):
            logger.info(f"⚠️ Skipping forecast too far in the future: {utc_time.strftime('%Y-%m-%d %H:%M:%S')} game id {game_id}")
            continue

        # Get and store the weather forecast
        weather = get_weather_nws(lat, lon, utc_time)
        if weather is not None:
            wind_effect_label = get_wind_effect_label(orientation, weather['wind_dir'])
            # Format current_time as string for database storage
            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
            
            if needs_update and weather_data:
                # Update existing forecast
                c.execute("""
                    UPDATE weather_forecasts 
                    SET wind_dir = %s, wind_speed = %s, temp = %s, rain = %s, timestamp = %s
                    WHERE game_id = %s
                """, (weather['wind_dir'], weather['wind_speed'], weather['temp'], 
                     weather['rain'], timestamp, game_id))
                updated_forecasts += 1
            else:
                # Insert new forecast
                c.execute("""
                    INSERT INTO weather_forecasts 
                    (game_id, wind_dir, wind_speed, temp, rain, timestamp) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (game_id, weather['wind_dir'], weather['wind_speed'], 
                     weather['temp'], weather['rain'], timestamp))
            
            c.execute("""
                UPDATE games SET wind_effect_label = %s WHERE id = %s
            """, (wind_effect_label, game_id))
        else:
            logger.info(f"Skipping game {game_id} due to API error.")

    if skipped_past_games > 0:
        logger.info(f"Skipped weather lookup for {skipped_past_games} past games")
    if updated_forecasts > 0:
        logger.info(f"Updated {updated_forecasts} weather forecasts that were older than {cache_timeout_hours} hour(s)")
    conn.commit()
    logger.info("Weather data processing complete.")

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

def process_hitter(conn, game_data, hitter_data, injuries, game_week_id):
    # Unpack game_data with local_date
    game_id, game_date, time, stadium_id, home_team_id, away_team_id, local_date = game_data
    
    # Use local_date instead of game_date for game date object
    game_date_obj = datetime.strptime(local_date, '%Y-%m-%d').date()

    player_name = normalize_name(hitter_data.get("name"))
    mlbam_id = hitter_data.get("mlbamid")
    if not player_name:
        logger.info(f"Warning: Null Name for hitter in game {game_id}")
        return

    player_team_id = hitter_data.get("teamid")
    if not player_team_id and mlbam_id:
        # Look up team ID by MLBAMID if not directly available
        c = conn.cursor()
        c.execute("SELECT team_id FROM player_teams WHERE mlbam_id = %s", (mlbam_id,))
        team_result = c.fetchone()
        if team_result:
            player_team_id = team_result[0]
    
    if not player_team_id:
        #logger.info(f"â� ï¸� Skipping {player_name} â�� no team assigned.")
        return
    if player_team_id not in (home_team_id, away_team_id):
        return

    # Create a unique player identifier with MLBAMID if available
    unique_player_key = mlbam_id if mlbam_id else f"{player_name}_{player_team_id}"
    
    c = conn.cursor()
    c.execute("""
        SELECT s.is_dome, s.orientation, w.wind_dir, w.wind_speed, w.temp 
        FROM stadiums s 
        LEFT JOIN weather_forecasts w ON w.game_id = %s
        WHERE s.id = %s
    """, (game_id, stadium_id))
    stadium_data = c.fetchone()

    if not stadium_data:
        is_dome, orientation, wind_dir, wind_speed, temp = (0, 0, 0, 0, 70)
    else:
        is_dome, orientation, wind_dir, wind_speed, temp = stadium_data

    c.execute("SELECT factor_type, value FROM park_factors WHERE stadium_id = %s", (stadium_id,))
    park_factor_rows = c.fetchall()
    park_factors = {row[0]: row[1] / 100 for row in park_factor_rows}
    
    if not park_factors:
        logger.info(f"park not found {stadium_id}")
        park_factors = {'R': 1.0, 'RBI': 1.0, '1B': 1.0, '2B': 1.0, '3B': 1.0, 'HR': 1.0, 'BB': 1.0, 'K': 1.0, 'SB': 1.0, 'CS': 1.0, 'HBP': 1.0}

    # --- NEW: Determine opposing pitcher and their handedness ---
    # Get game information to find opposing pitcher
    c.execute("""
        SELECT home_probable_pitcher_id, away_probable_pitcher_id
        FROM games WHERE id = %s
    """, (game_id,))
    game_info = c.fetchone()

    opposing_pitcher_id = None
    if game_info:
        home_pitcher_id, away_pitcher_id = game_info
        opposing_pitcher_id = away_pitcher_id if player_team_id == home_team_id else home_pitcher_id

    pitcher_handedness = None
    if opposing_pitcher_id:
        pitcher_handedness_data = get_player_handedness(conn, mlbam_id=opposing_pitcher_id)
        pitcher_handedness = pitcher_handedness_data.get('throws', 'Unknown')

    platoon_matchup = get_platoon_start_side_by_mlbamid(mlbam_id=mlbam_id)

    # --- NEW: Select appropriate hitter stats table based on pitcher handedness ---
    base_stats = {
        'R': 0, 'RBI': 0, '1B': 0, '2B': 0, '3B': 0, 'HR': 0,
        'BB': 0, 'K': 0, 'SB': 0, 'CS': 0, 'HBP': 0
    }

    if pitcher_handedness == 'L':
        # Use hitters_vs_lhp_per_game
        c.execute("""
            SELECT r_per_game, rbi_per_game, singles_per_game, doubles_per_game, triples_per_game, hr_per_game,
                   bb_per_game, k_per_game, sb_per_game, cs_per_game, hbp_per_game
            FROM hitters_vs_lhp_per_game
            WHERE mlbamid = %s OR name = %s
        """, (mlbam_id, player_name))
        hitter_specific = c.fetchone()
    elif pitcher_handedness == 'R':
        # Use hitters_vs_rhp_per_game
        c.execute("""
            SELECT r_per_game, rbi_per_game, singles_per_game, doubles_per_game, triples_per_game, hr_per_game,
                   bb_per_game, k_per_game, sb_per_game, cs_per_game, hbp_per_game
            FROM hitters_vs_rhp_per_game
            WHERE mlbamid = %s OR name = %s
        """, (mlbam_id, player_name))
        hitter_specific = c.fetchone()
    else:
        # Fallback to hitters_per_game
        c.execute("""
            SELECT r_per_game, rbi_per_game, singles_per_game, doubles_per_game, triples_per_game, hr_per_game,
                   bb_per_game, k_per_game, sb_per_game, cs_per_game, hbp_per_game
            FROM hitters_per_game
            WHERE mlbamid = %s OR name = %s
        """, (mlbam_id, player_name))
        hitter_specific = c.fetchone()

    # Populate base_stats with data from the selected table
    if hitter_specific:
        base_stats.update({
            'R': hitter_specific[0] or 0,
            'RBI': hitter_specific[1] or 0,
            '1B': hitter_specific[2] or 0,
            '2B': hitter_specific[3] or 0,
            '3B': hitter_specific[4] or 0,
            'HR': hitter_specific[5] or 0,
            'BB': hitter_specific[6] or 0,
            'K': hitter_specific[7] or 0,
            'SB': hitter_specific[8] or 0,
            'CS': hitter_specific[9] or 0,
            'HBP': hitter_specific[10] or 0
        })


# --- NEW: Adjust stats for platoon players facing same-handed pitchers ---
    platoon_adjustment = 1.0
    if platoon_matchup and pitcher_handedness and pitcher_handedness != platoon_matchup:
        platoon_adjustment = 0.25  # Reduce stats by 25% for same-handed matchups
        base_stats = {stat: value * platoon_adjustment for stat, value in base_stats.items()}
        logger.info(f"Applying platoon adjustment for {player_name}: {platoon_adjustment}x due to starter throws {pitcher_handedness} and batter starts vs {platoon_matchup} game {game_id}")

    # Apply platoon adjustment to base stats 
    adjusted_stats = adjust_stats(base_stats, park_factors, is_dome, orientation, wind_dir, wind_speed, temp)
    base_score = calculate_sorare_hitter_score(adjusted_stats, SCORING_MATRIX)
    fip_adjusted_score = apply_fip_adjustment(conn, game_id, player_team_id, base_score)
    # --- MODIFIED: Pass pitcher_handedness to apply_handedness_matchup_adjustment ---
    handedness_adjusted_score = apply_handedness_matchup_adjustment(
        conn, game_id, mlbam_id, is_pitcher=False, base_score=fip_adjusted_score)
    injury_data = injuries.get(unique_player_key, injuries.get(player_name, {'status': 'Active', 'return_estimate': None}))    
    final_score = adjust_score_for_injury(handedness_adjusted_score, injury_data['status'], injury_data['return_estimate'], game_date_obj)

    c.execute("""
        SELECT id FROM adjusted_projections 
        WHERE (player_name = %s AND game_id = %s AND team_id = %s) OR
            (mlbam_id = %s AND game_id = %s)
    """, (player_name, game_id, player_team_id, mlbam_id, game_id))
    existing = c.fetchone()
   # logging.info(f"existing {existing} player {player_name} with score {final_score}")
            
    try:
        if existing:
           # logging.info(f"update Adding new projection for {player_name} with score {final_score}")
            
            c.execute("""
                UPDATE adjusted_projections 
                SET sorare_score = %s, game_date = %s
                WHERE (player_name = %s AND game_id = %s AND team_id = %s) OR
                    (mlbam_id = %s AND game_id = %s)
            """, (final_score, local_date, player_name, game_id, player_team_id, mlbam_id, game_id))
        else:
           # logging.info(f"insert Adding new projection for {player_name} with score {final_score}")
            c.execute("""
                INSERT INTO adjusted_projections 
                (player_name, mlbam_id, game_id, game_date, sorare_score, game_week, team_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (player_name, mlbam_id, game_id, local_date, final_score, game_week_id, player_team_id))
    except Exception as e:
            logger.error(f"INSERT FAILED for {player_name} — {e}")

def process_pitcher(conn, game_data, pitcher_data, injuries, game_week_id, is_starter=False):
    # Unpack game_data with local_date
    game_id, game_date, time, stadium_id, home_team_id, away_team_id, local_date = game_data
    
    # Use local_date instead of game_date for game date object
    game_date_obj = datetime.strptime(local_date, '%Y-%m-%d').date()

    player_name = normalize_name(pitcher_data.get("name"))
    mlbam_id = pitcher_data.get("mlbamid")
    
    if not player_name:
        logger.info(f"Warning: Null Name for pitcher in game {game_id}")
        return

    player_team_id = pitcher_data.get("teamid")
    if not player_team_id and mlbam_id:
        # Look up team ID by MLBAMID if not directly available
        c = conn.cursor()
        c.execute("SELECT team_id FROM player_teams WHERE mlbam_id = %s", (mlbam_id,))
        team_result = c.fetchone()
        if team_result:
            player_team_id = team_result[0]
            
    if not player_team_id:
        logger.info(f"⚠️ Skipping {player_name} — no team assigned.")
        return
    if player_team_id not in (home_team_id, away_team_id):
        return
    
    # Create a unique player identifier with MLBAMID if available
    unique_player_key = mlbam_id if mlbam_id else f"{player_name}_{player_team_id}"
    
    # Determine if pitcher is generally a starter (projects to 2+ innings per game)
    innings_per_game = pitcher_data.get('ip_per_game', 0)
    is_generally_starter = innings_per_game > 2.0
    
    # If pitcher is generally a starter but not starting in this game, set score to 0
    if is_generally_starter and not is_starter:
        c = conn.cursor()
        
        # Check if this player already has a projection for this game
        c.execute("""
            SELECT id FROM adjusted_projections 
            WHERE (player_name = %s AND game_id = %s AND team_id = %s) OR
                (mlbam_id = %s AND game_id = %s)
        """, (player_name, game_id, player_team_id, mlbam_id, game_id))
        existing = c.fetchone()
        final_score = 0.0
        if existing:
            c.execute("""
                UPDATE adjusted_projections 
                SET sorare_score = %s, game_date = %s
                WHERE (player_name = %s AND game_id = %s AND team_id = %s) OR
                    (mlbam_id = %s AND game_id = %s)
            """, (final_score, local_date, player_name, game_id, player_team_id, mlbam_id, game_id))
        else:
            c.execute("""
                INSERT INTO adjusted_projections 
                (player_name, mlbam_id, game_id, game_date, sorare_score, game_week, team_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (player_name, mlbam_id, game_id, local_date, final_score, game_week_id, player_team_id))
        return
    
    c = conn.cursor()
    c.execute("""
        SELECT s.is_dome, s.orientation, w.wind_dir, w.wind_speed, w.temp 
        FROM stadiums s 
        LEFT JOIN weather_forecasts w ON w.game_id = %s
        WHERE s.id = %s
    """, (game_id, stadium_id))
    stadium_data = c.fetchone()
    
    if not stadium_data:
        logger.info(f"No stadium data for game {game_id} with stadium_id {stadium_id}")
        is_dome, orientation, wind_dir, wind_speed, temp = (0, 0, 0, 0, 70)
    else:
        is_dome, orientation, wind_dir, wind_speed, temp = stadium_data
    
    c.execute("SELECT factor_type, value FROM park_factors WHERE stadium_id = %s", (stadium_id,))
    park_factor_rows = c.fetchall()
    park_factors = {row[0]: row[1] / 100 for row in park_factor_rows}
    if not park_factors:
        logger.info(f"No park factors for stadium_id {stadium_id}, using default 1.0")
        park_factors = {'IP': 1.0, 'SO': 1.0, 'H': 1.0, 'ER': 1.0, 'BB': 1.0, 'HBP': 1.0, 'W': 1.0, 'SV': 1.0}
    
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
        # If not a starter, apply a reduction to the score as they don't pitch every game
        base_score *= 0.4
    else:
        base_score = apply_handedness_matchup_adjustment(
            conn, game_id, mlbam_id, is_pitcher=True, base_score=base_score
        )

    # Try to find injury data with the unique key first, then fall back to just the name
    injury_data = injuries.get(unique_player_key, injuries.get(player_name, {'status': 'Active', 'return_estimate': None}))
    final_score = adjust_score_for_injury(base_score, injury_data['status'], injury_data['return_estimate'], game_date_obj)

    # Check if this player already has a projection for this game
    c.execute("""
        SELECT id FROM adjusted_projections 
        WHERE (player_name = %s AND game_id = %s AND team_id = %s) OR
            (mlbam_id = %s AND game_id = %s)
    """, (player_name, game_id, player_team_id, mlbam_id, game_id))
    existing = c.fetchone()

    try:
        if existing:
            c.execute("""
                UPDATE adjusted_projections 
                SET sorare_score = %s, game_date = %s
                WHERE (player_name = %s AND game_id = %s AND team_id = %s) OR
                    (mlbam_id = %s AND game_id = %s)
            """, (final_score, local_date, player_name, game_id, player_team_id, mlbam_id, game_id))
        else:
            c.execute("""
                INSERT INTO adjusted_projections 
                (player_name, mlbam_id, game_id, game_date, sorare_score, game_week, team_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (player_name, mlbam_id, game_id, local_date, final_score, game_week_id, player_team_id))
    except Exception as e:
        logger.exception(f"S INSERT FAILED for {player_name} — {e}")

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
    
    c = conn.cursor()
    # Create injury lookup as before
    c.execute("SELECT player_name, status, return_estimate FROM injuries")
    injury_rows = c.fetchall()
    basic_injuries = {row[0]: {'status': row[1], 'return_estimate': row[2]} for row in injury_rows}
    
    # Create the team-specific injury entries
    injuries = {}
    for player_name, injury_data in basic_injuries.items():
        # First, keep the name-only version for backward compatibility
        injuries[player_name] = injury_data
        
        # Add team-specific entries
        c.execute("SELECT team_id FROM player_teams WHERE player_name = %s", (player_name,))
        team_results = c.fetchall()
        for team_result in team_results:
            team_id = team_result[0]
            unique_key = f"{player_name}_{team_id}"
            injuries[unique_key] = injury_data

    
    # Use local_date for filtering instead of date
    c.execute("""
        SELECT id, date, time, stadium_id, home_team_id, away_team_id, 
               home_probable_pitcher_id, away_probable_pitcher_id, local_date
        FROM games WHERE local_date BETWEEN %s AND %s
    """, (start_date, end_date))
    games = c.fetchall()
    
    logger.info(f"Found {len(games)} games for projection processing between {start_date} and {end_date}")
    
    # First, lookup the names of the probable pitchers
    for i, game in enumerate(games):
        game_id, game_date, time, stadium_id, home_team_id, away_team_id, home_pitcher_id, away_pitcher_id, local_date = game
        
        # Get pitcher names if IDs exist
        home_pitcher_name = None
        away_pitcher_name = None
        
        if home_pitcher_id:
            c.execute("""
                SELECT player_name FROM player_teams
                WHERE player_id = %s
            """, (home_pitcher_id,))
            home_pitcher_result = c.fetchone()
            
            if home_pitcher_result:
                home_pitcher_name = normalize_name(home_pitcher_result[0])
        
        if away_pitcher_id:
            c.execute("""
                SELECT player_name FROM player_teams
                WHERE player_id = %s
            """, (away_pitcher_id,))
            away_pitcher_result = c.fetchone()
            
            if away_pitcher_result:
                away_pitcher_name = normalize_name(away_pitcher_result[0])
        
        # Update the games tuple with the pitcher names
        games[i] = game + (home_pitcher_name, away_pitcher_name)
    
    # Process hitters for each game
    for game in games:
        game_id, game_date, time, stadium_id, home_team_id, away_team_id, home_pitcher_id, away_pitcher_id, local_date, home_pitcher_name, away_pitcher_name = game
        
        # Pass first 6 elements of game tuple (includes team IDs) plus local_date
        game_data = game[:6] + (local_date,)
        
        # Process home team hitters
        c.execute("""
            SELECT h.*, pt.team_id as teamid, pt.player_id 
            FROM hitters_per_game h
            LEFT JOIN player_teams pt ON h.mlbamid = pt.mlbam_id  
            WHERE pt.team_id = %s
        """, (home_team_id,))
        home_hitters = c.fetchall()
        
        # Process away team hitters
        c.execute("""
            SELECT h.*, pt.team_id as teamid, pt.player_id 
            FROM hitters_per_game h
            LEFT JOIN player_teams pt ON h.mlbamid = pt.mlbam_id  
            WHERE pt.team_id = %s
        """, (away_team_id,))
        away_hitters = c.fetchall()
        
        # Combine home and away hitters
        hitters = home_hitters + away_hitters
        hitter_columns = [col[0] for col in c.description]
        for hitter in hitters:
            hitter_dict = {hitter_columns[i]: hitter[i] for i in range(len(hitter_columns))}
            process_hitter(conn, game_data, hitter_dict, injuries, game_week_id)
        
        # Process home team pitchers
        c.execute("""
            SELECT p.*, pt.team_id as teamid, pt.player_id 
            FROM pitchers_per_game p 
            LEFT JOIN player_teams pt ON p.mlbamid = pt.mlbam_id 
            WHERE pt.team_id = %s
        """, (home_team_id,))
        home_pitchers = c.fetchall()
        
        # Process away team pitchers
        c.execute("""
            SELECT p.*, pt.team_id as teamid, pt.player_id 
            FROM pitchers_per_game p 
            LEFT JOIN player_teams pt ON p.mlbamid = pt.mlbam_id  
            WHERE pt.team_id = %s
        """, (away_team_id,))
        away_pitchers = c.fetchall()
        
        # Combine home and away pitchers
        pitchers = home_pitchers + away_pitchers
        pitcher_columns = [col[0] for col in c.description]
        for pitcher in pitchers:
            pitcher_dict = {pitcher_columns[i]: pitcher[i] for i in range(len(pitcher_columns))}
            player_name = normalize_name(pitcher_dict.get("name", ""))
            
            # Skip processing for Ohtani as a pitcher - we'll use his hitter stats only
            if "shohei ohtani" in player_name.lower():
                continue
                
            # Check if this pitcher is a probable starter for this game by NAME
            is_starter = False
            if home_pitcher_name and player_name == home_pitcher_name:
                is_starter = True
            elif away_pitcher_name and player_name == away_pitcher_name:
                is_starter = True
                
            process_pitcher(conn, game_data, pitcher_dict, injuries, game_week_id, is_starter=is_starter)
    
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