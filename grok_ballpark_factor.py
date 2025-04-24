import os
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import pytz
from utils import normalize_name, determine_game_week, DATABASE_FILE


SCORING_MATRIX = {
    'hitting': {'R': 3, 'RBI': 3, '1B': 2, '2B': 5, '3B': 8, 'HR': 10, 'BB': 2, 'K': -1, 'SB': 5, 'CS': -1, 'HBP': 2},
    'pitching': {'IP': 3, 'K': 2, 'H': -0.5, 'ER': -2, 'BB': -1, 'HBP': -1, 'W': 5, 'RA': 5, 'S': 10, 'HLD': 5 }
}
INJURY_STATUSES_OUT = ('Out', '15-Day-IL', '60-Day-IL')
DAY_TO_DAY_STATUS = 'Day-To-Day'
DAY_TO_DAY_REDUCTION = 0.8

# --- Database Initialization ---
def init_db():
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Create or update tables
    c.execute('''CREATE TABLE IF NOT EXISTS Stadiums 
                 (id INTEGER PRIMARY KEY, name TEXT, lat REAL, lon REAL, orientation REAL, is_dome INTEGER)''')
    c.execute('DROP TABLE IF EXISTS Games')
    c.execute('''CREATE TABLE IF NOT EXISTS Games 
                 (id INTEGER PRIMARY KEY, date TEXT, time TEXT, stadium_id INTEGER, 
                  home_team_id INTEGER, away_team_id INTEGER,
                  home_probable_pitcher_id TEXT, away_probable_pitcher_id TEXT, wind_effect_label TEXT)''')
    c.execute('DROP TABLE IF EXISTS WeatherForecasts')
    c.execute('''CREATE TABLE IF NOT EXISTS WeatherForecasts 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, 
                  wind_dir REAL, wind_speed REAL, temp REAL, rain REAL)''')
    c.execute('DROP TABLE IF EXISTS AdjustedProjections')
    c.execute('''CREATE TABLE IF NOT EXISTS AdjustedProjections 
                (id INTEGER PRIMARY KEY AUTOINCREMENT, player_name TEXT, mlbam_id TEXT,
                 game_id INTEGER, game_date TEXT, sorare_score REAL, team_id INTEGER, game_week TEXT)''')
    c.execute('DROP TABLE IF EXISTS PlayerTeams')
    c.execute('''CREATE TABLE IF NOT EXISTS PlayerTeams 
             (id INTEGER PRIMARY KEY AUTOINCREMENT, player_id TEXT, 
              player_name TEXT, team_id INTEGER, mlbam_id TEXT)''')
    
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
            game_date = game['gameDate'].split('T')[0]
            game_time = game['gameDate'].split('T')[1].split('.')[0]
            stadium_id = game['venue']['id']
            home_team_id = game['teams']['home']['team']['id']
            away_team_id = game['teams']['away']['team']['id']
            home_pitcher = game['teams']['home'].get('probablePitcher', {}).get('id', None)
            away_pitcher = game['teams']['away'].get('probablePitcher', {}).get('id', None)
            
            c.execute("INSERT OR IGNORE INTO Stadiums (id, name) VALUES (?, ?)",
                      (stadium_id, game['venue']['name']))
            c.execute("""
                INSERT OR REPLACE INTO Games 
                (id, date, time, stadium_id, home_team_id, away_team_id, 
                 home_probable_pitcher_id, away_probable_pitcher_id) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (game_id, game_date, game_time, stadium_id, home_team_id, away_team_id,
                  str(home_pitcher) if home_pitcher else None, 
                  str(away_pitcher) if away_pitcher else None))
    
    conn.commit()
    return game_week_id

def populate_player_teams(conn, start_date, end_date, update_rosters=False):
    c = conn.cursor()
    if not update_rosters:
        existing_count = c.execute("SELECT COUNT(*) FROM PlayerTeams").fetchone()[0]
        if existing_count > 0:
            print("Using cached roster data.")
            return
        print("No cached roster data found; fetching rosters...")
    
    print("Updating roster data...")
    c.execute("DELETE FROM PlayerTeams")
    
    teams = set()
    games = c.execute("SELECT home_team_id, away_team_id FROM Games WHERE date BETWEEN ? AND ?",
                      (start_date, end_date)).fetchall()
    for home_team_id, away_team_id in games:
        teams.add(home_team_id)
        teams.add(away_team_id)
    
    for team_id in teams:
        url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active"
        try:
            response = requests.get(url)
            roster_data = response.json()
            for player in roster_data.get('roster', []):
                player_id = str(player['person']['id'])  # This is the MLBAMID
                player_name = normalize_name(player['person']['fullName'])
                c.execute("INSERT OR IGNORE INTO PlayerTeams (player_id, player_name, team_id, mlbam_id) VALUES (?, ?, ?, ?)",
                        (player_id, player_name, team_id, player_id))  # Store MLBAMID
        except Exception as e:
            print(f"Error fetching roster for team {team_id}: {e}")
    
    conn.commit()
    add_projected_starting_pitchers(conn, start_date, end_date)

# --- Weather Functions ---
def get_weather_nws(lat, lon, forecast_time):
    """Fetches weather data from the National Weather Service API with enhanced error handling."""
    try:
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            print(f"Invalid coordinates: lat={lat}, lon={lon}")
            return None
        

        points_url = f"https://api.weather.gov/points/{lat},{lon}"
        points_response = requests.get(points_url)
        points_response.raise_for_status()
        points_data = points_response.json()

        if 'properties' not in points_data or 'forecastHourly' not in points_data['properties']:
            print(f"Invalid points data structure: {points_data}")
            return None

        forecast_hourly_url = points_data['properties']['forecastHourly']
        forecast_response = requests.get(forecast_hourly_url)
        forecast_response.raise_for_status()
        forecast_data = forecast_response.json()

        if 'properties' not in forecast_data or 'periods' not in forecast_data['properties']:
            print(f"Invalid forecast data structure: {forecast_data}")
            return None

        periods = forecast_data['properties']['periods']
        if not periods:
            print("No forecast periods available.")
            return None

        for period in periods:
            try:
                start_time = datetime.fromisoformat(period['startTime'].replace('Z', '+00:00'))
                end_time = datetime.fromisoformat(period['endTime'].replace('Z', '+00:00'))
                if start_time <= forecast_time < end_time:
                    weather = {
                        'temp': period['temperature'],
                        'wind_speed': int(period['windSpeed'].split()[0]),
                        'wind_dir': wind_dir_to_degrees(period['windDirection']),
                        'rain': period.get('probabilityOfPrecipitation', {}).get('value', 0),
                    }
                    return weather
            except (KeyError, ValueError) as e:
                print(f"Error parsing period times: {e} in period: {period}")
                continue

        print(f"No forecast period found for {forecast_time}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"NWS API Request Error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error in get_weather_nws: {e}")
        return None

def fetch_weather_and_store(conn, start_date, end_date):
    c = conn.cursor()
    games = c.execute("SELECT id, date, time, stadium_id FROM Games WHERE date BETWEEN ? AND ?",
                      (start_date, end_date)).fetchall()

    for game in games:
        game_id, date, time, stadium_id = game
        stadium = c.execute("SELECT lat, lon, is_dome, orientation FROM Stadiums WHERE id = ?", (stadium_id,)).fetchone()
        if not stadium or stadium[2]:
            continue
        lat, lon, is_dome, orientation = stadium
        if lat is None or lon is None:
            print(f"Skipping game https://baseballsavant.mlb.com/preview?game_pk={game_id} due to missing stadium {stadium_id} coordinates.")
            continue

        if time.endswith('Z'):
            utc_time = datetime.strptime(f"{date}T{time}".replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
            utc_time = utc_time.replace(tzinfo=pytz.utc)
        else:
            local_tz = pytz.timezone('America/New_York')
            local_time = datetime.strptime(f"{date}T{time}", "%Y-%m-%dT%H:%M:%S")
            local_time = local_tz.localize(local_time)
            utc_time = local_time.astimezone(pytz.utc)

        if utc_time > datetime.now(pytz.utc) + timedelta(days=7):
            print(f"⚠️ Skipping forecast too far in the future: {utc_time} game id {game_id}")
            continue

        weather = get_weather_nws(lat, lon, utc_time)
        wind_effect_label = get_wind_effect_label(orientation, weather['wind_dir'])
        if weather is not None:
            c.execute("INSERT INTO WeatherForecasts (game_id, wind_dir, wind_speed, temp, rain) VALUES (?, ?, ?, ?, ?)",
                      (game_id, weather['wind_dir'], weather['wind_speed'], weather['temp'], weather['rain']))
            c.execute("""
                    UPDATE Games SET wind_effect_label = ? WHERE id = ?
                """, (wind_effect_label, game_id))
        else:
            print(f"Skipping game {game_id} due to API error.")

    conn.commit()

# --- Adjustment Functions ---
def get_wind_effect(orientation, wind_dir, wind_speed):
    angle_diff = (wind_dir - orientation + 180) % 360 - 180
    if abs(angle_diff) < 45 and wind_speed > 10:
        return 0.9
    elif abs(angle_diff) > 135 and wind_speed > 10:
        return 1.1
    return 1.0

def get_wind_effect_label(orientation, wind_dir):
    """
    Determines the wind effect label ("Out", "In", "Neutral") based on the stadium's orientation and wind direction.
    """
    if orientation is None or wind_dir is None:
        return "Neutral"

    angle_diff = (wind_dir - orientation + 180) % 360 - 180
    if abs(angle_diff) < 45:
        return "In"
    elif abs(angle_diff) > 135:
        return "Out"
    else:
        return "Neutral"

def get_temp_adjustment(temp):
    if temp > 80:
        return 1.05
    elif temp < 60:
        return 0.95
    return 1.0

def wind_dir_to_degrees(wind_dir):
    directions = {
        'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5, 'E': 90, 'ESE': 112.5,
        'SE': 135, 'SSE': 157.5, 'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
        'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
    }
    return directions.get(wind_dir.upper() if wind_dir else 'N', 0)

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
            return_estimate_date = datetime.strptime(return_estimate, '%Y-%m-%d').date()
        except ValueError:
            print(f"Warning: Invalid return date format '{return_estimate}', treating as None")
            return_estimate_date = None

    if injury_status in INJURY_STATUSES_OUT and (not return_estimate_date or game_date <= return_estimate_date):
        return 0.0
    if injury_status == DAY_TO_DAY_STATUS and return_estimate_date and game_date <= return_estimate_date:
        return base_score * DAY_TO_DAY_REDUCTION
    return base_score

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

def process_hitter(conn, game_data, hitter_data, injuries, game_week_id):
    game_id, game_date, time, stadium_id, home_team_id, away_team_id = game_data[:6]
    game_date_obj = datetime.strptime(game_date, '%Y-%m-%d').date()

    player_name = normalize_name(hitter_data.get("Name"))
    mlbam_id = hitter_data.get("MLBAMID")
    
    if not player_name:
        print(f"Warning: Null Name for hitter in game {game_id}")
        return

    player_team_id = hitter_data.get("TeamID")
    if not player_team_id and mlbam_id:
        # Look up team ID by MLBAMID if not directly available
        c = conn.cursor()
        team_result = c.execute("SELECT team_id FROM PlayerTeams WHERE mlbam_id = ?", (mlbam_id,)).fetchone()
        if team_result:
            player_team_id = team_result[0]
    
    if not player_team_id:
        #print(f"⚠️ Skipping {player_name} — no team assigned.")
        return
    if player_team_id not in (home_team_id, away_team_id):
        return

    # Create a unique player identifier with MLBAMID if available
    unique_player_key = mlbam_id if mlbam_id else f"{player_name}_{player_team_id}"
    
    c = conn.cursor()
    stadium_data = c.execute("""
        SELECT s.is_dome, s.orientation, w.wind_dir, w.wind_speed, w.temp 
        FROM Stadiums s 
        LEFT JOIN WeatherForecasts w ON w.game_id = ?
        WHERE s.id = ?
    """, (game_id, stadium_id)).fetchone()

    if not stadium_data:
        is_dome, orientation, wind_dir, wind_speed, temp = (0, 0, 0, 0, 70)
    else:
        is_dome, orientation, wind_dir, wind_speed, temp = stadium_data

    park_factors = {row[0]: row[1] / 100 for row in c.execute("SELECT factor_type, value FROM ParkFactors WHERE stadium_id = ?", (stadium_id,)).fetchall()}
    if not park_factors:
        park_factors = {'R': 1.0, 'RBI': 1.0, '1B': 1.0, '2B': 1.0, '3B': 1.0, 'HR': 1.0, 'BB': 1.0, 'K': 1.0, 'SB': 1.0, 'CS': 1.0, 'HBP': 1.0}

    base_stats = {
        'R': hitter_data.get('R_per_game', 0),
        'RBI': hitter_data.get('RBI_per_game', 0),
        '1B': hitter_data.get('1B_per_game', 0),
        '2B': hitter_data.get('2B_per_game', 0),
        '3B': hitter_data.get('3B_per_game', 0),
        'HR': hitter_data.get('HR_per_game', 0),
        'BB': hitter_data.get('BB_per_game', 0),
        'K': hitter_data.get('K_per_game', 0),
        'SB': hitter_data.get('SB_per_game', 0),
        'CS': hitter_data.get('CS_per_game', 0),
        'HBP': hitter_data.get('HBP_per_game', 0)
    }

    adjusted_stats = adjust_stats(base_stats, park_factors, is_dome, orientation, wind_dir, wind_speed, temp)
    base_score = calculate_sorare_hitter_score(adjusted_stats, SCORING_MATRIX)
    injury_data = injuries.get(unique_player_key, injuries.get(player_name, {'status': 'Active', 'return_estimate': None}))
    final_score = adjust_score_for_injury(base_score, injury_data['status'], injury_data['return_estimate'], game_date_obj)

    existing = c.execute("""
        SELECT id FROM AdjustedProjections 
        WHERE (player_name = ? AND game_id = ? AND team_id = ?) OR
            (mlbam_id = ? AND game_id = ?)
    """, (player_name, game_id, player_team_id, mlbam_id, game_id)).fetchone()

    if existing:
        c.execute("""
            UPDATE AdjustedProjections 
            SET sorare_score = ? 
            WHERE (player_name = ? AND game_id = ? AND team_id = ?) OR
                (mlbam_id = ? AND game_id = ?)
        """, (final_score, player_name, game_id, player_team_id, mlbam_id, game_id))
    else:
        c.execute("""
            INSERT INTO AdjustedProjections 
            (player_name, mlbam_id, game_id, game_date, sorare_score, game_week, team_id) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (player_name, mlbam_id, game_id, game_date, final_score, game_week_id, player_team_id))


def process_pitcher(conn, game_data, pitcher_data, injuries, game_week_id, is_starter=False):
    game_id, game_date, time, stadium_id, home_team_id, away_team_id = game_data[:6]
    game_date_obj = datetime.strptime(game_date, '%Y-%m-%d').date()

    player_name = normalize_name(pitcher_data.get("Name"))
    mlbam_id = pitcher_data.get("MLBAMID")
    
    if not player_name:
        print(f"Warning: Null Name for pitcher in game {game_id}")
        return

    player_team_id = pitcher_data.get("TeamID")
    if not player_team_id and mlbam_id:
        # Look up team ID by MLBAMID if not directly available
        c = conn.cursor()
        team_result = c.execute("SELECT team_id FROM PlayerTeams WHERE mlbam_id = ?", (mlbam_id,)).fetchone()
        if team_result:
            player_team_id = team_result[0]
            
    if not player_team_id:
        print(f"⚠️ Skipping {player_name} — no team assigned.")
        return
    if player_team_id not in (home_team_id, away_team_id):
        return
    
    # Create a unique player identifier with MLBAMID if available
    unique_player_key = mlbam_id if mlbam_id else f"{player_name}_{player_team_id}"
    
    # Determine if pitcher is generally a starter (projects to 2+ innings per game)
    innings_per_game = pitcher_data.get('IP_per_game', 0)
    is_generally_starter = innings_per_game > 2.0
    
    # If pitcher is generally a starter but not starting in this game, set score to 0
    if is_generally_starter and not is_starter:
        c = conn.cursor()
        
        # Check if this player already has a projection for this game
        existing = c.execute("""
            SELECT id FROM AdjustedProjections 
            WHERE (player_name = ? AND game_id = ? AND team_id = ?) OR
                (mlbam_id = ? AND game_id = ?)
        """, (player_name, game_id, player_team_id, mlbam_id, game_id)).fetchone()
        final_score = 0.0
        if existing:
            c.execute("""
                UPDATE AdjustedProjections 
                SET sorare_score = ? 
                WHERE (player_name = ? AND game_id = ? AND team_id = ?) OR
                    (mlbam_id = ? AND game_id = ?)
            """, (final_score, player_name, game_id, player_team_id, mlbam_id, game_id))
        else:
            c.execute("""
                INSERT INTO AdjustedProjections 
                (player_name, mlbam_id, game_id, game_date, sorare_score, game_week, team_id) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (player_name, mlbam_id, game_id, game_date, final_score, game_week_id, player_team_id))
        return
    
    c = conn.cursor()
    stadium_data = c.execute("""
        SELECT s.is_dome, s.orientation, w.wind_dir, w.wind_speed, w.temp 
        FROM Stadiums s 
        LEFT JOIN WeatherForecasts w ON w.game_id = ?
        WHERE s.id = ?
    """, (game_id, stadium_id)).fetchone()
    
    if not stadium_data:
        print(f"No stadium data for game {game_id} with stadium_id {stadium_id}")
        is_dome, orientation, wind_dir, wind_speed, temp = (0, 0, 0, 0, 70)
    else:
        is_dome, orientation, wind_dir, wind_speed, temp = stadium_data
    
    park_factors = {row[0]: row[1] / 100 for row in c.execute("SELECT factor_type, value FROM ParkFactors WHERE stadium_id = ?", (stadium_id,)).fetchall()}
    if not park_factors:
        print(f"No park factors for stadium_id {stadium_id}, using default 1.0")
        park_factors = {'IP': 1.0, 'SO': 1.0, 'H': 1.0, 'ER': 1.0, 'BB': 1.0, 'HBP': 1.0, 'W': 1.0, 'SV': 1.0}
    
    base_stats = {
        'IP': innings_per_game,
        'SO': pitcher_data.get('K_per_game', 0),
        'H': pitcher_data.get('H_per_game', 0),
        'ER': pitcher_data.get('ER_per_game', 0),
        'BB': pitcher_data.get('BB_per_game', 0),
        'HBP': pitcher_data.get('HBP_per_game', 0),
        'W': pitcher_data.get('W_per_game', 0),
        'HLD': pitcher_data.get('HLD_per_game', 0),
        'S': pitcher_data.get('S_per_game', 0),
        'RA': not is_starter
    }
    
    adjusted_stats = adjust_stats(base_stats, park_factors, is_dome, orientation, wind_dir, wind_speed, temp, is_pitcher=True)
    base_score = calculate_sorare_pitcher_score(adjusted_stats, SCORING_MATRIX)

    if not is_starter:
        # If not a starter, apply a reduction to the score
        base_score *= 0.4

    # Try to find injury data with the unique key first, then fall back to just the name
    injury_data = injuries.get(unique_player_key, injuries.get(player_name, {'status': 'Active', 'return_estimate': None}))
    final_score = adjust_score_for_injury(base_score, injury_data['status'], injury_data['return_estimate'], game_date_obj)

    # Check if this player already has a projection for this game
    existing = c.execute("""
        SELECT id FROM AdjustedProjections 
        WHERE (player_name = ? AND game_id = ? AND team_id = ?) OR
            (mlbam_id = ? AND game_id = ?)
    """, (player_name, game_id, player_team_id, mlbam_id, game_id)).fetchone()
    


    if existing:
        c.execute("""
            UPDATE AdjustedProjections 
            SET sorare_score = ? 
            WHERE (player_name = ? AND game_id = ? AND team_id = ?) OR
                (mlbam_id = ? AND game_id = ?)
        """, (final_score, player_name, game_id, player_team_id, mlbam_id, game_id))
    else:
        c.execute("""
            INSERT INTO AdjustedProjections 
            (player_name, mlbam_id, game_id, game_date, sorare_score, game_week, team_id) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (player_name, mlbam_id, game_id, game_date, final_score, game_week_id, player_team_id))

def add_projected_starting_pitchers(conn, start_date, end_date):
    """
    Adds projected starting pitchers to the PlayerTeams table even if they're not on active rosters yet.
    This will allow the system to use their existing projections from pitchers_per_game.
    """
    print("Adding projected starting pitchers to PlayerTeams...")
    c = conn.cursor()
    
    # Get games in the date range
    games = c.execute("""
        SELECT id, date, home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id 
        FROM Games WHERE date BETWEEN ? AND ?
    """, (start_date, end_date)).fetchall()
    
    pitcher_count = 0
    
    for game in games:
        game_id, game_date, home_team_id, away_team_id, home_pitcher_id, away_pitcher_id = game
        
        # Process home pitcher if specified
        if home_pitcher_id and home_pitcher_id != 'None':
            # Check if the pitcher is already in PlayerTeams
            existing = c.execute("SELECT COUNT(*) FROM PlayerTeams WHERE player_id = ?", 
                                (home_pitcher_id,)).fetchone()[0]
            
            if existing == 0:
                # Pitcher not in PlayerTeams, fetch their details from MLB API
                try:
                    url = f"https://statsapi.mlb.com/api/v1/people/{home_pitcher_id}"
                    response = requests.get(url)
                    player_data = response.json()
                    
                    if 'people' in player_data and len(player_data['people']) > 0:
                        player = player_data['people'][0]
                        player_name = normalize_name(player['fullName'])
                        
                        # Check if this player exists in pitchers_per_game
                        pitcher_exists = c.execute("""
                            SELECT COUNT(*) FROM pitchers_per_game 
                            WHERE Name = ? OR MLBAMID = ?
                        """, (player['fullName'], home_pitcher_id)).fetchone()[0]
                        
                        if pitcher_exists > 0:
                            # Add to the PlayerTeams table to connect them to their stats
                            c.execute("""
                                INSERT INTO PlayerTeams (player_id, player_name, team_id, mlbam_id)
                                VALUES (?, ?, ?, ?)
                            """, (str(home_pitcher_id), player_name, home_team_id, str(home_pitcher_id)))
                            
                            pitcher_count += 1
                            print(f"Added projected starter: {player_name} (ID: {home_pitcher_id}) to team {home_team_id}")
                except Exception as e:
                    print(f"Error fetching pitcher {home_pitcher_id} data: {e}")
        
        # Process away pitcher if specified
        if away_pitcher_id and away_pitcher_id != 'None':
            # Check if the pitcher is already in PlayerTeams
            existing = c.execute("SELECT COUNT(*) FROM PlayerTeams WHERE player_id = ?", 
                                (away_pitcher_id,)).fetchone()[0]
            
            if existing == 0:
                # Pitcher not in PlayerTeams, fetch their details from MLB API
                try:
                    url = f"https://statsapi.mlb.com/api/v1/people/{away_pitcher_id}"
                    response = requests.get(url)
                    player_data = response.json()
                    
                    if 'people' in player_data and len(player_data['people']) > 0:
                        player = player_data['people'][0]
                        player_name = normalize_name(player['fullName'])
                        
                        # Check if this player exists in pitchers_per_game
                        pitcher_exists = c.execute("""
                            SELECT COUNT(*) FROM pitchers_per_game 
                            WHERE Name = ? OR MLBAMID = ?
                        """, (player['fullName'], away_pitcher_id)).fetchone()[0]
                        
                        if pitcher_exists > 0:
                            # Add to the PlayerTeams table to connect them to their stats
                            c.execute("""
                                INSERT INTO PlayerTeams (player_id, player_name, team_id, mlbam_id)
                                VALUES (?, ?, ?, ?)
                            """, (str(away_pitcher_id), player_name, away_team_id, str(away_pitcher_id)))
                            
                            pitcher_count += 1
                            print(f"Added projected starter: {player_name} (ID: {away_pitcher_id}) to team {away_team_id}")
                except Exception as e:
                    print(f"Error fetching pitcher {away_pitcher_id} data: {e}")
    
    conn.commit()
    print(f"Added {pitcher_count} projected starting pitchers to PlayerTeams")

# --- Updates to the main functions ---
def calculate_adjustments(conn, start_date, end_date, game_week_id):
    if not isinstance(start_date, str):
        start_date = start_date.strftime('%Y-%m-%d')
    if not isinstance(end_date, str):
        end_date = end_date.strftime('%Y-%m-%d')
    
    c = conn.cursor()
    c.execute("DELETE FROM AdjustedProjections WHERE game_week = ?", (game_week_id,))
    
    # Create injury lookup as before
    basic_injuries = {row[0]: {'status': row[1], 'return_estimate': row[2]} 
                for row in c.execute("SELECT player_name, status, return_estimate FROM injuries").fetchall()}
    
    # Create the team-specific injury entries
    injuries = {}
    for player_name, injury_data in basic_injuries.items():
        # First, keep the name-only version for backward compatibility
        injuries[player_name] = injury_data
        
        # Add team-specific entries
        team_results = c.execute("SELECT team_id FROM PlayerTeams WHERE player_name = ?", (player_name,)).fetchall()
        for team_result in team_results:
            team_id = team_result[0]
            unique_key = f"{player_name}_{team_id}"
            injuries[unique_key] = injury_data
    
    games = c.execute("""
        SELECT id, date, time, stadium_id, home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id 
        FROM Games WHERE date BETWEEN ? AND ?
    """, (start_date, end_date)).fetchall()
    
    # First, lookup the names of the probable pitchers
    for i, game in enumerate(games):
        game_id, game_date, time, stadium_id, home_team_id, away_team_id, home_pitcher_id, away_pitcher_id = game
        
        # Get pitcher names if IDs exist
        home_pitcher_name = None
        away_pitcher_name = None
        
        if home_pitcher_id:
            home_pitcher_result = c.execute("""
                SELECT player_name FROM PlayerTeams
                WHERE player_id = ?
            """, (home_pitcher_id,)).fetchone()
            
            if home_pitcher_result:
                home_pitcher_name = normalize_name(home_pitcher_result[0])
        
        if away_pitcher_id:
            away_pitcher_result = c.execute("""
                SELECT player_name FROM PlayerTeams
                WHERE player_id = ?
            """, (away_pitcher_id,)).fetchone()
            
            if away_pitcher_result:
                away_pitcher_name = normalize_name(away_pitcher_result[0])
        
        # Update the games tuple with the pitcher names
        games[i] = game + (home_pitcher_name, away_pitcher_name)
    
    # Process hitters for each game
    for game in games:
        game_id, game_date, time, stadium_id, home_team_id, away_team_id, home_pitcher_id, away_pitcher_id, home_pitcher_name, away_pitcher_name = game
        
        # Pass first 6 elements of game tuple (includes team IDs)
        game_data = game[:6]
        
        # Process home team hitters
        home_hitters = c.execute("""
            SELECT h.*, pt.team_id as TeamID, pt.player_id 
            FROM hitters_per_game h
            LEFT JOIN PlayerTeams pt ON h.MLBAMID = pt.mlbam_id  
            WHERE pt.team_id = ?
        """, (home_team_id,)).fetchall()
        
        # Process away team hitters
        away_hitters = c.execute("""
            SELECT h.*, pt.team_id as TeamID, pt.player_id 
            FROM hitters_per_game h
            LEFT JOIN PlayerTeams pt ON h.MLBAMID = pt.mlbam_id  
            WHERE pt.team_id = ?
        """, (away_team_id,)).fetchall()
        
        # Combine home and away hitters
        hitters = home_hitters + away_hitters
        hitter_columns = [col[0] for col in c.description]
        
        for hitter in hitters:
            hitter_dict = {hitter_columns[i]: hitter[i] for i in range(len(hitter_columns))}
            process_hitter(conn, game_data, hitter_dict, injuries, game_week_id)
        
        # Process home team pitchers
        home_pitchers = c.execute("""
            SELECT p.*, pt.team_id as TeamID, pt.player_id 
            FROM pitchers_per_game p 
            LEFT JOIN PlayerTeams pt ON p.MLBAMID = pt.mlbam_id 
            WHERE pt.team_id = ?
        """, (home_team_id,)).fetchall()
        
        # Process away team pitchers
        away_pitchers = c.execute("""
            SELECT p.*, pt.team_id as TeamID, pt.player_id 
            FROM pitchers_per_game p 
            LEFT JOIN PlayerTeams pt ON p.MLBAMID = pt.mlbam_id  
            WHERE pt.team_id = ?
        """, (away_team_id,)).fetchall()
        
        # Combine home and away pitchers
        pitchers = home_pitchers + away_pitchers
        pitcher_columns = [col[0] for col in c.description]

        for pitcher in pitchers:
            pitcher_dict = {pitcher_columns[i]: pitcher[i] for i in range(len(pitcher_columns))}
            player_name = normalize_name(pitcher_dict.get("Name", ""))
            
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
# --- Main Function ---
def main(update_rosters=False, specified_date=None):
    current_date = specified_date if specified_date else datetime.now().date()
    game_week_id = determine_game_week(current_date)  # Use the utils function
    start_date, end_date = game_week_id.split('_to_')  # Split the string for use
    print(f"Processing game week: {start_date} to {end_date}")
    conn = init_db()
    
    game_week_id = get_schedule(conn, start_date, end_date)  # Still returns the same string
    fetch_weather_and_store(conn, start_date, end_date)
    populate_player_teams(conn, start_date, end_date, update_rosters=update_rosters)
    calculate_adjustments(conn, start_date, end_date, game_week_id)
    conn.close()
    print(f"Projections adjusted for game week: {start_date} to {end_date}")
    print(f"Game week ID: {game_week_id}")

if __name__ == "__main__":
    main()
    # Examples:
    #main(update_rosters=True)
    # main(specified_date=date(2025, 3, 27))