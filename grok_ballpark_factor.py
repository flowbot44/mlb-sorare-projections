import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import pytz
from utils import normalize_name

# --- Game Week Handling ---
def determine_game_week(current_date):
    """
    Determine the start and end dates of the appropriate game week based on when the script is run:
    - If run Tuesday through Friday: return the upcoming/current Friday-Sunday period
    - If run Saturday through Monday: return the upcoming/current Monday-Thursday period
    
    Special case for season start (March 27-30, 2025)
    """
    if isinstance(current_date, str):
        current_date = datetime.strptime(current_date, '%Y-%m-%d').date()
    
    season_start = date(2025, 3, 27)
    if current_date <= date(2025, 3, 30):
        return season_start, date(2025, 3, 30)
    
    day_of_week = current_date.weekday()
    
    if 1 <= day_of_week <= 4:  # Tuesday to Friday
        days_until_friday = (4 - day_of_week) % 7
        target_friday = current_date + timedelta(days=days_until_friday)
        target_sunday = target_friday + timedelta(days=2)
        return target_friday, target_sunday
    else:  # Saturday, Sunday, or Monday
        days_until_monday = (0 - day_of_week) % 7
        target_monday = current_date + timedelta(days=days_until_monday)
        target_thursday = target_monday + timedelta(days=3)
        return target_monday, target_thursday

# --- Database Initialization ---
def init_db(db_path='mlb_sorare.db'):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Create or update tables
    c.execute('''CREATE TABLE IF NOT EXISTS Stadiums 
                 (id INTEGER PRIMARY KEY, name TEXT, lat REAL, lon REAL, orientation REAL, is_dome INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS ParkFactors 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, stadium_id INTEGER, factor_type TEXT, value REAL)''')
    c.execute('DROP TABLE IF EXISTS Games')
    c.execute('''CREATE TABLE IF NOT EXISTS Games 
                 (id INTEGER PRIMARY KEY, date TEXT, time TEXT, stadium_id INTEGER, 
                  home_team_id INTEGER, away_team_id INTEGER,
                  home_probable_pitcher_id TEXT, away_probable_pitcher_id TEXT)''')
    c.execute('DROP TABLE IF EXISTS WeatherForecasts')
    c.execute('''CREATE TABLE IF NOT EXISTS WeatherForecasts 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, 
                  wind_dir REAL, wind_speed REAL, temp REAL, rain REAL)''')
    c.execute('DROP TABLE IF EXISTS AdjustedProjections')
    c.execute('''CREATE TABLE IF NOT EXISTS AdjustedProjections 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, player_name TEXT, game_id INTEGER, 
                  game_date TEXT, sorare_score REAL, game_week TEXT)''')
    c.execute('DROP TABLE IF EXISTS PlayerTeams')
    c.execute('''CREATE TABLE IF NOT EXISTS PlayerTeams 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, player_id TEXT, 
                  player_name TEXT, team_id INTEGER)''')
    
    conn.commit()
    return conn

# --- Load Park Factors from CSV ---
def load_park_factors_from_csv(conn, csv_path='park_data.csv'):
    """Load park factors from a CSV file into the database."""
    df = pd.read_csv(csv_path)
    factor_types = ['Park Factor', 'wOBACon', 'xwOBACon', 'BACON', 'xBACON', 'HardHit', 
                    'R', 'OBP', 'H', '1B', '2B', '3B', 'HR', 'BB', 'SO']
    
    c = conn.cursor()
    c.execute("DELETE FROM ParkFactors")
    
    for _, row in df.iterrows():
        venue = row['Venue']
        stadium_id = c.execute("SELECT id FROM Stadiums WHERE name LIKE ?", (f"%{venue}%",)).fetchone()
        if stadium_id:
            stadium_id = stadium_id[0]
            for factor_type in factor_types:
                value = row[factor_type]
                c.execute("INSERT INTO ParkFactors (stadium_id, factor_type, value) VALUES (?, ?, ?)",
                          (stadium_id, factor_type, value))
    
    conn.commit()
    print(f"Park factors updated from {csv_path}")

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
                player_id = str(player['person']['id'])
                player_name = normalize_name(player['person']['fullName'])  # Normalize the name here
                c.execute("INSERT OR IGNORE INTO PlayerTeams (player_id, player_name, team_id) VALUES (?, ?, ?)",
                          (player_id, player_name, team_id))
        except Exception as e:
            print(f"Error fetching roster for team {team_id}: {e}")
    
    conn.commit()

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
        stadium = c.execute("SELECT lat, lon, is_dome FROM Stadiums WHERE id = ?", (stadium_id,)).fetchone()
        if not stadium or stadium[2]:
            continue
        lat, lon = stadium[0], stadium[1]
        if lat is None or lon is None:
            print(f"Skipping game {game_id} due to missing stadium {stadium_id} coordinates.")
            continue

        if time.endswith('Z'):
            utc_time = datetime.strptime(f"{date}T{time}".replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
            utc_time = utc_time.replace(tzinfo=pytz.utc)
        else:
            local_tz = pytz.timezone('America/New_York')
            local_time = datetime.strptime(f"{date}T{time}", "%Y-%m-%dT%H:%M:%S")
            local_time = local_tz.localize(local_time)
            utc_time = local_time.astimezone(pytz.utc)

        weather = get_weather_nws(lat, lon, utc_time)
        if weather is not None:
            c.execute("INSERT INTO WeatherForecasts (game_id, wind_dir, wind_speed, temp, rain) VALUES (?, ?, ?, ?, ?)",
                      (game_id, weather['wind_dir'], weather['wind_speed'], weather['temp'], weather['rain']))
        else:
            print(f"Skipping game {game_id} due to API error.")

    conn.commit()

# --- Adjustment Functions ---
def get_wind_effect(orientation, wind_dir, wind_speed):
    angle_diff = (wind_dir - orientation + 180) % 360 - 180
    if abs(angle_diff) < 45 and wind_speed > 10:
        return 1.1
    elif abs(angle_diff) > 135 and wind_speed > 10:
        return 0.9
    return 1.0

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
    singles = stats.get('H', 0) - (stats.get('2B', 0) + stats.get('3B', 0) + stats.get('HR', 0))
    score += singles * scoring_matrix['hitting'].get('1B', 0)
    score += stats.get('2B', 0) * scoring_matrix['hitting'].get('2B', 0)
    score += stats.get('3B', 0) * scoring_matrix['hitting'].get('3B', 0)
    score += stats.get('HR', 0) * scoring_matrix['hitting'].get('HR', 0)
    score += stats.get('R', 0) * scoring_matrix['hitting'].get('R', 0)
    score += stats.get('RBI', 0) * scoring_matrix['hitting'].get('RBI', 0)
    score += stats.get('BB', 0) * scoring_matrix['hitting'].get('BB', 0)
    score += stats.get('SO', 0) * scoring_matrix['hitting'].get('K', 0)
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
    score += stats.get('SV', 0) * scoring_matrix['pitching'].get('S', 0)
    return score

def calculate_adjustments(conn, start_date, end_date, game_week_id):
    if not isinstance(start_date, str):
        start_date = start_date.strftime('%Y-%m-%d')
    if not isinstance(end_date, str):
        end_date = end_date.strftime('%Y-%m-%d')
    
    c = conn.cursor()
    c.execute("DELETE FROM AdjustedProjections WHERE game_week = ?", (game_week_id,))
    
    scoring_matrix = {
        'hitting': {'R': 3, 'RBI': 3, '1B': 2, '2B': 5, '3B': 8, 'HR': 10, 'BB': 2, 'K': -1, 'SB': 5, 'CS': -1, 'HBP': 2},
        'pitching': {'IP': 3, 'K': 2, 'H': -0.5, 'ER': -2, 'BB': -1, 'HBP': -1, 'W': 5, 'RA': 5, 'S': 10}
    }
    
    # Fetch all injury data into a dictionary with status and return_estimate
    injuries = {row[0]: {'status': row[1], 'return_estimate': row[2]} 
                for row in c.execute("SELECT player_name, status, return_estimate FROM injuries").fetchall()}
    
    games = c.execute("SELECT id, date, time, stadium_id, home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id FROM Games WHERE date BETWEEN ? AND ?",
                      (start_date, end_date)).fetchall()
    
    processed_pitchers = set()
    
    for game in games:
        game_id, game_date, time, stadium_id, home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id = game
        game_date_obj = datetime.strptime(game_date, '%Y-%m-%d').date()
        
        stadium_data = c.execute("""
            SELECT s.is_dome, s.orientation, w.wind_dir, w.wind_speed, w.temp 
            FROM Stadiums s 
            LEFT JOIN WeatherForecasts w ON w.game_id = ?
            WHERE s.id = ?
        """, (game_id, stadium_id)).fetchone()
        if not stadium_data:
            continue
        is_dome, orientation, wind_dir, wind_speed, temp = stadium_data
        
        park_factors = {row[0]: row[1] / 100 for row in c.execute("SELECT factor_type, value FROM ParkFactors WHERE stadium_id = ?", (stadium_id,)).fetchall()}
        
        participating_team_ids = (home_team_id, away_team_id)
        
        # Process hitters
        hitters = c.execute("""
            SELECT h.* FROM hitters_per_game h 
            JOIN PlayerTeams pt ON h.Name = pt.player_name 
            WHERE pt.team_id IN (?, ?)
        """, participating_team_ids).fetchall()
        hitter_columns = [col[0] for col in c.description]
        
        for hitter in hitters:
            hitter_dict = {hitter_columns[i]: hitter[i] for i in range(len(hitter_columns))}
            player_name = normalize_name(hitter_dict.get("Name"))
            if player_name is None:
                print(f"Warning: Null Name for hitter in game {game_id}")
                continue
            
            # Check injury status and return estimate
            injury_info = injuries.get(player_name, {'status': 'Active', 'return_estimate': None})
            injury_status = injury_info['status']
            return_estimate = injury_info['return_estimate']
            if return_estimate:
                return_estimate = datetime.strptime(return_estimate, '%Y-%m-%d').date()
            
            if injury_status in ('Out', '15-Day-IL', '60-Day-IL') and (not return_estimate or game_date_obj <= return_estimate):
                sorare_score = 0.0
                print(f"Player {player_name} is {injury_status}, setting projection to 0 until {return_estimate}")
            else:
                base_stats = {
                    'R': hitter_dict.get('R', 0), 'RBI': hitter_dict.get('RBI', 0), 'H': hitter_dict.get('H', 0),
                    '2B': hitter_dict.get('2B', 0), '3B': hitter_dict.get('3B', 0), 'HR': hitter_dict.get('HR', 0),
                    'BB': hitter_dict.get('BB', 0), 'SO': hitter_dict.get('SO', 0), 'SB': hitter_dict.get('SB', 0),
                    'CS': hitter_dict.get('CS', 0), 'HBP': hitter_dict.get('HBP', 0)
                }
                adjusted_stats = {}
                for stat, value in base_stats.items():
                    park_adjustment = park_factors.get(stat, 1.0)
                    weather_factor = 1.0 if is_dome or stat != 'HR' else get_wind_effect(orientation or 0, wind_dir or 0, wind_speed or 0) * get_temp_adjustment(temp or 70)
                    adjusted_stats[stat] = value * park_adjustment * weather_factor
                sorare_score = calculate_sorare_hitter_score(adjusted_stats, scoring_matrix)
                
                # Apply 80% reduction for Day-to-Day up to and including return_estimate
                if injury_status == 'Day-To-Day' and return_estimate and game_date_obj <= return_estimate:
                    sorare_score *= 0.8
                    print(f"Player {player_name} is Day-To-Day, reducing projection to 80% until {return_estimate}")
            
            c.execute("INSERT INTO AdjustedProjections (player_name, game_id, game_date, sorare_score, game_week) VALUES (?, ?, ?, ?, ?)",
                      (player_name, game_id, game_date, sorare_score, game_week_id))
        
        # Process pitchers (only probable starters)
        safe_home_pitcher_id = home_probable_pitcher_id if home_probable_pitcher_id else ""
        safe_away_pitcher_id = away_probable_pitcher_id if away_probable_pitcher_id else ""
        
        probable_starters = c.execute("""
            SELECT p.*, pt.player_id 
            FROM pitchers_per_unit p 
            JOIN PlayerTeams pt ON p.Name = pt.player_name 
            WHERE pt.player_id IN (?, ?) AND pt.team_id IN (?, ?)
        """, (safe_home_pitcher_id, safe_away_pitcher_id, home_team_id, away_team_id)).fetchall()
        
        pitcher_columns = [col[0] for col in c.description]
        
        for pitcher in probable_starters:
            pitcher_dict = {pitcher_columns[i]: pitcher[i] for i in range(len(pitcher_columns))}
            player_name = normalize_name(pitcher_dict.get("Name"))
            player_id = pitcher_dict.get("player_id")
            
            if player_name is None:
                print(f"Warning: Null Name for pitcher in game {game_id}")
                continue
                
            processed_pitchers.add(player_id)
            
            # Check injury status and return estimate
            injury_info = injuries.get(player_name, {'status': 'Active', 'return_estimate': None})
            injury_status = injury_info['status']
            return_estimate = injury_info['return_estimate']
            if return_estimate:
                return_estimate = datetime.strptime(return_estimate, '%Y-%m-%d').date()
            
            if injury_status in ('Out', '15-Day-IL', '60-Day-IL') and (not return_estimate or game_date_obj <= return_estimate):
                sorare_score = 0.0
                print(f"Player {player_name} is {injury_status}, setting projection to 0 until {return_estimate}")
            else:
                base_stats = {
                    'IP': pitcher_dict.get('IP', 0), 'SO': pitcher_dict.get('SO', 0), 'H': pitcher_dict.get('H', 0),
                    'ER': pitcher_dict.get('ER', 0), 'BB': pitcher_dict.get('BB', 0), 'HBP': pitcher_dict.get('HBP', 0),
                    'W': pitcher_dict.get('W', 0), 'SV': pitcher_dict.get('SV', 0)
                }
                adjusted_stats = {}
                for stat, value in base_stats.items():
                    park_adjustment = 1 / park_factors.get(stat, 1.0) if stat in ['H', 'ER', 'BB', 'HR'] and stat in park_factors else park_factors.get(stat, 1.0)
                    weather_factor = 1.0 if is_dome or stat != 'HR' else get_wind_effect(orientation or 0, wind_dir or 0, wind_speed or 0) * get_temp_adjustment(temp or 70)
                    adjusted_stats[stat] = value * park_adjustment * weather_factor
                    if stat == 'W':
                        adjusted_stats[stat] = value / 30
                    elif stat == 'SV':
                        adjusted_stats[stat] = 0
                sorare_score = calculate_sorare_pitcher_score(adjusted_stats, scoring_matrix) * 5
                
                # Apply 80% reduction for Day-to-Day up to and including return_estimate
                if injury_status == 'Day-To-Day' and return_estimate and game_date_obj <= return_estimate:
                    sorare_score *= 0.8
                    print(f"Player {player_name} is Day-To-Day, reducing projection to 80% until {return_estimate}")
            
            c.execute("INSERT INTO AdjustedProjections (player_name, game_id, game_date, sorare_score, game_week) VALUES (?, ?, ?, ?, ?)",
                     (player_name, game_id, game_date, sorare_score, game_week_id))
    
    # Process relief pitchers
    teams = c.execute("""
        SELECT DISTINCT home_team_id as team_id FROM Games WHERE date BETWEEN ? AND ?
        UNION
        SELECT DISTINCT away_team_id as team_id FROM Games WHERE date BETWEEN ? AND ?
    """, (start_date, end_date, start_date, end_date)).fetchall()
    team_ids = [team[0] for team in teams]
    
    probable_starter_ids = set()
    for game in games:
        home_pitcher_id = game[6]
        away_pitcher_id = game[7]
        if home_pitcher_id:
            probable_starter_ids.add(home_pitcher_id)
        if away_pitcher_id:
            probable_starter_ids.add(away_pitcher_id)
    
    for team_id in team_ids:
        relief_pitchers = c.execute("""
            SELECT p.*, pt.player_id 
            FROM pitchers_per_unit p 
            JOIN PlayerTeams pt ON p.Name = pt.player_name 
            WHERE pt.team_id = ? AND pt.player_id NOT IN ({})
        """.format(','.join(['?' for _ in probable_starter_ids]) if probable_starter_ids else "'dummy'"), 
        [team_id] + list(probable_starter_ids) if probable_starter_ids else [team_id]).fetchall()
        
        pitcher_columns = [col[0] for col in c.description]
        
        for pitcher in relief_pitchers:
            pitcher_dict = {pitcher_columns[i]: pitcher[i] for i in range(len(pitcher_columns))}
            player_name = normalize_name(pitcher_dict.get("Name"))
            player_id = pitcher_dict.get("player_id")
            
            if player_name is None or player_id is None:
                continue
                
            if player_id in processed_pitchers:
                continue
                
            # Check injury status and return estimate
            injury_info = injuries.get(player_name, {'status': 'Active', 'return_estimate': None})
            injury_status = injury_info['status']
            return_estimate = injury_info['return_estimate']
            if return_estimate:
                return_estimate = datetime.strptime(return_estimate, '%Y-%m-%d').date()
            
            team_games = c.execute("""
                SELECT id, stadium_id, date FROM Games 
                WHERE (home_team_id = ? OR away_team_id = ?) AND date BETWEEN ? AND ?
            """, (team_id, team_id, start_date, end_date)).fetchall()
            
            for game_data in team_games:
                game_id, stadium_id, game_date = game_data
                game_date_obj = datetime.strptime(game_date, '%Y-%m-%d').date()
                
                stadium_data = c.execute("""
                    SELECT s.is_dome, s.orientation, w.wind_dir, w.wind_speed, w.temp 
                    FROM Stadiums s 
                    LEFT JOIN WeatherForecasts w ON w.game_id = ?
                    WHERE s.id = ?
                """, (game_id, stadium_id)).fetchone()
                
                if not stadium_data:
                    continue
                    
                is_dome, orientation, wind_dir, wind_speed, temp = stadium_data
                
                park_factors = {row[0]: row[1] / 100 for row in c.execute(
                    "SELECT factor_type, value FROM ParkFactors WHERE stadium_id = ?", 
                    (stadium_id,)).fetchall()}
                
                if injury_status in ('Out', '15-Day-IL', '60-Day-IL') and (not return_estimate or game_date_obj <= return_estimate):
                    sorare_score = 0.0
                    print(f"Player {player_name} is {injury_status}, setting projection to 0 until {return_estimate}")
                else:
                    base_stats = {
                        'IP': pitcher_dict.get('IP', 0), 'SO': pitcher_dict.get('SO', 0), 'H': pitcher_dict.get('H', 0),
                        'ER': pitcher_dict.get('ER', 0), 'BB': pitcher_dict.get('BB', 0), 'HBP': pitcher_dict.get('HBP', 0),
                        'W': pitcher_dict.get('W', 0), 'SV': pitcher_dict.get('SV', 0)
                    }
                    adjusted_stats = {}
                    for stat, value in base_stats.items():
                        park_adjustment = 1 / park_factors.get(stat, 1.0) if stat in ['H', 'ER', 'BB', 'HR'] and stat in park_factors else park_factors.get(stat, 1.0)
                        weather_factor = 1.0 if is_dome or stat != 'HR' else get_wind_effect(orientation or 0, wind_dir or 0, wind_speed or 0) * get_temp_adjustment(temp or 70)
                        adjusted_stats[stat] = value * park_adjustment * weather_factor
                        if stat == 'W':
                            adjusted_stats[stat] = 0
                    
                    sorare_score = calculate_sorare_pitcher_score(adjusted_stats, scoring_matrix)
                    
                    # Apply 80% reduction for Day-to-Day up to and including return_estimate
                    if injury_status == 'Day-To-Day' and return_estimate and game_date_obj <= return_estimate:
                        sorare_score *= 0.8
                        print(f"Player {player_name} is Day-To-Day, reducing projection to 80% until {return_estimate}")
                
                c.execute("""
                    INSERT INTO AdjustedProjections (player_name, game_id, game_date, sorare_score, game_week) 
                    VALUES (?, ?, ?, ?, ?)
                """, (player_name, game_id, game_date, sorare_score, game_week_id))
    
    conn.commit()

# --- Main Function ---
def main(update_park_factors=False, update_rosters=False, specified_date=None):
    current_date = specified_date if specified_date else datetime.now().date()
    start_date, end_date = determine_game_week(current_date)
    print(f"Processing game week: {start_date} to {end_date}")
    conn = init_db()
    if update_park_factors:
        load_park_factors_from_csv(conn, 'park_data.csv')
    game_week_id = get_schedule(conn, start_date, end_date)
    fetch_weather_and_store(conn, start_date, end_date)
    populate_player_teams(conn, start_date, end_date, update_rosters=update_rosters)
    calculate_adjustments(conn, start_date, end_date, game_week_id)
    conn.close()
    print(f"Projections adjusted for game week: {start_date} to {end_date}")
    print(f"Game week ID: {game_week_id}")

if __name__ == "__main__":
    # main()
    # Examples:
    # main(update_park_factors=True)
     main(update_rosters=True)
    # main(specified_date=date(2025, 3, 27))