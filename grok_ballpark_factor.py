import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

# --- Database Initialization ---
def init_db(db_path='mlb_sorare.db'):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS Stadiums 
                 (id INTEGER PRIMARY KEY, name TEXT, lat REAL, lon REAL, orientation REAL, is_dome INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS ParkFactors 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, stadium_id INTEGER, factor_type TEXT, value REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS Games 
                 (id INTEGER PRIMARY KEY, date TEXT, time TEXT, stadium_id INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS WeatherForecasts 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, 
                  wind_dir REAL, wind_speed REAL, temp REAL, rain REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS BaseProjections 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, player_id INTEGER, game_id INTEGER, 
                  stat_type TEXT, value REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS AdjustedProjections 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, player_id INTEGER, game_id INTEGER, 
                  stat_type TEXT, value REAL)''')
    conn.commit()
    return conn

# --- Load Park Factors from CSV ---
def load_park_factors_from_csv(conn, csv_path='park_data.csv'):
    """Load park factors from a CSV file into the database."""
    df = pd.read_csv(csv_path)
    factor_types = ['Park Factor', 'wOBACon', 'xwOBACon', 'BACON', 'xBACON', 'HardHit', 
                    'R', 'OBP', 'H', '1B', '2B', '3B', 'HR', 'BB', 'SO']
    
    c = conn.cursor()
    # Clear existing park factors to avoid duplicates (optional, comment out if appending)
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

# --- Other Functions (Unchanged) ---
def get_schedule(conn, start_date, end_date):
    url = f"https://statsapi.mlb.com/api/v1/schedule?date={start_date}&end_date={end_date}&sportId=1"
    response = requests.get(url)
    data = response.json()
    c = conn.cursor()

    for date in data.get('dates', []):
        for game in date.get('games', []):
            game_id = game['gamePk']
            game_date = game['gameDate'].split('T')[0]
            game_time = game['gameDate'].split('T')[1].split('.')[0]
            stadium_id = game['venue']['id']
            c.execute("INSERT OR REPLACE INTO Games (id, date, time, stadium_id) VALUES (?, ?, ?, ?)",
                      (game_id, game_date, game_time, stadium_id))
    conn.commit()

def get_weather_open_meteo(lat, lon, forecast_time):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,precipitation&start_date={forecast_time.strftime('%Y-%m-%d')}&end_date={forecast_time.strftime('%Y-%m-%d')}"
    response = requests.get(url)
    data = response.json()
    hourly_times = [datetime.fromisoformat(t.replace('Z', '+00:00')) for t in data['hourly']['time']]
    closest_time = min(hourly_times, key=lambda x: abs(x - forecast_time))
    index = hourly_times.index(closest_time)
    return {
        'temp': data['hourly']['temperature_2m'][index],
        'wind_speed': data['hourly']['wind_speed_10m'][index],
        'wind_dir': data['hourly']['wind_direction_10m'][index],
        'rain': data['hourly']['precipitation'][index]
    }

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
        local_tz = pytz.timezone('America/New_York')
        local_time = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S')
        local_time = local_tz.localize(local_time)
        utc_time = local_time.astimezone(pytz.utc)
        weather = get_weather_open_meteo(lat, lon, utc_time)
        c.execute("INSERT INTO WeatherForecasts (game_id, wind_dir, wind_speed, temp, rain) VALUES (?, ?, ?, ?, ?)",
                  (game_id, weather['wind_dir'], weather['wind_speed'], weather['temp'], weather['rain']))
    conn.commit()

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

def calculate_adjustments(conn, start_date, end_date):
    c = conn.cursor()
    projections = c.execute("""
        SELECT bp.id, bp.player_id, bp.game_id, bp.stat_type, bp.value, 
               g.stadium_id, s.is_dome, w.wind_dir, w.wind_speed, w.temp, s.orientation 
        FROM BaseProjections bp 
        JOIN Games g ON bp.game_id = g.id 
        JOIN Stadiums s ON g.stadium_id = s.id 
        LEFT JOIN WeatherForecasts w ON g.id = w.game_id 
        WHERE g.date BETWEEN ? AND ?
    """, (start_date, end_date)).fetchall()
    
    for proj in projections:
        _, player_id, game_id, stat_type, base_value, stadium_id, is_dome, wind_dir, wind_speed, temp, orientation = proj
        pf = c.execute("SELECT value FROM ParkFactors WHERE stadium_id = ? AND factor_type = ?",
                       (stadium_id, stat_type)).fetchone()
        park_factor = pf[0] / 100 if pf else 1.0
        weather_factor = 1.0
        if not is_dome and stat_type in ['HR', 'HR_allowed']:
            wind_effect = get_wind_effect(orientation, wind_dir or 0, wind_speed or 0)
            temp_effect = get_temp_adjustment(temp or 70)
            weather_factor = wind_effect * temp_effect
        adjusted_value = base_value * park_factor * weather_factor
        c.execute("INSERT INTO AdjustedProjections (player_id, game_id, stat_type, value) VALUES (?, ?, ?, ?)",
                  (player_id, game_id, stat_type, adjusted_value))
    conn.commit()

# --- Main Execution with Flag ---
def main(update_park_factors=False):
    today = datetime.now().date()
    start_date = today.strftime('%Y-%m-%d')
    end_date = (today + timedelta(days=3)).strftime('%Y-%m-%d')
    
    conn = init_db()
    
    # Update park factors only if flag is True
    if update_park_factors:
        load_park_factors_from_csv(conn, 'park_data.csv')
    
    get_schedule(conn, start_date, end_date)
    fetch_weather_and_store(conn, start_date, end_date)
    calculate_adjustments(conn, start_date, end_date)
    
    conn.close()
    print(f"Projections adjusted for {start_date} to {end_date}")

if __name__ == "__main__":
    # Default run (no park factor update)
    main()
    
    # To update park factors, call with True
    # main(update_park_factors=True)