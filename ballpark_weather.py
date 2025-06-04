from typing import Optional
import requests
from datetime import datetime, timedelta
import pytz
import math
import pandas as pd

import logging

from utils import determine_game_week, get_sqlalchemy_engine, get_db_connection
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ballpark_weather")

def get_weather_nws(lat, lon, forecast_time):
    """
    Fetches weather data from the National Weather Service API, averaging conditions over a 3-hour game period.
    Returns average wind, temperature, precipitation probability, and barometric pressure for the entire game duration.
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
                    'pressure': 1013,  # Standard sea level pressure
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
                    
                    # Extract barometric pressure - NWS provides this in millibars/hectopascals
                    try:
                        if 'pressure' in period and period['pressure'] is not None:
                            # Pressure might be a dict with 'value' key or direct value
                            if isinstance(period['pressure'], dict):
                                pressure = period['pressure'].get('value', 1013)
                            else:
                                pressure = period['pressure']
                        else:
                            pressure = 1013  # Standard sea level pressure as default
                    except (KeyError, ValueError, TypeError):
                        pressure = 1013  # Default fallback
                        
                    forecast = {
                        'temp': period.get('temperature', 70),
                        'wind_speed': wind_speed,
                        'wind_dir': wind_dir_to_degrees(period.get('windDirection', 'N')),
                        'rain': period.get('probabilityOfPrecipitation', {}).get('value', 0),
                        'pressure': pressure,
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
        avg_pressure = sum(f['pressure'] * f['weight'] for f in relevant_forecasts) / total_weight
        
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
            'pressure': round(avg_pressure, 1),  # Keep one decimal for pressure precision
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
    
    # Check if pressure column exists and add it if not
    c.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'weather_forecasts' AND column_name = 'pressure'
    """)
    if not c.fetchone():
        # The pressure column doesn't exist, so add it
        c.execute("ALTER TABLE weather_forecasts ADD COLUMN pressure REAL")
        conn.commit()
        logger.info("Added pressure column to weather_forecasts table")
    
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
        # Updated to include pressure in the SELECT
        c.execute("""
            SELECT wind_dir, wind_speed, temp, rain, timestamp, pressure 
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
                # Update existing forecast - now includes pressure
                c.execute("""
                    UPDATE weather_forecasts 
                    SET wind_dir = %s, wind_speed = %s, temp = %s, rain = %s, timestamp = %s, pressure = %s
                    WHERE game_id = %s
                """, (weather['wind_dir'], weather['wind_speed'], weather['temp'], 
                     weather['rain'], timestamp, weather['pressure'], game_id))
                updated_forecasts += 1
            else:
                # Insert new forecast - now includes pressure
                c.execute("""
                    INSERT INTO weather_forecasts 
                    (game_id, wind_dir, wind_speed, temp, rain, timestamp, pressure) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (game_id, weather['wind_dir'], weather['wind_speed'], 
                     weather['temp'], weather['rain'], timestamp, weather['pressure']))
            
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

def get_weather_summary(is_dome, temp, wind_speed, wind_effect_label):
    """
    Get a readable summary of weather conditions.
    
    Args:
        is_dome: Whether the stadium is a dome
        temp: Temperature in Fahrenheit
        wind_speed: Wind speed in mph
        wind_effect_label: Wind effect label (In, Out, Cross)
        
    Returns:
        str: Weather summary
    """
    if is_dome:
        return "Dome stadium (weather not a factor)"
    
    if temp is None or wind_speed is None:
        return "Weather data unavailable"
    
    temp_desc = "hot" if temp > 80 else "cold" if temp < 60 else "mild"
    wind_desc = f"{int(wind_speed)} mph {wind_effect_label or 'neutral'}"
    
    return f"{int(temp)}°F ({temp_desc}), {wind_desc}"

def get_wind_effect(orientation, wind_dir, wind_speed):
    """
    Calculate the wind effect on home run probability.
    Returns a factor to multiply with base HR probability.
    
    Args:
        orientation: Stadium orientation in degrees (0-360)
        wind_dir: Wind direction in degrees (0-360)
        wind_speed: Wind speed in mph
        
    Returns:
        float: Multiplier for HR probability
    """
    angle_diff = (wind_dir - orientation + 180) % 360 - 180
    if abs(angle_diff) < 45 and wind_speed > 10:
        return 0.9  # Wind blowing in
    elif abs(angle_diff) > 135 and wind_speed > 10:
        return 1.1  # Wind blowing out
    return 1.0  # Neutral wind effect

def get_wind_effect_label(orientation, wind_dir):
    """
    Determines the wind effect label ("Out", "In", "Cross") based on the stadium's orientation and wind direction.
    
    Args:
        orientation: Stadium orientation in degrees (0-360)
        wind_dir: Wind direction in degrees (0-360)
        
    Returns:
        str: Wind effect label
    """
    if orientation is None or wind_dir is None:
        return "Neutral"

    angle_diff = (wind_dir - orientation + 180) % 360 - 180
    if abs(angle_diff) < 45:
        return "In"
    elif abs(angle_diff) > 135:
        return "Out"
    else:
        return "Cross"

def get_temp_adjustment(temp):
    """
    Calculate temperature adjustment for home run probability.
    
    Args:
        temp: Temperature in Fahrenheit
        
    Returns:
        float: Multiplier for HR probability
    """
    if temp > 80:
        return 1.05  # Hot weather increases HR
    elif temp < 60:
        return 0.95  # Cold weather decreases HR
    return 1.0  # Neutral temperature effect

def wind_dir_to_degrees(wind_dir):
    """
    Convert cardinal wind direction to degrees.
    
    Args:
        wind_dir: String wind direction (e.g., "N", "SE", "WSW")
        
    Returns:
        float: Wind direction in degrees
    """
    directions = {
        'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5, 'E': 90, 'ESE': 112.5,
        'SE': 135, 'SSE': 157.5, 'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
        'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
    }
    return directions.get(wind_dir.upper() if wind_dir else 'N', 0)

def generate_weather_report() -> str:
    """Generate a more user-friendly report of high-rain games, focusing on the date."""
    report_lines = []
    report_lines.append("\n## WEATHER WATCH: Potential Rain Impact ##\n")

    try:
        high_rain_games = fetch_high_rain_games_details()

        if high_rain_games.empty:
            report_lines.append("No games found with a high rain probability (>= 75%) in the forecast.")
        else:
            report_lines.append(f"Found {len(high_rain_games)} game(s) with >= 75% rain probability:")
            report_lines.append("These games *may* face delays or postponement:\n")

            for _, game in high_rain_games.iterrows():
                game_id = int(game['game_id'])
                stadium_name = game['stadium_name'] if pd.notna(game['stadium_name']) else "Unknown Stadium"
                away_team = f"Team {game['away_team_id']}"
                home_team = f"Team {game['home_team_id']}"

                game_date_str = "Date Unknown"
                try:
                    # Parse the date string (assuming YYYY-MM-DD format from DB)
                    game_date_obj = datetime.strptime(str(game['game_date']), '%Y-%m-%d').date()
                    # Format the date clearly
                    game_date_str = game_date_obj.strftime("%a, %b %d, %Y") # Format: Fri, Apr 11, 2025
                except ValueError as date_err:
                    print(f"Warning: Could not parse game date '{game['game_date']}' for game {game_id}. Error: {date_err}")
                except Exception as general_date_err:
                     print(f"Warning: An error occurred during date formatting for game {game_id}. Error: {general_date_err}")

                report_lines.append(f"  - Forecast: {game['rain']:.0f}% Rain - Date: {game_date_str} - Location: {stadium_name}") # Display formatted date
                report_lines.append(f"  - Gameday Link: https://baseballsavant.mlb.com/preview?game_pk={game_id}")
                report_lines.append("") # Add a blank line for readability

    except Exception as e:
        report_lines.append(f"Error generating weather report: {e}")
        print(f"Error details in generate_weather_report: {e}") # Added print for debugging

    return "\n".join(report_lines)

def fetch_high_rain_games_details(date_filter: Optional[str] = None):
    """
    Fetch games with rain risk, optionally filtered to today's games only (based on local_date).
    """
    engine = get_sqlalchemy_engine()
    today = datetime.now().date().isoformat()

    query = """
        SELECT
            wf.game_id,
            g.date AS game_date,
            g.local_date,
            g.time AS game_time_utc,
            wf.rain,
            wf.temp,
            wf.wind_speed,
            wf.wind_dir,
            g.home_team_id,
            g.away_team_id,
            s.name as stadium_name
        FROM weather_forecasts wf
        JOIN games g ON wf.game_id = g.id
        LEFT JOIN stadiums s ON g.stadium_id = s.id
        WHERE wf.rain >= 75
    """

    if date_filter == "today":
        query += " AND g.local_date = %s ORDER BY g.local_date ASC, g.time ASC"
        params = (today,)
    else:
        game_week = determine_game_week()
        start_str, end_str = game_week.split("_to_")
        query += " AND g.local_date BETWEEN %s AND %s ORDER BY g.local_date ASC, g.time ASC"
        params = (start_str, end_str)

    try:
        df = pd.read_sql(query, engine, params=params)
        df['rain'] = pd.to_numeric(df['rain'], errors='coerce')
        df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
        df['wind_speed'] = pd.to_numeric(df['wind_speed'], errors='coerce')
        df['wind_dir'] = pd.to_numeric(df['wind_dir'], errors='coerce')
        return df.dropna(subset=['rain'])

    except Exception as e:
        print(f"Error fetching high rain games: {e}")
        return pd.DataFrame(columns=[
            'game_id', 'game_date', 'local_date', 'game_time_utc', 'rain',
            'temp', 'wind_speed', 'wind_dir', 'home_team_id', 'away_team_id', 'stadium_name'
        ])


def find_pressure_hr_boosts():
    conn = get_sqlalchemy_engine()
    query = """
    SELECT 
        wf.game_id,
        wf.pressure,
        DATE(wf.timestamp) AS forecast_date,
        s.id AS stadium_id,
        s.name AS stadium_name
    FROM weather_forecasts wf
    JOIN games g ON wf.game_id = g.id
    JOIN stadiums s ON g.stadium_id = s.id
    """
    df = pd.read_sql_query(query, conn)

    # Calculate baseline pressure per stadium per day
    baseline = df.groupby(['stadium_id', 'forecast_date'])['pressure'].mean().reset_index()
    baseline.rename(columns={'pressure': 'baseline_pressure'}, inplace=True)

    # Merge baseline back into forecast data
    df = df.merge(baseline, on=['stadium_id', 'forecast_date'])

    # Calculate pressure drop from baseline
    df['pressure_drop'] = df['baseline_pressure'] - df['pressure']

    # Convert pressure drop to HR boost factor (e.g., 0.5 HR boost per 5 hPa drop)
    df['hr_boost'] = df['pressure_drop'].apply(lambda x: round((x / 5.0) * 0.1, 2) if x > 0 else 0)

    # Return rows with non-zero boost
    boosted_games = df[df['hr_boost'] > 0]

    return boosted_games[['game_id', 'stadium_name', 'forecast_date', 'pressure', 'baseline_pressure', 'hr_boost']]
