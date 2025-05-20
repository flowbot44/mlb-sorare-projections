# utils.py
import unicodedata
from datetime import datetime, timedelta, date
import os
import math
import sqlite3
from typing import Optional
import pytz

DATABASE_FILE = os.environ.get('DATABASE_PATH', 'mlb_sorare.db')

# Dictionary for specific name translations
NAME_TRANSLATIONS = {
    "C J ABRAMS": "CJ ABRAMS",
    "DANIEL LYNCH": "DANIEL LYNCH IV",
    # Add more translations as needed, e.g.:
    "J T REALMUTO": "JT REALMUTO",
    "J D MARTINEZ": "JD MARTINEZ",
    "C J CRON": "CJ CRON",
    "A J PUK": "AJ PUK",
    

    # "HYUN JIN RYU": "HYUNJIN RYU"
}

def normalize_name(name):
    """Normalize a name by removing accents, converting to uppercase, replacing hyphens with spaces, removing periods, and applying specific translations."""
    if not name or (hasattr(name, 'isna') and name.isna()):  # Handle None or pandas NaN
        return name
    # Remove accents and normalize Unicode
    normalized = ''.join(c for c in unicodedata.normalize('NFKD', str(name)) if unicodedata.category(c) != 'Mn')
    # Apply standard transformations
    normalized = normalized.upper().replace('-', ' ').replace('.', '').strip()
    # Apply specific translations if present
    return NAME_TRANSLATIONS.get(normalized, normalized)

# New function
def determine_game_week(current_date=None):
    """
    Determine the game week string (e.g., '2025-03-31_to_2025-04-03') based on the current date:
    - If run Tuesday through Friday: return the upcoming/current Friday-Sunday period
    - If run Saturday through Monday: return the upcoming/current Monday-Thursday period
    - Special case for season start (March 27-30, 2025)
    Returns a string in the format 'YYYY-MM-DD_to_YYYY-MM-DD'.
    """
    if current_date is None:
        current_date = datetime.now().date()
    elif isinstance(current_date, str):
        current_date = datetime.strptime(current_date, '%Y-%m-%d').date()
    season_start = date(2025, 3, 27)
    if current_date <= date(2025, 3, 29):
        start_date = season_start
        end_date = date(2025, 3, 29)
    else:
        day_of_week = current_date.weekday()
        if 1 <= day_of_week <= 4:  # Tuesday to Friday
            days_until_friday = (4 - day_of_week) % 7
            start_date = current_date + timedelta(days=days_until_friday)
            end_date = start_date + timedelta(days=2)
        else:  # Saturday, Sunday, or Monday
            days_until_monday = (0 - day_of_week) % 7
            start_date = current_date + timedelta(days=days_until_monday)
            end_date = start_date + timedelta(days=3)

    return f"{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}"

def get_platoon_start_side_by_mlbamid(mlbam_id: int) -> Optional[str]:
    """
    Returns the handedness ('L' or 'R') of pitcher the player starts against,
    based on their platoon profile.

    Returns None if the player is not in the platoon_players table.
    """
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        'SELECT starts_vs FROM platoon_players WHERE mlbam_id = ?', 
        (mlbam_id,)
    )
    row = cursor.fetchone()
    conn.close()

    return row['starts_vs'] if row else None


def main():
    print(determine_game_week())

if __name__ == "__main__":
    main()

def get_db_connection():
    """Create a database connection and handle directory creation if needed"""
    db_dir = os.path.dirname(DATABASE_FILE)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    
    conn = sqlite3.connect(DATABASE_FILE)
    return conn

# --- HR Factor Utility Functions ---

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

def calculate_hr_factors(conn, stadium_name, is_dome, orientation, wind_dir, wind_speed, temp):
    """
    Calculate combined HR factors for a game based on stadium and weather.
    
    Args:
        conn: Database connection
        stadium_name: Name of the stadium
        is_dome: Whether the stadium is a dome (boolean)
        orientation: Stadium orientation in degrees
        wind_dir: Wind direction in degrees
        wind_speed: Wind speed in mph
        temp: Temperature in Fahrenheit
        
    Returns:
        dict: Dictionary with HR factor and detailed breakdown
    """
    # First, find the stadium_id from the stadium_name
    cursor = conn.cursor()
    stadium_id_result = cursor.execute("SELECT id FROM Stadiums WHERE name = ?", (stadium_name,)).fetchone()
    stadium_id = stadium_id_result[0] if stadium_id_result else None
    
    if not stadium_id:
        print(f"Warning: Could not find stadium ID for stadium name: {stadium_name}")
        # Use a default set of park factors
        park_factors = {'HR': 1.0, 'R': 1.0, '1B': 1.0, '2B': 1.0, '3B': 1.0}
    else:
        # Get park factors by stadium ID
        park_factors_data = cursor.execute("""
            SELECT factor_type, value 
            FROM ParkFactors 
            WHERE stadium_id = ?
        """, (stadium_id,)).fetchall()
        
        # Convert to dictionary with values divided by 100
        park_factors = {row[0]: row[1] / 100 for row in park_factors_data}
        
        if not park_factors:
            print(f"Warning: No park factors found for stadium ID: {stadium_id} ({stadium_name})")
            park_factors = {'HR': 1.0, 'R': 1.0, '1B': 1.0, '2B': 1.0, '3B': 1.0}
    
    # Get the HR park factor (default to 1.0 if not found)
    hr_factor = 1.0
    details = []
    
    # Get the park HR factor (default to 1.0 if not found)
    park_hr_factor = park_factors.get('HR', 1.0)
    
    # Add park factor
    if 'HR' in park_factors:
        hr_factor *= park_hr_factor
        details.append({
            'type': 'park',
            'effect': park_hr_factor,
            'description': f"{'Increases' if park_hr_factor > 1 else 'Decreases'} HR by {abs(park_hr_factor - 1) * 100:.1f}%"
        })
    
    # Add weather factors if not a dome
    if not is_dome:
        # Temperature effect
        if temp is not None:
            temp_effect = get_temp_adjustment(temp)
            if temp_effect != 1.0:
                hr_factor *= temp_effect
                details.append({
                    'type': 'temperature',
                    'value': temp,
                    'effect': temp_effect,
                    'description': f"{'Hot' if temp_effect > 1 else 'Cold'} temperature ({int(temp)}°F) {'increases' if temp_effect > 1 else 'decreases'} HR by {abs(temp_effect - 1) * 100:.1f}%"
                })
        
        # Wind effect
        if orientation is not None and wind_dir is not None and wind_speed is not None:
            wind_effect = get_wind_effect(orientation, wind_dir, wind_speed)
            wind_effect_label = get_wind_effect_label(orientation, wind_dir)
            
            if wind_effect != 1.0:
                hr_factor *= wind_effect
                wind_type = "Outward" if wind_effect > 1 else "Inward"
                details.append({
                    'type': 'wind',
                    'value': wind_speed,
                    'direction': wind_effect_label,
                    'effect': wind_effect,
                    'description': f"{wind_type} wind ({int(wind_speed)} mph) {'increases' if wind_effect > 1 else 'decreases'} HR by {abs(wind_effect - 1) * 100:.1f}%"
                })
    
    # Determine classification
    if hr_factor >= 1.15:
        classification = "Excellent"
        class_color = "success"
    elif hr_factor >= 1.05:
        classification = "Good"
        class_color = "primary"
    elif hr_factor >= 0.95:
        classification = "Neutral"
        class_color = "secondary"
    elif hr_factor >= 0.85:
        classification = "Poor"
        class_color = "warning"
    else:
        classification = "Very Poor"
        class_color = "danger"
    
    return {
        'hr_factor': hr_factor,
        'park_hr_factor': park_hr_factor,
        'details': details,
        'classification': classification,
        'class_color': class_color
    }

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

def get_top_hr_players(conn, game_date, team_rankings, limit=25):
    """
    Get the top players most likely to hit HRs based on individual stats and game factors.
    
    Args:
        conn: Database connection
        game_date: Date to get HR players for
        team_rankings: List of dictionaries with team HR factors
        limit: Maximum number of players to return
        
    Returns:
        list: Top players with their HR probabilities
    """
    player_data = []
    c = conn.cursor()
    
    # Get mapping from team name to mlb team ID
    team_id_map = {}
    teams = c.execute("SELECT id, name FROM Teams").fetchall()
    for team_id, team_name in teams:
        team_id_map[team_name] = team_id
    
    # Query player projections - focusing on home run hitters
    for team_rank in team_rankings:
        team_name = team_rank['team']
        if team_name not in team_id_map:
            continue
            
        team_id = team_id_map[team_name]
        hr_factor = team_rank['hr_factor']
        
        # Get players on this team with their projected stats
        query = """
        SELECT 
            h.Name, 
            p.mlbam_id,
            h.HR_per_game
        FROM 
            hitters_per_game h
        JOIN
            PlayerTeams p ON h.MLBAMID = p.mlbam_id
        WHERE 
            p.team_id = ? AND
            h.HR_per_game > 0
        ORDER BY
            h.HR_per_game DESC
        """
        
        player_results = c.execute(query, (team_id,)).fetchall()
        
        for player in player_results:
            name, mlbam_id, hr_per_game = player
            
            # Apply the game's HR factor to the player's HR rate
            adjusted_hr_per_game = hr_per_game * hr_factor
            
            # Calculate HR probability for this game
            hr_odds = 1 - (1 - adjusted_hr_per_game) ** 1  # Probability of at least 1 HR
            
            # Add player to the list
            player_data.append({
                'name': name,
                'mlbam_id': mlbam_id,
                'team': team_name,
                'team_abbrev': team_rank['abbrev'],
                'opponent': team_rank['opponent'],
                'is_home': team_rank['is_home'],
                'game_id': team_rank['game_id'],
                'stadium': team_rank['stadium'],
                'game_time': team_rank['time'],
                'hr_per_game': hr_per_game,
                'adjusted_hr_per_game': adjusted_hr_per_game,
                'hr_odds_pct': hr_odds * 100,  # Convert to percentage
                'game_hr_factor': hr_factor
            })
    
    # Sort by adjusted HR probability
    sorted_players = sorted(player_data, key=lambda x: x['hr_odds_pct'], reverse=True)
    
    # Return top N players
    return sorted_players[:limit]