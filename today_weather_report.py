#!/usr/bin/env python
import os
import sqlite3
from datetime import datetime, date
import pytz
import pandas as pd
from grok_ballpark_factor import (
    init_db, get_schedule, 
    fetch_weather_and_store, 
    get_wind_effect_label, 
    get_wind_effect, 
    get_temp_adjustment,
    DATABASE_FILE
)

def get_team_name(conn, team_id):
    """Get the team name from the team ID"""
    try:
        c = conn.cursor()
        result = c.execute("SELECT name FROM Teams WHERE id = ?", (team_id,)).fetchone()
        return result[0] if result else f"Team {team_id}"
    except:
        return f"Team {team_id}"

def get_ballpark_name(conn, stadium_id):
    """Get the ballpark name from the stadium ID"""
    try:
        c = conn.cursor()
        result = c.execute("SELECT name FROM Stadiums WHERE id = ?", (stadium_id,)).fetchone()
        return result[0] if result else f"Stadium {stadium_id}"
    except:
        return f"Stadium {stadium_id}"

def get_weather_report_for_today():
    """Generate a weather report for today's games with scoring boost information"""
    # Initialize database connection
    conn = init_db()
    c = conn.cursor()
    
    # Get today's date in the correct format
    today = date.today().strftime("%Y-%m-%d")
    
    # Fetch today's schedule and update weather data
    print(f"Fetching schedule for {today}...")
    get_schedule(conn, today, today)
    
    print("Updating weather forecasts...")
    fetch_weather_and_store(conn, today, today)
    
    # Query for today's games with weather data
    query = """
    SELECT 
        g.id as game_id,
        g.date,
        g.time,
        g.stadium_id,
        g.home_team_id,
        g.away_team_id,
        g.wind_effect_label,
        s.name as stadium_name,
        s.is_dome,
        s.orientation,
        w.wind_dir,
        w.wind_speed,
        w.temp,
        w.rain
    FROM 
        Games g
    JOIN 
        Stadiums s ON g.stadium_id = s.id
    LEFT JOIN 
        WeatherForecasts w ON g.id = w.game_id
    WHERE 
        g.date = ?
    ORDER BY 
        g.time
    """
    
    games = c.execute(query, (today,)).fetchall()
    
    # If no games today, exit early
    if not games:
        print(f"No games scheduled for today ({today}).")
        conn.close()
        return
    
    # Create a DataFrame for easier manipulation
    columns = [
        'game_id', 'date', 'time', 'stadium_id', 'home_team_id', 'away_team_id',
        'wind_effect_label', 'stadium_name', 'is_dome', 'orientation', 'wind_dir',
        'wind_speed', 'temp', 'rain'
    ]
    games_df = pd.DataFrame(games, columns=columns)
    
    # Get park factors for additional context
    park_factors = {}
    for stadium_id in games_df['stadium_id'].unique():
        park_factors[stadium_id] = {
            row[0]: row[1] / 100 
            for row in c.execute("SELECT factor_type, value FROM ParkFactors WHERE stadium_id = ?", 
                                (stadium_id,)).fetchall()
        }
    
    # Display the weather report for each game
    print("\n===== TODAY'S MLB WEATHER REPORT =====")
    print(f"Date: {today}\n")
    
    for _, game in games_df.iterrows():
        game_time = game['time']
        if game_time.endswith('Z'):
            game_time = datetime.strptime(f"{game['date']}T{game_time}", "%Y-%m-%dT%H:%M:%SZ")
            game_time = game_time.replace(tzinfo=pytz.utc)
            local_time = game_time.astimezone(pytz.timezone('America/New_York'))
            display_time = local_time.strftime("%I:%M %p ET")
        else:
            display_time = datetime.strptime(game_time, "%H:%M:%S").strftime("%I:%M %p ET")
        
        home_team = get_team_name(conn, game['home_team_id'])
        away_team = get_team_name(conn, game['away_team_id'])
        
        print(f"🏟️  {away_team} @ {home_team} - {display_time}")
        print(f"   Ballpark: {game['stadium_name']}")
        
        if game['is_dome']:
            print("   📋 Dome stadium - Weather conditions will not affect scoring")
        else:
            # If no weather data is available
            if pd.isna(game['temp']) or pd.isna(game['wind_speed']):
                print("   ⚠️ No weather data available for this game")
                continue
                
            # Calculate scoring effects
            temp_effect = get_temp_adjustment(game['temp'])
            wind_effect = 1.0
            if not pd.isna(game['orientation']) and not pd.isna(game['wind_dir']) and not pd.isna(game['wind_speed']):
                wind_effect = get_wind_effect(game['orientation'], game['wind_dir'], game['wind_speed'])
            
            # Temperature description
            if game['temp'] > 85:
                temp_desc = "🔥 Hot"
            elif game['temp'] < 60:
                temp_desc = "❄️ Cold"
            else:
                temp_desc = "🌡️ Mild"
            
            # Wind description
            wind_label = game['wind_effect_label'] or "Neutral"
            if game['wind_speed'] > 15:
                wind_strength = "Strong"
            elif game['wind_speed'] > 8:
                wind_strength = "Moderate"
            else:
                wind_strength = "Light"
            
            # Rain risk description
            if game['rain'] > 70:
                rain_desc = "⚠️ High rain risk"
            elif game['rain'] > 30:
                rain_desc = "☂️ Possible rain"
            else:
                rain_desc = "☀️ Dry conditions"
            
            print(f"   🌡️ Temperature: {int(game['temp'])}°F ({temp_desc})")
            print(f"   💨 Wind: {int(game['wind_speed'])} mph {wind_label} ({wind_strength})")
            print(f"   ☔ Precipitation: {int(game['rain'])}% chance ({rain_desc})")
            
            # Scoring impact
            combined_effect = temp_effect * wind_effect
            if combined_effect > 1.03:
                print(f"   🚀 SCORING BOOST: +{(combined_effect-1)*100:.1f}% for home runs")
            elif combined_effect < 0.97:
                print(f"   📉 SCORING REDUCTION: {(combined_effect-1)*100:.1f}% for home runs")
            else:
                print(f"   ➖ NEUTRAL EFFECT on scoring")
                
            # Park factors
            stadium_factors = park_factors.get(game['stadium_id'], {})
            if 'HR' in stadium_factors:
                hr_factor = stadium_factors['HR']
                if hr_factor > 1.10:
                    print(f"   💥 HOME RUN FRIENDLY PARK: +{(hr_factor-1)*100:.1f}% HR rate")
                elif hr_factor < 0.90:
                    print(f"   🧱 PITCHER'S PARK: {(hr_factor-1)*100:.1f}% HR rate")
        
        print("\n" + "-" * 60 + "\n")
    
    conn.close()

if __name__ == "__main__":
    get_weather_report_for_today() 