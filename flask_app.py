# app.py - Flask Application for Sorare MLB Lineup Optimizer

import os
import subprocess
import time
from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
from datetime import datetime, timedelta, date
import sqlite3
import math
import numpy as np
import pytz

# Import existing functionality
from chatgpt_lineup_optimizer import (
    fetch_cards, fetch_projections, build_all_lineups, 
    Config, get_db_connection,
    fetch_high_rain_games_details
)
from card_fetcher import SorareMLBClient
from injury_updates import fetch_injury_data, update_database
from grok_ballpark_factor import (
    main as update_projections, 
    determine_game_week,
    get_schedule,
    fetch_weather_and_store
)
from utils import (
    DATABASE_FILE, 
    get_wind_effect, 
    get_wind_effect_label, 
    get_temp_adjustment,
    calculate_hr_factors,
    get_weather_summary,
    get_top_hr_players
)
import logging

# Initialize Flask app
app = Flask(__name__)

# Add zip function to Jinja2 environment
app.jinja_env.globals.update(zip=zip)

# Script directory for running updates
script_dir = os.path.dirname(os.path.abspath(__file__))

# Default lineup parameters (same as in discord_bot.py)
DEFAULT_ENERGY_LIMITS = {"rare": 50, "limited": 50}
BOOST_2025 = 5.0
STACK_BOOST = 2.0
ENERGY_PER_CARD = 25
DEFAULT_LINEUP_ORDER = [
    "Rare Champion",
    "Rare All-Star_1", "Rare All-Star_2", "Rare All-Star_3",
    "Rare Challenger_1", "Rare Challenger_2",
    "Limited All-Star_1", "Limited All-Star_2", "Limited All-Star_3",
    "Limited Challenger_1", "Limited Challenger_2",
    "Common Minors"
]



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

def check_and_create_db():
    """Check if database exists and create it if not"""
    # Use the same database path as the rest of the application
    db_path = DATABASE_FILE
    
    if not os.path.exists(db_path):
        # Database doesn't exist, need to create and populate it
        # Make sure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        return False
    
    # Check if required tables exist
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check for existence of required tables
        tables_query = """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name IN ('hitters_per_game', 'pitchers_per_game', 'ParkFactors', 'Stadiums')
        """
        tables = cursor.execute(tables_query).fetchall()
        table_names = [t[0] for t in tables]
        
        required_tables = ['hitters_per_game', 'pitchers_per_game', 'ParkFactors', 'Stadiums']
        missing_tables = [table for table in required_tables if table not in table_names]
        
        conn.close()
        
        if missing_tables:
            return False
        return True
    except Exception:
        # Error accessing database, likely needs to be created
        return False

def run_full_update():
    """Run all update scripts to refresh the database"""
    try:

        create_teams_table()

        # Step 1: Run fangraph_fetcher to download CSVs
        print("Running fangraph_fetcher.py...")
        subprocess.run(["python3", os.path.join(script_dir, "fangraph_fetcher.py")], check=True)

        # Optional delay if needed
        time.sleep(5)

        # Step 2: Run park_factor_fetcher to download ballpark data
        print("Running park_factor_fetcher.py...")
        subprocess.run(["python3", os.path.join(script_dir, "park_factor_fetcher.py")], check=True)

        # Optional delay
        time.sleep(2)

        # Step 3: Run depth_projection to process CSVs into SQLite DB
        print("Running depth_projection.py...")
        subprocess.run(["python3", os.path.join(script_dir, "depth_projection.py")], check=True)

        # Step 4: Run update_stadiums to ensure stadium data is current
        print("Running update_stadiums.py...")
        subprocess.run(["python3", os.path.join(script_dir, "update_stadiums.py")], check=True)
        
        # Step 5: Update injury data
        injury_data = fetch_injury_data()
        if injury_data:
            update_database(injury_data)
        
        # Step 6: Update projections using existing function
        update_projections()

        return True
    except Exception as e:
        print(f"Error during full update: {str(e)}")
        return False

def add_team_names_to_games(high_rain_games):
    """Add team names to a DataFrame of games"""
    if high_rain_games.empty:
        return high_rain_games
        
    try:
        conn = get_db_connection()
        # Get home team names
        for i, game in high_rain_games.iterrows():
            home_team_id = game['home_team_id']
            away_team_id = game['away_team_id']
            
            # Get home team name
            home_team_query = "SELECT name FROM Teams WHERE id = ?"
            home_team_name = conn.execute(home_team_query, (home_team_id,)).fetchone()
            high_rain_games.at[i, 'home_team_name'] = home_team_name[0] if home_team_name else f"Team {home_team_id}"
            
            # Get away team name
            away_team_query = "SELECT name FROM Teams WHERE id = ?"
            away_team_name = conn.execute(away_team_query, (away_team_id,)).fetchone()
            high_rain_games.at[i, 'away_team_name'] = away_team_name[0] if away_team_name else f"Team {away_team_id}"
        
        conn.close()
    except Exception as e:
        print(f"Error adding team names: {e}")
    
    return high_rain_games
    
def format_game_dates(high_rain_games):
    """Format game dates in a DataFrame of games"""
    if high_rain_games.empty:
        return high_rain_games
        
    for i, game in high_rain_games.iterrows():
        try:
            # Parse the date string (assuming YYYY-MM-DD format from DB)
            game_date_obj = datetime.strptime(str(game['game_date']), '%Y-%m-%d').date()
            # Format the date clearly
            high_rain_games.at[i, 'game_date_formatted'] = game_date_obj.strftime("%a, %b %d, %Y")
        except Exception:
            high_rain_games.at[i, 'game_date_formatted'] = "Date Unknown"
    
    return high_rain_games

@app.route('/')
def index():
    """Render the main page with the lineup optimizer form"""
    # Check if database exists and is properly set up
    db_exists = check_and_create_db()
    
    # Fetch high rain games for weather report
    try:
        high_rain_games = fetch_high_rain_games_details()
        
        # Format game dates and add team names
        high_rain_games = format_game_dates(high_rain_games)
        high_rain_games = add_team_names_to_games(high_rain_games)
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        high_rain_games = pd.DataFrame()  # Empty DataFrame if error
    
    return render_template('index.html', 
                          active_page='home', 
                          game_week=determine_game_week(),
                          default_rare_energy=DEFAULT_ENERGY_LIMITS["rare"],
                          default_limited_energy=DEFAULT_ENERGY_LIMITS["limited"],
                          default_boost_2025=BOOST_2025,
                          default_stack_boost=STACK_BOOST,
                          default_energy_per_card=ENERGY_PER_CARD,
                          default_lineup_order=",".join(DEFAULT_LINEUP_ORDER),
                          db_exists=db_exists,
                          high_rain_games=high_rain_games)

@app.route('/generate', methods=['POST'])
def generate_lineup():
    """Generate lineup based on form inputs and return HTML content"""
    # Get form data
    username = request.form.get('username')
    rare_energy = int(request.form.get('rare_energy', DEFAULT_ENERGY_LIMITS["rare"]))
    limited_energy = int(request.form.get('limited_energy', DEFAULT_ENERGY_LIMITS["limited"]))
    boost_2025 = float(request.form.get('boost_2025', BOOST_2025))
    stack_boost = float(request.form.get('stack_boost', STACK_BOOST))
    energy_per_card = int(request.form.get('energy_per_card', ENERGY_PER_CARD))
    lineup_order = request.form.get('lineup_order', ','.join(DEFAULT_LINEUP_ORDER))
    ignore_players = request.form.get('ignore_players', '')
    ignore_games = request.form.get('ignore_games', '')
    
    # Parse ignore players list
    ignore_list = []
    if ignore_players:
        ignore_list = [name.strip() for name in ignore_players.split(',') if name.strip()]
    
    # Parse ignore games list
    ignore_game_ids = []
    if ignore_games:
        try:
            ignore_game_ids = [int(game_id.strip()) for game in ignore_games.split(',') if (game_id := game.strip()).isdigit()]
        except Exception as e:
            return jsonify({'error': f"Error parsing game IDs: {str(e)}. Make sure all IDs are valid integers."})
    
    # Parse custom lineup order
    try:
        custom_lineup_order = [lineup.strip() for lineup in lineup_order.split(',')]
        Config.PRIORITY_ORDER = custom_lineup_order
    except Exception as e:
        # Fallback to default order on error
        Config.PRIORITY_ORDER = DEFAULT_LINEUP_ORDER
        return jsonify({'error': f"Error parsing lineup order: {str(e)}. Using default order."})
    
    # Set energy limits
    energy_limits = {
        "rare": rare_energy,
        "limited": limited_energy
    }
    
    try:
        # FIRST: Fetch latest cards from Sorare API for this user
        sorare_client = SorareMLBClient()
        result = sorare_client.get_user_mlb_cards(username)
        
        if not result:
            return jsonify({'error': f"Failed to fetch cards for user {username} from Sorare."})
        
        # THEN: Fetch cards from the database
        cards_df = fetch_cards(username)
        
        # Pass ignore_game_ids to fetch_projections
        projections_df = fetch_projections(ignore_game_ids=ignore_game_ids)
        
        if cards_df.empty:
            return jsonify({'error': f"No eligible cards found for {username}."})
        if projections_df.empty:
            return jsonify({'error': f"No projections available for game week {determine_game_week()}. Update the database first."})
        
        # Generate lineups
        lineups = build_all_lineups(
            cards_df=cards_df,
            projections_df=projections_df,
            energy_limits=energy_limits,
            boost_2025=boost_2025,
            stack_boost=stack_boost,
            energy_per_card=energy_per_card,
            ignore_list=ignore_list
        )
        
        # Calculate total energy used
        total_energy_used = {"rare": 0, "limited": 0}
        for lineup_type in Config.PRIORITY_ORDER:
            data = lineups[lineup_type]
            if data["cards"]:
                total_energy_used["rare"] += data["energy_used"]["rare"]
                total_energy_used["limited"] += data["energy_used"]["limited"]
        
        # Find players missing projections
        merged = cards_df.merge(projections_df, left_on="name", right_on="player_name", how="left")
        missing_projections = merged[merged["total_projection"].isna()]
        missing_projections_list = []
        
        if not missing_projections.empty:
            for _, row in missing_projections.iterrows():
                missing_projections_list.append({"name": row['name'], "slug": row['slug']})
        
        # Fetch sealed cards data for the template
        db_path = DATABASE_FILE
        conn = sqlite3.connect(db_path)
        
        # Get current date and game week dates
        current_date = datetime.now()
        try:
            current_game_week = determine_game_week()
            # Parse the game week to get start and end dates
            start_date_str, end_date_str = current_game_week.split("_to_")
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except:
            # Fallback to 7 days if game week format is unexpected
            start_date = current_date
            end_date = current_date + timedelta(days=7)
        
        # Part 1: Get sealed cards with projections
        query = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions, 
               COUNT(ap.game_id) as game_count, 
               SUM(ap.sorare_score) as total_projected_score,
               AVG(ap.sorare_score) as avg_projected_score,
               MIN(ap.game_date) as next_game_date
        FROM cards c
        JOIN AdjustedProjections ap ON c.name = ap.player_name
        WHERE c.username = ? AND c.sealed = 1 AND ap.game_date >= ?
        GROUP BY c.slug, c.name, c.year, c.rarity, c.positions
        ORDER BY next_game_date ASC
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (username, current_date.strftime('%Y-%m-%d')))
        projection_results = cursor.fetchall()
        
        projections_df = None
        if projection_results:
            # Convert to DataFrame
            columns = ['Slug', 'Name', 'Year', 'Rarity', 'Positions', 
                      'Upcoming Games', 'Total Projected Score', 'Avg Score/Game', 'Next Game Date']
            projections_df = pd.DataFrame(projection_results, columns=columns)
            
            # Format the dataframe - round the scores to 2 decimal places
            projections_df['Total Projected Score'] = projections_df['Total Projected Score'].round(2)
            projections_df['Avg Score/Game'] = projections_df['Avg Score/Game'].round(2)
        
        # Part 2: Get injured sealed cards
        query = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions, i.status, 
               i.description, i.return_estimate, i.team
        FROM cards c
        JOIN injuries i ON c.name = i.player_name
        WHERE c.username = ? AND c.sealed = 1 AND i.return_estimate IS NOT NULL
        """
        
        cursor.execute(query, (username,))
        injury_results = cursor.fetchall()
        
        injured_df = None
        if injury_results:
            # Filter injuries with return dates within game week
            soon_returning = []
            
            for result in injury_results:
                return_estimate = result[7]
                
                # Check if return_estimate contains a date string
                try:
                    # Try different date formats
                    for date_format in ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%d/%m/%Y']:
                        try:
                            return_date = datetime.strptime(return_estimate, date_format)
                            if start_date <= return_date <= end_date:
                                soon_returning.append(result)
                            break
                        except ValueError:
                            continue
                except:
                    # If return_estimate isn't a date, check if it contains keywords
                    # suggesting imminent return during the game week
                    keywords = ['day to day', 'game time decision', 'probable', 
                               'questionable', 'today', 'tomorrow', '1-3 days',
                               'this week', 'expected back', 'returning']
                    if any(keyword in return_estimate.lower() for keyword in keywords):
                        soon_returning.append(result)
            
            if soon_returning:
                columns = ['Slug', 'Name', 'Year', 'Rarity', 'Positions', 'Status', 
                          'Description', 'Return Estimate', 'Team']
                injured_df = pd.DataFrame(soon_returning, columns=columns)
        
        conn.close()
        
        # Render the template with all the necessary data
        lineup_html = render_template(
            'partials/lineup_results.html',
            lineups=lineups,
            energy_limits=energy_limits,
            username=username,
            boost_2025=boost_2025,
            stack_boost=stack_boost,
            energy_per_card=energy_per_card,
            game_week=determine_game_week(),
            priority_order=Config.PRIORITY_ORDER,
            lineup_slots=Config.LINEUP_SLOTS,
            total_energy_used=total_energy_used,
            missing_projections=missing_projections_list,
            current_date=current_date,
            start_date=start_date,
            end_date=end_date,
            projections_df=projections_df,
            injured_df=injured_df
        )
        
        # Return success with HTML content
        return jsonify({
            'success': True,
            'lineup_html': lineup_html,
            'ignored_games': len(ignore_game_ids)
        })
        
    except Exception as e:
        return jsonify({'error': f"Error generating lineup: {str(e)}"})

@app.route('/weather_report', methods=['GET'])
def weather_report():
    """Generate weather report HTML that can be cached"""
    try:
        # Use the same code from index route to get high rain games
        high_rain_games = fetch_high_rain_games_details()
        
        # Format game dates and add team names
        high_rain_games = format_game_dates(high_rain_games)
        high_rain_games = add_team_names_to_games(high_rain_games)
        
        # Render the partial template directly
        weather_html = render_template('partials/weather_report.html', high_rain_games=high_rain_games)
        
        return jsonify({
            'success': True,
            'weather_html': weather_html
        })
    except Exception as e:
        return jsonify({'error': f"Error generating weather report: {str(e)}"})
    
@app.route('/fetch_cards', methods=['POST'])
def fetch_user_cards():
    username = request.form.get('username')
    
    if not username:
        return jsonify({'error': "Username is required"})
    
    try:
        sorare_client = SorareMLBClient()
        result = sorare_client.get_user_mlb_cards(username)
        
        if result:
            return jsonify({
                'success': True,
                'message': f"Successfully fetched {len(result['cards'])} cards for {username}",
                'card_count': len(result['cards'])
            })
        else:
            return jsonify({'error': f"Failed to fetch cards for {username}"})
    except Exception as e:
        return jsonify({'error': f"Error fetching cards: {str(e)}"})

@app.route('/download_lineup/<username>')
def download_lineup(username):
    """Allow downloading the generated lineup file"""
    filename = f"lineups/{username}.txt"
    if os.path.exists(filename):
        return send_file(filename, as_attachment=True)
    else:
        return "Lineup file not found. Please generate it first.", 404

def check_if_projections_exist(game_week_id):
    """Check if projections already exist for a specific game week"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if there are any AdjustedProjections for this game week
        query = "SELECT COUNT(*) FROM AdjustedProjections WHERE game_week = ?"
        result = cursor.execute(query, (game_week_id,)).fetchone()
        conn.close()
        
        # If count is greater than 0, projections exist
        return result[0] > 0
    except Exception as e:
        print(f"Error checking projections: {str(e)}")
        return False

@app.route('/update_data', methods=['POST'])
def update_data():
    """Update injury data and projections"""
    try:
        # Check if database exists and create it if needed
        db_exists = check_and_create_db()
        
        # Determine current game week
        current_game_week = determine_game_week()
        
        if not db_exists:
            # Database doesn't exist, run full update
            success = run_full_update()
            if not success:
                return jsonify({'error': "Failed to initialize database. Check logs for details."})
        else:
            # Check if projections exist for the current game week
            projections_exist = check_if_projections_exist(current_game_week)
            
            if not projections_exist:
                # First time for this game week - run full update
                print(f"No projections found for game week {current_game_week} - running full update")
                success = run_full_update()
                if not success:
                    return jsonify({'error': "Failed to run full update. Check logs for details."})
            else:
                # Just update injury data and projections
                print(f"Updating existing projections for game week {current_game_week}")
                injury_data = fetch_injury_data()
                if injury_data:
                    update_database(injury_data)
                
                # Update projections
                update_projections()
        
        return jsonify({
            'success': True,
            'message': f"Data updated successfully for game week {current_game_week}."
        })
    except Exception as e:
        return jsonify({'error': f"Error updating data: {str(e)}"})

@app.route('/check_db')
def check_db():
    """Check database connection and return status"""
    try:
        db_exists = check_and_create_db()
        
        if not db_exists:
            return jsonify({
                'status': 'missing',
                'message': 'Database does not exist or is missing required tables. Run update to initialize.'
            })
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check for existence of required tables
        tables_query = """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name IN ('AdjustedProjections', 'injuries', 'PlayerTeams')
        """
        tables = cursor.execute(tables_query).fetchall()
        table_names = [t[0] for t in tables]
       
        required_tables = ['AdjustedProjections', 'injuries', 'PlayerTeams']
        missing_tables = [table for table in required_tables if table not in table_names]
        
        # Get game week info
        game_week = determine_game_week()

        if missing_tables:
            return jsonify({
                'status': 'missing',
                'message': 'Game week info missing. Run update injuries and projections.'
            })
        
        # Get count of projections for current game week
        proj_count = cursor.execute(
            "SELECT COUNT(*) FROM AdjustedProjections WHERE game_week = ?", 
            (game_week,)
        ).fetchone()[0]
        
        conn.close()

        # ✅ Get the DB last modified time
        if os.path.exists(DATABASE_FILE):
            db_modified = datetime.fromtimestamp(os.path.getmtime(DATABASE_FILE)).strftime("%Y-%m-%d %H:%M")
        else:
            db_modified = "Unknown"
        
        return jsonify({
            'status': 'connected',
            'tables': table_names,
            'game_week': game_week,
            'projection_count': proj_count,
            'projections_exist': proj_count > 0,
            'needs_full_update': proj_count == 0,
            'last_updated': db_modified
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

@app.route('/run_full_update', methods=['POST'])
def full_update_route():
    """Trigger a full update of the database from scratch"""
    try:
        success = run_full_update()
        if success:
            return jsonify({
                'success': True,
                'message': "Full database update completed successfully."
            })
        else:
            return jsonify({
                'error': "Failed to complete full database update. Check logs for details."
            })
    except Exception as e:
        return jsonify({'error': f"Error during full update: {str(e)}"})

def create_teams_table():
    """Create and populate the Teams table if it doesn't exist"""
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Create Teams table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS Teams 
                 (id INTEGER PRIMARY KEY, name TEXT, abbreviation TEXT)''')
    
    # Check if table is empty
    count = c.execute("SELECT COUNT(*) FROM Teams").fetchone()[0]
    
    if count == 0:
        # MLB team data
        teams = [
            (108, "Los Angeles Angels", "LAA"),
            (109, "Arizona Diamondbacks", "ARI"),
            (110, "Baltimore Orioles", "BAL"),
            (111, "Boston Red Sox", "BOS"),
            (112, "Chicago Cubs", "CHC"),
            (113, "Cincinnati Reds", "CIN"),
            (114, "Cleveland Guardians", "CLE"),
            (115, "Colorado Rockies", "COL"),
            (116, "Detroit Tigers", "DET"),
            (117, "Houston Astros", "HOU"),
            (118, "Kansas City Royals", "KC"),
            (119, "Los Angeles Dodgers", "LAD"),
            (120, "Washington Nationals", "WSH"),
            (121, "New York Mets", "NYM"),
            (133, "Oakland Athletics", "OAK"),
            (134, "Pittsburgh Pirates", "PIT"),
            (135, "San Diego Padres", "SD"),
            (136, "Seattle Mariners", "SEA"),
            (137, "San Francisco Giants", "SF"),
            (138, "St. Louis Cardinals", "STL"),
            (139, "Tampa Bay Rays", "TB"),
            (140, "Texas Rangers", "TEX"),
            (141, "Toronto Blue Jays", "TOR"),
            (142, "Minnesota Twins", "MIN"),
            (143, "Philadelphia Phillies", "PHI"),
            (144, "Atlanta Braves", "ATL"),
            (145, "Chicago White Sox", "CWS"),
            (146, "Miami Marlins", "MIA"),
            (147, "New York Yankees", "NYY"),
            (158, "Milwaukee Brewers", "MIL")
        ]
        
        # Insert team data
        c.executemany("INSERT INTO Teams (id, name, abbreviation) VALUES (?, ?, ?)", teams)
        conn.commit()
        print(f"Populated Teams table with {len(teams)} MLB teams")
    
    conn.close()

@app.route('/projections')
@app.route('/projections/<game_week_id>')
def show_projections(game_week_id=None):
    create_teams_table()
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # If no game week specified, use current one
    if not game_week_id:
        current_date = datetime.now().date()
        game_week_id = determine_game_week(current_date)
    
    start_date, end_date = game_week_id.split('_to_')
    
    # Get all games in the date range
    games = c.execute("""
        SELECT g.id, g.date, g.time, g.stadium_id, g.home_team_id, g.away_team_id, 
               ht.name AS home_team_name, at.name AS away_team_name,
               s.name AS stadium_name, g.wind_effect_label
        FROM Games g
        JOIN Teams ht ON g.home_team_id = ht.id
        JOIN Teams at ON g.away_team_id = at.id
        JOIN Stadiums s ON g.stadium_id = s.id
        WHERE g.local_date BETWEEN ? AND ?
        ORDER BY g.date, g.time
    """, (start_date, end_date)).fetchall()

    # Get all projections for each game
    game_projections = {}
    for game in games:
        game_id = game['id']
        
        # Get home team projections
        home_players = c.execute("""
            SELECT ap.player_name, ap.sorare_score,
                h.R_per_game, h.RBI_per_game, h.HR_per_game,
                h.SB_per_game, h.CS_per_game, h.BB_per_game as h_BB_per_game, h.HBP_per_game as h_HBP_per_game,
                `1B_per_game` as h_1B_per_game, `2B_per_game` as h_2B_per_game,
                `3B_per_game` as h_3B_per_game, h.K_per_game as h_K_per_game,
                p.IP_per_game, p.K_per_game,  p.W_per_game, p.S_per_game, p.HLD_per_game,
                p.H_per_game, p.ER_per_game, p.BB_per_game, p.HBP_per_game,
                CASE WHEN p.IP_per_game > 0 THEN 'P' ELSE 'H' END as position
            FROM AdjustedProjections ap
            LEFT JOIN hitters_per_game h ON ap.mlbam_id = h.MLBAMID
            LEFT JOIN pitchers_per_game p ON ap.mlbam_id = p.MLBAMID
            WHERE ap.game_id = ? AND ap.team_id = ?
            ORDER BY ap.sorare_score DESC
        """, (game_id, game['home_team_id'])).fetchall()
        
        # Get away team projections
        away_players = c.execute("""
            SELECT ap.player_name, ap.sorare_score, 
                   h.R_per_game, h.RBI_per_game, h.HR_per_game,
                   h.SB_per_game, h.CS_per_game, h.BB_per_game as h_BB_per_game, h.HBP_per_game as h_HBP_per_game,
                   `1B_per_game` as h_1B_per_game, `2B_per_game` as h_2B_per_game, 
                                 `3B_per_game` as h_3B_per_game, h.K_per_game as h_K_per_game,
                   p.IP_per_game, p.K_per_game, p.W_per_game, p.S_per_game, p.HLD_per_game,
                   p.H_per_game, p.ER_per_game, p.BB_per_game, p.HBP_per_game,
                   CASE WHEN p.IP_per_game > 0 THEN 'P' ELSE 'H' END as position
            FROM AdjustedProjections ap
            LEFT JOIN hitters_per_game h ON ap.mlbam_id = h.MLBAMID
            LEFT JOIN pitchers_per_game p ON ap.mlbam_id = p.MLBAMID
            WHERE ap.game_id = ? AND ap.team_id = ?
            ORDER BY ap.sorare_score DESC
        """, (game_id, game['away_team_id'])).fetchall()
        
        # Get weather data
        weather = c.execute("""
            SELECT wind_dir, wind_speed, temp, rain
            FROM WeatherForecasts
            WHERE game_id = ?
        """, (game_id,)).fetchone()
        
        # Get park factors by stadium name
        stadium_id = game['stadium_id']
        park_factors = c.execute("""
            SELECT factor_type, value 
            FROM ParkFactors 
            WHERE stadium_id = ?
        """, (stadium_id,)).fetchall()
        
        # Convert park factors to dictionary
        park_factors_dict = {row[0]: row[1] / 100 for row in park_factors}
        
        game_projections[game_id] = {
            'game_info': game,
            'home_players': home_players,
            'away_players': away_players,
            'weather': weather,
            'park_factors': park_factors_dict
        }
    
    # Query for games missing probable pitchers
    missing_pitchers = c.execute("""
        SELECT g.id, g.date, g.time, 
               ht.name AS home_team_name, at.name AS away_team_name,
               g.home_probable_pitcher_id IS NULL AS home_probable_missing,
               g.away_probable_pitcher_id IS NULL AS away_probable_missing
        FROM Games g
        JOIN Teams ht ON g.home_team_id = ht.id
        JOIN Teams at ON g.away_team_id = at.id
        WHERE (g.home_probable_pitcher_id IS NULL OR g.away_probable_pitcher_id IS NULL)
          AND g.local_date BETWEEN ? AND ?
        ORDER BY g.date, g.time
    """, (start_date, end_date)).fetchall()
    
    # Generate Baseball Savant links for missing probable pitchers
    missing_pitchers_links = [
        {
            'game_id': game['id'],
            'date': game['date'],
            'time': game['time'],
            'home_team': game['home_team_name'],
            'away_team': game['away_team_name'],
            'home_probable_missing': bool(game['home_probable_missing']),
            'away_probable_missing': bool(game['away_probable_missing']),
            'savant_link': f"https://baseballsavant.mlb.com/preview?game_pk={game['id']}"
        }
        for game in missing_pitchers
    ]
      
    # Get available game weeks for navigation
    game_weeks = c.execute("""
        SELECT DISTINCT game_week 
        FROM AdjustedProjections 
        ORDER BY game_date
    """).fetchall()
    
    conn.close()
    
    return render_template('projections.html',
                          active_page='projections', 
                          games=games,
                          missing_pitchers_links=missing_pitchers_links,
                          current_game_week=game_week_id,
                          game_weeks=game_weeks,
                          game_projections=game_projections)


@app.template_filter('score_to_width')
def score_to_width(score):
    return min(float(score) * 2, 100)

def get_team_name(conn, team_id):
    """Get the team name from the team ID"""
    try:
        c = conn.cursor()
        result = c.execute("SELECT name FROM Teams WHERE id = ?", (team_id,)).fetchone()
        return result[0] if result else f"Team {team_id}"
    except:
        return f"Team {team_id}"

def get_team_abbrev(conn, team_id):
    """Get the team abbreviation from the team ID"""
    try:
        c = conn.cursor()
        result = c.execute("SELECT abbreviation FROM Teams WHERE id = ?", (team_id,)).fetchone()
        return result[0] if result else f"T{team_id}"
    except:
        return f"T{team_id}"

def get_ballpark_name(conn, stadium_id):
    """Get the ballpark name from the stadium ID"""
    try:
        c = conn.cursor()
        result = c.execute("SELECT name FROM Stadiums WHERE id = ?", (stadium_id,)).fetchone()
        return result[0] if result else f"Stadium {stadium_id}"
    except:
        return f"Stadium {stadium_id}"

def get_game_weather_data(specified_date=None):
    """Get weather and HR odds data for all games on a given date (default today)"""
    # Initialize database connection
    conn = get_db_connection()
    c = conn.cursor()
    
    # Get date in the correct format
    today = specified_date or date.today().strftime("%Y-%m-%d")
    
    # Fetch schedule and update weather data
    get_schedule(conn, today, today)
    fetch_weather_and_store(conn, today, today)
    
    # Query for games with weather data
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
        g.local_date = ?
    ORDER BY 
        g.time
    """
    
    games = c.execute(query, (today,)).fetchall()
    
    # If no games today, return empty data
    if not games:
        conn.close()
        return {"date": today, "games": [], "hr_rankings": []}
    
    # Create a DataFrame for easier manipulation
    columns = [
        'game_id', 'date', 'time', 'stadium_id', 'home_team_id', 'away_team_id',
        'wind_effect_label', 'stadium_name', 'is_dome', 'orientation', 'wind_dir',
        'wind_speed', 'temp', 'rain'
    ]
    games_df = pd.DataFrame(games, columns=columns)
    
    # Process each game's weather and calculate HR factors
    game_data = []
    hr_odds_rankings = []
    
    for _, game in games_df.iterrows():
        game_info = {}
        
        # Game identifiers
        game_info['game_id'] = int(game['game_id'])
        game_info['stadium_name'] = game['stadium_name']
        game_info['home_team'] = get_team_name(conn, game['home_team_id'])
        game_info['away_team'] = get_team_name(conn, game['away_team_id'])
        game_info['home_abbrev'] = get_team_abbrev(conn, game['home_team_id'])
        game_info['away_abbrev'] = get_team_abbrev(conn, game['away_team_id'])
        
        # Game time formatting
        game_time = game['time']
        if game_time.endswith('Z'):
            game_time = datetime.strptime(f"{game['date']}T{game_time}", "%Y-%m-%dT%H:%M:%SZ")
            game_time = game_time.replace(tzinfo=pytz.utc)
            local_time = game_time.astimezone(pytz.timezone('America/New_York'))
            game_info['time'] = local_time.strftime("%I:%M %p ET")
        else:
            game_info['time'] = datetime.strptime(game_time, "%H:%M:%S").strftime("%I:%M %p ET")
        
        # Calculate HR factors using the utility function
        hr_factors = calculate_hr_factors(
            conn, 
            game['stadium_name'],
            bool(game['is_dome']),
            game['orientation'],
            game['wind_dir'],
            game['wind_speed'],
            game['temp']
        )
        
        # Add HR factor details to the game info
        game_info['hr_factor'] = hr_factors['hr_factor']
        game_info['park_hr_factor'] = hr_factors['park_hr_factor']
        game_info['hr_details'] = hr_factors['details']
        game_info['hr_classification'] = hr_factors['classification']
        game_info['hr_class_color'] = hr_factors['class_color']
        
        # Summarize weather conditions
        game_info['weather_summary'] = get_weather_summary(
            bool(game['is_dome']),
            game['temp'],
            game['wind_speed'],
            game['wind_effect_label']
        )
        
        # Add to game data list
        game_data.append(game_info)
        
        # Add home and away teams to HR rankings
        hr_odds_rankings.append({
            'game_id': int(game['game_id']),
            'team': game_info['home_team'],
            'abbrev': game_info['home_abbrev'],
            'opponent': game_info['away_team'],
            'is_home': True,
            'hr_factor': hr_factors['hr_factor'],
            'park_hr_factor': hr_factors['park_hr_factor'],
            'stadium': game_info['stadium_name'],
            'time': game_info['time']
        })
        
        hr_odds_rankings.append({
            'game_id': int(game['game_id']),
            'team': game_info['away_team'],
            'abbrev': game_info['away_abbrev'],
            'opponent': game_info['home_team'],
            'is_home': False,
            'hr_factor': hr_factors['hr_factor'],
            'park_hr_factor': hr_factors['park_hr_factor'],
            'stadium': game_info['stadium_name'],
            'time': game_info['time']
        })
    
    # Sort HR odds rankings from best to worst
    hr_odds_rankings = sorted(hr_odds_rankings, key=lambda x: x['hr_factor'], reverse=True)
    
    # Get top players with HR potential
    hr_players = get_top_hr_players(conn, today, hr_odds_rankings)
    
    conn.close()
    
    return {
        "date": today,
        "games": game_data,
        "hr_rankings": hr_odds_rankings,
        "hr_players": hr_players
    }

@app.route('/hr-odds')
@app.route('/hr-odds/<date_str>')
def hr_odds(date_str=None):
    """Show home run odds based on weather conditions for today or a specific date"""
    # If date_str is provided, use it, otherwise use today's date
    specified_date = None
    if date_str:
        try:
            # Validate date format
            specified_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            # If invalid date, fall back to today
            pass
    
    # Get the HR odds data
    data = get_game_weather_data(specified_date)
    
    return render_template('hr_odds.html', 
                          active_page='hr-odds',
                          date=data['date'],
                          games=data['games'],
                          team_rankings=data['hr_rankings'],
                          players=data['hr_players'])

if __name__ == '__main__':
    # Ensure the lineups directory exists
    os.makedirs('lineups', exist_ok=True)
    create_teams_table()
    # Get port from environment variable (for deployment) or default to 5000
    port = int(os.environ.get('PORT', 5000))
    
    # Run the app
    app.run(host='0.0.0.0', port=port, debug=True)