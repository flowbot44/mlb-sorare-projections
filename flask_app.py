# app.py - Flask Application for Sorare MLB Lineup Optimizer

import os
import subprocess
import time
from flask import Flask, render_template, request, jsonify, send_file,  redirect, url_for
import pandas as pd
from datetime import datetime, timedelta, date
import math
import numpy as np
import pytz
import json
import traceback
import re

# Import existing functionality
from chatgpt_lineup_optimizer import (
    fetch_cards, fetch_projections, build_all_lineups,
    Config,
    build_daily_lineups, get_excluded_lineup_cards_details
)
from card_fetcher import SorareMLBClient
from injury_updates import fetch_injury_data, update_database
from grok_ballpark_factor import (
    main as update_projections,
    determine_game_week,
    get_schedule
)
from utils import (
    get_db_connection,
    calculate_hr_factors,
    get_top_hr_players,
    determine_daily_game_week
)
from ballpark_weather import (
        fetch_weather_and_store,
        fetch_high_rain_games_details, 
        get_weather_summary
)
import logging
import psycopg2 # Import psycopg2 for specific error handling

# Initialize Flask app
app = Flask(__name__)

# Add zip function to Jinja2 environment
app.jinja_env.globals.update(zip=zip)

# Script directory for running updates
script_dir = os.path.dirname(os.path.abspath(__file__))


# Default lineup parameters 
DEFAULT_ENERGY_LIMITS = {"rare": 50, "limited": 50}
BOOST_2025 = 2.0
STACK_BOOST = 2.0
ENERGY_PER_CARD = 25
DEFAULT_LINEUP_ORDER = [
    "Rare Champion",
    "Rare All-Star_1", "Rare All-Star_2", "Rare All-Star_3",
    "Rare Challenger_1", "Rare Challenger_2",
    "Limited All-Star_1", "Limited All-Star_2", "Limited All-Star_3",
    "Common Minors"
]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("flask_app")

def check_and_create_db():
    """Check if database exists and create it if not"""

    # Check if required tables exist
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check for existence of required tables in PostgreSQL
        tables_query = """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN ('hitters_vs_rhp_per_game', 'hitters_vs_lhp_per_game', 'hitters_per_game', 'pitchers_per_game', 'park_factors', 'stadiums')
        """
        cursor.execute(tables_query)
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]

        # Ensure all tables that are typically populated by the update process are present
        required_tables = [
            'hitters_vs_rhp_per_game', 'hitters_vs_lhp_per_game', 'hitters_per_game',
            'pitchers_per_game', 'park_factors', 'stadiums'
        ]
        missing_tables = [table for table in required_tables if table not in table_names]

        conn.close()
        
        if missing_tables:
            print(f"Missing tables: {missing_tables}")
            return False
        return True
    except psycopg2.Error as e:
        print(f"Database connection error during check_and_create_db: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during check_and_create_db: {e}")
        return False


def run_full_update():
    """Run all update scripts to refresh the database"""
    try:

        create_teams_table() # Ensure Teams table is created first

        # Step 1: Run fangraph_fetcher to download CSVs
        logger.info("Running fangraph_fetcher.py...")
        subprocess.run(["python3", os.path.join(script_dir, "fangraph_fetcher.py")], check=True)

        # Optional delay if needed
        time.sleep(5)

        # Step 2: Run park_factor_fetcher to download ballpark data
        logger.info("Running park_factor_fetcher.py...")
        subprocess.run(["python3", os.path.join(script_dir, "park_factor_fetcher.py")], check=True)

        # Optional delay
        time.sleep(2)

        # Step 3: Run depth_projection to process CSVs into PostgreSQL DB
        logger.info("Running depth_projection.py...")
        subprocess.run(["python3", os.path.join(script_dir, "depth_projection.py")], check=True)

        # Step 4: Run update_stadiums to ensure stadium data is current
        logger.info("Running update_stadiums.py...")
        subprocess.run(["python3", os.path.join(script_dir, "update_stadiums.py")], check=True)

        # Step 5: Update injury data
        logger.info("Running fetch_injury_data...")
        injury_data = fetch_injury_data()
        if injury_data:
            logger.info("Updating database with injury data...")
            update_database(injury_data)

        logger.info("Running update_projections...")
        # Step 6: Update projections using existing function
        update_projections()

        return True
    except subprocess.CalledProcessError as e:
       output = e.output.decode() if e.output else "<no output>"
       logger.error(f"Subprocess failed: {e.cmd} exited with code {e.returncode}. Output: {output}")
       return False
    except Exception as e:
        logger.error(f"Error during full update: {str(e)}")
        return False

def add_team_names_to_games(high_rain_games):
    """Add team names to a DataFrame of games"""
    if high_rain_games.empty:
        return high_rain_games

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Get home team names
        for i, game in high_rain_games.iterrows():
            home_team_id = game['home_team_id']
            away_team_id = game['away_team_id']

            # Get home team name
            home_team_query = "SELECT name FROM Teams WHERE id = %s"
            cursor.execute(home_team_query, (home_team_id,))
            home_team_name = cursor.fetchone()
            high_rain_games.at[i, 'home_team_name'] = home_team_name[0] if home_team_name else f"Team {home_team_id}"

            # Get away team name
            away_team_query = "SELECT name FROM Teams WHERE id = %s"
            cursor.execute(away_team_query, (away_team_id,))
            away_team_name = cursor.fetchone()
            high_rain_games.at[i, 'away_team_name'] = away_team_name[0] if away_team_name else f"Team {away_team_id}"

        cursor.close()
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

def create_teams_table():
    """Create and populate the Teams table if it doesn't exist, and add missing columns."""
    conn = get_db_connection()
    c = conn.cursor()
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

    # Create Teams table if it doesn't exist (PostgreSQL syntax)
    c.execute('''CREATE TABLE IF NOT EXISTS teams
                 (id INTEGER PRIMARY KEY, name TEXT UNIQUE, abbreviation TEXT)''') # Removed abbreviation here for IF NOT EXISTS
    conn.commit() # Commit the creation of the table before checking columns

    # Check if table is empty (or populate if abbreviation is new)
    c.execute("SELECT COUNT(*) FROM teams")
    result = c.fetchone()
    count = result[0] if result is not None else 0

    if count == 0: # Only populate if the table is truly empty
        # MLB team data
        
        # Insert team data (PostgreSQL syntax for ON CONFLICT)
        for team_id, name, abbreviation in teams:
            try:
                c.execute("""
                    INSERT INTO teams (id, name, abbreviation)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        abbreviation = EXCLUDED.abbreviation;
                """, (team_id, name, abbreviation))
            except psycopg2.IntegrityError as e:
                print(f"Skipping duplicate or integrity error for team {name}: {e}")
                conn.rollback() # Rollback on error
            except Exception as e:
                print(f"Error inserting team {name}: {e}")
                conn.rollback() # Rollback on other errors
                continue
        conn.commit()
        print(f"Populated Teams table with {len(teams)} MLB teams")
    else:
        print("Teams table already populated. Checking for abbreviation updates.")
        # If table is not empty, ensure all existing entries have abbreviations updated
        
        for team_id, name, abbreviation in teams:
            try:
                c.execute("""
                    INSERT INTO teams (id, name, abbreviation)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        abbreviation = EXCLUDED.abbreviation;
                """, (team_id, name, abbreviation))
            except psycopg2.IntegrityError as e:
                print(f"Skipping duplicate or integrity error for team {name}: {e}")
                conn.rollback() # Rollback on error
            except Exception as e:
                print(f"Error updating team {name}: {e}")
                conn.rollback() # Rollback on other errors
                continue
        conn.commit()


    c.close()
    conn.close()

def get_team_name(conn, team_id):
    """Get the team name from the team ID"""
    try:
        c = conn.cursor()
        c.execute("SELECT name FROM teams WHERE id = %s", (team_id,))
        result = c.fetchone()
        c.close()
        return result[0] if result else f"Team {team_id}"
    except Exception as e:
        print(f"Error fetching team name for ID {team_id}: {e}")
        return f"Team {team_id}"

def get_team_abbrev(conn, team_id):
    """Get the team abbreviation from the team ID"""
    try:
        c = conn.cursor()
        c.execute("SELECT abbreviation FROM teams WHERE id = %s", (team_id,))
        result = c.fetchone()
        c.close()
        return result[0] if result else f"T{team_id}"
    except Exception as e:
        print(f"Error fetching team abbreviation for ID {team_id}: {e}")
        return f"T{team_id}"

def get_ballpark_name(conn, stadium_id):
    """Get the ballpark name from the stadium ID"""
    try:
        c = conn.cursor()
        c.execute("SELECT name FROM stadiums WHERE id = %s", (stadium_id,))
        result = c.fetchone()
        c.close()
        return result[0] if result else f"Stadium {stadium_id}"
    except Exception as e:
        print(f"Error fetching stadium name for ID {stadium_id}: {e}")
        return f"Stadium {stadium_id}"

@app.route('/')
def index():
    """Render the main page with the lineup optimizer form"""
    # Check if database exists and is properly set up
    db_exists = check_and_create_db()
    print(f"Database exists: {db_exists}")
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

        # Server-side validation
    if not username:
        return jsonify({'error': "Sorare Username is required."}), 400

    try:
        rare_energy = int(rare_energy)
    except (ValueError, TypeError):
        return jsonify({'error': "Rare Energy is required and must be a number."}), 400

    try:
        limited_energy = int(limited_energy)
    except (ValueError, TypeError):
        return jsonify({'error': "Limited Energy is required and must be a number."}), 400

    try:
        boost_2025 = float(boost_2025)
    except (ValueError, TypeError):
        return jsonify({'error': "2025 Card Boost is required and must be a number."}), 400

    try:
        stack_boost = float(stack_boost)
    except (ValueError, TypeError):
        return jsonify({'error': "Stack Boost is required and must be a number."}), 400

    try:
        energy_per_card = int(energy_per_card)
    except (ValueError, TypeError):
        return jsonify({'error': "Energy per card must be a number."}), 400

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

    
    # Remove any leading non-alphanumeric characters (like ☰) from each lineup name
    custom_lineup_order = [
        re.sub(r'^[^\w]+', '', lineup.strip()) if lineup else ""
        for lineup in lineup_order.split(',')
    ]
    
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
            ignore_list=ignore_list,
            custom_lineup_order=custom_lineup_order,
            username=username
        )


        # Calculate total energy used
        total_energy_used = {"rare": 0, "limited": 0}
        for lineup_type in custom_lineup_order:
            data = lineups.get(lineup_type)
            if data and data["cards"]:
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

        conn = get_db_connection()

        # Get current date and game week dates
        current_date = datetime.now()
        try:
            current_game_week = determine_game_week()
            # Parse the game week to get start and end dates
            start_date_str, end_date_str = current_game_week.split("_to_")
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except Exception:
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
        JOIN adjusted_projections ap ON c.name = ap.player_name
        WHERE c.username = %s AND c.sealed = TRUE AND ap.game_date >= %s
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
        WHERE c.username = %s AND c.sealed = TRUE AND i.return_estimate IS NOT NULL
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
                except Exception: # Catch all exceptions if return_estimate is not a date string
                    # If return_estimate isn't a date, check if it contains keywords
                    # suggesting imminent return during the game week
                    keywords = ['day to day', 'game time decision', 'probable',
                               'questionable', 'today', 'tomorrow', '1-3 days',
                               'this week', 'expected back', 'returning']
                    if any(keyword in str(return_estimate).lower() for keyword in keywords):
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
            priority_order=custom_lineup_order,
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
        error_type = type(e).__name__
        tb = traceback.format_exc()
        logger.error(f"Error generating lineup: {error_type}: {e}\n{tb}")
        return jsonify({
            'error': f"Error generating lineup: {error_type}: {e}",
            'traceback': tb
        }), 500

@app.route('/platoon', methods=['GET', 'POST'])
def platoon():
    conn = get_db_connection()
    c = conn.cursor()

    # Create platoon_players table if it doesn't exist (PostgreSQL syntax)

    if request.method == 'POST':
        name = request.form['name']
        mlbam_id = request.form['mlbam_id']
        starts_vs = request.form['starts_vs']
        try:
            # Use %s placeholders for PostgreSQL
            c.execute(
                'INSERT INTO platoon_players (name, mlbam_id, starts_vs) VALUES (%s, %s, %s) ON CONFLICT (mlbam_id) DO UPDATE SET name = EXCLUDED.name, starts_vs = EXCLUDED.starts_vs',
                (name, mlbam_id, starts_vs)
            )
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error inserting/updating platoon player: {e}")


    c.execute('SELECT id, name, starts_vs FROM platoon_players')
    players = c.fetchall()
    c.execute('''
        SELECT DISTINCT Name, MLBAMID FROM hitters_per_game
        WHERE MLBAMID IS NOT NULL
    ''')
    name_id_pairs = c.fetchall()
    conn.close()

    return render_template('platoon.html', players=players, name_id_pairs=name_id_pairs)

@app.route('/platoon/delete/<int:id>', methods=['POST'])
def delete_platoon_player(id):
    conn = get_db_connection()
    c = conn.cursor()
    try:
        # Use %s placeholder for PostgreSQL
        c.execute('DELETE FROM platoon_players WHERE id = %s', (id,))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error deleting platoon player: {e}")
    finally:
        conn.close()
    return redirect(url_for('platoon'))


@app.route('/weather_report', methods=['GET'])
def weather_report():
    """Generate weather report HTML that can be cached"""
    try:
        is_daily = request.args.get("daily", "false").lower() == "true"

        if is_daily:
            high_rain_games = fetch_high_rain_games_details(date_filter="today")
        else:
            high_rain_games = fetch_high_rain_games_details()

        high_rain_games = format_game_dates(high_rain_games)
        high_rain_games = add_team_names_to_games(high_rain_games)

        weather_html = render_template(
            'partials/weather_report.html',
            high_rain_games=high_rain_games
        )

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

        # Check if there are any adjusted_projections for this game week
        query = "SELECT COUNT(*) FROM adjusted_projections WHERE game_week = %s"

        cursor.execute(query, (game_week_id,))
        result = cursor.fetchone()
        conn.close()

        # If count is greater than 0, projections exist
        if result is not None:
            return result[0] > 0
        else:
            return False
    except Exception as e:
        print(f"Error checking projections: {str(e)}")
        return False

@app.route('/update_data', methods=['POST'])
def update_data():
    """Update injury data and projections"""
    try:
        # Check if database exists and create it if needed
        db_exists = check_and_create_db()
        print(f"Database exists update_data: {db_exists}")
        # Determine current game week
        current_game_week = determine_game_week()

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

        # Check for existence of required tables in PostgreSQL
        # Changed from sqlite_master to information_schema.tables
        tables_query = """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name IN ('adjusted_projections', 'injuries', 'player_teams')
        """
        cursor.execute(tables_query)
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]

        required_tables = ['adjusted_projections', 'injuries', 'player_teams']
        missing_tables = [table for table in required_tables if table not in table_names]

        # Get game week info
        game_week = determine_game_week()

        if missing_tables:
            return jsonify({
                'status': 'missing',
                'message': 'Game week info missing. Run update injuries and projections.'
            })

        # Get count of projections for current game week
        cursor.execute(
            "SELECT COUNT(*) FROM adjusted_projections WHERE game_week = %s",
            (game_week,)
        )
        result = cursor.fetchone()
        proj_count = result[0] if result is not None else 0

        # ✅ Get the DB last modified time (Postgres version)
        # In PostgreSQL, there is no built-in "last modified" timestamp for the whole database file.
        # As an alternative, you can get the latest modification time from a key table (e.g., adjusted_projections)
        try:
            cursor.execute("SELECT MAX(timestamp) FROM weather_forecasts")
            db_modified_row = cursor.fetchone()
            logger.info(f"DB last modified row: {db_modified_row}")
            if db_modified_row and db_modified_row[0]:
                # Convert string to datetime if necessary
                utc_time = db_modified_row[0]
                if isinstance(utc_time, str):
                    try:
                        # Try parsing with microseconds first, then without
                        try:
                            utc_time = datetime.strptime(utc_time, "%Y-%m-%d %H:%M:%S.%f")
                        except ValueError:
                            utc_time = datetime.strptime(utc_time, "%Y-%m-%d %H:%M:%S")
                        utc_time = utc_time.replace(tzinfo=pytz.utc)
                    except Exception as parse_exc:
                        logger.error(f"Error parsing timestamp string: {parse_exc}")
                        db_modified = "Unknown"
                        utc_time = None
                if utc_time:
                    if utc_time.tzinfo is None:
                        utc_time = utc_time.replace(tzinfo=pytz.utc)
                    eastern = pytz.timezone('America/New_York')
                    db_modified = utc_time.astimezone(eastern).strftime("%Y-%m-%d %I:%M %p ET")
                else:
                    db_modified = "Unknown"
            else:
                db_modified = "Unknown"
        except Exception as e:
            logger.error(f"Error getting timestamp: {str(e)}")
            db_modified = "Unknown"

        conn.close() # Close connection here after all queries

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
        print(f"Error in check_db: {e}") # Log the error for debugging
        return jsonify({
            'status': 'error',
            'message': str(e)
        })


@app.route('/projections')
@app.route('/projections/<game_week_id>')
def show_projections(game_week_id=None):
    create_teams_table()
    conn = get_db_connection()
    c = conn.cursor()

    # If no game week specified, use current one
    if not game_week_id:
        current_date = datetime.now().date()
        game_week_id = determine_game_week(current_date)

    start_date, end_date = game_week_id.split('_to_')

    # Get all games in the date range
    c.execute("""
        SELECT g.id, g.local_date, g.stadium_id, g.home_team_id, g.away_team_id,
               ht.name AS home_team_name, at.name AS away_team_name,
               s.name AS stadium_name, g.wind_effect_label
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.id
        JOIN teams at ON g.away_team_id = at.id
        JOIN stadiums s ON g.stadium_id = s.id
        WHERE g.local_date BETWEEN %s AND %s
        ORDER BY g.local_date 
    """, (start_date, end_date))
    games = c.fetchall()

    # Get all projections for each game
    game_projections = {}
    for game in games:
        game_id = game[0]
        stadium_id = game[2]
        home_team_id = game[3]
        away_team_id = game[4]

        # Get home team projections

        c.execute("""
            SELECT ap.player_name, ap.sorare_score,
                h.R_per_game, h.RBI_per_game, h.HR_per_game,
                h.SB_per_game, h.CS_per_game, h.BB_per_game as h_BB_per_game, h.HBP_per_game as h_HBP_per_game,
                h.singles_per_game as h_1B_per_game, h.doubles_per_game as h_2B_per_game, 
                h.triples_per_game as h_3B_per_game, h.K_per_game as h_K_per_game, 
                p.IP_per_game, p.K_per_game,  p.W_per_game, p.S_per_game, p.HLD_per_game,
                p.H_per_game, p.ER_per_game, p.BB_per_game, p.HBP_per_game,
                CASE WHEN p.IP_per_game > 0 THEN 'P' ELSE 'H' END as position
            FROM adjusted_projections ap
            LEFT JOIN hitters_per_game h ON ap.mlbam_id = h.mlbamid
            LEFT JOIN pitchers_per_game p ON ap.mlbam_id = p.mlbamid
            WHERE ap.game_id = %s AND ap.team_id = %s
            ORDER BY ap.sorare_score DESC
        """, (game_id, home_team_id))
        home_players = c.fetchall()

        # Get away team projections
        # Renamed columns with backticks to be compatible with PostgreSQL
        c.execute("""
            SELECT ap.player_name, ap.sorare_score,
                   h.R_per_game, h.RBI_per_game, h.HR_per_game,
                   h.SB_per_game, h.CS_per_game, h.BB_per_game as h_BB_per_game, h.HBP_per_game as h_HBP_per_game,
                   h.singles_per_game as h_1B_per_game, h.doubles_per_game as h_2B_per_game, 
                   h.triples_per_game as h_3B_per_game, h.K_per_game as h_K_per_game, -- Changed backticks to double quotes
                   p.IP_per_game, p.K_per_game, p.W_per_game, p.S_per_game, p.HLD_per_game,
                   p.H_per_game, p.ER_per_game, p.BB_per_game, p.HBP_per_game,
                   CASE WHEN p.IP_per_game > 0 THEN 'P' ELSE 'H' END as position
            FROM adjusted_projections ap
            LEFT JOIN hitters_per_game h ON ap.mlbam_id = h.mlbamid
            LEFT JOIN pitchers_per_game p ON ap.mlbam_id = p.mlbamid
            WHERE ap.game_id = %s AND ap.team_id = %s
            ORDER BY ap.sorare_score DESC
        """, (game_id, away_team_id))
        away_players = c.fetchall()

        # Get weather data
        c.execute("""
            SELECT wind_dir, wind_speed, temp, rain
            FROM weather_forecasts
            WHERE game_id = %s
        """, (game_id,))
        weather = c.fetchone()
        # Get park factors by stadium name
        c.execute("""
            SELECT factor_type, value
            FROM park_factors
            WHERE stadium_id = %s
        """, (stadium_id,))
        park_factors = c.fetchall()

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
    c.execute("""
        SELECT g.id, g.local_date,
               ht.name AS home_team_name, at.name AS away_team_name,
               g.home_probable_pitcher_id IS NULL AS home_probable_missing,
               g.away_probable_pitcher_id IS NULL AS away_probable_missing
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.id
        JOIN teams at ON g.away_team_id = at.id
        WHERE (g.home_probable_pitcher_id IS NULL OR g.away_probable_pitcher_id IS NULL)
          AND g.local_date BETWEEN %s AND %s
        ORDER BY g.local_date
    """, (start_date, end_date))
    missing_pitchers = c.fetchall()

    # Generate Baseball Savant links for missing probable pitchers
    missing_pitchers_links = [
        {
            'game_id': game[0],
            'date': game[1],
            'home_team': game[2],
            'away_team': game[3],
            'home_probable_missing': bool(game[4]),
            'away_probable_missing': bool(game[5]),
            'savant_link': f"https://baseballsavant.mlb.com/preview?game_pk={game[0]}"
        }
        for game in missing_pitchers
    ]

    # Get available game weeks for navigation
    # This query might not be ideal for ordering if game_week string contains year-month-day ranges
    # It might be better to order by the start_date part of the game_week string if possible
    c.execute("""
    SELECT DISTINCT game_week
    FROM adjusted_projections
    ORDER BY game_week
    """)
    game_weeks_raw = c.fetchall()
    conn.close()

    # Convert list of tuples to a list of strings
    game_weeks = [week[0] for week in game_weeks_raw]

    return render_template('projections.html',
                            active_page='projections',
                            games=games,
                            missing_pitchers_links=missing_pitchers_links,
                            current_game_week=game_week_id,
                            game_weeks=game_weeks, # Now game_weeks is a list of strings
                            game_projections=game_projections)


@app.template_filter('score_to_width')
def score_to_width(score):
    return min(float(score) * 2, 100)

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
        games g
    JOIN
        stadiums s ON g.stadium_id = s.id
    LEFT JOIN
        weather_forecasts w ON g.id = w.game_id
    WHERE
        g.local_date = %s
    ORDER BY
        g.time
    """

    c.execute(query, (today,))
    games = c.fetchall()
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
        if isinstance(game_time, str) and game_time.endswith('Z'): # Ensure it's a string before splitting
            game_time = datetime.strptime(f"{game['date']}T{game_time}", "%Y-%m-%dT%H:%M:%SZ")
            game_time = game_time.replace(tzinfo=pytz.utc)
            local_time = game_time.astimezone(pytz.timezone('America/New_York'))
            game_info['time'] = local_time.strftime("%I:%M %p ET")
        elif isinstance(game_time, str): # Handle other string formats
            try:
                game_info['time'] = datetime.strptime(game_time, "%H:%M:%S").strftime("%I:%M %p ET")
            except ValueError:
                game_info['time'] = game_time # Fallback if parsing fails
        else: # Handle if it's already a datetime object or other types
            game_info['time'] = game_time

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


@app.route("/daily", methods=["GET"])
def daily_form():
    return render_template("daily.html")

@app.route("/daily-lineup", methods=["POST"])
def generate_daily_lineup():
    username = request.form["username"]
    ignore_players = request.form.get("ignore_players", "").split(",")
    boost_2025 = float(request.form.get("boost_2025", 0.0))
    stack_boost = float(request.form.get("stack_boost", 2.0))
    rare_energy = int(request.form.get('rare_energy', 0))
    limited_energy = int(request.form.get('limited_energy', 0))

    ignore_list = [p.strip() for p in ignore_players if p.strip()]

    energy_limits = {
        "rare": rare_energy,
        "limited": limited_energy
    }
    lineups = build_daily_lineups(username, energy_limits, boost_2025, stack_boost, ignore_list)

    return render_template("partials/daily_results.html", lineups=lineups, username=username, game_week=datetime.now().strftime('%Y-%m-%d'))

@app.route('/update_daily', methods=['POST'])
def update_daily():
    """Update injury data and projections"""
    try:
        # Check if database exists and create it if needed
        db_exists = check_and_create_db()
        print(f"Database exists update_data: {db_exists}")
        # Determine current game week
        current_game_week = determine_daily_game_week()

        # Just update injury data and projections
        print(f"Updating existing daily projections for game week {current_game_week}")
        injury_data = fetch_injury_data()
        if injury_data:
            update_database(injury_data)

        # Update projections
        update_projections(daily=True)

        return jsonify({
            'success': True,
            'message': f"Data updated successfully for today in game week {current_game_week}."
        })
    except Exception as e:
        return jsonify({'error': f"Error updating data: {str(e)}"})

@app.route('/fetch_excluded_lineups')
def fetch_excluded_lineups():
    username = request.args.get('username')
    if not username:
        return jsonify({"error": "Username is required"}), 400

    game_week = determine_daily_game_week() # Dynamically determine the current game week

    excluded_lineups = get_excluded_lineup_cards_details(username, game_week)
    return render_template('partials/excluded_lineups.html', excluded_lineups=excluded_lineups)


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
    create_teams_table() # Call this here to ensure Teams table is created on app start
    # Get port from environment variable (for deployment) or default to 5000
    port = int(os.environ.get('PORT', 5000))

    # Run the app
    app.run(host='0.0.0.0', port=port, debug=True)