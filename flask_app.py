# app.py - Flask Application for Sorare MLB Lineup Optimizer

import os
import subprocess
import time
from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
from datetime import datetime
import sqlite3

# Import existing functionality
from chatgpt_lineup_optimizer import (
    fetch_cards, fetch_projections, build_all_lineups, 
    save_lineups, Config, get_db_connection,
    generate_sealed_cards_report
)
from card_fetcher import SorareMLBClient
from injury_updates import fetch_injury_data, update_database
from grok_ballpark_factor import main as update_projections, determine_game_week

# Initialize Flask app
app = Flask(__name__)

# Script directory for running updates
script_dir = os.path.dirname(os.path.abspath(__file__))

# Default lineup parameters (same as in discord_bot.py)
DEFAULT_ENERGY_LIMITS = {"rare": 150, "limited": 275}
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

def check_and_create_db():
    """Check if database exists and create it if not"""
    db_path = os.path.join(script_dir, 'mlb_sorare.db')  # Assuming this is the database path
    
    if not os.path.exists(db_path):
        # Database doesn't exist, need to create and populate it
        return False
    
    # Check if required tables exist
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check for existence of required tables
        tables_query = """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name IN ('cards', 'AdjustedProjections', 'injuries', 'PlayerTeams')
        """
        tables = cursor.execute(tables_query).fetchall()
        table_names = [t[0] for t in tables]
        
        required_tables = ['cards', 'AdjustedProjections', 'injuries', 'PlayerTeams']
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
        
        return True
    except Exception as e:
        print(f"Error during full update: {str(e)}")
        return False

@app.route('/')
def index():
    """Render the main page with the lineup optimizer form"""
    # Check if database exists and is properly set up
    db_exists = check_and_create_db()
    
    return render_template('index.html', 
                          game_week=Config.GAME_WEEK,
                          default_rare_energy=DEFAULT_ENERGY_LIMITS["rare"],
                          default_limited_energy=DEFAULT_ENERGY_LIMITS["limited"],
                          default_boost_2025=BOOST_2025,
                          default_stack_boost=STACK_BOOST,
                          default_energy_per_card=ENERGY_PER_CARD,
                          default_lineup_order=",".join(DEFAULT_LINEUP_ORDER),
                          db_exists=db_exists)

@app.route('/generate_lineup', methods=['POST'])
def generate_lineup():
    """Generate lineup based on form inputs"""
    # Get form data
    username = request.form.get('username')
    rare_energy = int(request.form.get('rare_energy', DEFAULT_ENERGY_LIMITS["rare"]))
    limited_energy = int(request.form.get('limited_energy', DEFAULT_ENERGY_LIMITS["limited"]))
    boost_2025 = float(request.form.get('boost_2025', BOOST_2025))
    stack_boost = float(request.form.get('stack_boost', STACK_BOOST))
    energy_per_card = int(request.form.get('energy_per_card', ENERGY_PER_CARD))
    lineup_order = request.form.get('lineup_order', ','.join(DEFAULT_LINEUP_ORDER))
    ignore_players = request.form.get('ignore_players', '')
    
    # Parse ignore list
    ignore_list = []
    if ignore_players:
        ignore_list = [name.strip() for name in ignore_players.split(',') if name.strip()]
    
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
        # Fetch cards and projections
        cards_df = fetch_cards(username)
        projections_df = fetch_projections()
        
        if cards_df.empty:
            return jsonify({'error': f"No eligible cards found for {username}."})
        if projections_df.empty:
            return jsonify({'error': f"No projections available for game week {Config.GAME_WEEK}. Update the database first."})
        
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
        
        # Create directory if it doesn't exist
        os.makedirs('lineups', exist_ok=True)
        
        # Save lineups to a file
        output_file = f"lineups/{username}.txt"
        save_lineups(
            lineups=lineups,
            output_file=output_file,
            energy_limits=energy_limits,
            username=username,
            boost_2025=boost_2025,
            stack_boost=stack_boost,
            energy_per_card=energy_per_card,
            cards_df=cards_df,
            projections_df=projections_df
        )
        
        # Read the file content
        with open(output_file, 'r') as f:
            lineup_content = f.read()
        
        # Return success with file content
        return jsonify({
            'success': True,
            'lineup': lineup_content,
            'filename': f"{username}.txt"
        })
        
    except Exception as e:
        return jsonify({'error': f"Error generating lineup: {str(e)}"})

@app.route('/download_lineup/<username>')
def download_lineup(username):
    """Allow downloading the generated lineup file"""
    filename = f"lineups/{username}.txt"
    if os.path.exists(filename):
        return send_file(filename, as_attachment=True)
    else:
        return "Lineup file not found. Please generate it first.", 404

@app.route('/update_data', methods=['POST'])
def update_data():
    """Update injury data and projections"""
    try:
        # Check if database exists and create it if needed
        db_exists = check_and_create_db()
        
        if not db_exists:
            # Run full update to create and populate database
            success = run_full_update()
            if not success:
                return jsonify({'error': "Failed to initialize database. Check logs for details."})
        else:
            # Just update injury data and projections
            injury_data = fetch_injury_data()
            if injury_data:
                update_database(injury_data)
            
            # Update projections
            update_projections()
        
        # Get current game week after update
        current_game_week = determine_game_week()
        
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
        WHERE type='table' AND name IN ('cards', 'AdjustedProjections', 'injuries', 'PlayerTeams')
        """
        tables = cursor.execute(tables_query).fetchall()
        table_names = [t[0] for t in tables]
        
        # Get game week info
        game_week = Config.GAME_WEEK
        
        # Get count of projections for current game week
        proj_count = cursor.execute(
            "SELECT COUNT(*) FROM AdjustedProjections WHERE game_week = ?", 
            (game_week,)
        ).fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'status': 'connected',
            'tables': table_names,
            'game_week': game_week,
            'projection_count': proj_count
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

if __name__ == '__main__':
    # Ensure the lineups directory exists
    os.makedirs('lineups', exist_ok=True)
    
    # Get port from environment variable (for deployment) or default to 5000
    port = int(os.environ.get('PORT', 5000))
    
    # Run the app
    app.run(host='0.0.0.0', port=port, debug=True)