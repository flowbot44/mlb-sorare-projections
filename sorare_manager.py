#!/usr/bin/env python3
import os
import sys
import argparse
import sqlite3
from datetime import datetime, timedelta

# Import functionality from existing files
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from card_fetcher import SorareMLBClient, parse_player_string, main as fetch_cards
from injury_updates import fetch_injury_data, update_database as update_injury_db, main as update_injuries
from BBR_projection import pd, conn as bbr_conn
from projections import ProjectionGenerator
from grok_ballpark_factor import init_db as init_ballpark_db, load_park_factors_from_csv
from grok_ballpark_factor import get_schedule, fetch_weather_and_store, calculate_adjustments
from lineupGenerator import SorareLineupGenerator

class SorareMLBManager:
    def __init__(self, username=None, db_path="mlb_sorare.db"):
        self.username = username
        self.db_path = db_path
        self.current_date = datetime.now()
        
        # Verify all necessary files exist
        required_files = [
            'card_fetcher.py',
            'injury_updates.py',
            'BBR_projection.py',
            'projections.py',
            'grok_ballpark_factor.py',
            'lineupGenerator.py'
        ]
        
        for file in required_files:
            if not os.path.exists(file):
                print(f"ERROR: Required file {file} not found in the current directory.")
                sys.exit(1)
        
    def fetch_user_cards(self):
        """Fetch user's Sorare MLB cards"""
        print(f"\n=== Fetching Sorare MLB Cards for {self.username} ===")
        client = SorareMLBClient()
        
        if self.username:
            result = client.get_user_mlb_cards(self.username)
            if result:
                print(f"Successfully fetched {len(result['cards'])} cards for {self.username}")
                return True
            else:
                print(f"Failed to fetch cards for {self.username}")
                return False
        else:
            print("No username provided. Please provide a Sorare username.")
            return False
    
    def update_injury_data(self):
        """Update the MLB injury database"""
        print("\n=== Updating MLB Injury Data ===")
        success = fetch_injury_data()
        if success:
            update_injury_db(success)
            print("Successfully updated injury data")
            return True
        else:
            print("Failed to update injury data")
            return False
    
    def load_projections(self, hitters_csv, pitchers_csv):
        """Load season projections from CSV files"""
        print("\n=== Loading MLB Projections ===")
        
        if not os.path.exists(hitters_csv):
            print(f"ERROR: Hitters projection file {hitters_csv} not found")
            return False
        
        if not os.path.exists(pitchers_csv):
            print(f"ERROR: Pitchers projection file {pitchers_csv} not found")
            return False
        
        # Create tables and import data
        try:
            # This will reset the database and import the projections
            import BBR_projection
            print("Successfully loaded and processed projections")
            return True
        except Exception as e:
            print(f"Error loading projections: {str(e)}")
            return False
    
    def update_ballpark_factors(self, park_factors_csv):
        """Update ballpark factors in the database"""
        print("\n=== Updating Ballpark Factors ===")
        
        if not os.path.exists(park_factors_csv):
            print(f"ERROR: Park factors file {park_factors_csv} not found")
            return False
        
        try:
            # Initialize the ballpark database
            conn = init_ballpark_db('mlb_sorare.db')
            
            # Load park factors
            load_park_factors_from_csv(conn, park_factors_csv)
            
            # Get current schedule and weather data
            today = datetime.now().date()
            start_date = today.strftime('%Y-%m-%d')
            end_date = (today + timedelta(days=14)).strftime('%Y-%m-%d')  # Look ahead 2 weeks
            
            get_schedule(conn, start_date, end_date)
            fetch_weather_and_store(conn, start_date, end_date)
            calculate_adjustments(conn, start_date, end_date)
            
            conn.close()
            print("Successfully updated ballpark and weather factors")
            return True
        except Exception as e:
            print(f"Error updating ballpark factors: {str(e)}")
            return False
    
    def generate_projections(self):
        """Generate player projections for upcoming game weeks"""
        print("\n=== Generating Player Projections ===")
        
        try:
            # Initialize projection generator
            generator = ProjectionGenerator(self.db_path)
            generator.setup_database()
            
            # Generate projections for the next 4 weeks
            current_date = datetime.now()
            for i in range(4):
                # Monday-Thursday projections
                week_start = current_date + timedelta(days=(7 * i))
                game_week = week_start.strftime("%Y-%W") + "-MT"  # Monday-Thursday week
                print(f"Generating projections for {game_week}")
                generator.generate_week_projections(game_week)
                
                # Friday-Sunday projections
                game_week = week_start.strftime("%Y-%W") + "-FS"  # Friday-Sunday week
                print(f"Generating projections for {game_week}")
                generator.generate_week_projections(game_week)
            
            print("Successfully generated projections for upcoming game weeks")
            return True
        except Exception as e:
            print(f"Error generating projections: {str(e)}")
            return False
    
    def generate_lineups(self):
        """Generate optimal lineups for upcoming game weeks"""
        print("\n=== Generating Optimal Lineups ===")
        
        try:
            # Initialize lineup generator
            generator = SorareLineupGenerator(self.db_path)
            
            # Generate lineups for the next 4 weeks
            current_date = datetime.now()
            lineups_by_week = {}
            
            for i in range(4):
                week_start = current_date + timedelta(days=(7 * i))
                
                # Monday-Thursday lineups
                game_week = week_start.strftime("%Y-%W") + "-MT"
                print(f"\nGenerating lineups for {game_week} (Monday-Thursday)")
                
                regular_lineups, stacked_lineups = generator.generate_all_lineups(
                    game_week=game_week,
                    num_regular=3,
                    num_stacked=3,
                    stack_size=3,
                    max_projection_diff=5.0
                )
                
                lineups_by_week[game_week] = {
                    'regular': regular_lineups,
                    'stacked': stacked_lineups
                }
                
                # Friday-Sunday lineups
                game_week = week_start.strftime("%Y-%W") + "-FS"
                print(f"\nGenerating lineups for {game_week} (Friday-Sunday)")
                
                regular_lineups, stacked_lineups = generator.generate_all_lineups(
                    game_week=game_week,
                    num_regular=3,
                    num_stacked=3,
                    stack_size=3,
                    max_projection_diff=5.0
                )
                
                lineups_by_week[game_week] = {
                    'regular': regular_lineups,
                    'stacked': stacked_lineups
                }
            
            # Print all lineups
            print("\n\n=== OPTIMAL LINEUPS FOR UPCOMING GAME WEEKS ===")
            for game_week, lineups in lineups_by_week.items():
                print(f"\n\n{'=' * 80}")
                if "MT" in game_week:
                    print(f"GAME WEEK: {game_week} (Monday-Thursday)")
                else:
                    print(f"GAME WEEK: {game_week} (Friday-Sunday)")
                print(f"{'=' * 80}")
                
                print("\nREGULAR LINEUPS:")
                for i, lineup in enumerate(lineups['regular'], 1):
                    print(f"\nRegular Lineup {i}:")
                    generator.print_lineup(lineup)
                
                print("\nSTACKED LINEUPS (3+ players from same team):")
                for i, lineup in enumerate(lineups['stacked'], 1):
                    print(f"\nStacked Lineup {i}:")
                    generator.print_lineup(lineup)
            
            return True
        except Exception as e:
            print(f"Error generating lineups: {str(e)}")
            return False
    
    def run_full_workflow(self, hitters_csv, pitchers_csv, park_factors_csv):
        """Run the complete workflow from data fetching to lineup generation"""
        steps = [
            (self.fetch_user_cards, "Fetching Sorare Cards"),
            (self.update_injury_data, "Updating Injury Data"),
            (lambda: self.load_projections(hitters_csv, pitchers_csv), "Loading Projections"),
            (lambda: self.update_ballpark_factors(park_factors_csv), "Updating Ballpark Factors"),
            (self.generate_projections, "Generating Player Projections"),
            (self.generate_lineups, "Generating Optimal Lineups")
        ]
        
        for step_func, step_name in steps:
            print(f"\n{'=' * 30} {step_name} {'=' * 30}")
            success = step_func()
            if not success:
                print(f"ERROR: Failed at step '{step_name}'")
                return False
        
        print("\n🎉 Successfully completed the full Sorare MLB lineup optimization workflow!")
        return True


def main():
    parser = argparse.ArgumentParser(description='Sorare MLB Lineup Manager')
    parser.add_argument('--username', '-u', type=str, help='Sorare username')
    parser.add_argument('--hitters', type=str, default='2025_bbr_hitter_projections.csv', 
                        help='Path to hitters projection CSV file')
    parser.add_argument('--pitchers', type=str, default='2025_bbr_pitching_projections.csv', 
                        help='Path to pitchers projection CSV file')
    parser.add_argument('--park-factors', type=str, default='park_data.csv', 
                        help='Path to park factors CSV file')
    parser.add_argument('--db', type=str, default='mlb_sorare.db', 
                        help='Path to SQLite database file')
    
    args = parser.parse_args()
    
    if not args.username:
        parser.error("A Sorare username is required. Use --username to specify.")
    
    manager = SorareMLBManager(username=args.username, db_path=args.db)
    manager.run_full_workflow(args.hitters, args.pitchers, args.park_factors)

if __name__ == "__main__":
    main()