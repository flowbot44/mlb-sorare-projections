
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import os

def generate_sealed_cards_report(db_path, username):
    """
    Generate a report of sealed cards with projections and injured players expected back soon.
    
    Args:
        db_path (str): Path to the SQLite database file
        username (str): Username to filter cards by
    """
    conn = None
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get current date
        current_date = datetime.now()
        print(f"Report generated on: {current_date.strftime('%Y-%m-%d')}")
        print("\n" + "="*80)
        
        # Part 1: Sealed cards with projections (distinct cards with totaled projections)
        print("\n## SEALED CARDS WITH UPCOMING PROJECTIONS (TOTALED) ##\n")
        
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
        
        cursor.execute(query, (username, current_date.strftime('%Y-%m-%d')))
        projection_results = cursor.fetchall()
        
        if projection_results:
            # Convert to DataFrame for better display
            columns = ['Slug', 'Name', 'Year', 'Rarity', 'Positions', 
                      'Upcoming Games', 'Total Projected Score', 'Avg Score/Game', 'Next Game Date']
            df_projections = pd.DataFrame(projection_results, columns=columns)
            
            # Format the dataframe - round the scores to 2 decimal places
            df_projections['Total Projected Score'] = df_projections['Total Projected Score'].round(2)
            df_projections['Avg Score/Game'] = df_projections['Avg Score/Game'].round(2)
            
            print(f"Found {len(df_projections)} distinct sealed cards with upcoming projections:")
            print(df_projections.to_string(index=False))
        else:
            print("No sealed cards with upcoming projections found.")
        
        # Part 2: Injured sealed cards expected back within 3 days
        print("\n" + "="*80)
        print("\n## INJURED SEALED CARDS RETURNING WITHIN 3 DAYS ##\n")
        
        # Calculate the date 3 days from now
        three_days_later = current_date + timedelta(days=3)
        
        query = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions, i.status, 
               i.description, i.return_estimate, i.team
        FROM cards c
        JOIN injuries i ON c.name = i.player_name
        WHERE c.username = ? AND c.sealed = 1 AND i.return_estimate IS NOT NULL
        """
        
        cursor.execute(query, (username,))
        injury_results = cursor.fetchall()
        
        if injury_results:
            # Filter injuries with return dates within 3 days
            soon_returning = []
            
            for result in injury_results:
                return_estimate = result[7]
                
                # Check if return_estimate contains a date string
                try:
                    # Try different date formats
                    for date_format in ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%d/%m/%Y']:
                        try:
                            return_date = datetime.strptime(return_estimate, date_format)
                            if current_date <= return_date <= three_days_later:
                                soon_returning.append(result)
                            break
                        except ValueError:
                            continue
                except:
                    # If return_estimate isn't a date, check if it contains keywords
                    # suggesting imminent return
                    keywords = ['day to day', 'game time decision', 'probable', 
                               'questionable', 'today', 'tomorrow', '1-3 days']
                    if any(keyword in return_estimate.lower() for keyword in keywords):
                        soon_returning.append(result)
            
            if soon_returning:
                columns = ['Slug', 'Name', 'Year', 'Rarity', 'Positions', 'Status', 
                          'Description', 'Return Estimate', 'Team']
                df_injuries = pd.DataFrame(soon_returning, columns=columns)
                
                print(f"Found {len(df_injuries)} injured sealed cards expected to return within 3 days:")
                print(df_injuries.to_string(index=False))
            else:
                print("No injured sealed cards expected to return within 3 days.")
        else:
            print("No injured sealed cards found.")
        
        print("\n" + "="*80)
        print("\nReport completed successfully.")
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Get database path from user or use default

    
    # Get username from user
    username = input("Enter username to generate report for: ")
    if not username:
        print("Error: Username cannot be empty.")
        exit(1)
    
    # Generate report
    generate_sealed_cards_report("mlb_sorare.db", username)