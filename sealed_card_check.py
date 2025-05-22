from datetime import datetime, timedelta
import pandas as pd
# Assuming utils.py is in the same directory and get_db_connection
# is updated to return a psycopg2 connection
from utils import get_db_connection


def generate_sealed_cards_report(username):
    """
    Generate a report of sealed cards with projections and injured players expected back soon.

    Args:
        username (str): Username to filter cards by
    """
    conn = None
    try:
        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get current date
        current_date = datetime.now()
        print(f"Report generated on: {current_date.strftime('%Y-%m-%d')}")
        print("\n" + "="*80)

        # Part 1: Sealed cards with projections (distinct cards with totaled projections)
        print("\n## SEALED CARDS WITH UPCOMING PROJECTIONS (TOTALED) ##\n")

        # IMPORTANT: Fixed SQL Injection vulnerability.
        # Use %s placeholder for username and pass it as a parameter.
        query_part1 = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions,
               COUNT(ap.game_id) as game_count,
               SUM(ap.sorare_score) as total_projected_score,
               AVG(ap.sorare_score) as avg_projected_score,
               MIN(ap.game_date) as next_game_date
        FROM cards c
        JOIN adjusted_projections ap ON c.slug = ap.card_slug
        WHERE c.sealed = TRUE AND c.username = %s AND ap.game_date >= CURRENT_DATE
        GROUP BY c.slug, c.name, c.year, c.rarity, c.positions
        ORDER BY next_game_date ASC, total_projected_score DESC;
        """
        # Execute the query with the username as a parameter
        cursor.execute(query_part1, (username,))
        sealed_cards_projections = cursor.fetchall()

        if sealed_cards_projections:
            columns = ['Slug', 'Name', 'Year', 'Rarity', 'Positions',
                       'Game Count', 'Total Projected Score', 'Avg Projected Score', 'Next Game Date']
            df_projections = pd.DataFrame(sealed_cards_projections, columns=columns)
            print(f"Found {len(df_projections)} sealed cards with upcoming projections for {username}:")
            print(df_projections.to_string(index=False))
        else:
            print(f"No sealed cards with upcoming projections found for {username}.")

        print("\n" + "="*80)

        # Part 2: Sealed cards currently injured but expected back soon
        print("\n## SEALED CARDS CURRENTLY INJURED (EXPECTED BACK SOON) ##\n")

        # IMPORTANT: Fixed SQL Injection vulnerability.
        # Use %s placeholder for username and pass it as a parameter.
        query_part2 = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions,
               i.status, i.description, i.return_estimate, i.team
        FROM cards c
        JOIN injuries i ON c.name = i.player_name -- Assuming player_name links cards to injuries
        WHERE c.sealed = TRUE AND c.username = %s AND i.player_name IS NOT NULL;
        """
        # Execute the query with the username as a parameter
        cursor.execute(query_part2, (username,))
        injured_sealed_cards = cursor.fetchall()

        if injured_sealed_cards:
            soon_returning = []
            for result in injured_sealed_cards:
                return_estimate = result[7] # return_estimate is the 8th column (index 7)
                if return_estimate:
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

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close() # Ensure the connection is always closed

if __name__ == "__main__":
    # Get username from user
    username = input("Enter username to generate report for: ")
    if not username:
        print("Error: Username cannot be empty.")
        exit(1)

    # Generate report
    generate_sealed_cards_report(username)