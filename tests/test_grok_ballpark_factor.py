import sqlite3
import unittest
from datetime import datetime
import sys
import os

# Add the parent directory to the path so we can import grok_ballpark_factor
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from grok_ballpark_factor import process_hitter, process_pitcher, apply_fip_adjustment, adjust_score_for_injury, DAY_TO_DAY_STATUS,DAY_TO_DAY_REDUCTION,INJURY_STATUSES_OUT

class TestProcessHitter(unittest.TestCase):
    def setUp(self):
        # Create an in-memory SQLite database
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()

        # Create necessary tables
        self.cursor.execute("""
            CREATE TABLE player_teams (
                mlbam_id TEXT PRIMARY KEY,
                team_id INTEGER
            )
        """)
        self.cursor.execute("""
            CREATE TABLE Stadiums (
                id INTEGER PRIMARY KEY,
                is_dome INTEGER,
                orientation INTEGER
            )
        """)
        self.cursor.execute("""
            CREATE TABLE weather_forecasts (
                game_id INTEGER,
                wind_dir INTEGER,
                wind_speed INTEGER,
                temp INTEGER
            )
        """)
        self.cursor.execute("""
            CREATE TABLE park_factors (
                stadium_id INTEGER,
                factor_type TEXT,
                value REAL
            )
        """)
        self.cursor.execute("""
            CREATE TABLE adjusted_projections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT,
                mlbam_id TEXT,
                game_id INTEGER,
                game_date TEXT,
                sorare_score REAL,
                game_week INTEGER,
                team_id INTEGER
            )
        """)
        self.cursor.execute("""
            CREATE TABLE Games (
                id INTEGER PRIMARY KEY,
                home_team_id INTEGER,
                away_team_id INTEGER,
                home_probable_pitcher_id INTEGER,  
                away_probable_pitcher_id INTEGER   
            )
        """)
        
        # Add pitcher FIP data for neutral FIP values
        self.cursor.execute("""
            CREATE TABLE pitchers_full_season (
                MLBAMID INTEGER PRIMARY KEY,
                fip REAL
            )
        """)
        # Insert a pitcher with a "neutral" FIP value (around 4.00)
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (1, 4.00)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (2, 4.00)")

    def tearDown(self):
        self.conn.close()

    def test_process_hitter_inserts_projection(self):
        # Insert mock data
        self.cursor.execute("INSERT INTO player_teams (mlbam_id, team_id) VALUES ('12345', 1)")
        self.cursor.execute("INSERT INTO Stadiums (id, is_dome, orientation) VALUES (1, 0, 90)")
        self.cursor.execute("INSERT INTO park_factors (stadium_id, factor_type, value) VALUES (1, 'HR', 100)")
        # Insert a game with neutral FIP pitchers
        self.cursor.execute("INSERT INTO Games (id, home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id) VALUES (1, 1, 2, 1, 2)")

        # Mock inputs
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2, '2023-10-01')  # Updated to match new game_data format
        hitter_data = {
            "Name": "John Doe",
            "MLBAMID": "12345",
            "TeamID": 1,
            "HR_per_game": 0.5
        }
        injuries = {}
        game_week_id = 1

        # Mock SCORING_MATRIX
        global SCORING_MATRIX
        SCORING_MATRIX = {
            'hitting': {
                'HR': 10
            }
        }

        # Call the function
        process_hitter(self.conn, game_data, hitter_data, injuries, game_week_id)

        # Verify the result
        self.cursor.execute("SELECT * FROM adjusted_projections WHERE mlbam_id = '12345'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[1], "JOHN DOE")  # player_name
        self.assertEqual(result[2], "12345")    # mlbam_id
        self.assertEqual(result[3], 1)         # game_id
        self.assertEqual(result[5], 5.0)       # sorare_score (0.5 HR * 10 points)

    def test_process_hitter_updates_existing_projection(self):


        self.cursor.execute("INSERT INTO player_teams (mlbam_id, team_id) VALUES ('12345', 1)")
        self.cursor.execute("INSERT INTO Stadiums (id, is_dome, orientation) VALUES (1, 0, 90)")
        self.cursor.execute("INSERT INTO park_factors (stadium_id, factor_type, value) VALUES (1, 'HR', 100)")


        self.cursor.execute("INSERT INTO Games (id, home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id) VALUES (1, 1, 2, 1, 2)")
        self.cursor.execute("""
            INSERT INTO adjusted_projections (player_name, mlbam_id, game_id, game_date, sorare_score, game_week, team_id)
            VALUES ('John Doe', '12345', 1, '2023-10-01', 3.0, 1, 1)
        """)

     
     
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2, '2023-10-01')  # Updated
        hitter_data = {
            "Name": "John Doe",
            "MLBAMID": "12345",
            "TeamID": 1,
            "HR_per_game": 0.5
        }
        injuries = {}
        game_week_id = 1

       
    
        global SCORING_MATRIX
        SCORING_MATRIX = {
            'hitting': {
                'HR': 10
            }
        }

        
        
        process_hitter(self.conn, game_data, hitter_data, injuries, game_week_id)

        
        
        self.cursor.execute("SELECT sorare_score FROM adjusted_projections WHERE mlbam_id = '12345'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 5.0)  # Updated sorare_score (0.5 HR * 10 points)

    def test_hitter_projection_calculation(self):
      
        self.cursor.execute("INSERT INTO player_teams (mlbam_id, team_id) VALUES ('12345', 1)")
        self.cursor.execute("INSERT INTO Stadiums (id, is_dome, orientation) VALUES (1, 0, 90)")
        self.cursor.execute("INSERT INTO park_factors (stadium_id, factor_type, value) VALUES (1, 'HR', 100)")
      
        self.cursor.execute("INSERT INTO Games (id, home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id) VALUES (1, 1, 2, 1, 2)")

     
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2, '2023-10-01') 
        hitter_data = {
            "Name": "John Doe",
            "MLBAMID": "12345",
            "TeamID": 1,
            "HR_per_game": 1.0,
            "RBI_per_game": 1.0,
            "1B_per_game": 1.0,
            "2B_per_game": 1.0,
            "3B_per_game": 1.0,
            "BB_per_game": 1.0,
            "HBP_per_game": 1.0,
            "SB_per_game": 1.0,
            "CS_per_game": 1.0,
            "K_per_game": 1.0,
            "R_per_game": 1.0
        }
        injuries = {}
        game_week_id = 1

        
        global SCORING_MATRIX
        SCORING_MATRIX = {
                'hitting': {'R': 3, 'RBI': 3, '1B': 2, 
                            '2B': 5, '3B': 8, 'HR': 10, 
                            'BB': 2, 'K': -1, 'SB': 5, 
                            'CS': -1, 'HBP': 2}
        }

        
        process_hitter(self.conn, game_data, hitter_data, injuries, game_week_id)

       
        self.cursor.execute("SELECT sorare_score FROM adjusted_projections WHERE mlbam_id = '12345'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)


        expected_score = 38
        self.assertAlmostEqual(result[0], expected_score, places=2)

    def test_starting_pitcher_projection_calculation(self):
        # Insert mock data
        self.cursor.execute("INSERT INTO player_teams (mlbam_id, team_id) VALUES ('54321', 2)")
        self.cursor.execute("INSERT INTO Stadiums (id, is_dome, orientation) VALUES (1, 1, 90)")
        self.cursor.execute("INSERT INTO park_factors (stadium_id, factor_type, value) VALUES (1, 'IP', 100)")
        self.cursor.execute("INSERT INTO park_factors (stadium_id, factor_type, value) VALUES (1, 'K', 100)")
        # Insert a game with neutral FIP pitchers
        self.cursor.execute("INSERT INTO Games (id, home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id) VALUES (1, 1, 2, 1, 2)")

        # Mock inputs - Updated to match new game_data format
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2, '2023-10-01')
        pitcher_data = {
            "Name": "Jane Doe",
            "MLBAMID": "54321",
            "TeamID": 2,
            "IP_per_game": 6.0,
            "K_per_game": 1,
            "H_per_game": 1,
            "ER_per_game": 1,
            "BB_per_game": 1,
            "HBP_per_game": 1,
            "W_per_game": 1,
            "S_per_game": 0.0,
            "HLD_per_game": 0.0
        }
        injuries = {}
        game_week_id = 1

        # Mock SCORING_MATRIX
        global SCORING_MATRIX
        SCORING_MATRIX = {
            'pitching': {'IP': 3, 'K': 2, 'H': -0.5, 
                        'ER': -2, 'BB': -1, 'HBP': -1, 
                        'W': 5, 'RA': 5, 'S': 10, 'HLD': 5 }
        }

        # Call the function
        process_pitcher(self.conn, game_data, pitcher_data, injuries, game_week_id, is_starter=True)

        # Verify the result
        self.cursor.execute("SELECT sorare_score FROM adjusted_projections WHERE mlbam_id = '54321'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)

        # Expected score: (6 IP * 3) + (8 K * 2) + (5 H * -0.5) + (2 ER * -2) + (1 BB * -1) + (0 HBP * -1) + (0.5 W * 5)
        expected_score = 20.5
        self.assertAlmostEqual(result[0], expected_score, places=2)

    def test_relief_pitcher_projection_calculation(self):
        # Insert mock data
        self.cursor.execute("INSERT INTO player_teams (mlbam_id, team_id) VALUES ('54321', 2)")
        self.cursor.execute("INSERT INTO Stadiums (id, is_dome, orientation) VALUES (1, 1, 90)")
        self.cursor.execute("INSERT INTO park_factors (stadium_id, factor_type, value) VALUES (1, 'IP', 100)")
        self.cursor.execute("INSERT INTO park_factors (stadium_id, factor_type, value) VALUES (1, 'K', 100)")
        # Insert a game with neutral FIP pitchers
        self.cursor.execute("INSERT INTO Games (id, home_team_id, away_team_id, home_probable_pitcher_id, away_probable_pitcher_id) VALUES (1, 1, 2, 1, 2)")
        
        # Mock inputs - Updated to match new game_data format
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2, '2023-10-01')
        pitcher_data = {
            "Name": "Jane Doe",
            "MLBAMID": "54321",
            "TeamID": 2,
            "IP_per_game": 1.0,
            "K_per_game": 1,
            "H_per_game": 1,
            "ER_per_game": 1,
            "BB_per_game": 1,
            "HBP_per_game": 1,
            "W_per_game": 1,
            "S_per_game": 1.0,
            "HLD_per_game": 1.0
        }
        injuries = {}
        game_week_id = 1

        # Mock SCORING_MATRIX
        global SCORING_MATRIX
        SCORING_MATRIX = {
            'pitching': {'IP': 3, 'K': 2, 'H': -0.5, 
                        'ER': -2, 'BB': -1, 'HBP': -1, 
                        'W': 5, 'RA': 5, 'S': 10, 'HLD': 5 }
        }

        # Call the function
        process_pitcher(self.conn, game_data, pitcher_data, injuries, game_week_id, is_starter=False)

        # Verify the result
        self.cursor.execute("SELECT sorare_score FROM adjusted_projections WHERE mlbam_id = '54321'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)

        # Expected score: (6 IP * 3) + (8 K * 2) + (5 H * -0.5) + (2 ER * -2) + (1 BB * -1) + (0 HBP * -1) + (0.5 W * 5)
        expected_score = 25.5 * .4
        self.assertAlmostEqual(result[0], expected_score, places=2)

if __name__ == "__main__":
    unittest.main()


class TestAdjustScoreForInjury(unittest.TestCase):
    def test_out_player_no_return(self):
        score = adjust_score_for_injury(10.0, 'Out', 'No estimated return date', datetime(2024, 6, 1))
        self.assertEqual(score, 0.0)

    def test_out_player_with_return_before_game(self):
        score = adjust_score_for_injury(10.0, 'Out', '2024-06-05', datetime(2024, 6, 1))
        self.assertEqual(score, 0.0)

    def test_day_to_day_before_return(self):
        score = adjust_score_for_injury(10.0, 'Day-To-Day', '2024-06-10', datetime(2024, 6, 5))
        self.assertEqual(score, 10.0 * DAY_TO_DAY_REDUCTION)

    def test_day_to_day_after_return(self):
        score = adjust_score_for_injury(10.0, 'Day-To-Day', '2024-06-01', datetime(2024, 6, 10))
        self.assertEqual(score, 10.0)

    def test_healthy_player(self):
        score = adjust_score_for_injury(10.0, 'Healthy', None, datetime(2024, 6, 1))
        self.assertEqual(score, 10.0)

    def test_bad_date_format(self):
        score = adjust_score_for_injury(10.0, 'Out', 'bad-date', datetime(2024, 6, 1))
        self.assertEqual(score, 0.0)


class TestApplyFipAdjustment(unittest.TestCase):
    """
    Unit tests for the apply_fip_adjustment function.
    """

    def setUp(self):
        """
        Set up an in-memory SQLite database for testing.  This avoids
        needing actual database files.  We create the tables and insert
        test data here.
        """
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()

        # Create the Games table
        self.cursor.execute("""
            CREATE TABLE Games (
                id INTEGER PRIMARY KEY,
                home_team_id INTEGER,
                away_team_id INTEGER,
                home_probable_pitcher_id INTEGER,
                away_probable_pitcher_id INTEGER
            )
        """)

        # Create the pitchers_full_season table
        self.cursor.execute("""
            CREATE TABLE pitchers_full_season (
                MLBAMID INTEGER PRIMARY KEY,
                fip REAL
            )
        """)

        # Insert some sample data into the Games table
        self.cursor.execute("INSERT INTO Games VALUES (1, 101, 202, 303, 404)")
        self.cursor.execute("INSERT INTO Games VALUES (2, 101, 202, 303, NULL)")  # away pitcher null
        self.cursor.execute("INSERT INTO Games VALUES (3, 101, 202, NULL, 404)")  # home pitcher null
        self.cursor.execute("INSERT INTO Games VALUES (4, 101, 202, NULL, NULL)") # both null
        self.cursor.execute("INSERT INTO Games VALUES (5, 101, 202, 303, 404)") # for team mismatch
        self.cursor.execute("INSERT INTO Games VALUES (6, 101, 202, 303, 404)") # pitcher with no FIP
        self.cursor.execute("INSERT INTO Games VALUES (7, 101, 202, 303, 404)")
        self.cursor.execute("INSERT INTO Games VALUES (8, 101, 202, 303, 404)") # Added for test_fip_2_8
        self.cursor.execute("INSERT INTO Games VALUES (9, 101, 202, 303, 404)") # Added for test_edge_cases
        self.cursor.execute("INSERT INTO Games VALUES (10, 101, 202, 303, 404)")  # Added for test_fip_adjustment
        self.cursor.execute("INSERT INTO Games VALUES (11, 101, 202, 303, 404)")  # Added for test_fip_boundaries
        self.cursor.execute("INSERT INTO Games VALUES (12, 101, 202, 303, 404)")  # Added for test_no_fip_available


        # Insert sample data into the pitchers_full_season table
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (303, 4.00)")  # Pitcher for Game 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (404, 3.50)")  # Pitcher for Game 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (505, 2.00)") # pitcher with very good FIP
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (506, 6.00)") # pitcher with very bad FIP

        self.conn.commit()

    def tearDown(self):
        """
        Clean up the database connection after testing.
        """
        self.conn.close()

    def test_fip_adjustment(self):
        """Test with a valid game and pitcher FIP."""
        adjusted_score = apply_fip_adjustment(self.conn, 10, 101, 100.0)  # Hitter team is home team
        self.assertEqual(adjusted_score, 95.0) # Expect no change, FIP is 3.50

        adjusted_score = apply_fip_adjustment(self.conn, 1, 202, 100.0)  # Hitter team is away team.
        self.assertEqual(adjusted_score, 100.0) # Expect 0.90 mulitplier, FIP is 4.00

    def test_game_not_found(self):
        """Test when the game ID is not found."""
        adjusted_score = apply_fip_adjustment(self.conn, 999, 101, 100.0)
        self.assertEqual(adjusted_score, 100.0)  # Should return the base score

    def test_no_probable_pitcher(self):
        """Test when there is no probable pitcher listed for the game."""
        adjusted_score_away_null = apply_fip_adjustment(self.conn, 2, 101, 100.0) # away pitcher is null
        self.assertEqual(adjusted_score_away_null, 100.0)

        adjusted_score_home_null = apply_fip_adjustment(self.conn, 3, 202, 100.0) # home pitcher is null
        self.assertEqual(adjusted_score_home_null, 100.0)

        adjusted_score_both_null = apply_fip_adjustment(self.conn, 4, 101, 100.0) # both pitchers are null
        self.assertEqual(adjusted_score_both_null, 100.0)

    def test_team_mismatch(self):
        """Test when the hitter team ID doesn't match either home or away team."""
        adjusted_score = apply_fip_adjustment(self.conn, 5, 303, 100.0)
        self.assertEqual(adjusted_score, 100.0)  # Should return the base score



    def test_fip_boundaries(self):
        """Test the FIP adjustment at different FIP boundaries."""
        # Add pitchers with FIP values at the boundaries
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (501, 3.19)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (502, 3.20)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (503, 3.49)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (504, 3.50)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (507, 3.79)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (508, 3.80)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (509, 4.19)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (510, 4.20)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (511, 4.39)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (512, 4.40)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (513, 4.69)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (514, 4.70)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (515, 4.99)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (516, 5.00)")
        self.conn.commit()

        # Update Game 11 to use a valid game
        self.cursor.execute("UPDATE Games SET home_probable_pitcher_id = 501, away_probable_pitcher_id = 502 WHERE id = 11")
        self.conn.commit()
        adjusted_score_1 = apply_fip_adjustment(self.conn, 11, 101, 100.0) # home team hitting, against 502 (3.20)
        self.assertEqual(adjusted_score_1, 90.0)

        adjusted_score_2 = apply_fip_adjustment(self.conn, 11, 202, 100.0) # away team hitting, against 501 (3.19)
        self.assertEqual(adjusted_score_2, 80.0)

        self.cursor.execute("UPDATE Games SET home_probable_pitcher_id = 503, away_probable_pitcher_id = 504 WHERE id = 11")
        self.conn.commit()
        adjusted_score_3 = apply_fip_adjustment(self.conn, 11, 101, 100.0) # home team hitting, against 504 (3.50)
        self.assertEqual(adjusted_score_3, 95.0)

        adjusted_score_4 = apply_fip_adjustment(self.conn, 11, 202, 100.0) # away team hitting, against 503 (3.49)
        self.assertEqual(adjusted_score_4, 90.0)

        self.cursor.execute("UPDATE Games SET home_probable_pitcher_id = 507, away_probable_pitcher_id = 508 WHERE id = 11")
        self.conn.commit()
        adjusted_score_5 = apply_fip_adjustment(self.conn, 11, 101, 100.0) # home team hitting, against 508 (3.80)
        self.assertEqual(adjusted_score_5, 100.0)

        adjusted_score_6 = apply_fip_adjustment(self.conn, 11, 202, 100.0) # away team hitting, against 507 (3.79)
        self.assertEqual(adjusted_score_6, 95.0)

        self.cursor.execute("UPDATE Games SET home_probable_pitcher_id = 509, away_probable_pitcher_id = 510 WHERE id = 11")
        self.conn.commit()
        adjusted_score_7 = apply_fip_adjustment(self.conn, 11, 101, 100.0) # home team, against 510 (4.20)
        self.assertEqual(adjusted_score_7, 105.0)

        adjusted_score_8 = apply_fip_adjustment(self.conn, 11, 202, 100.0) # away team, against 509 (4.19)
        self.assertEqual(adjusted_score_8, 100.0)

        self.cursor.execute("UPDATE Games SET home_probable_pitcher_id = 511, away_probable_pitcher_id = 512 WHERE id = 11")
        self.conn.commit()
        adjusted_score_9 = apply_fip_adjustment(self.conn, 11, 101, 100.0) # home team, against 512 (4.40)
        self.assertEqual(round(adjusted_score_9, 2), round(110.0, 2) )

        adjusted_score_10 = apply_fip_adjustment(self.conn, 11, 202, 100.0) # away team, against 511 (4.39)
        self.assertEqual(adjusted_score_10, 105.0)

        self.cursor.execute("UPDATE Games SET home_probable_pitcher_id = 513, away_probable_pitcher_id = 514 WHERE id = 11")
        self.conn.commit()
        adjusted_score_11 = apply_fip_adjustment(self.conn, 11, 101, 100.0) # home team, against 514 (4.70)
        self.assertEqual(round(adjusted_score_11, 2), 115.0)
        adjusted_score_12 = apply_fip_adjustment(self.conn, 11, 202, 100.0) # away team, against 513 (4.69)
        self.assertEqual(round(adjusted_score_12, 2), 110.0)

        self.cursor.execute("UPDATE Games SET home_probable_pitcher_id = 515, away_probable_pitcher_id = 516 WHERE id = 11")
        self.conn.commit()

        adjusted_score_13 = apply_fip_adjustment(self.conn, 11, 101, 100.0) # home team, against 516 (5.00)
        self.assertEqual(adjusted_score_13, 120.0)
        adjusted_score_14 = apply_fip_adjustment(self.conn, 11, 202, 100.0) # away team, against 515 (4.99)
        self.assertEqual(round(adjusted_score_14,2), 115.0)
    def test_edge_cases(self):
        """Test with extreme FIP values."""
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (517, 0.00)")
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (518, 10.00)")
        self.conn.commit()
        self.cursor.execute("UPDATE Games SET home_probable_pitcher_id = 517, away_probable_pitcher_id = 518 WHERE id = 9")
        self.conn.commit()

        adjusted_score_low_fip = apply_fip_adjustment(self.conn, 9, 101, 100.0)
        self.assertEqual(adjusted_score_low_fip, 120.0)

        adjusted_score_high_fip = apply_fip_adjustment(self.conn, 9, 202, 100.0)
        self.assertEqual(adjusted_score_high_fip, 80.0)

    def test_fip_2_8(self):
        """Test with a pitcher FIP of 2.8."""
        self.cursor.execute("INSERT INTO pitchers_full_season VALUES (600, 2.80)")
        self.conn.commit()
        self.cursor.execute("UPDATE Games SET home_probable_pitcher_id = 600, away_probable_pitcher_id = 600 WHERE id = 8") #update a new game
        self.conn.commit()
        adjusted_score = apply_fip_adjustment(self.conn, 8, 101, 100.0)
        self.assertEqual(adjusted_score, 80.0)