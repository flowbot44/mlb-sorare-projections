import sqlite3
import unittest
from datetime import datetime
from grok_ballpark_factor import process_hitter, process_pitcher

class TestProcessHitter(unittest.TestCase):
    def setUp(self):
        # Create an in-memory SQLite database
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()

        # Create necessary tables
        self.cursor.execute("""
            CREATE TABLE PlayerTeams (
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
            CREATE TABLE WeatherForecasts (
                game_id INTEGER,
                wind_dir INTEGER,
                wind_speed INTEGER,
                temp INTEGER
            )
        """)
        self.cursor.execute("""
            CREATE TABLE ParkFactors (
                stadium_id INTEGER,
                factor_type TEXT,
                value REAL
            )
        """)
        self.cursor.execute("""
            CREATE TABLE AdjustedProjections (
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
                away_team_id INTEGER
            )
        """)

    def tearDown(self):
        self.conn.close()

    def test_process_hitter_inserts_projection(self):
        # Insert mock data
        self.cursor.execute("INSERT INTO PlayerTeams (mlbam_id, team_id) VALUES ('12345', 1)")
        self.cursor.execute("INSERT INTO Stadiums (id, is_dome, orientation) VALUES (1, 0, 90)")
        self.cursor.execute("INSERT INTO ParkFactors (stadium_id, factor_type, value) VALUES (1, 'HR', 100)")

        # Mock inputs
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2)
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
        self.cursor.execute("SELECT * FROM AdjustedProjections WHERE mlbam_id = '12345'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[1], "JOHN DOE")  # player_name
        self.assertEqual(result[2], "12345")    # mlbam_id
        self.assertEqual(result[3], 1)         # game_id
        self.assertEqual(result[5], 5.0)       # sorare_score (0.5 HR * 10 points)

    def test_process_hitter_skips_no_team(self):
        # Mock inputs
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2)
        hitter_data = {
            "Name": "John Doe",
            "MLBAMID": "12345",
            "HR_per_game": 0.5
        }
        injuries = {}
        game_week_id = 1

        # Call the function
        process_hitter(self.conn, game_data, hitter_data, injuries, game_week_id)

        # Verify no projection was inserted
        self.cursor.execute("SELECT * FROM AdjustedProjections WHERE mlbam_id = '12345'")
        result = self.cursor.fetchone()
        self.assertIsNone(result)

    def test_process_hitter_updates_existing_projection(self):
        # Insert mock data
        self.cursor.execute("INSERT INTO PlayerTeams (mlbam_id, team_id) VALUES ('12345', 1)")
        self.cursor.execute("INSERT INTO Stadiums (id, is_dome, orientation) VALUES (1, 0, 90)")
        self.cursor.execute("INSERT INTO ParkFactors (stadium_id, factor_type, value) VALUES (1, 'HR', 100)")
        self.cursor.execute("""
            INSERT INTO AdjustedProjections (player_name, mlbam_id, game_id, game_date, sorare_score, game_week, team_id)
            VALUES ('John Doe', '12345', 1, '2023-10-01', 3.0, 1, 1)
        """)

        # Mock inputs
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2)
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
        self.cursor.execute("SELECT sorare_score FROM AdjustedProjections WHERE mlbam_id = '12345'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 5.0)  # Updated sorare_score (0.5 HR * 10 points)

    def test_hitter_projection_calculation(self):
        # Insert mock data
        self.cursor.execute("INSERT INTO PlayerTeams (mlbam_id, team_id) VALUES ('12345', 1)")
        self.cursor.execute("INSERT INTO Stadiums (id, is_dome, orientation) VALUES (1, 0, 90)")
        self.cursor.execute("INSERT INTO ParkFactors (stadium_id, factor_type, value) VALUES (1, 'HR', 100)")

        # Mock inputs
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2)
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

        # Mock SCORING_MATRIX
        global SCORING_MATRIX
        SCORING_MATRIX = {
                'hitting': {'R': 3, 'RBI': 3, '1B': 2, 
                            '2B': 5, '3B': 8, 'HR': 10, 
                            'BB': 2, 'K': -1, 'SB': 5, 
                            'CS': -1, 'HBP': 2}
        }

        # Call the function
        process_hitter(self.conn, game_data, hitter_data, injuries, game_week_id)

        # Verify the result
        self.cursor.execute("SELECT sorare_score FROM AdjustedProjections WHERE mlbam_id = '12345'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)

        # Expected score: (0.5 HR * 10) + (1.0 RBI * 2) + (0.3 BB * 1) = 5 + 2 + 0.3 = 7.3
        expected_score = 38
        self.assertAlmostEqual(result[0], expected_score, places=2)

    def test_starting_pitcher_projection_calculation(self):
        # Insert mock data
        self.cursor.execute("INSERT INTO PlayerTeams (mlbam_id, team_id) VALUES ('54321', 2)")
        self.cursor.execute("INSERT INTO Stadiums (id, is_dome, orientation) VALUES (1, 1, 90)")
        self.cursor.execute("INSERT INTO ParkFactors (stadium_id, factor_type, value) VALUES (1, 'IP', 100)")
        self.cursor.execute("INSERT INTO ParkFactors (stadium_id, factor_type, value) VALUES (1, 'K', 100)")

        # Mock inputs
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2)
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
        self.cursor.execute("SELECT sorare_score FROM AdjustedProjections WHERE mlbam_id = '54321'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)

        # Expected score: (6 IP * 3) + (8 K * 2) + (5 H * -0.5) + (2 ER * -2) + (1 BB * -1) + (0 HBP * -1) + (0.5 W * 5)
        expected_score = 20.5
        self.assertAlmostEqual(result[0], expected_score, places=2)

    def test_relief_pitcher_projection_calculation(self):
        # Insert mock data
        self.cursor.execute("INSERT INTO PlayerTeams (mlbam_id, team_id) VALUES ('54321', 2)")
        self.cursor.execute("INSERT INTO Stadiums (id, is_dome, orientation) VALUES (1, 1, 90)")
        self.cursor.execute("INSERT INTO ParkFactors (stadium_id, factor_type, value) VALUES (1, 'IP', 100)")
        self.cursor.execute("INSERT INTO ParkFactors (stadium_id, factor_type, value) VALUES (1, 'K', 100)")

        # Mock inputs
        game_data = (1, '2023-10-01', '19:00', 1, 1, 2)
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
        self.cursor.execute("SELECT sorare_score FROM AdjustedProjections WHERE mlbam_id = '54321'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result)

        # Expected score: (6 IP * 3) + (8 K * 2) + (5 H * -0.5) + (2 ER * -2) + (1 BB * -1) + (0 HBP * -1) + (0.5 W * 5)
        expected_score = 25.5 * .4
        self.assertAlmostEqual(result[0], expected_score, places=2)

if __name__ == "__main__":
    unittest.main()