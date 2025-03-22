import sqlite3
from datetime import datetime, timedelta
import random  # For demonstration - replace with actual projection logic

class ProjectionGenerator:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.scoring_matrix = {
            'hitting': {
                'R': 3,
                'RBI': 3,
                '1B': 2,
                '2B': 5,
                '3B': 8,
                'HR': 10,
                'BB': 2,
                'K': -1,
                'SB': 5,
                'CS': -1,
                'HBP': 2
            },
            'pitching': {
                'IP': 3,
                'K': 2,
                'H': -0.5,
                'ER': -2,
                'BB': -1,
                'HBP': -1,
                'W': 5,
                'RA': 5,
                'S': 10,
                'H': 5
            }
        }

    def setup_database(self):
        """Create the projections table if it doesn't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS projections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT,
            game_week TEXT,
            team TEXT,
            opponent TEXT,
            projection_value REAL,
            stats_projection TEXT,  -- JSON string of projected stats
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(player_name, game_week)
        )
        """)
        
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_projections_week 
        ON projections(game_week)
        """)
        
        conn.commit()
        conn.close()

    def generate_week_projections(self, game_week: str):
        """Generate projections for all players for a specific game week, setting projection to 0 if player is out"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT DISTINCT c.name, i.status
        FROM cards c
        LEFT JOIN injuries i ON c.name = i.player_name
        """)
        players = cursor.fetchall()
        
        for name, status in players:
            team = "NYY" if random.random() < 0.5 else "BOS"  # Example
            opponent = "BOS" if team == "NYY" else "NYY"
            
            if status == 'out':
                projection_value, stats_projection = 0, "{}"
            else:
                projection_value, stats_projection = self.generate_player_projection(
                    name, game_week, team, opponent
                )
            
            cursor.execute("""
            INSERT OR REPLACE INTO projections 
            (player_name, game_week, team, opponent, projection_value, stats_projection)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (name, game_week, team, opponent, projection_value, stats_projection))
        
        conn.commit()
        conn.close()

    def generate_player_projection(self, player_name: str, game_week: str, 
                                 team: str, opponent: str) -> tuple:
        """
        Generate projections for a player for a specific game week
        Returns (projection_value, stats_projection)
        """
        import json
        
        if 'pitcher' in player_name.lower():
            projected_stats = {
                'IP': round(random.uniform(5, 7), 1),
                'K': round(random.uniform(4, 8)),
                'H': round(random.uniform(4, 8)),
                'ER': round(random.uniform(1, 4)),
                'BB': round(random.uniform(1, 4)),
                'W': random.random() < 0.5,
                'S': random.random() < 0.2
            }
            
            projection_value = (
                projected_stats['IP'] * self.scoring_matrix['pitching']['IP'] +
                projected_stats['K'] * self.scoring_matrix['pitching']['K'] +
                projected_stats['H'] * self.scoring_matrix['pitching']['H'] +
                projected_stats['ER'] * self.scoring_matrix['pitching']['ER'] +
                projected_stats['BB'] * self.scoring_matrix['pitching']['BB'] +
                (5 if projected_stats['W'] else 0) +
                (10 if projected_stats['S'] else 0)
            )
        else:
            projected_stats = {
                'AB': round(random.uniform(3, 5)),
                'H': round(random.uniform(0, 3)),
                '2B': round(random.random()),
                'HR': round(random.random() * 0.3),
                'BB': round(random.uniform(0, 2)),
                'K': round(random.uniform(0, 2)),
                'SB': round(random.random() * 0.2)
            }
            
            projection_value = (
                projected_stats['H'] * self.scoring_matrix['hitting']['1B'] +
                projected_stats['2B'] * self.scoring_matrix['hitting']['2B'] +
                projected_stats['HR'] * self.scoring_matrix['hitting']['HR'] +
                projected_stats['BB'] * self.scoring_matrix['hitting']['BB'] +
                projected_stats['K'] * self.scoring_matrix['hitting']['K'] +
                projected_stats['SB'] * self.scoring_matrix['hitting']['SB']
            )
        
        return projection_value, json.dumps(projected_stats)

    
if __name__ == "__main__":
    generator = ProjectionGenerator("mlb_sorare.db")
    generator.setup_database()
    
    current_date = datetime.now()
    for i in range(4):
        game_week = (current_date + timedelta(weeks=i)).strftime("%Y-%W")
        print(f"Generating projections for week {game_week}")
        generator.generate_week_projections(game_week)
