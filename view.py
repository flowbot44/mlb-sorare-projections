import sqlite3
from utils import DATABASE_FILE  # Assuming utils.py is in the same directory



db_path = DATABASE_FILE  # Assuming utils.py is in the same directory

def get_cards_with_injuries():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
        SELECT player_name, status  
        FROM injuries i WHERE i.player_name = 'ONEIL CRUZ'
    """
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    
    for row in results:
        print(row)  # Adjust this to format the output as needed

def get_all_players():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file

    cursor = conn.execute("SELECT * FROM cards")

    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()

def get_all_games():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file
    #(id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, 
                  #wind_dir REAL, wind_speed REAL, temp REAL, rain REAL)
    cursor = conn.execute("SELECT * FROM Games WHERE home_team_id = 119 or away_team_id = 119")

    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()

def get_all_projections():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file
 
    cursor = conn.execute("SELECT * FROM AdjustedProjections WHERE player_name IN ('ALEK THOMAS', 'TIM TAWA')")

    
    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()

def get_all_stats():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file
    #cursor = conn.execute("PRAGMA table_info(hitters_per_game)")
    #cursor = conn.execute('SELECT `1B_per_game`, "2B_per_game", "3B_per_game" FROM hitters_per_game WHERE name IN ("KYLE TUCKER")')
    cursor = conn.execute("SELECT DISTINCT Name, MLBAMID FROM hitters_per_game WHERE MLBAMID IS NOT NULL")
    #cursor = conn.execute("SELECT name, fip FROM pitchers_full_season WHERE name IN ('TYLOR MEGILL', 'GRIFFIN CANNING', 'DAVID PETERSON')")

    #cursor = conn.execute("SELECT HLD_per_game, IP_per_game, H_per_game, ER_per_game, BB_per_game, HBP_per_game, W_per_game, K_per_game FROM pitchers_per_game WHERE name IN ('PAUL SEWALD','TANNER BIBEE')")
    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()
def get_all_hitters_stats():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file

    for prefix in {'hitters', 'hitters_vs_rhp', 'hitters_vs_lhp',}:
        cursor = conn.execute(f"SELECT name, G, HR_per_game, RBI_per_game, K_per_game FROM {prefix}_per_game WHERE name IN ('ELLY DE LA CRUZ')")
        #cursor = conn.execute(f"PRAGMA table_info({prefix}_per_game)")
        print(f"Stats for {prefix}:")
        for row in cursor:
            print(row) #This will print tuples of the rows in the db.
    conn.close()

def get_all_teams():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file

    cursor = conn.execute("SELECT * FROM PlayerTeams where player_name IN ('LAMONTE WADE JR', 'LUIS MATOS')")

    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()

def get_player_handedness():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file

    cursor = conn.execute("SELECT * FROM PlayerHandedness where player_name IN ('JUNIOR CAMINERO','MATT WALLNER')")

    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()

def get_best_players():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file

    query = """
        SELECT g.home_probable_pitcher_id, g.away_probable_pitcher_id, g.id, g.date, g.time, ht.name AS home_team_name, at.name AS away_team_name
        FROM Games g
        JOIN Teams ht ON g.home_team_id = ht.id
        JOIN Teams at ON g.away_team_id = at.id
        WHERE (g.home_probable_pitcher_id IS NULL OR g.away_probable_pitcher_id IS NULL)
          
        ORDER BY g.date, g.time
    """
    cursor = conn.execute(query)

    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()

def main():
    #get_all_players()
    #get_cards_with_injuries()
    get_all_stats()
    #get_all_hitters_stats()
    #get_all_projections()
    #get_player_handedness()
    #get_all_teams()
    #get_best_players()
    #get_all_games()

if __name__ == "__main__":
    main()