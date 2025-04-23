import sqlite3
from utils import DATABASE_FILE  # Assuming utils.py is in the same directory



db_path = DATABASE_FILE  # Assuming utils.py is in the same directory

def get_cards_with_injuries():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
        SELECT distinct status  
        FROM cards c
        INNER JOIN injuries i ON c.name = i.player_name
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
    cursor = conn.execute("SELECT * FROM WeatherForecasts WHERE  rain >= 75 ;")

    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()

def get_all_projections():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file
 
    #cursor = conn.execute("SELECT * FROM AdjustedProjections WHERE player_name IN ('NICK PIVETTA','TANNER BIBEE')")

    #cursor = conn.execute("PRAGMA table_info(pitchers_per_game)")
    cursor = conn.execute("SELECT IP_per_game, H_per_game, ER_per_game, BB_per_game, HBP_per_game, W_per_game, K_per_game FROM pitchers_per_game WHERE name IN ('NICK PIVETTA','TANNER BIBEE')")

    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()



def get_all_teams():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file

    cursor = conn.execute("SELECT * FROM PlayerTeams where player_name IN ('JUNIOR CAMINERO','MATT WALLNER')")

    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()

def get_best_players():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file

    query = """
    SELECT
        ap.player_name,
        ap.team_id,
        g.id as game_id,
        ap.sorare_score
    FROM
        AdjustedProjections ap
    JOIN
        Games g ON ap.game_id = g.id
    JOIN
        hitters_per_game hpg ON ap.player_name = hpg.Name -- Join to ensure only hitters are included
    WHERE
        g.date = '2025-04-12' -- Filter for today's date
    ORDER BY
        ap.sorare_score DESC -- Order by highest projected score
    LIMIT 10; -- Limit the results to 10
    """
    cursor = conn.execute(query)

    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()

def main():
    #get_all_players()
    #get_cards_with_injuries()
    get_all_projections()
    #get_all_teams()
    #get_best_players()
    #get_all_games()

if __name__ == "__main__":
    main()