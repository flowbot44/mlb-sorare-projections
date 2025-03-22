import sqlite3



db_path = "mlb_sorare.db"
def get_cards_with_injuries():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
        SELECT c.slug, c.name, i.status, i.return_estimate, i.long_description  
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

def get_all_projections():
    conn = sqlite3.connect(db_path)  # Create/connect to the database file

    cursor = conn.execute("SELECT * FROM projections")

    for row in cursor:
        print(row) #This will print tuples of the rows in the db.
    
    conn.close()

def main():
    #get_all_players()
    #get_cards_with_injuries()
    get_all_projections()
if __name__ == "__main__":
    main()