import requests
import json
import re
import sqlite3
import time
import random
from utils import normalize_name, DATABASE_FILE


class SorareMLBClient:
    def __init__(self):
        self.base_url = "https://api.sorare.com/graphql"
        self.headers = {'content-type': 'application/json'}
    
    def get_user_mlb_cards(self, username):
        """Fetch all MLB cards of specific rarities owned by a username using pagination."""
        query = """
            query UserMLBCards($username: String!, $after: String) {
                user(slug: $username) {
                    cards(first: 25, after: $after, rarities: [common, limited, rare], sport: BASEBALL) {
                        nodes {
                            slug
                            anyPositions
                            sealed
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                }
            }
        """
        
        variables = {"username": username, "after": None}
        all_cards = []
        retry_count = 0
        
        while True:
            response = requests.post(
                self.base_url,
                json={"query": query, "variables": variables},
                headers=self.headers
            )
            
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 2 ** retry_count + random.uniform(0, 1)))
                print(f"Rate limit exceeded. Retrying in {retry_after:.2f} seconds...")
                time.sleep(retry_after)
                retry_count += 1
                continue
            
            if response.status_code != 200:
                print(f"Error: API request failed with status code {response.status_code} for {username}")
                try:
                    error_data = response.json()
                    print(f"Error details: {error_data}")
                except json.JSONDecodeError:
                    print("Could not decode error response.")
                return None
            
            retry_count = 0  # Reset retry count on success
            
            try:
                data = response.json()
                if not data or "data" not in data:
                    print(f"Error: Invalid API response format for {username}: {data}")
                    return None
                
                errors = data.get("errors", [])
                if errors:
                    print(f"Error: {errors}")
                    return None
                
                user_data = data.get("data", {}).get("user", {})
                if not user_data:
                    print(f"Error: No user data found for {username}")
                    return None
                
                cards_data = user_data.get("cards", {}).get("nodes", [])
                all_cards.extend(cards_data)
                
                page_info = user_data.get("cards", {}).get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break
                
                variables["after"] = page_info.get("endCursor")
            
            except json.JSONDecodeError:
                print(f"Error: Invalid JSON response for {username}")
                return None
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                return None
        
        formatted_cards = [
            {
                "slug": card.get("slug"),
                "name": parse_player_string(card.get("slug")).get("name"),
                "birthday": parse_player_string(card.get("slug")).get("birthday"),
                "year": parse_player_string(card.get("slug")).get("card_year"),
                "rarity": parse_player_string(card.get("slug")).get("rarity"),
                "positions": card.get("anyPositions"),
                "sealed": card.get("sealed")
            }
            for card in all_cards
        ]
        
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cards (
                slug TEXT PRIMARY KEY, 
                name TEXT,
                birthday TEXT,
                year TEXT,
                rarity TEXT,
                positions TEXT,
                username TEXT,
                sealed BOOLEAN DEFAULT 0
            )
        ''')

        cursor.execute("DELETE FROM cards WHERE username = ?", (username,))  
        
        for card in formatted_cards:
            #print(f"Inserting card: {card['name']} ({card['slug']})")
            cursor.execute('''
                INSERT OR REPLACE INTO cards (slug, name, birthday, year, rarity, positions, sealed, username)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (card['slug'], card['name'], card['birthday'], card['year'], extract_rarity(card['rarity']), ', '.join(card['positions']), card['sealed'], username))
        
        conn.commit()
        conn.close()
        
        print(f"Data saved to SQLite database 'mlb_sorare.db'")
        return {"nickname": username, "cards": formatted_cards}
def extract_rarity(rarity_string):
    """
    Extracts the base rarity (common, rare, limited) from a string using split.

    Args:
      rarity_string: The rarity string, which may include a UUID.

    Returns:
      The base rarity (common, rare, limited), or the original string if no hyphen.
    """
    if rarity_string is None:
        return None

    parts = rarity_string.split("-")
    return parts[0] if parts else rarity_string

def parse_player_string(player_string):
    match = re.match(r"(.+)-(\d{8})-(\d{4})-(.+)-(\d+)", player_string)
    if match:
        return {
            "name": normalize_name(match.group(1).replace("-", " ")),
            "birthday": match.group(2),
            "card_year": match.group(3),
            "rarity": match.group(4),
            "serial_number": match.group(5)
        }
    return {}

def main():
    client = SorareMLBClient()
    username = input("Enter Sorare username to look up: ")
    
    print(f"Fetching MLB cards (Rare) for user '{username}'...")
    result = client.get_user_mlb_cards(username)
    
if __name__ == "__main__":
    main()
