"""
Handles all database interactions for the Sorare Lineup Optimizer.
"""
import json
import logging
import traceback
from typing import Dict, Set
from config import Config

from utils import get_db_connection  # Assuming utils.py contains get_db_connection

logger = logging.getLogger(__name__)

def create_lineups_table() -> None:
    """Create the lineups table in the database if it doesn't already exist."""
    conn = get_db_connection()
    if not conn:
        logger.error("Could not establish database connection.")
        return

    create_table_query = """
    CREATE TABLE IF NOT EXISTS lineups (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100) NOT NULL,
        game_week VARCHAR(50) NOT NULL,
        lineup_type VARCHAR(50) NOT NULL,
        cards JSON NOT NULL,
        slot_assignments JSON NOT NULL,
        projections JSON NOT NULL,
        projected_score DECIMAL(10,2) NOT NULL,
        energy_used JSON NOT NULL,
        boost_2025 DECIMAL(5,2) NOT NULL,
        stack_boost DECIMAL(5,2) NOT NULL,
        energy_per_card INT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (username, game_week, lineup_type)
    );
    """
    create_index_query = "CREATE INDEX IF NOT EXISTS idx_username_gameweek ON lineups (username, game_week);"

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            cursor.execute(create_index_query)
            conn.commit()
            logger.info("Lineups table created or verified successfully.")
    except Exception as e:
        logger.error(f"Error creating lineups table: {e}")
        conn.rollback()
    finally:
        conn.close()


def save_lineups_to_database(
    lineups: Dict[str, Dict], 
    username: str, 
    game_week: str,
    boost_2025: float, 
    stack_boost: float, 
    energy_per_card: int, 
    custom_lineup_order: list[str]
) -> None:
    """Save lineups to the database, replacing existing ones for the same username and game week."""
    create_lineups_table()  # Ensure table exists

    conn = get_db_connection()
    if not conn:
        logger.error("Could not establish database connection for saving lineups.")
        return

    try:
        with conn.cursor() as cursor:
            # Delete existing lineups for this user and game week
            delete_query = "DELETE FROM lineups WHERE username = %s AND game_week = %s"
            cursor.execute(delete_query, (username, game_week))
            logger.info(f"Deleted {cursor.rowcount} existing lineups for {username} in game week {game_week}")

            # Insert new lineups
            insert_query = """
            INSERT INTO lineups (
                username, game_week, lineup_type, cards, slot_assignments, 
                projections, projected_score, energy_used, boost_2025, 
                stack_boost, energy_per_card
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            inserted_count = 0
            for lineup_type in custom_lineup_order:
                data = lineups.get(lineup_type)
                if data and data.get("cards"):
                    params = (
                        username,
                        game_week,
                        lineup_type,
                        json.dumps(data["cards"]),
                        json.dumps(data["slot_assignments"]),
                        json.dumps(data["projections"]),
                        data["projected_score"],
                        json.dumps(data["energy_used"]),
                        boost_2025,
                        stack_boost,
                        energy_per_card
                    )
                    cursor.execute(insert_query, params)
                    inserted_count += 1
            
            conn.commit()
            logger.info(f"Successfully saved {inserted_count} new lineups for {username} in game week {game_week}")
    except Exception as e:
        logger.error(
            f"Error saving lineups to database for {username} in game week {game_week}: {e}\n"
            f"Traceback: {traceback.format_exc()}"
        )
        conn.rollback()
    finally:
        conn.close()


def get_used_card_slugs(username: str, game_week: str) -> Set[str]:
    """Get slugs of cards already used in a given game week's non-daily lineups."""
    conn = get_db_connection()
    if not conn:
        return set()
    
    used_slugs = set()
    query = """
        SELECT cards FROM lineups 
        WHERE username = %s AND game_week = %s AND lineup_type NOT LIKE '%%Daily%%'
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (username, game_week))
            results = cursor.fetchall()
            for row in results:
                cards_data = row[0]
                if isinstance(cards_data, str):
                    used_slugs.update(json.loads(cards_data))
                elif isinstance(cards_data, list):
                    used_slugs.update(cards_data)
        logger.info(f"Loaded {len(used_slugs)} used card slugs for {username} in game week {game_week}.")
    except Exception as e:
        logger.error(f"Error loading used cards: {e}")
    finally:
        conn.close()
    return used_slugs

def get_weekly_lineup_details(username: str, game_week: str) -> Dict[str, Dict]:
    """
    Gets details of cards used in weekly lineups, to show why they are excluded from daily.
    """
    conn = get_db_connection()
    if not conn:
        return {}
    
    excluded_details = {}
    query = """
        SELECT lineup_type, cards, slot_assignments, projections, projected_score
        FROM lineups
        WHERE username = %s AND game_week = %s AND lineup_type NOT LIKE '%%Daily%%'
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (username, game_week))
            results = cursor.fetchall()

            for row in results:
                lineup_type, cards_slugs, slots, projections, score = row
                
                # Handle both JSON string and list for cards_slugs, slots, projections
                if isinstance(cards_slugs, str):
                    slug_list = json.loads(cards_slugs)
                else:
                    slug_list = cards_slugs
                if not slug_list: continue

                if isinstance(slots, str):
                    slots_list = json.loads(slots)
                else:
                    slots_list = slots
                if isinstance(projections, str):
                    projections_list = json.loads(projections)
                else:
                    projections_list = projections

                # Create a placeholder for each slug to pass to the query
                slug_placeholders = ','.join(['%s'] * len(slug_list))
                card_info_query = f"SELECT slug, name FROM cards WHERE slug IN ({slug_placeholders})"
                cursor.execute(card_info_query, slug_list)
                
                card_name_map = {slug: name for slug, name in cursor.fetchall()}

                excluded_cards = []
                for slug, slot, proj in zip(slug_list, slots_list, projections_list):
                    excluded_cards.append({
                        "card_name": card_name_map.get(slug, slug), # Fallback to slug
                        "slot": slot,
                        "projection": proj
                    })
                
                # Sort by standard lineup slot order for consistent display
                slot_order = {slot: idx for idx, slot in enumerate(Config.LINEUP_SLOTS)}
                sorted_cards = sorted(excluded_cards, key=lambda x: slot_order.get(x["slot"], 999))
                
                excluded_details[lineup_type] = {
                    "cards": sorted_cards,
                    "projected_score": score
                }
        logger.info(f"Loaded details for {len(excluded_details)} excluded lineups for {username}.")
        return excluded_details
    except Exception as e:
        logger.error(f"Error loading excluded lineup details: {e}\n{traceback.format_exc()}")
        return {}
    finally:
        conn.close()