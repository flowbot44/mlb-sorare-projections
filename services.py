"""
Service layer to orchestrate the lineup generation process.
This separates the core business logic from the web framework (Flask).
"""
import logging
from typing import Dict, List, Set, Optional, Tuple
import pandas as pd
from datetime import datetime, timedelta

from config import Config
from data_fetcher import fetch_cards, fetch_projections, fetch_daily_projections, merge_projections
from database import save_lineups_to_database, get_used_card_slugs, get_db_connection
from lineup_optimizer import build_lineup_optimized, uses_energy_lineup
from utils import determine_game_week, determine_daily_game_week
from card_fetcher import SorareMLBClient 

logger = logging.getLogger(__name__)

def generate_all_lineups_for_user(
    username: str,
    energy_limits: Dict[str, int],
    boost_2025: float,
    stack_boost: float,
    energy_per_card: int,
    ignore_list: List[str],
    ignore_games: List[int],
    custom_lineup_order: List[str] = Config.PRIORITY_ORDER
) -> Dict:
    """
    The main service function to generate, save, and report weekly lineups.
    """
    game_week = determine_game_week()
    logger.info(f"Initiating lineup generation for user '{username}' in game week {game_week}")

    logger.info(f"Fetching latest MLB cards for user '{username}' from Sorare API...") #
    client = SorareMLBClient() #
    card_fetch_result = client.get_user_mlb_cards(username) #

    if card_fetch_result is None: # Handle potential errors during card fetching #
        error_message = "Failed to fetch latest card data from Sorare API." #
        logger.error(error_message) #
        return {"error": error_message} #

    # 1. Fetch and Prepare Data
    cards_df = fetch_cards(username)
    projections_df = fetch_projections(ignore_game_ids=ignore_games)

    if cards_df.empty or projections_df.empty:
        error_message = "Could not fetch necessary card or projection data."
        logger.error(error_message)
        return {"error": error_message}
    
    if ignore_list:
        cards_df = cards_df[~cards_df['name'].str.lower().isin({n.lower() for n in ignore_list})]

    merged_df = merge_projections(cards_df, projections_df)

    # 2. Core Lineup Generation Logic
    used_card_slugs: Set[str] = set()
    remaining_energy = energy_limits.copy()
    all_lineups: Dict[str, Dict] = {}

    lineup_types_to_process = [lt for lt in custom_lineup_order if uses_energy_lineup(lt)] + \
                              [lt for lt in custom_lineup_order if not uses_energy_lineup(lt)]

    for lineup_type in lineup_types_to_process:
        logger.info(f"--- Building lineup for: {lineup_type} ---")
        lineup_data = build_lineup_optimized(
            merged_df, lineup_type, used_card_slugs, remaining_energy,
            boost_2025, stack_boost, energy_per_card
        )

        if lineup_data and lineup_data.get("cards"):
            all_lineups[lineup_type] = lineup_data
            used_card_slugs.update(lineup_data["cards"])
            energy_used = lineup_data.get("energy_used", {})
            remaining_energy["rare"] -= energy_used.get("rare", 0)
            remaining_energy["limited"] -= energy_used.get("limited", 0)
        else:
            all_lineups[lineup_type] = {}

    # 3. Save Results
    if all_lineups:
        save_lineups_to_database(
            all_lineups, username, game_week, boost_2025,
            stack_boost, energy_per_card, custom_lineup_order
        )

    return {"lineups": all_lineups, "cards_df": cards_df, "projections_df": projections_df}


def generate_daily_lineups_for_user(
    username: str,
    energy_limits: Dict[str, int],
    boost_2025: float,
    stack_boost: float,
    ignore_list: List[str],
    ignore_games: List[int],
    swing_max_team_stack: int,
    positional_boosts: Optional[Dict[str, float]] = None,
    position_restrictions: Optional[List[str]] = Config.DAILY_LINEUP_SLOTS
) -> Dict:
    """
    Service function to generate lineups for daily contests.
    """
    game_week = determine_daily_game_week()
    logger.info(f"Initiating daily lineup generation for user '{username}' on {game_week}")
    
    logger.info(f"Fetching latest MLB cards for user '{username}' from Sorare API...") #
    client = SorareMLBClient() #
    card_fetch_result = client.get_user_mlb_cards(username) #

    if card_fetch_result is None: # Handle potential errors during card fetching #
        error_message = "Failed to fetch latest card data from Sorare API." #
        logger.error(error_message) #
        return {"error": error_message} #

    # 1. Fetch Data, respecting cards already used in weekly lineups
    used_in_weekly = get_used_card_slugs(username, game_week)
    cards_df = fetch_cards(username)
    projections_df = fetch_daily_projections(ignore_game_ids=ignore_games)
    
    if cards_df.empty or projections_df.empty:
        return {"error": "Could not fetch cards or daily projections."}
    
    # Filter out ignored and already used cards
    if ignore_list:
        cards_df = cards_df[~cards_df['name'].str.lower().isin({n.lower() for n in ignore_list})]
    cards_df = cards_df[~cards_df['slug'].isin(used_in_weekly)]
    merged_df = merge_projections(cards_df, projections_df)

    # Fetch and apply custom scoring rules for Daily Swing
    from database import get_custom_scoring_rules
    custom_rules = get_custom_scoring_rules(username, "Daily Swing")
    if custom_rules:
        merged_df = apply_custom_scoring_rules(merged_df, custom_rules)

    # 2. Generate Lineups
    all_lineups: Dict[str, Dict] = {}
    used_card_slugs: Set[str] = set()
    remaining_energy = energy_limits.copy()
    energy_per_card = 10 # Daily contest energy is fixed

    for lineup_type in Config.DAILY_LINEUP_ORDER:
        if "Swing" in lineup_type:
            lineup_slots = position_restrictions if position_restrictions is not None else Config.DAILY_LINEUP_SLOTS
            max_stack = swing_max_team_stack
            boosted_df = apply_positional_boosts(merged_df, lineup_type, positional_boosts)
        else:
            lineup_slots = Config.DAILY_LINEUP_SLOTS
            max_stack = Config.MAX_TEAM_STACK
            boosted_df = merged_df

        lineup_data = build_lineup_optimized(
            boosted_df, lineup_type, used_card_slugs, remaining_energy,
            boost_2025, stack_boost, energy_per_card, lineup_slots, max_stack
        )
        if lineup_data and lineup_data.get("cards"):
            all_lineups[lineup_type] = lineup_data
            used_card_slugs.update(lineup_data["cards"])
            energy_used = lineup_data.get("energy_used", {})
            remaining_energy["rare"] -= energy_used.get("rare", 0)
            remaining_energy["limited"] -= energy_used.get("limited", 0)
        else:
            all_lineups[lineup_type] = {}
            
    return {"lineups": all_lineups}


def generate_sealed_cards_report(username: str) -> Tuple[datetime, datetime, datetime, Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    # Get current date and game week dates
        current_date = datetime.now()
        try:
            current_game_week = determine_game_week()
            # Parse the game week to get start and end dates
            start_date_str, end_date_str = current_game_week.split("_to_")
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except Exception:
            # Fallback to 7 days if game week format is unexpected
            start_date = current_date
            end_date = current_date + timedelta(days=7)

        # Part 1: Get sealed cards with projections
        query = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions,
               COUNT(ap.game_id) as game_count,
               SUM(ap.sorare_score) as total_projected_score,
               AVG(ap.sorare_score) as avg_projected_score,
               MIN(ap.game_date) as next_game_date
        FROM cards c
        JOIN adjusted_projections ap ON c.name = ap.player_name
        WHERE c.username = %s AND c.sealed = TRUE AND ap.game_date >= %s
        GROUP BY c.slug, c.name, c.year, c.rarity, c.positions
        ORDER BY next_game_date ASC
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (username, current_date.strftime('%Y-%m-%d')))
        projection_results = cursor.fetchall()

        projections_df = None
        if projection_results:
            # Convert to DataFrame
            columns = ['Slug', 'Name', 'Year', 'Rarity', 'Positions',
                      'Upcoming Games', 'Total Projected Score', 'Avg Score/Game', 'Next Game Date']
            projections_df = pd.DataFrame(projection_results, columns=columns)

            # Format the dataframe - round the scores to 2 decimal places
            projections_df['Total Projected Score'] = projections_df['Total Projected Score'].round(2)
            projections_df['Avg Score/Game'] = projections_df['Avg Score/Game'].round(2)

        # Part 2: Get injured sealed cards
        query = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions, i.status,
               i.description, i.return_estimate, i.team
        FROM cards c
        JOIN injuries i ON c.name = i.player_name
        WHERE c.username = %s AND c.sealed = TRUE AND i.return_estimate IS NOT NULL
        """

        cursor.execute(query, (username,))
        injury_results = cursor.fetchall()

        injured_df = None
        if injury_results:
            # Filter injuries with return dates within game week
            soon_returning = []

            for result in injury_results:
                return_estimate = result[7]

                # Check if return_estimate contains a date string
                try:
                    # Try different date formats
                    for date_format in ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%d/%m/%Y']:
                        try:
                            return_date = datetime.strptime(return_estimate, date_format)
                            if start_date <= return_date <= end_date:
                                soon_returning.append(result)
                            break
                        except ValueError:
                            continue
                except Exception: # Catch all exceptions if return_estimate is not a date string
                    # If return_estimate isn't a date, check if it contains keywords
                    # suggesting imminent return during the game week
                    keywords = ['day to day', 'game time decision', 'probable',
                               'questionable', 'today', 'tomorrow', '1-3 days',
                               'this week', 'expected back', 'returning']
                    if any(keyword in str(return_estimate).lower() for keyword in keywords):
                        soon_returning.append(result)

            if soon_returning:
                columns = ['Slug', 'Name', 'Year', 'Rarity', 'Positions', 'Status',
                          'Description', 'Return Estimate', 'Team']
                injured_df = pd.DataFrame(soon_returning, columns=columns)
        return current_date, start_date, end_date, projections_df, injured_df 
    
def generate_missing_projections_report(username: str) -> pd.DataFrame:
    cards_df = fetch_cards(username)
    projections_df = fetch_projections()
    merged = cards_df.merge(projections_df, left_on="name", right_on="player_name", how="left")
    missing = merged[merged["total_projection"].isna()]
    return missing

def apply_positional_boosts(
    cards_df: pd.DataFrame, 
    lineup_type: str, 
    positional_boosts: Optional[Dict[str, float]] = None
) -> pd.DataFrame:
    """
    Apply positional boosts to cards based on lineup type and position eligibility.
    """
    if not positional_boosts:
        return cards_df

    cards_df = cards_df.copy()
    for position_group, boost_percentage in positional_boosts.items():
        if position_group not in Config.POSITIONS:
            logger.warning(f"Unknown position group: {position_group}")
            continue
        eligible_mask = cards_df["positions"].apply(
            lambda pos: card_eligible_for_position_group(pos, position_group)
        )
        eligible_cards = cards_df[eligible_mask]
        if len(eligible_cards) > 0:
            boost_multiplier = 1 + (boost_percentage / 100.0)
            cards_df.loc[eligible_mask, "final_projection"] *= boost_multiplier
            logger.info(f"Successfully applied {boost_percentage}% boost to {len(eligible_cards)} cards")
        else:
            logger.info(f"No cards eligible for {position_group} boost in {lineup_type}")
    return cards_df

def card_eligible_for_position_group(card_positions: str, position_group: str) -> bool:
    """Check if a card is eligible for a specific position group (e.g., 'OF', 'CI', 'MI')."""
    if pd.isna(card_positions):
        return False
    card_position_set = {pos.strip() for pos in card_positions.split(",")}
    return bool(card_position_set & Config.POSITIONS.get(position_group, set()))

def apply_custom_scoring_rules(df: pd.DataFrame, rules: list[dict]) -> pd.DataFrame:
    df = df.copy()
    for rule in rules:
        applies_to = rule["applies_to"]
        stat = rule["stat"]
        new_value = rule["new_value"]
        # Example: For pitcher stats, update the relevant columns
        if applies_to == "pitcher":
            mask = df["positions"].str.contains("P") | df["positions"].str.contains("SP") | df["positions"].str.contains("RP")
            if stat in df.columns:
                df.loc[mask, stat] = new_value
        elif applies_to == "hitter":
            mask = ~df["positions"].str.contains("P|SP|RP")
            if stat in df.columns:
                df.loc[mask, stat] = new_value
        elif applies_to == "all":
            if stat in df.columns:
                df[stat] = new_value
    return df

