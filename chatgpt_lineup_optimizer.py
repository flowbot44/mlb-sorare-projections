import pandas as pd
from ortools.sat.python import cp_model
import os
import argparse
from typing import Dict, List, Set, Optional
from utils import determine_game_week, get_db_connection, get_sqlalchemy_engine, determine_daily_game_week # Add get_sqlalchemy_engine
from datetime import datetime, timedelta
from pandas.errors import DatabaseError
import json
import logging
import traceback
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("lineup_optimizer")

# Configuration Constants
class Config:
    USERNAME = "flowbot44"
    SHOHEI_NAME = "shohei-ohtani"
    BOOST_2025 = 5.0
    STACK_BOOST = 2.0
    ENERGY_PER_NON_2025_CARD = 25
    DEFAULT_ENERGY_LIMITS = {"rare": 50, "limited": 50}
    PRIORITY_ORDER = [
        "Rare Champion_1", "Rare Champion_2", "Rare Champion_3",
        "Rare All-Star_1", "Rare All-Star_2", "Rare All-Star_3",
        "Rare Challenger_1", "Rare Challenger_2",
        "Limited All-Star_1", "Limited All-Star_2", "Limited All-Star_3",
        "Limited Champion_1", "Limited Champion_2", "Limited Champion_3",
        "Limited Challenger_1", "Limited Challenger_2",
        "Common Minors"
    ]
    DAILY_LINEUP_ORDER = [
        "Rare Derby", "Rare Swing",
        "Limited Derby", "Limited Swing",
        "Common Derby", "Common Swing"        
    ]
    ALL_STAR_LIMITS = {
        "Rare All-Star": {"max_limited": 3, "allowed_rarities": {"rare", "limited"}},
        "Limited All-Star": {"max_common": 3, "allowed_rarities": {"limited", "common"}}
    }
    POSITIONS = {
        "CI": {"baseball_first_base", "baseball_third_base", "baseball_designated_hitter"},
        "MI": {"baseball_shortstop", "baseball_second_base", "baseball_catcher"},
        "OF": {"baseball_outfield"},
        "SP": {"baseball_starting_pitcher"},
        "RP": {"baseball_relief_pitcher"}
    }
    POSITIONS["H"] = POSITIONS["CI"] | POSITIONS["MI"] | POSITIONS["OF"]
    POSITIONS["Flx"] = POSITIONS["CI"] | POSITIONS["MI"] | POSITIONS["OF"] | POSITIONS["RP"]
    POSITIONS["Flx+"] = POSITIONS["CI"] | POSITIONS["MI"] | POSITIONS["OF"] | POSITIONS["RP"] | POSITIONS["SP"]
    LINEUP_SLOTS = ["SP", "RP", "CI", "MI", "OF", "H", "Flx"]
    DAILY_LINEUP_SLOTS = ["SP", "RP", "CI", "MI", "OF", "H", "Flx+"]

def create_lineups_table():
    
    """Create the lineups table if it doesn't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
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
    
    create_index_query = """
    CREATE INDEX IF NOT EXISTS idx_username_gameweek ON lineups (username, game_week);
    """

    try:
        cursor.execute(create_table_query)
        cursor.execute(create_index_query)
        conn.commit()
        logger.info("Lineups table created or verified successfully")
    except Exception as e:
        logger.error(f"Error creating lineups table: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def save_lineups_to_database(lineups: Dict[str, Dict], username: str, game_week: str,
                           boost_2025: float, stack_boost: float, energy_per_card: int, custom_lineup_order: list[str]) -> None:
    """Save lineups to the database, replacing existing ones for the same username and game week."""
    create_lineups_table()  # Ensure table exists
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # First, delete existing lineups for this username and game week
        delete_query = """
        DELETE FROM lineups 
        WHERE username = %s AND game_week = %s
        """
        cursor.execute(delete_query, (username, game_week))
        deleted_count = cursor.rowcount
        if deleted_count > 0:
            logger.info(f"Deleted {deleted_count} existing lineups for {username} in game week {game_week}")
        
        # Insert new lineups
        insert_query = """
        INSERT INTO lineups (
            username, game_week, lineup_type, cards, slot_assignments, 
            projections, projected_score, energy_used, boost_2025, 
            stack_boost, energy_per_card
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        logger.info(custom_lineup_order)
        inserted_count = 0
        for lineup_type in custom_lineup_order:
            data = lineups.get(lineup_type)
            logger.info(f"Saving lineups to database for {username} in game week {game_week} with type {lineup_type} and cards  ")
            if data and data["cards"]:  # Only save lineups that have cards
                cursor.execute(insert_query, (
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
                ))
                inserted_count += 1
                
        
        conn.commit()
        logger.info(f"Successfully saved {inserted_count} lineups to database for {username} in game week {game_week}")
        
    except Exception as e:
        logger.error(
            f"Error saving lineups to database for {username} in game week {game_week}: {e}\n"
            f"Traceback: {traceback.format_exc()}"
        )
    finally:
        cursor.close()
        conn.close()

def get_used_card_slugs(username: str, game_week: str) -> Set[str]:
    """Get slugs of cards already used in a game week."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT cards FROM lineups WHERE username = %s AND game_week = %s",
            (username, game_week)
        )
        results = cursor.fetchall()
        used = set()
        for row in results:
            cards = row[0]
            if isinstance(cards, str):
                used.update(json.loads(cards))
            elif isinstance(cards, list):
                used.update(cards)
        return used
    except Exception as e:
        logger.error(f"Error loading used cards: {e}")
        return set()
    finally:
        cursor.close()
        conn.close()

def fetch_cards(username: str) -> pd.DataFrame:
    """Fetch eligible cards with team info from the database."""
    engine = get_sqlalchemy_engine()
    query = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions, c.username, c.sealed, pt.team_id
        FROM cards c
        LEFT JOIN player_teams pt ON c.name = pt.player_name
        WHERE c.username = %s AND c.sealed = FALSE
    """
    cards_df = pd.read_sql(query, engine, params=(username,))
    print(f"cards found {len(cards_df)}")
    cards_df["year"] = cards_df["year"].astype(int)
    cards_df.loc[cards_df["slug"].str.contains(Config.SHOHEI_NAME), "positions"] = "baseball_designated_hitter"

    # Add debug info for players with same name but different teams
    # Convert team_id to integer if it's coming back as float
    if 'team_id' in cards_df.columns:
        cards_df['team_id'] = cards_df['team_id'].fillna(-1).astype(int)
        
    # Better check for players with same name but different teams
    name_groups = cards_df.groupby("name")["team_id"].nunique()
    multi_team_players = name_groups[name_groups > 1].index.tolist()
    
    if multi_team_players:
        print("Note: Found players who truly appear on multiple teams:")
        for name in multi_team_players:
            teams = cards_df[cards_df["name"] == name]["team_id"].unique()
            print(f"  - {name}: Teams {teams}")

    return cards_df

def fetch_projections(ignore_game_ids: Optional[List] = None) -> pd.DataFrame:
    """
    Fetch Sorare projections for the current game week with team separation.
    
    Args:
        ignore_game_ids: Optional list of game IDs to exclude from the results
        
    Returns:
        DataFrame with player projections
    """
    engine = get_sqlalchemy_engine()
    current_game_week = determine_game_week()
    
    # Base query
    query = """
        SELECT player_name, team_id, SUM(sorare_score) AS total_projection 
        FROM adjusted_projections 
        WHERE game_week = %s
    """
    
    params = [current_game_week]
    
    # Add filter for ignored game IDs if provided
    if ignore_game_ids and len(ignore_game_ids) > 0:
        placeholder = ','.join(['%s'] * len(ignore_game_ids))
        query += f" AND game_id NOT IN ({placeholder})"
        params.extend(ignore_game_ids)
    
    # Complete the query with grouping
    query += " GROUP BY player_name, team_id"
    
    # Execute query and return dataframe
    projections_df = pd.read_sql(query, engine, params=tuple(params))
    projections_df["total_projection"] = projections_df["total_projection"].fillna(0).infer_objects()
    
    return projections_df


def fetch_daily_projections(ignore_game_ids: Optional[List] = None) -> pd.DataFrame:
    """
    Fetch Sorare projections for games scheduled today (Eastern time),
    using `local_date` from the `games` table.
    """
    engine = get_sqlalchemy_engine()
    today = datetime.now().date().isoformat()  # e.g., '2025-05-24'

    query = """
        SELECT player_name, team_id, SUM(sorare_score) AS total_projection
        FROM adjusted_projections ap
        JOIN games g ON ap.game_id = g.id
        WHERE g.local_date = %s
    """
    params = [today]

    if ignore_game_ids:
        placeholder = ','.join(['%s'] * len(ignore_game_ids))
        query += f" AND ap.game_id NOT IN ({placeholder})"
        params.extend(ignore_game_ids)

    query += " GROUP BY player_name, team_id"

    df = pd.read_sql(query, engine, params=tuple(params))
    df["total_projection"] = df["total_projection"].fillna(0).infer_objects()
    return df


def can_fill_position(card_positions: Optional[str], slot: str) -> bool:
    """Check if a card can fill the given position slot."""
    if pd.isna(card_positions):
        return False
    return bool(set(card_positions.split(",")) & Config.POSITIONS[slot])

def is_hitter(card_positions: Optional[str]) -> bool:
    """Check if a card is a hitter (eligible for CI, MI, OF, or H)."""
    if pd.isna(card_positions):
        return False
    return bool(set(card_positions.split(",")) & Config.POSITIONS["H"])

def check_missing_projections(cards_df: pd.DataFrame, projections_df: pd.DataFrame) -> None:
    """Validate that all cards have projections, printing errors if not."""
    merged = cards_df.merge(projections_df, left_on="name", right_on="player_name", how="left")
    missing = merged[merged["total_projection"].isna()]
    if not missing.empty:
        print("Error: The following players lack projections:")
        for _, row in missing.iterrows():
            print(f"  - {row['name']} (slug: {row['slug']})")

def apply_boosts(cards_df: pd.DataFrame, lineup_type: str, boost_2025: float) -> pd.DataFrame:
    """Apply 2025 boost to card projections."""
    cards_df["selection_projection"] = cards_df["total_projection"]
    if "Challenger" not in lineup_type and "Minors" not in lineup_type:
        cards_df.loc[cards_df["year"] == 2025, "selection_projection"] += boost_2025
    return cards_df

def get_rarity_from_lineup_type(lineup_type: str) -> str:
    """Extract rarity from lineup type."""
    return "common" if "Common" in lineup_type else "limited" if "Limited" in lineup_type else "rare"

def filter_cards_by_lineup_type(cards_df: pd.DataFrame, lineup_type: str) -> pd.DataFrame:
    """Filter cards based on lineup type and rarity rules."""
    rarity = get_rarity_from_lineup_type(lineup_type)
    if "Champion" in lineup_type or "Challenger" in lineup_type:
        return cards_df[cards_df["rarity"] == rarity]
    elif "Minors" in lineup_type:
        return cards_df[cards_df["rarity"] == "common"]
    elif "All-Star" in lineup_type:
        limits = Config.ALL_STAR_LIMITS.get(f"{rarity.capitalize()} All-Star", {})
        return cards_df[cards_df["rarity"].isin(limits["allowed_rarities"])]
    elif "Derby" in lineup_type:
        return cards_df[cards_df["rarity"] == rarity]
    elif "Swing" in lineup_type:
        return cards_df[cards_df["rarity"] == rarity]
    return cards_df

def uses_energy_lineup(lineup_type: str) -> bool:
    """Determine if a lineup type uses energy."""
    return "All-Star" in lineup_type or "Champion" in lineup_type or "Derby" in lineup_type

def build_lineup(cards_df: pd.DataFrame, lineup_type: str, used_cards: Set[str],
                 remaining_energy: Dict[str, int], boost_2025: float, stack_boost: float,
                 energy_per_card: int, lineup_slots: list[str] = Config.LINEUP_SLOTS) -> Dict:
    """Build a single lineup by prioritizing highest projected cards across all positions."""
    available_cards = cards_df[~cards_df["slug"].isin(used_cards)].copy()
    available_cards = apply_boosts(available_cards, lineup_type, boost_2025)
    available_cards = filter_cards_by_lineup_type(available_cards, lineup_type)

    lineup = []
    slot_assignments = []
    projections = []
    used_players = set()
    rarity_count = {"common": 0, "limited": 0, "rare": 0}
    team_counts = {}
    energy_used = {"rare": 0, "limited": 0}
    remaining_slots = lineup_slots.copy()

    rarity = get_rarity_from_lineup_type(lineup_type)
    uses_energy = uses_energy_lineup(lineup_type)
    
    # Add effective projection with stacking
    available_cards["effective_projection"] = available_cards["selection_projection"]
    for slot in lineup_slots:
        is_hitting_slot = slot in ["CI", "MI", "OF", "H", "Flx"]
        if is_hitting_slot:
            available_cards.loc[
                available_cards["positions"].apply(is_hitter), "effective_projection"
            ] += available_cards["team_id"].map(lambda x: team_counts.get(x, 0)) * stack_boost

    # Sort cards by best projections first
    available_cards = available_cards.sort_values("effective_projection", ascending=False)

    limits = Config.ALL_STAR_LIMITS.get(f"{rarity.capitalize()} All-Star", {}) if "All-Star" in lineup_type else {}

    for _, card in available_cards.iterrows():
        card_key = (card["name"], card["team_id"])
        if card_key in used_players:
            continue

        # Try placing the card in a valid slot
        for slot in remaining_slots:
            if not can_fill_position(card["positions"], slot):
                continue

            card_rarity = card["rarity"]
            is_non_2025 = card["year"] != 2025
            energy_cost = energy_per_card if (uses_energy and is_non_2025 and card_rarity in remaining_energy) else 0

            if energy_cost > 0 and remaining_energy.get(card_rarity, 0) < energy_cost:
                continue

            if "max_common" in limits and rarity_count["common"] >= limits["max_common"] and card_rarity == "common":
                continue
            if "max_limited" in limits and rarity_count["limited"] >= limits["max_limited"] and card_rarity == "limited":
                continue

            # Assign this card to the lineup
            lineup.append(card["slug"])
            slot_assignments.append(slot)
            projections.append(card["total_projection"])
            used_players.add(card_key)
            rarity_count[card_rarity] += 1
            remaining_slots.remove(slot)

            if uses_energy and is_non_2025 and card_rarity in remaining_energy:
                remaining_energy[card_rarity] -= energy_cost
                energy_used[card_rarity] += energy_cost

            if is_hitter(card["positions"]):
                team_counts[card["team_id"]] = team_counts.get(card["team_id"], 0) + 1

            break  # Move to next card

        if not remaining_slots:
            break

    if len(lineup) == 7:
        return {
            "cards": lineup,
            "slot_assignments": slot_assignments,
            "projections": projections,
            "projected_score": round(sum(projections), 2),
            "energy_used": energy_used
        }
    return {"cards": [], "slot_assignments": [], "projections": [], "projected_score": 0, "energy_used": {"rare": 0, "limited": 0}}

def build_lineup_optimized(cards_df: pd.DataFrame, lineup_type: str, used_cards: Set[str],
                           remaining_energy: Dict[str, int], boost_2025: float, stack_boost: float,
                           energy_per_card: int, lineup_slots: list[str] = Config.LINEUP_SLOTS) -> Dict:
    """Build a lineup using OR-Tools with a second pass for stacking boosts (hitters only)."""

    def run_optimization(cards, projections_override=None):
        model = cp_model.CpModel()
        card_vars = []
        card_slot_vars = []

        for i, card in enumerate(cards):
            var = model.NewBoolVar(f"use_{i}")
            card_vars.append(var)

            slot_vars = {}
            for slot in lineup_slots:
                if can_fill_position(card["positions"], slot):
                    slot_vars[slot] = model.NewBoolVar(f"{i}_{slot}")
            card_slot_vars.append(slot_vars)

        model.Add(sum(card_vars) == 7)

        for slot in lineup_slots:
            model.Add(sum(card_slot_vars[i].get(slot, 0) for i in range(len(cards))) == 1)

        for i, slot_vars in enumerate(card_slot_vars):
            model.Add(sum(slot_vars.values()) == card_vars[i])

        name_team_to_indices = {}
        for i, card in enumerate(cards):
            key = (card["name"], card["team_id"])
            name_team_to_indices.setdefault(key, []).append(i)
        for indices in name_team_to_indices.values():
            if len(indices) > 1:
                model.Add(sum(card_vars[i] for i in indices) <= 1)

        # Rarity rules
        rarity = get_rarity_from_lineup_type(lineup_type)
        uses_energy = uses_energy_lineup(lineup_type)
        limits = Config.ALL_STAR_LIMITS.get(f"{rarity.capitalize()} All-Star", {}) if "All-Star" in lineup_type else {}
        for rar in ["common", "limited", "rare"]:
            max_key = f"max_{rar}"
            indices = [i for i, c in enumerate(cards) if c["rarity"] == rar]
            if max_key in limits:
                model.Add(sum(card_vars[i] for i in indices) <= limits[max_key])

        if "allowed_rarities" in limits:
            allowed = limits["allowed_rarities"]
            disallowed_indices = [i for i, c in enumerate(cards) if c["rarity"] not in allowed]
            for i in disallowed_indices:
                model.Add(card_vars[i] == 0)

        if uses_energy:
            for rar in ["rare", "limited"]:
                indices = [i for i, c in enumerate(cards)
                           if c["rarity"] == rar and c["year"] != 2025]
                total_energy = sum(card_vars[i] * energy_per_card for i in indices)
                model.Add(total_energy <= remaining_energy.get(rar, 0))

        # Objective
        proj_values = projections_override if projections_override else [
            int(card["selection_projection"] * 100) for card in cards
        ]
        model.Maximize(sum(card_vars[i] * proj_values[i] for i in range(len(cards))))

        # Solve
        solver = cp_model.CpSolver()
        status = solver.Solve(model)
        return status, solver, card_vars, card_slot_vars

    # Prep cards
    available_cards = cards_df[~cards_df["slug"].isin(used_cards)].copy()
    available_cards = apply_boosts(available_cards, lineup_type, boost_2025)
    available_cards = filter_cards_by_lineup_type(available_cards, lineup_type)

    if len(available_cards) < 7:
        return {"cards": [], "slot_assignments": [], "projections": [], "projected_score": 0,
                "energy_used": {"rare": 0, "limited": 0}}

    cards = available_cards.to_dict("records")

    # --- PASS 1: Run with raw projections ---
    status, solver, card_vars, card_slot_vars = run_optimization(cards)
    if status != cp_model.OPTIMAL:
        return {"cards": [], "slot_assignments": [], "projections": [], "projected_score": 0,
                "energy_used": {"rare": 0, "limited": 0}}

    # --- PASS 2: Count team stacks from hitters in the selected lineup ---
    team_stack_counts = {}
    for i, card in enumerate(cards):
        if solver.BooleanValue(card_vars[i]) and is_hitter(card["positions"]):
            team_id = card["team_id"]
            team_stack_counts[team_id] = team_stack_counts.get(team_id, 0) + 1

    # Keep only the top 3 most stacked teams
    top_team_ids = sorted(team_stack_counts.items(), key=lambda x: -x[1])[:3]
    top_stack_teams = {team_id for team_id, _ in top_team_ids}

    # --- PASS 3: Re-run optimizer with boosted projections for stacked hitters ---
    projections_with_stack = []
    for i, card in enumerate(cards):
        proj = card["selection_projection"]
        if is_hitter(card["positions"]) and card["team_id"] in top_stack_teams:
            stack_size = team_stack_counts[card["team_id"]]
            proj += stack_size * stack_boost
        projections_with_stack.append(int(proj * 100))

    status, solver, card_vars, card_slot_vars = run_optimization(cards, projections_with_stack)
    if status != cp_model.OPTIMAL:
        return {"cards": [], "slot_assignments": [], "projections": [], "projected_score": 0,
                "energy_used": {"rare": 0, "limited": 0}}

    # --- Final output ---
    lineup, slots, projections = [], [], []
    energy_used = {"rare": 0, "limited": 0}
    uses_energy = uses_energy_lineup(lineup_type)

    for i, card in enumerate(cards):
        if solver.BooleanValue(card_vars[i]):
            assigned_slot = next(slot for slot, var in card_slot_vars[i].items()
                                 if solver.BooleanValue(var))
            lineup.append(card["slug"])
            slots.append(assigned_slot)
            projections.append(card["total_projection"])

            if uses_energy and card["year"] != 2025 and card["rarity"] in energy_used:
                energy_used[card["rarity"]] += energy_per_card
                remaining_energy[card["rarity"]] -= energy_per_card

     # Sort lineup, slots, and projections by the order of lineup_slots
    slot_order = {slot: idx for idx, slot in enumerate(lineup_slots)}
    sorted_entries = sorted(
        zip(lineup, slots, projections),
        key=lambda x: slot_order.get(x[1], 999)
    )
    lineup, slots, projections = zip(*sorted_entries) if sorted_entries else ([], [], [])

    return {
        "cards": list(lineup),
        "slot_assignments": list(slots),
        "projections": list(projections),
        "projected_score": round(sum(projections), 2),
        "energy_used": energy_used
    }


def build_all_lineups(cards_df: pd.DataFrame, projections_df: pd.DataFrame, energy_limits: Dict[str, int],
                      boost_2025: float, stack_boost: float, energy_per_card: int,
                      ignore_list: Optional[List[str]] = None, custom_lineup_order: list[str] = Config.PRIORITY_ORDER) -> Dict[str, Dict]:

    if ignore_list:
        cards_df = cards_df[~cards_df['name'].str.upper().isin({n.upper() for n in ignore_list})]

    cards_df = merge_projections(cards_df, projections_df)
    lineups = generate_lineups_from_cards(cards_df, boost_2025, stack_boost, energy_per_card, energy_limits, custom_lineup_order)
    save_lineups_to_database(lineups, Config.USERNAME, determine_game_week(), boost_2025, stack_boost, energy_per_card, custom_lineup_order)
    return lineups

def build_daily_lineups(username: str,  energy_limits: Dict[str, int], boost_2025: float, stack_boost: float,
                        ignore_players: Optional[List[str]] = None, custom_lineup_order: list[str] = Config.DAILY_LINEUP_ORDER,
                        lineup_slots: list[str] = Config.DAILY_LINEUP_SLOTS, derby_lineup_slots: list[str] = Config.DAILY_LINEUP_SLOTS) -> Dict[str, Dict]:
    energy_per_card = 10
    game_week = determine_daily_game_week()

    used_cards = get_used_card_slugs(username, game_week)
    cards_df = get_eligible_cards(username, ignore_players, used_cards)
    projections_df = fetch_daily_projections()
    cards_df = merge_projections(cards_df, projections_df)

    lineups = generate_lineups_from_cards(cards_df, boost_2025, stack_boost, energy_per_card, energy_limits, custom_lineup_order, lineup_slots, derby_lineup_slots)

    return lineups

def generate_lineups_from_cards(cards_df: pd.DataFrame, boost_2025: float, stack_boost: float,
                                energy_per_card: int, energy_limits: Dict[str, int], custom_lineup_order: list[str],
                                lineup_slots: list[str] = Config.LINEUP_SLOTS, derby_lineup_slots: list[str] = Config.LINEUP_SLOTS) -> Dict[str, Dict]:
    """Generate full set of lineups using greedy and OR-Tools builders."""
    used_card_slugs = set()
    remaining_energy = energy_limits.copy()

    lineups = {lt: {"cards": [], "slot_assignments": [], "projections": [], "projected_score": 0,
                    "energy_used": {"rare": 0, "limited": 0}} for lt in custom_lineup_order}

    energy_lineups = [lt for lt in custom_lineup_order if uses_energy_lineup(lt)]
    non_energy_lineups = [lt for lt in custom_lineup_order if not uses_energy_lineup(lt)]

    first = True
    for lineup_type in energy_lineups + non_energy_lineups:
        #builder = build_lineup if first else build_lineup_optimized
        lineup_data = build_lineup_optimized(
            cards_df, lineup_type, used_card_slugs, remaining_energy,
            boost_2025, stack_boost, energy_per_card, lineup_slots
        )
        if lineup_data["cards"]:
            lineups[lineup_type] = lineup_data
            used_card_slugs.update(lineup_data["cards"])
        #first = False



    return lineups

def merge_projections(cards_df: pd.DataFrame, projections_df: pd.DataFrame) -> pd.DataFrame:
    """Merge cards with projections, defaulting missing values to zero."""
    merged = cards_df.merge(
        projections_df,
        left_on=["name", "team_id"],
        right_on=["player_name", "team_id"],
        how="left"
    )
    merged = merged.infer_objects()
    merged = merged.fillna({"total_projection": 0})
    return merged

def get_eligible_cards(username: str, ignore_list: Optional[List[str]] = None, used_card_slugs: Optional[Set[str]] = None) -> pd.DataFrame:
    """Fetch cards excluding sealed and used/ignored players."""
    cards_df = fetch_cards(username)

    if used_card_slugs:
        cards_df = cards_df[~cards_df["slug"].isin(used_card_slugs)]

    if ignore_list:
        ignore_set = {name.strip().upper() for name in ignore_list}
        cards_df = cards_df[~cards_df["name"].str.upper().isin(ignore_set)]

    return cards_df

def fetch_high_rain_games_details():
    """
    Fetch details for games with rain probability >= 75% for the current game week,
    joining with Games and Stadiums tables.
    """
    engine = get_sqlalchemy_engine()
    query = """
        SELECT
            wf.game_id,
            g.date AS game_date,
            g.time AS game_time_utc,
            wf.rain,
            wf.temp,
            wf.wind_speed,
            wf.wind_dir,
            g.home_team_id,
            g.away_team_id,
            s.name as stadium_name
        FROM weather_forecasts wf
        JOIN games g ON wf.game_id = g.id
        LEFT JOIN stadiums s ON g.stadium_id = s.id
        WHERE wf.rain >= 75
        ORDER BY g.date ASC, g.time ASC;
    """
    try:
        df = pd.read_sql(query, engine)
        # Basic type conversion check
        df['rain'] = pd.to_numeric(df['rain'], errors='coerce')
        df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
        df['wind_speed'] = pd.to_numeric(df['wind_speed'], errors='coerce')
        df['wind_dir'] = pd.to_numeric(df['wind_dir'], errors='coerce')
        df = df.dropna(subset=['rain']) # Remove rows where rain couldn't be parsed
        return df
    except DatabaseError as e:
        print(f"Database query error: {e}. Check if tables 'games' or 'stadiums' exist or have correct columns.")
        return pd.DataFrame(columns=['game_id', 'game_date', 'game_time_utc', 'rain', 'temp', 'wind_speed', 'wind_dir', 'home_team_id', 'away_team_id', 'stadium_name'])
    except Exception as e:
        print(f"An unexpected error occurred during fetch_high_rain_games_details: {e}")
        return pd.DataFrame(columns=['game_id', 'game_date', 'game_time_utc', 'rain', 'temp', 'wind_speed', 'wind_dir', 'home_team_id', 'away_team_id', 'stadium_name'])

def generate_weather_report() -> str:
    """Generate a more user-friendly report of high-rain games, focusing on the date."""
    report_lines = []
    report_lines.append("\n## WEATHER WATCH: Potential Rain Impact ##\n")

    try:
        high_rain_games = fetch_high_rain_games_details()

        if high_rain_games.empty:
            report_lines.append("No games found with a high rain probability (>= 75%) in the forecast.")
        else:
            report_lines.append(f"Found {len(high_rain_games)} game(s) with >= 75% rain probability:")
            report_lines.append("These games *may* face delays or postponement:\n")

            for _, game in high_rain_games.iterrows():
                game_id = int(game['game_id'])
                stadium_name = game['stadium_name'] if pd.notna(game['stadium_name']) else "Unknown Stadium"
                away_team = f"Team {game['away_team_id']}"
                home_team = f"Team {game['home_team_id']}"

                game_date_str = "Date Unknown"
                try:
                    # Parse the date string (assuming YYYY-MM-DD format from DB)
                    game_date_obj = datetime.strptime(str(game['game_date']), '%Y-%m-%d').date()
                    # Format the date clearly
                    game_date_str = game_date_obj.strftime("%a, %b %d, %Y") # Format: Fri, Apr 11, 2025
                except ValueError as date_err:
                    print(f"Warning: Could not parse game date '{game['game_date']}' for game {game_id}. Error: {date_err}")
                except Exception as general_date_err:
                     print(f"Warning: An error occurred during date formatting for game {game_id}. Error: {general_date_err}")

                report_lines.append(f"  - Forecast: {game['rain']:.0f}% Rain - Date: {game_date_str} - Location: {stadium_name}") # Display formatted date
                report_lines.append(f"  - Gameday Link: https://baseballsavant.mlb.com/preview?game_pk={game_id}")
                report_lines.append("") # Add a blank line for readability

    except Exception as e:
        report_lines.append(f"Error generating weather report: {e}")
        print(f"Error details in generate_weather_report: {e}") # Added print for debugging

    return "\n".join(report_lines)

def generate_sealed_cards_report(username: str) -> str:
    """
    Generate a report of sealed cards with projections and injury info.

    """

    report = []
    
    try:
        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get current date
        current_date = datetime.now()
        report.append(f"Sealed Cards Report generated on: {current_date.strftime('%Y-%m-%d')}")
        report.append("=" * 80)
        
        # Get game week dates
        game_week = determine_game_week()
        try:
            # Parse the game week to get start and end dates
            start_date_str, end_date_str = game_week.split("_to_")
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except:
            # Fallback to 7 days if game week format is unexpected
            start_date = current_date
            end_date = current_date + timedelta(days=7)
        
        # Part 1: Sealed cards with projections (distinct cards with totaled projections)
        report.append("\n## SEALED CARDS WITH UPCOMING PROJECTIONS (TOTALED) ##\n")
        
        query = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions, 
               COUNT(ap.game_id) as game_count, 
               SUM(ap.sorare_score) as total_projected_score,
               AVG(ap.sorare_score) as avg_projected_score,
               MIN(ap.game_date) as next_game_date
        FROM cards c
        JOIN adjustedarojections ap ON c.name = ap.player_name
        WHERE c.username = %s AND c.sealed = TRUE AND ap.game_date >= %s
        GROUP BY c.slug, c.name, c.year, c.rarity, c.positions
        ORDER BY next_game_date ASC
        """
        
        cursor.execute(query, (username, current_date.strftime('%Y-%m-%d')))
        projection_results = cursor.fetchall()
        
        if projection_results:
            # Convert to DataFrame for better display
            columns = ['Slug', 'Name', 'Year', 'Rarity', 'Positions', 
                      'Upcoming Games', 'Total Projected Score', 'Avg Score/Game', 'Next Game Date']
            df_projections = pd.DataFrame(projection_results, columns=columns)
            
            # Format the dataframe - round the scores to 2 decimal places
            df_projections['Total Projected Score'] = df_projections['Total Projected Score'].round(2)
            df_projections['Avg Score/Game'] = df_projections['Avg Score/Game'].round(2)
            
            report.append(f"Found {len(df_projections)} distinct sealed cards with upcoming projections:")
            report.append(df_projections.to_string(index=False))
        else:
            report.append("No sealed cards with upcoming projections found.")
        
        # Part 2: Injured sealed cards expected back within game week
        report.append("\n" + "=" * 80)
        report.append(f"\n## INJURED SEALED CARDS RETURNING DURING GAME WEEK ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}) ##\n")
        
        query = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions, i.status, 
               i.description, i.return_estimate, i.team
        FROM cards c
        JOIN injuries i ON c.name = i.player_name
        WHERE c.username = %s AND c.sealed = TRUE AND i.return_estimate IS NOT NULL
        """
        
        cursor.execute(query, (username,))
        injury_results = cursor.fetchall()
        
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
                except:
                    # If return_estimate isn't a date, check if it contains keywords
                    # suggesting imminent return during the game week
                    keywords = ['day to day', 'game time decision', 'probable', 
                               'questionable', 'today', 'tomorrow', '1-3 days',
                               'this week', 'expected back', 'returning']
                    if any(keyword in return_estimate.lower() for keyword in keywords):
                        soon_returning.append(result)
            
            if soon_returning:
                columns = ['Slug', 'Name', 'Year', 'Rarity', 'Positions', 'Status', 
                          'Description', 'Return Estimate', 'Team']
                df_injuries = pd.DataFrame(soon_returning, columns=columns)
                
                report.append(f"Found {len(df_injuries)} injured sealed cards expected to return during game week:")
                report.append(df_injuries.to_string(index=False))
            else:
                report.append("No injured sealed cards expected to return during game week.")
        else:
            report.append("No injured sealed cards found.")
        
        report.append("\n" + "=" * 80)
        cursor.close()
        conn.close()
        
    except Exception as e:
        report.append(f"Error: {e}")
        
    return "\n".join(report)

def save_lineups(lineups: Dict[str, Dict], output_file: str, energy_limits: Dict[str, int],
                username: str, boost_2025: float, stack_boost: float, energy_per_card: int,
                cards_df: pd.DataFrame, projections_df: pd.DataFrame) -> None:
    """Save lineup output to a file."""
    with open(output_file, 'w') as f:
        # Write header information
        f.write(f"Lineups for {username} - Game Week {determine_game_week()}\n")
        f.write(f"2025 Card Boost: {boost_2025}\n")
        f.write(f"Stack Boost: {stack_boost}\n")
        f.write(f"Energy Per Card: {energy_per_card}\n\n")
        
        total_energy_used = {"rare": 0, "limited": 0}
        
        # Print lineups in priority order
        for lineup_type in Config.PRIORITY_ORDER:
            data = lineups[lineup_type]
            if data["cards"]:
                f.write(f"{lineup_type}\n")
                f.write(f"Projected Score: {data['projected_score']}\n")
                f.write(f"Energy Used: Rare={data['energy_used']['rare']}, Limited={data['energy_used']['limited']}\n")
                
                f.write("Cards:\n")
                
                # Sort cards by consistent position order

                card_entries = list(zip(data["cards"], data["slot_assignments"], data["projections"]))
                               
                for card, slot, proj in card_entries:
                    f.write(f"  {slot}: {card} - {proj:.2f}\n")
                
                f.write("\n")
                
                total_energy_used["rare"] += data["energy_used"]["rare"]
                total_energy_used["limited"] += data["energy_used"]["limited"]
        
        # Print energy summary
        remaining_rare = energy_limits["rare"] - total_energy_used["rare"]
        remaining_limited = energy_limits["limited"] - total_energy_used["limited"]
        
        f.write("\nEnergy Summary:\n")
        f.write(f"Total Rare Energy Used: {total_energy_used['rare']}/{energy_limits['rare']} (Remaining: {remaining_rare})\n")
        f.write(f"Total Limited Energy Used: {total_energy_used['limited']}/{energy_limits['limited']} (Remaining: {remaining_limited})\n")

        # Find missing projections
        f.write("\nPLAYERS LACKING PROJECTIONS\n")
        merged = cards_df.merge(projections_df, left_on="name", right_on="player_name", how="left")
        missing = merged[merged["total_projection"].isna()]
        
        if not missing.empty:
            f.write("The following players lack projections:\n")
            for _, row in missing.iterrows():
                f.write(f"- {row['name']} (slug: {row['slug']})\n")
        else:
            f.write("All players have projections. Great!\n")


def parse_arguments():
    """Parse command line arguments with sensible defaults."""
    parser = argparse.ArgumentParser(description='Sorare MLB Lineup Optimizer')
    parser.add_argument('--username', type=str, default=Config.USERNAME,
                        help=f'Sorare username (default: {Config.USERNAME})')
    parser.add_argument('--rare-energy', type=int, default=Config.DEFAULT_ENERGY_LIMITS["rare"],
                        help=f'Rare energy limit (default: {Config.DEFAULT_ENERGY_LIMITS["rare"]})')
    parser.add_argument('--limited-energy', type=int, default=Config.DEFAULT_ENERGY_LIMITS["limited"],
                        help=f'Limited energy limit (default: {Config.DEFAULT_ENERGY_LIMITS["limited"]})')
    parser.add_argument('--boost-2025', type=float, default=Config.BOOST_2025,
                        help=f'2025 card boost (default: {Config.BOOST_2025})')
    parser.add_argument('--stack-boost', type=float, default=Config.STACK_BOOST,
                        help=f'Stack boost (default: {Config.STACK_BOOST})')
    parser.add_argument('--energy-per-card', type=int, default=Config.ENERGY_PER_NON_2025_CARD,
                        help=f'Energy cost per non-2025 card (default: {Config.ENERGY_PER_NON_2025_CARD})')
    parser.add_argument('--ignore-players', type=str, default=None,
                        help='Comma-separated list of player NAMES to ignore (case-insensitive)')
    parser.add_argument('--game-week', type=str, default=determine_game_week(),
                    help='Game week in format YYYY-MM-DD_to_YYYY-MM-DD (default: dynamically determined)')
    
    return parser.parse_args()

def main():
    """Main function with command line arguments."""
    try:
        args = parse_arguments()
        
        energy_limits = {"rare": args.rare_energy, "limited": args.limited_energy}
        print(f"Using energy limits: Rare={energy_limits['rare']}, Limited={energy_limits['limited']}")
        print(f"Username: {args.username}")
        print(f"Game Week: {determine_game_week()}")  # Print to confirm
        print(f"2025 Card Boost: {args.boost_2025}")
        print(f"Stack Boost: {args.stack_boost}")
        print(f"Energy Per Non-2025 Card: {args.energy_per_card}")
        print(f"Players to Ignore for Lineups: {args.ignore_players}")
        ignore_list = []
        if args.ignore_players:
            ignore_list = [name.strip() for name in args.ignore_players.split(',') if name.strip()]
     
        cards_df = fetch_cards(args.username)
        projections_df = fetch_projections()
        lineups = build_all_lineups(
            cards_df, projections_df, energy_limits, 
            args.boost_2025, args.stack_boost, args.energy_per_card, ignore_list
        )
        output_file = os.path.join("lineups", f"{args.username}.txt")
        save_lineups(
            lineups, output_file, energy_limits, args.username, 
            args.boost_2025, args.stack_boost, args.energy_per_card,
            cards_df, projections_df  # Pass the dataframes
        )
        print(f"Lineups saved to {output_file}")
    except Exception as e:
        print(f"Error running script: {e}")

if __name__ == "__main__":
    main()