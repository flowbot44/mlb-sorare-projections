import sqlite3
import pandas as pd
from ortools.sat.python import cp_model
import os
from typing import Dict, List, Set, Optional

# Configuration Constants
class Config:
    DB_PATH = "mlb_sorare.db"
    USERNAME = "flowbot44"
    GAME_WEEK = "2025-03-27_to_2025-03-30"
    SHOHEI_NAME = "shohei-ohtani"
    BOOST_2025 = 5.0
    STACK_BOOST = 2.0
    ENERGY_PER_NON_2025_CARD = 25
    DEFAULT_ENERGY_LIMITS = {"rare": 325, "limited": 275}
    PRIORITY_ORDER = [
        "Rare Champion",
        "Rare All-Star_1", "Rare All-Star_2", "Rare All-Star_3",
        "Rare Challenger_1", "Rare Challenger_2",
        "Limited All-Star_1", 
        "Limited Challenger_1", "Limited Challenger_2",
        "Common Minors"
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
    LINEUP_SLOTS = ["SP", "RP", "CI", "MI", "OF", "H", "Flx"]

# Utility Functions
def get_db_connection() -> sqlite3.Connection:
    """Create a connection to the SQLite database."""
    try:
        return sqlite3.connect(Config.DB_PATH)
    except sqlite3.Error as e:
        raise RuntimeError(f"Failed to connect to database: {e}")

def fetch_cards() -> pd.DataFrame:
    """Fetch eligible cards with team info from the database."""
    with get_db_connection() as conn:
        query = """
            SELECT c.slug, c.name, c.year, c.rarity, c.positions, c.username, c.sealed, pt.team_id
            FROM cards c
            LEFT JOIN PlayerTeams pt ON c.name = pt.player_name
            WHERE c.username = ? AND c.sealed = 0
        """
        cards_df = pd.read_sql(query, conn, params=(Config.USERNAME,))
        cards_df["year"] = cards_df["year"].astype(int)
        cards_df.loc[cards_df["name"] == Config.SHOHEI_NAME, "positions"] = "baseball_designated_hitter"
    return cards_df

def fetch_projections() -> pd.DataFrame:
    """Fetch Sorare projections for the current game week."""
    with get_db_connection() as conn:
        query = """
            SELECT player_name, SUM(sorare_score) AS total_projection 
            FROM AdjustedProjections WHERE game_week = ?
            GROUP BY player_name
        """
        projections_df = pd.read_sql(query, conn, params=(Config.GAME_WEEK,))
        projections_df["total_projection"] = projections_df["total_projection"].fillna(0).infer_objects(copy=False)
    return projections_df

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

def apply_boosts(cards_df: pd.DataFrame, lineup_type: str) -> pd.DataFrame:
    """Apply 2025 boost to card projections."""
    cards_df["selection_projection"] = cards_df["total_projection"]
    if "Challenger" not in lineup_type and "Minors" not in lineup_type:
        cards_df.loc[cards_df["year"] == 2025, "selection_projection"] += Config.BOOST_2025
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
    return cards_df

def build_lineup(cards_df: pd.DataFrame, lineup_type: str, used_cards: Set[str], remaining_energy: Dict[str, int]) -> Dict:
    """Build a single lineup respecting remaining global energy limits."""
    available_cards = cards_df[~cards_df["slug"].isin(used_cards)].copy()
    available_cards = apply_boosts(available_cards, lineup_type)
    available_cards = filter_cards_by_lineup_type(available_cards, lineup_type)
    
    lineup = []
    slot_assignments = []
    projections = []
    used_players = set()
    rarity_count = {"common": 0, "limited": 0, "rare": 0}
    team_counts = {}
    energy_used = {"rare": 0, "limited": 0}
    
    rarity = get_rarity_from_lineup_type(lineup_type)
    uses_energy = "All-Star" in lineup_type or "Champion" in lineup_type
    
    for slot in Config.LINEUP_SLOTS:
        candidates = available_cards[
            available_cards["name"].apply(lambda x: x not in used_players) &
            available_cards["positions"].apply(lambda p: can_fill_position(p, slot))
        ].copy()
        
        if candidates.empty:
            break
        
        # Apply stacking boost for hitters
        candidates["effective_projection"] = candidates["selection_projection"]
        if slot in ["CI", "MI", "OF", "H", "Flx"]:
            candidates.loc[candidates["positions"].apply(is_hitter), "effective_projection"] += (
                candidates["team_id"].map(lambda x: team_counts.get(x, 0)) * Config.STACK_BOOST
            )
        
        candidates = candidates.sort_values("effective_projection", ascending=False)
        
        selected_card = None
        limits = Config.ALL_STAR_LIMITS.get(f"{rarity.capitalize()} All-Star", {}) if "All-Star" in lineup_type else {}
        for _, card in candidates.iterrows():
            card_rarity = card["rarity"]
            energy_cost = Config.ENERGY_PER_NON_2025_CARD if (uses_energy and card["year"] != 2025 and card_rarity in remaining_energy) else 0
            if card_rarity in remaining_energy and remaining_energy[card_rarity] - energy_cost < 0:
                continue
            if "max_common" in limits and rarity_count["common"] >= limits["max_common"] and card_rarity == "common":
                continue
            if "max_limited" in limits and rarity_count["limited"] >= limits["max_limited"] and card_rarity == "limited":
                continue
            selected_card = card
            break
        
        if selected_card is None:
            break
        
        lineup.append(selected_card["slug"])
        slot_assignments.append(slot)
        projections.append(selected_card["total_projection"])
        used_players.add(selected_card["name"])
        rarity_count[selected_card["rarity"]] += 1
        if uses_energy and selected_card["year"] != 2025 and selected_card["rarity"] in remaining_energy:
            energy_used[selected_card["rarity"]] += Config.ENERGY_PER_NON_2025_CARD
        if is_hitter(selected_card["positions"]):
            team_counts[selected_card["team_id"]] = team_counts.get(selected_card["team_id"], 0) + 1
        available_cards = available_cards[available_cards["slug"] != selected_card["slug"]]
    
    if len(lineup) == 7:
        return {
            "cards": lineup,
            "slot_assignments": slot_assignments,
            "projections": projections,
            "projected_score": round(sum(projections), 2),
            "energy_used": energy_used
        }
    return {"cards": [], "slot_assignments": [], "projections": [], "projected_score": 0, "energy_used": {"rare": 0, "limited": 0}}

def build_all_lineups(cards_df: pd.DataFrame, projections_df: pd.DataFrame, energy_limits: Dict[str, int]) -> Dict[str, Dict]:
    """Build optimal lineups respecting global energy constraints."""
    check_missing_projections(cards_df, projections_df)
    cards_df = cards_df.merge(projections_df, left_on="name", right_on="player_name", how="left").fillna({"total_projection": 0})
    
    used_cards = set()
    remaining_energy = energy_limits.copy()  # Track remaining energy globally
    lineups = {key: {"cards": [], "slot_assignments": [], "projections": [], "projected_score": 0, "energy_used": {"rare": 0, "limited": 0}} 
               for key in Config.PRIORITY_ORDER}
    
    for lineup_type in Config.PRIORITY_ORDER:
        lineup_data = build_lineup(cards_df, lineup_type, used_cards, remaining_energy)
        if lineup_data["cards"]:
            lineups[lineup_type] = lineup_data
            used_cards.update(lineup_data["cards"])
            # Update remaining energy
            for rarity in remaining_energy:
                remaining_energy[rarity] -= lineup_data["energy_used"][rarity]
    
    return lineups

def save_lineups(lineups: Dict[str, Dict], output_file: str, energy_limits: Dict[str, int]) -> None:
    """Save lineups to a file with energy usage and print remaining energy."""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        f.write(f"Lineups for Game Week {Config.GAME_WEEK}\n")
        f.write("=" * 50 + "\n\n")
        total_energy_used = {"rare": 0, "limited": 0}
        for lineup_type, data in lineups.items():
            if data["cards"]:
                f.write(f"{lineup_type.replace('_', ' #')}\n")
                f.write(f"Projected Score: {data['projected_score']}\n")
                f.write(f"Energy Used: Rare={data['energy_used']['rare']}, Limited={data['energy_used']['limited']}\n")
                f.write("Cards:\n")
                for card, slot, proj in zip(data["cards"], data["slot_assignments"], data["projections"]):
                    f.write(f"  - {slot}: {card} ({proj:.2f})\n")
                f.write("\n")
                total_energy_used["rare"] += data["energy_used"]["rare"]
                total_energy_used["limited"] += data["energy_used"]["limited"]
        
        remaining_rare = energy_limits["rare"] - total_energy_used["rare"]
        remaining_limited = energy_limits["limited"] - total_energy_used["limited"]
        f.write("=" * 50 + "\n")
        f.write(f"Energy Summary:\n")
        f.write(f"Total Rare Energy Used: {total_energy_used['rare']}/{energy_limits['rare']} (Remaining: {remaining_rare})\n")
        f.write(f"Total Limited Energy Used: {total_energy_used['limited']}/{energy_limits['limited']} (Remaining: {remaining_limited})\n")

def main(rare_energy: int = Config.DEFAULT_ENERGY_LIMITS["rare"], limited_energy: int = Config.DEFAULT_ENERGY_LIMITS["limited"]):
    """Main function with configurable energy limits."""
    try:
        energy_limits = {"rare": rare_energy, "limited": limited_energy}
        print(f"Using energy limits: Rare={energy_limits['rare']}, Limited={energy_limits['limited']}")
        
        cards_df = fetch_cards()
        projections_df = fetch_projections()
        lineups = build_all_lineups(cards_df, projections_df, energy_limits)
        output_file = os.path.join("lineups", f"{Config.USERNAME}.txt")
        save_lineups(lineups, output_file, energy_limits)
        print(f"Lineups saved to {output_file}")
    except Exception as e:
        print(f"Error running script: {e}")

if __name__ == "__main__":
    main(rare_energy=325, limited_energy=275)