import sqlite3
import pandas as pd
from ortools.sat.python import cp_model
import os
import argparse
from typing import Dict, List, Set, Optional
from utils import determine_game_week  # Import from utils

# Configuration Constants
class Config:
    DB_PATH = "mlb_sorare.db"
    USERNAME = "flowbot44"
    GAME_WEEK = determine_game_week()
    SHOHEI_NAME = "shohei-ohtani"
    BOOST_2025 = 5.0
    STACK_BOOST = 2.0
    ENERGY_PER_NON_2025_CARD = 25
    DEFAULT_ENERGY_LIMITS = {"rare": 150, "limited": 275}
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

def fetch_cards(username: str) -> pd.DataFrame:
    """Fetch eligible cards with team info from the database."""
    with get_db_connection() as conn:
        query = """
            SELECT c.slug, c.name, c.year, c.rarity, c.positions, c.username, c.sealed, pt.team_id
            FROM cards c
            LEFT JOIN PlayerTeams pt ON c.name = pt.player_name
            WHERE c.username = ? AND c.sealed = 0
        """
        cards_df = pd.read_sql(query, conn, params=(username,))
        cards_df["year"] = cards_df["year"].astype(int)
        cards_df.loc[cards_df["name"] == Config.SHOHEI_NAME, "positions"] = "baseball_designated_hitter"
    
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

def fetch_projections() -> pd.DataFrame:
    """Fetch Sorare projections for the current game week with team separation."""
    with get_db_connection() as conn:
        query = """
            SELECT player_name, team_id, SUM(sorare_score) AS total_projection 
            FROM AdjustedProjections WHERE game_week = ?
            GROUP BY player_name, team_id
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
    return cards_df

def uses_energy_lineup(lineup_type: str) -> bool:
    """Determine if a lineup type uses energy."""
    return "All-Star" in lineup_type or "Champion" in lineup_type

def build_lineup(cards_df: pd.DataFrame, lineup_type: str, used_cards: Set[str], 
                remaining_energy: Dict[str, int], boost_2025: float, stack_boost: float,
                energy_per_card: int) -> Dict:
    """Build a single lineup respecting remaining global energy limits."""
    available_cards = cards_df[~cards_df["slug"].isin(used_cards)].copy()
    available_cards = apply_boosts(available_cards, lineup_type, boost_2025)
    available_cards = filter_cards_by_lineup_type(available_cards, lineup_type)
    
    lineup = []
    slot_assignments = []
    projections = []
    used_players = set()  # This will now store (name, team_id) tuples
    rarity_count = {"common": 0, "limited": 0, "rare": 0}
    team_counts = {}
    energy_used = {"rare": 0, "limited": 0}
    
    rarity = get_rarity_from_lineup_type(lineup_type)
    uses_energy = uses_energy_lineup(lineup_type)
    
    # Skip energy-using lineups if not enough energy is available
    if uses_energy:
        # Calculate minimum energy needed
        min_energy_needed = energy_per_card  # Assume at least one non-2025 card
        if (rarity in remaining_energy and 
            remaining_energy[rarity] < min_energy_needed):
            print(f"Skipping {lineup_type} due to insufficient {rarity} energy")
            return {"cards": [], "slot_assignments": [], "projections": [], "projected_score": 0, "energy_used": {"rare": 0, "limited": 0}}
    
    for slot in Config.LINEUP_SLOTS:
        # Update this line to check name-team combination
        candidates = available_cards[
            available_cards.apply(lambda x: (x["name"], x["team_id"]) not in used_players, axis=1) &
            available_cards["positions"].apply(lambda p: can_fill_position(p, slot))
        ].copy()
        
        if candidates.empty:
            break
        
        # Apply stacking boost for hitters
        candidates["effective_projection"] = candidates["selection_projection"]
        if slot in ["CI", "MI", "OF", "H", "Flx"]:
            candidates.loc[candidates["positions"].apply(is_hitter), "effective_projection"] += (
                candidates["team_id"].map(lambda x: team_counts.get(x, 0)) * stack_boost
            )
        
        candidates = candidates.sort_values("effective_projection", ascending=False)
        
        selected_card = None
        limits = Config.ALL_STAR_LIMITS.get(f"{rarity.capitalize()} All-Star", {}) if "All-Star" in lineup_type else {}
        for _, card in candidates.iterrows():
            card_rarity = card["rarity"]
            energy_cost = energy_per_card if (uses_energy and card["year"] != 2025 and card_rarity in remaining_energy) else 0
            
            # Skip card if it would exceed energy limits
            if energy_cost > 0 and (card_rarity not in remaining_energy or remaining_energy[card_rarity] < energy_cost):
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
        used_players.add((selected_card["name"], selected_card["team_id"]))
        rarity_count[selected_card["rarity"]] += 1
        
        # Update energy used tracking
        if uses_energy and selected_card["year"] != 2025 and selected_card["rarity"] in remaining_energy:
            energy_cost = energy_per_card
            energy_used[selected_card["rarity"]] += energy_cost
            remaining_energy[selected_card["rarity"]] -= energy_cost  # Deduct energy immediately
            
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

def build_all_lineups(cards_df: pd.DataFrame, projections_df: pd.DataFrame, energy_limits: Dict[str, int],
                     boost_2025: float, stack_boost: float, energy_per_card: int,
                     ignore_list: List[str] = None) -> Dict[str, Dict]:
    """Build optimal lineups respecting global energy constraints."""
    check_missing_projections(cards_df, projections_df)

    if ignore_list:
        initial_count = len(cards_df)
        # Convert the user-provided names to uppercase for comparison
        ignore_list_upper = {name.upper() for name in ignore_list}
        print(f"Uppercase ignore list: {ignore_list_upper}")

        # Ensure the 'name' column in the DataFrame is uppercase (as stated in the user request)
        # If it might not be, uncomment the line below:
        # cards_df['name_upper'] = cards_df['name'].str.upper()

        # Filter based on the 'name' column being in the uppercase ignore list.
        # Assumes cards_df['name'] is already uppercase. If not, use cards_df['name_upper']
        cards_df = cards_df[~cards_df['name'].isin(ignore_list_upper)]
        filtered_count = len(cards_df)
        print(f"Ignored {initial_count - filtered_count} cards based on case-insensitive name list: {ignore_list}")
    
    cards_df = cards_df.merge(
        projections_df, 
        left_on=["name", "team_id"], 
        right_on=["player_name", "team_id"], 
        how="left"
    ).fillna({"total_projection": 0}).infer_objects(copy=False)
    
    used_cards = set()
    remaining_energy = energy_limits.copy()
    lineups = {key: {"cards": [], "slot_assignments": [], "projections": [], "projected_score": 0, "energy_used": {"rare": 0, "limited": 0}}
               for key in Config.PRIORITY_ORDER}
    
    # First, process all energy-using lineups
    energy_lineups = [lt for lt in Config.PRIORITY_ORDER if uses_energy_lineup(lt)]
    non_energy_lineups = [lt for lt in Config.PRIORITY_ORDER if not uses_energy_lineup(lt)]
    
    # Process energy lineups in priority order
    for lineup_type in energy_lineups:
        lineup_data = build_lineup(
            cards_df, lineup_type, used_cards, remaining_energy, 
            boost_2025, stack_boost, energy_per_card
        )
        if lineup_data["cards"]:
            lineups[lineup_type] = lineup_data
            used_cards.update(lineup_data["cards"])
    
    # Then process non-energy lineups in priority order
    for lineup_type in non_energy_lineups:
        lineup_data = build_lineup(
            cards_df, lineup_type, used_cards, remaining_energy,
            boost_2025, stack_boost, energy_per_card
        )
        if lineup_data["cards"]:
            lineups[lineup_type] = lineup_data
            used_cards.update(lineup_data["cards"])
    
    return lineups

def save_lineups(lineups: Dict[str, Dict], output_file: str, energy_limits: Dict[str, int],
                username: str, boost_2025: float, stack_boost: float, energy_per_card: int) -> None:
    """Save lineups to a file with energy usage and print remaining energy."""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        f.write(f"Lineups for Game Week {Config.GAME_WEEK}\n")
        f.write(f"Username: {username}\n")
        f.write(f"2025 Card Boost: {boost_2025}\n")
        f.write(f"Stack Boost: {stack_boost}\n")
        f.write(f"Energy Per Non-2025 Card: {energy_per_card}\n")
        f.write("=" * 50 + "\n\n")
        total_energy_used = {"rare": 0, "limited": 0}
        
        # Print lineups in priority order
        for lineup_type in Config.PRIORITY_ORDER:
            data = lineups[lineup_type]
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
        print(f"Game Week: {Config.GAME_WEEK}")  # Print to confirm
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
            args.boost_2025, args.stack_boost, args.energy_per_card
        )
        print(f"Lineups saved to {output_file}")
    except Exception as e:
        print(f"Error running script: {e}")

if __name__ == "__main__":
    main()