import sqlite3
import pandas as pd
from ortools.sat.python import cp_model
import os

# Constants
DB_PATH = "mlb_sorare.db"
USERNAME = "flowbot44"
GAME_WEEK = "2025-03-27_to_2025-03-30"
SHOHEI_NAME = "shohei-ohtani"
BOOST_2025 = 5.0  # Boost for 2025 cards in Champion and All-Star
STACK_BOOST = 3.0  # Boost per teammate for stacking hitters

# Priority Order with multiples
PRIORITY_ORDER = [
    "Rare Champion",
    "Rare All-Star_1", "Rare All-Star_2", "Rare All-Star_3",
    "Rare Challenger_1", "Rare Challenger_2",
    "Limited All-Star_1", "Limited All-Star_2", "Limited All-Star_3",
    "Limited Challenger_1", "Limited Challenger_2",
    "Common Minors"
]

# Max lower-quality cards per All-Star lineup
ALL_STAR_LIMITS = {
    "Rare All-Star": {"max_limited": 3, "allowed_rarities": {"rare", "limited"}},
    "Limited All-Star": {"max_common": 3, "allowed_rarities": {"limited", "common"}}
}

# Position groups
POSITIONS = {
    "CI": {"baseball_first_base", "baseball_third_base", "baseball_designated_hitter"},
    "MI": {"baseball_shortstop", "baseball_second_base", "baseball_catcher"},
    "OF": {"baseball_outfield"},
    "SP": {"baseball_starting_pitcher"},
    "RP": {"baseball_relief_pitcher"}
}
POSITIONS["H"] = POSITIONS["CI"] | POSITIONS["MI"] | POSITIONS["OF"]
POSITIONS["Flx"] = POSITIONS["CI"] | POSITIONS["MI"] | POSITIONS["OF"] | POSITIONS["RP"]

# Required lineup slots (7 cards)
LINEUP_SLOTS = ["CI", "MI", "OF", "SP", "RP", "H", "Flx"]

def fetch_cards():
    """Fetch eligible cards from the database with team information."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions, c.username, c.sealed, pt.team_id
        FROM cards c
        LEFT JOIN PlayerTeams pt ON c.name = pt.player_name
        WHERE c.username = ? AND c.sealed = 0
    """
    df = pd.read_sql(query, conn, params=(USERNAME,))
    df["year"] = df["year"].astype(int)  # Ensure year is an integer
    df.loc[df["name"] == SHOHEI_NAME, "positions"] = "baseball_designated_hitter"
    conn.close()
    return df


def fetch_projections():
    """Fetch Sorare projections for the current game week."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT player_name, SUM(sorare_score) AS total_projection 
        FROM AdjustedProjections WHERE game_week = ?
        GROUP BY player_name
    """
    df = pd.read_sql(query, conn, params=(GAME_WEEK,))
    df["total_projection"] = df["total_projection"].fillna(0).infer_objects(copy=False)
    conn.close()
    return df

def can_fill_position(card_positions, slot):
    """Check if card can fill the given position slot."""
    if pd.isna(card_positions):
        return False
    card_pos_set = set(card_positions.split(","))
    return bool(card_pos_set & POSITIONS[slot])

def is_hitter(card_positions):
    """Check if card is a hitter (eligible for CI, MI, OF, or H)."""
    if pd.isna(card_positions):
        return False
    card_pos_set = set(card_positions.split(","))
    return bool(card_pos_set & POSITIONS["H"])

def build_lineups(cards_df, projections_df):
    """Build optimal lineups based on priority order with stacking."""
    used_cards = set()
    lineups = {key: {"cards": [], "slot_assignments": [], "projections": [], "projected_score": 0, "non_2025": 0} for key in PRIORITY_ORDER}
    
    # Check for missing projections
    all_cards_with_projections = cards_df.merge(projections_df, left_on="name", right_on="player_name", how="left")
    missing_projections = all_cards_with_projections[all_cards_with_projections["total_projection"].isna()]
    if not missing_projections.empty:
        print("Error: The following players were not found in projections:")
        for _, row in missing_projections.iterrows():
            print(f"  - {row['name']} (slug: {row['slug']})")
    
    for lineup_type in PRIORITY_ORDER:
        available_cards = cards_df[~cards_df["slug"].isin(used_cards)].copy()
        available_cards = available_cards.merge(projections_df, left_on="name", right_on="player_name", how="left")
        available_cards["total_projection"] = available_cards["total_projection"].fillna(0)
        available_cards["selection_projection"] = available_cards["total_projection"]  # Copy for selection
        
        # Apply 2025 boost for Champion and All-Star before selection
        if "Challenger" not in lineup_type and "Minors" not in lineup_type:
            available_cards.loc[available_cards["year"] == 2025, "selection_projection"] += BOOST_2025
            
        # Extract rarity and type from lineup name
        rarity = "common" if "Common" in lineup_type else "limited" if "Limited" in lineup_type else "rare"
        is_champion = "Champion" in lineup_type
        is_challenger = "Challenger" in lineup_type
        is_minors = "Minors" in lineup_type
        
        # Filter for specific rarity requirements
        if is_champion or is_challenger:
            available_cards = available_cards[available_cards["rarity"] == rarity]
        elif is_minors:
            available_cards = available_cards[available_cards["rarity"] == "common"]
        elif "All-Star" in lineup_type:
            limit = ALL_STAR_LIMITS.get(f"{rarity.capitalize()} All-Star", {})
            available_cards = available_cards[available_cards["rarity"].isin(limit["allowed_rarities"])]
            
        lineup = []
        slot_assignments = []
        projections = []  # Store individual projections
        used_players = set()
        rarity_count = {"common": 0, "limited": 0, "rare": 0}
        team_counts = {}  # Track number of hitters per team_id
        
        for slot in LINEUP_SLOTS:
            # Filter cards that can fill the slot and aren't used, explicitly copy
            candidates = available_cards[
                available_cards["name"].apply(lambda x: x not in used_players) &
                available_cards["positions"].apply(lambda p: can_fill_position(p, slot))
            ].copy()  # Explicit copy to avoid SettingWithCopyWarning
            
            if candidates.empty:
                break
                
            # Apply stacking boost for hitter slots using .loc
            candidates.loc[:, "effective_projection"] = candidates["selection_projection"]
            if slot in ["CI", "MI", "OF", "H", "Flx"]:
                candidates.loc[:, "effective_projection"] = candidates.apply(
                    lambda row: row["selection_projection"] + 
                                (team_counts.get(row["team_id"], 0) * STACK_BOOST 
                                 if is_hitter(row["positions"]) else 0),
                    axis=1
                )
            
            # Sort by effective projection
            candidates = candidates.sort_values("effective_projection", ascending=False)
            
            # Select first valid card considering rarity limits
            selected_card = None
            for _, card in candidates.iterrows():
                if not (is_champion or is_challenger or is_minors):
                    limit = ALL_STAR_LIMITS.get(f"{rarity.capitalize()} All-Star", {})
                    if "max_common" in limit and rarity_count["common"] >= limit["max_common"] and card["rarity"] == "common":
                        continue
                    if "max_limited" in limit and rarity_count["limited"] >= limit["max_limited"] and card["rarity"] == "limited":
                        continue
                selected_card = card
                break
            
            if selected_card is None:
                break
                
            lineup.append(selected_card["slug"])
            slot_assignments.append(slot)
            projections.append(selected_card["total_projection"])  # Store individual projection
            used_players.add(selected_card["name"])
            rarity_count[selected_card["rarity"]] += 1
            if is_hitter(selected_card["positions"]):
                team_counts[selected_card["team_id"]] = team_counts.get(selected_card["team_id"], 0) + 1
            available_cards = available_cards[available_cards["slug"] != selected_card["slug"]]  # Fixed typo here
            
        if len(lineup) == 7:  # Only accept complete lineups
            lineup_cards = cards_df[cards_df["slug"].isin(lineup)].copy()
            projected_score = sum(projections)
            non_2025 = sum(1 for year in lineup_cards["year"] if year != 2025)
            lineups[lineup_type] = {
                "cards": lineup,
                "slot_assignments": slot_assignments,
                "projections": projections,
                "projected_score": round(projected_score, 2),
                "non_2025": non_2025
            }
            used_cards.update(lineup)
    
    return lineups

def main():
    cards_df = fetch_cards()
    projections_df = fetch_projections()
    lineups = build_lineups(cards_df, projections_df)

    # Ensure the 'lineups' folder exists
    output_dir = "lineups"
    os.makedirs(output_dir, exist_ok=True)  # Creates the folder if it doesn't exist
    output_file = os.path.join(output_dir, f"{USERNAME}.txt")
    # Save output
    with open(output_file, "w") as f:
        f.write(f"Lineups for Game Week {GAME_WEEK}\n")
        f.write("=" * 50 + "\n\n")
        
        for lineup_type, data in lineups.items():
            if data["cards"]:
                f.write(f"{lineup_type.replace('_', ' #')}\n")
                f.write(f"Projected Score: {data['projected_score']}\n")
                f.write(f"Non-2025 Cards: {data['non_2025']}\n")
                f.write("Cards:\n")
                for card, slot, proj in zip(data["cards"], data["slot_assignments"], data["projections"]):
                    f.write(f"  - {slot}: {card} ({proj:.2f})\n")
                f.write("\n")

    print(f"Lineups saved to {output_file}")

if __name__ == "__main__":
    main()