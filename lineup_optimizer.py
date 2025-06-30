"""
Core lineup optimization logic using OR-Tools with positional boost support.
"""
import pandas as pd
from ortools.sat.python import cp_model
from typing import Dict, List, Set, Optional
import logging
from collections import defaultdict

from config import Config

logger = logging.getLogger(__name__)

slot_priority = {
    "1B": 1,
    "2B": 1,
    "3B": 1,
    "SS": 1,
    "C": 1,
    "DH": 1,
    "SP": 2,
    "RP": 2,
    "CI": 2,
    "MI": 2,
    "OF": 2,
    "H": 3,
    "Flx": 4,
    "Flx+": 4
}

# --- Helper Functions ---

def can_fill_position(card_positions: str, slot: str) -> bool:
    """Check if a card's positions can fill a given slot."""
    if pd.isna(card_positions):
        return False
    # FIX: Strip whitespace from each position after splitting
    card_position_set = {pos.strip() for pos in card_positions.split(",")}
    return bool(card_position_set & Config.POSITIONS.get(slot, set()))

def is_hitter(card_positions: str) -> bool:
    """Check if a card is a hitter."""
    if pd.isna(card_positions):
        return False
    return bool(set(card_positions.split(",")) & Config.POSITIONS["H"])

def card_eligible_for_position_group(card_positions: str, position_group: str) -> bool:
    """Check if a card is eligible for a specific position group (e.g., 'OF', 'CI', 'MI')."""
    if pd.isna(card_positions):
        return False
    card_position_set = {pos.strip() for pos in card_positions.split(",")}
    return bool(card_position_set & Config.POSITIONS.get(position_group, set()))


def get_rarity_from_lineup_type(lineup_type: str) -> str:
    """Extract rarity from the lineup type string."""
    if "Common" in lineup_type: return "common"
    if "Limited" in lineup_type: return "limited"
    if "Rare" in lineup_type: return "rare"
    return ""

def uses_energy_lineup(lineup_type: str) -> bool:
    """Determine if a lineup type consumes energy."""
    return any(keyword in lineup_type for keyword in ["All-Star", "Champion", "Derby", "Legend"])

# --- Main Optimizer ---

def build_lineup_optimized(
    cards_df: pd.DataFrame, 
    lineup_type: str, 
    used_cards: Set[str],
    remaining_energy: Dict[str, int], 
    boost_2025: float, 
    stack_boost: float,
    energy_per_card: int, 
    lineup_slots: list[str] = Config.LINEUP_SLOTS,
    max_team_stack: int = Config.MAX_TEAM_STACK,
    positional_boosts: Optional[Dict[str, float]] = None
) -> Dict:
    available_cards = cards_df[~cards_df["slug"].isin(used_cards)].copy()
    available_cards["base_projection"] = available_cards["total_projection"]
    is_2025 = available_cards["year"] == 2025
    is_boostable_lineup = "Challenger" not in lineup_type and "Minors" not in lineup_type
    available_cards.loc[is_2025 & is_boostable_lineup, "base_projection"] += boost_2025

    rarity = get_rarity_from_lineup_type(lineup_type)
    if "Champion" in lineup_type or "Legend" in lineup_type or "Challenger" in lineup_type or "Derby" in lineup_type or "Swing" in lineup_type or "Minors" in lineup_type:
        available_cards = available_cards[available_cards["rarity"] == rarity]
    elif "All-Star" in lineup_type:
        limits = Config.ALL_STAR_LIMITS.get(f"{rarity.capitalize()} All-Star", {})
        allowed = limits.get("allowed_rarities", {rarity})
        available_cards = available_cards[available_cards["rarity"].isin(allowed)]

    lineup_slots = sorted(lineup_slots, key=lambda s: slot_priority.get(s, 99))

    if len(available_cards) < len(lineup_slots):
        return {}

    # Initialize final_projection with base_projection
    available_cards["final_projection"] = available_cards["base_projection"]

    solution = _run_or_tools_solver(
        available_cards.to_dict("records"), lineup_type, lineup_slots,
        remaining_energy, energy_per_card, max_team_stack, stack_boost
    )
    
    return solution if solution.get("cards") else {}

def _run_or_tools_solver(
    cards: List[Dict], 
    lineup_type: str,
    lineup_slots: List[str],
    remaining_energy: Dict[str, int], 
    energy_per_card: int, 
    max_team_stack: int,
    stack_boost: float
) -> Dict:
    model = cp_model.CpModel()
    num_cards = len(cards)
    num_slots = len(lineup_slots)

    card_selected = [model.NewBoolVar(f"card_{i}") for i in range(num_cards)]
    slot_used = [[model.NewBoolVar(f"slot_{i}_{j}") for j in range(num_slots)] for i in range(num_cards)]

    model.Add(sum(card_selected) == num_slots)

    for i in range(num_cards):
        model.Add(sum(slot_used[i]) == card_selected[i])

    for j in range(num_slots):
        model.Add(sum(slot_used[i][j] for i in range(num_cards)) == 1)
        
    for i in range(num_cards):
        for j in range(num_slots):
            if not can_fill_position(cards[i]["positions"], lineup_slots[j]):
                model.Add(slot_used[i][j] == 0)

    player_map = {}
    for i, card in enumerate(cards):
        player_key = (card["name"], card["team_id"])
        player_map.setdefault(player_key, []).append(card_selected[i])
    for vars in player_map.values():
        if len(vars) > 1:
            model.Add(sum(vars) <= 1)

    team_map = {}
    for i, card in enumerate(cards):
        team_id = card.get("team_id", -1)
        if team_id != -1:
            team_map.setdefault(team_id, []).append(card_selected[i])
    for team_vars in team_map.values():
        model.Add(sum(team_vars) <= max_team_stack)

    rarity = get_rarity_from_lineup_type(lineup_type)
    if "All-Star" in lineup_type:
        limits = Config.ALL_STAR_LIMITS.get(f"{rarity.capitalize()} All-Star", {})
        for rar_type, max_count in limits.items():
            if "max_" in rar_type:
                limit_rarity = rar_type.split("_")[1]
                indices = [i for i, c in enumerate(cards) if c["rarity"] == limit_rarity]
                if indices:
                    model.Add(sum(card_selected[i] for i in indices) <= max_count)

    if uses_energy_lineup(lineup_type):
        for rar_type in ["rare", "limited"]:
            energy_cost_vars = [
                card_selected[i] for i, c in enumerate(cards)
                if c["rarity"] == rar_type and c["year"] != 2025
            ]
            if energy_cost_vars:
                model.Add(sum(energy_cost_vars) * energy_per_card <= remaining_energy.get(rar_type, 0))

    objective_terms = []
    for i in range(num_cards):
        projection = int(cards[i].get("final_projection", 0) * 100)
        objective_terms.append(card_selected[i] * projection)

    hitter_indices = [i for i, c in enumerate(cards) if is_hitter(c["positions"])]
    team_hitter_indices = defaultdict(list)
    for i in hitter_indices:
        team_id = cards[i]["team_id"]
        team_hitter_indices[team_id].append(i)

    team_stack_bonus_terms = []
    for team_id, indices in team_hitter_indices.items():
        selected_vars = [card_selected[i] for i in indices]
        team_stack_size = model.NewIntVar(0, len(indices), f"team_{team_id}_stack_size")
        model.Add(team_stack_size == sum(selected_vars))
        stack_bonus = model.NewIntVar(0, 10000, f"stack_bonus_team_{team_id}")
        bonus_table = [(i, int((i * (i - 1) / 2) * stack_boost * 100)) for i in range(len(indices)+1)]
        model.AddAllowedAssignments([team_stack_size, stack_bonus], bonus_table)
        team_stack_bonus_terms.append(stack_bonus)

    model.Maximize(sum(objective_terms) + sum(team_stack_bonus_terms))

    solver = cp_model.CpSolver()
    solver.parameters.num_search_workers = 8
    status = solver.Solve(model)

    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        lineup_cards, slots, projections = [], [], []
        energy_used = {"rare": 0, "limited": 0}

        for i in range(num_cards):
            if solver.BooleanValue(card_selected[i]):
                card_info = cards[i]
                lineup_cards.append(card_info["slug"])
                projections.append(card_info["total_projection"])
                for j in range(num_slots):
                    if solver.BooleanValue(slot_used[i][j]):
                        slots.append(lineup_slots[j])
                        break
                if uses_energy_lineup(lineup_type) and card_info["year"] != 2025:
                    if card_info["rarity"] in energy_used:
                        energy_used[card_info["rarity"]] += energy_per_card

        slot_order = {slot: idx for idx, slot in enumerate(lineup_slots)}
        sorted_lineup = sorted(zip(lineup_cards, slots, projections), key=lambda x: slot_order.get(x[1], 999))
        if sorted_lineup:
            lineup_cards, slots, projections = zip(*sorted_lineup)

        return {
            "cards": list(lineup_cards),
            "slot_assignments": list(slots),
            "projections": list(projections),
            "projected_score": round(sum(projections), 2),
            "energy_used": energy_used,
        }
    else:
        log_message = (
            f"Solver returned {solver.StatusName(status)} for lineup type '{lineup_type}'. "
            f"Lineup slots requested: {lineup_slots}. "
            f"Number of available cards: {num_cards}. "
            "This indicates that no lineup can satisfy all constraints. \n"
        )
        reasons = []

        eligibility_details = {}
        is_energy_lineup = uses_energy_lineup(lineup_type)

        for slot in lineup_slots:
            total_eligible = 0
            energy_exempt_eligible = 0
            
            for card in cards:
                if can_fill_position(card["positions"], slot):
                    total_eligible += 1
                    # A card is energy-exempt if it's a 2025 card
                    # and this is an energy-restricted lineup type.
                    if is_energy_lineup and card.get("year", 0) == 2025:
                        energy_exempt_eligible += 1
            
            eligibility_details[slot] = {
                "total_eligible": total_eligible,
                "energy_exempt_eligible": energy_exempt_eligible
            }
            if energy_exempt_eligible == 0:
                reasons.append(f"Slot '{slot}': No energy-exempt eligible cards available.\n")

        if num_cards < num_slots:
            reasons.append(f"Not enough total cards ({num_cards}) to fill all slots ({num_slots}).\n")
        
        if "All-Star" in lineup_type:
            rarity = get_rarity_from_lineup_type(lineup_type)
            limits = Config.ALL_STAR_LIMITS.get(f"{rarity.capitalize()} All-Star", {})
            for rar_type, max_count in limits.items():
                if "max_" in rar_type:
                    limit_rarity = rar_type.split("_")[1]
                    count_rarity_cards = sum(1 for c in cards if c["rarity"] == limit_rarity)
                    # Only suggest as a reason if it's potentially violated
                    if count_rarity_cards > max_count and num_slots > max_count: # Heuristic check
                        reasons.append(f"Rarity limit for {limit_rarity} cards (max {max_count}) might be too restrictive. Available of this rarity: {count_rarity_cards}.\n")

        if uses_energy_lineup(lineup_type):
            for rar_type in ["rare", "limited"]:
                available_energy = remaining_energy.get(rar_type, 0)
                if available_energy < energy_per_card * num_slots: 
                    reasons.append(f"Energy limit for {rar_type} cards (remaining: {available_energy}) might be too low, as each selected card costs {energy_per_card}.\n")


        if status == cp_model.INFEASIBLE:
            log_message += "This is often due to conflicting constraints."
        elif status == cp_model.UNKNOWN:
            log_message += "This typically means the solver timed out or could not determine feasibility within its limits. Consider increasing `max_time_in_seconds`."
        
        log_message += " Specific potential issues: " + "; ".join(reasons)
        logging.warning(log_message)

    return {}