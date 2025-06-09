"""
Functions for reporting and saving lineup results.
"""
import pandas as pd
from typing import Dict
import os
import logging

from config import Config
from utils import determine_game_week
from data_fetcher import merge_projections # Assuming data_fetcher.py is in the same directory

logger = logging.getLogger(__name__)

def save_lineups_to_file(
    lineups: Dict[str, Dict], 
    output_file: str, 
    energy_limits: Dict[str, int],
    username: str, 
    boost_2025: float, 
    stack_boost: float, 
    energy_per_card: int,
    cards_df: pd.DataFrame, 
    projections_df: pd.DataFrame
) -> None:
    """Save the generated lineups to a text file."""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        f.write(f"Lineups for {username} - Game Week {determine_game_week()}\n")
        f.write(f"2025 Card Boost: {boost_2025}, Stack Boost: {stack_boost}, Energy Per Card: {energy_per_card}\n\n")

        total_energy_used = {"rare": 0, "limited": 0}

        for lineup_type in Config.PRIORITY_ORDER:
            data = lineups.get(lineup_type)
            if not data or not data.get("cards"):
                continue

            f.write(f"--- {lineup_type} ---\n")
            f.write(f"Projected Score: {data['projected_score']:.2f}\n")
            f.write(f"Energy Used: Rare={data['energy_used']['rare']}, Limited={data['energy_used']['limited']}\n")
            
            f.write("Cards:\n")
            for card, slot, proj in zip(data["cards"], data["slot_assignments"], data["projections"]):
                f.write(f"  {slot:<4} : {card:<30} - {proj:.2f}\n")
            f.write("\n")

            total_energy_used["rare"] += data.get("energy_used", {}).get("rare", 0)
            total_energy_used["limited"] += data.get("energy_used", {}).get("limited", 0)
        
        # Energy Summary
        f.write("--- ENERGY SUMMARY ---\n")
        remaining_rare = energy_limits["rare"] - total_energy_used["rare"]
        remaining_limited = energy_limits["limited"] - total_energy_used["limited"]
        f.write(f"Total Rare Energy Used: {total_energy_used['rare']}/{energy_limits['rare']} (Remaining: {remaining_rare})\n")
        f.write(f"Total Limited Energy Used: {total_energy_used['limited']}/{energy_limits['limited']} (Remaining: {remaining_limited})\n\n")

        # Missing Projections Report
        f.write("--- MISSING PROJECTIONS ---\n")
        merged_df = merge_projections(cards_df, projections_df)
        missing_df = merged_df[merged_df["total_projection"].isna() | (merged_df["total_projection"] == 0)]
        
        if not missing_df.empty:
            f.write("The following owned cards have zero or missing projections:\n")
            for _, row in missing_df.iterrows():
                f.write(f"- {row['name']} (slug: {row['slug']})\n")
        else:
            f.write("All cards have projections.\n")
            
    logger.info(f"Lineups successfully saved to {output_file}")
