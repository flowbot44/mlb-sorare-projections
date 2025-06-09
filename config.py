"""
Configuration constants for the Sorare MLB Lineup Optimizer.
"""

class Config:
    # Player/Card Specific
    SHOHEI_NAME = "shohei-ohtani"

    # Optimizer Parameters
    BOOST_2025 = 5.0
    STACK_BOOST = 2.0
    ENERGY_PER_NON_2025_CARD = 25
    MAX_TEAM_STACK = 6
    DEFAULT_ENERGY_LIMITS = {"rare": 50, "limited": 50}

    # Lineup Definitions
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

    # Position Definitions
    POSITIONS = {
        "CI": {"baseball_first_base", "baseball_third_base", "baseball_designated_hitter"},
        "MI": {"baseball_shortstop", "baseball_second_base", "baseball_catcher"},
        "OF": {"baseball_outfield"},
        "SP": {"baseball_starting_pitcher"},
        "RP": {"baseball_relief_pitcher"},
        "1B": {"baseball_first_base"},
        "2B": {"baseball_second_base"},
        "3B": {"baseball_third_base"},
        "SS": {"baseball_shortstop"},
        "C": {"baseball_catcher"},
        "DH": {"baseball_designated_hitter"}
    }
    POSITIONS["H"] = POSITIONS["CI"] | POSITIONS["MI"] | POSITIONS["OF"]
    POSITIONS["Flx"] = POSITIONS["CI"] | POSITIONS["MI"] | POSITIONS["OF"] | POSITIONS["RP"]
    POSITIONS["Flx+"] = POSITIONS["CI"] | POSITIONS["MI"] | POSITIONS["OF"] | POSITIONS["RP"] | POSITIONS["SP"]
    
    LINEUP_SLOTS = ["SP", "RP", "CI", "MI", "OF", "H", "Flx"]
    DAILY_LINEUP_SLOTS = ["SP", "RP", "CI", "MI", "OF", "H", "Flx+"]

    # Reverse mapping for dynamic slot creation
    _REVERSE_POSITIONS = {frozenset(v): k for k, v in POSITIONS.items()}
