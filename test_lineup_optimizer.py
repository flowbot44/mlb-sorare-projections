import unittest
import pandas as pd
from unittest.mock import patch

# Assuming lineup_optimizer.py and config.py are in the same directory,
# or configured in your PYTHONPATH.
# For testing, we'll often mock 'config' to ensure tests are self-contained.
# If you actually import Config directly, make sure your actual config.py is available.

# --- Mock Config for Testing ---
# This simulates your config.py content
class MockConfig:
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

# --- Import the function to be tested ---
# You'll need to adjust this import based on your project structure.
# For this example, we'll assume it's in a file named lineup_optimizer.py
from lineup_optimizer import can_fill_position

class TestCanFillPosition(unittest.TestCase):

    @patch('lineup_optimizer.Config', new=MockConfig)
    def test_will_smith_mi_eligibility(self):
        """
        Test if Will Smith's positions allow him to fill MI, given the provided config.
        Positions: baseball_designated_hitter, baseball_catcher
        Expected for MI: True (due to baseball_catcher)
        """
        card_positions = "baseball_designated_hitter, baseball_catcher"
        slot = "MI"
        self.assertTrue(can_fill_position(card_positions, slot),
                        f"Will Smith with positions '{card_positions}' should be eligible for {slot}")

    @patch('lineup_optimizer.Config', new=MockConfig)
    def test_will_smith_ci_eligibility(self):
        """
        Test if Will Smith's positions allow him to fill CI, given the provided config.
        Positions: baseball_designated_hitter, baseball_catcher
        Expected for CI: True (due to baseball_designated_hitter)
        """
        card_positions = "baseball_designated_hitter, baseball_catcher"
        slot = "CI"
        self.assertTrue(can_fill_position(card_positions, slot),
                        f"Will Smith with positions '{card_positions}' should be eligible for {slot}")
        

    @patch('lineup_optimizer.Config', new=MockConfig)
    def test_basic_eligibility(self):
        """Test a simple positive case."""
        card_positions = "baseball_first_base"
        slot = "1B"
        self.assertTrue(can_fill_position(card_positions, slot))

    @patch('lineup_optimizer.Config', new=MockConfig)
    def test_multiple_positions_single_slot(self):
        """Test a card with multiple positions matching a single slot."""
        card_positions = "baseball_shortstop,baseball_second_base"
        slot = "MI" # MI allows SS and 2B
        self.assertTrue(can_fill_position(card_positions, slot))

    @patch('lineup_optimizer.Config', new=MockConfig)
    def test_no_eligibility(self):
        """Test a card not eligible for a given slot."""
        card_positions = "baseball_starting_pitcher"
        slot = "MI"
        self.assertFalse(can_fill_position(card_positions, slot))

    @patch('lineup_optimizer.Config', new=MockConfig)
    def test_unlisted_slot(self):
        """Test a slot that doesn't exist in Config.POSITIONS."""
        card_positions = "baseball_first_base"
        slot = "INVALID_SLOT"
        self.assertFalse(can_fill_position(card_positions, slot))

    @patch('lineup_optimizer.Config', new=MockConfig)
    def test_empty_card_positions(self):
        """Test with an empty string for card positions."""
        card_positions = ""
        slot = "MI"
        self.assertFalse(can_fill_position(card_positions, slot))

    @patch('lineup_optimizer.Config', new=MockConfig)
    def test_nan_card_positions(self):
        """Test with NaN for card positions (as pandas might yield)."""
        card_positions = pd.NA # Or float('nan')
        slot = "MI"
        self.assertFalse(can_fill_position("" if pd.isna(card_positions) else str(card_positions), slot))

    @patch('lineup_optimizer.Config', new=MockConfig)
    def test_hitter_eligibility(self):
        """Test if a player with various hitter positions is eligible for 'H'."""
        card_positions_1 = "baseball_first_base"
        self.assertTrue(can_fill_position(card_positions_1, "H"))
        card_positions_2 = "baseball_catcher"
        self.assertTrue(can_fill_position(card_positions_2, "H"))
        card_positions_3 = "baseball_designated_hitter,baseball_left_fielder"
        self.assertTrue(can_fill_position(card_positions_3, "H"))

    @patch('lineup_optimizer.Config', new=MockConfig)
    def test_flex_eligibility(self):
        """Test if a player with various positions is eligible for 'Flx' and 'Flx+'."""
        card_positions_hitter = "baseball_third_base"
        self.assertTrue(can_fill_position(card_positions_hitter, "Flx"))
        self.assertTrue(can_fill_position(card_positions_hitter, "Flx+"))

        card_positions_pitcher_sp = "baseball_starting_pitcher"
        self.assertFalse(can_fill_position(card_positions_pitcher_sp, "Flx")) # Flx doesn't allow pitchers
        self.assertTrue(can_fill_position(card_positions_pitcher_sp, "Flx+")) # Flx+ does allow pitchers

        card_positions_pitcher_rp = "baseball_relief_pitcher"
        self.assertTrue(can_fill_position(card_positions_pitcher_rp, "Flx"))
        self.assertTrue(can_fill_position(card_positions_pitcher_rp, "Flx+"))

if __name__ == '__main__':
    unittest.main()