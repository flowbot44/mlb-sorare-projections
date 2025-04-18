# utils.py
import unicodedata
from datetime import datetime, timedelta, date
import os

DATABASE_FILE = os.environ.get('DATABASE_PATH', 'mlb_sorare.db')

# Dictionary for specific name translations
NAME_TRANSLATIONS = {
    "C J ABRAMS": "CJ ABRAMS",
    "DANIEL LYNCH": "DANIEL LYNCH IV",
    # Add more translations as needed, e.g.:
    "J T REALMUTO": "JT REALMUTO",
    "J D MARTINEZ": "JD MARTINEZ",
    "C J CRON": "CJ CRON",
    "A J PUK": "AJ PUK",
    

    # "HYUN JIN RYU": "HYUNJIN RYU"
}

def normalize_name(name):
    """Normalize a name by removing accents, converting to uppercase, replacing hyphens with spaces, removing periods, and applying specific translations."""
    if not name or (hasattr(name, 'isna') and name.isna()):  # Handle None or pandas NaN
        return name
    # Remove accents and normalize Unicode
    normalized = ''.join(c for c in unicodedata.normalize('NFKD', str(name)) if unicodedata.category(c) != 'Mn')
    # Apply standard transformations
    normalized = normalized.upper().replace('-', ' ').replace('.', '').strip()
    # Apply specific translations if present
    return NAME_TRANSLATIONS.get(normalized, normalized)

# New function
def determine_game_week(current_date=None):
    """
    Determine the game week string (e.g., '2025-03-31_to_2025-04-03') based on the current date:
    - If run Tuesday through Friday: return the upcoming/current Friday-Sunday period
    - If run Saturday through Monday: return the upcoming/current Monday-Thursday period
    - Special case for season start (March 27-30, 2025)
    Returns a string in the format 'YYYY-MM-DD_to_YYYY-MM-DD'.
    """
    if current_date is None:
        current_date = datetime.now().date()
    elif isinstance(current_date, str):
        current_date = datetime.strptime(current_date, '%Y-%m-%d').date()

    season_start = date(2025, 3, 27)
    if current_date <= date(2025, 3, 29):
        start_date = season_start
        end_date = date(2025, 3, 29)
    else:
        day_of_week = current_date.weekday()
        if 1 <= day_of_week <= 4:  # Tuesday to Friday
            days_until_friday = (4 - day_of_week) % 7
            start_date = current_date + timedelta(days=days_until_friday)
            end_date = start_date + timedelta(days=2)
        else:  # Saturday, Sunday, or Monday
            days_until_monday = (0 - day_of_week) % 7
            start_date = current_date + timedelta(days=days_until_monday)
            end_date = start_date + timedelta(days=3)

    return f"{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}"