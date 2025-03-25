# utils.py
import unicodedata

# Dictionary for specific name translations
NAME_TRANSLATIONS = {
    "C J ABRAMS": "CJ ABRAMS",
    # Add more translations as needed, e.g.:
    # "J T REALMUTO": "JT REALMUTO",
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