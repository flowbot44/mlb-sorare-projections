"""
Data fetching functions for the Sorare Lineup Optimizer.
"""
import pandas as pd
from typing import List, Optional, Dict
from datetime import datetime, timedelta
import logging

from utils import get_sqlalchemy_engine, determine_game_week, determine_daily_game_week, get_db_connection
from config import Config

logger = logging.getLogger(__name__)

def fetch_cards(username: str) -> pd.DataFrame:
    """Fetch all of a user's cards from the database."""
    engine = get_sqlalchemy_engine()
    query = """
        SELECT c.slug, c.name, c.year, c.rarity, c.positions, c.username, c.sealed, pt.team_id
        FROM cards c
        LEFT JOIN player_teams pt ON c.name = pt.player_name
        WHERE c.username = %s AND c.sealed = FALSE
    """
    try:
        cards_df = pd.read_sql(query, engine, params=(username,))
        logger.info(f"Fetched {len(cards_df)} unsealed cards for user {username}.")
        cards_df["year"] = cards_df["year"].astype(int)
        
        # Handle Shohei Ohtani's special case
        cards_df.loc[cards_df["slug"].str.contains(Config.SHOHEI_NAME), "positions"] = "baseball_designated_hitter"
        
        # Ensure team_id is an integer
        if 'team_id' in cards_df.columns:
            # Convert to numeric first, then fill NaN, then convert to int
            cards_df['team_id'] = pd.to_numeric(cards_df['team_id'], errors='coerce').fillna(-1).astype(int)
        
        return cards_df
    except Exception as e:
        logger.error(f"Failed to fetch cards for {username}: {e}")
        return pd.DataFrame()

def fetch_projections(ignore_game_ids: Optional[List] = None) -> pd.DataFrame:
    """Fetch Sorare projections for the current game week."""
    engine = get_sqlalchemy_engine()
    current_game_week = determine_game_week()
    
    query = "SELECT player_name, team_id, SUM(sorare_score) AS total_projection FROM adjusted_projections WHERE game_week = %s"
    params = [current_game_week]
    
    if ignore_game_ids:
        placeholders = ','.join(['%s'] * len(ignore_game_ids))
        query += f" AND game_id NOT IN ({placeholders})"
        params.extend(ignore_game_ids)
    
    query += " GROUP BY player_name, team_id"
    
    try:
        projections_df = pd.read_sql(query, engine, params=tuple(params))
        projections_df["total_projection"] = projections_df["total_projection"].fillna(0)
        logger.info(f"Fetched {len(projections_df)} projections for game week {current_game_week}.")
        return projections_df
    except Exception as e:
        logger.error(f"Failed to fetch projections: {e}")
        return pd.DataFrame()


def fetch_daily_projections(ignore_game_ids: Optional[List] = None) -> pd.DataFrame:
    """Fetch Sorare projections for games scheduled today (Eastern time)."""
    engine = get_sqlalchemy_engine()
    today_iso = datetime.now().date().isoformat()

    query = """
        SELECT player_name, team_id, SUM(sorare_score) AS total_projection
        FROM adjusted_projections ap
        JOIN games g ON ap.game_id = g.id
        WHERE g.local_date = %s
    """
    params = [today_iso]

    if ignore_game_ids:
        placeholders = ','.join(['%s'] * len(ignore_game_ids))
        query += f" AND ap.game_id NOT IN ({placeholders})"
        params.extend(ignore_game_ids)

    query += " GROUP BY player_name, team_id"

    try:
        df = pd.read_sql(query, engine, params=tuple(params))
        df["total_projection"] = df["total_projection"].fillna(0)
        logger.info(f"Fetched {len(df)} daily projections for {today_iso}.")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch daily projections: {e}")
        return pd.DataFrame()


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
