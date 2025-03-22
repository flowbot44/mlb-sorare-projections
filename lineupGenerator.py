import sqlite3
from typing import List, Dict, Set, Tuple
from datetime import datetime

class SorareLineupGenerator:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.position_groups = {
            'corner_infield': {'baseball_first_base', 'baseball_third_base', 'baseball_designated_hitter'},
            'middle_infield': {'baseball_shortstop', 'baseball_second_base', 'baseball_catcher'},
            'outfield': {'baseball_outfield'},
            'starting_pitcher': {'baseball_starting_pitcher'},
            'relief_pitcher': {'baseball_relief_pitcher'}
        }
        self.hitting_positions = {
            'baseball_first_base', 'baseball_third_base', 'baseball_designated_hitter',
            'baseball_shortstop', 'baseball_second_base', 'baseball_catcher',
            'baseball_outfield'
        }
        self.pitching_positions = {'baseball_starting_pitcher', 'baseball_relief_pitcher'}
        self.used_slugs = set()  # This will now persist across all lineup generations


    def get_projections(self, game_week: str) -> Dict[str, float]:
        """Get simple projections (name/value pairs) for a specific game week."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT player_name, projection_value
        FROM projections
        WHERE game_week = ?
        """, (game_week,))
        
        projections = {player_name: value for player_name, value in cursor.fetchall()}
        conn.close()
        return projections

    def get_user_cards(self) -> Dict[str, Dict]:
        """Fetch user's cards with their positions, team, and player name information."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT slug, name, positions, team
        FROM cards
        """)
        
        cards = {}
        for slug, name, positions, team in cursor.fetchall():
            cards[slug] = {
                'name': name,
                'positions': positions.split(','),
                'team': team
            }
        
        conn.close()
        return cards

    def is_eligible_for_position(self, player: Dict, position: str) -> bool:
        """Check if a player is eligible for a given lineup position."""
        player_positions = set(player['positions'])
        
        if position == 'starting_pitcher':
            return bool(player_positions & {'baseball_starting_pitcher'})
        elif position == 'relief_pitcher':
            return bool(player_positions & {'baseball_relief_pitcher'})
        elif position == 'corner_infield':
            return bool(player_positions & self.position_groups['corner_infield'])
        elif position == 'middle_infield':
            return bool(player_positions & self.position_groups['middle_infield'])
        elif position == 'outfield':
            return bool(player_positions & self.position_groups['outfield'])
        elif position == 'any_hitter':
            return bool(player_positions & self.hitting_positions)
        elif position == 'any_player':
            return True
        return False
    
    def generate_lineups(self, game_week: str, max_lineups: int = 10, 
                        enable_stacking: bool = False, stack_size: int = 3,
                        max_projection_diff: float = 5.0) -> List[List[Dict]]:
        """Generate valid lineups based on available cards and projections."""
        projections = self.get_projections(game_week)
        user_cards = self.get_user_cards()
        
        # Combine card data with projections, matching on player name
        players = []
        for slug, card_info in user_cards.items():
            if card_info['name'] in projections:
                players.append({
                    'slug': slug,
                    'name': card_info['name'],
                    'projection_value': projections[card_info['name']],
                    'team': card_info['team'],
                    'positions': card_info['positions']
                })
        
        valid_lineups = []
        self.used_slugs = set()

        def get_stack_candidates(team: str, current_projection: float) -> List[Dict]:
            """Get eligible players from the same team within projection threshold."""
            return [
                player for player in players 
                if (player['team'] == team and 
                    player['slug'] not in self.used_slugs and
                    abs(player['projection_value'] - current_projection) <= max_projection_diff)
            ]

        def try_build_lineup():
            lineup = []
            used_slugs_this_lineup = set()
            
            position_order = [
                'starting_pitcher',
                'relief_pitcher',
                'corner_infield',
                'middle_infield',
                'outfield',
                'any_hitter',
                'any_player'
            ]
            
            # Handle stacking logic
            stack_team = None
            if enable_stacking:
                # Find best available hitter for potential stack
                hitting_eligible = [
                    p for p in players 
                    if (p['slug'] not in self.used_slugs 
                        and any(pos in self.hitting_positions for pos in p['positions']))
                ]
                
                if hitting_eligible:
                    best_hitter = max(hitting_eligible, key=lambda x: x['projection_value'])
                    stack_candidates = get_stack_candidates(
                        best_hitter['team'], 
                        best_hitter['projection_value']
                    )
                    if len(stack_candidates) >= stack_size:
                        stack_team = best_hitter['team']
            
            for position in position_order:
                eligible_players = [
                    p for p in players
                    if (p['slug'] not in used_slugs_this_lineup 
                        and p['slug'] not in self.used_slugs
                        and self.is_eligible_for_position(p, position))
                ]
                
                if not eligible_players:
                    return None
                
                # Prefer stack team players for non-pitching positions
                if stack_team and position not in ['starting_pitcher', 'relief_pitcher']:
                    stack_players = [p for p in eligible_players if p['team'] == stack_team]
                    if stack_players:
                        eligible_players = stack_players
                
                # Choose player with highest projection
                chosen_player = max(eligible_players, key=lambda p: p['projection_value'])
                lineup.append(chosen_player)
                used_slugs_this_lineup.add(chosen_player['slug'])
            
            # Verify stack requirement
            if stack_team:
                stack_count = sum(1 for p in lineup if p['team'] == stack_team)
                if stack_count < stack_size:
                    return None
            
            return lineup if len(lineup) == 7 else None

        # Generate lineups
        while len(valid_lineups) < max_lineups:
            lineup = try_build_lineup()
            if lineup is None:
                break
                
            total_projection = sum(p['projection_value'] for p in lineup)
            valid_lineups.append((lineup, total_projection))
            self.used_slugs.update(p['slug'] for p in lineup)
        
        valid_lineups.sort(key=lambda x: x[1], reverse=True)
        return [lineup for lineup, _ in valid_lineups[:max_lineups]]

    def print_lineup(self, lineup: List[Dict]):
        """Print a formatted lineup with positions and team stacking information."""
        positions = ['SP', 'RP', 'Corner IF', 'Middle IF', 'Outfield', 'Any Hitter', 'Any Player']
        total_projection = sum(p['projection_value'] for p in lineup)
        
        team_counts = {}
        for player in lineup:
            team_counts[player['team']] = team_counts.get(player['team'], 0) + 1
        
        print("\nLineup Projection:", f"{total_projection:.2f}")
        
        stacks = [f"{team}: {count}" for team, count in team_counts.items() if count >= 2]
        if stacks:
            print("Team Stacks:", ", ".join(stacks))
        
        print("-" * 100)
        print(f"{'Position':<12} {'Player':<30} {'Card ID':<20} {'Team':<6} {'Projection':<10}")
        print("-" * 100)
        
        for position, player in zip(positions, lineup):
            print(f"{position:<12} {player['name']:<30} {player['slug']:<20}  {player['projection_value']:.2f}")
        print("-" * 100)

    def generate_all_lineups(self, game_week: str, num_regular: int = 3, num_stacked: int = 3,
                            stack_size: int = 3, max_projection_diff: float = 5.0) -> Tuple[List[List[Dict]], List[List[Dict]]]:
            """
            Generate both regular and stacked lineups while tracking used cards across all lineups.
            
            Returns:
            Tuple of (regular_lineups, stacked_lineups)
            """
            # Clear used slugs at the start of generating ALL lineups
            self.used_slugs = set()
            
            # Generate regular lineups
            regular_lineups = self.generate_lineups(
                game_week=game_week,
                max_lineups=num_regular,
                enable_stacking=False
            )
            
            # Generate stacked lineups (used_slugs is maintained from regular lineups)
            stacked_lineups = self.generate_lineups(
                game_week=game_week,
                max_lineups=num_stacked,
                enable_stacking=True,
                stack_size=stack_size,
                max_projection_diff=max_projection_diff
            )
            
            return regular_lineups, stacked_lineups

if __name__ == "__main__":
    generator = SorareLineupGenerator("mlb_sorare.db")
    current_week = datetime.now().strftime("%Y-%W")
    
    # Generate both regular and stacked lineups while tracking used cards
    regular_lineups, stacked_lineups = generator.generate_all_lineups(
        game_week=current_week,
        num_regular=3,
        num_stacked=3,
        stack_size=3,
        max_projection_diff=5.0
    )
    
    # Print regular lineups
    print("\nRegular Lineups:")
    for i, lineup in enumerate(regular_lineups, 1):
        print(f"\nRegular Lineup {i}:")
        generator.print_lineup(lineup)
    
    # Print stacked lineups
    print("\nStacked Lineups (3+ players from same team):")
    for i, lineup in enumerate(stacked_lineups, 1):
        print(f"\nStacked Lineup {i}:")
        generator.print_lineup(lineup)