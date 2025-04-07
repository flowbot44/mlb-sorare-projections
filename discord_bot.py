import os
import discord
from discord import app_commands
from discord.ext import commands
import asyncio
from dotenv import load_dotenv
from card_fetcher import SorareMLBClient  # Fetch cards
from chatgpt_lineup_optimizer import fetch_cards, fetch_projections, build_all_lineups, save_lineups, Config, parse_arguments
from injury_updates import fetch_injury_data, update_database
from grok_ballpark_factor import main as update_projections, determine_game_week

# Load environment variables
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

# Bot setup
intents = discord.Intents.default()
intents.message_content = True  # Enable message content intent

# Create a bot instance with a minimal prefix (not used for slash commands but required for setup)
bot = commands.Bot(command_prefix='!', intents=intents)

# Default lineup parameters (can be overridden through command parameters)
DEFAULT_ENERGY_LIMITS = {"rare": 150, "limited": 275}
BOOST_2025 = 5.0
STACK_BOOST = 2.0
ENERGY_PER_CARD = 25
DEFAULT_LINEUP_ORDER = [
    "Rare Champion",
    "Rare All-Star_1", "Rare All-Star_2", "Rare All-Star_3",
    "Rare Challenger_1", "Rare Challenger_2",
    "Limited All-Star_1", "Limited All-Star_2", "Limited All-Star_3",
    "Limited Challenger_1", "Limited Challenger_2",
    "Common Minors"
]

@bot.event
async def on_ready():
    print(f'Bot is ready! Logged in as {bot.user}')
    
    # Sync commands with Discord
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} command(s)")
    except Exception as e:
        print(f"Failed to sync commands: {e}")

@bot.tree.command(name='lineup', description="Generate a lineup for the given Sorare MLB username")
@app_commands.describe(
    username="Sorare MLB username",
    rare_energy="Energy limit for rare cards (default: 150)",
    limited_energy="Energy limit for limited cards (default: 275)",
    boost_2025="Boost value for 2025 cards (default: 5.0)",
    stack_boost="Boost value for stacking players from same team (default: 2.0)",
    energy_per_card="Energy cost per non-2025 card (default: 25)",
    lineup_order="Custom lineup priority order (comma-separated list of lineup types)",
    ignore_players="Comma-separated list of player NAMES to ignore (e.g., Shohei Ohtani, Mike Trout)"
)
async def slash_lineup(
    interaction: discord.Interaction, 
    username: str,
    rare_energy: int = None,
    limited_energy: int = None,
    boost_2025: float = None,
    stack_boost: float = None,
    energy_per_card: int = None,
    lineup_order: str = None,
    ignore_players: str = None
):
    """Fetch cards and generate a lineup for the given username with optional parameters."""
    await interaction.response.defer(thinking=True)  # Let user know we're working on it
    
    # Use provided parameters or defaults
    energy_limits = {
        "rare": rare_energy if rare_energy is not None else DEFAULT_ENERGY_LIMITS["rare"],
        "limited": limited_energy if limited_energy is not None else DEFAULT_ENERGY_LIMITS["limited"]
    }
    
    boost_2025_value = boost_2025 if boost_2025 is not None else BOOST_2025
    stack_boost_value = stack_boost if stack_boost is not None else STACK_BOOST
    energy_per_card_value = energy_per_card if energy_per_card is not None else ENERGY_PER_CARD
    ignore_list = []
    if ignore_players:
        try:
            # Keep using names as provided by the user for now
            ignore_list = [name.strip() for name in ignore_players.split(',') if name.strip()]
            print(f"Ignoring players (case-insensitive): {ignore_list}") # Log ignored players
        except Exception as e:
            await interaction.followup.send(f"Warning: Error parsing ignore list: {str(e)}. Proceeding without ignoring.")
            ignore_list = []
    # Parse custom lineup order if provided
    if lineup_order:
        try:
            custom_lineup_order = [lineup.strip() for lineup in lineup_order.split(',')]
            # Verify each lineup type is valid (optional, can be removed if too restrictive)
            valid_lineup_types = set([lt.split('_')[0] for lt in DEFAULT_LINEUP_ORDER])
            for lineup in custom_lineup_order:
                base_type = lineup.split('_')[0] if '_' in lineup else lineup
                if base_type not in valid_lineup_types:
                    await interaction.followup.send(f"Warning: Lineup type '{base_type}' may not be valid.")
            
            # Update Config with custom lineup order
            Config.PRIORITY_ORDER = custom_lineup_order
        except Exception as e:
            await interaction.followup.send(f"Error parsing lineup order: {str(e)}. Using default order.")
            Config.PRIORITY_ORDER = DEFAULT_LINEUP_ORDER
    else:
        Config.PRIORITY_ORDER = DEFAULT_LINEUP_ORDER
    
    # Log parameters for debugging
    print(f"Parameters: username={username}, rare_energy={energy_limits['rare']}, "
          f"limited_energy={energy_limits['limited']}, boost_2025={boost_2025_value}, "
          f"stack_boost={stack_boost_value}, energy_per_card={energy_per_card_value}")
    print(f"Lineup order: {Config.PRIORITY_ORDER}")
    
    # Step 1: Fetch cards using SorareMLBClient
    client = SorareMLBClient()
    card_data = client.get_user_mlb_cards(username)
    
    if not card_data or not card_data.get("cards"):
        await interaction.followup.send(f"Failed to fetch cards for {username}. Check the username or try again later.")
        return

    # Step 2: Fetch cards and projections from the database
    try:
        cards_df = fetch_cards(username)
        projections_df = fetch_projections()
        
        if cards_df.empty:
            await interaction.followup.send(f"No eligible cards found for {username}.")
            return
        if projections_df.empty:
            await interaction.followup.send(f"No projections available for game week {Config.GAME_WEEK}. Run /update first.")
            return

        # Step 3: Generate lineups
        lineups = build_all_lineups(
            cards_df=cards_df,
            projections_df=projections_df,
            energy_limits=energy_limits,
            boost_2025=boost_2025_value,
            stack_boost=stack_boost_value,
            energy_per_card=energy_per_card_value,
            ignore_list=ignore_list # Pass the list of names
        )

        # Step 4: Save lineups to a file
        output_file = f"lineups/{username}.txt"
        save_lineups(
            lineups=lineups,
            output_file=output_file,
            energy_limits=energy_limits,
            username=username,
            boost_2025=boost_2025_value,
            stack_boost=stack_boost_value,
            energy_per_card=energy_per_card_value,
            cards_df=cards_df,             
            projections_df=projections_df  
        )

        # Create a summary of parameters used
        params_summary = (
            f"**Parameters Used:**\n"
            f"• Rare Energy: {energy_limits['rare']}\n"
            f"• Limited Energy: {energy_limits['limited']}\n"
            f"• 2025 Card Boost: {boost_2025_value}\n"
            f"• Stack Boost: {stack_boost_value}\n"
            f"• Energy Per Card: {energy_per_card_value}\n"
        )

        # Step 5: Send the file or a summary to Discord
        with open(output_file, 'r') as f:
            content = f.read()
            if len(content) > 1500:  # Lower threshold to account for parameters summary
                await interaction.followup.send(f"{params_summary}\nLineup file generated for {username}. Uploading file...")
                await interaction.followup.send(file=discord.File(output_file))
            else:
                await interaction.followup.send(f"{params_summary}\nLineup for {username}:\n```\n{content}\n```")

    except Exception as e:
        await interaction.followup.send(f"Error generating lineup: {str(e)}")

@bot.tree.command(name='update', description="Update injury data and game projections")
async def slash_update(interaction: discord.Interaction):
    """Update injuries and projections."""
    await interaction.response.defer(thinking=True)
    
    try:
        # Step 1: Update injury data
        injury_data = fetch_injury_data()
        if injury_data:
            update_database(injury_data)
            await interaction.followup.send("Injury data updated successfully. Starting Projections...")
        else:
            await interaction.followup.send("Failed to fetch injury data.")

        # Step 2: Update projections
        update_projections()  # Runs the main function from grok_ballpark_factor
        await interaction.followup.send(f"Projections updated for game week {determine_game_week()}.")

    except Exception as e:
        await interaction.followup.send(f"Error during update: {str(e)}")

@bot.tree.command(name='help', description="Show information about available commands and parameters")
async def slash_help(interaction: discord.Interaction):
    """Display help information about the bot's commands."""
    help_text = """
**Sorare MLB Lineup Optimizer Bot**

**Commands:**
• `/lineup` - Generate optimized lineups for a Sorare MLB username
• `/update` - Update injury data and projections
• `/help` - Display this help message

**Parameters for `/lineup`:**
• `username` (required) - Your Sorare MLB username
• `rare_energy` - Energy limit for rare cards (default: 150)
• `limited_energy` - Energy limit for limited cards (default: 275)
• `boost_2025` - Boost value for 2025 cards (default: 5.0)
• `stack_boost` - Boost value for stacking players from same team (default: 2.0)
• `energy_per_card` - Energy cost per non-2025 card (default: 25)
• `lineup_order` - Custom lineup priority order (comma-separated list)

**Default Lineup Order:**
```
"Rare Champion",
Rare All-Star_1, Rare All-Star_2, Rare All-Star_3,
Rare Challenger_1, Rare Challenger_2,
Limited All-Star_1, 
Limited Challenger_1, Limited Challenger_2,
Common Minors
```

To use a custom lineup order, provide a comma-separated list of lineup types.
Example: `Rare All-Star_1,Limited All-Star_1,Rare Challenger_1`
"""
    await interaction.response.send_message(help_text)

# Run the bot
if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("No Discord token found. Make sure to create a .env file with DISCORD_TOKEN=your_token")
    bot.run(TOKEN)