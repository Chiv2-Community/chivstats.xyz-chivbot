import os
import asyncio
import psycopg2
import discord
from discord.ext import commands
from discord.ext.commands import check, CheckFailure
from datetime import datetime, timedelta
import time
import asyncpg
import traceback
import pytz


# Database connection credentials
DATABASE = "chivstats"
USER = "webchiv"
HOST = "/var/run/postgresql"

# URL for the duels leaderboard and list of Discord guild IDs where the bot is active.
# Not including guild ids causes a delay in command update replication.
DUELS_LEADERBOARD_URL = "https://chivstats.xyz/leaderboards/ranked_combat/" 
GUILD_IDS = [1111684756896239677, 878005964685582406, 1163168644524687394]

# Fetch the Discord bot token from environment variables
TOKEN = os.getenv('CHIVBOT_KEY')

# Initialize the bot with command prefix and defined intents
intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)


# Indicate bot startup in console
print("Bot is starting up...")

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name}')

# Global variables and constants
duel_queue = [] # Queue for managing duels
leaderboard_classes = ["GlobalXp", "experienceknight"] # List of leaderboards (todo)

# Async function to establish a database connection
async def create_db_connection():
    return await asyncpg.connect(database=DATABASE, user=USER, host=HOST)

# Async function to close a database connection
async def close_db_connection(conn):
    await conn.close()


# Decorator to restrict command usage to specific channels
def is_channel_named(allowed_channel_names):
    async def predicate(interaction: discord.Interaction):
        if interaction.channel.name not in allowed_channel_names:
            raise commands.CheckFailure(
                "This command can only be used in specified channels."
            )
        return True
    return commands.check(predicate)

# Error handling for restricted commands
@bot.event
async def on_application_command_error(interaction: discord.Interaction, error):
    if isinstance(error, CheckFailure):
        embed = discord.Embed(
            title="Command Restricted",
            description=(
                "This command can only be used in a `#chivstats-ranked` channel.\n"
                "Navigate to the `#chivstats-ranked` channel in this server to use the bot.\n"
                "For more info, visit [chivstats.xyz](https://chivstats.xyz)."
            ),
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
    else:
        # Log unexpected errors for debugging
        print(f"An unexpected error occurred: {error}")

@bot.slash_command(guild_ids=GUILD_IDS, description="Provides help information about commands.")
async def help(interaction: discord.Interaction):
    commands_info = {
        "/register": "Links your Discord account to a PlayFab ID. Usage: `/register [PlayFabID]`",
        "/submit_duel": "Submit the result of a duel between two players. Usage: `/submit_duel @User1 [score1] @User2 [score2]`",
        "/rank": "Displays the rank and stats of a player. Usage: `/rank [@User]`",
        "/stats": "Displays stats for a PlayFab ID. Usage: `/stats [PlayFabID]`",
        "/status": "Checks registered status. Usage: `/status [PlayFabID]`",
        "/retire": "Retire your account from ranked matches. Usage: `/retire`",
        "/reactivate": "Reactivate your account for ranked matches. Usage: `/reactivate`",
        "/gamename": "Manually set an in-game name. Usage: `/gamename [name]`",
        "/elo": "Explains how the ELO system works. Usage: `/elo (public)`",
        "/queue": "Enter or exit the matchmaking queue for a duel. Usage: `/queue [action]`",
        "/duo_teams": "List all duo teams, their players, and ELO ranks in descending order. Usage: `/duo_teams`",
        "/duo_setup_team": "Create and update the name of your duo team. Usage: `/duo_setup_team @TeamMember [team_name]`",
        "/submit_duo": "Submit the result of a 2v2 duel between two teams. Usage: `/submit_duo @TeamMember [team_score] @Enemy1 @Enemy2 [enemy_score]`"
    }
    
    embed = discord.Embed(
        title="Chivbot Help",
        description="Here are the commands you can use with Chivbot:",
        color=discord.Color.blue()
    )

    for cmd, desc in commands_info.items():
        embed.add_field(name=cmd, value=desc, inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)

# Example of an updated async function with asyncpg
async def get_display_name_from_ranked_players(playfabid):
    conn = await create_db_connection()
    try:
        row = await conn.fetchrow("SELECT gamename, common_name FROM ranked_players WHERE playfabid = $1", playfabid)
        return row['gamename'] if row and row['gamename'] else (row['common_name'] if row else "Unknown Player")
    finally:
        await close_db_connection(conn)

# Helper function to format PlayFab ID with URL
def format_playfab_id_with_url(playfabid):
    return f"[{playfabid}](https://chivstats.xyz/leaderboards/player/{playfabid}/)"

async def format_playfab_id_with_url(playfabid):
    conn = await create_db_connection()
    try:
        most_common_alias = await get_most_common_alias(conn, playfabid)
        alias_display = f"{playfabid} ('{most_common_alias}')"
        return f"[{alias_display}](https://chivstats.xyz/leaderboards/player/{playfabid}/)"
    finally:
        await close_db_connection(conn)

# And the updated get_most_common_alias function
async def get_most_common_alias(conn, playfabid):
    try:
        result = await conn.fetchrow("SELECT alias_history FROM players WHERE playfabid = $1", playfabid)
        if result and result['alias_history']:
            alias_history = result['alias_history']
            most_common_alias = max(alias_history, key=alias_history.get, default="Unknown Alias")
            return most_common_alias
        else:
            return "Unknown Alias"
    except Exception as e:
        print(f"Error in get_most_common_alias: {e}")
        return "Error"

async def get_playfabid_of_discord_id(conn, discord_id):
    result = await conn.fetchrow("SELECT playfabid FROM ranked_players WHERE discordid = $1", discord_id)
    return result[0] if result else None

async def get_common_name_from_ranked_players(conn, playfabid):
    result = await conn.fetchrow("SELECT common_name FROM ranked_players WHERE playfabid = $1", playfabid)

    if result and result['common_name']:  # Return the common name if found
        return result['common_name']
    else:  # If no common name found in ranked_players, look in the players table
        return await get_most_common_alias(conn, playfabid)

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name}')

@bot.slash_command(guild_ids=GUILD_IDS, description="Delete a specific message by its ID.")
async def send_delete(interaction: discord.Interaction, message_id: str):
    # Convert the message_id string to an integer
    try:
        message_id = int(message_id)
    except ValueError:
        await interaction.response.send_message("Invalid message ID format.", ephemeral=True)
        return

    # Check if the user requesting the deletion is authorized (i.e., the bot owner or a specific user)
    authorized_user_id = 230773943240228864  # gimmic
    if interaction.user.id != authorized_user_id:
        await interaction.response.send_message("You are not authorized to use this command.", ephemeral=True)
        return
    try:
        message = await interaction.channel.fetch_message(message_id)
        await message.delete()
        await interaction.response.send_message("Message deleted successfully.", ephemeral=True)
    except discord.NotFound:
        await interaction.response.send_message("Message not found.", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message("I don't have permissions to delete the message.", ephemeral=True)
    except discord.HTTPException as e:
        await interaction.response.send_message(f"Failed to delete message: {e}", ephemeral=True)

##################
#ELO Duel related code
async def log_duel(conn, submitting_playfabid, winner_playfabid, winner_score, winner_elo, loser_playfabid, loser_score, loser_elo):
    try:
        await conn.execute("""
            INSERT INTO duels (submitting_playfabid, winner_playfabid, winner_score, winner_elo, loser_playfabid, loser_score, loser_elo)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            submitting_playfabid, winner_playfabid, winner_score, winner_elo, loser_playfabid, loser_score, loser_elo)
    except Exception as e:
        print(f"Error in log_duel: {e}")


def calculate_elo(R, K, games_won, games_played, opponent_rating, c=400):
    """
    Calculate the new ELO rating based on games played.
    :param R: Current ELO rating
    :param K: Weight of the game
    :param games_won: Total games won
    :param games_played: Total games played (should be 1 for a duel)
    :param opponent_rating: ELO rating of the opponent
    :param c: Constant determining the influence of the rating difference
    :return: New ELO rating as a float
    """
    expected_score = 1 / (1 + 10 ** ((opponent_rating - R) / c))
    actual_score = games_won / games_played
    new_rating = R + K * (actual_score - expected_score)
    return new_rating  # Return as float for precise calculation


@bot.slash_command(guild_ids=GUILD_IDS, description="Submit the result of a duel between two players.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def submit_duel(interaction: discord.Interaction, initiator: discord.Member, initiator_score: int, opponent: discord.Member, opponent_score: int):
    try:
        if initiator.id == opponent.id:
            await interaction.response.send_message("You cannot duel yourself!", ephemeral=True)
            return

        conn = await asyncpg.connect(database=DATABASE, user=USER, host=HOST)

        records = await conn.fetch("SELECT discordid, retired FROM ranked_players WHERE discordid = ANY($1::bigint[])", [initiator.id, opponent.id])
        retired_players = {record['discordid']: record['retired'] for record in records if record['retired']}

        if retired_players:
            message = ""
            for discord_id, retired in retired_players.items():
                if retired:
                    mention = f"<@{discord_id}>"
                    message += f"{mention} has retired. Please reactivate your account using /reactivate.\n" if discord_id == interaction.user.id else f"Please ask {mention} to reactivate.\n"
            await interaction.response.send_message(message, ephemeral=True)
            return

        initiator_playfabid = await get_playfabid_of_discord_id(conn, initiator.id)
        opponent_playfabid = await get_playfabid_of_discord_id(conn, opponent.id)
        initiator_name = await get_display_name_from_ranked_players(initiator_playfabid)
        opponent_name = await get_display_name_from_ranked_players(opponent_playfabid)

        winner, loser = (initiator, opponent) if initiator_score > opponent_score else (opponent, initiator)
        winner_score, loser_score = max(initiator_score, opponent_score), min(initiator_score, opponent_score)
        winner_label = " (winner)" if winner != interaction.user else " (winner, submitter)"
        submitter_label = " (submitter)" if initiator == interaction.user and winner != initiator else ""
        command_text = f"/submit_duel @{initiator.display_name} {initiator_score} @{opponent.display_name} {opponent_score}"

        embed = discord.Embed(title="Duel Result (UNVERIFIED)", description=f"Command: `{command_text}`", color=discord.Color.orange())
        embed.add_field(name="Matchup", value=f"{winner.display_name}{winner_label} vs {loser.display_name}{submitter_label}", inline=False)
        embed.add_field(name="Score", value=f"{winner_score}-{loser_score}", inline=True)
        await interaction.response.defer()

        user_to_verify = opponent if interaction.user == initiator else initiator
        duel_message = await interaction.followup.send(embed=embed)
        cst_timezone = pytz.timezone('America/Chicago')
        current_time_cst = datetime.now(pytz.utc).astimezone(cst_timezone)
        expiration_time_cst = current_time_cst + timedelta(minutes=60)
        expiration_unix_timestamp = int(expiration_time_cst.timestamp())

        verification_request = f"This result will automatically expire <t:{expiration_unix_timestamp}:R>.\n"
        verification_request += f"{user_to_verify.mention} please react to this message confirming or denying the match results."
        embed.description += f"\n\n{verification_request}"
        await duel_message.edit(embed=embed)
        verification_message = await interaction.followup.send(f"{user_to_verify.mention} please react to the above message confirming or denying the match results.")
        await duel_message.add_reaction('✅')
        await duel_message.add_reaction('❌')

        submitter_id = interaction.user.id

        def check(reaction, user):
            if reaction.message.id != duel_message.id:
                return False
            non_submitter_id = initiator.id if submitter_id == opponent.id else opponent.id
            if user.id == non_submitter_id:
                return str(reaction.emoji) in ['✅', '❌']  # Other player can confirm or deny
            elif user.id == submitter_id:
                return str(reaction.emoji) == '❌'  # Submitter can only deny
            else:
                return False  # Ignore other users

        try:
            reaction, user = await bot.wait_for('reaction_add', timeout=3600.0, check=check)
            if str(reaction.emoji) == '✅':
                winner_data = await conn.fetchrow("SELECT playfabid, elo_duelsx FROM ranked_players WHERE discordid = $1", winner.id)
                loser_data = await conn.fetchrow("SELECT playfabid, elo_duelsx FROM ranked_players WHERE discordid = $1", loser.id)

                if winner_data and loser_data:
                    winner_playfabid, winner_rating = winner_data
                    loser_playfabid, loser_rating = loser_data
                    new_winner_elo_exact = calculate_elo(winner_rating, 32, 1, 1, loser_rating)
                    new_loser_elo_exact = calculate_elo(loser_rating, 32, 0, 1, winner_rating)
                    winner_elo_change = round(new_winner_elo_exact - winner_rating)
                    loser_elo_change = round(new_loser_elo_exact - loser_rating)
                    winner_elo_change_formatted = f"+{winner_elo_change}" if winner_elo_change >= 0 else f"{winner_elo_change}"
                    loser_elo_change_formatted = f"+{loser_elo_change}" if loser_elo_change >= 0 else f"{loser_elo_change}"
                    submitting_playfabid = winner_playfabid if interaction.user.id == winner.id else loser_playfabid

                    await log_duel(conn, submitting_playfabid, winner_playfabid, winner_score, new_winner_elo_exact, loser_playfabid, loser_score, new_loser_elo_exact)

                    # Update Kills and Deaths
                    await conn.execute("UPDATE ranked_players SET kills = kills + $1, deaths = deaths + $2 WHERE discordid = $3", winner_score, loser_score, winner.id)
                    await conn.execute("UPDATE ranked_players SET kills = kills + $1, deaths = deaths + $2 WHERE discordid = $3", loser_score, winner_score, loser.id)

                    # Update ELO score for winner, then loser, then increase their overall match count.
                    await conn.execute("UPDATE ranked_players SET elo_duelsx = $1 WHERE discordid = $2", new_winner_elo_exact, winner.id)
                    await conn.execute("UPDATE ranked_players SET elo_duelsx = $1 WHERE discordid = $2", new_loser_elo_exact, loser.id)
                    await conn.execute("UPDATE ranked_players SET matches = matches + 1 WHERE discordid = ANY($1::bigint[])", [winner.id, loser.id])
                    # static coin reward?
                    coin_reward = 3
                    await conn.execute("UPDATE ranked_players SET coins = coins + $1 WHERE playfabid = ANY($2::text[])", coin_reward, [winner_playfabid, loser_playfabid])

                    house_account_result = await conn.fetchrow("SELECT balance, payout_rate FROM house_account ORDER BY id DESC LIMIT 1")
                    house_balance, payout_rate_percentage = house_account_result if house_account_result else (0, 0)
                    payout_rate_percentage /= 100

                    payout_amount = 0
                    new_house_balance = house_balance
                    if house_balance > 0 and payout_rate_percentage > 0:
                        payout_amount = round(house_balance * payout_rate_percentage)
                        if house_balance >= payout_amount * 2:
                            new_house_balance = house_balance - (payout_amount * 2)
                            await conn.execute("UPDATE ranked_players SET coins = coins + $1 WHERE discordid = ANY($2::bigint[])", payout_amount, [winner.id, loser.id])
                            await conn.execute("UPDATE house_account SET balance = $1", new_house_balance)

                    total_reward = coin_reward + payout_amount
                    embed = discord.Embed(title="Duel Result Confirmed", description=f"Command: `{command_text}`\n\nView the updated rankings on the [Duels Leaderboard]({DUELS_LEADERBOARD_URL}).", color=discord.Color.green())
                    embed.set_footer(text=f"Match result confirmed by {user.display_name}.")
                    embed.add_field(name="Winner's New ELO", value=f"{winner.display_name}: {round(new_winner_elo_exact)} ({winner_elo_change_formatted})", inline=True)
                    embed.add_field(name="Loser's New ELO", value=f"{loser.display_name}: {round(new_loser_elo_exact)} ({loser_elo_change_formatted})", inline=True)
                    embed.add_field(name=f"Coin Reward [+{total_reward} :coin:]", value=f"Both fighters paid {coin_reward} coin, with a +{payout_amount} house bonus.", inline=False)
                    await duel_message.edit(embed=embed)
                    await verification_message.delete()
                else:
                    await interaction.followup.send("One or both players are not registered in the ranking system.", ephemeral=True)

            elif str(reaction.emoji) == '❌':
                cancel_message = f"[ @{initiator.display_name} vs @{opponent.display_name} ] Duel denied by {user_to_verify.mention}." if user.id == user_to_verify.id else f"[ @{initiator.display_name} vs @{opponent.display_name} ] Duel cancelled by {initiator.mention}."
                await duel_message.edit(content=cancel_message, embed=None)
                await verification_message.delete()

        except asyncio.TimeoutError:
            timeout_message = f"[ @{initiator.display_name} vs @{opponent.display_name} ] Duel confirmation timed out."
            await duel_message.edit(content=timeout_message, embed=None)

        try:
            await duel_message.clear_reactions()
        except discord.errors.Forbidden:
            pass

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        traceback.print_exc()
        await interaction.response.send_message("An error occurred while processing the duel.", ephemeral=True)
    finally:
        if conn:
            await conn.close()

@bot.slash_command(guild_ids=GUILD_IDS, description="Send an embedded update notice to all servers.")
async def notice(interaction: discord.Interaction, title: str, message: str):
    allowed_user_id = 230773943240228864  # Your Discord user ID

    # Check if the user invoking the command is the allowed user
    if interaction.user.id != allowed_user_id:
        await interaction.response.send_message("You are not authorized to use this command.", ephemeral=True)
        return

    # Defer the response to give the bot more time to send out notices
    await interaction.response.defer(ephemeral=True)

    # Create the embed with the provided title and message
    embed = discord.Embed(title=title, description=message, color=discord.Color.blue())

    # Counter for the number of channels the message has been sent to
    channels_sent = 0

    # Iterate through each guild the bot is a part of
    for guild in bot.guilds:
        # Find the #chivstats-ranked channel
        channel = discord.utils.get(guild.text_channels, name="chivstats-ranked")
        if channel:
            try:
                # Send the embed to the channel
                await channel.send(embed=embed)
                channels_sent += 1
            except Exception as e:
                print(f"Failed to send message to {channel.name} in {guild.name}: {e}")

    # Send a follow-up message to the user with the results
    await interaction.followup.send(f"Notice success: sent to {channels_sent} channels.")

@bot.slash_command(guild_ids=GUILD_IDS, description="Explains how the ELO system works.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def elo(interaction: discord.Interaction, public: bool = False):
    # Create the embed with a detailed explanation of the ELO system
    embed = discord.Embed(
        title="Understanding the Ranked Combat ELO System",
        description=(
            "**Ranked Combat Chivstats ELO System Overview**\n"
            "The ELO rating system is a method for calculating the relative skill levels of players in competitive chiv 2 matches. It's a dynamic system that adjusts after each game based on the match's victor outcome. This is under continual refinement, please help.\n\n"

            "**How ELO is Calculated**\n"
            "1. **Win Expectancy:** Before a match, the system calculates the expected outcome based on the difference in ELO between the players.\n"
            "2. **ELO Adjustment:** After the match, your ELO is adjusted. A win against a stronger opponent yields a higher ELO increase than a win against a weaker opponent.\n"
            "3. **K-Factor:** The weight applied to the outcome of a match. A higher K-factor can result in larger swings in your ELO score.\n\n"

            "**Detailed ELO Calculation**\n"
            "`ELO_new = ELO_current + K * (S_actual - S_expected)`\n"
            "Where:\n"
            "- ELO_current: Current ELO rating of the player\n"
            "- K: K-factor, which adjusts the volatility of the ELO (set to 32 in our system)\n"
            "- S_actual: Actual match outcome (1 for a win, 0 for a loss)\n"
            "- S_expected: Expected match outcome, calculated using the formula below\n"
            "- c: A constant that determines the influence of the ELO difference (set to 400 in our system)\n\n"
            "`S_expected = 1 / (1 + 10^((ELO_opponent - ELO_current) / c))`\n\n"

            "**Establishing ELO**\n"
            "New players start with a default ELO, which is fine-tuned as they play more games.\n\n"

            "**Ranked Combat Success in ELO**\n"
            "In ranked combat, the focus is on match outcomes over individual performance metrics...\n\n"

            "**Impact of Kills and Deaths**\n"
            "Kills and deaths are significant for personal stats and other rankings but do not directly affect the ELO score.\n\n"

            "**TL;DR - The ELO System in Brief**\n"
            "Your ELO score changes after every match, depending on whether you win or lose, and by how much you were expected to win or lose. It's a way to measure your skill level compared to other players, and it keeps changing as you play more games. Type `/register` to get started.\n\n"

            "*Ranked Combat: To the victor go the spoils. [chivstats.xyz/leaderboards/ranked_combat](https://chivstats.xyz/leaderboards/ranked_combat/)*"
        ),
        color=discord.Color.blue()
    )

    # Decide whether to send the embed as a public or ephemeral message
    if public:
        await interaction.response.send_message(embed=embed, ephemeral=False)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


###################
#DUOS LOGIC
###################
# Helper function to calculate new ELO ratings for a duo match
async def calculate_duo_elo(team1_elo, team2_elo, team1_score, team2_score):
    # K factor for ELO calculation - may vary based on your requirements
    K = 32
    team1_new_elo = calculate_elo(team1_elo, K, team1_score > team2_score, 1, team2_elo)
    team2_new_elo = calculate_elo(team2_elo, K, team2_score > team1_score, 1, team1_elo)
    return team1_new_elo, team2_new_elo

# Helper function to check if a duo team exists and create one if not
async def check_or_create_duo_team(conn, playfabid1, playfabid2):
    try:
        # Check if the team already exists
        team = await conn.fetchrow("""
            SELECT id FROM duo_teams
            WHERE (player1_id = $1 AND player2_id = $2) OR (player1_id = $2 AND player2_id = $1)
            """, playfabid1, playfabid2)

        if not team:
            # Retrieve the display names of both players
            player1_name = await get_display_name_from_ranked_players(playfabid1)
            player2_name = await get_display_name_from_ranked_players(playfabid2)

            # Generate team name
            part1 = player1_name[:4] if player1_name else "Unk"
            part2 = player2_name[:4] if player2_name else "Unk"
            team_name = f"{part1}{part2}"
            # Create the new team with the generated team name and initial ELO rating
            initial_elo = 1500  # Example initial ELO rating
            team_id = await conn.fetchval("""
                INSERT INTO duo_teams (player1_id, player2_id, team_name, elo_rating)
                VALUES ($1, $2, $3, $4) RETURNING id
                """, playfabid1, playfabid2, team_name, initial_elo)
            return team_id
        else:
            return team['id']

    except Exception as e:
        print(f"Error in check_or_create_duo_team: {e}")
        raise e  # Re-raise the exception so that it can be handled by the calling function






@bot.slash_command(guild_ids=GUILD_IDS, description="Submit the result of a 2v2 duel between two teams.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def submit_duo(interaction: discord.Interaction, team_member: discord.Member, team_score: int, 
                      enemy1: discord.Member, enemy2: discord.Member, enemy_score: int):
    try: 
        conn = await asyncpg.connect(database=DATABASE, user=USER, host=HOST)

        # Fetch PlayFab IDs for all members of both teams
        team1_playfabid1 = await get_playfabid_of_discord_id(conn, interaction.user.id)
        team1_playfabid2 = await get_playfabid_of_discord_id(conn, team_member.id)
        team2_playfabid1 = await get_playfabid_of_discord_id(conn, enemy1.id)
        team2_playfabid2 = await get_playfabid_of_discord_id(conn, enemy2.id)
        command_text = f"/submit_duo @{interaction.user.display_name} & @{team_member.display_name} {team_score} vs @{enemy1.display_name} & @{enemy2.display_name} {enemy_score}"

        # Check for duplicate players
        players = [interaction.user, team_member, enemy1, enemy2]
        if len(players) != len(set(player.id for player in players)):
            await interaction.response.send_message("Duplicate players detected! Please ensure all players are unique.", ephemeral=True)
            return

        # Verify none of the players are retired
        player_ids = [interaction.user.id, team_member.id, enemy1.id, enemy2.id]
        results = await conn.fetch("SELECT discordid, retired FROM ranked_players WHERE discordid = ANY($1)", player_ids)
        retired_players = [discord_id for discord_id, retired in results if retired]
        if retired_players:
            message = "The following players are retired: "
            message += ', '.join(f"<@{discord_id}>" for discord_id in retired_players)
            message += "\nPlease reactivate your account using /reactivate."
            await interaction.response.send_message(message, ephemeral=True)
            return

        # Send an ephemeral response to the submitter to indicate the submission was accepted
        await interaction.response.send_message("Submission accepted. Please wait for confirmation from the opposing team.", ephemeral=True)

        # Check for existing teams or create new ones
        team1_id = await check_or_create_duo_team(conn, team1_playfabid1, team1_playfabid2)
        team2_id = await check_or_create_duo_team(conn, team2_playfabid1, team2_playfabid2)
        # Fetch current ELO ratings
        team1_elo = await conn.fetchval("SELECT elo_rating FROM duo_teams WHERE id = $1", team1_id)
        team2_elo = await conn.fetchval("SELECT elo_rating FROM duo_teams WHERE id = $1", team2_id)

        embed = discord.Embed(
            title="2v2 Duel Result (UNVERIFIED)",
            description=f"`{command_text}`\n\n"
                        f"Team 1: <@{interaction.user.id}> and <@{team_member.id}> - Score: {team_score}\n"
                        f"Team 2: <@{enemy1.id}> and <@{enemy2.id}> - Score: {enemy_score}",
            color=discord.Color.orange()
        )

        # Send the initial response message or edit if it already exists
        sent_message = await interaction.channel.send(embed=embed)

        # Add reactions to the sent message
        await sent_message.add_reaction('✅')
        await sent_message.add_reaction('❌')

        def check(reaction, user):
            if reaction.message.id != sent_message.id:
                return False
            # Check if the user is part of the opposing team
            is_opposing_team = user.id in [enemy1.id, enemy2.id]
            # Check if the user is part of the submitting team
            is_submitting_team = user.id in [interaction.user.id, team_member.id]
            # Allow '✅' and '❌' for opposing team members
            if is_opposing_team and str(reaction.emoji) in ['✅', '❌']:
                return True
            # Allow only '❌' for submitting team members
            elif is_submitting_team and str(reaction.emoji) == '❌':
                return True
            # Ignore other reactions and users not part of the teams
            return False

        try:
            # Wait for a valid reaction from the opposing team
            reaction, user = await bot.wait_for('reaction_add', timeout=3600.0, check=check)
            if str(reaction.emoji) == '✅':

                # Calculate new ELO ratings
                team1_new_elo, team2_new_elo = await calculate_duo_elo(team1_elo, team2_elo, team_score, enemy_score)

                # Update the duo_teams table with new ELO ratings
                await conn.execute("UPDATE duo_teams SET elo_rating = $1 WHERE id = $2", team1_new_elo, team1_id)
                await conn.execute("UPDATE duo_teams SET elo_rating = $1 WHERE id = $2", team2_new_elo, team2_id)
                # Fetch team names
                team1_name = await conn.fetchval("SELECT team_name FROM duo_teams WHERE id = $1", team1_id)
                team2_name = await conn.fetchval("SELECT team_name FROM duo_teams WHERE id = $1", team2_id)

                # Calculate ELO changes
                team1_elo_change = int(team1_new_elo - team1_elo)
                team2_elo_change = int(team2_new_elo - team2_elo)
                team1_new_elo_rounded = round(team1_new_elo)
                team2_new_elo_rounded = round(team2_new_elo)

                # Format ELO changes for display
                team1_elo_change_str = f"({team1_elo_change:+})" if team1_elo_change != 0 else "(±0)"
                team2_elo_change_str = f"({team2_elo_change:+})" if team2_elo_change != 0 else "(±0)"

                # Update the embed with the new ELO ratings
                embed.title = "2v2 Duel Result Confirmed"
                embed.color = discord.Color.green()
                embed.set_footer(text=f"Match result confirmed by {user.display_name}.")

                # Determine the winning team and set labels accordingly
                if team_score > enemy_score:
                    submitting_team_label = "Winners"
                    opposing_team_label = "Losers"
                else:
                    submitting_team_label = "Losers"
                    opposing_team_label = "Winners"
                embed.add_field(name=f"{submitting_team_label} - {team1_name} ELO: {team1_new_elo_rounded} {team1_elo_change_str}", value=f"<@{interaction.user.id}> (submitter) and <@{team_member.id}>", inline=False)
                embed.add_field(name=f"{opposing_team_label} - {team2_name} ELO: {team2_new_elo_rounded} {team2_elo_change_str}", value=f"<@{enemy1.id}> and <@{enemy2.id}>", inline=False)
                await sent_message.edit(embed=embed)

            elif str(reaction.emoji) == '❌':
                # Process the duel result as denied
                await sent_message.edit(content=f"Duel result denied by {user.display_name}.", embed=None)

        except asyncio.TimeoutError:
            await sent_message.edit(content="Duel confirmation timed out.", embed=None)
        finally:
            # Clear reactions after processing
            try:
                await sent_message.clear_reactions()
            except discord.errors.Forbidden:
                pass

    except Exception as e:
        # Handle exceptions and possibly roll back
        print(f"Error in submit_duo: {e}")
        await conn.execute('ROLLBACK')
    finally:
        # Ensure the connection is closed
        await conn.close()





@bot.slash_command(guild_ids=GUILD_IDS, description="Create and or update your duos team name.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def duo_setup_team(interaction: discord.Interaction, team_member: discord.Member, team_name: str, debug: bool = False):
    conn = await asyncpg.connect(database=DATABASE, user=USER, host=HOST)

    # Fetch PlayFab IDs for both members of the team
    playfabid1 = await get_playfabid_of_discord_id(conn, interaction.user.id)  # Assuming this is not an async function
    playfabid2 = await get_playfabid_of_discord_id(conn, team_member.id)      # Assuming this is not an async function

    try:
        # Check for an existing team or create a new one
        team_id = await check_or_create_duo_team(conn, playfabid1, playfabid2)  # Assuming this is not an async function

        # Update the team name
        await conn.execute("UPDATE duo_teams SET team_name = $1 WHERE id = $2", team_name, team_id)

        # Announce the update
        await interaction.response.send_message(f"Team name set to '{team_name}'.", ephemeral=True)

    except Exception as e:
        print(f"Error in duo_setup_team: {e}")
        await interaction.response.send_message("An error occurred while processing your request.", ephemeral=True)

    finally:
        await conn.close()



@bot.slash_command(guild_ids=GUILD_IDS, description="List all duo teams, their players, and ELO ranks in descending order.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def duo_teams(interaction: discord.Interaction):
    conn = await asyncpg.connect(database=DATABASE, user=USER, host=HOST)

    try:
        # Query all active duo teams with their ELO rating, ordered by ELO in descending order
        teams = await conn.fetch("""
            SELECT team_name, player1_id, player2_id, elo_rating, ROW_NUMBER() OVER (ORDER BY elo_rating DESC)
            FROM duo_teams
            WHERE retired = false
        """)

        if not teams:
            await interaction.response.send_message("There are currently no duo teams registered.", ephemeral=True)
            return

        # Create an embed to list the duo teams
        embed = discord.Embed(
            title="Duo Teams Ranked by ELO",
            description="Here are the currently registered duo teams, sorted by their ELO in descending order:",
            color=discord.Color.blue()
        )
        
        # Loop through the teams and add each to the embed
        for team_info in teams:
            team_name, player1_id, player2_id, elo_rating, position = team_info
            
            # Fetch display names for both players
            player1_name = await get_common_name_from_ranked_players(conn, player1_id)
            player2_name = await get_common_name_from_ranked_players(conn, player2_id)
            embed.add_field(
                name=f"#{position} - Team: {team_name}",
                value=f"Players: {player1_name} and {player2_name} - ELO: {elo_rating}",
                inline=False
            )

        await interaction.response.send_message(embed=embed)

    except Exception as e:
        print(f"Error in duo_teams: {e}")
        await interaction.response.send_message("An error occurred while retrieving the duo teams.", ephemeral=True)
    finally:
        await conn.close()

##########END OF DUOS#############


@bot.slash_command(guild_ids=GUILD_IDS, description="Displays the rank and stats of a player.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def rank(interaction: discord.Interaction, target_member: discord.Member = None):
    discord_id = target_member.id if target_member else interaction.user.id
    conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
    cur = conn.cursor()

    try:
        # Fetch player's Duels ELO (elo_duelsx), kills, deaths, matches, PlayFab ID, username, and coins
        cur.execute("""
            SELECT elo_duelsx, kills, deaths, matches, playfabid, discord_username, common_name, coins 
            FROM ranked_players 
            WHERE discordid = %s
            """, (discord_id,))
        result = cur.fetchone()

        if result:
            elo_duelsx, kills, deaths, matches, playfabid, discord_username, common_name, coins = result
            elo_duelsx_rounded = round(elo_duelsx)  # Round ELO to a whole number
            kdr = kills / deaths if deaths > 0 else kills  # Avoid division by zero


            # Calculate player's wealth rank based on coins
            cur.execute("""
                SELECT COUNT(*) + 1 
                FROM ranked_players 
                WHERE coins > %s
                """, (coins,))
            wealth_rank_result = cur.fetchone()
            wealth_rank = f"**#{wealth_rank_result[0]}**" if wealth_rank_result else '**N/A**'

            # Calculate the player's ELO rank
            cur.execute("""
                SELECT COUNT(*) + 1 
                FROM ranked_players
                WHERE elo_duelsx > %s
                """, (elo_duelsx,))
            elo_rank_result = cur.fetchone()
            elo_rank = f"**#{elo_rank_result[0]}**" if elo_rank_result else '**N/A**'

            # Calculate the player's KDR rank
            cur.execute("""
                SELECT COUNT(*) + 1 
                FROM ranked_players
                WHERE (CAST(kills AS FLOAT) / NULLIF(deaths, 0)) > %s
                """, (kdr,))
            kdr_rank_result = cur.fetchone()
            kdr_rank = f"**#{kdr_rank_result[0]}**" if kdr_rank_result else '**N/A**'

            # Calculate the player's matches rank
            cur.execute("""
                SELECT COUNT(*) + 1 
                FROM ranked_players
                WHERE matches > %s
                """, (matches,))
            matches_rank_result = cur.fetchone()
            matches_rank = f"**#{matches_rank_result[0]}**" if matches_rank_result else '**N/A**'

            profile_url = f"https://chivstats.xyz/leaderboards/player/{playfabid}/"
            leaderboard_url = "https://chivstats.xyz/leaderboards/ranked_combat/"
            # Embed construction
            embed = discord.Embed(
                title=f"{common_name} Ranked Statistics",
                description=(
                    f"<@{discord_id}>'s Stats:\n"
                    f"[Duels ELO Rating:]({leaderboard_url}) {round(elo_duelsx)} ({elo_rank})\n"
                    f"KDR: {kills}:{deaths} ({kdr_rank})\n"
                    f"Matches: {matches} ({matches_rank})\n"
                    f"Purse: {coins} coins ({wealth_rank})\n\n"  # Added line for coins and wealth rank
                    f"[{discord_username} on ChivStats.xyz]({profile_url})"
                ),
                color=discord.Color.blue(),
                url=profile_url
            )
            await interaction.response.send_message(embed=embed)
        else:
            await interaction.response.send_message("Player not found in the ranking system.", ephemeral=True)
    except Exception as e:
        print(f"Database error: {e}")
        await interaction.response.send_message("An error occurred while fetching the player rank.", ephemeral=True)
    finally:
        cur.close()
        conn.close()

# Global list to track the queue for duels
@bot.slash_command(guild_ids=GUILD_IDS, description="Enter the matchmaking queue for a duel.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def queue(interaction: discord.Interaction, action: str):
    global duel_queue
    # User wants to enter the queue
    if action.lower() == 'duel':
        if interaction.user.id in [user.id for user, _, _ in duel_queue]:
            await interaction.response.send_message("You are already in the queue.", ephemeral=True)
            return

        # Defer the interaction because we need more time to process
        await interaction.response.defer(ephemeral=False)

        # Add user to the queue with the current time and a placeholder for the message
        duel_queue.append((interaction.user, time.time(), None))

        # Send a message to the channel about entering the queue and store the message
        queue_message = await interaction.followup.send(f"{interaction.user.display_name} has entered the duel queue. Position: {len(duel_queue)}")
        duel_queue[-1] = (interaction.user, time.time(), queue_message)

        if len(duel_queue) >= 2:
            player1, entry_time1, message1 = duel_queue.pop(0)
            player2, entry_time2, message2 = duel_queue.pop(0)

            # Calculate time spent in queue for this session
            session_time1 = round(time.time() - entry_time1)
            session_time2 = round(time.time() - entry_time2)

            # Update database with time spent in queue and get total time in queue
            total_time1 = update_queue_time(player1.id, session_time1)
            total_time2 = update_queue_time(player2.id, session_time2)

            # Delete original queue messages
            await message1.delete()
            await message2.delete()

            # Announce the match in the channel with the time information
            match_announcement = (
                f"Match ready: {player1.display_name} vs {player2.display_name}\n"
                f"{player1.display_name} waited {session_time1} seconds this match. Lifetime queued: {total_time1}s.\n"
                f"{player2.display_name} waited {session_time2} seconds this match. Lifetime queued: {total_time2}s."
            )
            await interaction.channel.send(match_announcement)

    # User wants to exit the queue
    elif action.lower() == 'exit':
        found = False
        for user, entry_time, queue_message in duel_queue:
            if user.id == interaction.user.id:
                found = True
                # Calculate time spent in queue
                time_spent = round(time.time() - entry_time)
                
                # Update database with time spent in queue
                update_queue_time(interaction.user.id, time_spent)

                # Edit the original queue message
                if queue_message:
                    await queue_message.edit(content=f"{interaction.user.display_name} has left the duel queue. Total time queued: {time_spent}s.")
                
                # Remove user from the queue
                duel_queue.remove((user, entry_time, queue_message))
                break

        if not found:
            # If the user wasn't found in the queue, send an ephemeral message
            await interaction.response.send_message("You are not in the queue.", ephemeral=True)
        else:
            # Acknowledge the exit if the user was found
            await interaction.response.send_message("You have exited the queue.", ephemeral=True)

@bot.slash_command(guild_ids=GUILD_IDS, description="COST: 25 Add or remove a clown emoji to a user's nickname globally.")
async def clown(interaction: discord.Interaction, member: discord.Member):
    cost = 25
    clown_emoji = "🤡"
    
    # Connect to the database
    conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
    cur = conn.cursor()
    conn.autocommit = False
    
    try:
        # Check and fetch the user's coins and lock the row
        cur.execute("SELECT coins FROM ranked_players WHERE discordid = %s FOR UPDATE", (interaction.user.id,))
        user_coins = cur.fetchone()[0]
        
        if user_coins < cost:
            await interaction.response.send_message("You do not have enough coins.", ephemeral=True)
            conn.rollback()
            return
        
        # Determine the global action to be taken (clown or declown)
        global_action = "clown" if clown_emoji not in member.display_name else "declown"
        
        # Proceed with the coin transaction
        new_balance = user_coins - cost
        cur.execute("UPDATE ranked_players SET coins = %s WHERE discordid = %s", (new_balance, interaction.user.id))
        
        # Update the house account balance
        update_house_account_balance(cur, cost)
        
        # Commit the transaction
        conn.commit()

        # Iterate through all guilds to update the member's nickname
        for guild in bot.guilds:
            try:
                guild_member = await guild.fetch_member(member.id)
                if guild_member and guild.me.guild_permissions.manage_nicknames:
                    new_nickname = f"{clown_emoji} {guild_member.display_name}" if global_action == "clown" else guild_member.display_name.replace(clown_emoji, "").strip()
                    await guild_member.edit(nick=new_nickname)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass  # Handle exceptions silently

        # Announce the action in all relevant channels
        for guild in bot.guilds:
            if guild.get_member(member.id):
                chivstats_channel = discord.utils.get(guild.text_channels, name="chivstats-ranked")
                if chivstats_channel:
                    announcement = f"{member.mention} has been {global_action}ed globally. 25 coins deposited to the house account by an anonymous donor."
                    await chivstats_channel.send(announcement)

        # Send confirmation to the user who invoked the command
        await interaction.response.send_message(f"{member.mention} has been {global_action}ed globally. Cost was {cost} coins. Your remaining balance is {new_balance} coins.", ephemeral=True)

    except Exception as e:
        await interaction.response.send_message("An error occurred while processing your request. Please try again.", ephemeral=True)
        conn.rollback()
    finally:
        cur.close()
        conn.autocommit = True
        conn.close()

def update_house_account_balance(cur, amount):
    # Fetch the latest house balance or initialize if no entry exists
    cur.execute("SELECT balance FROM house_account ORDER BY last_updated DESC LIMIT 1")
    house_balance_entry = cur.fetchone()
    
    if house_balance_entry:
        house_balance = house_balance_entry[0] + amount
    else:
        # Initialize house account with a zero balance if it's the first transaction
        house_balance = amount
        cur.execute("INSERT INTO house_account (balance, last_updated, payout_rate) VALUES (%s, CURRENT_TIMESTAMP, 5.00)", (house_balance,))
    
    # Update house account with new balance
    cur.execute("UPDATE house_account SET balance = %s", (house_balance,))


@bot.slash_command(guild_ids=GUILD_IDS, description="Displays the house account value and the current payout rate.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def bank(interaction: discord.Interaction):
    conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
    cur = conn.cursor()

    try:
        cur.execute("SELECT balance, payout_rate FROM house_account ORDER BY last_updated DESC LIMIT 1")
        house_account_entry = cur.fetchone()

        if house_account_entry:
            balance, payout_rate = house_account_entry
            embed = discord.Embed(
                title=":bank: House Account",
                description=f"**Account Balance:** {balance} coins(:coin:)\n**Payout Rate:** {payout_rate}%",
                color=discord.Color.gold()
            )
            await interaction.response.send_message(embed=embed)
        else:
            await interaction.response.send_message("The house bank information is currently unavailable.", ephemeral=True)

    except Exception as e:
        await interaction.response.send_message("An error occurred while retrieving the bank information.", ephemeral=True)
        print(f"Database error: {e}")

    finally:
        cur.close()
        conn.close()

def update_queue_time(discord_id, time_spent):
    total_time_queued = 0
    try:
        conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
        cur = conn.cursor()

        cur.execute("SELECT time_queued FROM ranked_players WHERE discordid = %s", (discord_id,))
        result = cur.fetchone()
        if result:
            # Add the current session's time spent to the total time and update the database
            total_time_queued = result[0] + time_spent
            cur.execute("""
                UPDATE ranked_players
                SET time_queued = %s
                WHERE discordid = %s
            """, (total_time_queued, discord_id))
            conn.commit()
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

    return total_time_queued

@bot.slash_command(guild_ids=GUILD_IDS, description="Displays stats for a PlayFab ID.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def stats(interaction: discord.Interaction, playfabid: str = None):
    discord_id = interaction.user.id
    conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
    cur = conn.cursor()
    cur.execute("SELECT retired FROM ranked_players WHERE discordid = %s", (discord_id,))
    retired = cur.fetchone()
    if retired and retired[0]:
        await interaction.response.send_message("This account is retired. Please reactivate using /reactivate.", ephemeral=True)
        return

    try:
        conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
        cur = conn.cursor()

        if playfabid is None:
            discord_id = interaction.user.id
            cur.execute("SELECT playfabid FROM ranked_players WHERE discordid = %s", (discord_id,))
            result = cur.fetchone()
            if result:
                playfabid = result[0]
            else:
                embed = discord.Embed(
                    title="Stats Lookup",
                    description=f"Your Discord account is not linked to any PlayFab ID. Find your PlayFab ID [here](https://chivstats.xyz/leaderboards/player_search/), and use /register",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed)
                return

        common_name = await get_common_name_from_ranked_players(conn, playfabid)
        playfab_link = await format_playfab_id_with_url(conn, playfabid)

        stats = get_player_latest_stats_and_rank(playfabid)
        if stats:
            embed = discord.Embed(
                title=f"Latest Stats for {common_name}",
                description=f"Stats for PlayFab ID {playfab_link}:",
                color=discord.Color.blue()
            )
            for leaderboard, info in stats.items():
                embed.add_field(
                    name=leaderboard,
                    value=f"Value - {info['stat_value']}, Serial Number - {info['serialnumber']}, Rank - {info['rank']}",
                    inline=False
                )
            await interaction.response.send_message(embed=embed)
        else:
            embed = discord.Embed(
                title="Stats Lookup",
                description=f"Could not find stats for PlayFab ID {playfab_link}",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed)
    except Exception as e:
        print(f"Database error: {e}")
        embed = discord.Embed(
            title="Stats Lookup",
            description=f"An error occurred while processing your request.",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)
    finally:
        if conn is not None:
            conn.close()

def get_player_latest_stats_and_rank(playfabid):
    stats = {}
    try:
        conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
        cur = conn.cursor()

        for leaderboard in leaderboard_classes:
            # Get the most recent serialnumber and stat_value for the player
            cur.execute(f"""
                SELECT stat_value, serialnumber
                FROM {leaderboard}
                WHERE playfabid = %s
                ORDER BY serialnumber DESC
                LIMIT 1
                """, (playfabid,))
            result = cur.fetchone()

            if result:
                stat_value, serialnumber = result
                # Get the rank of the player based on stat_value
                cur.execute(f"""
                    SELECT COUNT(*) + 1
                    FROM {leaderboard}
                    WHERE serialnumber = %s AND stat_value > %s
                    """, (serialnumber, stat_value))
                rank = cur.fetchone()[0]

                stats[leaderboard] = {'stat_value': stat_value, 'serialnumber': serialnumber, 'rank': rank}
            else:
                stats[leaderboard] = {'stat_value': 'No data', 'serialnumber': None, 'rank': None}

        cur.close()
        return stats
    except Exception as e:
        print(f"Database error: {e}")  # Debugging print
        return None

@bot.slash_command(guild_ids=GUILD_IDS, description="Checks registered status.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def status(interaction: discord.Interaction, playfabid: str = None):
    try:
        conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
        cur = conn.cursor()

        # Use the Discord ID to find the linked PlayFab ID and retirement status
        discord_id = interaction.user.id if playfabid is None else None
        if discord_id:
            cur.execute("SELECT playfabid, retired FROM ranked_players WHERE discordid = %s", (discord_id,))
            result = cur.fetchone()
            if result:
                playfabid, retired = result
                playfab_link = f"https://chivstats.xyz/leaderboards/player/{playfabid}/"
                retirement_status = "Retired" if retired else "Active"
                description = f"Your Discord account is linked to PlayFab ID [View Profile]({playfab_link}).\nStatus: {retirement_status}."
            else:
                description = "Your Discord account is not linked to any PlayFab ID."
        else:
            cur.execute("SELECT discordid, retired FROM ranked_players WHERE playfabid = %s", (playfabid,))
            result = cur.fetchone()
            if result:
                linked_discord_id, retired = result
                retirement_status = "Retired" if retired else "Active"
                description = f"PlayFab ID {playfabid} is linked to Discord account: <@{linked_discord_id}>.\nStatus: {retirement_status}."
            else:
                description = f"No player found with PlayFab ID: {playfabid}"

        embed = discord.Embed(
            title="Account Status",
            description=description,
            color=discord.Color.blue() if result else discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)

    except Exception as e:
        print(f"Database error: {e}")
        await interaction.response.send_message(f"An error occurred while processing your request.", ephemeral=True)

    finally:
        if conn is not None:
            conn.close()


@bot.slash_command(guild_ids=GUILD_IDS, description="Links your Discord account to a PlayFab ID.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def register(interaction: discord.Interaction, playfabid: str):
    # Defer the response to give us time to process the linking
    await interaction.response.defer()

    try:
        conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
        cur = conn.cursor()

        # Check if the provided PlayFab ID exists in the players table
        cur.execute("SELECT id FROM players WHERE playfabid = %s", (playfabid,))
        player = cur.fetchone()
        if player is None:
            await interaction.followup.send("The provided PlayFab ID does not exist.", ephemeral=True)
            return

        player_id = player[0]

        # Retrieve the common name (most common alias)
        common_name = await get_most_common_alias(conn, playfabid)

        # Check if PlayFab ID already linked to a different Discord account
        cur.execute("SELECT discordid FROM ranked_players WHERE playfabid = %s", (playfabid,))
        linked_discord_id = cur.fetchone()
        if linked_discord_id and linked_discord_id[0] != interaction.user.id:
            await interaction.followup.send("This PlayFab ID is already linked to another Discord account.", ephemeral=True)
            return

        # Check if Discord account already linked to a different PlayFab ID
        cur.execute("SELECT playfabid FROM ranked_players WHERE discordid = %s", (interaction.user.id,))
        linked_playfab_id = cur.fetchone()
        if linked_playfab_id:
            await interaction.followup.send("Your Discord account is already linked to a PlayFab ID.", ephemeral=True)
            return

        # Link PlayFab ID and Discord ID in players table
        cur.execute("UPDATE players SET discordid = %s WHERE id = %s", (interaction.user.id, player_id))
        conn.commit()

        # Insert into ranked_players table or update if exists
        cur.execute("""
            INSERT INTO ranked_players (player_id, playfabid, discordid, discord_username, common_name, elo_rating)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (playfabid) DO 
            UPDATE SET discordid = EXCLUDED.discordid, discord_username = EXCLUDED.discord_username, common_name = EXCLUDED.common_name
        """, (player_id, playfabid, interaction.user.id, interaction.user.display_name, common_name, 1500))
        conn.commit()

        # Find the "Ranked Combatant" role in the guild
        role = discord.utils.get(interaction.guild.roles, name="Ranked Combatant")
        if role:
            try:
                # Add the role to the user
                await interaction.user.add_roles(role)
                role_message = f" You have been assigned the '{role.name}' role."
            except Exception as e:
                print(f"Failed to assign role: {e}")
                role_message = " However, I was unable to assign the 'Ranked Combatant' role."
        else:
            role_message = " However, the 'Ranked Combatant' role was not found in this server."

        # Public message with the registration information
        embed = discord.Embed(
            title="Player Registration Complete",
            description=f"{interaction.user.mention} has successfully registered for ranked combat.\n\n"
                        f"Common Name: {common_name}\n"
                        f"Starting Duels ELO: 1500\n"
                        f"View [ChivStats.xyz player profile](https://chivstats.xyz/leaderboards/player/{playfabid}/)\n\n{role_message}",
            color=discord.Color.green()
        )
        await interaction.followup.send(embed=embed)

    except Exception as e:
        await interaction.followup.send("An error occurred while processing your request. Please try again.", ephemeral=True)
        print(f"Database error: {e}")
    finally:
        if conn is not None:
            conn.close()


@bot.slash_command(guild_ids=GUILD_IDS, description="Reactivate your account for ranked matches.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def reactivate(interaction: discord.Interaction):
    try:
        conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
        cur = conn.cursor()

        cur.execute("UPDATE ranked_players SET retired = FALSE WHERE discordid = %s RETURNING playfabid, common_name, elo_rating", (interaction.user.id,))
        result = cur.fetchone()
        playfabid, common_name, elo_rating = result
        conn.commit()

        # Find the "Ranked Combatant" role in the guild
        role = discord.utils.get(interaction.guild.roles, name="Ranked Combatant")
        if role:
            try:
                # Add the role back to the user
                await interaction.user.add_roles(role)
                role_message = "Player re-added to the 'Ranked Combatant' role."
            except Exception as e:
                print(f"Failed to assign role: {e}")
                role_message = "Error: Unable to re-add player to the 'Ranked Combatant' role."
        else:
            role_message = "Error: 'Ranked Combatant' role was not found in this server."

        playfab_link = f"https://chivstats.xyz/leaderboards/player/{playfabid}/"
        embed = discord.Embed(
            title="Reactivation Announcement",
            description=f"{interaction.user.mention} ({common_name}) has reactivated their account for ranked matches.\n\nDuels ELO: {elo_rating}\n[View {common_name} on ChivStats.xyz]({playfab_link})\n\n{role_message}",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        await interaction.response.send_message("An error occurred while processing your request. Please try again.", ephemeral=True)
        print(f"Database error: {e}")
    finally:
        if conn is not None:
            conn.close()


@bot.slash_command(guild_ids=GUILD_IDS, description="Retire your account from ranked matches.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def retire(interaction: discord.Interaction):
    try:
        conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
        cur = conn.cursor()
        cur.execute("UPDATE ranked_players SET retired = TRUE WHERE discordid = %s RETURNING playfabid, common_name, elo_rating", (interaction.user.id,))
        result = cur.fetchone()
        playfabid, common_name, elo_rating = result
        conn.commit()

        # Find the "Ranked Combatant" role in the guild
        role = discord.utils.get(interaction.guild.roles, name="Ranked Combatant")
        if role:
            try:
                # Remove the role from the user
                await interaction.user.remove_roles(role)
                role_message = "Player removed from the 'Ranked Combatant' role."
            except Exception as e:
                print(f"Failed to remove role: {e}")
                role_message = "Error: Unable to remove player from the 'Ranked Combatant' role."
        else:
            role_message = "Error: The 'Ranked Combatant' role was not found in this server."

        playfab_link = f"https://chivstats.xyz/leaderboards/player/{playfabid}/"
        embed = discord.Embed(
            title="Retirement Announcement",
            description=f"{interaction.user.mention} ({common_name}) has retired from ranked matches.\n\nDuels ELO: {elo_rating}\n[View {common_name} on ChivStats.xyz]({playfab_link})\n\n{role_message}",
            color=discord.Color.blue()
        )
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        await interaction.response.send_message("An error occurred while processing your request. Please try again.", ephemeral=True)
        print(f"Database error: {e}")
    finally:
        if conn is not None:
            conn.close()

@bot.slash_command(guild_ids=GUILD_IDS, description="Set your in-game name.")
@is_channel_named(['chivstats-ranked', 'chivstats-test'])
async def gamename(interaction: discord.Interaction, name: str):
    safe_name = psycopg2.extensions.adapt(name).getquoted().decode()
    conn = psycopg2.connect(database=DATABASE, user=USER, host=HOST)
    cur = conn.cursor()
    try:
        # Update the gamename in the ranked_players table
        cur.execute("""
            UPDATE ranked_players
            SET gamename = %s
            WHERE discordid = %s
        """, (safe_name, interaction.user.id))
        conn.commit()
        # Confirm the update to the user
        await interaction.response.send_message(f"Your in-game name has been set to: {safe_name}", ephemeral=True)
    except Exception as e:
        print(f"Database error: {e}")
        await interaction.response.send_message("An error occurred while updating your in-game name.", ephemeral=True)
    finally:
        cur.close()
        conn.close()

# Run the bot, maaan
bot.run(TOKEN)
