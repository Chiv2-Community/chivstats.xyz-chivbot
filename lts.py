######################################################################################################
##### BEGIN 3v3+ LTS #################################################################################
######################################################################################################
import os
import asyncio
import discord
from discord.ext import commands
from discord.ext.commands import check, CheckFailure
from datetime import datetime, timedelta, timezone
import time
import asyncpg
import traceback
import pytz
import json
from discord.ui import Button, View

def calculate_new_elo(winner_elo, loser_elo, k=32):
    """
    Calculate the new ELO ratings for the winner and loser of a match.
    :param winner_elo: Current ELO rating of the winner.
    :param loser_elo: Current ELO rating of the loser.
    :param k: The maximum possible adjustment per game (default 32).
    :return: Tuple containing the new ELO ratings (winner_new_elo, loser_new_elo).
    """
    winner_transformed_rating = 10 ** (winner_elo / 400)
    loser_transformed_rating = 10 ** (loser_elo / 400)

    expected_winner = winner_transformed_rating / (winner_transformed_rating + loser_transformed_rating)
    expected_loser = loser_transformed_rating / (winner_transformed_rating + loser_transformed_rating)

    winner_new_elo = round(winner_elo + k * (1 - expected_winner))
    loser_new_elo = round(loser_elo + k * (0 - expected_loser))

    return winner_new_elo, loser_new_elo

async def lts_register_team(interaction: discord.Interaction, team_name: str):
    conn = await create_db_connection()
    try:
        # Check if the team name already exists
        existing_team_name = await conn.fetchval("SELECT id FROM lts_teams WHERE team_name = $1", team_name)
        if existing_team_name:
            await interaction.response.send_message("This team name is already taken. Please choose a different name.", ephemeral=True)
            return

        # Check if the user is already a member or owner of an LTS team
        user_id_str = str(interaction.user.id)  # Convert user ID to string
        existing_team_member = await conn.fetchval("SELECT id FROM lts_teams WHERE team_owner = $1 OR roster @> $2::jsonb", interaction.user.id, json.dumps([user_id_str]))
        if existing_team_member:
            await interaction.response.send_message("You are already a member or owner of an LTS team.", ephemeral=True)
            return

        # Convert the user's Discord ID to a string and insert the new team into the lts_teams table
        initial_roster = [user_id_str]
        await conn.execute("""
            INSERT INTO lts_teams (team_name, team_owner, roster)
            VALUES ($1, $2, $3::jsonb)
            """, team_name, interaction.user.id, json.dumps(initial_roster))

        await interaction.response.send_message(f"Team '{team_name}' created successfully.", ephemeral=True)

    except Exception as e:
        await interaction.response.send_message("An error occurred while creating the LTS team.", ephemeral=True)
        print(f"Error in lts_register_team: {e}")

    finally:
        await close_db_connection(conn)

async def lts_roster(interaction: discord.Interaction, action: str, player: discord.Member = None):
    conn = await create_db_connection()
    try:
        if action == "list":
            await handle_list_action(interaction, player, conn)
        elif action in ["add", "remove"]:
            if player is None:
                await interaction.response.send_message("Please specify a player for 'add' or 'remove' actions.", ephemeral=True)
                return
            await handle_add_remove_action(interaction, action, player, conn)
        else:
            await interaction.response.send_message("Invalid action. Please specify 'list', 'add', or 'remove'.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send("An error occurred while processing the command.", ephemeral=True)
        print(f"Error in lts_roster: {e}")
    finally:
        await close_db_connection(conn)

async def handle_list_action(interaction, player, conn):
    # Defer the interaction to process the command
    await interaction.response.defer(ephemeral=True)

    # Determine the target ID based on provided player or command invoker
    target_id = str(player.id) if player else str(interaction.user.id)

    # Fetch the team information
    team_info = await conn.fetchrow("SELECT team_name, roster FROM lts_teams WHERE roster @> $1::jsonb", json.dumps([target_id]))
    if not team_info:
        await interaction.followup.send("The specified player is not a member of any LTS team.", ephemeral=True)
        return

    team_name, roster = team_info
    roster = json.loads(roster)

    # Create a list of mentions for each team member
    roster_mentions = [f"<@{member_id}>" for member_id in roster]
    roster_display = ", ".join(roster_mentions)

    # Send the roster list as a followup message
    await interaction.followup.send(f"**Team '{team_name}' Roster:**\n{roster_display}", ephemeral=True)

async def handle_add_remove_action(interaction, action, player, conn):
    if not player:
        await interaction.response.send_message("Please specify a player for 'add' or 'remove' actions.", ephemeral=True)
        return

    player_id_str = str(player.id)  # Convert player ID to string for JSONB query
    team = await conn.fetchrow("SELECT id, roster FROM lts_teams WHERE team_owner = $1", interaction.user.id)
    
    if not team:
        await interaction.response.send_message("You are not the owner of any LTS team.", ephemeral=True)
        return

    team_id, roster = team
    roster = json.loads(roster)

    # Handling 'add' action
    if action == "add":
        existing_team = await conn.fetchval("SELECT id FROM lts_teams WHERE roster @> $1::jsonb", json.dumps([player_id_str]))
        if existing_team:
            await interaction.response.send_message(f"{player.display_name} is already in a team.", ephemeral=True)
            return

        if player_id_str not in roster:
            roster.append(player_id_str)
            await conn.execute("UPDATE lts_teams SET roster = $1::jsonb WHERE id = $2", json.dumps(roster), team_id)
            await interaction.response.send_message(f"{player.display_name} has been added to your team.", ephemeral=True)
        else:
            await interaction.response.send_message(f"{player.display_name} is already in your team.", ephemeral=True)

    # Handling 'remove' action
    elif action == "remove":
        if player_id_str in roster:
            roster.remove(player_id_str)
            await conn.execute("UPDATE lts_teams SET roster = $1::jsonb WHERE id = $2", json.dumps(roster), team_id)
            await interaction.response.send_message(f"{player.display_name} has been removed from your team.", ephemeral=True)
        else:
            await interaction.response.send_message(f"{player.display_name} is not in your team.", ephemeral=True)


async def lts_rename_team(interaction: discord.Interaction, new_team_name: str):
    conn = await create_db_connection()
    try:
        # Check if the team name already exists
        existing_team_name = await conn.fetchval("SELECT id FROM lts_teams WHERE team_name = $1", new_team_name)
        if existing_team_name:
            await interaction.response.send_message("This team name is already taken. Please choose a different name.", ephemeral=True)
            return

        # Fetch the team where the user is the owner
        team_id = await conn.fetchval("SELECT id FROM lts_teams WHERE team_owner = $1", interaction.user.id)
        if not team_id:
            await interaction.response.send_message("You are not the owner of any LTS team.", ephemeral=True)
            return

        # Update the team name
        await conn.execute("UPDATE lts_teams SET team_name = $1 WHERE id = $2", new_team_name, team_id)

        await interaction.response.send_message(f"Your team's name has been successfully changed to '{new_team_name}'.", ephemeral=True)

    except Exception as e:
        await interaction.response.send_message("An error occurred while renaming the LTS team.", ephemeral=True)
        print(f"Error in lts_rename_team: {e}")

    finally:
        await close_db_connection(conn)

async def lts_leave_team(interaction: discord.Interaction):
    conn = await create_db_connection()
    try:
        player_id_str = str(interaction.user.id)  # Convert player ID to string for JSONB query

        # Check if the user is the owner of any LTS team
        team_owner_check = await conn.fetchrow("SELECT id, team_name FROM lts_teams WHERE team_owner = $1", interaction.user.id)
        if team_owner_check:
            team_name = team_owner_check['team_name']
            await interaction.response.send_message(f"You are the owner of the team '{team_name}' and cannot leave it. Transfer ownership or disband the team to proceed.", ephemeral=True)
            return

        # Fetch the team where the user is a member
        team_info = await conn.fetchrow("SELECT id, roster, team_name FROM lts_teams WHERE roster @> $1::jsonb", json.dumps([player_id_str]))
        if not team_info:
            await interaction.response.send_message("You are not a member of any LTS team.", ephemeral=True)
            return

        team_id, roster, team_name = team_info
        roster = json.loads(roster)

        # Remove the player from the roster
        if player_id_str in roster:
            roster.remove(player_id_str)
            await conn.execute("UPDATE lts_teams SET roster = $1::jsonb WHERE id = $2", json.dumps(roster), team_id)
            leave_message = f"You have successfully left the team '{team_name}'."
            await interaction.response.send_message(leave_message, ephemeral=True)
        else:
            await interaction.response.send_message("You are not a member of any team.", ephemeral=True)

    except Exception as e:
        await interaction.response.send_message("An error occurred while leaving the LTS team.", ephemeral=True)
        print(f"Error in lts_leave_team: {e}")

    finally:
        await close_db_connection(conn)

async def lts_teams(interaction: discord.Interaction):
    conn = await create_db_connection()
    try:
        # Fetch all teams and their owners
        teams = await conn.fetch("SELECT team_name, team_owner FROM lts_teams ORDER BY team_name")

        if not teams:
            await interaction.response.send_message("There are currently no registered LTS teams.", ephemeral=True)
            return

        # Format the list of teams
        team_list = []
        for team in teams:
            team_name = team['team_name']
            owner_id = team['team_owner']
            team_list.append(f"Team: {team_name} - Owner: <@{owner_id}>")

        formatted_team_list = "\n".join(team_list)

        # Send the list of teams
        await interaction.response.send_message(f"**Registered LTS Teams:**\n{formatted_team_list}", ephemeral=True)

    except Exception as e:
        await interaction.response.send_message("An error occurred while retrieving the list of LTS teams.", ephemeral=True)
        print(f"Error in lts_teams: {e}")

    finally:
        await close_db_connection(conn)

class ConfirmLTS(View):
    def __init__(self, submitter, opposing_player, submitter_team, opposing_team):
        super().__init__(timeout=180)  # Timeout set to 3 minutes for example
        self.submitter = submitter
        self.opposing_player = opposing_player
        self.submitter_team = submitter_team
        self.opposing_team = opposing_team
        self.confirmed = None
        self.canceller = None

    def clear_buttons(self):
        # Clear all buttons from the view
        self.clear_items()

    @discord.ui.button(label='Confirm', style=discord.ButtonStyle.green, custom_id='confirm_lts')
    async def confirm_button_callback(self, button, interaction):
        print(f"Confirm button clicked by {interaction.user} (ID: {interaction.user.id})")  # Debug print with ID
        if str(interaction.user.id) in self.opposing_team['roster']:
            print("Confirmation from opposing team member.")  # Debug print
            await interaction.response.defer()
            self.clear_buttons()
            self.confirmed = True
            await interaction.edit_original_response(view=self)
            self.stop()
        else:
            print("Confirmation attempt from unauthorized user.")  # Debug print
            await interaction.response.send_message("You are not authorized to confirm this match.", ephemeral=True)

    @discord.ui.button(label='Cancel', style=discord.ButtonStyle.red, custom_id='cancel_lts')
    async def cancel_button_callback(self, button, interaction):
        print(f"Cancel button clicked by {interaction.user} (ID: {interaction.user.id})")  # Debug print with ID
        if str(interaction.user.id) in self.submitter_team['roster'] or \
           str(interaction.user.id) in self.opposing_team['roster']:
            print("Cancellation by authorized team member.")  # Debug print
            self.confirmed = False
            self.canceller = interaction.user
            self.clear_buttons()
            await interaction.response.edit_message(view=self)
            self.stop()
        else:
            print("Cancellation attempt from unauthorized user.")  # Debug print
            await interaction.response.send_message("You are not authorized to cancel this match.", ephemeral=True)


async def submit_lts(interaction: discord.Interaction, team_score: int, opposing_player: discord.Member, opposing_team_score: int):
    conn = await create_db_connection()
    try:
        submitter_team = await get_team_of_player(conn, interaction.user.id)
        opposing_team = await get_team_of_player(conn, opposing_player.id)

        if not submitter_team or not opposing_team:
            await interaction.response.send_message("One of the players is not registered in a team.", ephemeral=True)
            return

        unverified_embed = discord.Embed(
            title="LTS 3v3+ Team Match Reported (UNVERIFIED)",
            description=f"**{submitter_team['team_name']}** ({team_score}) vs **{opposing_team['team_name']}** ({opposing_team_score})",
            color=discord.Color.orange()
        )
        unverified_embed.set_footer(text=f"Any teammate from {submitter_team['team_name']} can confirm this match. Either team members can cancel it")
        unverified_embed.add_field(name="Submitter", value=interaction.user.mention)
        unverified_embed.add_field(name="Opposing Player", value=opposing_player.mention)

        view = ConfirmLTS(submitter=interaction.user, opposing_player=opposing_player, submitter_team=submitter_team, opposing_team=opposing_team)
        message = await interaction.response.send_message(embed=unverified_embed, view=view)
        view.message = message

        await view.wait()

        if hasattr(view, 'confirmed') and view.confirmed is not None:
            if view.confirmed:
                # Determine winner, loser and their scores
                if team_score > opposing_team_score:
                    winner_team, loser_team = submitter_team, opposing_team
                    winner_score, loser_score = team_score, opposing_team_score
                else:
                    winner_team, loser_team = opposing_team, submitter_team
                    winner_score, loser_score = opposing_team_score, team_score


                # Calculate new ELO ratings (without match scores)
                winner_new_elo, loser_new_elo = calculate_new_elo(winner_team['elo_rating'], loser_team['elo_rating'])

                # Update lts_matches table
                await conn.execute("""
                    INSERT INTO lts_matches (submitting_discordid, confirming_discordid, winner_team_id, winner_score, winner_elo, loser_team_id, loser_score, loser_elo, match_purse)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """, interaction.user.id, opposing_player.id, winner_team['team_id'], winner_score, winner_new_elo, loser_team['team_id'], loser_score, loser_new_elo, 0)

                # Update lts_teams table for both winner and loser
                await conn.execute("UPDATE lts_teams SET elo_rating = $1, matches_played = matches_played + 1, wins = wins + 1, last_activity = CURRENT_TIMESTAMP WHERE id = $2", winner_new_elo, winner_team['team_id'])
                await conn.execute("UPDATE lts_teams SET elo_rating = $1, matches_played = matches_played + 1, losses = losses + 1, last_activity = CURRENT_TIMESTAMP WHERE id = $2", loser_new_elo, loser_team['team_id'])

                # Calculate the change in ELO for display purposes
                winner_elo_change = winner_new_elo - winner_team['elo_rating']
                loser_elo_change = loser_new_elo - loser_team['elo_rating']
                winner_elo_change_formatted = f"(+{winner_elo_change})" if winner_elo_change > 0 else f"({winner_elo_change})"
                loser_elo_change_formatted = f"(-{-loser_elo_change})" if loser_elo_change < 0 else f"({loser_elo_change})"

                # Construct the description with match command and potential purse
                description_lines = [
                    f"`/submit_lts {winner_score} @{opposing_player.display_name} {loser_score}`",
                    f"**({winner_score}-{loser_score}) {winner_team['team_name']}** vs {loser_team['team_name']}"
                    # Additional lines for purse and other information can be added here
                ]

                # Create and send the updated embed
                updated_embed = discord.Embed(
                    title=f"LTS 3v3+ Team WIN: {winner_team['team_name']}",
                    description="\n".join(description_lines),
                    color=discord.Color.green()
                )
                updated_embed.add_field(name=f"{winner_team['team_name']}: {winner_new_elo} {winner_elo_change_formatted}", value=":medal:   :coin: 0", inline=True)  # Replace with actual tier emoji and purse if necessary
                updated_embed.add_field(name=f"{loser_team['team_name']}: {loser_new_elo} {loser_elo_change_formatted}", value=":medal:   :coin: 0", inline=True)  # Replace with actual tier emoji and purse if necessary
                updated_embed.set_footer(text=f"Submitted by {interaction.user.display_name}, Confirmed by {opposing_player.display_name}")
                updated_embed.timestamp = datetime.now()

                # Set the URL for the leaderboard (if applicable)
                updated_embed.url = "https://chivstats.xyz/leaderboards/ranked_teams/"  # Placeholder URL

                # Update the original message
                # After confirming the match and editing the original message
                await message.edit(embed=updated_embed)

                # Echo the updated result to other channels named 'chivstats-ranked'
                guild_names_sent_to = []
                for guild in bot.guilds:
                    channel = discord.utils.get(guild.text_channels, name="chivstats-ranked")
                    if channel and channel.id != interaction.channel.id:
                        try:
                            embed_copy = updated_embed.copy()
                            await channel.send(embed=embed_copy)
                            guild_names_sent_to.append(guild.name)
                        except Exception as e:
                            print(f"Failed to send message to {channel.name} in {guild.name}: {e}")

                if guild_names_sent_to:
                    audit_message = f"LTS match echoed to the following guilds: {', '.join(guild_names_sent_to)}"
                else:
                    audit_message = "LTS match was not echoed to any other guilds."

                await interaction.followup.send("Match result confirmed.", ephemeral=True)
            elif view.confirmed is False:
                # Logic for when the match is cancelled
                canceller_team_name = submitter_team['team_name'] if str(view.canceller.id) in submitter_team['roster'] else opposing_team['team_name']
                cancelled_embed = discord.Embed(
                    title=f"LTS 3v3+ Match Cancelled by {canceller_team_name}",
                    description=f"**{submitter_team['team_name']}** ({team_score}) vs **{opposing_team['team_name']}** ({opposing_team_score})",
                    color=discord.Color.red()
                )
                cancelled_embed.set_footer(text=f"Cancelled by {view.canceller.display_name}")
                cancelled_embed.timestamp = datetime.now()
                await message.edit(embed=cancelled_embed)
                await interaction.followup.send("Match result cancelled.", ephemeral=True)
            else:
                await interaction.followup.send("No response from the opposing player. The match confirmation has timed out.", ephemeral=True)
        else:
            await interaction.followup.send("An unexpected error occurred in the confirmation process.", ephemeral=True)

    except Exception as e:
        print(f"Error in submit_lts: {e}")
        if interaction.response.is_done():
            await interaction.followup.send(f"An error occurred while processing the match submission: {e}", ephemeral=True)
        else:
            await interaction.response.send_message(f"An error occurred while processing the match submission: {e}", ephemeral=True)
    finally:
        await close_db_connection(conn)


async def get_team_of_player(conn, discord_id):
    discord_id_str = str(discord_id)
    print(f"Fetching team for Discord ID: {discord_id_str}")

    query = """
    SELECT lts_teams.id, lts_teams.team_name, lts_teams.roster, lts_teams.elo_rating
    FROM lts_teams 
    WHERE roster @> $1::jsonb
    """

    team_info = await conn.fetchrow(query, json.dumps([discord_id_str]))
    print(f"Query result for team information: {team_info}")

    if team_info:
        team_data = {
            'team_id': team_info['id'],
            'team_name': team_info['team_name'],
            'roster': [str(member_id) for member_id in json.loads(team_info['roster'])],
            'elo_rating': team_info['elo_rating']
        }
        print(f"Returning team data: {team_data}")
        return team_data
    else:
        print("Player is not in any team.")
        return None


##########################
##########END LTS#########
##########################