import discord
from discord.ext import commands
import asyncpg
import json
from datetime import datetime
import asyncio
import re

DATABASE = "chivstats"
USER = "webchiv"
HOST = "/var/run/postgresql"

def calculate_elo(R, K, games_won, games_played, opponent_rating, c=400):
    expected_score = 1 / (1 + 10 ** ((opponent_rating - R) / c))
    actual_score = games_won / games_played
    new_rating = R + K * (actual_score - expected_score)
    return new_rating

async def calculate_duo_elo(team1_elo, team2_elo, team1_score, team2_score):
    # K factor for ELO calculation - may vary based on your requirements
    K = 32
    team1_new_elo = calculate_elo(team1_elo, K, team1_score > team2_score, 1, team2_elo)
    team2_new_elo = calculate_elo(team2_elo, K, team2_score > team1_score, 1, team1_elo)
    return team1_new_elo, team2_new_elo

def remove_mentions(text):
    # Regular expression pattern to match Discord user and role mentions
    pattern = r'<@!?[0-9]+>|<@&[0-9]+>'
    # Replace found mentions with an empty string
    return re.sub(pattern, '', text)

async def echo_to_guilds(bot, interaction, embed, echo_channel_name):
    origin_guild_name = interaction.guild.name
    guild_names_sent_to = []

    for guild in bot.guilds:
        echo_channel = discord.utils.get(guild.text_channels, name=echo_channel_name)
        if echo_channel and echo_channel.id != interaction.channel.id:
            try:
                embed_copy = embed.copy()

                # Use the custom remove_mentions function
                if embed_copy.title:
                    embed_copy.title = remove_mentions(embed_copy.title)
                if embed_copy.description:
                    embed_copy.description = remove_mentions(embed_copy.description)

                # Optionally, strip mentions from fields
                for field in embed_copy.fields:
                    field.name = remove_mentions(field.name)
                    field.value = remove_mentions(field.value)

                await echo_channel.send(embed=embed_copy)
                guild_names_sent_to.append(guild.name)
            except Exception as e:
                print(f"Failed to send message to {echo_channel.name} in {guild.name}: {e}")

    if guild_names_sent_to:
        audit_message = f"Message from {origin_guild_name} echoed to the following guilds: {', '.join(guild_names_sent_to)}"
    else:
        audit_message = f"Message from {origin_guild_name} was not echoed to any other guilds."
    return audit_message

class ConfirmationViewLTS(discord.ui.View):
    def __init__(self, bot, db_pool, submitter, opponent_team, team1_score, team2_score, team1_id, team2_id, team1_elo, team2_elo, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bot = bot
        self.db_pool = db_pool
        self.submitter = submitter
        self.opponent_team = opponent_team
        self.team1_score = team1_score
        self.team2_score = team2_score
        self.team1_id = team1_id
        self.team2_id = team2_id
        self.team1_elo = team1_elo
        self.team2_elo = team2_elo

        self.confirm_button = discord.ui.Button(label="Confirm", style=discord.ButtonStyle.green)
        self.confirm_button.callback = self.confirm_button_clicked
        self.add_item(self.confirm_button)

        self.deny_button = discord.ui.Button(label="Deny", style=discord.ButtonStyle.red)
        self.deny_button.callback = self.deny_button_clicked
        self.add_item(self.deny_button)

    async def confirm_button_clicked(self, interaction: discord.Interaction):
        async with self.db_pool.acquire() as conn:
            opponent_ids = [int(member_id) for member_id in json.loads(self.opponent_team['roster'])]
            if interaction.user.id not in opponent_ids:
                await interaction.response.send_message("You are not authorized to confirm this match.", ephemeral=True)
                return

            # Update the match to confirmed status
            await conn.execute("""
                UPDATE lts_matches
                SET confirmed = TRUE, confirming_discordid = $1
                WHERE submitting_discordid = $2 AND winner_team_id = $3 AND loser_team_id = $4
            """, interaction.user.id, self.submitter.id, self.team1_id, self.team2_id)

            # Fetch current ELOs directly before calculating new ELOs
            team1_elo = await conn.fetchval("SELECT elo_rating FROM lts_teams WHERE id = $1", self.team1_id)
            team2_elo = await conn.fetchval("SELECT elo_rating FROM lts_teams WHERE id = $1", self.team2_id)
            # Fetch team names and current ELOs for embed update
            team1_info = await conn.fetchrow("SELECT team_name, elo_rating FROM lts_teams WHERE id = $1", self.team1_id)
            team2_info = await conn.fetchrow("SELECT team_name, elo_rating FROM lts_teams WHERE id = $1", self.team2_id)
            team1_name = team1_info['team_name']
            team2_name = team2_info['team_name']
            
            # Calculate new ELO ratings for both teams based on the match outcome
            team1_new_elo, team2_new_elo = await calculate_duo_elo(team1_elo, team2_elo, self.team1_score, self.team2_score)
            
            # Determine winning and losing team names, scores, and ELOs
            if self.team1_score > self.team2_score:
                winning_team_name, winners_score, winner_new_elo = team1_name, self.team1_score, team1_new_elo
                losing_team_name, losers_score, loser_new_elo = team2_name, self.team2_score, team2_new_elo
            else:
                winning_team_name, winners_score, winner_new_elo = team2_name, self.team2_score, team2_new_elo
                losing_team_name, losers_score, loser_new_elo = team1_name, self.team1_score, team1_new_elo


            # Update database with new ELO ratings and match counts for both teams
            await conn.execute("""
                UPDATE lts_teams SET elo_rating = $1, matches_played = matches_played + 1, 
                wins = wins + $2, losses = losses + $3 WHERE id = $4
            """, team1_new_elo, 1 if self.team1_score > self.team2_score else 0, 1 if self.team1_score < self.team2_score else 0, self.team1_id)

            await conn.execute("""
                UPDATE lts_teams SET elo_rating = $1, matches_played = matches_played + 1, 
                wins = wins + $2, losses = losses + $3 WHERE id = $4
            """, team2_new_elo, 1 if self.team2_score > self.team1_score else 0, 1 if self.team2_score < self.team1_score else 0, self.team2_id)

            # Fetch the embed from the original message
            embed = interaction.message.embeds[0]
            
            # Prepare the embed with updated information
            # Prepare the updated embed with all necessary information
            embed = discord.Embed(
                title="LTS Match Confirmed",
                description=f"Winners: **{winning_team_name} ({winners_score})**\nLosers: {losing_team_name} ({losers_score})\n\n**New ELO Ratings:**\n- {winning_team_name}: {round(team1_new_elo)}\n- {losing_team_name}: {round(team2_new_elo)}",
                color=discord.Color.green()
            )
            embed.set_footer(text=f"Match submitted by {self.submitter.display_name}, confirmed by {interaction.user.display_name}")
            embed.timestamp = datetime.now()

            origin_channel_name = interaction.channel.name
            await echo_to_guilds(self.bot, interaction, embed, origin_channel_name)
            # Edit the message with the new embed and remove the view (buttons)
            await interaction.message.edit(embed=embed, view=None)


    async def deny_button_clicked(self, interaction: discord.Interaction):
        async with self.db_pool.acquire() as conn:
            # Fetch the rosters for both teams to check if the user is authorized
            submitting_team_roster = await conn.fetchval("SELECT roster FROM lts_teams WHERE id = $1", self.team1_id)
            opponent_team_roster = await conn.fetchval("SELECT roster FROM lts_teams WHERE id = $1", self.team2_id)
            
            # Ensure rosters are loaded correctly and convert JSON strings to Python lists
            submitting_team_roster = json.loads(submitting_team_roster) if submitting_team_roster else []
            opponent_team_roster = json.loads(opponent_team_roster) if opponent_team_roster else []

            # Convert roster IDs to integers for comparison
            combined_roster_ids = [int(member_id) for member_id in submitting_team_roster + opponent_team_roster]
            user_id = interaction.user.id

            # Check if the user is in either roster
            if user_id not in combined_roster_ids:
                await interaction.response.send_message("You are not authorized to deny this match.", ephemeral=True)
                return

            # If authorized, proceed to mark the match as denied or disputed
            await conn.execute("""
                UPDATE lts_matches
                SET confirmed = FALSE, confirming_discordid = $1
                WHERE submitting_discordid = $2 AND winner_team_id = $3 AND loser_team_id = $4
            """, user_id, self.submitter.id, self.team1_id, self.team2_id)
            
            embed = discord.Embed(title="LTS Match Denied",
                                description="The match result submission has been denied.",
                                color=discord.Color.red())
            embed.timestamp = datetime.now()
            embed.set_footer(text=f"Match submitted by {self.submitter.display_name}, denied by {interaction.user.display_name}")
            try:
                await interaction.message.edit(embed=embed, view=None)  # Attempt to edit the message to reflect denial

            except discord.NotFound:
                await interaction.followup.send("Unable to find the original message to edit. It may have been deleted.", ephemeral=True)


class LTSCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_details = {
            'database': DATABASE,
            'user': USER,
            'host': HOST
        }

    async def create_db_pool(self):
        self.bot.db_pool = await asyncpg.create_pool(**self.db_details)

    @commands.Cog.listener()
    async def on_ready(self):
        await self.create_db_pool()
        print("LTS Cog ready and DB pool created.")

    @commands.slash_command(name="submit_lts", description="Submit the result of an LTS match.")
    async def submit_lts(self, interaction: discord.Interaction, your_score: int, opponent_team_player: discord.Member, their_score: int):
        async with self.bot.db_pool.acquire() as conn:
            user_id_str = str(interaction.user.id)
            opponent_id_str = str(opponent_team_player.id)

            user_team_info = await conn.fetchrow(
                "SELECT id, team_name, elo_rating, roster FROM lts_teams WHERE roster @> $1::jsonb",
                json.dumps([user_id_str])
            )

            if not user_team_info or user_id_str not in json.loads(user_team_info['roster']):
                await interaction.response.send_message("You are not registered on any LTS team or not in the roster.", ephemeral=True)
                return

            opponent_team_info = await conn.fetchrow(
                "SELECT id, team_name, elo_rating, roster FROM lts_teams WHERE roster @> $1::jsonb",
                json.dumps([opponent_id_str])
            )

            if not opponent_team_info or opponent_id_str not in json.loads(opponent_team_info['roster']):
                await interaction.response.send_message(f"Opponent team for player {opponent_team_player.display_name} not found or the player is not on the roster.", ephemeral=True)
                return

            # Determine winner and loser based on score
            if your_score > their_score:
                winning_team_name, winners_score = user_team_info['team_name'], your_score
                losing_team_name, losers_score = opponent_team_info['team_name'], their_score
            else:
                winning_team_name, winners_score = opponent_team_info['team_name'], their_score
                losing_team_name, losers_score = user_team_info['team_name'], your_score

            # Prepare and send a confirmation message with the ConfirmationViewLTS
            view = ConfirmationViewLTS(
                bot=self.bot,
                db_pool=self.bot.db_pool,
                submitter=interaction.user,
                opponent_team=opponent_team_info,
                team1_score=your_score,
                team2_score=their_score,
                team1_id=user_team_info['id'],
                team2_id=opponent_team_info['id'],
                team1_elo=user_team_info['elo_rating'],
                team2_elo=opponent_team_info['elo_rating']
            )

            embed = discord.Embed(
                title="LTS(3v3+) Match Submission (UNVERIFIED)",
                description=f"Winners: {winning_team_name} ({winners_score})\nLosers: {losing_team_name} ({losers_score})",
                color=discord.Color.orange()
            ).set_footer(text=f"Match submitted by {interaction.user.display_name}")

            # Send the message to the channel, making it visible to everyone
            await interaction.response.send_message(embed=embed, view=view, ephemeral=False)


    @commands.slash_command(name="lts_register_team", description="Register a new LTS team.")
    async def lts_register_team(self, interaction: discord.Interaction, team_name: str):
        async with self.bot.db_pool.acquire() as conn:
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

                # Insert the new team into the lts_teams table
                initial_roster = json.dumps([user_id_str])
                await conn.execute("""
                    INSERT INTO lts_teams (team_name, team_owner, roster)
                    VALUES ($1, $2, $3::jsonb)
                    """, team_name, interaction.user.id, initial_roster)

                # Prepare the embed message for echoing
                embed = discord.Embed(title="New LTS Team Created", color=discord.Color.green())
                embed.add_field(name="Team Name", value=team_name, inline=False)
                
                # Check if the user has an avatar, if not, omit the icon_url
                if interaction.user.avatar:
                    embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.avatar.url)
                else:
                    embed.set_author(name=interaction.user.display_name)

                embed.set_footer(text="LTS Team Registration")

                # Echo the embed to the same channel name in all guilds
                origin_channel_name = interaction.channel.name
                await echo_to_guilds(self.bot, interaction, embed, origin_channel_name)

                # Also send the embed in the origin channel
                await interaction.response.send_message(embed=embed, ephemeral=False)

            except Exception as e:
                await interaction.response.send_message("An error occurred while creating the LTS team.", ephemeral=True)
                print(f"Error in lts_register_team: {e}")



    @commands.slash_command(name="lts_rename_team", description="Rename your LTS team.")
    async def lts_rename_team(self, interaction: discord.Interaction, new_team_name: str):
        async with self.bot.db_pool.acquire() as conn:
            try:
                # Check if the team name already exists
                existing_team_name = await conn.fetchval("SELECT id FROM lts_teams WHERE team_name = $1", new_team_name)
                if existing_team_name:
                    await interaction.response.send_message("This team name is already taken. Please choose a different name.", ephemeral=True)
                    return

                # Fetch the team where the user is the owner
                team = await conn.fetchrow("SELECT id, team_name FROM lts_teams WHERE team_owner = $1", interaction.user.id)
                if not team:
                    await interaction.response.send_message("You are not the owner of any LTS team.", ephemeral=True)
                    return

                # Update the team name
                await conn.execute("UPDATE lts_teams SET team_name = $1 WHERE id = $2", new_team_name, team['id'])

                # Prepare the embed message
                embed = discord.Embed(title="Team Name Changed", color=discord.Color.blue())
                embed.add_field(name="Change", value=f"{interaction.user.display_name} has changed their team name from **{team['team_name']}** to **{new_team_name}**.", inline=False)
                embed.set_footer(text="LTS Team Update")

                # Echo the embed to the same channel name in all guilds
                origin_channel_name = interaction.channel.name
                await echo_to_guilds(self.bot, interaction, embed, origin_channel_name)

                # Send the confirmation embed in the origin channel too, instead of sending an ephemeral message
                await interaction.response.send_message(embed=embed, ephemeral=False)

            except Exception as e:
                # If there's an error, send an ephemeral message instead
                await interaction.response.send_message("An error occurred while renaming the LTS team.", ephemeral=True)
                print(f"Error in lts_rename_team: {e}")



    @commands.slash_command(name="lts_leave_team", description="Leave your current LTS team.")
    async def lts_leave_team(self, interaction: discord.Interaction):
        async with self.bot.db_pool.acquire() as conn:
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


    @commands.slash_command(name="lts_add_teammate", description="Add a teammate to your LTS team.")
    async def lts_add_teammate(self, interaction: discord.Interaction, member: discord.Member):
        async with self.bot.db_pool.acquire() as conn:
            # Check if the command issuer is the owner of any team
            owner_team = await conn.fetchrow("SELECT id, roster FROM lts_teams WHERE team_owner = $1", interaction.user.id)
            if not owner_team:
                await interaction.response.send_message("You are not the owner of any LTS team.", ephemeral=True)
                return

            # Check if the targeted member is already in a team
            member_in_any_team = await conn.fetchval("SELECT id FROM lts_teams WHERE roster @> $1::jsonb", json.dumps([str(member.id)]))
            if member_in_any_team:
                await interaction.response.send_message(f"{member.display_name} is already a member of a team.", ephemeral=True)
                return

            # Add the targeted member to the owner's team roster
            try:
                updated_roster = json.loads(owner_team['roster'])
                updated_roster.append(str(member.id))  # Add new member ID as a string
                await conn.execute("UPDATE lts_teams SET roster = $1::jsonb WHERE id = $2", json.dumps(updated_roster), owner_team['id'])

                await interaction.response.send_message(f"{member.display_name} has been successfully added to your team.", ephemeral=True)
            except Exception as e:
                await interaction.response.send_message("An error occurred while adding a teammate.", ephemeral=True)
                print(f"Error in lts_add_teammate: {e}")


    @commands.slash_command(name="lts_remove_teammate", description="Remove a teammate from your LTS team.")
    async def lts_remove_teammate(self, interaction: discord.Interaction, member: discord.Member):
        async with self.bot.db_pool.acquire() as conn:
            # Check if the command issuer is the owner of any team
            owner_team = await conn.fetchrow("SELECT id, roster FROM lts_teams WHERE team_owner = $1", interaction.user.id)
            if not owner_team:
                await interaction.response.send_message("You are not the owner of any LTS team.", ephemeral=True)
                return

            # Convert the roster from JSON to a Python list
            roster = json.loads(owner_team['roster'])

            # Check if the targeted member is on the owner's team roster
            if str(member.id) not in roster:
                await interaction.response.send_message(f"{member.display_name} is not a member of your team.", ephemeral=True)
                return

            # Remove the targeted member from the team roster
            try:
                roster.remove(str(member.id))  # Remove member ID from roster
                await conn.execute("UPDATE lts_teams SET roster = $1::jsonb WHERE id = $2", json.dumps(roster), owner_team['id'])

                await interaction.response.send_message(f"{member.display_name} has been successfully removed from your team.", ephemeral=True)
            except Exception as e:
                await interaction.response.send_message("An error occurred while removing a teammate.", ephemeral=True)
                print(f"Error in lts_remove_teammate: {e}")


    @commands.slash_command(name="lts_list_teams", description="List all registered LTS teams.")
    async def lts_list_teams(self, interaction: discord.Interaction):
        async with self.bot.db_pool.acquire() as conn:
            # Fetch all teams ordered by ELO rating in descending order, including the team owner's ID
            teams = await conn.fetch("""
                SELECT team_name, elo_rating, team_owner FROM lts_teams ORDER BY elo_rating DESC
            """)

            if not teams:
                await interaction.response.send_message("There are currently no registered LTS teams.", ephemeral=True)
                return

            # Start constructing the embed
            embed = discord.Embed(title="Registered LTS Teams", description="Teams sorted by ELO ranking:", color=discord.Color.blue())

            # Loop through each team to add them to the embed
            for index, team in enumerate(teams, start=1):
                # Fetch the team owner's name using their ID
                owner = await self.bot.fetch_user(team['team_owner'])
                owner_name = owner.name if owner else "Unknown Owner"

                # Add the team to the embed with numbering and include the owner's name
                embed.add_field(name=f"{index}. {team['team_name']} ({owner_name})", value=f"ELO: {team['elo_rating']}", inline=False)

            # Check if the embed exceeds Discord's limits
            if len(embed) > 6000:  # Discord embed total character limit
                await interaction.response.send_message("The list of teams is too long to display in one message.", ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=False)

    @commands.slash_command(name="lts_search", description="Display details about a team by a team member.")
    async def lts_search(self, interaction: discord.Interaction, member: discord.Member):
        async with self.bot.db_pool.acquire() as conn:
            # Fetch team information including ELO rating, matches played, wins, and roster.
            team_info = await conn.fetchrow(
                """
                SELECT team_name, roster, team_owner, elo_rating, matches_played, wins
                FROM lts_teams
                WHERE roster @> $1::jsonb
                """,
                json.dumps([str(member.id)])
            )

            if not team_info:
                await interaction.response.send_message("No team found with the provided member.", ephemeral=True)
                return

            # Calculate ELO ranking position
            elo_ranking_position = await conn.fetchval(
                """
                SELECT COUNT(*) + 1
                FROM lts_teams
                WHERE elo_rating > $1
                """,
                team_info['elo_rating']
            )

            team_name = team_info["team_name"]
            roster = json.loads(team_info["roster"])
            team_owner_id = team_info["team_owner"]

            # Asynchronously fetch user names for the roster
            async def get_username(user_id):
                user = await self.bot.fetch_user(int(user_id))
                return user.name if user else "Unknown Member"

            roster_names = await asyncio.gather(*(get_username(member_id) for member_id in roster))
            
            # Ensure the team owner's name is listed first
            owner_index = roster.index(str(team_owner_id))
            owner_name = roster_names.pop(owner_index)
            #roster_names.insert(0, owner_name)

            # Create embed with team details
            embed = discord.Embed(title=f"Team: {team_name}", color=discord.Color.blue())
            embed.add_field(name="ELO Rating", value=str(round(team_info["elo_rating"])), inline=True)
            embed.add_field(name="Matches Played", value=str(team_info["matches_played"]), inline=True)
            embed.add_field(name="Wins", value=str(team_info["wins"]), inline=True)
            embed.add_field(name="ELO Ranking Position", value=str(elo_ranking_position), inline=True)
            embed.add_field(name="Team Owner", value=owner_name, inline=False)
            embed.add_field(name="Roster", value="\n".join(roster_names), inline=False)

            await interaction.response.send_message(embed=embed, ephemeral=False)




def setup(bot):
    bot.add_cog(LTSCog(bot))
