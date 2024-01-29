# admin.py
from discord.ext import commands
import discord

# Define a set of administrative Discord IDs
ADMIN_USER_IDS = {
    230773943240228864,  # gimmic
    340925929679486976,  # codyno
    103639916243611648,  # KC
    255495056054550529,  # funk
    304408829544759297,  # snakeCase
}

def is_admin():
    async def predicate(ctx):
        return ctx.author.id in ADMIN_USER_IDS
    return commands.check(predicate)

class AdminCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.slash_command(name='admin_leave_guild_command', description="Leave a specified server.")
    @is_admin()
    async def admin_leave_guild_command(self, interaction, guild_id: int):
        try:
            guild = self.bot.get_guild(guild_id)
            if guild:
                await guild.leave()
                response = f"The bot has left the server: {guild.name} (ID: {guild_id})"
            else:
                response = "Server not found or the bot is not in that server."
        except Exception as e:
            response = f"An error occurred: {e}"
        await interaction.response.send_message(response)

    @commands.slash_command(name='admin_delete_command', description="Delete a specific message by its ID.")
    @is_admin()
    async def admin_delete_command(self, interaction, message_id: int):
        try:
            message = await ctx.channel.fetch_message(message_id)
            await message.delete()
            response = "Message deleted successfully."
        except discord.NotFound:
            response = "Message not found."
        except discord.Forbidden:
            response = "I don't have permissions to delete the message."
        except discord.HTTPException as e:
            response = f"Failed to delete message: {e}"
        await interaction.response.send_message(response)

    @commands.slash_command(name='admin_notice_command', description="Send an embedded update notice to all servers.")
    @is_admin()
    async def admin_notice_command(self, interaction, title: str, message: str):
        embed = discord.Embed(title=title, description=message, color=discord.Color.blue())
        channels_sent = 0
        for guild in self.bot.guilds:
            channel = discord.utils.get(guild.text_channels, name="chivstats-ranked")
            if channel:
                try:
                    await channel.send(embed=embed)
                    channels_sent += 1
                except Exception as e:
                    print(f"Failed to send message to {channel.name} in {guild.name}: {e}")
        await interaction.response.send_message(f"Notice sent to {channels_sent} channels.")

    @commands.slash_command(name='admin_register', description="Administratively correct user registration.")
    @is_admin()
    async def admin_register(self, interaction, member: discord.Member, playfabid: str):
        # Check if the command user is an admin
        if ctx.author.id not in ADMIN_USER_IDS:
            await interaction.response.send_message("You do not have permissions to use this command.", delete_after=10)
            return

        conn = await create_db_connection()  # Ensure you have this function defined or imported

        try:
            player_id = await conn.fetchval("SELECT id FROM players WHERE playfabid = $1", playfabid)
            if player_id is None:
                await interaction.response.send_message("The provided PlayFab ID does not exist in the players table.", delete_after=10)
                return

            existing_user = await conn.fetchrow("SELECT * FROM ranked_players WHERE discordid = $1", member.id)
            if existing_user:
                await conn.execute("UPDATE ranked_players SET playfabid = $1, player_id = $2 WHERE discordid = $3", playfabid, player_id, member.id)
                action = "updated with new PlayFab ID."
            else:
                await conn.execute("INSERT INTO ranked_players (playfabid, player_id, discordid, discord_username, retired) VALUES ($1, $2, $3, $4, FALSE)", playfabid, player_id, member.id, member.display_name)
                action = "registered and activated."

            roles_to_assign = ['Ranked Combatant', '1v1 pings', '2v2 pings']
            for role_name in roles_to_assign:
                role = discord.utils.get(ctx.guild.roles, name=role_name)
                if role:
                    await member.add_roles(role)
            await interaction.response.send_message(f"The PlayFab ID for {member.mention} has been {action} and roles have been assigned.")

        except Exception as e:
            await interaction.response.send_message("An error occurred while processing the request.")
            print(f"An error occurred: {e}")
        finally:
            await close_db_connection(conn)

def setup(bot):
    bot.add_cog(AdminCommands(bot))