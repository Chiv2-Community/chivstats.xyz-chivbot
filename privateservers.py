#privateservers.py
import json
import discord
from discord.ext import commands

class PrivateServers(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def read_current_games(self):
        """Reads and parses the /tmp/currentgames file."""
        try:
            with open('/tmp/currentgames', 'r') as file:
                data = json.load(file)
                return data
        except Exception as e:
            print(f"Failed to read or parse /tmp/currentgames: {e}")
            return None

    def extract_servers(self, data):
        """Extracts server information from parsed data."""
        if not data or 'Data' not in data or 'Games' not in data['Data']:
            return []

        servers = data['Data']['Games']
        return servers

    @commands.command(name='syncslashcommands', hidden=True)
    @commands.is_owner()
    async def sync_slash_commands(self, ctx):
        await self.bot.tree.sync()
        await ctx.send("Slash commands have been synced!", delete_after=10)

    @commands.Cog.listener()
    async def on_ready(self):
        print("Private Server List Cog loaded.")

    @commands.slash_command(name="bocoboco", description="Test command to verify slash commands are working.")
    async def bocoboco(self, interaction: discord.Interaction):
        # Sending an ephemeral message back to the user
        await interaction.response.send_message("Slash command test successful! This is an ephemeral message.", ephemeral=True)

    @commands.slash_command(name="listservers", description="Lists private game servers with players.")
    async def listservers(self, interaction: discord.Interaction):
        data = await self.read_current_games()
        servers = self.extract_servers(data)

        # Filter out servers without players
        servers_with_players = [server for server in servers if server.get('PlayerUserIds') and len(server['PlayerUserIds']) > 0]

        if not servers_with_players:
            await interaction.response.send_message("No private servers with players found.", ephemeral=True)
            return

        embed = discord.Embed(title="Private Game Servers with Players", description=f"Found {len(servers_with_players)} servers with players.", color=discord.Color.blue())
        for server in servers_with_players[:25]:  # Still limit to 25 servers to avoid hitting embed limits
            server_name = server['Tags'].get('ServerName_s', 'Unknown')
            player_count = len(server['PlayerUserIds'])
            map_name = server['Tags'].get('MapName_s', 'Unknown')
            embed.add_field(name=server_name, value=f"Map: {map_name}, Players: {player_count}", inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=False)

def setup(bot):
    bot.add_cog(PrivateServers(bot))
