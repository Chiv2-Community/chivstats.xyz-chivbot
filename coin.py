import discord
from discord.ext import commands
import asyncpg
import json
from datetime import datetime

# Assuming DATABASE, USER, and HOST are defined as before
DATABASE = "chivstats"
USER = "webchiv"
HOST = "/var/run/postgresql"

class CoinCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # Ensure the database details are correctly set for the bot instance
        self.db_details = {
            'database': DATABASE,
            'user': USER,
            'host': HOST
        }

    async def update_house_account_balance(self, conn, amount):
        try:
            current_balance = await conn.fetchval("""
                SELECT balance FROM house_account 
                ORDER BY last_updated DESC LIMIT 1
            """)
            if current_balance is not None:
                new_balance = current_balance + amount
                await conn.execute("""
                    UPDATE house_account SET balance = $1, last_updated = CURRENT_TIMESTAMP
                    WHERE balance = $2
                """, new_balance, current_balance)
            else:
                await conn.execute("""
                    INSERT INTO house_account (balance, last_updated, payout_rate) 
                    VALUES ($1, CURRENT_TIMESTAMP, 5.00)
                """, amount)
        except Exception as e:
            print(f"Error updating house account balance: {e}")


    @commands.slash_command(name='coin_clown', description="Toggle clown status for a user at a cost")
    async def coin_clown_command(self, ctx: discord.ApplicationContext, member: discord.Member):
        cost = 50
        clown_emoji = "🤡"
        
        async with self.bot.db_pool.acquire() as conn:
            # Convert ctx.user.id to an integer
            user_id_int = int(ctx.user.id)

            user_coins = await conn.fetchval("""
                SELECT coins FROM ranked_players 
                WHERE discordid = $1 FOR UPDATE
            """, user_id_int)  # Use the converted integer ID here
            
            if user_coins < cost:
                await ctx.respond("You do not have enough coins.", ephemeral=True)
                return

            global_action = "clown" if clown_emoji not in member.display_name else "declown"
            new_balance = user_coins - cost
            await conn.execute("""
                UPDATE ranked_players SET coins = $1 
                WHERE discordid = $2
            """, new_balance, user_id_int)  # Use the converted integer ID here
            await self.update_house_account_balance(conn, cost)

            for guild in self.bot.guilds:
                try:
                    guild_member = await guild.fetch_member(member.id)
                    if guild_member and guild.me.guild_permissions.manage_nicknames:
                        new_nickname = f"{clown_emoji} {guild_member.display_name}" if global_action == "clown" else guild_member.display_name.replace(clown_emoji, "").strip()
                        await guild_member.edit(nick=new_nickname)
                except Exception:
                    continue  # Simplified error handling

            announcement = f"{member.mention} has been {global_action}ed globally by {ctx.user.mention}. Cost: {cost} coins."
            await ctx.channel.send(announcement)

            await ctx.respond(f"{member.display_name} has been {global_action}ed globally by you. Cost: {cost} coins. Your remaining balance is {new_balance} coins.", ephemeral=True)

    @commands.slash_command(name='coin_mass_declown', description="Remove the clown emoji from all users for 200 coins.")
    async def coin_mass_declown_command(self, ctx: discord.ApplicationContext):
        cost = 200  # Cost for the mass declowning
        clown_emoji = "🤡"

        async with self.bot.db_pool.acquire() as conn:
            # Fetch the user's current coin balance; ensure the user ID is passed as an integer
            user_coins = await conn.fetchval("""
                SELECT coins FROM ranked_players 
                WHERE discordid = $1 FOR UPDATE
            """, ctx.user.id)  # ctx.user.id is already an integer, so no need to convert it to a string

            if user_coins < cost:
                await ctx.respond("You do not have enough coins for this action.", ephemeral=True)
                return

            # Deduct the cost from the user's balance and update the database
            new_balance = user_coins - cost
            await conn.execute("""
                UPDATE ranked_players SET coins = $1 
                WHERE discordid = $2
            """, new_balance, ctx.user.id)  # Again, ensure ctx.user.id is passed as it is, without converting to a string

            declowned_count = 0
            for guild in self.bot.guilds:
                for member in guild.members:
                    if clown_emoji in member.display_name and guild.me.guild_permissions.manage_nicknames:
                        try:
                            new_nickname = member.display_name.replace(clown_emoji, "").strip()
                            await member.edit(nick=new_nickname)
                            declowned_count += 1
                        except Exception as e:
                            print(f"Failed to declown {member.display_name} in {guild.name}: {e}")
                            continue

            await ctx.respond(f"Mass declowning complete. {declowned_count} members have been declowned. Cost: {cost} coins. Your remaining balance is {new_balance} coins.", ephemeral=True)

    @commands.slash_command(name='coin_announce', description="Make a global announcement at a cost")
    async def coin_announce_command(self, ctx: discord.ApplicationContext, title: str, message_content: str):
        cost = 10  # Cost for the announcement
        await ctx.defer()

        channels_sent_to = 0  # Initialize the counter for the number of channels
        async with self.bot.db_pool.acquire() as conn:
            user_coins = await conn.fetchval("""
                SELECT coins FROM ranked_players 
                WHERE discordid = $1 FOR UPDATE
            """, str(ctx.user.id))

            if user_coins < cost:
                await ctx.followup.send("You do not have enough coins for this announcement.", ephemeral=True)
                return

            new_balance = user_coins - cost
            await conn.execute("""
                UPDATE ranked_players SET coins = $1 
                WHERE discordid = $2
            """, new_balance, str(ctx.user.id))

            await self.update_house_account_balance(conn, cost)

            echo_channel_name = 'chivstats-test' if ctx.channel.name == 'chivstats-test' else 'chivstats-ranked'
            embed = discord.Embed(title=title, description=message_content, color=discord.Color.yellow())
            embed.set_author(name=ctx.user.display_name, icon_url=ctx.user.display_avatar.url)

            for guild in self.bot.guilds:
                chivstats_channel = discord.utils.get(guild.text_channels, name=echo_channel_name)
                if chivstats_channel:
                    await chivstats_channel.send(embed=embed)
                    channels_sent_to += 1

            footer_text = f"{ctx.user.display_name} spent {cost} coins to send this to {channels_sent_to} channels ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
            embed.set_footer(text=footer_text)
            await ctx.followup.send(f"Your announcement has been sent to {channels_sent_to} channels.", ephemeral=True)

    # Initialization and DB pool setup
    @commands.Cog.listener()
    async def on_ready(self):
        if not hasattr(self.bot, 'db_pool'):
            self.bot.db_pool = await asyncpg.create_pool(database=DATABASE, user=USER, host=HOST)
            print("Database connection pool created for CoinCog")

def setup(bot):
    bot.add_cog(CoinCog(bot))