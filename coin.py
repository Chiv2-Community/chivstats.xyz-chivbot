# coin.py
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



async def update_house_account_balance(conn, amount):
    try:
        # Fetch the latest house balance or initialize if no entry exists
        house_balance = await conn.fetchval("SELECT balance FROM house_account ORDER BY last_updated DESC LIMIT 1")

        if house_balance is not None:
            house_balance += amount
        else:
            # Initialize house account with a zero balance if it's the first transaction
            house_balance = amount
            await conn.execute("INSERT INTO house_account (balance, last_updated, payout_rate) VALUES ($1, CURRENT_TIMESTAMP, 5.00)", house_balance)

        # Update house account with new balance
        await conn.execute("UPDATE house_account SET balance = $1", house_balance)
    except Exception as e:
        print(f"An error occurred while updating house account balance: {e}")

async def coin_clown_command(interaction: discord.Interaction, member: discord.Member, bot, update_house_account_balance, DATABASE, USER, HOST):
    cost = 50
    clown_emoji = "🤡"
    # Acknowledge the interaction immediately
    await interaction.response.defer(ephemeral=True)
    

    try:
        # Establish an asynchronous connection to the database
        conn = await asyncpg.connect(database=DATABASE, user=USER, host=HOST)

        # Check and fetch the user's coins and lock the row
        async with conn.transaction():
            user_coins = await conn.fetchval("SELECT coins FROM ranked_players WHERE discordid = $1 FOR UPDATE", interaction.user.id)

            if user_coins < cost:
                await interaction.followup.send("You do not have enough coins.", ephemeral=True)
                return

            # Determine the global action to be taken (clown or declown)
            global_action = "clown" if clown_emoji not in member.display_name else "declown"

            # Proceed with the coin transaction
            new_balance = user_coins - cost
            await conn.execute("UPDATE ranked_players SET coins = $1 WHERE discordid = $2", new_balance, interaction.user.id)

            # Update the house account balance
            await update_house_account_balance(conn, cost)

            # Iterate through all guilds to update the member's nickname
            for guild in bot.guilds:
                try:
                    guild_member = await guild.fetch_member(member.id)
                    if guild_member and guild.me.guild_permissions.manage_nicknames:
                        new_nickname = f"{clown_emoji} {guild_member.display_name}" if global_action == "clown" else guild_member.display_name.replace(clown_emoji, "").strip()
                        await guild_member.edit(nick=new_nickname)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass  # Handle exceptions silently

            # Announce the action in the origin channel with a ping
            origin_channel = interaction.channel
            origin_guild_id = interaction.guild.id
            if origin_channel:
                announcement = f"{member.mention} has been {global_action}ed globally by {interaction.user.mention}. Cost: {cost} coins."
                await origin_channel.send(announcement)

            # Announce the action in all other relevant channels without pinging
            for guild in bot.guilds:
                if guild.id != origin_guild_id:
                    chivstats_channel = discord.utils.get(guild.text_channels, name="chivstats-ranked")
                    if chivstats_channel:
                        # No pinging in the echoed message
                        announcement = f"{member.display_name} has been {global_action}ed globally. Cost: {cost} coins deposited to the house account by {interaction.user.display_name}."
                        await chivstats_channel.send(announcement)

            # Send confirmation to the user who invoked the command
            await interaction.followup.send(f"{member.display_name} has been {global_action}ed globally by you. Cost: {cost} coins. Your remaining balance is {new_balance} coins.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send("An error occurred while processing your request. Please try again.", ephemeral=True)
    finally:
        # Close the connection
        if conn:
            await conn.close()

async def coin_announce_command(bot, interaction: discord.Interaction, title: str, message_content: str, db_details):
    cost = 10  # Cost for the announcement
    await interaction.response.defer()

    database, user, host = db_details  # Unpack the database details
    channels_sent_to = 0  # Initialize the counter for the number of channels
    try:
        conn = await asyncpg.connect(database=database, user=user, host=host)

        # Fetch user's coin balance and verify they have enough coins
        user_coins = await conn.fetchval("SELECT coins FROM ranked_players WHERE discordid = $1", interaction.user.id)
        if user_coins < cost:
            await interaction.followup.send("You do not have enough coins for this announcement.", ephemeral=True)
            return

        # Deduct the cost from the user's account
        await conn.execute("UPDATE ranked_players SET coins = coins - $1 WHERE discordid = $2", cost, interaction.user.id)
        # Update house account balance
        await update_house_account_balance(conn, cost)
        echo_channel_name = 'chivstats-test' if interaction.channel.name == 'chivstats-test' else 'chivstats-ranked'
        # Prepare the announcement embed without the final footer
        embed = discord.Embed(
            title=title,
            description=message_content,
            color=discord.Color.yellow()
        )
        embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)

        # Initialize a variable to keep the message object of the origin channel
        origin_channel_message = None

        # Echo the message to other guilds based on the channel name
        for guild in bot.guilds:
            chivstats_channel = discord.utils.get(guild.text_channels, name=echo_channel_name)
            if chivstats_channel:
                sent_message = await chivstats_channel.send(embed=embed)
                channels_sent_to += 1  # Increment the counter for each successful send
                if guild.id == interaction.guild_id:  # Check if it's the origin guild
                    origin_channel_message = sent_message

        # Update the footer with the number of channels and cost
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Format current time
        footer_text = f"{interaction.user.display_name} spent {cost} coin to send this to {channels_sent_to} channels ({current_time})"
        embed.set_footer(text=footer_text)

        # Edit the original message in the origin channel with the updated embed
        if origin_channel_message:
            await origin_channel_message.edit(embed=embed)

        # Confirm the announcement to the user
        await interaction.followup.send(f"Your announcement has been sent to {channels_sent_to} channels.", ephemeral=True)

    except Exception as e:
        await interaction.followup.send("An error occurred while processing your request.", ephemeral=True)
        print(f"Error in coin_announce: {e}")
    finally:
        if conn:
            await conn.close()