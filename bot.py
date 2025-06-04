# Telegram Message Scheduler Bot with Telethon
# Language: Python
# Purpose: A bot for Telegram groups to allow admins (including anonymous) to schedule messages with text, media (photos/videos from the group), and inline buttons at specific times or intervals (in seconds).
# Dependencies: telethon, schedule, pymongo
# Setup Instructions:
# 1. Install dependencies: `pip install telethon schedule pymongo`
# 2. Configure variables in the CONFIG section below (API_ID, API_HASH, BOT_TOKEN, MONGODB_URI, etc.).
# 3. Obtain API_ID and API_HASH from https://my.telegram.org/apps.
# 4. Ensure MongoDB is running (local or cloud) before starting the bot.
# 5. Add the bot to your Telegram group with permissions to send messages, photos, and videos.
# 6. Run the script: `python bot.py`
# Notes:
# - Check bot.log for debugging information if issues arise.
# - Test with both regular and anonymous admins to verify functionality.
# - Ensure API_ID, API_HASH, and BOT_TOKEN are valid and MONGODB_URI is accessible.
# - First run may prompt for phone number and code to authenticate the bot.

import telethon
from telethon import TelegramClient, events, types
from telethon.tl.custom import Button
import schedule
import time
import pymongo
from datetime import datetime
import asyncio
import logging
import re
from bson import ObjectId
import os

# Configuration Section
CONFIG = {
    'API_ID': 12345678,  # Replace with your Telegram API ID from my.telegram.org
    'API_HASH': "your_api_hash",  # Replace with your Telegram API hash
    'BOT_TOKEN': "YOUR_TELEGRAM_BOT_TOKEN",  # Replace with your bot token from BotFather
    'MONGODB_URI': "mongodb://localhost:27017/",  # MongoDB connection string
    'MONGODB_DATABASE': "telegram_scheduler",
    'MONGODB_COLLECTION': "messages",
    'MONGODB_TIMEOUT_MS': 5000,
    'LOG_FILE': "bot.log",
    'SCHEDULE_CHECK_INTERVAL_SECONDS': 1,
    'SESSION_NAME': "bot_session"  # Telethon session file name
}

# Set up logging to console and file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(CONFIG['LOG_FILE']),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# MongoDB setup
def init_db():
    try:
        client = pymongo.MongoClient(
            CONFIG['MONGODB_URI'],
            serverSelectionTimeoutMS=CONFIG['MONGODB_TIMEOUT_MS']
        )
        client.admin.command('ping')
        db = client[CONFIG['MONGODB_DATABASE']]
        collection = db[CONFIG['MONGODB_COLLECTION']]
        logger.info("MongoDB connection established")
        return collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise SystemExit("MongoDB connection failed. Exiting.")

# Bot class to handle Telegram interactions and scheduling
class MessageSchedulerBot:
    def __init__(self, api_id, api_hash, bot_token):
        self.client = TelegramClient(
            CONFIG['SESSION_NAME'],
            api_id,
            api_hash
        )
        self.bot_token = bot_token
        self.collection = init_db()
        self.user_states = {}  # Track conversation states: {user_id: {chat_id, state, data}}
        self.setup_handlers()

    def setup_handlers(self):
        """Set up Telethon event handlers"""
        @self.client.on(events.NewMessage(pattern='/start'))
        async def start(event):
            await self.handle_start(event)

        @self.client.on(events.NewMessage(pattern='/help'))
        async def help(event):
            await self.handle_help(event)

        @self.client.on(events.NewMessage(pattern='/schedule_message'))
        async def schedule_message(event):
            await self.handle_schedule_message_start(event)

        @self.client.on(events.NewMessage(pattern='/time'))
        async def set_time(event):
            await self.handle_set_time(event)

        @self.client.on(events.NewMessage(pattern='/list'))
        async def list_schedules(event):
            await self.handle_list_schedules(event)

        @self.client.on(events.NewMessage(pattern='/delete'))
        async def delete_schedule(event):
            await self.handle_delete_schedule(event)

        @self.client.on(events.NewMessage(pattern='/cancel'))
        async def cancel(event):
            await self.handle_cancel(event)

        @self.client.on(events.NewMessage)
        async def handle_message(event):
            await self.handle_conversation(event)

    async def is_admin(self, user_id, chat_id):
        """Check if the user is an admin, including anonymous admins"""
        try:
            participant = await self.client.get_permissions(chat_id, user_id)
            is_admin = (
                participant.is_admin or
                participant.is_creator or
                getattr((await self.client.get_entity(user_id)), 'is_anonymous', False)
            )
            logger.debug(f"User {user_id} admin check: {is_admin}")
            return is_admin
        except Exception as e:
            logger.error(f"Error checking admin status for user {user_id}: {e}")
            return False

    async def handle_start(self, event):
        """Handle /start command"""
        try:
            await event.respond(
                "Welcome to the Telegram Message Scheduler Bot!\n"
                "This bot allows group admins (including anonymous admins) to schedule messages with text, photos, videos, and inline buttons.\n"
                "Key features:\n"
                "- Schedule messages for a specific time or repeating intervals (in seconds).\n"
                "- Include media uploaded in the group and buttons with URLs.\n"
                "- Only group admins can schedule or delete messages.\n"
                "Commands:\n"
                "- /schedule_message: Start setting up a message (text, media, buttons).\n"
                "- /time <id> <time or interval>: Set a specific time (YYYY-MM-DD HH:MM:SS) or interval (every X seconds).\n"
                "- /list: View all scheduled messages.\n"
                "- /delete <id>: Delete a scheduled message.\n"
                "- /help: Get detailed instructions and examples.\n"
                "- /cancel: Cancel the scheduling process.\n"
                "Use /help for detailed usage examples."
            )
        except Exception as e:
            logger.error(f"Error in /start: {e}")
            await event.respond("An error occurred. Please try again.")

    async def handle_help(self, event):
        """Handle /help command"""
        try:
            await event.respond(
                "Telegram Message Scheduler Bot - Help\n"
                "This bot is for group admins (including anonymous admins) to schedule messages in Telegram groups.\n"
                "Steps to schedule a message:\n"
                "1. Use /schedule_message to enter message text, then add optional media (photo/video) or buttons (text|url), and type 'done'.\n"
                "2. Use /time <id> to set the schedule (specific time or interval in seconds).\n"
                "\nCommands:\n"
                "- /schedule_message: Start a conversation to set up a message.\n"
                "  - Step 1: Enter message text (e.g., 'Meeting reminder').\n"
                "  - Step 2: Send a photo/video (optional), add buttons (e.g., 'Link|https://example.com'), or type 'done'.\n"
                "  - Example: /schedule_message -> 'Hello team!' -> send photo -> 'Register|https://example.com' -> 'done'\n"
                "- /time <id> <YYYY-MM-DD HH:MM:SS> or /time <id> every X seconds: Set the schedule for a message.\n"
                "  - Example: /time 1234567890abcdef12345678 2025-06-05 14:00:00\n"
                "  - Example: /time 1234567890abcdef12345678 every 300 seconds\n"
                "- /list: Show all scheduled messages with IDs, time/interval, text, media, and buttons.\n"
                "- /delete <id>: Delete a scheduled message (admin only).\n"
                "  - Example: /delete 1234567890abcdef12345678\n"
                "- /cancel: Cancel the /schedule_message process.\n"
                "\nNotes:\n"
                "- Only admins (including anonymous) can use /schedule_message, /time, and /delete.\n"
                "- Media must be photos or videos uploaded in the group.\n"
                "- Buttons are in the format 'text|url' (e.g., 'Visit|https://example.com').\n"
                f"Check {CONFIG['LOG_FILE']} for issues or contact your bot admin."
            )
        except Exception as e:
            logger.error(f"Error in /help: {e}")
            await event.respond("An error occurred. Please try again.")

    async def handle_schedule_message_start(self, event):
        """Start the /schedule_message conversation"""
        chat_id = event.chat_id
        user_id = event.sender_id

        try:
            if not await self.is_admin(user_id, chat_id):
                await event.respond("Only group admins can schedule messages!")
                return

            self.user_states[user_id] = {
                'chat_id': chat_id,
                'state': 'MESSAGE',
                'data': {
                    'chat_id': chat_id,
                    'message_text': None,
                    'schedule_time': None,
                    'interval_seconds': None,
                    'media_type': None,
                    'file_id': None,
                    'buttons': []
                }
            }
            await event.respond("Please provide the message text.")
        except Exception as e:
            logger.error(f"Error in /schedule_message start: {e}")
            await event.respond("An error occurred. Please try again.")

    async def handle_conversation(self, event):
        """Handle conversation steps for /schedule_message"""
        user_id = event.sender_id
        chat_id = event.chat_id
        state_data = self.user_states.get(user_id)

        if not state_data or state_data['chat_id'] != chat_id:
            return

        try:
            if state_data['state'] == 'MESSAGE':
                message_text = event.message.text.strip() if event.message.text else ""
                if not message_text:
                    await event.respond("Message text cannot be empty!")
                    return

                state_data['data']['message_text'] = message_text
                state_data['state'] = 'MEDIA_BUTTONS'
                await event.respond(
                    "Now send a photo or video (optional), or provide inline buttons (text|url, one per message) or type 'done'."
                )

            elif state_data['state'] == 'MEDIA_BUTTONS':
                if event.message.photo:
                    file_id = event.message.photo.id
                    state_data['data']['media_type'] = 'photo'
                    state_data['data']['file_id'] = str(file_id)
                    await event.respond("Photo received! Send another photo/video, buttons (text|url), or type 'done'.")

                elif event.message.video:
                    file_id = event.message.video.id
                    state_data['data']['media_type'] = 'video'
                    state_data['data']['file_id'] = str(file_id)
                    await event.respond("Video received! Send another photo/video, buttons (text|url), or type 'done'.")

                elif event.message.text:
                    text = event.message.text.strip()
                    if text.lower() == 'done':
                        result = self.collection.insert_one(state_data['data'])
                        message_id = str(result.inserted_id)
                        await event.respond(
                            f"Message saved with ID: {message_id}. Use /time {message_id} <YYYY-MM-DD HH:MM:SS> or /time {message_id} every X seconds to schedule it."
                        )
                        del self.user_states[user_id]
                    elif re.match(r'.+\|.+', text):
                        text, url = text.split('|', 1)
                        state_data['data']['buttons'].append({"text": text, "url": url})
                        await event.respond("Button added! Send another photo/video, more buttons, or type 'done'.")
                    else:
                        await event.respond("Invalid button format! Use text|url or type 'done'.")
        except Exception as e:
            logger.error(f"Error in conversation: {e}")
            await event.respond("An error occurred. Please try again.")

    async def handle_set_time(self, event):
        """Handle /time command to set specific time or interval"""
        chat_id = event.chat_id
        user_id = event.sender_id
        args = event.message.text.split()[1:] if event.message.text else []

        try:
            if not await self.is_admin(user_id, chat_id):
                await event.respond("Only group admins can set schedules!")
                return

            if len(args) < 2:
                await event.respond(
                    "Usage:\n"
                    "- /time <id> YYYY-MM-DD HH:MM:SS\n"
                    "- /time <id> every X seconds"
                )
                return

            message_id = args[0]
            message = self.collection.find_one({"_id": ObjectId(message_id), "chat_id": chat_id})
            if not message:
                await event.respond("Message ID not found or does not belong to this group!")
                return

            interval_seconds = None
            time_str = None

            if args[1].lower() == "every" and len(args) >= 4 and args[3].lower() == "seconds":
                try:
                    interval_seconds = int(args[2])
                    if interval_seconds <= 0:
                        await event.respond("Interval must be a positive number of seconds!")
                        return
                except ValueError:
                    await event.respond("Invalid interval! Use a number, e.g., 'every 300 seconds'")
                    return
            else:
                time_str = " ".join(args[1:3])
                try:
                    schedule_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    if schedule_time < datetime.now():
                        await event.respond("Cannot schedule messages in the past!")
                        return
                except ValueError:
                    await event.respond("Invalid time format! Use YYYY-MM-DD HH:MM:SS or 'every X seconds'")
                    return

            update_data = {
                "schedule_time": time_str,
                "interval_seconds": interval_seconds,
                "sent": False
            }
            self.collection.update_one({"_id": ObjectId(message_id)}, {"$set": update_data})

            schedule.clear(f"message_{message_id}")

            if interval_seconds:
                schedule.every(interval_seconds).seconds.do(
                    self.send_scheduled_message, chat_id=chat_id, message_id=message_id
                ).tag(f"message_{message_id}")
                await event.respond(f"Message {message_id} scheduled to repeat every {interval_seconds} seconds")
            else:
                schedule.every().day.at(time_str.split()[1]).do(
                    self.send_scheduled_message, chat_id=chat_id, message_id=message_id
                ).tag(f"message_{message_id}")
                await event.respond(f"Message {message_id} scheduled for {time_str}")
        except Exception as e:
            logger.error(f"Error in /time: {e}")
            await event.respond("An error occurred. Please try again.")

    async def handle_cancel(self, event):
        """Cancel the scheduling process"""
        user_id = event.sender_id
        try:
            if user_id in self.user_states:
                del self.user_states[user_id]
                await event.respond("Scheduling cancelled.")
            else:
                await event.respond("No active scheduling process to cancel.")
        except Exception as e:
            logger.error(f"Error in /cancel: {e}")
            await event.respond("An error occurred. Please try again.")

    def send_scheduled_message(self, chat_id, message_id):
        """Send the scheduled message to the chat"""
        async def send_message():
            try:
                message = self.collection.find_one({"_id": ObjectId(message_id)})
                if not message or message.get("sent"):
                    return

                buttons = []
                if message.get("buttons"):
                    for btn in message["buttons"]:
                        buttons.append([Button.url(btn["text"], btn["url"])])
                keyboard = buttons if buttons else None

                if message.get("file_id") and message.get("media_type"):
                    if message["media_type"] == "photo":
                        await self.client.send_file(
                            chat_id,
                            message["file_id"],
                            caption=message["message_text"],
                            buttons=keyboard
                        )
                    elif message["media_type"] == "video":
                        await self.client.send_file(
                            chat_id,
                            message["file_id"],
                            caption=message["message_text"],
                            buttons=keyboard
                        )
                else:
                    await self.client.send_message(
                        chat_id,
                        message["message_text"],
                        buttons=keyboard
                    )

                if not message.get("interval_seconds"):
                    self.collection.update_one({"_id": ObjectId(message_id)}, {"$set": {"sent": True}})
            except Exception as e:
                logger.error(f"Error sending message {message_id}: {e}")

        asyncio.run_coroutine_threadsafe(send_message(), self.client.loop)
        message = self.collection.find_one({"_id": ObjectId(message_id)})
        if message and not message.get("interval_seconds"):
            return schedule.CancelJob
        return None

    async def handle_list_schedules(self, event):
        """List all scheduled messages for the chat"""
        chat_id = event.chat_id

        try:
            messages = self.collection.find({"chat_id": chat_id, "sent": False})
            response = "Scheduled messages:\n"
            for msg in messages:
                time_info = f"Time: {msg['schedule_time']}" if msg.get("schedule_time") else f"Every {msg['interval_seconds']} seconds"
                media_info = f" | Media: {msg['media_type']}" if msg.get("media_type") else ""
                buttons_info = f" | Buttons: {', '.join([b['text'] for b in msg.get('buttons', [])])}" if msg.get("buttons") else ""
                response += f"ID: {msg['_id']} | {time_info} | Message: {msg['message_text']}{media_info}{buttons_info}\n"

            if response == "Scheduled messages:\n":
                await event.respond("No scheduled messages.")
                return
            await event.respond(response)
        except Exception as e:
            logger.error(f"Error in /list: {e}")
            await event.respond("An error occurred. Please try again.")

    async def handle_delete_schedule(self, event):
        """Delete a scheduled message by ID"""
        chat_id = event.chat_id
        user_id = event.sender_id
        args = event.message.text.split()[1:] if event.message.text else []

        try:
            if not await self.is_admin(user_id, chat_id):
                await event.respond("Only group admins can delete messages!")
                return

            if not args:
                await event.respond("Usage: /delete <id>")
                return

            msg_id = args[0]
            result = self.collection.delete_one({"_id": ObjectId(msg_id), "chat_id": chat_id, "sent": False})
            if result.deleted_count == 0:
                await event.respond("Message ID not found or already sent!")
                return

            schedule.clear(f"message_{msg_id}")
            await event.respond(f"Scheduled message {msg_id} deleted.")
        except Exception as e:
            logger.error(f"Error in /delete: {e}")
            await event.respond("An error occurred. Please try again.")

    async def run(self):
        """Start the bot and schedule loop"""
        try:
            await self.client.start(bot_token=self.bot_token)
            logger.info("Bot started successfully")
            loop = asyncio.get_event_loop()
            while True:
                schedule.run_pending()
                await asyncio.sleep(CONFIG['SCHEDULE_CHECK_INTERVAL_SECONDS'])
        except Exception as e:
            logger.error(f"Error in run loop: {e}")
            raise

# Main execution
if __name__ == "__main__":
    try:
        if CONFIG['BOT_TOKEN'] == "YOUR_TELEGRAM_BOT_TOKEN":
            raise ValueError("BOT_TOKEN not configured. Update CONFIG['BOT_TOKEN'].")
        if CONFIG['API_ID'] == 12345678 or CONFIG['API_HASH'] == "your_api_hash":
            raise ValueError("API_ID or API_HASH not configured. Update CONFIG['API_ID'] and CONFIG['API_HASH'].")
        
        bot = MessageSchedulerBot(
            CONFIG['API_ID'],
            CONFIG['API_HASH'],
            CONFIG['BOT_TOKEN']
        )
        bot.client.loop.run_until_complete(bot.run())
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise SystemExit("Bot failed to start. Check logs for details.")
