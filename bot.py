# Telegram Message Scheduler Bot with Structured Conversation
# Language: Python
# Purpose: A bot for Telegram groups to allow admins (including anonymous) to schedule messages with a name, text, optional media (photos/videos), optional buttons, and time intervals (seconds or specific time).
# Dependencies: telethon, schedule, pymongo, python-dotenv
# Setup Instructions:
# 1. Install dependencies: `pip install telethon==1.36.0 schedule==1.2.2 pymongo==4.10.1 python-dotenv==1.0.1`
# 2. Create a .env file with:
#    API_ID=your_api_id
#    API_HASH=your_api_hash
#    BOT_TOKEN=your_bot_token
#    MONGODB_URI=mongodb://localhost:27017/
# 3. Obtain API_ID and API_HASH from https://my.telegram.org/apps.
# 4. Obtain BOT_TOKEN from BotFather.
# 5. Ensure MongoDB is running.
# 6. Add the bot to your Telegram group with permissions to send messages, photos, and videos.
# 7. Run: `python bot.py`
# Notes:
# - Check bot.log for debugging.
# - Test with regular and anonymous admins.
# - First run may prompt for authentication.

import telethon
from telethon import TelegramClient, events, types
from telethon.tl.custom import Button
from telethon.tl.types import InputMediaPhoto, InputMediaDocument
import schedule
import time
import pymongo
from datetime import datetime
import asyncio
import logging
import re
from bson import ObjectId
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration Section
CONFIG = {
    'API_ID': os.getenv('API_ID'),
    'API_HASH': os.getenv('API_HASH'),
    'BOT_TOKEN': os.getenv('BOT_TOKEN'),
    'MONGODB_URI': os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
    'MONGODB_DATABASE': 'telegram_scheduler',
    'MONGODB_COLLECTION': 'messages',
    'MONGODB_TIMEOUT_MS': 5000,
    'LOG_FILE': 'bot.log',
    'SCHEDULE_CHECK_INTERVAL_SECONDS': 1,
    'SESSION_NAME': 'bot_session'
}

# Set up logging
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
        raise SystemExit("MongoDB connection failed. Check MONGODB_URI in .env.")

# Bot class
class MessageSchedulerBot:
    def __init__(self, api_id, api_hash, bot_token):
        self.client = TelegramClient(CONFIG['SESSION_NAME'], api_id, api_hash)
        self.bot_token = bot_token
        self.collection = init_db()
        self.user_states = {}  # {user_id: {chat_id, state, data}}
        self.setup_handlers()

    def setup_handlers(self):
        @self.client.on(events.NewMessage(pattern='/start'))
        async def start(event):
            await self.handle_start(event)

        @self.client.on(events.NewMessage(pattern='/help'))
        async def help(event):
            await self.handle_help(event)

        @self.client.on(events.NewMessage(pattern='/schedule_message'))
        async def schedule_message(event):
            await self.handle_schedule_message_start(event)

        @self.client.on(events.NewMessage(pattern='/list'))
        async def list_schedules(event):
            try:
                await self.handle_list_schedules(event)
            except Exception as e:
                logger.error(f"Error in /list: {e}")
                await event.respond("An error occurred.")

        @self.client.on(events.NewMessage(pattern='/delete'))
        async def delete_schedule(event):
            try:
                await self.handle_delete_schedule(event)
            except Exception as e:
                logger.error(f"Error in /delete: {e}")
                await event.respond("An error occurred.")

        @self.client.on(events.NewMessage(pattern='/cancel'))
        async def cancel(event):
            try:
                await self.handle_cancel(event)
            except Exception as e:
                logger.error(f"Error in /cancel: {e}")
                await event.respond("An error occurred.")

        @self.client.on(events.NewMessage)
        async def handle_message(event):
            try:
                await self.handle_conversation(event)
            except Exception as e:
                logger.error(f"Error in conversation: {e}")
                await event.respond("An error occurred.")

    async def is_admin(self, user_id, chat_id):
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
        try:
            await event.respond(
                "Welcome to the Telegram Message Scheduler Bot!\n"
                "This bot allows group admins (including anonymous) to schedule messages.\n"
                "Key features:\n"
                "- Schedule messages with a name, text, optional media, and buttons.\n"
                "- Set repeating intervals (seconds) or specific times.\n"
                "- Only admins can schedule or delete messages.\n"
                "Commands:\n"
                "- /schedule_message: Set up a message (guided process).\n"
                "- /list: View all scheduled messages.\n"
                "- /delete <id>: Delete a scheduled message.\n"
                "- /help: Get detailed instructions.\n"
                "- /cancel: Cancel the scheduling process.\n"
                "Use /help for details."
            )
        except Exception as e:
            logger.error(f"Error in /start: {e}")
            await event.respond("An error occurred.")

    async def handle_help(self, event):
        try:
            await event.respond(
                "Telegram Message Scheduler Bot - Help\n"
                "This bot allows group admins to schedule messages.\n"
                "Steps to schedule a message:\n"
                "1. Use /schedule_message to start.\n"
                "2. Provide:\n"
                "   - Schedule name (e.g., 'Weekly Update').\n"
                "   - Message text (e.g., 'Team meeting at 2 PM').\n"
                "   - Media (photo/video, optional; type 'skip' to skip).\n"
                "   - Buttons (text|url, optional; type 'skip' to skip).\n"
                "   - Time interval (seconds for repeating, or YYYY-MM-DD HH:MM:SS for one-time).\n"
                "Example:\n"
                "- /schedule_message\n"
                "- Name: 'Daily Reminder'\n"
                "- Text: 'Check tasks!'\n"
                "- Media: Send photo or 'skip'\n"
                "- Buttons: 'Tasks|https://example.com' or 'skip'\n"
                "- Interval: '300' (every 300 seconds) or '2025-06-05 14:00:00'\n"
                "Commands:\n"
                "- /schedule_message: Start scheduling.\n"
                "- /list: Show scheduled messages.\n"
                "- /delete <id>: Delete a message (admin only).\n"
                "- /cancel: Cancel scheduling.\n"
                "Notes:\n"
                "- Only admins can use /schedule_message and /delete.\n"
                f"Check {CONFIG['LOG_FILE']} for issues."
            )
        except Exception as e:
            logger.error(f"Error in /help: {e}")
            await event.respond("An error occurred.")

    async def handle_schedule_message_start(self, event):
        chat_id = event.chat_id
        user_id = event.sender_id

        try:
            if not await self.is_admin(user_id, chat_id):
                await event.respond("Only group admins can schedule messages!")
                return

            self.user_states[user_id] = {
                'chat_id': chat_id,
                'state': 'SCHEDULE_NAME',
                'data': {
                    'chat_id': chat_id,
                    'schedule_name': None,
                    'message_text': None,
                    'schedule_time': None,
                    'interval_seconds': None,
                    'media_type': None,
                    'file_id': None,
                    'access_hash': None,
                    'buttons': []
                }
            }
            await event.respond("Please provide the schedule name (e.g., 'Weekly Update').")
        except Exception as e:
            logger.error(f"Error in /schedule_message start: {e}")
            await event.respond("An error occurred.")

    async def handle_conversation(self, event):
        user_id = event.sender_id
        chat_id = event.chat_id
        state_data = self.user_states.get(user_id)

        if not state_data or state_data['chat_id'] != chat_id:
            return

        try:
            if state_data['state'] == 'SCHEDULE_NAME':
                schedule_name = event.message.text.strip() if event.message.text else ""
                if not schedule_name:
                    await event.respond("Schedule name cannot be empty!")
                    return
                state_data['data']['schedule_name'] = schedule_name
                state_data['state'] = 'MESSAGE_TEXT'
                await event.respond("Please provide the message text (e.g., 'Team meeting at 2 PM').")

            elif state_data['state'] == 'MESSAGE_TEXT':
                message_text = event.message.text.strip() if event.message.text else ""
                if not message_text:
                    await event.respond("Message text cannot be empty!")
                    return
                state_data['data']['message_text'] = message_text
                state_data['state'] = 'MEDIA'
                await event.respond("Send a photo or video (optional), or type 'skip' to proceed.")

            elif state_data['state'] == 'MEDIA':
                if event.message.text and event.message.text.strip().lower() == 'skip':
                    state_data['state'] = 'BUTTONS'
                    await event.respond("Provide an inline button (text|url, e.g., 'Join|https://example.com'), or type 'skip' to proceed.")
                elif event.message.photo:
                    photo = event.message.photo
                    state_data['data']['media_type'] = 'photo'
                    state_data['data']['file_id'] = str(photo.id)
                    state_data['data']['access_hash'] = photo.access_hash
                    logger.info(f"Stored photo: file_id={photo.id}, access_hash={photo.access_hash}")
                    state_data['state'] = 'BUTTONS'
                    await event.respond("Photo received! Provide an inline button (text|url), or type 'skip' to proceed.")
                elif event.message.video:
                    video = event.message.video
                    state_data['data']['media_type'] = 'video'
                    state_data['data']['file_id'] = str(video.id)
                    state_data['data']['access_hash'] = video.access_hash
                    logger.info(f"Stored video: file_id={video.id}, access_hash={video.access_hash}")
                    state_data['state'] = 'BUTTONS'
                    await event.respond("Video received! Provide an inline button (text|url), or type 'skip' to proceed.")
                else:
                    await event.respond("Please send a photo/video or type 'skip'.")

            elif state_data['state'] == 'BUTTONS':
                text = event.message.text.strip() if event.message.text else ""
                if text.lower() == 'skip':
                    state_data['state'] = 'INTERVAL'
                    await event.respond("Enter the time interval in seconds (e.g., '300' for every 300 seconds) or a specific time (YYYY-MM-DD HH:MM:SS, e.g., '2025-06-05 14:00:00').")
                elif re.match(r'.+\|.+', text):
                    text, url = text.split('|', 1)
                    state_data['data']['buttons'].append({"text": text.strip(), "url": url.strip()})
                    await event.respond("Button added! Add another button (text|url) or type 'skip' to proceed.")
                else:
                    await event.respond("Invalid button format! Use text|url (e.g., 'Join|https://example.com') or type 'skip'.")

            elif state_data['state'] == 'INTERVAL':
                text = event.message.text.strip() if event.message.text else ""
                interval_seconds = None
                time_str = None
                try:
                    interval_seconds = int(text)
                    if interval_seconds <= 0:
                        await event.respond("Interval must be a positive number of seconds!")
                        return
                except ValueError:
                    try:
                        schedule_time = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
                        if schedule_time < datetime.now():
                            await event.respond("Cannot schedule messages in the past!")
                            return
                        time_str = text
                    except ValueError:
                        await event.respond("Invalid input! Enter a number of seconds (e.g., '300') or a time (YYYY-MM-DD HH:MM:SS).")
                        return

                state_data['data']['interval_seconds'] = interval_seconds
                state_data['data']['schedule_time'] = time_str

                result = self.collection.insert_one(state_data['data'])
                message_id = str(result.inserted_id)

                if interval_seconds:
                    schedule.every(interval_seconds).seconds.do(
                        self.send_scheduled_message, chat_id=chat_id, message_id=message_id
                    ).tag(f"message_{message_id}")
                    await event.respond(f"Message '{state_data['data']['schedule_name']}' (ID: {message_id}) scheduled to repeat every {interval_seconds} seconds.")
                else:
                    schedule.every().day.at(time_str.split()[1]).do(
                        self.send_scheduled_message, chat_id=chat_id, message_id=message_id
                    ).tag(f"message_{message_id}")
                    await event.respond(f"Message '{state_data['data']['schedule_name']}' (ID: {message_id}) scheduled for {time_str}.")

                del self.user_states[user_id]
        except Exception as e:
            logger.error(f"Error in conversation: {e}")
            await event.respond("An error occurred. Please try again.")

    async def handle_cancel(self, event):
        user_id = event.sender_id
        try:
            if user_id in self.user_states:
                del self.user_states[user_id]
                await event.respond("Scheduling cancelled.")
            else:
                await event.respond("No active scheduling process to cancel.")
        except Exception as e:
            logger.error(f"Error in /cancel: {e}")
            await event.respond("An error occurred.")

    def send_scheduled_message(self, chat_id, message_id):
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

                if message.get("file_id") and message.get("media_type") and message.get("access_hash"):
                    media = None
                    file_id = int(message["file_id"])
                    access_hash = message["access_hash"]
                    if message["media_type"] == "photo":
                        media = InputMediaPhoto(
                            id=types.InputPhoto(
                                id=file_id,
                                access_hash=access_hash,
                                file_reference=b''
                            )
                        )
                    elif message["media_type"] == "video":
                        media = InputMediaDocument(
                            id=types.InputDocument(
                                id=file_id,
                                access_hash=access_hash,
                                file_reference=b''
                            )
                        )
                    if media:
                        await self.client.send_message(
                            chat_id,
                            message["message_text"],
                            file=media,
                            buttons=keyboard
                        )
                        logger.info(f"Sent {message['media_type']} message {message_id}")
                    else:
                        logger.error(f"Invalid media type for message {message_id}")
                        return
                else:
                    await self.client.send_message(
                        chat_id,
                        message["message_text"],
                        buttons=keyboard
                    )
                    logger.info(f"Sent text message {message_id}")

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
        chat_id = event.chat_id

        try:
            messages = self.collection.find({"chat_id": chat_id, "sent": False})
            response = "Scheduled messages:\n"
            for msg in messages:
                time_info = f"Time: {msg['schedule_time']}" if msg.get("schedule_time") else f"Every {msg['interval_seconds']} seconds"
                media_info = f" | Media: {msg['media_type']}" if msg.get("media_type") else ""
                buttons_info = f" | Buttons: {', '.join([b['text'] for b in msg.get('buttons', [])])}" if msg.get("buttons") else ""
                response += f"ID: {msg['_id']} | Name: {msg['schedule_name']} | {time_info} | Message: {msg['message_text']}{media_info}{buttons_info}\n"

            if response == "Scheduled messages:\n":
                await event.respond("No scheduled messages.")
                return
            await event.respond(response)
        except Exception as e:
            logger.error(f"Error in /list: {e}")
            await event.respond("An error occurred.")

    async def handle_delete_schedule(self, event):
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
            await event.respond("An error occurred.")

    async def run(self):
        try:
            await self.client.start(bot_token=self.bot_token)
            logger.info("Bot started successfully")
            while True:
                schedule.run_pending()
                await asyncio.sleep(CONFIG['SCHEDULE_CHECK_INTERVAL_SECONDS'])
        except Exception as e:
            logger.error(f"Error in run loop: {e}")
            raise

# Main execution
if __name__ == "__main__":
    try:
        if not CONFIG['BOT_TOKEN'] or CONFIG['BOT_TOKEN'] == "YOUR_TELEGRAM_BOT_TOKEN":
            raise ValueError("BOT_TOKEN not configured in .env.")
        if not CONFIG['API_ID'] or not CONFIG['API_HASH']:
            raise ValueError("API_ID or API_HASH not configured in .env.")
        
        bot = MessageSchedulerBot(
            int(CONFIG['API_ID']),
            CONFIG['API_HASH'],
            CONFIG['BOT_TOKEN']
        )
        bot.client.loop.run_until_complete(bot.run())
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise SystemExit("Bot failed to start. Check logs for details.")
