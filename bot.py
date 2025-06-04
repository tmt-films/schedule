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
from functools import wraps # For decorator
from urllib.parse import urlparse # Added for URL validation
# telethon.tl.custom.Button is already imported via `from telethon import ... Button` indirectly if used
# but explicit can be good: from telethon.tl.custom import Button

load_dotenv()
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
    'SESSION_NAME': 'bot_session',
    'FORCESUB_CHANNELS_RAW': os.getenv('FORCESUB_CHANNELS', ''), # New raw config
    'FORCESUB_CHANNELS_LIST': [] # New parsed list, initialized empty
}

# Parse FORCESUB_CHANNELS_RAW into FORCESUB_CHANNELS_LIST
if CONFIG['FORCESUB_CHANNELS_RAW']:
    raw_channels = CONFIG['FORCESUB_CHANNELS_RAW'].split(',')
    CONFIG['FORCESUB_CHANNELS_LIST'] = [
        ch.strip() for ch in raw_channels if ch.strip() # Ensure no empty strings if input is like ",channel,"
    ]

# Decorator Definition
def require_subscription(func):
    @wraps(func)
    async def wrapper(self, event, *args, **kwargs):
        # `self` is the MessageSchedulerBot instance
        # `event` is the Telethon event object

        # Ensure event has sender_id; typically true for messages bot acts upon.
        # For channel posts without sender_id, this check might not be relevant,
        # but commands usually come from users.
        if not hasattr(event, 'sender_id') or not event.sender_id:
            logger.warning("Could not get valid sender_id from event for fsub check. Allowing.")
            return await func(self, event, *args, **kwargs)

        user_id = event.sender_id
        # is_user_subscribed now returns: tuple[bool, list[dict]]
        is_subscribed, missing_channel_details_list = await self.is_user_subscribed(user_id)

        if not is_subscribed:
            if not missing_channel_details_list: # Defensive check
                logger.error(f"User {user_id} failed fsub check (is_subscribed=False) but no missing channels details listed. This might indicate an issue in is_user_subscribed.")
                await event.respond("There was an issue verifying your channel subscription. Please try again later.")
                return

            response_message = "To use this bot, you are required to subscribe to the following channel(s):\n\n"
            generated_buttons = [] # List of button rows

            for channel_detail in missing_channel_details_list:
                # Use title (which defaults to identifier if title fetch failed), then username if available
                title = channel_detail.get('title', str(channel_detail.get('identifier', 'Unknown Channel')))
                username = channel_detail.get('username')

                response_message += f"- **{title}**" # Use markdown for bold title
                if username:
                    response_message += f" (@{username})\n"
                    # Create a button row for this specific channel
                    generated_buttons.append(
                        [Button.url(f"Join {title}", f"https://t.me/{username}")]
                    )
                else:
                    # If no username, it might be a private channel or public by ID without a direct link.
                    response_message += " (Please find and join this channel if a link isn't provided elsewhere)\n"

            response_message += "\nOnce subscribed to all, please try your command again or use the button below."

            # Add the "Retry" button as the last row
            generated_buttons.append(
                [Button.inline("✅ I've Joined / Retry Command", data="fsub_retry")]
            )

            try:
                await event.respond(response_message, buttons=generated_buttons, link_preview=False)
            except Exception as e_resp:
                logger.error(f"Error sending fsub required message to user {user_id}: {e_resp}", exc_info=True)
            return  # Stop further processing of the original command

        # User is subscribed, proceed with the original function
        return await func(self, event, *args, **kwargs)
    return wrapper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(CONFIG['LOG_FILE']),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info(f"Force subscription channels loaded. Raw: '{CONFIG['FORCESUB_CHANNELS_RAW']}', Parsed: {CONFIG['FORCESUB_CHANNELS_LIST']}")

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

class MessageSchedulerBot:
    def __init__(self, api_id, api_hash, bot_token):
        self.client = TelegramClient(CONFIG['SESSION_NAME'], api_id, api_hash)
        self.bot_token = bot_token
        self.collection = init_db()
        self.user_states = {}  # {user_id: {chat_id, state, data}}
        # Note: load_and_reschedule_jobs should be called after client is ready,
        # potentially in run() or after client.start(). For now, just defining the method.
        self.setup_handlers()

    async def load_and_reschedule_jobs(self):
        logger.info("Attempting to load and reschedule pending jobs from database...")

        query = {
            "$or": [
                { "interval_seconds": { "$exists": True, "$ne": None } },
                {
                    "schedule_time": { "$exists": True, "$ne": None },
                    "$or": [
                        { "sent": False },
                        { "sent": { "$exists": False } }
                    ]
                }
            ]
        }

        pending_jobs = self.collection.find(query)
        rescheduled_count = 0

        for job_doc in pending_jobs:
            chat_id = job_doc.get('chat_id')
            # Ensure message_db_id is a string, as used in send_scheduled_message tags and calls
            message_db_id = str(job_doc.get('_id'))

            if not chat_id: # message_db_id should always exist if job_doc comes from MongoDB with _id
                logger.error(f"Skipping reload of job with DB ID {message_db_id} due to missing chat_id.")
                continue

            interval = job_doc.get('interval_seconds')
            time_str = job_doc.get('schedule_time')

            job_rescheduled_successfully = False
            if interval and isinstance(interval, (int, float)) and interval > 0:
                try:
                    schedule.every(interval).seconds.do(
                        self.send_scheduled_message, chat_id=chat_id, message_id=message_db_id
                    ).tag(f"message_{message_db_id}")
                    logger.info(f"Reloaded recurring job for DB ID {message_db_id}, chat {chat_id}, every {interval}s.")
                    rescheduled_count += 1
                    job_rescheduled_successfully = True
                except Exception as e_reschedule_interval:
                    logger.error(f"Failed to reschedule recurring job DB ID {message_db_id}: {e_reschedule_interval}", exc_info=True)

            elif time_str:
                try:
                    scheduled_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    if scheduled_dt < datetime.now():
                        logger.info(f"Skipping reload of one-time job DB ID {message_db_id} for chat {chat_id} as its schedule time {time_str} is in the past.")
                        # Optionally mark as sent if it should have been, and if it's not already.
                        if not job_doc.get("sent"):
                             self.collection.update_one({"_id": ObjectId(message_db_id)}, {"$set": {"sent": True}})
                             logger.info(f"Marked past one-time job DB ID {message_db_id} as sent.")
                        continue

                    schedule_time_only = scheduled_dt.strftime("%H:%M:%S") # Use strftime for safety
                    schedule.every().day.at(schedule_time_only).do(
                        self.send_scheduled_message, chat_id=chat_id, message_id=message_db_id
                    ).tag(f"message_{message_db_id}")
                    logger.info(f"Reloaded one-time job for DB ID {message_db_id}, chat {chat_id}, at {time_str}.")
                    rescheduled_count += 1
                    job_rescheduled_successfully = True
                except ValueError:
                    logger.error(f"Invalid time format '{time_str}' for job DB ID {message_db_id}. Skipping reschedule.")
                except Exception as e_reschedule_specific:
                    logger.error(f"Failed to reschedule one-time job DB ID {message_db_id}: {e_reschedule_specific}", exc_info=True)

            if not job_rescheduled_successfully and not (interval and isinstance(interval, (int, float)) and interval > 0) and not time_str :
                 logger.warning(f"Job DB ID {message_db_id} (chat {chat_id}) has neither valid interval nor schedule_time. Skipping.")

        logger.info(f"Successfully reloaded and rescheduled {rescheduled_count} jobs.")


    async def is_user_subscribed(self, user_id: int) -> tuple[bool, list[dict]]:
        """
        Checks if a user is subscribed to all channels specified in CONFIG['FORCESUB_CHANNELS_LIST'].

        Args:
            user_id: The ID of the user to check.

        Returns:
            A tuple: (is_subscribed_to_all, list_of_missing_channel_details)
            `is_subscribed_to_all` is True if the user is subscribed to all required channels
            or if no channels are configured.
            `list_of_missing_channel_details` contains dictionaries with details of channels
            the user is not subscribed to (identifier, title, username).
        """
        required_channels = CONFIG['FORCESUB_CHANNELS_LIST']
        if not required_channels:
            logger.debug("No force subscription channels configured. Skipping check.")
            return True, []

        missing_subscriptions_details = [] # New list name
        logger.debug(f"Checking fsub for user {user_id} against channels: {required_channels}")

        for channel_identifier in required_channels:
            is_member = False
            # Default channel_info, using identifier as fallback for title
            channel_info = {
                'identifier': str(channel_identifier), # Store original identifier
                'title': str(channel_identifier),      # Fallback title
                'username': None                       # Default username
            }

            try:
                target_channel_entity = await self.client.get_entity(channel_identifier)
                # Attempt to populate title and username from the fetched entity
                channel_info['title'] = getattr(target_channel_entity, 'title', str(channel_identifier))
                if hasattr(target_channel_entity, 'username') and target_channel_entity.username:
                    channel_info['username'] = target_channel_entity.username

                permissions = await self.client.get_permissions(target_channel_entity, user_id)

                # Refined membership check logic
                if permissions:
                    # Explicit flags take precedence
                    if permissions.is_member or permissions.is_creator or permissions.is_admin:
                        is_member = True
                    # Fallback: if user can view messages (common for members in public channels/groups)
                    elif hasattr(permissions, 'view_messages') and permissions.view_messages:
                         is_member = True
                    # The isinstance check for specific participant types can be added here if needed,
                    # for now, is_member, is_creator, is_admin, and view_messages cover many cases.
                    # Example: or isinstance(permissions, (types.ChannelParticipantAdmin, types.ChannelParticipantCreator, types.ChannelParticipant))

            except telethon.errors.rpcerrorlist.UserNotParticipantError:
                is_member = False
                logger.debug(f"User {user_id} is NOT a participant in channel '{channel_identifier}' (UserNotParticipantError). Fetched info: {channel_info}")
            except (ValueError, telethon.errors.rpcerrorlist.ChannelPrivateError, telethon.errors.rpcerrorlist.ChatAdminRequiredError) as e_entity:
                # Handles malformed identifiers, or bot can't access channel (e.g., private, bot kicked)
                logger.error(f"Could not resolve or access channel entity '{channel_identifier}' for fsub check: {e_entity}. Using identifier as title. User treated as not subscribed.")
                # is_member remains False, channel_info uses defaults (identifier as title)
            except Exception as e:
                logger.error(f"Unexpected error checking subscription for user {user_id} in channel '{channel_identifier}' (entity info: {channel_info}): {e}", exc_info=True)
                is_member = False # Default to not subscribed on unexpected error for safety.

            if not is_member:
                missing_subscriptions_details.append(channel_info)

        if not missing_subscriptions_details:
            logger.info(f"User {user_id} IS SUBSCRIBED to all required channels.")
            return True, []
        else:
            logger.info(f"User {user_id} is MISSING SUBSCRIPTIONS for channels: {missing_subscriptions_details}")
            return False, missing_subscriptions_details

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

        @self.client.on(events.NewMessage(pattern='/stop'))
        async def stop_schedule(event):
            try:
                await self.handle_stop_schedule(event)
            except Exception as e:
                logger.error(f"Error in /stop: {e}")
                await event.respond("An error occurred.")

        @self.client.on(events.NewMessage(pattern='/cancel'))
        async def cancel(event):
            try:
                await self.handle_cancel(event)
            except Exception as e:
                logger.error(f"Error in /cancel: {e}")
                await event.respond("An error occurred.")

        @self.client.on(events.CallbackQuery)
        async def handle_callback(event):
            try:
                await self.handle_button_click(event)
            except Exception as e:
                logger.error(f"Error in callback: {e}")
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

    @require_subscription
    async def handle_start(self, event):
        try:
            await event.respond(
                "Welcome to the Telegram Message Scheduler Bot!\n"
                "This bot allows group admins (including anonymous) to schedule messages.\n"
                "Key features:\n"
                "- Schedule messages with a name, text, optional media, and buttons.\n"
                "- Set repeating intervals (seconds) or specific times.\n"
                "- Only admins can schedule or stop messages.\n"
                "Commands:\n"
                "- /schedule_message: Set up a message (guided process).\n"
                "- /list: View all scheduled messages as buttons.\n"
                "- /stop: Stop a scheduled message (admin only).\n"
                "- /cancel: Cancel the scheduling process.\n"
                "- /help: Get detailed instructions.\n"
                "Use /help for details."
            )
        except Exception as e:
            logger.error(f"Error in /start: {e}")
            await event.respond("An error occurred.")

    @require_subscription
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
                "- /list: Show scheduled messages as buttons.\n"
                "- /stop: Stop a scheduled message (admin only).\n"
                "- /cancel: Cancel scheduling.\n"
                "Notes:\n"
                "- Only admins can use /schedule_message and /stop.\n"
                f"Check {CONFIG['LOG_FILE']} for issues."
            )
        except Exception as e:
            logger.error(f"Error in /help: {e}")
            await event.respond("An error occurred.")

    @require_subscription
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
                logger.info(f"Schedule name set for user {user_id}: {schedule_name}") # Added log
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
                elif re.match(r'.+\|.+', text): # Original text is 'text' from event.message.text.strip()
                    button_text_content, button_url_str = text.split('|', 1) # text was the input from user

                    button_text_content = button_text_content.strip()
                    button_url_str = button_url_str.strip()

                    if not button_url_str:
                        await event.respond("Button URL cannot be empty. Please provide text|url or type 'skip'.")
                        return # Stay in BUTTONS state

                    # Basic scheme check - Telegram typically requires http/https for web links
                    if not (button_url_str.lower().startswith('http://') or button_url_str.lower().startswith('https://')):
                        # Allow common tg:// links as well
                        if not button_url_str.lower().startswith('tg://'):
                             await event.respond("Button URL seems invalid. It should typically start with http://, https://, or tg://. Please correct it or type 'skip'.")
                             return # Stay in BUTTONS state

                    # More robust check using urlparse for http/https links
                    if button_url_str.lower().startswith('http://') or button_url_str.lower().startswith('https://'):
                        try:
                            parsed_url = urlparse(button_url_str)
                            if not parsed_url.scheme or not parsed_url.netloc:
                                await event.respond(f"The URL '{button_url_str}' seems incomplete or malformed (e.g., missing domain for http/https). Please provide a valid URL or type 'skip'.")
                                return # Stay in BUTTONS state
                        except ValueError: # Handles grossly malformed URLs that urlparse can't handle
                            await event.respond(f"The URL '{button_url_str}' is badly malformed. Please provide a valid URL or type 'skip'.")
                            return # Stay in BUTTONS state

                    # If validation passes:
                    state_data['data']['buttons'].append({"text": button_text_content, "url": button_url_str})
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

                logger.info(f"Attempting to insert schedule for user {user_id}: {state_data['data']}") # Added log
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

    @require_subscription
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

    @require_subscription
    async def handle_stop_schedule(self, event):
        chat_id = event.chat_id
        user_id = event.sender_id

        try:
            if not await self.is_admin(user_id, chat_id):
                await event.respond("Only group admins can stop schedules!")
                return

            query = {
                "chat_id": chat_id,
                "$or": [
                    { "interval_seconds": { "$exists": True, "$ne": None, "$gt": 0 } },
                    {
                        "schedule_time": { "$exists": True, "$ne": None },
                        "$or": [
                            { "sent": False },
                            { "sent": { "$exists": False } }
                        ]
                    }
                ]
            }
            messages_cursor = self.collection.find(query)

            buttons = []
            # Convert cursor to list to process it, as we need to check if it's empty.
            messages_list = list(messages_cursor)

            if not messages_list:
                await event.respond("No active schedules to stop.")
                return

            for msg_doc in messages_list:
                msg_id_str = str(msg_doc.get('_id'))
                schedule_name_for_button = msg_doc.get('schedule_name', f"ID {msg_id_str}")

                max_button_text_len = 30 # Consistent with handle_list_schedules
                button_text = f"Stop: {schedule_name_for_button}"
                if len(button_text) > max_button_text_len:
                    button_text = button_text[:max_button_text_len-3] + "..."

                buttons.append([Button.inline(button_text, data=f"stop_{msg_id_str}")])

            # This check is now implicitly handled by checking messages_list earlier.
            # if not buttons:
            #     await event.respond("No scheduled messages to stop.")
                return

            await event.respond("Select a schedule to stop:", buttons=buttons)
        except Exception as e:
            logger.error(f"Error in /stop: {e}")
            await event.respond("An error occurred.")

    async def handle_button_click(self, event):
        user_id = event.sender_id
        # chat_id from event might be the user's PM if the fsub message was sent there,
        # or group chat if the button (like stop) originated from a group message.
        chat_id = event.chat_id
        data = event.data.decode('utf-8')

        try:
            if data.startswith("stop_"):
                # Admin check specifically for the stop action
                if not await self.is_admin(user_id, chat_id): # chat_id here is where the /list or /stop command was issued
                    await event.answer("Only group admins can perform this action!", alert=True)
                    return

                msg_id_to_stop = data[5:]
                # Remove sent: False condition, as we want to stop it regardless of its sent status if the job exists.
                # chat_id is kept for security/scoping.
                result = self.collection.delete_one({"_id": ObjectId(msg_id_to_stop), "chat_id": chat_id})
                if result.deleted_count == 0:
                    await event.edit("Message ID not found in database (perhaps already stopped or never existed).", buttons=None)
                    return

                schedule.clear(f"message_{msg_id_to_stop}") # Clear the job from the scheduler
                logger.info(f"Cleared schedule job for message ID {msg_id_to_stop} with tag message_{msg_id_to_stop}")
                await event.edit(f"Scheduled message ID `{msg_id_to_stop}` stopped and removed.", buttons=None)
                # Using event.edit to change the original message (e.g. the /list message)

            elif data == "fsub_retry":
                logger.info(f"User {user_id} clicked 'fsub_retry'. Re-checking subscriptions.")

                try:
                    # Delete the message that contained the "Retry" button
                    await event.delete()
                except Exception as e_del_prompt:
                    logger.warning(f"Could not delete fsub_retry prompt for user {user_id}: {e_del_prompt}")

                is_subscribed, missing_channel_details_list = await self.is_user_subscribed(user_id)

                if is_subscribed:
                    # Send a new message to the user (PM)
                    await self.client.send_message(user_id, "✅ Subscription confirmed! You can now try your original command again.")
                else:
                    # Re-generate the "please subscribe" message
                    response_message = "It seems you are still not subscribed to all required channel(s):\n\n"
                    generated_buttons = []

                    if not missing_channel_details_list: # Should ideally not happen
                         response_message = "There was an issue re-checking your subscriptions. Please try the command again."
                    else:
                        for channel_detail in missing_channel_details_list:
                            title = channel_detail.get('title', str(channel_detail.get('identifier', 'Unknown Channel')))
                            username = channel_detail.get('username')
                            response_message += f"- **{title}**"
                            if username:
                                response_message += f" (@{username})\n"
                                generated_buttons.append([Button.url(f"Join {title}", f"https://t.me/{username}")])
                            else:
                                response_message += " (Please find and join this channel)\n"
                        response_message += "\nOnce subscribed to all, please click the button below."

                    generated_buttons.append([Button.inline("✅ I've Joined / Retry", data="fsub_retry")])

                    # Send a new message to the user (PM)
                    await self.client.send_message(user_id, response_message, buttons=generated_buttons, link_preview=False)
                return # Explicitly return after handling fsub_retry to avoid falling into generic event.answer

        except Exception as e:
            logger.error(f"Error in handle_button_click for data '{data}': {e}", exc_info=True)
            try:
                # Try to inform user about general error via answer, then respond if that fails
                await event.answer("An error occurred while processing this action.", alert=True)
            except Exception: # If event.answer fails (e.g. callback expired)
                 if event.is_private : # Only send message if it's a PM context
                    await self.client.send_message(user_id, "An error occurred. Please try again.")

        # Default event.answer() if no other response has been sent by event.edit or event.respond by this point
        # This is mainly for buttons that don't send new messages or edit their own message.
        # fsub_retry and stop_ actions already send/edit messages.
        if not (data.startswith("stop_") or data == "fsub_retry"):
            try:
                await event.answer()
            except telethon.errors.rpcerrorlist.CallbackQueryIdInvalidError:
                 pass # Callback ID might be invalid if already processed or message deleted, ignore.


    def send_scheduled_message(self, chat_id, message_id):
        async def send_message():
            try:
                # 1. Retrieve Schedule Details
                message_doc = self.collection.find_one({"_id": ObjectId(message_id)})

                if not message_doc:
                    logger.error(f"Schedule document {message_id} not found. Cannot send message.")
                    return
                if message_doc.get("sent") and not message_doc.get("interval_seconds"): # Already sent one-time message
                    logger.info(f"One-time message {message_id} already marked as sent. Skipping.")
                    return schedule.CancelJob # Ensure it's unscheduled if this state is reached

                schedule_name = message_doc.get('schedule_name', f"UnnamedSchedule_{message_id}")
                last_telegram_id = message_doc.get('last_sent_telegram_message_id')

                # 2. Delete Previous Message (if applicable for this specific schedule document)
                if last_telegram_id:
                    try:
                        logger.info(f"Attempting to delete old message {last_telegram_id} for schedule '{schedule_name}' (ID: {message_id}) in chat {chat_id}")
                        await self.client.delete_messages(chat_id, last_telegram_id)
                        logger.info(f"Successfully deleted old message {last_telegram_id} for schedule '{schedule_name}' (ID: {message_id})")
                    except Exception as e_del:
                        logger.warning(f"Could not delete old message {last_telegram_id} for schedule '{schedule_name}' (ID: {message_id}) in chat {chat_id}: {e_del}. It might have been deleted already or permissions changed.")

                # Prepare message content (buttons, text)
                buttons = []
                if message_doc.get("buttons"):
                    for btn_data in message_doc["buttons"]:
                        buttons.append([Button.url(btn_data["text"], btn_data["url"])])
                keyboard = buttons if buttons else None
                message_text_content = message_doc.get("message_text", "")

                # 3. Send the New Message
                sent_message_object = None
                if message_doc.get("file_id") and message_doc.get("media_type") and message_doc.get("access_hash"):
                    media = None
                    file_id = int(message_doc["file_id"]) # Ensure int
                    access_hash = message_doc["access_hash"] # Ensure correct type if needed by Telethon
                    file_reference = b'' # Often required, can be empty bytes

                    if message_doc["media_type"] == "photo":
                        media = types.InputMediaPhoto(
                            id=types.InputPhoto(id=file_id, access_hash=access_hash, file_reference=file_reference)
                        )
                    elif message_doc["media_type"] == "video":
                         media = types.InputMediaDocument(
                            id=types.InputDocument(id=file_id, access_hash=access_hash, file_reference=file_reference)
                        )

                    if media:
                        sent_message_object = await self.client.send_message(
                            chat_id,
                            message_text_content,
                            file=media,
                            buttons=keyboard
                        )
                        logger.info(f"Sent {message_doc['media_type']} message for schedule '{schedule_name}' (ID: {message_id}) to chat {chat_id}")
                    else:
                        logger.error(f"Invalid media type or setup for schedule '{schedule_name}' (ID: {message_id})")
                        return # Stop if media setup is wrong
                else:
                    sent_message_object = await self.client.send_message(
                        chat_id,
                        message_text_content,
                        buttons=keyboard
                    )
                    logger.info(f"Sent text message for schedule '{schedule_name}' (ID: {message_id}) to chat {chat_id}")

                # 4. Store New Telegram Message ID
                if sent_message_object:
                    new_telegram_id = sent_message_object.id
                    self.collection.update_one(
                        {"_id": ObjectId(message_id)},
                        {"$set": {"last_sent_telegram_message_id": new_telegram_id}}
                    )
                    logger.info(f"Updated schedule '{schedule_name}' (ID: {message_id}) with new sent Telegram message ID: {new_telegram_id}")
                else:
                    logger.warning(f"Did not receive sent_message_object for schedule '{schedule_name}' (ID: {message_id}). Not updating last_sent_telegram_message_id.")

                # 5. Handle one-time messages (mark as sent and unschedule)
                if not message_doc.get("interval_seconds"):
                    self.collection.update_one(
                        {"_id": ObjectId(message_id)},
                        {"$set": {"sent": True}} # Mark as sent
                    )
                    logger.info(f"Marked one-time schedule '{schedule_name}' (ID: {message_id}) as sent.")
                    # The job will be cancelled by the return value below

            except Exception as e:
                # Ensure message_doc is defined for logging, or use message_id if not.
                # 'message_doc' is the variable name holding the document from MongoDB in this function.
                schedule_name_for_log = message_doc.get('schedule_name', 'N/A') if message_doc else 'N/A'

                if isinstance(e, telethon.errors.rpcerrorlist.ButtonUrlInvalidError):
                    problematic_buttons = message_doc.get('buttons', []) if message_doc else []
                    logger.error(
                        f"ButtonUrlInvalidError for schedule ID {message_id} (Name: '{schedule_name_for_log}'). "
                        f"Attempted buttons: {problematic_buttons}. Error: {e}",
                        exc_info=True
                    )
                else:
                    logger.error(
                        f"Error sending message for schedule ID {message_id} (Name: '{schedule_name_for_log}'): {e}",
                        exc_info=True
                    )

        # Run the async send_message function
        asyncio.run_coroutine_threadsafe(send_message(), self.client.loop)

        # For one-time jobs, determine if they should be cancelled.
        # This part runs synchronously after scheduling the async task.
        # It needs to fetch the document again to be sure, or rely on the initial fetch if safe.
        # To be safe, let's re-fetch, though it's a bit redundant.
        current_message_doc_for_cancel = self.collection.find_one({"_id": ObjectId(message_id)})
        if current_message_doc_for_cancel and not current_message_doc_for_cancel.get("interval_seconds"):
            return schedule.CancelJob
        return None

    @require_subscription
    async def handle_list_schedules(self, event):
        chat_id = event.chat_id
        user_id = event.sender_id # Get sender_id for admin check

        try:
            query = {
                "chat_id": chat_id,
                "$or": [
                    { "interval_seconds": { "$exists": True, "$ne": None, "$gt": 0 } },
                    {
                        "schedule_time": { "$exists": True, "$ne": None },
                        "$or": [
                            { "sent": False },
                            { "sent": { "$exists": False } }
                        ]
                    }
                ]
            }
            messages_cursor = self.collection.find(query)

            # Convert cursor to list to avoid issues if we need to check if it's empty first
            # or if admin check needs to happen before iterating.
            # For now, admin check can be inside the loop.

            keyboard_buttons = [] # Main list for all button rows
            response_parts = ["**Scheduled Messages:**\n\n"] # Use a list to build the response string
            processed_count = 0

            # Determine if the event sender is an admin once, before the loop
            is_event_sender_admin = await self.is_admin(user_id, chat_id)

            for msg_doc in messages_cursor:
                try:
                    msg_id_str = str(msg_doc.get('_id'))
                    schedule_name = msg_doc.get('schedule_name', f"Unnamed (ID: {msg_id_str})")
                    message_text = msg_doc.get('message_text', '')
                    message_text_preview = (message_text[:27] + "...") if len(message_text) > 30 else message_text

                    # Timing info
                    schedule_time = msg_doc.get("schedule_time")
                    interval_seconds = msg_doc.get("interval_seconds")
                    if schedule_time:
                        timing_info = f"One-time at {schedule_time}"
                    elif interval_seconds:
                        timing_info = f"Repeats every {interval_seconds} seconds"
                    else:
                        timing_info = "N/A"

                    media_type = msg_doc.get('media_type')
                    media_info = "Yes" if media_type else "No"

                    buttons_attached_list = msg_doc.get('buttons', [])
                    buttons_attached_info = "Yes" if buttons_attached_list else "No"

                    text_response_part = (
                        f"**Name:** {schedule_name} (ID: `{msg_id_str}`)\n"
                        f"**Text:** \"{message_text_preview}\"\n"
                        f"**Timing:** {timing_info}\n"
                        f"**Media Attached:** {media_info}, **Buttons Attached:** {buttons_attached_info}\n"
                        "--------------------\n\n"
                    )
                    response_parts.append(text_response_part)
                    processed_count += 1
                    logger.info(f"Formatting message for /list: ID {msg_id_str}, Name: {schedule_name}")

                    # Button generation for admins
                    if is_event_sender_admin:
                        schedule_name_for_button = msg_doc.get('schedule_name', f"ID {msg_id_str}")
                        max_button_text_len = 30 # Max length for button text to avoid Telegram errors
                        button_text = f"Stop: {schedule_name_for_button}"
                        if len(button_text) > max_button_text_len:
                            button_text = button_text[:max_button_text_len-3] + "..."

                        keyboard_buttons.append([Button.inline(button_text, data=f"stop_{msg_id_str}")])

                except Exception as e_loop:
                    logger.error(f"Error processing message document {msg_doc.get('_id', 'UNKNOWN_ID')} in /list loop: {e_loop}", exc_info=True)
                    response_parts.append(f"Error displaying one schedule (ID: {msg_doc.get('_id', 'UNKNOWN_ID')}).\n--------------------\n\n")


            if processed_count == 0:
                await event.respond("No scheduled messages.")
                return

            final_response = "".join(response_parts)
            # Ensure the final response isn't too long for a single Telegram message
            # Telegram message limit is 4096 characters.
            if len(final_response) > 4096:
                logger.warning(f"List schedule response too long ({len(final_response)} chars). Truncating.")
                # A more sophisticated truncation or pagination would be better for production.
                # For now, just truncate brutally.
                final_response = final_response[:4090] + "\n... (list truncated)"

            await event.respond(final_response, buttons=keyboard_buttons if keyboard_buttons else None)

        except Exception as e:
            logger.error(f"Error in /list_schedules: {e}", exc_info=True)
            await event.respond("An error occurred while fetching the list of schedules.")

    async def run(self):
        try:
            await self.client.start(bot_token=self.bot_token)
            logger.info("Bot started successfully")

            # Load and reschedule jobs from database
            await self.load_and_reschedule_jobs()

            logger.info("Starting main scheduling loop...")
            while True:
                schedule.run_pending()
                await asyncio.sleep(CONFIG['SCHEDULE_CHECK_INTERVAL_SECONDS'])
        except Exception as e:
            logger.error(f"Error in run loop: {e}", exc_info=True)
            raise

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
