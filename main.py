import os
import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException
from database import Database
import time
import threading
import re
from datetime import datetime, timedelta
from flask import Flask, jsonify
import requests

# Initialize bot with token from environment variable
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set in environment variables")
bot = telebot.TeleBot(BOT_TOKEN)

# Load environment variables
ADMINS = [
    int(admin_id) for admin_id in os.getenv('ADMINS', '').split(',')
    if admin_id.strip().isdigit()
]
OWNER = int(os.getenv('OWNER', '0'))
CHANNEL_1 = os.getenv('CHANNEL_1', '')
CHANNEL_2 = os.getenv('CHANNEL_2', '')
join_access_enabled = False  # Global flag for channel join requirement

# Initialize database
db = Database()

# User states for handling multi-step input
user_states = {}

# States for channel broadcasting
BROADCAST_STATES = {
    'AWAITING_CHANNEL': 'awaiting_channel',
    'AWAITING_CONTENT': 'awaiting_content',
    'AWAITING_IMAGE': 'awaiting_image'
}

# Track bot start time for uptime calculation
bot_start_time = datetime.now()

# Cache bot info to avoid API calls in health checks
bot_info_cache = None

# Flask app for health checks and keep-alive (Render free tier)
app = Flask(__name__)

@app.route('/health')
def health_check():
    """Health check endpoint for Render"""
    uptime = datetime.now() - bot_start_time
    return jsonify({
        'status': 'healthy',
        'uptime_seconds': int(uptime.total_seconds()),
        'bot_username': bot_info_cache.get('username', 'unknown') if bot_info_cache else 'unknown'
    })

@app.route('/')
def home():
    """Root endpoint"""
    return jsonify({'message': 'Video Bot is running!', 'status': 'online'})

def keep_alive_ping():
    """Self-ping to prevent Render free tier from sleeping"""
    render_url = os.getenv('RENDER_EXTERNAL_URL')  # Set this in Render environment
    if not render_url:
        print("⚠️ RENDER_EXTERNAL_URL not set - keep-alive ping disabled")
        return
    
    while True:
        try:
            time.sleep(840)  # Ping every 14 minutes (before 15-minute timeout)
            requests.get(f"{render_url}/health", timeout=10)
            print("🔄 Keep-alive ping sent successfully")
        except Exception as e:
            print(f"⚠️ Keep-alive ping failed: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying on failure


def escape_markdown_v2(text):
    """Helper function to escape special characters for MarkdownV2."""
    special_chars = r"([_*[\]()~`>#+-=|{}.!])"
    return re.sub(special_chars, r"\\\1", text)


def check_user_access(user_id,
                      username=None,
                      first_name=None,
                      action="access"):
    """
    Comprehensive user access check with ban enforcement and activity logging
    Returns True if user can proceed, False if banned
    """
    # Update user information and activity
    db.add_user(user_id, username, first_name)
    db.log_user_activity(user_id, action, f"Attempted: {action}")

    # Check if user is banned
    if db.is_user_banned(user_id):
        return False

    return True


def send_banned_message(message_or_chat_id):
    """Send consistent banned message to user"""
    if hasattr(message_or_chat_id, 'chat'):
        chat_id = message_or_chat_id.chat.id
    else:
        chat_id = message_or_chat_id

    try:
        bot.send_message(chat_id,
                         "🚫 You are banned from using this bot\\.",
                         parse_mode='MarkdownV2')
    except Exception as e:
        print(f"Failed to send banned message to {chat_id}: {e}")


def delete_video_message(chat_id, message_id):
    """Function to be run in a separate thread to delete a message after 20 minutes"""
    time.sleep(1200)  # 20 minutes * 60 seconds = 1200 seconds
    try:
        bot.delete_message(chat_id, message_id)
        print(
            f"✅ Message {message_id} in chat {chat_id} deleted successfully.")
    except Exception as e:
        print(
            f"❌ Failed to delete message {message_id} in chat {chat_id}: {e}")


def process_scheduled_broadcasts():
    """Background function to process scheduled broadcasts"""
    while True:
        try:
            pending_broadcasts = db.get_pending_broadcasts()

            for broadcast in pending_broadcasts:
                try:
                    target_channel = broadcast['target_channel']
                    content = broadcast['content']
                    content_type = broadcast['content_type']
                    media_file_id = broadcast['media_file_id']

                    # Normalize channel identifier
                    if target_channel.startswith('@'):
                        channel_id = target_channel
                    elif target_channel.startswith('-'):
                        channel_id = int(target_channel)
                    else:
                        channel_id = f'@{target_channel}'

                    # Send the broadcast
                    if content_type == 'photo' and media_file_id:
                        bot.send_photo(channel_id,
                                       media_file_id,
                                       caption=content,
                                       parse_mode=None)
                    elif content_type == 'text':
                        bot.send_message(channel_id,
                                         content,
                                         parse_mode=None,
                                         disable_web_page_preview=False)

                    # Update status to completed
                    db.update_broadcast_status(broadcast['id'], 'completed')
                    db.log_user_activity(
                        broadcast['admin_id'], "scheduled_broadcast_sent",
                        f"Scheduled broadcast sent to {target_channel}")
                    print(
                        f"✅ Scheduled broadcast {broadcast['id']} sent successfully to {target_channel}"
                    )

                except Exception as e:
                    # Mark as failed
                    db.update_broadcast_status(broadcast['id'], 'failed')
                    target = broadcast.get('target_channel', 'unknown')
                    db.log_user_activity(
                        broadcast['admin_id'], "scheduled_broadcast_failed",
                        f"Failed to send to {target}: {str(e)}")
                    print(
                        f"❌ Failed to send scheduled broadcast {broadcast['id']}: {e}"
                    )

            # Sleep for 60 seconds before checking again
            time.sleep(60)

        except Exception as e:
            print(f"❌ Error in scheduled broadcast processor: {e}")
            time.sleep(60)


def check_channel_membership(user_id):
    """Check if user is a member of required channels"""
    try:
        if CHANNEL_1:
            # Ensure channel name has @ prefix
            channel_1 = CHANNEL_1 if CHANNEL_1.startswith(
                '@') else f'@{CHANNEL_1}'
            print(
                f"Checking membership for user {user_id} in channel {channel_1}"
            )
            status = bot.get_chat_member(channel_1, user_id).status
            print(f"User {user_id} status in {channel_1}: {status}")
            if status not in ['member', 'administrator', 'creator']:
                return False
        if CHANNEL_2:
            # Ensure channel name has @ prefix
            channel_2 = CHANNEL_2 if CHANNEL_2.startswith(
                '@') else f'@{CHANNEL_2}'
            print(
                f"Checking membership for user {user_id} in channel {channel_2}"
            )
            status = bot.get_chat_member(channel_2, user_id).status
            print(f"User {user_id} status in {channel_2}: {status}")
            if status not in ['member', 'administrator', 'creator']:
                return False
        return True
    except Exception as e:
        print(f"Error checking membership for user {user_id}: {e}")
        return False


def prompt_join_channels(chat_id, user_id, callback_data):
    """Prompt user to join channels with buttons"""
    keyboard = types.InlineKeyboardMarkup()

    if CHANNEL_1:
        channel1_clean = CHANNEL_1.lstrip('@')
        channel1_url = f"https://t.me/{channel1_clean}"
        keyboard.add(
            types.InlineKeyboardButton("📢 Join Channel 1", url=channel1_url))

    if CHANNEL_2:
        channel2_clean = CHANNEL_2.lstrip('@')
        channel2_url = f"https://t.me/{channel2_clean}"
        keyboard.add(
            types.InlineKeyboardButton("📢 Join Channel 2", url=channel2_url))

    keyboard.add(
        types.InlineKeyboardButton("✅ I've Joined Both Channels",
                                   callback_data=callback_data))
    bot.send_message(chat_id,
                     "🚫 Please join both channels first to access videos\\!",
                     reply_markup=keyboard,
                     parse_mode='MarkdownV2')


@bot.message_handler(commands=['start'])
def start_command(message):
    """Handle /start command with optional video_id"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    # Comprehensive user access check
    if not check_user_access(user_id, username, first_name, "start"):
        send_banned_message(message)
        return

    args = message.text.split()
    if len(args) > 1:
        video_id = args[1]
        if join_access_enabled and not check_channel_membership(user_id):
            prompt_join_channels(chat_id, user_id, f"retry_start_{video_id}")
            return

        video = db.get_video_by_id(video_id)
        if video:
            # Log video view for analytics
            db.log_video_view(video_id, user_id)

            caption_text = f"🎥 *{escape_markdown_v2(video['name'])}*\n\n{escape_markdown_v2(video['description'])}"
            sent_message = bot.send_video(chat_id,
                                          video['file_id'],
                                          caption=caption_text,
                                          supports_streaming=True,
                                          parse_mode='MarkdownV2')
            threading.Thread(target=delete_video_message,
                             args=(chat_id, sent_message.message_id)).start()
        else:
            bot.reply_to(
                message,
                "❌ Video not found\\. Please check the ID and try again\\.",
                parse_mode='MarkdownV2')
        return

    bot.reply_to(
        message,
        "👋 Welcome to the Video Bot! Use /browse to see videos, /random for a random video, or /find <query> to search."
    )


@bot.message_handler(commands=['ping'])
def ping_command(message):
    """Handle /ping command"""
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    # Comprehensive user access check
    if not check_user_access(user_id, username, first_name, "ping"):
        send_banned_message(message)
        return

    bot.reply_to(message, "🏓 Pong! Bot is online.")


@bot.message_handler(commands=['join_access'])
def join_access_command(message):
    """Toggle channel join requirement (owner only)"""
    global join_access_enabled
    if message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 Only the owner can use this command\\.",
                     parse_mode='MarkdownV2')
        return

    join_access_enabled = not join_access_enabled
    status = "enabled" if join_access_enabled else "disabled"
    print(
        f"📢 Channel join requirement is now {status} by owner {message.from_user.id}"
    )
    bot.reply_to(
        message,
        f"✅ Channel join requirement {status}\\!\n\n🔧 Channels: @{CHANNEL_1}, @{CHANNEL_2}",
        parse_mode='MarkdownV2')


@bot.message_handler(commands=['browse'])
def browse_command(message):
    """List all videos with pagination"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    # Comprehensive user access check
    if not check_user_access(user_id, username, first_name, "browse"):
        send_banned_message(message)
        return

    if join_access_enabled and not check_channel_membership(user_id):
        prompt_join_channels(chat_id, user_id, "retry_browse")
        return

    videos = db.get_all_videos()
    if not videos:
        bot.reply_to(message,
                     "📪 No videos available\\.",
                     parse_mode='MarkdownV2')
        return

    keyboard = types.InlineKeyboardMarkup(row_width=2)
    for video in videos[:20]:  # Show up to 20 videos
        keyboard.add(
            types.InlineKeyboardButton(video['name'],
                                       callback_data=f"video_{video['id']}"))

    bot.send_message(chat_id,
                     f"📹 Available videos \\({len(videos)} total\\)\\:",
                     reply_markup=keyboard,
                     parse_mode='MarkdownV2')


@bot.message_handler(commands=['random'])
def random_command(message):
    """Send a random video"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    # Comprehensive user access check
    if not check_user_access(user_id, username, first_name, "random"):
        send_banned_message(message)
        return

    if join_access_enabled and not check_channel_membership(user_id):
        prompt_join_channels(chat_id, user_id, "retry_random")
        return

    video = db.get_random_video()
    if video:
        # Log video view for analytics
        db.log_video_view(video['id'], user_id)

        caption_text = f"🎲 Random video: {escape_markdown_v2(video['name'])}\n\n{escape_markdown_v2(video['description'])}"
        sent_message = bot.send_video(chat_id,
                                      video['file_id'],
                                      caption=caption_text,
                                      supports_streaming=True,
                                      parse_mode='MarkdownV2')
        threading.Thread(target=delete_video_message,
                         args=(chat_id, sent_message.message_id)).start()
    else:
        bot.reply_to(message,
                     "📪 No videos available\\.",
                     parse_mode='MarkdownV2')


@bot.message_handler(commands=['find', 'search'])
def search_command(message):
    """Search videos by name or description (for admins only)"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    is_admin = user_id in ADMINS or user_id == OWNER

    query = ' '.join(message.text.split()[1:])
    if not query:
        bot.reply_to(
            message,
            "🔍 Please provide a search query, e\\.g\\., `/find funny video`",
            parse_mode='MarkdownV2')
        return

    # Comprehensive user access check
    if not check_user_access(user_id, username, first_name, f"search:{query}"):
        send_banned_message(message)
        return

    if join_access_enabled and not is_admin and not check_channel_membership(
            user_id):
        prompt_join_channels(chat_id, user_id, f"retry_find_{query}")
        return

    videos = db.search_videos(query)
    count = len(videos)

    if not videos:
        bot.reply_to(message,
                     "❌ No videos found matching your query\\.",
                     parse_mode='MarkdownV2')
        return

    response_text = f"🔍 Found {count} results for '`{escape_markdown_v2(query)}`':"

    keyboard = types.InlineKeyboardMarkup(row_width=2)
    for video in videos[:20]:  # Show up to 20 results
        keyboard.add(
            types.InlineKeyboardButton(video['name'],
                                       callback_data=f"video_{video['id']}"))

    bot.send_message(chat_id,
                     response_text,
                     reply_markup=keyboard,
                     parse_mode='MarkdownV2')


@bot.message_handler(commands=['delete'])
def delete_command(message):
    """Delete a video by ID (owner/admins only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 Only admins can delete videos\\.",
                     parse_mode='MarkdownV2')
        return

    args = message.text.split()
    if len(args) < 2:
        bot.reply_to(
            message,
            "❌ Please provide a video ID, e\\.g\\., `/delete a1b2c3d4e5`",
            parse_mode='MarkdownV2')
        return

    video_id = args[1]
    if db.delete_video(video_id):
        bot.reply_to(message,
                     f"✅ Video `{video_id}` deleted successfully\\.",
                     parse_mode='MarkdownV2')
    else:
        bot.reply_to(message,
                     f"❌ Video `{video_id}` not found\\.",
                     parse_mode='MarkdownV2')


@bot.message_handler(commands=['database'])
def database_command(message):
    """Provide admins with database stats and a list of videos"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    videos = db.get_all_videos()
    if not videos:
        bot.reply_to(message,
                     "📪 The database is empty\\.",
                     parse_mode='MarkdownV2')
        return

    stats_text = f"**Database Stats**\nTotal Videos: {len(videos)}\n\n"
    for video in videos:
        stats_text += f"ID: `{video['id']}` \\- {escape_markdown_v2(video['name'])}\n"

    bot.reply_to(message, stats_text, parse_mode='MarkdownV2')


@bot.message_handler(commands=['help'])
def help_command(message):
    """Display help message for admins"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    help_text = (
        "🎛️ **Main Admin Commands:**\n"
        "/admin\\_panel \\- 🎛️ Interactive admin control panel\n"
        "/stats \\- 📊 Comprehensive bot statistics\n"
        "/video\\_manage \\- 🎥 Video management interface\n"
        "/system\\_info \\- 🖥️ Detailed system information\n"
        "\n📹 **Video Commands:**\n"
        "/start \\<id\\> \\- Get a video by its ID\n"
        "/browse \\- Browse all videos\n"
        "/random \\- Get a random video\n"
        "/find \\<query\\> \\- Search for videos\n"
        "/delete \\<id\\> \\- Delete a video by ID\n"
        "/database \\- List all videos with IDs\n"
        "\n👥 **User Management:**\n"
        "/ban\\_user \\<id\\> \\<reason\\> \\- Ban a user\n"
        "/unban\\_user \\<id\\> \\- Unban a user\n"
        "/banned\\_users \\- List all banned users\n"
        "/user\\_info \\<id\\> \\- Get detailed user information\n"
        "/search\\_users \\<query\\> \\- Search for users\n"
        "/recent\\_activity \\- Show recent user activity\n"
        "/broadcast \\<message\\> \\- Send message to all users\n"
        "/channel\\_broadcast \\- Send image/text/links to channels\n"
        "/analytics \\- View video analytics and statistics\n"
        "/templates \\- Manage message templates\n"
        "/schedule \\- Schedule broadcasts to channels\n"
        "/cleanup\\_users \\- Remove inactive users \\(owner\\)\n"
        "\n⚙️ **Settings:**\n"
        "/join\\_access \\- Toggle channel join requirement\n"
        "/admin \\- List bot admins\n"
        "/ping \\- Check bot status\n"
        "\n📤 **Video Upload:**\n"
        "Send a video file to upload\\. Bot will ask for name/description\\.")

    bot.reply_to(message, help_text, parse_mode='MarkdownV2')


@bot.message_handler(commands=['admin'])
def admin_list_command(message):
    """List admins and owner"""
    admin_usernames = []
    for admin_id in ADMINS:
        try:
            user = bot.get_chat(admin_id)
            username = user.username if user.username else str(admin_id)
            admin_usernames.append(username)
        except Exception:
            admin_usernames.append(str(admin_id))

    try:
        owner_user = bot.get_chat(OWNER)
        owner_username = owner_user.username if owner_user.username else str(
            OWNER)
    except Exception:
        owner_username = str(OWNER)

    # Escape usernames for MarkdownV2
    escaped_owner_username = escape_markdown_v2(owner_username)
    escaped_admin_usernames = [escape_markdown_v2(u) for u in admin_usernames]

    response_text = f"👑 **Owner:** @{escaped_owner_username}\n"
    if escaped_admin_usernames:
        response_text += "👮 **Admins:**\n"
        for username in escaped_admin_usernames:
            response_text += f"\\- @{username}\n"

    bot.reply_to(message, response_text, parse_mode='MarkdownV2')


@bot.message_handler(commands=['broadcast'])
def broadcast_command(message):
    """Send a message to all tracked users (owner only)"""
    if message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 Only the owner can use this command\\.",
                     parse_mode='MarkdownV2')
        return

    text_to_broadcast = ' '.join(message.text.split()[1:])
    if not text_to_broadcast:
        bot.reply_to(message,
                     "❌ Please provide a message to broadcast\\.",
                     parse_mode='MarkdownV2')
        return

    users = db.get_active_users()  # Only get non-banned users
    sent_count = 0
    for user in users:
        try:
            bot.send_message(user['user_id'], text_to_broadcast)
            sent_count += 1
            time.sleep(0.1)  # Small delay to avoid rate limiting
        except ApiTelegramException as e:
            if e.description == "Forbidden: bot was blocked by the user":
                db.remove_user(user['user_id'])
            else:
                print(f"Failed to send message to {user['user_id']}: {e}")

    total_active = len(users)
    bot.reply_to(
        message,
        f"✅ Broadcast sent to {sent_count}/{total_active} active users\\.",
        parse_mode='MarkdownV2')


@bot.message_handler(commands=['channel_broadcast'])
def channel_broadcast_command(message):
    """Start channel broadcasting process (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 Only admins can broadcast to channels\\.",
                     parse_mode='MarkdownV2')
        return

    if not CHANNEL_1 and not CHANNEL_2:
        bot.reply_to(
            message,
            "❌ No channels configured\\. Please set CHANNEL\\_1 and/or CHANNEL\\_2 environment variables\\.",
            parse_mode='MarkdownV2')
        return

    user_id = message.from_user.id
    user_states[user_id] = {
        'state': BROADCAST_STATES['AWAITING_CHANNEL'],
        'broadcast_type': 'channel'
    }

    keyboard = types.InlineKeyboardMarkup()

    if CHANNEL_1:
        keyboard.add(
            types.InlineKeyboardButton(f"📢 Channel 1 ({CHANNEL_1})",
                                       callback_data="broadcast_channel_1"))
    if CHANNEL_2:
        keyboard.add(
            types.InlineKeyboardButton(f"📢 Channel 2 ({CHANNEL_2})",
                                       callback_data="broadcast_channel_2"))

    keyboard.add(
        types.InlineKeyboardButton("❌ Cancel",
                                   callback_data="broadcast_cancel"))

    bot.send_message(
        message.chat.id,
        "📢 **Channel Broadcast**\n\nSelect which channel to broadcast to:",
        reply_markup=keyboard,
        parse_mode='MarkdownV2')


@bot.message_handler(commands=['analytics'])
def analytics_command(message):
    """Show video analytics (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    # Get analytics summary
    summary = db.get_analytics_summary()
    popular_videos = db.get_popular_videos(5)

    analytics_text = f"""📊 **Video Analytics Summary**

📈 **Overview:**
\\• Total Views: {summary['total_views']}
\\• Views Today: {summary['views_today']}
\\• Top Video: {escape_markdown_v2(summary['top_video']['name']) if summary['top_video'] else 'N/A'} \\({summary['top_video']['views'] if summary['top_video'] else 0} views\\)

🔥 **Most Popular Videos:**"""

    for i, video in enumerate(popular_videos[:5], 1):
        video_name = escape_markdown_v2(video['name'][:30] +
                                        "..." if len(video['name']) >
                                        30 else video['name'])
        analytics_text += f"\n{i}\\. {video_name} \\- {video['view_count']} views"

    if not popular_videos:
        analytics_text += "\nNo video views recorded yet\\."

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("📊 Detailed Analytics",
                                   callback_data="analytics_detailed"))

    bot.send_message(message.chat.id,
                     analytics_text,
                     reply_markup=keyboard,
                     parse_mode='MarkdownV2')


@bot.message_handler(commands=['templates'])
def templates_command(message):
    """Manage message templates (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    templates = db.get_templates()

    if not templates:
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("➕ Create Template",
                                       callback_data="template_create"))
        bot.send_message(message.chat.id,
                         "📝 **Message Templates**\n\nNo templates found\\.",
                         reply_markup=keyboard,
                         parse_mode='MarkdownV2')
        return

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for template in templates[:10]:  # Show first 10 templates
        template_name = template['name'][:25] + "..." if len(
            template['name']) > 25 else template['name']
        keyboard.add(
            types.InlineKeyboardButton(
                f"📝 {template_name}",
                callback_data=f"template_view_{template['id']}"))

    keyboard.add(
        types.InlineKeyboardButton("➕ Create New Template",
                                   callback_data="template_create"))

    templates_text = f"📝 **Message Templates** \\({len(templates)} total\\)\n\nSelect a template to view or edit:"
    bot.send_message(message.chat.id,
                     templates_text,
                     reply_markup=keyboard,
                     parse_mode='MarkdownV2')


@bot.message_handler(commands=['schedule'])
def schedule_command(message):
    """Schedule broadcasts (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    if not CHANNEL_1 and not CHANNEL_2:
        bot.reply_to(message,
                     "❌ No channels configured for scheduling\\.",
                     parse_mode='MarkdownV2')
        return

    user_id = message.from_user.id
    scheduled_broadcasts = db.get_scheduled_broadcasts(user_id)

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("➕ Schedule New Broadcast",
                                   callback_data="schedule_new"))

    if scheduled_broadcasts:
        keyboard.add(
            types.InlineKeyboardButton("📋 View Scheduled",
                                       callback_data="schedule_view"))

    pending_count = len(
        [b for b in scheduled_broadcasts if b['status'] == 'pending'])

    schedule_text = f"""⏰ **Scheduled Broadcasts**

📊 **Your Schedules:**
\\• Total: {len(scheduled_broadcasts)}
\\• Pending: {pending_count}
\\• Completed: {len(scheduled_broadcasts) - pending_count}

Select an action:"""

    bot.send_message(message.chat.id,
                     schedule_text,
                     reply_markup=keyboard,
                     parse_mode='MarkdownV2')


# ===== NEW ADMIN FEATURES =====


@bot.message_handler(commands=['stats'])
def admin_stats_command(message):
    """Show comprehensive bot statistics (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    # Get statistics
    user_count = db.get_user_count()
    video_count = db.get_video_count()
    video_stats = db.get_video_stats()
    recent_videos = db.get_recent_videos(3)

    # Bot info
    bot_info = bot.get_me()
    join_status = "enabled" if join_access_enabled else "disabled"

    stats_text = f"""📊 **Bot Statistics**

👥 **Users:** {user_count} registered
🎥 **Videos:** {video_count} total
📝 **With Descriptions:** {video_stats['with_description']}/{video_stats['total']}
🔧 **Channel Join:** {join_status}
🤖 **Bot:** @{escape_markdown_v2(bot_info.username)}

📋 **Recent Videos:**"""

    for video in recent_videos:
        video_name = escape_markdown_v2(video['name'][:30] +
                                        "..." if len(video['name']) >
                                        30 else video['name'])
        stats_text += f"\n\\• {video_name}"

    if not recent_videos:
        stats_text += "\n\\• No videos yet"

    bot.reply_to(message, stats_text, parse_mode='MarkdownV2')


@bot.message_handler(commands=['admin_panel'])
def admin_panel_command(message):
    """Show admin control panel with buttons (admin only)"""
    user_id = message.from_user.id

    # Log admin activity
    try:
        db.log_user_activity(user_id, "admin_panel",
                             "Accessed admin control panel")
    except Exception as e:
        print(f"Failed to log admin activity: {e}")

    if user_id not in ADMINS and user_id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    keyboard = types.InlineKeyboardMarkup(row_width=2)

    # Statistics and Info
    keyboard.add(
        types.InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
        types.InlineKeyboardButton("👥 Users", callback_data="admin_users"))

    # Video Management
    keyboard.add(
        types.InlineKeyboardButton("🎥 Videos", callback_data="admin_videos"),
        types.InlineKeyboardButton("🔍 Search", callback_data="admin_search"))

    # User Management
    keyboard.add(
        types.InlineKeyboardButton("🚫 Banned", callback_data="admin_banned"),
        types.InlineKeyboardButton("📝 Activity",
                                   callback_data="admin_activity"))

    # System Controls
    keyboard.add(
        types.InlineKeyboardButton("🔧 Settings",
                                   callback_data="admin_settings"),
        types.InlineKeyboardButton("📢 Broadcast",
                                   callback_data="admin_broadcast"))

    # Channel Broadcasting and Advanced Features
    keyboard.add(
        types.InlineKeyboardButton("📺 Channel Broadcast",
                                   callback_data="admin_channel_broadcast"),
        types.InlineKeyboardButton("📊 Analytics",
                                   callback_data="admin_analytics"))

    keyboard.add(
        types.InlineKeyboardButton("📝 Templates",
                                   callback_data="admin_templates"),
        types.InlineKeyboardButton("⏰ Schedule",
                                   callback_data="admin_schedule"))

    keyboard.add(
        types.InlineKeyboardButton("🔧 Bulk Operations",
                                   callback_data="admin_bulk"))

    # Maintenance (Owner only)
    if message.from_user.id == OWNER:
        keyboard.add(
            types.InlineKeyboardButton("🗄️ Database",
                                       callback_data="admin_database"),
            types.InlineKeyboardButton("⚠️ Cleanup",
                                       callback_data="admin_cleanup"))

    bot.send_message(message.chat.id,
                     "🎛️ **Admin Control Panel**\n\nSelect an option:",
                     reply_markup=keyboard,
                     parse_mode='MarkdownV2')


@bot.message_handler(commands=['cleanup_users'])
def cleanup_users_command(message):
    """Remove inactive/blocked users (owner only)"""
    if message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 Only the owner can use this command\\.",
                     parse_mode='MarkdownV2')
        return

    users = db.get_all_users()
    removed_count = 0

    bot.reply_to(message,
                 f"🧹 Starting cleanup of {len(users)} users\\.\\.\\.",
                 parse_mode='MarkdownV2')

    for user in users:
        try:
            # Try to send a test message (this will fail if user blocked the bot)
            bot.send_chat_action(user['user_id'], 'typing')
            time.sleep(0.1)
        except ApiTelegramException:
            db.remove_user(user['user_id'])
            removed_count += 1

    bot.reply_to(
        message,
        f"✅ Cleanup complete\\! Removed {removed_count} inactive users\\.",
        parse_mode='MarkdownV2')


@bot.message_handler(commands=['system_info'])
def system_info_command(message):
    """Show detailed system information (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    import sys
    import platform

    # System info
    python_version = f"{sys.version_info.major}\\.{sys.version_info.minor}\\.{sys.version_info.micro}"
    platform_info = escape_markdown_v2(platform.platform())

    # Bot uptime calculation
    uptime = datetime.now() - bot_start_time
    uptime_str = f"{uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m"
    current_time = datetime.now().strftime("%Y\\-%m\\-%d %H:%M")

    # Database stats
    user_count = db.get_user_count()
    video_count = db.get_video_count()

    system_text = f"""🖥️ **System Information**

🐍 **Python:** {python_version}
💻 **Platform:** {platform_info}
⏰ **Current Time:** {current_time}
⏱️ **Uptime:** {uptime_str}

📊 **Database:**
\\• Users: {user_count}
\\• Videos: {video_count}

🤖 **Bot Configuration:**
\\• Owner: {OWNER}
\\• Admins: {len(ADMINS)}
\\• Channels: {len([c for c in [CHANNEL_1, CHANNEL_2] if c])}
\\• Join Requirement: {"✅" if join_access_enabled else "❌"}"""

    bot.reply_to(message, system_text, parse_mode='MarkdownV2')


@bot.message_handler(commands=['video_manage'])
def video_manage_command(message):
    """Video management interface (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    videos = db.get_all_videos()
    if not videos:
        bot.reply_to(message,
                     "📪 No videos in database\\.",
                     parse_mode='MarkdownV2')
        return

    keyboard = types.InlineKeyboardMarkup(row_width=1)

    # Show first 10 videos with management options
    for video in videos[:10]:
        video_name = video['name'][:25] + "..." if len(
            video['name']) > 25 else video['name']
        keyboard.add(
            types.InlineKeyboardButton(
                f"🎥 {video_name}",
                callback_data=f"manage_video_{video['id']}"))

    if len(videos) > 10:
        keyboard.add(
            types.InlineKeyboardButton("➡️ Show More",
                                       callback_data="admin_videos_more"))

    bot.send_message(
        message.chat.id,
        f"🎥 **Video Management** \\({len(videos)} total\\)\n\nSelect a video to manage:",
        reply_markup=keyboard,
        parse_mode='MarkdownV2')


# ===== ENHANCED USER MANAGEMENT COMMANDS =====


@bot.message_handler(commands=['ban_user'])
def ban_user_command(message):
    """Ban a user (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    args = message.text.split()
    if len(args) < 2:
        bot.reply_to(
            message,
            "❌ Please provide a user ID, e\\.g\\., `/ban_user 123456789 Spam`",
            parse_mode='MarkdownV2')
        return

    try:
        user_id_to_ban = int(args[1])
        reason = ' '.join(args[2:]) if len(args) > 2 else "No reason provided"

        if user_id_to_ban == OWNER:
            bot.reply_to(message,
                         "❌ Cannot ban the bot owner\\.",
                         parse_mode='MarkdownV2')
            return

        if user_id_to_ban in ADMINS:
            bot.reply_to(message,
                         "❌ Cannot ban other admins\\.",
                         parse_mode='MarkdownV2')
            return

        if db.ban_user(user_id_to_ban, message.from_user.id, reason):
            escaped_reason = escape_markdown_v2(reason)
            bot.reply_to(
                message,
                f"✅ User `{user_id_to_ban}` has been banned\\.\n**Reason:** {escaped_reason}",
                parse_mode='MarkdownV2')
        else:
            bot.reply_to(message,
                         f"❌ Failed to ban user `{user_id_to_ban}`\\.",
                         parse_mode='MarkdownV2')
    except ValueError:
        bot.reply_to(
            message,
            "❌ Invalid user ID\\. Please provide a numeric user ID\\.",
            parse_mode='MarkdownV2')


@bot.message_handler(commands=['unban_user'])
def unban_user_command(message):
    """Unban a user (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    args = message.text.split()
    if len(args) < 2:
        bot.reply_to(
            message,
            "❌ Please provide a user ID, e\\.g\\., `/unban_user 123456789`",
            parse_mode='MarkdownV2')
        return

    try:
        user_id_to_unban = int(args[1])

        if db.unban_user(user_id_to_unban):
            bot.reply_to(message,
                         f"✅ User `{user_id_to_unban}` has been unbanned\\.",
                         parse_mode='MarkdownV2')
        else:
            bot.reply_to(
                message,
                f"❌ User `{user_id_to_unban}` was not banned or doesn't exist\\.",
                parse_mode='MarkdownV2')
    except ValueError:
        bot.reply_to(
            message,
            "❌ Invalid user ID\\. Please provide a numeric user ID\\.",
            parse_mode='MarkdownV2')


@bot.message_handler(commands=['banned_users'])
def banned_users_command(message):
    """List all banned users (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    banned_users = db.get_banned_users()
    if not banned_users:
        bot.reply_to(message,
                     "✅ No users are currently banned\\.",
                     parse_mode='MarkdownV2')
        return

    response_text = f"🚫 **Banned Users** \\({len(banned_users)} total\\)\n\n"
    for user in banned_users[:10]:  # Show up to 10 banned users
        user_display = f"@{escape_markdown_v2(user['username'])}" if user[
            'username'] else f"{user['first_name'] or 'Unknown'}"
        banned_date = user['banned_at'][:10] if user['banned_at'] else 'Unknown'
        reason = escape_markdown_v2(user['reason'][:30] +
                                    "..." if len(user['reason']) >
                                    30 else user['reason'])
        response_text += f"• **{user_display}** \\(`{user['user_id']}`\\)\n  Banned: {banned_date} \\- {reason}\n\n"

    if len(banned_users) > 10:
        response_text += f"\\.\\.\\. and {len(banned_users) - 10} more\\."

    bot.reply_to(message, response_text, parse_mode='MarkdownV2')


@bot.message_handler(commands=['user_info'])
def user_info_command(message):
    """Get detailed information about a user (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    args = message.text.split()
    if len(args) < 2:
        bot.reply_to(
            message,
            "❌ Please provide a user ID, e\\.g\\., `/user_info 123456789`",
            parse_mode='MarkdownV2')
        return

    try:
        user_id = int(args[1])

        # Get user details
        users = db.search_users(str(user_id))
        if not users:
            bot.reply_to(message,
                         f"❌ User `{user_id}` not found in database\\.",
                         parse_mode='MarkdownV2')
            return

        user = users[0]
        is_banned = db.is_user_banned(user_id)
        recent_activity = db.get_user_activity(user_id, 5)

        # Format user info
        user_display = f"@{escape_markdown_v2(user['username'])}" if user[
            'username'] else "No username"
        first_name = escape_markdown_v2(
            user['first_name']) if user['first_name'] else "Unknown"
        joined_date = user['joined_at'][:10] if user['joined_at'] else 'Unknown'
        last_activity = user['last_activity'][:16] if user[
            'last_activity'] else 'Unknown'

        info_text = f"""👤 **User Information**

**ID:** `{user['user_id']}`
**Name:** {first_name}
**Username:** {user_display}
**Joined:** {joined_date}
**Last Active:** {last_activity}
**Status:** {"🚫 Banned" if is_banned else "✅ Active"}

📋 **Recent Activity:**"""

        for activity in recent_activity:
            action = escape_markdown_v2(activity['action'])
            timestamp = activity['timestamp'][:16] if activity[
                'timestamp'] else 'Unknown'
            info_text += f"\n• {action} \\- {timestamp}"

        if not recent_activity:
            info_text += "\n• No recent activity"

        bot.reply_to(message, info_text, parse_mode='MarkdownV2')

    except ValueError:
        bot.reply_to(
            message,
            "❌ Invalid user ID\\. Please provide a numeric user ID\\.",
            parse_mode='MarkdownV2')


@bot.message_handler(commands=['search_users'])
def search_users_command(message):
    """Search for users by username or name (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    query = ' '.join(message.text.split()[1:])
    if not query:
        bot.reply_to(
            message,
            "❌ Please provide a search query, e\\.g\\., `/search_users john`",
            parse_mode='MarkdownV2')
        return

    users = db.search_users(query)
    if not users:
        bot.reply_to(
            message,
            f"❌ No users found matching '`{escape_markdown_v2(query)}`'\\.",
            parse_mode='MarkdownV2')
        return

    response_text = f"🔍 **User Search Results** \\({len(users)} found\\)\n\n"
    for user in users[:10]:  # Show up to 10 results
        user_display = f"@{escape_markdown_v2(user['username'])}" if user[
            'username'] else "No username"
        first_name = escape_markdown_v2(
            user['first_name']) if user['first_name'] else "Unknown"
        last_active = user['last_activity'][:10] if user[
            'last_activity'] else 'Unknown'
        is_banned = "🚫" if db.is_user_banned(user['user_id']) else "✅"

        response_text += f"• **{first_name}** {user_display} \\(`{user['user_id']}`\\)\n  Last active: {last_active} {is_banned}\n\n"

    if len(users) > 10:
        response_text += f"\\.\\.\\. and {len(users) - 10} more results\\."

    bot.reply_to(message, response_text, parse_mode='MarkdownV2')


@bot.message_handler(commands=['recent_activity'])
def recent_activity_command(message):
    """Show recent user activity (admin only)"""
    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 This command is for admins only\\.",
                     parse_mode='MarkdownV2')
        return

    activities = db.get_recent_activity(15)
    if not activities:
        bot.reply_to(message,
                     "📝 No recent activity recorded\\.",
                     parse_mode='MarkdownV2')
        return

    response_text = f"📝 **Recent Activity** \\({len(activities)} events\\)\n\n"
    for activity in activities:
        user_display = f"@{escape_markdown_v2(activity['username'])}" if activity[
            'username'] else f"{activity['first_name'] or 'Unknown'}"
        action = escape_markdown_v2(activity['action'])
        timestamp = activity['timestamp'][5:16] if activity[
            'timestamp'] else 'Unknown'
        details = escape_markdown_v2(
            activity['details'][:30] + "..." if len(activity['details']) >
            30 else activity['details']) if activity['details'] else ""

        response_text += f"• **{user_display}** \\- {action}\n  {timestamp}"
        if details:
            response_text += f" \\- {details}"
        response_text += "\n\n"

    bot.reply_to(message, response_text, parse_mode='MarkdownV2')


# Enhanced callback handler for admin features
@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_"))
def admin_callback_handler(call):
    """Handle admin panel callbacks"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    data = call.data

    # Check admin permissions
    if user_id not in ADMINS and user_id != OWNER:
        bot.answer_callback_query(call.id,
                                  "❌ Admin access required",
                                  show_alert=True)
        return

    # Log admin activity
    try:
        db.log_user_activity(user_id, "admin_callback",
                             f"Used admin panel: {data}")
    except Exception as e:
        print(f"Failed to log admin activity: {e}")

    bot.answer_callback_query(call.id)

    if data == "admin_panel_main":
        # Recreate the main admin panel
        keyboard = types.InlineKeyboardMarkup(row_width=2)

        # Statistics and Info
        keyboard.add(
            types.InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
            types.InlineKeyboardButton("👥 Users", callback_data="admin_users"))

        # Video Management
        keyboard.add(
            types.InlineKeyboardButton("🎥 Videos",
                                       callback_data="admin_videos"),
            types.InlineKeyboardButton("🔍 Search",
                                       callback_data="admin_search"))

        # User Management
        keyboard.add(
            types.InlineKeyboardButton("🚫 Banned",
                                       callback_data="admin_banned"),
            types.InlineKeyboardButton("📝 Activity",
                                       callback_data="admin_activity"))

        # System Controls
        keyboard.add(
            types.InlineKeyboardButton("🔧 Settings",
                                       callback_data="admin_settings"),
            types.InlineKeyboardButton("📢 Broadcast",
                                       callback_data="admin_broadcast"))

        # Channel Broadcasting and Advanced Features
        keyboard.add(
            types.InlineKeyboardButton(
                "📺 Channel Broadcast",
                callback_data="admin_channel_broadcast"),
            types.InlineKeyboardButton("📊 Analytics",
                                       callback_data="admin_analytics"))

        keyboard.add(
            types.InlineKeyboardButton("📝 Templates",
                                       callback_data="admin_templates"),
            types.InlineKeyboardButton("⏰ Schedule",
                                       callback_data="admin_schedule"))

        keyboard.add(
            types.InlineKeyboardButton("🔧 Bulk Operations",
                                       callback_data="admin_bulk"))

        panel_text = """🎛️ **Admin Control Panel**
        
Welcome to the admin dashboard\\! Use the buttons below to manage the bot\\."""

        bot.edit_message_text(panel_text,
                              chat_id,
                              call.message.message_id,
                              reply_markup=keyboard,
                              parse_mode='MarkdownV2')

    elif data == "admin_stats":
        # Show stats
        user_count = db.get_user_count()
        video_count = db.get_video_count()
        join_status = "enabled" if join_access_enabled else "disabled"

        stats_text = f"""📊 **Quick Stats**
        
👥 Users: {user_count}
🎥 Videos: {video_count}  
🔧 Join Requirement: {join_status}"""

        bot.edit_message_text(stats_text,
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')

    elif data == "admin_users":
        user_count = db.get_user_count()
        users_text = f"""👥 **User Management**
        
Total Users: {user_count}

Use /cleanup\\_users to remove inactive users
Use /broadcast to send messages to all users"""

        bot.edit_message_text(users_text,
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')

    elif data == "admin_videos":
        video_count = db.get_video_count()
        recent = db.get_recent_videos(3)

        videos_text = f"""🎥 **Video Management**
        
Total Videos: {video_count}

Recent uploads:"""

        for video in recent:
            name = escape_markdown_v2(video['name'][:30] +
                                      "..." if len(video['name']) >
                                      30 else video['name'])
            videos_text += f"\n\\• {name}"

        if not recent:
            videos_text += "\n\\• No videos yet"

        videos_text += "\n\nUse /video\\_manage for detailed management"

        bot.edit_message_text(videos_text,
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')

    elif data == "admin_settings":
        join_status = "✅ Enabled" if join_access_enabled else "❌ Disabled"
        channels = [c for c in [CHANNEL_1, CHANNEL_2] if c]

        settings_text = f"""🔧 **Bot Settings**
        
Channel Join Requirement: {join_status}
Configured Channels: {len(channels)}

Commands:
\\• /join\\_access \\- Toggle channel requirement
\\• /system\\_info \\- Detailed system info"""

        bot.edit_message_text(settings_text,
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')

    elif data == "admin_search":
        search_text = f"""🔍 **Search Videos**
        
Use the following commands to search:
\\• /find \\<query\\> \\- Search videos by name/description
\\• /database \\- List all videos with IDs

Example: `/find funny` to search for videos with "funny" in the name"""

        bot.edit_message_text(search_text,
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')

    elif data == "admin_broadcast":
        broadcast_text = f"""📢 **Broadcast Messages**
        
To send a message to all users:
\\• `/broadcast Your message here`

Example: `/broadcast New videos available\\!`

The message will be sent to all registered users\\."""

        bot.edit_message_text(broadcast_text,
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')

    elif data == "admin_banned":
        banned_users = db.get_banned_users()
        banned_text = f"""🚫 **Banned Users Management**
        
Total Banned Users: {len(banned_users)}

Commands:
\\• `/banned\\_users` \\- List all banned users
\\• `/ban\\_user <id> <reason>` \\- Ban a user
\\• `/unban\\_user <id>` \\- Unban a user
\\• `/user\\_info <id>` \\- Get user details

Recent bans:"""

        recent_banned = banned_users[:3] if banned_users else []
        for user in recent_banned:
            user_display = f"@{escape_markdown_v2(user['username'])}" if user[
                'username'] else f"{user['first_name'] or 'Unknown'}"
            banned_text += f"\n\\• {user_display} \\- {user['banned_at'][:10] if user['banned_at'] else 'Unknown'}"

        if not recent_banned:
            banned_text += "\n\\• No banned users"

        bot.edit_message_text(banned_text,
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')

    elif data == "admin_activity":
        activities = db.get_recent_activity(5)
        user_stats = db.get_user_stats_detailed()

        activity_text = f"""📝 **User Activity Monitor**
        
Active Users \\(7d\\): {user_stats['active_users_7d']}
New Users \\(7d\\): {user_stats['new_users_7d']}

Commands:
\\• `/recent\\_activity` \\- Show detailed activity
\\• `/search\\_users <query>` \\- Find users
\\• `/user\\_info <id>` \\- User details

Recent Activity:"""

        for activity in activities:
            user_display = f"@{escape_markdown_v2(activity['username'])}" if activity[
                'username'] else f"{activity['first_name'] or 'Unknown'}"
            action = escape_markdown_v2(activity['action'])
            time_str = activity['timestamp'][5:16] if activity[
                'timestamp'] else 'Unknown'
            activity_text += f"\n\\• {user_display} \\- {action} \\({time_str}\\)"

        if not activities:
            activity_text += "\n\\• No recent activity"

        bot.edit_message_text(activity_text,
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')

    elif data == "admin_cleanup":
        user_count = db.get_user_count()
        cleanup_text = f"""🧹 **Database Cleanup**
        
Current users: {user_count}

Available cleanup options:
\\• `/cleanup\\_users` \\- Remove inactive users \\(owner only\\)

This will remove users who have blocked the bot\\."""

        bot.edit_message_text(cleanup_text,
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')

    elif data == "admin_videos_more":
        # Handle video pagination
        videos = db.get_all_videos()
        keyboard = types.InlineKeyboardMarkup(row_width=1)

        # Show videos 11-20
        for video in videos[10:20]:
            video_name = video['name'][:25] + "..." if len(
                video['name']) > 25 else video['name']
            keyboard.add(
                types.InlineKeyboardButton(
                    f"🎥 {video_name}",
                    callback_data=f"manage_video_{video['id']}"))

        if len(videos) > 20:
            keyboard.add(
                types.InlineKeyboardButton("➡️ Show Even More",
                                           callback_data="admin_videos_more2"))

        keyboard.add(
            types.InlineKeyboardButton("⬅️ Back to First 10",
                                       callback_data="admin_videos"))

        videos_text = f"""🎥 **Video Management** \\(Videos 11\\-20 of {len(videos)}\\)

Select a video to manage:"""

        bot.edit_message_text(videos_text,
                              chat_id,
                              call.message.message_id,
                              reply_markup=keyboard,
                              parse_mode='MarkdownV2')

    elif data == "admin_videos_more2":
        # Handle further pagination
        videos = db.get_all_videos()
        keyboard = types.InlineKeyboardMarkup(row_width=1)

        # Show remaining videos from 21+
        for video in videos[20:30]:
            video_name = video['name'][:25] + "..." if len(
                video['name']) > 25 else video['name']
            keyboard.add(
                types.InlineKeyboardButton(
                    f"🎥 {video_name}",
                    callback_data=f"manage_video_{video['id']}"))

        keyboard.add(
            types.InlineKeyboardButton("⬅️ Back to Videos 11-20",
                                       callback_data="admin_videos_more"))
        keyboard.add(
            types.InlineKeyboardButton("🏠 Back to Panel",
                                       callback_data="admin_videos"))

        videos_text = f"""🎥 **Video Management** \\(Videos 21\\+ of {len(videos)}\\)

Select a video to manage:"""

        bot.edit_message_text(videos_text,
                              chat_id,
                              call.message.message_id,
                              reply_markup=keyboard,
                              parse_mode='MarkdownV2')

    elif data == "admin_database" and user_id == OWNER:
        user_count = db.get_user_count()
        video_count = db.get_video_count()

        db_text = f"""🗄️ **Database Status**
        
Users Table: {user_count} records
Videos Table: {video_count} records

⚠️ **Maintenance Commands:**
\\• /cleanup\\_users \\- Remove inactive users

*Use with caution\\!*"""

        bot.edit_message_text(db_text,
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')

    elif data == "admin_channel_broadcast":
        if not CHANNEL_1 and not CHANNEL_2:
            bot.edit_message_text(
                "❌ No channels configured\\. Please set CHANNEL\\_1 and/or CHANNEL\\_2 environment variables\\.",
                chat_id,
                call.message.message_id,
                parse_mode='MarkdownV2')
            return

        keyboard = types.InlineKeyboardMarkup()

        if CHANNEL_1:
            keyboard.add(
                types.InlineKeyboardButton(
                    f"📢 Channel 1 ({CHANNEL_1})",
                    callback_data="broadcast_channel_1"))
        if CHANNEL_2:
            keyboard.add(
                types.InlineKeyboardButton(
                    f"📢 Channel 2 ({CHANNEL_2})",
                    callback_data="broadcast_channel_2"))

        keyboard.add(
            types.InlineKeyboardButton("⬅️ Back to Panel",
                                       callback_data="admin_panel_main"))

        bot.edit_message_text(
            "📢 **Channel Broadcast**\n\nSelect which channel to broadcast to:",
            chat_id,
            call.message.message_id,
            reply_markup=keyboard,
            parse_mode='MarkdownV2')

    elif data == "admin_analytics":
        summary = db.get_analytics_summary()
        popular_videos = db.get_popular_videos(3)

        analytics_text = f"""📊 **Video Analytics Summary**

📈 **Overview:**
\\• Total Views: {summary['total_views']}
\\• Views Today: {summary['views_today']}
\\• Top Video: {escape_markdown_v2(summary['top_video']['name']) if summary['top_video'] else 'N/A'}

🔥 **Top 3 Videos:**"""

        for i, video in enumerate(popular_videos, 1):
            video_name = escape_markdown_v2(video['name'][:25] +
                                            "..." if len(video['name']) >
                                            25 else video['name'])
            analytics_text += f"\n{i}\\. {video_name} \\- {video['view_count']} views"

        if not popular_videos:
            analytics_text += "\nNo views recorded yet\\."

        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("📊 Detailed Analytics",
                                       callback_data="analytics_detailed"))
        keyboard.add(
            types.InlineKeyboardButton("⬅️ Back to Panel",
                                       callback_data="admin_panel_main"))

        bot.edit_message_text(analytics_text,
                              chat_id,
                              call.message.message_id,
                              reply_markup=keyboard,
                              parse_mode='MarkdownV2')

    elif data == "admin_templates":
        templates = db.get_templates()

        keyboard = types.InlineKeyboardMarkup(row_width=1)

        if templates:
            for template in templates[:5]:  # Show first 5 templates
                template_name = template['name'][:20] + "..." if len(
                    template['name']) > 20 else template['name']
                keyboard.add(
                    types.InlineKeyboardButton(
                        f"📝 {template_name}",
                        callback_data=f"template_view_{template['id']}"))

        keyboard.add(
            types.InlineKeyboardButton("➕ Create Template",
                                       callback_data="template_create"))
        keyboard.add(
            types.InlineKeyboardButton("⬅️ Back to Panel",
                                       callback_data="admin_panel_main"))

        templates_text = f"📝 **Message Templates** \\({len(templates)} total\\)\n\nManage your broadcast templates:"
        bot.edit_message_text(templates_text,
                              chat_id,
                              call.message.message_id,
                              reply_markup=keyboard,
                              parse_mode='MarkdownV2')

    elif data == "admin_schedule":
        scheduled_broadcasts = db.get_scheduled_broadcasts(user_id)
        pending_count = len(
            [b for b in scheduled_broadcasts if b['status'] == 'pending'])

        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("➕ Schedule New",
                                       callback_data="schedule_new"))

        if scheduled_broadcasts:
            keyboard.add(
                types.InlineKeyboardButton("📋 View All",
                                           callback_data="schedule_view"))

        keyboard.add(
            types.InlineKeyboardButton("⬅️ Back to Panel",
                                       callback_data="admin_panel_main"))

        schedule_text = f"""⏰ **Scheduled Broadcasts**

📊 **Your Schedules:**
\\• Total: {len(scheduled_broadcasts)}
\\• Pending: {pending_count}
\\• Completed: {len(scheduled_broadcasts) - pending_count}"""

        bot.edit_message_text(schedule_text,
                              chat_id,
                              call.message.message_id,
                              reply_markup=keyboard,
                              parse_mode='MarkdownV2')

    elif data == "admin_bulk":
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("🗑️ Bulk Delete Videos",
                                       callback_data="bulk_delete_videos"),
            types.InlineKeyboardButton("🚫 Bulk Ban Users",
                                       callback_data="bulk_ban_users"))
        keyboard.add(
            types.InlineKeyboardButton("📊 Export Data",
                                       callback_data="bulk_export"),
            types.InlineKeyboardButton("🧹 Clean Database",
                                       callback_data="bulk_clean"))
        keyboard.add(
            types.InlineKeyboardButton("⬅️ Back to Panel",
                                       callback_data="admin_panel_main"))

        bulk_text = """🔧 **Bulk Operations**

⚠️ **Warning:** These operations affect multiple items\\. Use with caution\\.

Select an operation:"""

        bot.edit_message_text(bulk_text,
                              chat_id,
                              call.message.message_id,
                              reply_markup=keyboard,
                              parse_mode='MarkdownV2')


@bot.callback_query_handler(
    func=lambda call: call.data.startswith("manage_video_"))
def video_manage_callback(call):
    """Handle video management callbacks"""
    if call.from_user.id not in ADMINS and call.from_user.id != OWNER:
        bot.answer_callback_query(call.id,
                                  "❌ Admin access required",
                                  show_alert=True)
        return

    video_id = call.data.replace("manage_video_", "")
    video = db.get_video_by_id(video_id)

    if not video:
        bot.answer_callback_query(call.id,
                                  "❌ Video not found",
                                  show_alert=True)
        return

    bot.answer_callback_query(call.id)

    # Create management buttons for this video
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("🎬 Preview",
                                   callback_data=f"preview_video_{video_id}"),
        types.InlineKeyboardButton("🗑️ Delete",
                                   callback_data=f"delete_video_{video_id}"))
    keyboard.add(
        types.InlineKeyboardButton("⬅️ Back", callback_data="admin_videos"))

    video_info = f"""🎥 **Video Details**
    
**Name:** {escape_markdown_v2(video['name'])}
**ID:** `{video_id}`
**Description:** {escape_markdown_v2(video['description'] or 'No description')}

Choose an action:"""

    bot.edit_message_text(video_info,
                          call.message.chat.id,
                          call.message.message_id,
                          reply_markup=keyboard,
                          parse_mode='MarkdownV2')


@bot.callback_query_handler(func=lambda call: call.data.startswith(
    ("preview_video_", "delete_video_")))
def video_action_callback(call):
    """Handle video preview/delete actions"""
    if call.from_user.id not in ADMINS and call.from_user.id != OWNER:
        bot.answer_callback_query(call.id,
                                  "❌ Admin access required",
                                  show_alert=True)
        return

    bot.answer_callback_query(call.id)

    if call.data.startswith("preview_video_"):
        video_id = call.data.replace("preview_video_", "")
        video = db.get_video_by_id(video_id)

        if video:
            caption = f"🎥 *{escape_markdown_v2(video['name'])}*\n\n{escape_markdown_v2(video['description'] or 'No description')}\n\n📋 ID: `{video_id}`"
            bot.send_video(call.message.chat.id,
                           video['file_id'],
                           caption=caption,
                           parse_mode='MarkdownV2')

    elif call.data.startswith("delete_video_"):
        video_id = call.data.replace("delete_video_", "")
        video = db.get_video_by_id(video_id)

        if video and db.delete_video(video_id):
            bot.edit_message_text(
                f"✅ Video '{escape_markdown_v2(video['name'])}' deleted successfully\\!",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='MarkdownV2')
        else:
            bot.edit_message_text("❌ Failed to delete video\\.",
                                  call.message.chat.id,
                                  call.message.message_id,
                                  parse_mode='MarkdownV2')


@bot.callback_query_handler(func=lambda call: call.data.startswith(
    ("broadcast_channel_", "broadcast_cancel")))
def handle_channel_broadcast_callback(call):
    """Handle channel broadcast callbacks with enhanced security"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id

    # Re-validate admin/owner permissions on every callback
    if user_id not in ADMINS and user_id != OWNER:
        bot.answer_callback_query(call.id,
                                  "❌ Admin access required",
                                  show_alert=True)
        return

    bot.answer_callback_query(call.id)

    if call.data == "broadcast_cancel":
        # Clean up state and confirm cancellation
        if user_id in user_states:
            del user_states[user_id]
        bot.edit_message_text("❌ Channel broadcast cancelled\\.",
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')
        db.log_user_activity(user_id, "broadcast_cancelled",
                             "Channel broadcast cancelled")
        return

    # Handle channel selection with validation
    target_channel = None
    channel_name = None

    if call.data == "broadcast_channel_1" and CHANNEL_1:
        target_channel = CHANNEL_1
        channel_name = "Channel 1"
    elif call.data == "broadcast_channel_2" and CHANNEL_2:
        target_channel = CHANNEL_2
        channel_name = "Channel 2"
    else:
        bot.edit_message_text("❌ Invalid channel selection\\.",
                              chat_id,
                              call.message.message_id,
                              parse_mode='MarkdownV2')
        return

    # Store the selected channel with initiator verification
    user_states[user_id] = {
        'state': BROADCAST_STATES['AWAITING_CONTENT'],
        'broadcast_type': 'channel',
        'target_channel': target_channel,
        'channel_name': channel_name,
        'initiator_id': user_id,  # Track who started this flow
        'chat_id': chat_id  # Track original chat
    }

    # Log activity
    db.log_user_activity(
        user_id, "broadcast_started",
        f"Started broadcast to {channel_name} ({target_channel})")

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("❌ Cancel Broadcast",
                                   callback_data="broadcast_cancel"))

    bot.edit_message_text(
        f"📢 **Broadcasting to {channel_name}** \\(`{escape_markdown_v2(target_channel)}`\\)\n\n"
        "Now send me:\n"
        "\\• 📷 **Photo/Image** with caption \\(text \\+ links\\)\n"
        "\\• 📝 **Text message** \\(with optional links\\)\n\n"
        "*The content will be sent exactly as you provide it\\.*",
        chat_id,
        call.message.message_id,
        reply_markup=keyboard,
        parse_mode='MarkdownV2')


@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    """Handle photo uploads for channel broadcasting with enhanced security"""
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    # Check if user is banned first
    if not check_user_access(user_id, username, first_name, "photo_upload"):
        send_banned_message(message)
        return

    # Check if this is for channel broadcasting
    if user_id in user_states and user_states[user_id].get(
            'broadcast_type') == 'channel':
        state = user_states[user_id]

        # Verify user identity and state validity
        if (state.get('state') != BROADCAST_STATES['AWAITING_CONTENT']
                or state.get('initiator_id') != user_id
                or user_id not in ADMINS and user_id != OWNER):
            # Clean up invalid state
            if user_id in user_states:
                del user_states[user_id]
            bot.reply_to(
                message,
                "❌ Invalid broadcast state\\. Please start over with /channel\\_broadcast\\.",
                parse_mode='MarkdownV2')
            return

        target_channel = state['target_channel']
        channel_name = state['channel_name']

        # Get the highest resolution photo
        photo = message.photo[-1]
        caption = message.caption or ""

        # Validate caption length (Telegram limit is 1024 characters)
        if len(caption) > 1024:
            bot.reply_to(
                message,
                "❌ Caption too long\\! Maximum 1024 characters allowed\\.",
                parse_mode='MarkdownV2')
            return

        try:
            # Normalize channel identifier
            if target_channel.startswith('@'):
                channel_id = target_channel
            elif target_channel.startswith('-'):
                channel_id = int(target_channel)  # Numeric chat ID
            else:
                channel_id = f'@{target_channel}'

            # Send photo to channel
            bot.send_photo(
                channel_id,
                photo.file_id,
                caption=caption,
                parse_mode=None  # Send caption as-is to preserve formatting
            )

            # Log successful broadcast
            db.log_user_activity(
                user_id, "photo_broadcast",
                f"Photo sent to {channel_name} ({target_channel})")

            # Notify admin of success
            bot.reply_to(
                message,
                f"✅ Photo broadcast sent to {channel_name} (`{target_channel}`)!",
                parse_mode=None)

            # Clear user state
            del user_states[user_id]

        except ApiTelegramException as e:
            error_msg = str(e)
            if "not found" in error_msg.lower():
                error_response = f"❌ Channel not found: {target_channel}"
            elif "not enough rights" in error_msg.lower(
            ) or "forbidden" in error_msg.lower():
                error_response = f"❌ Bot lacks permissions to post in {target_channel}"
            else:
                error_response = f"❌ Failed to send photo: {error_msg}"

            bot.reply_to(message, error_response, parse_mode=None)
            db.log_user_activity(
                user_id, "photo_broadcast_failed",
                f"Failed to send to {target_channel}: {error_msg}")

            # Clean up state on error
            if user_id in user_states:
                del user_states[user_id]

        except Exception as e:
            bot.reply_to(message,
                         f"❌ Unexpected error: `{escape_markdown_v2(str(e))}`",
                         parse_mode='MarkdownV2')
            print(f"Failed to send photo to {target_channel}: {e}")
            db.log_user_activity(user_id, "photo_broadcast_error",
                                 f"Unexpected error: {str(e)}")

            # Clean up state on error
            if user_id in user_states:
                del user_states[user_id]

        return

    # If not admin or not broadcasting, ignore
    if user_id not in ADMINS and user_id != OWNER:
        return


@bot.message_handler(content_types=['video'])
def handle_video(message):
    """Handle video uploads from admins"""
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    # Check if user is banned first (ban overrides admin status)
    if not check_user_access(user_id, username, first_name, "video_upload"):
        send_banned_message(message)
        return

    if message.from_user.id not in ADMINS and message.from_user.id != OWNER:
        bot.reply_to(message,
                     "🚫 Only admins can upload videos\\.",
                     parse_mode='MarkdownV2')
        return

    user_states[user_id] = {
        'state': 'awaiting_name',
        'file_id': message.video.file_id
    }
    bot.reply_to(message,
                 "📛 Please send the name for this video\\:",
                 parse_mode='MarkdownV2')


@bot.message_handler(content_types=['text'])
def handle_text(message):
    """Handle text input for video name and description"""
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    # Check if user is banned
    if not check_user_access(user_id, username, first_name, "text_input"):
        send_banned_message(message)
        return

    if user_id not in user_states:
        return

    state_info = user_states[user_id]
    state = state_info.get('state')

    # Handle channel broadcasting text content with enhanced security
    if state_info.get(
            'broadcast_type'
    ) == 'channel' and state == BROADCAST_STATES['AWAITING_CONTENT']:
        # Verify user identity and state validity
        if (state_info.get('initiator_id') != user_id
                or user_id not in ADMINS and user_id != OWNER):
            # Clean up invalid state
            if user_id in user_states:
                del user_states[user_id]
            bot.reply_to(
                message,
                "❌ Invalid broadcast state\\. Please start over with /channel\\_broadcast\\.",
                parse_mode='MarkdownV2')
            return

        target_channel = state_info['target_channel']
        channel_name = state_info['channel_name']
        text_content = message.text

        # Validate text length (Telegram limit is 4096 characters for messages)
        if len(text_content) > 4096:
            bot.reply_to(
                message,
                "❌ Message too long\\! Maximum 4096 characters allowed\\.",
                parse_mode='MarkdownV2')
            return

        try:
            # Normalize channel identifier
            if target_channel.startswith('@'):
                channel_id = target_channel
            elif target_channel.startswith('-'):
                channel_id = int(target_channel)  # Numeric chat ID
            else:
                channel_id = f'@{target_channel}'

            # Send text message to channel
            bot.send_message(
                channel_id,
                text_content,
                parse_mode=None,  # Send text as-is to preserve formatting
                disable_web_page_preview=False  # Allow link previews
            )

            # Log successful broadcast
            db.log_user_activity(
                user_id, "text_broadcast",
                f"Text sent to {channel_name} ({target_channel})")

            # Notify admin of success
            bot.reply_to(
                message,
                f"✅ Text broadcast sent to {channel_name} (`{target_channel}`)!",
                parse_mode=None)

            # Clear user state
            del user_states[user_id]

        except ApiTelegramException as e:
            error_msg = str(e)
            if "not found" in error_msg.lower():
                error_response = f"❌ Channel not found: {target_channel}"
            elif "not enough rights" in error_msg.lower(
            ) or "forbidden" in error_msg.lower():
                error_response = f"❌ Bot lacks permissions to post in {target_channel}"
            else:
                error_response = f"❌ Failed to send message: {error_msg}"

            bot.reply_to(message, error_response, parse_mode=None)
            db.log_user_activity(
                user_id, "text_broadcast_failed",
                f"Failed to send to {target_channel}: {error_msg}")

            # Clean up state on error
            if user_id in user_states:
                del user_states[user_id]

        except Exception as e:
            bot.reply_to(message,
                         f"❌ Unexpected error: `{escape_markdown_v2(str(e))}`",
                         parse_mode='MarkdownV2')
            print(f"Failed to send text to {target_channel}: {e}")
            db.log_user_activity(user_id, "text_broadcast_error",
                                 f"Unexpected error: {str(e)}")

            # Clean up state on error
            if user_id in user_states:
                del user_states[user_id]

        return

    # Handle video upload states
    if state == 'awaiting_name':
        user_states[user_id]['name'] = message.text
        user_states[user_id]['state'] = 'awaiting_description'
        bot.reply_to(
            message,
            "📝 Please send the description for this video \\(or type 'skip' to skip\\)\\:",
            parse_mode='MarkdownV2')

    elif state == 'awaiting_description':
        description = message.text if message.text.lower() != 'skip' else ''
        file_id = user_states[user_id]['file_id']
        name = user_states[user_id]['name']

        video_id = db.add_video(file_id, name, description)

        bot_info = bot.get_me()
        bot_username = bot_info.username or ""
        if not bot_username:
            bot.reply_to(
                message,
                "⚠️ Unable to determine bot username; cannot create shareable URL\\.",
                parse_mode='MarkdownV2')
            del user_states[user_id]
            return
        shareable_url = f"https://t.me/{bot_username}?start={video_id}"
        escaped_shareable_url = escape_markdown_v2(shareable_url)

        bot.reply_to(
            message,
            f"✅ Video uploaded successfully\\! ID: `{escape_markdown_v2(str(video_id))}`\n\n**Shareable URL:** {escaped_shareable_url}",
            parse_mode='MarkdownV2')
        del user_states[user_id]


@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    """Handle button clicks"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    data = call.data
    username = call.from_user.username
    first_name = call.from_user.first_name

    # Check if user is banned
    if not check_user_access(user_id, username, first_name,
                             f"callback:{data}"):
        bot.answer_callback_query(call.id,
                                  "❌ You are banned from using this bot",
                                  show_alert=True)
        return

    bot.answer_callback_query(call.id)  # Acknowledge the button press

    if data.startswith("video_"):
        video_id = data.split("_")[1]
        if join_access_enabled and not check_channel_membership(user_id):
            prompt_join_channels(chat_id, user_id, f"video_{video_id}")
            return

        video = db.get_video_by_id(video_id)
        if video:
            # Log video view for analytics
            db.log_video_view(video_id, user_id)

            caption_text = f"🎥 *{escape_markdown_v2(video['name'])}*\n\n{escape_markdown_v2(video['description'])}"
            sent_message = bot.send_video(chat_id,
                                          video['file_id'],
                                          caption=caption_text,
                                          supports_streaming=True,
                                          parse_mode='MarkdownV2')
            threading.Thread(target=delete_video_message,
                             args=(chat_id, sent_message.message_id)).start()
        else:
            bot.send_message(
                chat_id,
                "❌ Video not found\\. Please check the ID and try again\\.",
                parse_mode='MarkdownV2')

    elif data == "bulk_delete_videos":
        # Check admin access
        if user_id not in ADMINS and user_id != OWNER:
            bot.answer_callback_query(call.id,
                                      "❌ Admin access required",
                                      show_alert=True)
            return

        videos = db.get_all_videos()
        if not videos:
            bot.edit_message_text("📪 No videos available to delete\\.",
                                  chat_id,
                                  call.message.message_id,
                                  parse_mode='MarkdownV2')
            return

        # Create confirmation keyboard with all videos
        keyboard = types.InlineKeyboardMarkup()
        confirmation_text = f"🗑️ **Bulk Delete Videos**\n\n⚠️ **WARNING:** This will permanently delete ALL {len(videos)} videos\\!\n\nAre you sure you want to proceed\\?"

        keyboard.add(
            types.InlineKeyboardButton(
                "✅ Yes, Delete ALL", callback_data="confirm_bulk_delete_all"),
            types.InlineKeyboardButton("❌ Cancel", callback_data="admin_bulk"))

        bot.edit_message_text(confirmation_text,
                              chat_id,
                              call.message.message_id,
                              reply_markup=keyboard,
                              parse_mode='MarkdownV2')

    elif data == "confirm_bulk_delete_all":
        # Check admin access again for security
        if user_id not in ADMINS and user_id != OWNER:
            bot.answer_callback_query(call.id,
                                      "❌ Admin access required",
                                      show_alert=True)
            return

        # Delete all videos
        videos = db.get_all_videos()
        deleted_count = 0

        for video in videos:
            if db.delete_video(video['id']):
                deleted_count += 1

        # Log the bulk delete action
        db.log_user_activity(user_id, "bulk_delete_videos",
                             f"Bulk deleted {deleted_count} videos")

        success_text = f"✅ **Bulk Delete Completed**\n\nSuccessfully deleted {deleted_count} videos\\."

        # Return to admin panel
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("⬅️ Back to Admin Panel",
                                       callback_data="admin_panel_main"))

        bot.edit_message_text(success_text,
                              chat_id,
                              call.message.message_id,
                              reply_markup=keyboard,
                              parse_mode='MarkdownV2')

    elif data.startswith("retry_"):
        original_command_data = data.split("_", 1)[1]
        if not check_channel_membership(user_id):
            prompt_join_channels(chat_id, user_id, data)
            return

        if original_command_data.startswith("start_"):
            video_id = original_command_data.split("_")[1]
            video = db.get_video_by_id(video_id)
            if video:
                caption_text = f"🎥 *{escape_markdown_v2(video['name'])}*\n\n{escape_markdown_v2(video['description'])}"
                sent_message = bot.send_video(chat_id,
                                              video['file_id'],
                                              caption=caption_text,
                                              supports_streaming=True,
                                              parse_mode='MarkdownV2')
                threading.Thread(target=delete_video_message,
                                 args=(chat_id,
                                       sent_message.message_id)).start()
            else:
                bot.send_message(
                    chat_id,
                    "❌ Video not found\\. Please check the ID and try again\\.",
                    parse_mode='MarkdownV2')

        elif original_command_data == "browse":
            videos = db.get_all_videos()
            if not videos:
                bot.send_message(chat_id,
                                 "📪 No videos available\\.",
                                 parse_mode='MarkdownV2')
                return
            keyboard = types.InlineKeyboardMarkup(row_width=2)
            for video in videos[:20]:
                keyboard.add(
                    types.InlineKeyboardButton(
                        video['name'], callback_data=f"video_{video['id']}"))
            bot.send_message(
                chat_id,
                f"📹 Available videos \\({len(videos)} total\\)\\:",
                reply_markup=keyboard,
                parse_mode='MarkdownV2')

        elif original_command_data == "random":
            video = db.get_random_video()
            if video:
                caption_text = f"🎲 Random video: {escape_markdown_v2(video['name'])}\n\n{escape_markdown_v2(video['description'])}"
                sent_message = bot.send_video(chat_id,
                                              video['file_id'],
                                              caption=caption_text,
                                              supports_streaming=True,
                                              parse_mode='MarkdownV2')
                threading.Thread(target=delete_video_message,
                                 args=(chat_id,
                                       sent_message.message_id)).start()
            else:
                bot.send_message(chat_id,
                                 "📪 No videos available\\.",
                                 parse_mode='MarkdownV2')

        elif original_command_data.startswith("find_"):
            query = original_command_data.split("_", 1)[1]
            videos = db.search_videos(query)
            count = len(videos)
            if not videos:
                bot.send_message(chat_id,
                                 "❌ No videos found matching your query\\.",
                                 parse_mode='MarkdownV2')
                return

            response_text = f"🔍 Found {count} results for '`{escape_markdown_v2(query)}`'\\:"

            keyboard = types.InlineKeyboardMarkup(row_width=2)
            for video in videos[:20]:
                keyboard.add(
                    types.InlineKeyboardButton(
                        video['name'], callback_data=f"video_{video['id']}"))
            bot.send_message(chat_id,
                             response_text,
                             reply_markup=keyboard,
                             parse_mode='MarkdownV2')


if __name__ == "__main__":
def start_bot_services():
    """Start all bot services (Flask, scheduler, ping)"""
    global bot_info_cache
    
    print("🤖 Enhanced Video Bot starting...")
    print(f"👑 Owner User ID: {OWNER}")
    print(f"👮 Admins: {ADMINS}")
    print(f"📢 Channels: {CHANNEL_1}, {CHANNEL_2}")
    
    # Test bot connection and cache info
    bot_info = bot.get_me()
    bot_info_cache = {'username': bot_info.username}
    print(f"✅ Bot connected successfully: @{bot_info.username}")

    # Start scheduled broadcast processor in background
    scheduler_thread = threading.Thread(
        target=process_scheduled_broadcasts, daemon=True)
    scheduler_thread.start()
    print("⏰ Scheduled broadcast processor started")

    # Start keep-alive ping for Render free tier (only if URL is set)
    render_url = os.getenv('RENDER_EXTERNAL_URL')
    if render_url:
        ping_thread = threading.Thread(target=keep_alive_ping, daemon=True)
        ping_thread.start()
        print("🔄 Keep-alive ping started")
    else:
        print("⚠️ RENDER_EXTERNAL_URL not set - keep-alive ping disabled")

    # Start Flask web server in background (for health checks)
    port = int(os.getenv('PORT', 5000))  # Render will override with PORT env var
    def run_flask():
        app.run(host='0.0.0.0', port=port, debug=False)
    
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print(f"🌐 Web server started on port {port}")


def start_bot_with_retry():
    """Start bot with automatic restart on API timeout errors"""
    max_retries = 5
    retry_count = 0
    
    while True:
        try:
            print(f"🔄 Starting bot polling (attempt {retry_count + 1})")
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except ApiTelegramException as e:
            error_msg = str(e).lower()
            print(f"❌ Telegram API Error: {e}")
            
            # Handle specific timeout errors that should trigger restart
            if any(phrase in error_msg for phrase in [
                "query is too old", 
                "response timeout expired", 
                "query id is invalid",
                "conflict: terminated by other getupdates request",
                "network error",
                "connection error"
            ]):
                retry_count += 1
                if retry_count <= max_retries:
                    wait_time = min(30 * retry_count, 300)  # Progressive backoff, max 5 minutes
                    print(f"🔄 Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"❌ Max retries ({max_retries}) exceeded. Restarting with fresh connection...")
                    retry_count = 0  # Reset counter for fresh start
                    time.sleep(60)  # Wait 1 minute before fresh restart
                    continue
            else:
                # For other API errors, wait and retry
                print(f"⚠️ Unhandled API error, waiting 30 seconds before retry...")
                time.sleep(30)
                continue
                
        except requests.exceptions.RequestException as e:
            print(f"🌐 Network/Request Error: {e}")
            retry_count += 1
            if retry_count <= max_retries:
                wait_time = min(15 * retry_count, 180)  # Progressive backoff, max 3 minutes  
                print(f"🔄 Network retry in {wait_time} seconds (attempt {retry_count}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ Max network retries exceeded. Restarting...")
                retry_count = 0
                time.sleep(30)
                continue
                
        except Exception as e:
            print(f"💥 Unexpected Error: {e}")
            print(f"🔄 Restarting in 10 seconds...")
            time.sleep(10)
            retry_count = 0  # Reset for unexpected errors
            continue
            
        # If we get here, polling stopped normally (should not happen with none_stop=True)
        print("⚠️ Polling stopped unexpectedly, restarting...")
        time.sleep(5)
    try:
        # Initialize services once
        start_bot_services()
        
        # Start bot with automatic retry/restart logic
        start_bot_with_retry()
        
    except KeyboardInterrupt:
        print("\n👋 Bot shutdown requested by user")
    except Exception as e:
        print(f"💥 Fatal error in main: {e}")
        print("🔄 Attempting restart in 30 seconds...")
        time.sleep(30)
