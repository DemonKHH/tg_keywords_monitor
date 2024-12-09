import datetime
import os
import logging
import sqlite3
import asyncio
from logging.handlers import RotatingFileHandler
import uuid
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telethon.tl.functions.auth import SendCodeRequest, SignInRequest
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.tl.types import (
    ChannelParticipantSelf,
    ChannelParticipantCreator,
    ChannelParticipantAdmin,
    ChannelParticipant,
    ChannelParticipantBanned,
    ChannelParticipantLeft
)
from telethon.sessions import StringSession
from telethon.errors import PhoneCodeExpiredError, SessionPasswordNeededError, RPCError, PhoneCodeInvalidError,PasswordHashInvalidError
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    filters,
    CallbackContext
)
from telegram.helpers import escape_markdown
from telethon import TelegramClient, events, errors
from dotenv import load_dotenv
import stat

# 加载环境变量
load_dotenv()

# 配置日志记录
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # 设置日志级别为 DEBUG

# 创建日志格式
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 创建控制台日志处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # 控制台显示 DEBUG 级别及以上的日志
console_handler.setFormatter(formatter)

# 创建文件日志处理器，使用 RotatingFileHandler，并设置编码为 UTF-8
file_handler = RotatingFileHandler(
    'bot.log',  # 日志文件名
    maxBytes=5*1024*1024,  # 每个日志文件最大5MB
    backupCount=5,  # 保留5个备份文件
    encoding='utf-8'  # 明确设置文件编码为 UTF-8
)
file_handler.setLevel(logging.DEBUG)  # 文件中记录 DEBUG 级别及以上的日志
file_handler.setFormatter(formatter)

# 将处理器添加到日志器
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# 数据库文件路径
DB_PATH = 'bot.db'

# 环境变量
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_IDS = os.getenv('ADMIN_IDS')  # 逗号分隔的管理员用户 ID
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'demonkinghaha')  # 默认值为 'demonkinghaha'
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
SESSIONS_DIR = os.getenv('SESSIONS_DIR', 'sessions')

# 确保会话根目录存在
os.makedirs(SESSIONS_DIR, exist_ok=True)
# 验证必要的环境变量
required_env_vars = ['TELEGRAM_BOT_TOKEN', 'ADMIN_IDS', 'TELEGRAM_API_ID', 'TELEGRAM_API_HASH']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    logger.critical(f"未设置以下环境变量: {', '.join(missing_vars)}")
    exit(1)

# 解析管理员用户 ID
try:
    ADMIN_IDS = set(map(int, ADMIN_IDS.split(',')))
except ValueError:
    logger.error("ADMIN_IDS 必须是逗号分隔的整数。")
    ADMIN_IDS = set()

# 数据库管理类
class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.initialize_database()

    def initialize_database(self):
        logger.debug("初始化数据库连接。")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # 创建允许用户表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    username TEXT
                )
            ''')
            # 创建申请表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pending_applications (
                    application_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL UNIQUE,
                    first_name TEXT,
                    username TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # 创建配置表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            ''')
            # 创建用户配置表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_config (
                    user_id INTEGER PRIMARY KEY,
                    interval_seconds INTEGER DEFAULT 60
                )
            ''')
            # 创建推送日志表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS push_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    keyword TEXT NOT NULL,
                    chat_id INTEGER,
                    message_id INTEGER,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # 创建用户 Telegram 账号表，支持多账号
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_accounts (
                    account_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    username TEXT NOT NULL UNIQUE,
                    firstname TEXT,
                    lastname TEXT,
                    session_file TEXT NOT NULL UNIQUE,
                    is_authenticated INTEGER DEFAULT 0,
                    two_factor_enabled INTEGER DEFAULT 0,
                    FOREIGN KEY(user_id) REFERENCES allowed_users(user_id)
                )
            ''')
            # 创建用户群组监听表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_monitored_groups (
                    user_id INTEGER NOT NULL,
                    group_id INTEGER NOT NULL,
                    PRIMARY KEY (user_id, group_id),
                    FOREIGN KEY(user_id) REFERENCES allowed_users(user_id),
                    FOREIGN KEY(group_id) REFERENCES groups(group_id)
                )
            ''')
            # 创建群组信息表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS groups (
                    group_id INTEGER PRIMARY KEY,
                    group_name TEXT NOT NULL
                )
            ''')
            # 创建屏蔽用户表，新增 receiving_user_id 字段
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blocked_users (
                    receiving_user_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    first_name TEXT,
                    username TEXT,
                    PRIMARY KEY (receiving_user_id, user_id),
                    FOREIGN KEY(receiving_user_id) REFERENCES allowed_users(user_id)
                )
            ''')
            # 创建关键词表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS keywords (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    keyword TEXT NOT NULL,
                    UNIQUE(user_id, keyword)
                )
            ''')

            # 如果没有设置默认的 interval，则插入一个默认值，例如 60 秒
            cursor.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ("global_interval_seconds", "60"))

            conn.commit()
        logger.info("数据库初始化完成。")
        
    # 添加授权用户的方法
    def add_allowed_user(self, user_id, first_name, username):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO allowed_users (user_id, first_name, username) VALUES (?, ?, ?)", (user_id, first_name, username))
                conn.commit()
            logger.info(f"用户 {user_id} ({first_name}, @{username}) 已被授权。")
        except sqlite3.IntegrityError:
            logger.warning(f"用户 {user_id} ({first_name}, @{username}) 已经被授权。")
        except Exception as e:
            logger.error(f"添加授权用户失败: {e}", exc_info=True)       

    # 获取已授权的用户列表
    def get_allowed_users(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, first_name, username FROM allowed_users")
            rows = cursor.fetchall()
            return {row[0]: {'first_name': row[1], 'username': row[2]} for row in rows}

    # 添加存储用户账号信息的方法
    def add_user_account(self, user_id, username, firstname, lastname, session_file, is_authenticated=0, two_factor_enabled=0):
        if not session_file:
            raise ValueError("session_file 必须提供。")
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO user_accounts 
                (user_id, username, firstname, lastname, session_file, is_authenticated, two_factor_enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, username, firstname, lastname, session_file, is_authenticated, two_factor_enabled))
            account_id = cursor.lastrowid
            conn.commit()
        return account_id

    def get_user_accounts(self, user_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT account_id, username, firstname, lastname, session_file, is_authenticated, two_factor_enabled
                FROM user_accounts WHERE user_id = ?
            ''', (user_id,))
            return cursor.fetchall()

    def get_account_by_id(self, account_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT user_id, username, firstname, lastname, session_file, is_authenticated, two_factor_enabled
                FROM user_accounts WHERE account_id = ?
            ''', (account_id,))
            account = cursor.fetchone()  # 获取查询结果

            if account is None:
                return None  # 如果没有找到对应的账号，返回 None

            # 返回查询到的结果
            return account


    def set_user_authenticated(self, account_id, is_authenticated=1):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE user_accounts SET is_authenticated = ? WHERE account_id = ?
            ''', (is_authenticated, account_id))
            conn.commit()

    def set_session_file(self, account_id, session_file):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE user_accounts SET session_file = ? WHERE account_id = ?
            ''', (session_file, account_id))
            conn.commit()

    def remove_user_account(self, account_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # 获取会话文件路径以便删除
            cursor.execute('SELECT session_file FROM user_accounts WHERE account_id = ?', (account_id,))
            row = cursor.fetchone()
            if row:
                session_file = row[0]
                if os.path.exists(session_file):
                    os.remove(session_file)
                    logger.debug(f"已删除会话文件: {session_file}")
            # 删除数据库记录
            cursor.execute('''
                DELETE FROM user_accounts WHERE account_id = ?
            ''', (account_id,))
            conn.commit()

    def get_all_authenticated_accounts(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(''' 
                SELECT account_id, user_id, username, firstname, lastname, session_file
                FROM user_accounts WHERE is_authenticated = 1
            ''')
            return cursor.fetchall()

    # 添加方法获取会话文件路径
    def get_session_file_path(self, user_id, session_filename):
        """
        获取指定用户和会话文件名的会话文件路径。
        """
        user_folder = os.path.join(SESSIONS_DIR, str(user_id))
        os.makedirs(user_folder, exist_ok=True)
        session_file = os.path.join(user_folder, session_filename)
        return session_file

    # 申请相关的方法
    def add_pending_application(self, user_id, first_name, username):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO pending_applications (user_id, first_name, username)
                VALUES (?, ?, ?)
            ''', (user_id, first_name, username))
            conn.commit()

    def get_pending_application(self, user_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT application_id, first_name, username, timestamp FROM pending_applications
                WHERE user_id = ?
            ''', (user_id,))
            return cursor.fetchone()

    def approve_application(self, user_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # 获取申请信息
            cursor.execute('''
                SELECT first_name, username FROM pending_applications
                WHERE user_id = ?
            ''', (user_id,))
            row = cursor.fetchone()
            if not row:
                return None
            first_name, username = row
            # 添加到允许用户表
            cursor.execute('''
                INSERT OR IGNORE INTO allowed_users (user_id, first_name, username)
                VALUES (?, ?, ?)
            ''', (user_id, first_name, username))
            # 删除申请记录
            cursor.execute('''
                DELETE FROM pending_applications WHERE user_id = ?
            ''', (user_id,))
            conn.commit()
            return first_name, username

    def reject_application(self, user_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM pending_applications WHERE user_id = ?
            ''', (user_id,))
            conn.commit()

    # 群组相关的方法
    def add_group(self, user_id, group_id, group_name):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # 添加群组到 groups 表
            cursor.execute('''
                INSERT OR IGNORE INTO groups (group_id, group_name)
                VALUES (?, ?)
            ''', (group_id, group_name))
            # 添加到用户监控的群组列表
            cursor.execute('''
                INSERT OR IGNORE INTO user_monitored_groups (user_id, group_id)
                VALUES (?, ?)
            ''', (user_id, group_id))
            conn.commit()

    def remove_group(self, user_id, group_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM user_monitored_groups WHERE user_id = ? AND group_id = ?
            ''', (user_id, group_id))
            conn.commit()

    def get_user_monitored_groups(self, user_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT groups.group_id, groups.group_name FROM user_monitored_groups
                JOIN groups ON user_monitored_groups.group_id = groups.group_id
                WHERE user_monitored_groups.user_id = ?
            ''', (user_id,))
            return cursor.fetchall()

    def get_group_name(self, group_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT group_name FROM groups WHERE group_id = ?
            ''', (group_id,))
            row = cursor.fetchone()
            return row[0] if row else "未知群组"

    # 添加/移除屏蔽用户的方法
    def add_blocked_user(self, receiving_user_id, target_user_id, first_name, username):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO blocked_users (receiving_user_id, user_id, first_name, username)
                VALUES (?, ?, ?, ?)
            ''', (receiving_user_id, target_user_id, first_name, username))
            conn.commit()

    def remove_blocked_user(self, receiving_user_id, target_user_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM blocked_users WHERE receiving_user_id = ? AND user_id = ?
            ''', (receiving_user_id, target_user_id))
            conn.commit()

    def list_blocked_users(self, receiving_user_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT user_id, first_name, username FROM blocked_users
                WHERE receiving_user_id = ?
            ''', (receiving_user_id,))
            rows = cursor.fetchall()
            return {row[0]: {'first_name': row[1], 'username': row[2]} for row in rows}

    # 添加获取所有已认证用户的方法
    # 获取所有已认证用户的ID
    def get_all_authenticated_users(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT DISTINCT user_id FROM user_accounts WHERE is_authenticated = 1
                ''')
                rows = cursor.fetchall()
                user_ids = [row[0] for row in rows]
                logger.info(f"获取到 {len(user_ids)} 个已认证用户。")
                return user_ids
        except Exception as e:
            logger.error(f"获取用户ID失败: {e}", exc_info=True)
            return []

    
    def add_keyword(self, user_id, keyword):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO keywords (user_id, keyword) VALUES (?, ?)", (user_id, keyword))
                conn.commit()
            logger.info(f"关键词 '{keyword}' 被用户 {user_id} 添加。")
            return True
        except sqlite3.IntegrityError:
            logger.warning(f"关键词 '{keyword}' 已存在，无法添加。")
            return False
        except Exception as e:
            logger.error(f"添加关键词失败: {e}", exc_info=True)
            return False

    def remove_keyword(self, user_id, keyword):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM keywords WHERE user_id = ? AND keyword = ?", (user_id, keyword))
                conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"用户 {user_id} 删除了关键词 '{keyword}'。")
                return True
            else:
                logger.info(f"用户 {user_id} 没有找到关键词 '{keyword}'。")
                return False
        except Exception as e:
            logger.error(f"删除关键词失败: {e}", exc_info=True)
            return False

    def get_keywords(self, user_id):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT keyword FROM keywords WHERE user_id = ?", (user_id,))
                rows = cursor.fetchall()
            return [row[0] for row in rows] if rows else []
        except Exception as e:
            logger.error(f"获取关键词列表失败: {e}", exc_info=True)
            return []

    def is_keyword_exists(self, user_id, keyword):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM keywords WHERE user_id = ? AND keyword = ?", (user_id, keyword))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"检查关键词是否存在失败: {e}", exc_info=True)
            return False
    
    # 获取用户的总推送次数
    def get_total_pushes(self, user_id):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM push_logs WHERE user_id = ?", (user_id,))
                total_pushes = cursor.fetchone()[0]
            return total_pushes
        except Exception as e:
            logger.error(f"获取总推送次数失败: {e}", exc_info=True)
            return 0

    # 获取按关键词统计的前10条数据
    def get_keyword_stats(self, user_id):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT keyword, COUNT(*) FROM push_logs WHERE user_id = ? GROUP BY keyword ORDER BY COUNT(*) DESC LIMIT 10",
                    (user_id,)
                )
                keyword_stats = cursor.fetchall()
            return keyword_stats
        except Exception as e:
            logger.error(f"获取关键词统计失败: {e}", exc_info=True)
            return []
        
    # 记录推送日志
    def record_push_log(self, user_id, keyword, chat_id, message_id,timestamp):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO push_logs (user_id, keyword, chat_id, message_id, timestamp) VALUES (?, ?, ?, ?, ?)",
                    (user_id, keyword, chat_id, message_id, timestamp)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"记录推送日志失败: {e}", exc_info=True)
        

# 主机器人类
class TelegramBot:
    def __init__(self, token, admin_ids, admin_username, api_id, api_hash,  db_path='bot.db'):
        self.token = token
        self.admin_ids = admin_ids
        self.admin_username = admin_username
        self.api_id = int(api_id)
        self.api_hash = api_hash
        self.db_manager = DatabaseManager(db_path)
        self.allowed_users = self.db_manager.get_allowed_users()
        self.parseMode = 'Markdown'
        self.application = Application.builder().token(self.token).build()
        self.user_clients = {}  # key: account_id, value: TelegramClient
        self.setup_handlers()

    def setup_handlers(self):
        # 添加命令处理器
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("login", self.login))
        self.application.add_handler(CommandHandler("add_keyword", self.add_keyword))
        self.application.add_handler(CommandHandler("remove_keyword", self.remove_keyword))
        self.application.add_handler(CommandHandler("list_keywords", self.list_keywords))
        self.application.add_handler(CommandHandler("list_accounts", self.list_accounts))
        self.application.add_handler(CommandHandler("remove_account", self.remove_account))
        self.application.add_handler(CommandHandler("block", self.block_user))
        self.application.add_handler(CommandHandler("unblock", self.unblock_user))
        self.application.add_handler(CommandHandler("list_blocked_users", self.list_blocked_users))
        self.application.add_handler(CommandHandler("my_account", self.my_account))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback_query))
        self.application.add_handler(MessageHandler(filters.Document.FileExtension("session") & ~filters.COMMAND, self.handle_login_step))
        logger.debug("已设置所有命令处理器。")
        
    def restricted(func):
        async def wrapped(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            user = update.effective_user
            user_id = user.id
            logger.debug(f"用户 {user_id} 请求执行命令 {update.message.text}.")
            if user_id in self.allowed_users or user_id in self.admin_ids:
                return await func(self, update, context)
            else:
                # 检查是否已经有待处理的申请
                pending = self.db_manager.get_pending_application(user_id)
                if pending:
                    await update.message.reply_text("ℹ️ 您的申请正在处理中，请稍候。")
                    logger.debug(f"用户 {user_id} 已有待处理的申请。")
                    return
                # 添加新的申请
                self.db_manager.add_pending_application(user_id, user.first_name, user.username)
                logger.info(f"用户 {user_id} 提交了访问申请。")
                # 通知用户，包含管理员的用户名
                await update.message.reply_text(
                    f"📝 您需要申请使用此机器人。您的申请已提交，管理员 @{self.admin_username} 将尽快审核。"
                )
                # 通知管理员
                for admin_id in self.admin_ids:
                    try:
                        if user.username:
                            user_link = f"[{user.first_name}](https://t.me/{user.username})"
                        else:
                            user_link = user.first_name
                        keyboard = InlineKeyboardMarkup([
                            [
                                InlineKeyboardButton("✅ 同意", callback_data=f"approve:{user_id}"),
                                InlineKeyboardButton("❌ 拒绝", callback_data=f"reject:{user_id}")
                            ]
                        ])
                        # 发送申请信息给管理员
                        await self.application.bot.send_message(
                            chat_id=admin_id,
                            text=(
                                f"📋 *新用户申请使用机器人*\n\n"
                                f"👤 *用户信息:*\n"
                                f"姓名: {user.first_name}\n"
                                f"用户名: @{user.username}" if user.username else "用户名: 无\n"
                                f"用户ID: `{user_id}`"
                            ),
                            parse_mode='Markdown',
                            reply_markup=keyboard
                        )
                        logger.debug(f"向管理员 {admin_id} 发送申请通知。")
                    except Exception as e:
                        logger.error(f"无法发送申请通知给管理员 {admin_id}: {e}", exc_info=True)
        return wrapped

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id
        logger.debug(f"用户 {user_id} 启动了机器人。")
        welcome_text = (
            f"👋 *欢迎使用消息转发机器人！*\n\n"
            f"此机器人允许您管理自己感兴趣的 Telegram 账号和群组，并在这些群组中有新消息时接收通知。\n\n"
            f"*命令列表：*\n"
            f"• `/login` - 登录您的 Telegram 账号。\n"
            f"• `/list_accounts` - 列出您已登录的 Telegram 账号。\n"
            f"• `/remove_account <account_id>` - 移除一个已登录的 Telegram 账号。\n"
            f"• `/add_keyword <关键词>` - 添加关键词\n"
            f"• `/remove_keyword <关键词>` - 删除关键词\n"
            f"• `/list_keywords` - 列出所有关键词\n"
            f"• `/my_stats` - 查看您的推送分析信息\n"
            f"• `/block <用户ID>` - 屏蔽指定用户的消息。\n"
            f"  - *示例*: `/block 123456789`\n\n"
            f"• `/unblock <用户ID>` - 解除屏蔽指定用户的消息。\n"
            f"  - *示例*: `/unblock 123456789`\n\n"
            f"• `/list_blocked_users` - 查看您的屏蔽用户列表。\n\n"
            f"• `/my_account <account_id>` - 查看指定 Telegram 账号的信息。\n\n"
            f"• `/help` - 显示此帮助信息。\n\n"
            f"如果您尚未获得使用权限，请使用受限命令（如 `/login`），机器人将引导您申请使用。"
        )
        await update.message.reply_text(welcome_text, parse_mode='Markdown')

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        help_text = (
            f"📖 *使用说明*\n\n"
            f"• `/login` - 登录您的 Telegram 账号。\n"
            f"• `/list_accounts` - 列出您已登录的 Telegram 账号。\n"
            f"• `/remove_account <account_id>` - 移除一个已登录的 Telegram 账号。\n"
            f"• `/add_keyword <关键词>` - 添加关键词\n"
            f"• `/remove_keyword <关键词>` - 删除关键词\n"
            f"• `/list_keywords` - 列出所有关键词\n"
            f"• `/my_stats` - 查看您的推送分析信息\n"
            f"• `/block <用户ID>` - 屏蔽指定用户的消息。\n"
            f"  - *示例*: `/block 123456789`\n\n"
            f"• `/unblock <用户ID>` - 解除屏蔽指定用户的消息。\n"
            f"  - *示例*: `/unblock 123456789`\n\n"
            f"• `/list_blocked_users` - 查看您的屏蔽用户列表。\n\n"
            f"• `/my_account <account_id>` - 查看指定 Telegram 账号的信息。\n\n"
            f"• `/start` - 显示欢迎信息。\n"
            f"• `/help` - 显示此帮助信息。\n\n"
            f"如果您没有使用权限，请使用受限命令（如 `/login`），机器人将引导您申请使用。"
        )
        await update.message.reply_text(help_text, parse_mode='Markdown')

    # 主机器人类中的 login 方法
    @restricted
    async def login(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id

        await update.message.reply_text(
            "🔐 请上传您的 Telegram 会话文件（.session）。",
            parse_mode=self.parseMode
        )
        logger.info(f"用户 {user_id} 启动了登录流程。")

        # 初始化用户数据
        context.user_data['login_stage'] = 'awaiting_session'
    
    # 处理登录步骤
    async def handle_login_step(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id

        stage = context.user_data.get('login_stage')
        if not stage:
            logger.debug(f"用户 {user_id} 没有处于登录流程中。")
            return  # 用户不在登录流程中，无需处理

        if stage == 'awaiting_session':
            # 处理会话文件上传
            await self._handle_session_file(update, context)
        
    async def _handle_session_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id

        # 检查是否有上传文件
        if not update.message.document:
            await update.message.reply_text(
                "❌ 请上传一个有效的 Telegram 会话文件（.session）。",
                parse_mode=None  # 取消 Markdown 解析
            )
            logger.warning(f"用户 {user_id} 没有上传会话文件。")
            return

        document = update.message.document

        # 检查文件扩展名
        if not document.file_name.endswith('.session'):
            await update.message.reply_text(
                "❌ 文件格式错误。请确保上传的是一个 `.session` 文件。",
                parse_mode=None  # 取消 Markdown 解析
            )
            logger.warning(f"用户 {user_id} 上传了非 .session 文件：{document.file_name}")
            return

        try:
            # 获取 File 对象
            file = await document.get_file()

            # 下载会话文件内容
            session_bytes = await file.download_as_bytearray()

            # 生成唯一的 session_file 路径
            session_filename = f'session_{uuid.uuid4().hex}.session'
            user_folder = os.path.join(SESSIONS_DIR, str(user_id))
            os.makedirs(user_folder, exist_ok=True)
            session_file = os.path.join(user_folder, session_filename)

            # 保存会话文件
            with open(session_file, 'wb') as f:
                f.write(session_bytes)

            # 设置文件权限（仅所有者可读写）
            os.chmod(session_file, stat.S_IRUSR | stat.S_IWUSR)

            # 使用 Telethon 客户端从会话文件中获取用户信息
            client = TelegramClient(session_file, self.api_id, self.api_hash)

            # 尝试连接客户端并进行授权检查
            await client.connect()

            # 获取用户信息
            user = await client.get_me()
            username = user.username or ''  # 如果没有 username，设为空字符串
            firstname = user.first_name or ''
            lastname = user.last_name or ''

            # 添加用户账号到数据库，获取 account_id
            account_id = self.db_manager.add_user_account(
                user_id=user_id,
                username=username,
                firstname=firstname,
                lastname=lastname,
                session_file=session_file,
                is_authenticated=1  # 标记为已认证
            )

            # 如果未授权，则提示错误
            if not await client.is_user_authorized():
                await update.message.reply_text(
                    "❌ 会话文件无效或未授权。请确认您的会话文件正确。",
                    parse_mode=None  # 取消 Markdown 解析
                )
                logger.error(f"用户 {user_id} 上传的会话文件未授权或无效。")
                self.db_manager.remove_user_account(account_id)
                os.remove(session_file)
                return

            # 将客户端添加到用户客户端字典
            self.user_clients[account_id] = client

            # 注册消息事件处理器
            client.add_event_handler(lambda event, uid=user_id: self.handle_new_message(event, uid), events.NewMessage)

            await update.message.reply_text(
                "🎉 登录成功！您的会话已保存，您现在可以使用机器人。",
                parse_mode=None  # 取消 Markdown 解析
            )
            logger.info(f"用户 {user_id} 上传了会话文件并登录成功。")
        except Exception as e:
            # 确保错误消息不包含未闭合的 Markdown 实体
            error_message = f"❌ 处理会话文件时出错：{e}".replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
            await update.message.reply_text(
                error_message,
                parse_mode='MarkdownV2'  # 使用 MarkdownV2 并正确转义
            )
            logger.error(f"用户 {user_id} 处理会话文件时出错：{e}", exc_info=True)
        finally:
            # 清理用户数据
            context.user_data.clear()
            
    async def handle_new_message(self, event, uid):
        # logger.debug(f"处理新消息事件")
        try:
            chat_id = event.chat_id
            # 获取发送者信息
            sender = await event.get_sender()
            if not sender:
                logger.debug("无法获取发送者信息，忽略。")
                return

            # 忽略来自机器人的消息
            if sender.bot:
                logger.debug("忽略来自机器人发送的消息。")
                return

            user_id = sender.id
            username = sender.username
            first_name = sender.first_name or "未知用户"

            logger.debug(f"消息发送者 ID: {user_id}, 用户名: {username}, 是否为机器人: {sender.bot}")
            blocked_users = self.db_manager.list_blocked_users(uid)
            # 检查用户是否被屏蔽
            if user_id in blocked_users:
                logger.debug(f"用户 {user_id} 已被屏蔽，忽略其消息。")
                return

            # 获取消息内容
            message = event.message.message
            if not message:
                logger.debug("消息内容为空，忽略。")
                return  # 忽略没有文本的消息

            keyword_text = None
            
            # 查看是否包含关键词
            keywords = self.db_manager.get_keywords(uid)
            for keyword in keywords:
                if keyword in message:
                    logger.debug(f"消息包含关键词 '{keyword}',触发监控。")
                    keyword_text = keyword
                    break

            if not keyword_text:
                logger.debug("消息不包含关键词，忽略。")
                return
            
            # 获取消息所在的聊天
            chat = await event.get_chat()
            message_id = event.message.id
            logger.debug(f"消息所在的聊天 ID: {chat_id}, 聊天标题: {chat.title}")

            # 构建消息链接和群组名称
            if hasattr(chat, 'username') and chat.username:
                chat_username = chat.username
                message_link = f"https://t.me/{chat_username}/{message_id}"
                group_display_name = f"[{chat.title}](https://t.me/{chat_username})"
            else:
                if chat_id < 0:
                    # 群组
                    chat_id_str = str(chat_id)[4:]
                    message_link = f"https://t.me/c/{chat_id_str}/{message_id}"
                    group_display_name = f"[{chat.title}](https://t.me/c/{chat_id_str})"
                else:
                    # 用户对话
                    message_link = f"https://t.me/c/{chat_id}/{message_id}"
                    group_display_name = f"[私人聊天](https://t.me/{username})" if username else "私人聊天"

            logger.debug(f"构建的消息链接: {message_link}")
            logger.debug(f"群组显示名称: {group_display_name}")

            # 获取发送者信息
            sender_name = first_name
            sender_link = f"[{sender_name}](https://t.me/{username})" if username else sender_name

            logger.debug(f"发送者链接: {sender_link}")

            # 创建按钮，新增“🔒 屏蔽此用户”按钮
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔗 跳转到原消息", url=message_link),
                    InlineKeyboardButton("🔒 屏蔽此用户", callback_data=f"block_user:{user_id}:{uid}")
                ]
            ])
            logger.debug("创建了跳转按钮和屏蔽按钮。")

            # 构建转发消息的内容
            forward_text = (
                f"📢 *新消息来自群组：* {group_display_name}\n\n"
                f"🧑‍💻 *发送者：* {sender_link}\n\n"
                f"📝 *内容：*\n{message}"
            )
            logger.debug(f"构建的转发消息内容:\n{forward_text}")
            try:
                await self.application.bot.send_message(
                    chat_id=uid,
                    text=forward_text,
                    parse_mode='Markdown',
                    reply_markup=keyboard
                )
                logger.info(f"消息已成功转发给用户 {uid}。")
                self.db_manager.record_push_log(uid, keyword_text,chat_id, message_id, datetime.now())
                # 记录推送日志
                logger.debug(f"已记录推送日志: 用户 {uid}, 聊天 {chat_id}, 消息 {message_id}")
            except Exception as e:
                logger.error(f"转发消息给用户 {uid} 失败: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"处理消息失败: {e}", exc_info=True)
            
    @restricted
    async def remove_account(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id

        if len(context.args) < 1:
            await update.message.reply_text(
                "❌ 请提供要移除的账号ID。例如：`/remove_account 1`",
                parse_mode='Markdown'
            )
            logger.debug("remove_account 命令缺少参数。")
            return

        try:
            account_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text(
                "❌ 账号ID必须是整数。例如：`/remove_account 1`",
                parse_mode='Markdown'
            )
            logger.debug("remove_account 命令参数不是整数。")
            return

        accounts = self.db_manager.get_user_accounts(user_id)
        account_ids = [account[0] for account in accounts]
        if account_id not in account_ids:
            await update.message.reply_text(
                "❌ 该账号ID不存在或不属于您。",
                parse_mode='Markdown'
            )
            logger.warning(f"用户 {user_id} 尝试移除不存在或不属于他们的账号ID {account_id}。")
            return

        # 断开 Telethon 客户端
        client = self.user_clients.get(account_id)
        if client:
            client.disconnect()
            del self.user_clients[account_id]

        # 从数据库移除账号
        self.db_manager.remove_user_account(account_id)

        await update.message.reply_text(
            f"✅ 已移除账号ID `{account_id}`。",
            parse_mode='Markdown'
        )
        logger.info(f"用户 {user_id} 移除了账号ID {account_id}。")
    @restricted
    async def my_account(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id

        # 确保提供了账号ID
        if len(context.args) < 1:
            await update.message.reply_text(
                "❌ 请提供账号ID。例如：`/my_account 1`",
                parse_mode='Markdown'
            )
            logger.debug("my_account 命令缺少参数。")
            return

        # 尝试获取并转换账号ID为整数
        try:
            account_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text(
                "❌ 账号ID必须是整数。例如：`/my_account 1`",
                parse_mode='Markdown'
            )
            logger.debug("my_account 命令参数不是整数。")
            return

        # 从数据库获取账号信息
        account = self.db_manager.get_account_by_id(account_id)
        if not account or account[0] != user_id:
            await update.message.reply_text(
                "❌ 该账号ID不存在或不属于您。",
                parse_mode='Markdown'
            )
            logger.warning(f"用户 {user_id} 请求查看不存在或不属于他们的账号ID {account_id}。")
            return

        # 构建返回的账号信息
        account_info = (
            f"📱 *Telegram 账号信息：*\n\n"
            f"• *账号ID*: `{account[0]}`\n"
            f"  *用户名*: @{account[1] if account[1] else '无'}\n"
            f"  *名称*: {account[2]} {account[3]}\n"
            f"  *已认证*: {'✅ 是' if account[5] else '❌ 否'}\n"
        )

        # 发送账号信息
        await update.message.reply_text(account_info, parse_mode='Markdown')
        logger.info(f"用户 {user_id} 查看了账号ID {account_id} 的信息。")

    async def handle_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        logger.debug(f"收到回调查询: {data}")

        if data.startswith("approve:"):
            user_id = int(data.split(":")[1])
            result = self.db_manager.approve_application(user_id)
            if result:
                first_name, username = result
                self.allowed_users[user_id] = {'first_name': first_name, 'username': username}
                 # 添加授权用户
                self.db_manager.add_allowed_user(user_id, first_name, username)
                logger.info(f"用户 {user_id} ({first_name}, @{username}) 已被批准访问。")
                # 通知用户
                try:
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text="✅ 您的申请已被批准，您现在可以使用此机器人。"
                    )
                    logger.debug(f"已通知用户 {user_id} 申请已批准。")
                except Exception as e:
                    logger.error(f"无法通知用户 {user_id} 申请已批准: {e}", exc_info=True)
                # 更新管理员消息，包含用户信息
                await query.edit_message_text(
                    f"✅ 已批准用户 {first_name} (@{username}) 的访问请求。"
                )
            else:
                logger.warning(f"没有找到用户 {user_id} 的申请记录。")
                await query.edit_message_text("❌ 找不到该用户的申请记录。")

        elif data.startswith("reject:"):
            user_id = int(data.split(":")[1])
            self.db_manager.reject_application(user_id)
            logger.info(f"用户 {user_id} 的访问申请已被拒绝。")
            # 通知用户
            try:
                await self.application.bot.send_message(
                    chat_id=user_id,
                    text="❌ 您的申请已被拒绝，您无法使用此机器人。"
                )
                logger.debug(f"已通知用户 {user_id} 申请已拒绝。")
            except Exception as e:
                logger.error(f"无法通知用户 {user_id} 申请已拒绝: {e}", exc_info=True)
            # 更新管理员消息，包含用户信息
            await query.edit_message_text(
                f"❌ 已拒绝用户的访问请求。"
            )

        # elif data.startswith("remove_group:"):
        #     parts = data.split(":")
        #     if len(parts) != 3:
        #         logger.warning(f"无效的 remove_group 回调数据: {data}")
        #         await query.edit_message_text("❓ 无效的操作。")
        #         return

        #     group_id = int(parts[1])
        #     user_id = int(parts[2])

        #     # 获取群组名称
        #     with sqlite3.connect(self.db_path) as conn:
        #         cursor = conn.cursor()
        #         cursor.execute("SELECT group_name FROM groups WHERE group_id = ?", (group_id,))
        #         row = cursor.fetchone()
        #         group_name = row[0] if row else "未知群组"

        #     # 从用户监听列表中移除群组
        #     try:
        #         self.db_manager.remove_group(user_id, group_id)
        #         await query.edit_message_text(
        #             f"✅ 群组 `{group_id}` - *{group_name}* 已从您的监听列表中移除。",
        #             parse_mode='Markdown'
        #         )
        #         logger.info(f"用户 {user_id} 移除了群组 {group_id} - {group_name} 从自己的监听列表。")

        #         # 取消监听该群组的消息
        #         accounts = self.db_manager.get_user_accounts(user_id)
        #         for account in accounts:
        #             account_id = account[0]
        #             client = self.user_clients.get(account_id)
        #             if client:
        #                 client.remove_event_handler(self.handle_new_message, events.NewMessage(chats=group_id))
        #                 logger.info(f"账号 {account_id} 停止监听群组 {group_id}。")
        #     except Exception as e:
        #         await query.edit_message_text(
        #             f"❌ 无法移除群组。\n错误详情: {e}",
        #             parse_mode='Markdown'
        #         )
        #         logger.error(f"移除群组 {group_id} 失败: {e}", exc_info=True)

        elif data.startswith("block_user:"):
            parts = data.split(":")
            if len(parts) != 3:
                logger.warning(f"无效的 block_user 回调数据: {data}")
                await query.edit_message_text("❓ 无效的操作。")
                return

            target_user_id = int(parts[1])
            receiving_user_id = int(parts[2])

            # 获取被屏蔽用户的信息
            try:
                target_user = await self.application.bot.get_chat(target_user_id)
                target_first_name = target_user.first_name
                target_username = target_user.username
            except Exception as e:
                await query.edit_message_text(
                    f"❌ 无法获取用户信息。请确保用户ID正确。\n错误详情: {e}"
                )
                logger.error(f"获取用户 {target_user_id} 信息失败: {e}", exc_info=True)
                return

            blocked_users = self.db_manager.list_blocked_users(receiving_user_id)
            if target_user_id in blocked_users:
                await query.edit_message_text("ℹ️ 该用户已经在您的屏蔽列表中。")
                logger.info(f"用户 {receiving_user_id} 尝试屏蔽已屏蔽的用户 {target_user_id}。")
            else:
                try:
                    self.db_manager.add_blocked_user(receiving_user_id, target_user_id, target_first_name, target_username)
                    await query.edit_message_text(
                        f"✅ 已将用户 `{target_user_id}` - *{target_first_name}* @{target_username if target_username else '无'} 添加到您的屏蔽列表。",
                        parse_mode='Markdown'
                    )
                    logger.info(f"用户 {receiving_user_id} 屏蔽了用户 {target_user_id} - {target_first_name} @{target_username if target_username else '无'}。")
                except Exception as e:
                    await query.edit_message_text(
                        f"❌ 无法屏蔽用户。\n错误详情: {e}",
                        parse_mode='Markdown'
                    )
                    logger.error(f"屏蔽用户 {target_user_id} 失败: {e}", exc_info=True)

        else:
            logger.warning(f"未知的回调查询数据: {data}")
            await query.edit_message_text("❓ 未知的操作。")

        
    @restricted
    async def block_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id

        if not context.args:
            await update.message.reply_text(
                "❌ 请提供要屏蔽的用户ID。例如：`/block 123456789`",
                parse_mode='Markdown'
            )
            logger.debug("block_user 命令缺少参数。")
            return

        try:
            target_user_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text(
                "❌ 用户ID必须是整数。例如：`/block 123456789`",
                parse_mode='Markdown'
            )
            logger.debug("block_user 命令参数不是整数。")
            return

        # 获取被屏蔽用户的信息
        try:
            target_user = await self.application.bot.get_chat(target_user_id)
            target_first_name = target_user.first_name
            target_username = target_user.username
        except Exception as e:
            await update.message.reply_text(
                f"❌ 无法获取用户信息。请确保用户ID正确。\n错误详情: {e}",
                parse_mode='Markdown'
            )
            logger.error(f"获取用户 {target_user_id} 信息失败: {e}", exc_info=True)
            return

        try:
            self.db_manager.add_blocked_user(user_id, target_user_id, target_first_name, target_username)
            await update.message.reply_text(
                f"✅ 已屏蔽用户 `{target_user_id}` - *{target_first_name}* @{target_username if target_username else '无'}。",
                parse_mode='Markdown'
            )
            logger.info(f"用户 {user_id} 屏蔽了用户 {target_user_id} - {target_first_name} @{target_username if target_username else '无'}。")
        except Exception as e:
            await update.message.reply_text(
                f"❌ 无法屏蔽用户。\n错误详情: {e}",
                parse_mode='Markdown'
            )
            logger.error(f"屏蔽用户 {target_user_id} 失败: {e}", exc_info=True)
            
    @restricted
    async def unblock_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id

        if not context.args:
            await update.message.reply_text(
                "❌ 请提供要解除屏蔽的用户ID。例如：`/unblock 123456789`",
                parse_mode='Markdown'
            )
            logger.debug("unblock_user 命令缺少参数。")
            return

        try:
            target_user_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text(
                "❌ 用户ID必须是整数。例如：`/unblock 123456789`",
                parse_mode='Markdown'
            )
            logger.debug("unblock_user 命令参数不是整数。")
            return

        try:
            self.db_manager.remove_blocked_user(user_id, target_user_id)
            await update.message.reply_text(
                f"✅ 已解除对用户 `{target_user_id}` 的屏蔽。",
                parse_mode='Markdown'
            )
            logger.info(f"用户 {user_id} 解除屏蔽了用户 {target_user_id}。")
        except Exception as e:
            await update.message.reply_text(
                f"❌ 无法解除屏蔽用户。\n错误详情: {e}",
                parse_mode='Markdown'
            )
            logger.error(f"解除屏蔽用户 {target_user_id} 失败: {e}", exc_info=True)

    @restricted
    async def list_blocked_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id

        blocked_users = self.db_manager.list_blocked_users(user_id)

        if not blocked_users:
            await update.message.reply_text(
                "ℹ️ 您当前没有屏蔽任何用户。",
                parse_mode='Markdown'
            )
            logger.info(f"用户 {user_id} 请求列出屏蔽用户，但没有被屏蔽的用户。")
            return

        # 构建用户列表，显示用户ID、姓名和用户名
        user_list = '\n'.join([
            f"• `{uid}` - *{info['first_name']}* @{info['username']}" if info['username'] else f"• `{uid}` - *{info['first_name']}*"
            for uid, info in blocked_users.items()
        ])

        await update.message.reply_text(
            f"📋 *您当前屏蔽的用户列表：*\n{user_list}",
            parse_mode='Markdown'
        )
        logger.info(f"用户 {user_id} 列出了自己的屏蔽用户列表。")

    @restricted
    async def list_accounts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id

        # 获取当前用户的所有账号信息
        accounts = self.db_manager.get_user_accounts(user_id)
        if not accounts:
            await update.message.reply_text(
                "ℹ️ 您当前没有登录任何 Telegram 账号。请使用 `/login` 命令进行登录。",
                parse_mode='Markdown'
            )
            logger.info(f"用户 {user_id} 请求列出账号，但没有登录的账号。")
            return
        
        # 创建账号列表的文本
        account_list = '\n\n'.join([ 
            f"• *账号ID*: `{account[0]}`\n"
            f"  *用户名*: @{account[1] if account[1] else '无'}\n"
            f"  *名称*: {account[2]} {account[3]}\n"
            f"  *已认证*: {'✅ 是' if account[5] else '❌ 否'}\n"
            for account in accounts
        ])
        
        # 发送用户已登录的账号信息
        await update.message.reply_text(
            f"📋 *您已登录的 Telegram 账号：*\n{account_list}",
            parse_mode='Markdown'
        )
        logger.info(f"用户 {user_id} 列出了他们的 Telegram 账号。")
    
    @restricted
    async def remove_account(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id

        if len(context.args) < 1:
            await update.message.reply_text(
                "❌ 请提供要移除的账号ID。例如：`/remove_account 1`",
                parse_mode='Markdown'
            )
            logger.debug("remove_account 命令缺少参数。")
            return

        try:
            account_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text(
                "❌ 账号ID必须是整数。例如：`/remove_account 1`",
                parse_mode='Markdown'
            )
            logger.debug("remove_account 命令参数不是整数。")
            return

        accounts = self.db_manager.get_user_accounts(user_id)
        account_ids = [account[0] for account in accounts]
        if account_id not in account_ids:
            await update.message.reply_text(
                "❌ 该账号ID不存在或不属于您。",
                parse_mode='Markdown'
            )
            logger.warning(f"用户 {user_id} 尝试移除不存在或不属于他们的账号ID {account_id}。")
            return

        # 断开 Telethon 客户端
        client = self.user_clients.get(account_id)
        if client:
            client.disconnect()
            del self.user_clients[account_id]

        # 从数据库移除账号
        self.db_manager.remove_user_account(account_id)

        await update.message.reply_text(
            f"✅ 已移除账号ID `{account_id}`。",
            parse_mode='Markdown'
        )
        logger.info(f"用户 {user_id} 移除了账号ID {account_id}。")

    @restricted
    async def add_keyword(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.debug("执行添加关键词命令。")
        
        if not context.args:
            await update.message.reply_text("❌ 请提供要添加的关键词。例如：`/add Python Django Flask`", parse_mode='Markdown')
            logger.debug("添加关键词命令缺少参数。")
            return
        
        # 获取用户输入的关键词，并按空格分割
        raw_keywords = ' '.join(context.args).strip()
        
        # 使用空格分割关键词
        keywords = [kw.strip() for kw in raw_keywords.split() if kw.strip()]  # 去除空白关键词

        if not keywords:
            await update.message.reply_text("❌ 关键词不能为空。", parse_mode='Markdown')
            logger.debug("添加关键词时关键词为空。")
            return
        
        # 收集成功添加和失败的关键词
        added_keywords = []
        existing_keywords = []

        # 遍历分词后的每个关键词，逐个添加
        for keyword in keywords:
            if self.db_manager.add_keyword(update.effective_user.id, keyword):
                added_keywords.append(keyword)
            else:
                existing_keywords.append(keyword)
        
        # 构造返回的消息
        if added_keywords:
            added_message = "✅ 关键词已添加：" + ", ".join(added_keywords)
        else:
            added_message = "❌ 没有关键词被添加。"

        if existing_keywords:
            existing_message = "⚠️ 已存在的关键词：" + ", ".join(existing_keywords)
        else:
            existing_message = ""

        # 合并消息
        message = f"{added_message}\n{existing_message}"

        # 发送消息
        await update.message.reply_text(message, parse_mode='Markdown')

    @restricted
    async def remove_keyword(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.debug("执行删除关键词命令。")
        try:
            # 获取用户的关键词列表
            keywords = self.db_manager.get_keywords(update.effective_user.id)
            
            if keywords:
                keyboard = [
                    [InlineKeyboardButton(kw, callback_data=f"delete:{kw}")] for kw in keywords
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text("📋 *请选择要删除的关键词：*", parse_mode='Markdown', reply_markup=reply_markup)
                logger.info(f"向用户 {update.effective_user.id} 显示删除关键词按钮。")
            else:
                await update.message.reply_text("ℹ️ 您当前没有设置任何关键词。", parse_mode='Markdown')
                logger.info(f"用户 {update.effective_user.id} 没有任何关键词可删除。")
        except Exception as e:
            logger.error(f"获取关键词列表失败: {e}", exc_info=True)
            await update.message.reply_text("❌ 获取关键词列表时发生错误。", parse_mode='Markdown')

    @restricted
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        data = query.data

        if data.startswith("delete:"):
            keyword_to_delete = data.split(":", 1)[1]
            
            # 使用 DatabaseManager 删除关键词
            if self.db_manager.remove_keyword(update.effective_user.id, keyword_to_delete):
                await query.answer()
                await query.edit_message_text(f"✅ 关键词 '{keyword_to_delete}' 已删除。", parse_mode='Markdown')
                logger.info(f"用户 {update.effective_user.id} 删除了关键词 '{keyword_to_delete}'。")
            else:
                await query.answer()
                await query.edit_message_text(f"⚠️ 关键词 '{keyword_to_delete}' 未找到。", parse_mode='Markdown')

    @restricted
    async def list_keywords(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.debug("执行列出关键词命令。")
        try:
            # 获取用户的关键词列表
            keywords = self.db_manager.get_keywords(update.effective_user.id)

            if keywords:
                keyword_list = '\n'.join([f"• {kw}" for kw in keywords])
                await update.message.reply_text(f"📄 *您设置的关键词列表：*\n{keyword_list}", parse_mode='Markdown')
                logger.info(f"用户 {update.effective_user.id} 列出了关键词。")
            else:
                await update.message.reply_text("ℹ️ 您当前没有设置任何关键词。", parse_mode='Markdown')
                logger.info(f"用户 {update.effective_user.id} 没有任何关键词。")
        except Exception as e:
            logger.error(f"获取关键词列表失败: {e}", exc_info=True)
            await update.message.reply_text("❌ 获取关键词列表时发生错误。", parse_mode='Markdown')

    # 查看自己的推送分析信息命令
    @restricted
    async def my_stats(self,update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.debug("执行查看自己的推送分析命令。")
        user_id = update.effective_user.id
        
        # 获取统计信息
        total_pushes = self.db_manager.get_total_pushes(user_id)
        keyword_stats = self.db_manager.get_keyword_stats(user_id)
        
        # 构建消息内容
        stats_text = (
            f"📊 *您的推送统计信息：*\n\n"
            f"• *总推送次数:* {total_pushes}\n\n"
            f"• *按关键词统计（前10）:*\n"
        )
        
        if keyword_stats:
            for keyword, count in keyword_stats:
                stats_text += f"  - {keyword}: {count} 次\n"
        else:
            stats_text += "  - 暂无数据。\n"
        
        # 发送消息
        await update.message.reply_text(stats_text, parse_mode='Markdown')
        logger.info(f"用户 {user_id} 查看了自己的推送统计信息。")
            
    async def send_announcement(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        logger.debug(f"用户 {user_id} 尝试发送公告。")

        # 权限检查
        if user_id not in self.admin_ids:
            await update.message.reply_text("❌ 你没有权限发送公告。")
            logger.warning(f"用户 {user_id} 尝试发送公告但没有权限。")
            return

        # 获取公告内容
        if not context.args:
            await update.message.reply_text("❌ 请提供公告内容。例如：`/send_announcement 这是公告内容`", parse_mode='Markdown')
            logger.debug("发送公告命令缺少公告内容。")
            return

        announcement_text = ' '.join(context.args).strip()
        if not announcement_text:
            await update.message.reply_text("❌ 公告内容不能为空。", parse_mode='Markdown')
            logger.debug("发送公告时公告内容为空。")
            return

        # 获取所有已认证用户的ID
        user_ids = self.db_manager.get_all_authenticated_users()

        if not user_ids:
            await update.message.reply_text("ℹ️ 当前没有已认证的用户。")
            logger.info("没有找到已认证的用户。")
            return

        # 确定并发发送的最大数量，避免触发速率限制
        semaphore = asyncio.Semaphore(10)  # 每次最多30个并发任务

        async def send_message(user_id, message):
            async with semaphore:
                try:
                    await context.bot.send_message(chat_id=user_id, text=message, parse_mode='Markdown')
                    logger.info(f"成功向用户 {user_id} 发送公告。")
                except Exception as e:
                    logger.error(f"发送公告给用户 {user_id} 失败: {e}")

        # 创建发送任务
        tasks = [send_message(uid, announcement_text) for uid in user_ids]

        # 执行所有发送任务
        await asyncio.gather(*tasks)

        # 发送反馈给管理员
        await update.message.reply_text(f"✅ 公告已成功发送给 {len(user_ids)} 个用户。")
        logger.info(f"用户 {user_id} 发送公告给 {len(user_ids)} 个用户。")

    def run(self):
        try:
            # 启动所有已登录用户的 Telethon 客户端
            authenticated_accounts = self.db_manager.get_all_authenticated_accounts()
            for account in authenticated_accounts:
                account_id, user_id, username, firstname, lastname, session_file = account

                # 检查会话文件是否存在
                if not os.path.exists(session_file):
                    # 如果会话文件不存在，删除该账号的记录
                    self.db_manager.remove_user_account(account_id)
                    logger.warning(f"用户 {user_id} 的会话文件 {session_file} 不存在，已删除该账号记录 (账号ID: {account_id})。")
                    continue  # 跳过该账号，处理下一个账号

                # 如果会话文件存在，启动 Telethon 客户端
                client = TelegramClient(session_file, self.api_id, self.api_hash)
                self.user_clients[account_id] = client
                client.start()

                # 注册消息事件处理器
                client.add_event_handler(lambda event, uid=user_id: self.handle_new_message(event, uid), events.NewMessage)

                logger.info(f"已启动并连接用户 {user_id} 用户名： @{username} 全名： {firstname} {lastname} 的 Telethon 客户端 (账号ID: {account_id})。")

            # 启动机器人
            self.application.run_polling()

        except (KeyboardInterrupt, SystemExit):
            logger.info("程序已手动停止。")
        except Exception as e:
            logger.critical(f"程序异常终止: {e}", exc_info=True)
        finally:
            # 断开所有 Telethon 客户端连接
            for client in self.user_clients.values():
                client.disconnect()
            logger.info("所有 Telethon 客户端已断开连接。")

# 启动脚本
if __name__ == "__main__":
    bot = TelegramBot(
        token=BOT_TOKEN,
        admin_ids=ADMIN_IDS,
        admin_username=ADMIN_USERNAME,
        api_id=API_ID,
        api_hash=API_HASH,
        db_path=DB_PATH
    )
    bot.run()