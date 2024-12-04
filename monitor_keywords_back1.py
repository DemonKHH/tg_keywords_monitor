import os
import logging
import re
import sqlite3
import asyncio
import threading
from telegram import Chat, Update, InlineKeyboardButton, InlineKeyboardMarkup, User
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler
from telethon import TelegramClient, events
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 配置日志记录
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # 设置为DEBUG以获取更详细的日志
)
logger = logging.getLogger(__name__)

# 数据库文件路径
DB_PATH = 'keywords.db'

# 环境变量
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_IDS = os.getenv('ADMIN_IDS')  # 逗号分隔的管理员用户 ID
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'demonkinghaha')  # 默认值为 'demonkinghaha'
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
USER_SESSION = os.getenv('TELEGRAM_SESSION') or 'user_session'

# 解析管理员用户 ID
if ADMIN_IDS:
    try:
        ADMIN_IDS = set(map(int, ADMIN_IDS.split(',')))
    except ValueError:
        logger.error("ADMIN_IDS 必须是逗号分隔的整数。")
        ADMIN_IDS = set()
else:
    ADMIN_IDS = set()
    logger.warning("未设置 ADMIN_IDS 环境变量。")

# 初始化 SQLite 数据库
def initialize_database():
    logger.debug("初始化数据库连接。")
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        # 创建关键词表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                UNIQUE(user_id, keyword)
            )
        ''')
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
                user_id INTEGER NOT NULL,
                first_name TEXT,
                username TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
    logger.info("数据库初始化完成。")

initialize_database()

# 初始化允许用户列表
def load_allowed_users():
    logger.debug("加载允许用户列表。")
    allowed_users = set()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM allowed_users")
        rows = cursor.fetchall()
        allowed_users = set(row[0] for row in rows)
    logger.info(f"加载了 {len(allowed_users)} 个允许用户。")
    return allowed_users

# 创建 Telegram Application
try:
    application = Application.builder().token(BOT_TOKEN).build()
    logger.info("成功创建 Telegram 应用。")
except Exception as e:
    logger.critical(f"无法创建 Telegram 应用: {e}", exc_info=True)
    raise

# 初始化允许用户列表
allowed_users = load_allowed_users()

# 装饰器：限制命令使用者
def restricted(func):
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id
        logger.debug(f"用户 {user_id} 请求执行命令 {update.message.text}.")
        if user_id in allowed_users or user_id in ADMIN_IDS:
            return await func(update, context)
        else:
            # 检查是否已经有待处理的申请
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM pending_applications WHERE user_id = ?", (user_id,))
                if cursor.fetchone():
                    await update.message.reply_text("ℹ️ 您的申请正在处理中，请稍候。")
                    logger.debug(f"用户 {user_id} 已有待处理的申请。")
                    return
            # 添加新的申请
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO pending_applications (user_id, first_name, username) VALUES (?, ?, ?)",
                    (user_id, user.first_name, user.username)
                )
                conn.commit()
            logger.info(f"用户 {user_id} 提交了访问申请。")
            # 通知用户，包含管理员的用户名
            await update.message.reply_text(
                f"📝 您需要申请使用此机器人。您的申请已提交，管理员 @{ADMIN_USERNAME} 将尽快审核。"
            )
            # 通知管理员
            for admin_id in ADMIN_IDS:
                try:
                    user_link = f"[{user.first_name}](https://t.me/{user.username})" if user.username else user.first_name
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("✅ 同意", callback_data=f"approve:{user_id}"),
                            InlineKeyboardButton("❌ 拒绝", callback_data=f"reject:{user_id}")
                        ]
                    ])
                    # 发送申请信息给管理员
                    await application.bot.send_message(
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

# 添加 /start 命令
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.debug(f"用户 {user.id} 启动了机器人。")
    welcome_text = (
        f"👋 *欢迎使用关键词监控机器人！*\n\n"
        f"此机器人允许您管理关键词，并在相关消息出现时通知您。\n\n"
        f"*命令列表：*\n"
        f"• `/add <关键词>` - 添加关键词\n"
        f"• `/remove` - 删除关键词\n"
        f"• `/list` - 列出所有关键词\n"
        f"• `/help` - 查看帮助信息\n\n"
        f"如果您尚未获得使用权限，请尝试使用任何受限命令，机器人将引导您申请使用。"
    )
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

# 添加 /help 命令
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        f"📖 *使用说明*\n\n"
        f"• `/add <关键词>` - 添加一个新的关键词。当检测到该关键词时，您将收到通知。\n"
        f"  - *示例*: `/add Python`\n\n"
        f"• `/remove` - 删除您之前添加的关键词。点击相应的按钮即可删除。\n\n"
        f"• `/list` - 列出您当前设置的所有关键词。\n\n"
        f"• `/start` - 显示欢迎信息和基本指引。\n"
        f"• `/help` - 显示此帮助信息。\n\n"
        f"如果您没有使用权限，请使用受限命令（如 `/add`），机器人将引导您申请使用。"
    )
    await update.message.reply_text(help_text, parse_mode='Markdown')

# 添加关键词命令
@restricted
async def add_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.debug("执行添加关键词命令。")
    if not context.args:
        await update.message.reply_text("❌ 请提供要添加的关键词。例如：`/add Python`", parse_mode='Markdown')
        logger.debug("添加关键词命令缺少参数。")
        return
    keyword = ' '.join(context.args).strip()
    if not keyword:
        await update.message.reply_text("❌ 关键词不能为空。", parse_mode='Markdown')
        logger.debug("添加关键词时关键词为空。")
        return
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO keywords (user_id, keyword) VALUES (?, ?)", (update.effective_user.id, keyword))
            conn.commit()
        # 更新缓存
        keywords_cache.add_keyword(update.effective_user.id, keyword)
        await update.message.reply_text(f"✅ 关键词 '{keyword}' 已添加。", parse_mode='Markdown')
        logger.info(f"关键词 '{keyword}' 被用户 {update.effective_user.id} 添加。")
    except sqlite3.IntegrityError:
        await update.message.reply_text(f"⚠️ 关键词 '{keyword}' 已存在。", parse_mode='Markdown')
        logger.warning(f"用户 {update.effective_user.id} 尝试添加已存在的关键词 '{keyword}'。")
    except Exception as e:
        logger.error(f"添加关键词失败: {e}", exc_info=True)
        await update.message.reply_text("❌ 添加关键词时发生错误。", parse_mode='Markdown')

# 删除关键词命令（显示按钮供用户选择删除）
@restricted
async def remove_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.debug("执行删除关键词命令。")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT keyword FROM keywords WHERE user_id = ?", (update.effective_user.id,))
            rows = cursor.fetchall()
        if rows:
            keywords = [row[0] for row in rows]
            keyboard = []
            for kw in keywords:
                keyboard.append([InlineKeyboardButton(kw, callback_data=f"delete:{kw}")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("📋 *请选择要删除的关键词：*", parse_mode='Markdown', reply_markup=reply_markup)
            logger.info(f"向用户 {update.effective_user.id} 显示删除关键词按钮。")
        else:
            await update.message.reply_text("ℹ️ 您当前没有设置任何关键词。", parse_mode='Markdown')
            logger.info(f"用户 {update.effective_user.id} 没有任何关键词可删除。")
    except Exception as e:
        logger.error(f"获取关键词列表失败: {e}", exc_info=True)
        await update.message.reply_text("❌ 获取关键词列表时发生错误。", parse_mode='Markdown')

# 列出关键词命令
@restricted
async def list_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.debug("执行列出关键词命令。")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT keyword FROM keywords WHERE user_id = ?", (update.effective_user.id,))
            rows = cursor.fetchall()
        if rows:
            keywords = [row[0] for row in rows]
            keyword_list = '\n'.join([f"• {kw}" for kw in keywords])
            await update.message.reply_text(f"📄 *您设置的关键词列表：*\n{keyword_list}", parse_mode='Markdown')
            logger.info(f"用户 {update.effective_user.id} 列出了关键词。")
        else:
            await update.message.reply_text("ℹ️ 您当前没有设置任何关键词。", parse_mode='Markdown')
            logger.info(f"用户 {update.effective_user.id} 没有任何关键词。")
    except Exception as e:
        logger.error(f"获取关键词列表失败: {e}", exc_info=True)
        await update.message.reply_text("❌ 获取关键词列表时发生错误。", parse_mode='Markdown')

# 关键词缓存类
class KeywordsCache:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.keyword_dict = {}
        self.load_keywords()

    def load_keywords(self):
        logger.debug("加载关键词列表到缓存。")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT keyword, user_id FROM keywords")
                rows = cursor.fetchall()
            # 返回一个字典，键为关键词，值为设置该关键词的用户 ID 列表
            keyword_dict = {}
            for keyword, user_id in rows:
                keyword_lower = keyword.lower()
                if keyword_lower in keyword_dict:
                    keyword_dict[keyword_lower].append(user_id)
                else:
                    keyword_dict[keyword_lower] = [user_id]
            with self.lock:
                self.keyword_dict = keyword_dict
            logger.info(f"加载了 {len(keyword_dict)} 个唯一关键词。")
        except Exception as e:
            logger.error(f"加载关键词到缓存失败: {e}", exc_info=True)
            self.keyword_dict = {}

    def add_keyword(self, user_id, keyword):
        logger.debug(f"添加关键词到缓存: {keyword}，用户 ID: {user_id}")
        keyword_lower = keyword.lower()
        with self.lock:
            if keyword_lower in self.keyword_dict:
                if user_id not in self.keyword_dict[keyword_lower]:
                    self.keyword_dict[keyword_lower].append(user_id)
            else:
                self.keyword_dict[keyword_lower] = [user_id]
        logger.debug(f"关键词缓存更新后: {keyword_lower} -> {self.keyword_dict[keyword_lower]}")

    def remove_keyword(self, user_id, keyword):
        logger.debug(f"从缓存中移除关键词: {keyword}，用户 ID: {user_id}")
        keyword_lower = keyword.lower()
        with self.lock:
            if keyword_lower in self.keyword_dict:
                if user_id in self.keyword_dict[keyword_lower]:
                    self.keyword_dict[keyword_lower].remove(user_id)
                    if not self.keyword_dict[keyword_lower]:
                        del self.keyword_dict[keyword_lower]
        logger.debug(f"关键词缓存更新后: {keyword_lower} -> {self.keyword_dict.get(keyword_lower, [])}")

    def get_keywords(self):
        with self.lock:
            return self.keyword_dict.copy()

# 初始化关键词缓存
keywords_cache = KeywordsCache(DB_PATH)

async def handle_new_message(event):
    logger.debug("处理新消息事件。")
    try:
        # 获取发送者信息
        sender = await event.get_sender()
        logger.debug(f"消息发送者 ID: {sender.id}, 用户名: {sender.username}, 是否为机器人: {sender.bot}")

        # 忽略来自机器人的消息
        if sender.bot:
            logger.debug("忽略来自机器人发送的消息。")
            return

        # 获取消息内容
        message = event.message.message
        logger.debug(f"接收到的消息内容: {message}")

        if not message:
            logger.debug("消息内容为空，忽略。")
            return  # 忽略没有文本的消息

        # 获取当前关键词缓存
        keywords = keywords_cache.get_keywords()
        logger.debug(f"当前加载的关键词数量: {len(keywords)}")
        logger.debug(f"加载的关键词列表: {list(keywords.keys())}")

        # 将关键词转换为正则表达式模式，不使用单词边界
        keyword_patterns = [
            (re.compile(fr'{re.escape(keyword)}', re.IGNORECASE), keyword, user_ids)
            for keyword, user_ids in keywords.items()
        ]

        logger.debug(f"编译的关键词正则表达式数量: {len(keyword_patterns)}")

        # 标记是否有关键词匹配
        keyword_matched = False

        for pattern, keyword, user_ids in keyword_patterns:
            logger.debug(f"检查关键词 '{keyword}' 是否匹配消息。")
            if pattern.search(message):  # 使用正则表达式匹配
                logger.debug(f"匹配到关键词: {keyword}")
                keyword_matched = True

                # 获取消息所在的聊天
                chat = await event.get_chat()
                message_id = event.message.id
                logger.debug(f"消息所在的聊天 ID: {chat.id}, 聊天标题: {chat.title}")

                # 构建消息链接和群组名称
                if hasattr(chat, 'username') and chat.username:
                    chat_username = chat.username
                    message_link = f"https://t.me/{chat_username}/{message_id}"
                    group_name = f"[{chat.title}](https://t.me/{chat_username})"
                else:
                    # 使用私有链接
                    chat_id = chat.id
                    if chat_id < 0:
                        # 群组
                        chat_id_str = str(chat_id)[1:]
                        message_link = f"https://t.me/c/{chat_id_str}/{message_id}"
                        group_name = f"[{chat.title}](https://t.me/c/{chat_id_str})"
                    else:
                        # 用户对话
                        message_link = f"https://t.me/c/{chat_id}/{message_id}"
                        group_name = f"[私人聊天](https://t.me/{sender.username})" if sender.username else "私人聊天"

                logger.debug(f"构建的消息链接: {message_link}")
                logger.debug(f"群组名称: {group_name}")

                # 获取发送者信息
                sender_name = sender.first_name or "未知用户"
                if sender.username:
                    sender_link = f"[{sender_name}](https://t.me/{sender.username})"
                else:
                    sender_link = sender_name  # 没有用户名时，仅显示名字

                logger.debug(f"发送者链接: {sender_link}")

                # 创建按钮
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 跳转到原消息", url=message_link)]
                ])
                logger.debug("创建了跳转按钮。")

                # 构建转发消息的内容
                forward_text = (
                    f"🔍 *检测到关键词：* {keyword}\n\n"
                    f"🧑‍💻 *发送者：* {sender_link}\n"
                    f"📢 *群组：* {group_name}\n\n"
                    f"📝 *原文：*\n{message}"
                )
                logger.debug(f"构建的转发消息内容:\n{forward_text}")

                # 发送转发消息给每个用户
                for user_id in user_ids:
                    logger.debug(f"准备发送转发消息给用户 ID: {user_id}")
                    try:
                        await application.bot.send_message(
                            chat_id=user_id,
                            text=forward_text,
                            parse_mode='Markdown',
                            reply_markup=keyboard
                        )
                        logger.info(f"检测到关键词 '{keyword}'，消息已成功转发给用户 {user_id}。")
                    except Exception as e:
                        logger.error(f"转发消息给用户 {user_id} 失败: {e}", exc_info=True)

                # 如果只需要转发第一个匹配的关键词，可以保留 break
                break

        if not keyword_matched:
            logger.debug("消息中未匹配到任何关键词。")

    except Exception as e:
        logger.error(f"处理新消息时发生错误: {e}", exc_info=True)

# Telethon 客户端初始化并启动
def start_telethon():
    async def run_client():
        try:
            client = TelegramClient(USER_SESSION, int(API_ID), API_HASH)
            await client.start()
            client.add_event_handler(handle_new_message, events.NewMessage)
            logger.info("Telethon 客户端已启动，开始监听消息...")
            await client.run_until_disconnected()
        except Exception as e:
            logger.critical(f"Telethon 客户端启动失败: {e}", exc_info=True)

    # 运行 Telethon 客户端的事件循环
    asyncio.run(run_client())

# 处理管理员的回调查询
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    logger.debug(f"收到回调查询: {data}")

    if data.startswith("approve:"):
        user_id = int(data.split(":")[1])
        # 添加用户到允许用户表
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT first_name, username FROM pending_applications WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row:
                first_name, username = row
                cursor.execute(
                    "INSERT INTO allowed_users (user_id, first_name, username) VALUES (?, ?, ?)", 
                    (user_id, first_name, username)
                )
                cursor.execute("DELETE FROM pending_applications WHERE user_id = ?", (user_id,))
                conn.commit()
                allowed_users.add(user_id)
                logger.info(f"用户 {user_id} ({first_name}, @{username}) 已被批准访问。")
                
                # 通知用户
                try:
                    await application.bot.send_message(
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
        # 先获取用户信息，然后删除申请记录
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT first_name, username FROM pending_applications WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row:
                first_name, username = row
                # 删除用户的申请记录
                cursor.execute("DELETE FROM pending_applications WHERE user_id = ?", (user_id,))
                conn.commit()
                logger.info(f"用户 {user_id} ({first_name}, @{username}) 的访问申请已被拒绝。")
                
                # 通知用户
                try:
                    await application.bot.send_message(
                        chat_id=user_id,
                        text="❌ 您的申请已被拒绝，您无法使用此机器人。"
                    )
                    logger.debug(f"已通知用户 {user_id} 申请已拒绝。")
                except Exception as e:
                    logger.error(f"无法通知用户 {user_id} 申请已拒绝: {e}", exc_info=True)
                
                # 更新管理员消息，包含用户信息
                await query.edit_message_text(
                    f"❌ 已拒绝用户 {first_name} (@{username}) 的访问请求。"
                )
            else:
                logger.warning(f"没有找到用户 {user_id} 的申请记录。")
                await query.edit_message_text("❌ 找不到该用户的申请记录。")
    
    elif data.startswith("delete:"):
        keyword = data.split("delete:", 1)[1]
        user = update.effective_user
        user_id = user.id
        logger.debug(f"用户 {user_id} 请求删除关键词: {keyword}")
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM keywords WHERE user_id = ? AND keyword = ?", (user_id, keyword))
                conn.commit()
                rows_deleted = cursor.rowcount
            if rows_deleted > 0:
                # 更新缓存
                keywords_cache.remove_keyword(user_id, keyword)
                await query.edit_message_text(f"✅ 关键词 '{keyword}' 已删除。")
                logger.info(f"关键词 '{keyword}' 被用户 {user_id} 删除。")
            else:
                await query.edit_message_text(f"⚠️ 关键词 '{keyword}' 未找到或已被删除。")
                logger.warning(f"用户 {user_id} 尝试删除不存在的关键词 '{keyword}'。")
        except Exception as e:
            logger.error(f"删除关键词失败: {e}", exc_info=True)
            await query.edit_message_text("❌ 删除关键词时发生错误。")
    
    else:
        logger.warning(f"未知的回调查询数据: {data}")
        await query.edit_message_text("❓ 未知的操作。")

# 主函数：运行机器人和监听器
def main():
    logger.debug("启动主函数。")
    # 检查必要的环境变量
    missing_vars = []
    required_vars = ['TELEGRAM_BOT_TOKEN', 'ADMIN_IDS', 'TELEGRAM_API_ID', 'TELEGRAM_API_HASH']
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    if missing_vars:
        logger.error(f"未设置以下环境变量: {', '.join(missing_vars)}")
        return

    # 添加命令处理器
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("add", add_keyword))
    application.add_handler(CommandHandler("remove", remove_keyword))
    application.add_handler(CommandHandler("list", list_keywords))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    logger.debug("命令处理器已添加。")

    # 启动 Telethon 客户端在单独的线程
    telethon_thread = threading.Thread(target=start_telethon, daemon=True)
    telethon_thread.start()
    logger.debug("Telethon 客户端线程已启动。")

    # 运行 PTB 机器人
    try:
        logger.info("开始运行 Telegram 机器人。")
        application.run_polling()
    except Exception as e:
        logger.critical(f"Telegram 机器人运行失败: {e}", exc_info=True)

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        logger.info("程序已停止。")
    except Exception as e:
        logger.critical(f"程序异常终止: {e}", exc_info=True)
