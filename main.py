import os
import logging
import sqlite3
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telethon import TelegramClient, events
from telegram import Bot
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 配置日志记录
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 数据库文件路径
DB_PATH = 'keywords.db'

# 环境变量
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_USER_IDS = os.getenv('ALLOWED_USER_IDS')  # 逗号分隔的用户 ID
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
USER_SESSION = os.getenv('TELEGRAM_SESSION') or 'user_session'
# FORWARD_CHAT_ID 不再使用，因为转发目标是设置关键词的用户

# 解析允许的用户 ID
if ALLOWED_USER_IDS:
    try:
        ALLOWED_USER_IDS = set(map(int, ALLOWED_USER_IDS.split(',')))
    except ValueError:
        logger.error("ALLOWED_USER_IDS 必须是逗号分隔的整数。")
        ALLOWED_USER_IDS = set()
else:
    ALLOWED_USER_IDS = set()

# 初始化 Telegram 机器人
bot = Bot(token=TOKEN)

# 初始化 Telethon 客户端
client = TelegramClient(USER_SESSION, int(API_ID), API_HASH)

# 初始化 SQLite 数据库
def initialize_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            keyword TEXT NOT NULL,
            UNIQUE(user_id, keyword)
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("数据库初始化完成。")

initialize_database()

# 装饰器：限制命令使用者
def restricted(func):
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ALLOWED_USER_IDS:
            await update.message.reply_text("您没有权限执行此操作。")
            logger.warning(f"未经授权的访问被拒绝，用户 ID: {user_id}.")
            return
        return await func(update, context)
    return wrapped

# 添加关键词命令
@restricted
async def add_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("请提供要添加的关键词。例如：/add 关键字")
        return
    keyword = ' '.join(context.args).strip()
    if not keyword:
        await update.message.reply_text("关键词不能为空。")
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO keywords (user_id, keyword) VALUES (?, ?)", (update.effective_user.id, keyword))
        conn.commit()
        conn.close()
        await update.message.reply_text(f"关键词 '{keyword}' 已添加。")
        logger.info(f"关键词 '{keyword}' 被用户 {update.effective_user.id} 添加。")
    except sqlite3.IntegrityError:
        await update.message.reply_text(f"关键词 '{keyword}' 已存在。")
    except Exception as e:
        logger.error(f"添加关键词失败: {e}")
        await update.message.reply_text("添加关键词时发生错误。")

# 删除关键词命令
@restricted
async def remove_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("请提供要删除的关键词。例如：/remove 关键字")
        return
    keyword = ' '.join(context.args).strip()
    if not keyword:
        await update.message.reply_text("关键词不能为空。")
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM keywords WHERE user_id = ? AND keyword = ?", (update.effective_user.id, keyword))
        conn.commit()
        rows_deleted = cursor.rowcount
        conn.close()
        if rows_deleted > 0:
            await update.message.reply_text(f"关键词 '{keyword}' 已删除。")
            logger.info(f"关键词 '{keyword}' 被用户 {update.effective_user.id} 删除。")
        else:
            await update.message.reply_text(f"关键词 '{keyword}' 未找到。")
    except Exception as e:
        logger.error(f"删除关键词失败: {e}")
        await update.message.reply_text("删除关键词时发生错误。")

# 列出关键词命令
@restricted
async def list_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT keyword FROM keywords WHERE user_id = ?", (update.effective_user.id,))
        rows = cursor.fetchall()
        conn.close()
        if rows:
            keywords = [row[0] for row in rows]
            keyword_list = '\n'.join(keywords)
            await update.message.reply_text(f"您设置的关键词列表：\n{keyword_list}")
        else:
            await update.message.reply_text("您当前没有设置任何关键词。")
    except Exception as e:
        logger.error(f"获取关键词列表失败: {e}")
        await update.message.reply_text("获取关键词列表时发生错误。")

# 从数据库中获取当前关键词列表
def get_keywords():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT keyword, user_id FROM keywords")
        rows = cursor.fetchall()
        conn.close()
        # 返回一个字典，键为关键词，值为设置该关键词的用户 ID 列表
        keyword_dict = {}
        for keyword, user_id in rows:
            keyword_lower = keyword.lower()
            if keyword_lower in keyword_dict:
                keyword_dict[keyword_lower].append(user_id)
            else:
                keyword_dict[keyword_lower] = [user_id]
        logger.info(f"加载了 {len(keyword_dict)} 个唯一关键词。")
        return keyword_dict
    except Exception as e:
        logger.error(f"获取关键词失败: {e}")
        return {}

# Telethon 监听新消息并转发
@client.on(events.NewMessage)
async def handle_new_message(event):
    message = event.message.message
    if not message:
        return  # 忽略没有文本的消息
    message_lower = message.lower()
    keywords = get_keywords()
    for keyword, user_ids in keywords.items():
        if keyword in message_lower:
            # 检测到关键词，执行转发操作
            for user_id in user_ids:
                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text=f"🔍 *检测到关键词：* `{keyword}`\n\n📝 *原文：*\n{message}",
                        parse_mode='Markdown'
                    )
                    logger.info(f"检测到关键词 '{keyword}'，消息已转发给用户 {user_id}。")
                except Exception as e:
                    logger.error(f"转发消息给用户 {user_id} 失败: {e}")
            break  # 只转发第一个匹配的关键词

# 主函数：运行机器人和监听器
async def main():
    # 检查必要的环境变量
    missing_vars = []
    required_vars = ['TELEGRAM_BOT_TOKEN', 'ALLOWED_USER_IDS', 'TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'TELEGRAM_SESSION']
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    if missing_vars:
        logger.error(f"未设置以下环境变量: {', '.join(missing_vars)}")
        return

    # 初始化 Telegram 机器人应用
    application = ApplicationBuilder().token(TOKEN).build()

    # 添加命令处理器
    application.add_handler(CommandHandler("add", add_keyword))
    application.add_handler(CommandHandler("remove", remove_keyword))
    application.add_handler(CommandHandler("list", list_keywords))

    # 启动 Telethon 客户端
    await client.start()
    logger.info("Telegram 用户客户端已启动，开始监听消息...")

    # 运行机器人和 Telethon 客户端并发运行
    await asyncio.gather(
        application.run_polling(),
        client.run_until_disconnected()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("程序已停止。")
