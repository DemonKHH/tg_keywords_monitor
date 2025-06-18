# Telegram 关键词监控机器人

一个功能强大的 Telegram 机器人，可以监控多个 Telegram 账号的群组消息，并根据设置的关键词自动转发相关消息。

## 🌟 功能特性

- **多账号管理**：支持管理多个 Telegram 账号进行消息监控
- **关键词监控**：设置关键词，自动监控包含关键词的消息
- **实时转发**：匹配到关键词的消息会实时转发到您的私聊
- **用户屏蔽**：可以屏蔽特定用户，不接收其消息
- **消息统计**：查看推送统计和关键词命中情况
- **安全可靠**：会话文件本地存储，支持错误恢复

## 📋 功能列表

### 账号管理
- 🔐 登录账号 - 添加新的监控账号
- 📱 账号列表 - 查看已登录的账号
- ❌ 删除账号 - 移除不需要的账号

### 关键词管理
- ➕ 添加关键词 - 设置需要监控的关键词
- ➖ 删除关键词 - 移除不需要的关键词
- 📄 关键词列表 - 查看所有关键词

### 用户管理
- 🔒 屏蔽用户 - 不再接收某用户的消息
- 🔓 解除屏蔽 - 恢复接收某用户的消息
- 📋 屏蔽列表 - 查看已屏蔽的用户

### 数据统计
- 📊 推送统计 - 查看总推送次数
- 🏆 关键词排行 - 查看关键词命中排行榜

## 🚀 快速开始

### 环境要求

- Python 3.8+
- Telegram Bot Token
- Telegram API ID 和 API Hash

### 安装步骤

1. **克隆项目**
   ```bash
   git clone <repository-url>
   cd tg_keywords_monitor
   ```

2. **安装依赖**
   ```powershell
   pip install -r requirements.txt
   ```

3. **配置环境变量**
   
   创建 `.env` 文件并填入以下信息：
   ```env
   # Telegram Bot Token (从 @BotFather 获取)
   TELEGRAM_BOT_TOKEN=your_bot_token_here
   
   # 管理员用户ID (逗号分隔，用于权限控制)
   ADMIN_IDS=123456789,987654321
   
   # 管理员用户名 (用于联系方式显示)
   ADMIN_USERNAME=your_username
   
   # Telegram API 信息 (从 https://my.telegram.org 获取)   
   TELEGRAM_API_ID=your_api_id
   TELEGRAM_API_HASH=your_api_hash
   ```
   
   > 💡 **提示**：项目包含详细的 `.env.example` 文件，其中有完整的配置说明和获取方法，建议直接复制使用。

4. **获取必要的 API 信息**

   **获取 Bot Token：**
   - 联系 [@BotFather](https://t.me/BotFather)
   - 发送 `/newbot` 创建新机器人
   - 按提示设置机器人名称和用户名
   - 获取 Bot Token

   **获取 API ID 和 API Hash：**
   - 访问 [https://my.telegram.org](https://my.telegram.org)
   - 使用手机号登录
   - 进入 "API development tools"
   - 创建应用获取 API ID 和 API Hash

5. **配置检查**
   
   在运行机器人之前，请确认以下配置已正确完成：
   
   ✅ **配置文件检查清单**
   - [ ] 已复制 `.env.example` 为 `.env`
   - [ ] 已填写 `TELEGRAM_BOT_TOKEN`（从 @BotFather 获取）
   - [ ] 已填写 `TELEGRAM_API_ID`（从 my.telegram.org 获取）
   - [ ] 已填写 `TELEGRAM_API_HASH`（从 my.telegram.org 获取）
   - [ ] 已填写 `ADMIN_IDS`（自己的用户ID，从 @userinfobot 获取）
   - [ ] 已检查所有必填项都不为空
   

6. **运行机器人**
   
   ```bash
   # Linux/macOS
   python3 monitor_keywords.py
   ```
   
   🎉 **首次运行成功标志**
   - 控制台显示 "Bot started successfully" 或类似信息
   - 没有出现配置错误信息
   - 在 Telegram 中能找到机器人并成功发送 `/start` 命令

## 📖 使用说明

### 基本使用流程

1. **启动机器人**
   - 运行程序后，在 Telegram 中找到你的机器人
   - 发送 `/start` 开始使用

2. **登录账号**
   - 发送 `/login` 命令
   - 上传你的 Telegram 会话文件 (`.session` 文件)
   - 等待验证成功

3. **设置关键词**
   - 发送 `/add_keyword` 命令添加关键词
   - 例如：`/add_keyword Python 编程 开发`

4. **开始监控**
   - 设置完成后，机器人会自动监控所有群组消息
   - 包含关键词的消息会自动转发给你

### 命令列表

| 命令 | 功能 | 示例 |
|------|------|------|
| `/start` | 启动机器人，显示欢迎信息 | `/start` |
| `/help` | 显示帮助信息 | `/help` |
| `/login` | 登录新的 Telegram 账号 | `/login` |
| `/list_accounts` | 查看已登录的账号列表 | `/list_accounts` |
| `/remove_account` | 删除指定账号 | `/remove_account 1` |
| `/add_keyword` | 添加监控关键词 | `/add_keyword Python Django` |
| `/remove_keyword` | 删除关键词（交互式选择） | `/remove_keyword` |
| `/list_keywords` | 查看所有关键词 | `/list_keywords` |
| `/block` | 屏蔽指定用户 | `/block 123456789` |
| `/unblock` | 解除屏蔽指定用户 | `/unblock 123456789` |
| `/list_blocked_users` | 查看屏蔽用户列表 | `/list_blocked_users` |
| `/my_stats` | 查看推送统计信息 | `/my_stats` |

### 获取会话文件

**方法一：使用现有程序生成**
```python
from telethon import TelegramClient

api_id = 'your_api_id'
api_hash = 'your_api_hash'
phone = '+1234567890'  # 你的手机号

client = TelegramClient('session_name', api_id, api_hash)
client.start(phone)
```

**方法二：从其他 Telegram 客户端获取**
- 如果你已经有其他使用 Telethon 的程序
- 直接复制对应的 `.session` 文件

## 🛡️ 安全说明

- **🔐 数据安全**：账号认证信息使用数据库加密存储
- **👮 权限控制**：只有配置文件中的管理员才能使用机器人
- **🔒 数据隔离**：每个用户的数据独立存储，互不影响
- **🔄 错误恢复**：单个账号故障不影响其他账号和整体功能
- **📝 日志审计**：完整的操作日志记录，便于问题排查
- **🚫 隐私保护**：不记录消息内容，只记录必要的元数据

## 📁 项目结构

```
tg_keywords_monitor/
├── monitor_keywords.py    # 主程序文件
├── requirements.txt       # Python 依赖包列表
├── README.md             # 项目说明文档
├── CHANGELOG.md          # 更新日志
├── LICENSE               # 开源许可证
├── .env                  # 环境变量配置文件 (需要创建)
├── .env.example          # 环境变量配置示例
├── bot.db               # SQLite 数据库 (自动创建)
├── bot.log              # 运行日志文件 (自动创建)
├── nohup.out            # 后台运行日志 (自动创建)
├── scripts/             # 部署脚本目录
│   └── build.sh         # Linux 生产环境部署脚本
└── __pycache__/         # Python 缓存目录 (自动创建)
```

## 🔧 高级配置

### 日志配置
- 日志文件：`bot.log`
- 日志级别：DEBUG
- 文件滚动：5MB 一个文件，保留 5 个备份

### 数据库
- 使用 SQLite 数据库存储用户数据
- 数据库文件：`bot.db`
- 支持多用户，数据隔离

### 错误处理
- 单个账号错误不影响整体运行
- 自动重连机制
- 详细错误日志记录

## 🐛 故障排除

### 常见问题

1. **机器人无响应**
   - 检查 Bot Token 是否正确
   - 确认网络连接正常
   - 查看日志文件 `bot.log`

2. **账号登录失败**
   - ✅ 检查 API ID 和 API Hash 是否正确
   - ✅ 确认手机号格式正确（包含国家代码）
   - ✅ 检查验证码是否正确
   - ✅ 确认账号未被 Telegram 限制
   - ✅ 检查网络连接是否稳定

3. **消息未转发**
   - 检查关键词设置是否正确
   - 确认账号在目标群组中
   - 查看是否误屏蔽了发送者

4. **权限错误**
   - 确认用户 ID 在 `ADMIN_IDS` 列表中
   - 检查用户是否在指定群组中

### 日志分析
```powershell
# 查看最新日志
Get-Content bot.log -Tail 50

# 搜索错误信息
Select-String "ERROR" bot.log
```

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来帮助改进这个项目！

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 📞 联系方式

如有问题或建议，请联系：
- 作者：[@demonkinghaha](https://t.me/demonkinghaha)
- 或提交 Issue 到项目仓库