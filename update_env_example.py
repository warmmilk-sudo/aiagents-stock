# -*- coding: utf-8 -*-
"""更新.env.example模板文件"""

env_example_content = """# ============================================================
# AI股票分析系统环境配置模板
# ============================================================
# 使用说明：
# 1. 复制此文件并重命名为 .env
# 2. 根据实际情况填写配置项
# 3. 敏感信息（如API密钥、密码）请妥善保管，不要提交到版本控制
# ============================================================

# ========== AI模型 API配置 ==========
# AI模型 API密钥（必填）
AI_MODEL_API_KEY=your_actual_ai_model_api_key_here

# AI模型 API基础URL（可选，使用默认值即可）
AI_MODEL_BASE_URL=https://api.deepseek.com/v1

# 三类模型配置（可选）
AI_MODEL_LIGHTWEIGHT_NAME=deepseek-chat
AI_MODEL_LONG_CONTEXT_NAME=qwen-long
AI_MODEL_REASONING_NAME=deepseek-reasoner
DEFAULT_MODEL_NAME=deepseek-chat


# ========== 网站备案配置 ==========
# 网站底部显示的ICP备案号（为空则不显示）
ICP_NUMBER=京ICP备2026007346号
ICP_LINK=https://beian.miit.gov.cn/


# ========== 管理员登录配置 ==========
# 管理员前台登录密码（为空则免密，直接进入系统）
ADMIN_PASSWORD=

# （推荐）管理员密码哈希（比明文密码优先级更高）。格式为 pbkdf2_sha256$iterations$salt_hex$hash_hex
ADMIN_PASSWORD_HASH=

# 安全：最大登录失败重试次数，及锁定时间（秒）
LOGIN_MAX_ATTEMPTS=5
LOGIN_LOCKOUT_SECONDS=300

# 登录会话过期时间（秒），默认8小时
ADMIN_SESSION_TTL_SECONDS=28800


# ========== Tushare数据接口（可选）==========
# Tushare Token（可选，用于获取更多金融数据）
# 获取地址：https://tushare.pro/register
TUSHARE_TOKEN=

# Tushare API接口地址（一般无需修改）
TUSHARE_URL=https://api.tushare.pro


# ========== MiniQMT量化交易配置（可选）==========
# 是否启用MiniQMT量化交易接口
MINIQMT_ENABLED=false

# MiniQMT账户ID
MINIQMT_ACCOUNT_ID=

# MiniQMT服务器地址
MINIQMT_HOST=127.0.0.1

# MiniQMT服务器端口
MINIQMT_PORT=58610


# ========== 邮件通知配置（可选）==========
# 是否启用邮件通知
EMAIL_ENABLED=false

# SMTP服务器地址
# 常用邮箱SMTP服务器：
#   - 163邮箱：smtp.163.com
#   - QQ邮箱：smtp.qq.com
#   - Gmail：smtp.gmail.com
#   - 企业邮箱：根据邮箱服务商提供的信息填写
SMTP_SERVER=smtp.163.com

# SMTP服务器端口
# 常用端口：
#   - 465：SSL加密连接（推荐）
#   - 587：TLS加密连接
SMTP_PORT=465

# 发件人邮箱地址
EMAIL_FROM=your_email@163.com

# 邮箱授权码（不是登录密码！）
# 获取方法：
#   - 163邮箱：设置 -> POP3/SMTP/IMAP -> 开启服务 -> 获取授权码
#   - QQ邮箱：设置 -> 账户 -> POP3/IMAP/SMTP/Exchange/CardDAV/CalDAV服务 -> 开启服务 -> 生成授权码
EMAIL_PASSWORD=your_email_authorization_code

# 收件人邮箱地址（接收通知的邮箱）
EMAIL_TO=receiver@example.com


# ========== Webhook通知配置（可选）==========
# 是否启用Webhook通知（钉钉/飞书）
WEBHOOK_ENABLED=false

# Webhook类型
# 可选值：dingtalk（钉钉）、feishu（飞书）
WEBHOOK_TYPE=dingtalk

# Webhook地址
# 钉钉机器人：https://oapi.dingtalk.com/robot/send?access_token=YOUR_ACCESS_TOKEN
# 飞书机器人：https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_HOOK_ID
WEBHOOK_URL=

# Webhook自定义关键词（仅钉钉需要）
# 说明：
#   - 如果钉钉机器人设置了"自定义关键词"安全验证，请在此填写关键词
#   - 系统会自动在消息标题和内容中包含此关键词
#   - 如果不使用关键词验证，可以留空或使用其他安全方式（加签、IP白名单）
#   - 飞书机器人通常不需要关键词，可以留空
# 示例：aiagents通知
WEBHOOK_KEYWORD=


# ========== 时区配置 ==========
# 系统时区设置（可选）
TZ=Asia/Shanghai
"""

with open('.env.example', 'w', encoding='utf-8') as f:
    f.write(env_example_content)

print("[OK] .env.example file updated successfully")
print("[OK] File encoding: UTF-8")
print("[OK] All Chinese characters should display correctly")

