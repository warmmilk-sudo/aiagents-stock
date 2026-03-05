"""
环境配置管理界面
从 app.py 提取的 display_config_manager() 函数
"""
import streamlit as st
import os
import time
from config_manager import config_manager

def display_config_manager():
    """显示环境配置管理界面"""
    st.subheader("环境配置管理")

    st.markdown("""
    <div class="agent-card">
        <p>在这里可以配置系统的环境变量，包括API密钥、数据源配置、量化交易配置等。</p>
        <p><strong>注意：</strong>配置修改后需要重启应用才能生效。</p>
    </div>
    """, unsafe_allow_html=True)

    # 获取当前配置
    config_info = config_manager.get_config_info()

    # 创建标签页
    tab1, tab2, tab3, tab4 = st.tabs(["基本配置", "数据源配置", "量化交易配置", "通知配置"])

    # 使用session_state保存临时配置
    if 'temp_config' not in st.session_state:
        st.session_state.temp_config = {key: info["value"] for key, info in config_info.items()}

    with tab1:
        st.markdown("### DeepSeek API配置")
        st.markdown("DeepSeek是系统的核心AI引擎，必须配置才能使用分析功能。")
        st.markdown("DeepSeek:https://api.deepseek.com/v1")
        st.markdown("硅基流动:https://api.siliconflow.cn/v1")
        st.markdown("火山引擎:https://ark.cn-beijing.volces.com/api/v3")
        st.markdown("阿里:https://dashscope.aliyuncs.com/compatible-mode/v1")

    # DeepSeek API Key
        api_key_info = config_info["DEEPSEEK_API_KEY"]
        current_api_key = st.session_state.temp_config.get("DEEPSEEK_API_KEY", "")

        new_api_key = st.text_input(
            f"{api_key_info['description']} {'*' if api_key_info['required'] else ''}",
            value=current_api_key,
            type="password",
            help="从 https://platform.deepseek.com 获取API密钥",
            key="input_deepseek_api_key"
        )
        st.session_state.temp_config["DEEPSEEK_API_KEY"] = new_api_key

        # 显示当前状态
        if new_api_key:
            masked_key = new_api_key[:8] + "*" * (len(new_api_key) - 12) + new_api_key[-4:] if len(new_api_key) > 12 else "***"
            st.success(f"API密钥已设置: {masked_key}")
        else:
            st.warning("未设置API密钥，系统无法使用AI分析功能")

        st.markdown("---")

        # DeepSeek Base URL
        base_url_info = config_info["DEEPSEEK_BASE_URL"]
        current_base_url = st.session_state.temp_config.get("DEEPSEEK_BASE_URL", "")

        new_base_url = st.text_input(
            f"{base_url_info['description']}",
            value=current_base_url,
            help="一般无需修改，保持默认即可",
            key="input_deepseek_base_url"
        )
        st.session_state.temp_config["DEEPSEEK_BASE_URL"] = new_base_url

        st.markdown("---")

        # AI模型名称
        model_name_info = config_info["DEFAULT_MODEL_NAME"]
        current_model_name = st.session_state.temp_config.get("DEFAULT_MODEL_NAME", "deepseek-chat")

        new_model_name = st.text_input(
            f"{model_name_info['description']}",
            value=current_model_name,
            help="输入OpenAI兼容的模型名称，修改后重启生效",
            key="input_default_model_name"
        )
        st.session_state.temp_config["DEFAULT_MODEL_NAME"] = new_model_name

        if new_model_name:
            st.success(f"当前模型: **{new_model_name}**")
        else:
            st.warning("未设置模型名称，将使用默认值 deepseek-chat")

        st.markdown("---")
        st.markdown("### 网站备案配置")

        icp_number_info = config_info["ICP_NUMBER"]
        current_icp_number = st.session_state.temp_config.get("ICP_NUMBER", "")
        new_icp_number = st.text_input(
            f"{icp_number_info['description']}",
            value=current_icp_number,
            placeholder="例如：京ICP备12345678号",
            key="input_icp_number"
        )
        st.session_state.temp_config["ICP_NUMBER"] = new_icp_number.strip()

        icp_link_info = config_info.get(
            "ICP_LINK",
            {"description": "备案号跳转地址（留空则仅显示文本）"}
        )
        current_icp_link = st.session_state.temp_config.get(
            "ICP_LINK",
            "https://beian.miit.gov.cn/"
        )
        new_icp_link = st.text_input(
            f"{icp_link_info['description']}",
            value=current_icp_link,
            placeholder="https://beian.miit.gov.cn/",
            key="input_icp_link"
        )
        st.session_state.temp_config["ICP_LINK"] = new_icp_link.strip()

        if st.session_state.temp_config["ICP_NUMBER"]:
            st.success("备案号将显示在页面底部")
        else:
            st.info("备案号为空时将不显示")

        st.markdown("""
        **常用模型名称参考：**
        - `deepseek-chat` — DeepSeek Chat（默认）
        - `deepseek-reasoner` — DeepSeek Reasoner（推理增强）
        - `qwen-plus` — 通义千问 Plus
        - `qwen-turbo` — 通义千问 Turbo
        - `gpt-4o` — OpenAI GPT-4o
        - `gpt-4o-mini` — OpenAI GPT-4o Mini
        
        >  使用非 DeepSeek 模型时，请同时修改上方的 API地址 和 API密钥
        """)

        st.info("如何获取DeepSeek API密钥？\n\n1. 访问 https://platform.deepseek.com\n2. 注册/登录账号\n3. 进入API密钥管理页面\n4. 创建新的API密钥\n5. 复制密钥并粘贴到上方输入框")

    with tab2:
        st.markdown("### Tushare数据接口（可选）")
        st.markdown("Tushare提供更丰富的A股财务数据，配置后可以获取更详细的财务分析。")

        tushare_info = config_info["TUSHARE_TOKEN"]
        current_tushare = st.session_state.temp_config.get("TUSHARE_TOKEN", "")

        new_tushare = st.text_input(
            f"{tushare_info['description']}",
            value=current_tushare,
            type="password",
            help="从 https://tushare.pro 获取Token",
            key="input_tushare_token"
        )
        st.session_state.temp_config["TUSHARE_TOKEN"] = new_tushare

        if new_tushare:
            st.success("Tushare Token已设置")
        else:
            st.info("ℹ 未设置Tushare Token，系统将使用其他数据源")

        st.info("如何获取Tushare Token？\n\n1. 访问 https://tushare.pro\n2. 注册账号\n3. 进入个人中心\n4. 获取Token\n5. 复制并粘贴到上方输入框")

    with tab3:
        st.markdown("### MiniQMT量化交易配置（可选）")
        st.markdown("配置后可以使用量化交易功能，自动执行交易策略。")

        # 启用开关
        miniqmt_enabled_info = config_info["MINIQMT_ENABLED"]
        current_enabled = st.session_state.temp_config.get("MINIQMT_ENABLED", "false") == "true"

        new_enabled = st.checkbox(
            "启用MiniQMT量化交易",
            value=current_enabled,
            help="开启后可以使用量化交易功能",
            key="input_miniqmt_enabled"
        )
        st.session_state.temp_config["MINIQMT_ENABLED"] = "true" if new_enabled else "false"

        # 其他配置
        col1, col2 = st.columns(2)

        with col1:
            account_id_info = config_info["MINIQMT_ACCOUNT_ID"]
            current_account_id = st.session_state.temp_config.get("MINIQMT_ACCOUNT_ID", "")

            new_account_id = st.text_input(
                f"🆔 {account_id_info['description']}",
                value=current_account_id,
                disabled=not new_enabled,
                key="input_miniqmt_account_id"
            )
            st.session_state.temp_config["MINIQMT_ACCOUNT_ID"] = new_account_id

            host_info = config_info["MINIQMT_HOST"]
            current_host = st.session_state.temp_config.get("MINIQMT_HOST", "")

            new_host = st.text_input(
                f"{host_info['description']}",
                value=current_host,
                disabled=not new_enabled,
                key="input_miniqmt_host"
            )
            st.session_state.temp_config["MINIQMT_HOST"] = new_host

        with col2:
            port_info = config_info["MINIQMT_PORT"]
            current_port = st.session_state.temp_config.get("MINIQMT_PORT", "")

            new_port = st.text_input(
                f"{port_info['description']}",
                value=current_port,
                disabled=not new_enabled,
                key="input_miniqmt_port"
            )
            st.session_state.temp_config["MINIQMT_PORT"] = new_port

        if new_enabled:
            st.success("MiniQMT已启用")
        else:
            st.info("ℹ MiniQMT未启用")

        st.warning("警告：量化交易涉及真实资金操作，请谨慎配置和使用！")

    with tab4:
        st.markdown("### 通知配置")
        st.markdown("配置邮件和Webhook通知，用于实时监测和智策定时分析的提醒。")

        # 创建两列布局
        col_email, col_webhook = st.columns(2)

        with col_email:
            st.markdown("#### 邮件通知")

            # 邮件启用开关
            email_enabled_info = config_info.get("EMAIL_ENABLED", {"value": "false"})
            current_email_enabled = st.session_state.temp_config.get("EMAIL_ENABLED", "false") == "true"

            new_email_enabled = st.checkbox(
                "启用邮件通知",
                value=current_email_enabled,
                help="开启后可以接收邮件提醒",
                key="input_email_enabled"
            )
            st.session_state.temp_config["EMAIL_ENABLED"] = "true" if new_email_enabled else "false"

            # SMTP服务器
            smtp_server_info = config_info.get("SMTP_SERVER", {"description": "SMTP服务器地址", "value": ""})
            current_smtp_server = st.session_state.temp_config.get("SMTP_SERVER", "")

            new_smtp_server = st.text_input(
                f"{smtp_server_info['description']}",
                value=current_smtp_server,
                disabled=not new_email_enabled,
                placeholder="smtp.qq.com",
                key="input_smtp_server"
            )
            st.session_state.temp_config["SMTP_SERVER"] = new_smtp_server

            # SMTP端口
            smtp_port_info = config_info.get("SMTP_PORT", {"description": "SMTP端口", "value": "587"})
            current_smtp_port = st.session_state.temp_config.get("SMTP_PORT", "587")

            new_smtp_port = st.text_input(
                f"{smtp_port_info['description']}",
                value=current_smtp_port,
                disabled=not new_email_enabled,
                placeholder="587 (TLS) 或 465 (SSL)",
                key="input_smtp_port"
            )
            st.session_state.temp_config["SMTP_PORT"] = new_smtp_port

            # 发件人邮箱
            email_from_info = config_info.get("EMAIL_FROM", {"description": "发件人邮箱", "value": ""})
            current_email_from = st.session_state.temp_config.get("EMAIL_FROM", "")

            new_email_from = st.text_input(
                f"{email_from_info['description']}",
                value=current_email_from,
                disabled=not new_email_enabled,
                placeholder="your-email@qq.com",
                key="input_email_from"
            )
            st.session_state.temp_config["EMAIL_FROM"] = new_email_from

            # 邮箱授权码
            email_password_info = config_info.get("EMAIL_PASSWORD", {"description": "邮箱授权码", "value": ""})
            current_email_password = st.session_state.temp_config.get("EMAIL_PASSWORD", "")

            new_email_password = st.text_input(
                f"{email_password_info['description']}",
                value=current_email_password,
                type="password",
                disabled=not new_email_enabled,
                help="不是邮箱登录密码，而是SMTP授权码",
                key="input_email_password"
            )
            st.session_state.temp_config["EMAIL_PASSWORD"] = new_email_password

            # 收件人邮箱
            email_to_info = config_info.get("EMAIL_TO", {"description": "收件人邮箱", "value": ""})
            current_email_to = st.session_state.temp_config.get("EMAIL_TO", "")

            new_email_to = st.text_input(
                f"{email_to_info['description']}",
                value=current_email_to,
                disabled=not new_email_enabled,
                placeholder="receiver@qq.com",
                key="input_email_to"
            )
            st.session_state.temp_config["EMAIL_TO"] = new_email_to

            if new_email_enabled and all([new_smtp_server, new_email_from, new_email_password, new_email_to]):
                st.success("邮件配置完整")
            elif new_email_enabled:
                st.warning("邮件配置不完整")
            else:
                st.info("ℹ 邮件通知未启用")

            st.caption("QQ邮箱授权码获取：设置 → 账户 → POP3/IMAP/SMTP → 生成授权码")

        with col_webhook:
            st.markdown("#### Webhook通知")

            # Webhook启用开关
            webhook_enabled_info = config_info.get("WEBHOOK_ENABLED", {"value": "false"})
            current_webhook_enabled = st.session_state.temp_config.get("WEBHOOK_ENABLED", "false") == "true"

            new_webhook_enabled = st.checkbox(
                "启用Webhook通知",
                value=current_webhook_enabled,
                help="开启后可以发送到钉钉或飞书群",
                key="input_webhook_enabled"
            )
            st.session_state.temp_config["WEBHOOK_ENABLED"] = "true" if new_webhook_enabled else "false"

            # Webhook类型选择
            webhook_type_info = config_info.get("WEBHOOK_TYPE", {"description": "Webhook类型", "value": "dingtalk", "options": ["dingtalk", "feishu"]})
            current_webhook_type = st.session_state.temp_config.get("WEBHOOK_TYPE", "dingtalk")

            new_webhook_type = st.selectbox(
                f"{webhook_type_info['description']}",
                options=webhook_type_info.get('options', ["dingtalk", "feishu"]),
                index=0 if current_webhook_type == "dingtalk" else 1,
                disabled=not new_webhook_enabled,
                key="input_webhook_type"
            )
            st.session_state.temp_config["WEBHOOK_TYPE"] = new_webhook_type

            # Webhook URL
            webhook_url_info = config_info.get("WEBHOOK_URL", {"description": "Webhook地址", "value": ""})
            current_webhook_url = st.session_state.temp_config.get("WEBHOOK_URL", "")

            new_webhook_url = st.text_input(
                f"{webhook_url_info['description']}",
                value=current_webhook_url,
                disabled=not new_webhook_enabled,
                placeholder="https://oapi.dingtalk.com/robot/send?access_token=...",
                key="input_webhook_url"
            )
            st.session_state.temp_config["WEBHOOK_URL"] = new_webhook_url

            # Webhook自定义关键词（钉钉安全验证）
            webhook_keyword_info = config_info.get("WEBHOOK_KEYWORD", {"description": "自定义关键词（钉钉安全验证）", "value": "aiagents通知"})
            current_webhook_keyword = st.session_state.temp_config.get("WEBHOOK_KEYWORD", "aiagents通知")

            new_webhook_keyword = st.text_input(
                f"{webhook_keyword_info['description']}",
                value=current_webhook_keyword,
                disabled=not new_webhook_enabled or new_webhook_type != "dingtalk",
                placeholder="aiagents通知",
                help="钉钉机器人安全设置中的自定义关键词，飞书不需要此设置",
                key="input_webhook_keyword"
            )
            st.session_state.temp_config["WEBHOOK_KEYWORD"] = new_webhook_keyword

            # 测试连通按钮
            if new_webhook_enabled and new_webhook_url:
                if st.button("测试Webhook连通", width='stretch', key="test_webhook_btn"):
                    with st.spinner("正在发送测试消息..."):
                        # 临时更新配置
                        temp_env_backup = {}
                        for key in ["WEBHOOK_ENABLED", "WEBHOOK_TYPE", "WEBHOOK_URL", "WEBHOOK_KEYWORD"]:
                            temp_env_backup[key] = os.getenv(key)
                            os.environ[key] = st.session_state.temp_config.get(key, "")

                        try:
                            # 创建临时通知服务实例
                            from notification_service import NotificationService
                            temp_notification_service = NotificationService()
                            success, message = temp_notification_service.send_test_webhook()

                            if success:
                                st.success(f"{message}")
                            else:
                                st.error(f"{message}")
                        except Exception as e:
                            st.error(f"测试失败: {str(e)}")
                        finally:
                            # 恢复环境变量
                            for key, value in temp_env_backup.items():
                                if value is not None:
                                    os.environ[key] = value
                                elif key in os.environ:
                                    del os.environ[key]

            if new_webhook_enabled and new_webhook_url:
                st.success(f"Webhook配置完整 ({new_webhook_type})")
            elif new_webhook_enabled:
                st.warning("请配置Webhook URL")
            else:
                st.info("ℹ Webhook通知未启用")

            # 显示帮助信息
            if new_webhook_type == "dingtalk":
                st.caption("钉钉机器人配置：\n1. 进入钉钉群 → 设置 → 智能群助手\n2. 添加机器人 → 自定义\n3. 复制Webhook地址\n4. 安全设置选择【自定义关键词】，填写上方的关键词")
            else:
                st.caption("飞书机器人配置：\n1. 进入飞书群 → 设置 → 群机器人\n2. 添加机器人 → 自定义机器人\n3. 复制Webhook地址")

        st.markdown("---")
        st.info("**使用说明**：\n- 可以同时启用邮件和Webhook通知\n- 实时监测和智策定时分析都会使用配置的通知方式\n- 配置后建议使用各功能中的测试按钮验证通知是否正常")

    # 操作按钮
    st.markdown("---")
    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])

    with col1:
        if st.button("保存配置", type="primary", width='stretch'):
            # 验证配置
            is_valid, message = config_manager.validate_config(st.session_state.temp_config)

            if is_valid:
                # 保存配置
                if config_manager.write_env(st.session_state.temp_config):
                    st.success("配置已保存到 .env 文件")
                    st.info("ℹ 请重启应用使配置生效")

                    # 尝试重新加载配置
                    try:
                        config_manager.reload_config()
                        st.success("配置已重新加载")
                    except Exception as e:
                        st.warning(f"配置重新加载失败: {e}")

                    time.sleep(2)
                    st.rerun()
                else:
                    st.error("保存配置失败")
            else:
                st.error(f"配置验证失败: {message}")

    with col2:
        if st.button("重置", width='stretch'):
            # 重置为当前文件中的值
            st.session_state.temp_config = {key: info["value"] for key, info in config_info.items()}
            st.success("已重置为当前配置")
            st.rerun()

    with col3:
        if st.button("返回 返回", width='stretch'):
            if 'show_config' in st.session_state:
                del st.session_state.show_config
            if 'temp_config' in st.session_state:
                del st.session_state.temp_config
            st.rerun()

    # 显示当前.env文件内容
    st.markdown("---")
    with st.expander("查看当前 .env 文件内容"):
        current_config = config_manager.read_env()

        st.code(f"""# AI股票分析系统环境配置
# 由系统自动生成和管理

# ========== DeepSeek API配置 ==========
DEEPSEEK_API_KEY="{current_config.get('DEEPSEEK_API_KEY', '')}"
DEEPSEEK_BASE_URL="{current_config.get('DEEPSEEK_BASE_URL', '')}"
ADMIN_PASSWORD="{current_config.get('ADMIN_PASSWORD', '')}"
ICP_NUMBER="{current_config.get('ICP_NUMBER', '')}"
ICP_LINK="{current_config.get('ICP_LINK', 'https://beian.miit.gov.cn/')}"

# ========== Tushare数据接口（可选）==========
TUSHARE_TOKEN="{current_config.get('TUSHARE_TOKEN', '')}"

# ========== MiniQMT量化交易配置（可选）==========
MINIQMT_ENABLED="{current_config.get('MINIQMT_ENABLED', 'false')}"
MINIQMT_ACCOUNT_ID="{current_config.get('MINIQMT_ACCOUNT_ID', '')}"
MINIQMT_HOST="{current_config.get('MINIQMT_HOST', '127.0.0.1')}"
MINIQMT_PORT="{current_config.get('MINIQMT_PORT', '58610')}"

# ========== 邮件通知配置（可选）==========
EMAIL_ENABLED="{current_config.get('EMAIL_ENABLED', 'false')}"
SMTP_SERVER="{current_config.get('SMTP_SERVER', '')}"
SMTP_PORT="{current_config.get('SMTP_PORT', '587')}"
EMAIL_FROM="{current_config.get('EMAIL_FROM', '')}"
EMAIL_PASSWORD="{current_config.get('EMAIL_PASSWORD', '')}"
EMAIL_TO="{current_config.get('EMAIL_TO', '')}"

# ========== Webhook通知配置（可选）==========
WEBHOOK_ENABLED="{current_config.get('WEBHOOK_ENABLED', 'false')}"
WEBHOOK_TYPE="{current_config.get('WEBHOOK_TYPE', 'dingtalk')}"
WEBHOOK_URL="{current_config.get('WEBHOOK_URL', '')}"
WEBHOOK_KEYWORD="{current_config.get('WEBHOOK_KEYWORD', 'aiagents通知')}"
""", language="bash")
