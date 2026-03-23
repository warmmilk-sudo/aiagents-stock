import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import os
from typing import Dict, Optional
from urllib.parse import urlparse

from monitor_db import monitor_db

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*args, **kwargs):
        return False

class NotificationService:
    """通知服务"""
    
    def __init__(self):
        # 强制重新加载环境变量
        load_dotenv()
        self.config = self._load_config()
        self.last_webhook_error = ""
    
    def _load_config(self) -> Dict:
        """加载通知配置"""
        config = {
            'email_enabled': False,
            'smtp_server': '',
            'smtp_port': 587,
            'email_from': '',
            'email_password': '',
            'email_to': '',
            'webhook_enabled': False,
            'webhook_url': '',
            'webhook_type': 'dingtalk',  # dingtalk 或 feishu
            'webhook_keyword': 'aiagents通知'  # 钉钉自定义关键词
        }
        
        # 从环境变量加载配置
        if os.getenv('EMAIL_ENABLED'):
            config['email_enabled'] = os.getenv('EMAIL_ENABLED').lower() == 'true'
        if os.getenv('SMTP_SERVER'):
            config['smtp_server'] = os.getenv('SMTP_SERVER')
        if os.getenv('SMTP_PORT'):
            config['smtp_port'] = int(os.getenv('SMTP_PORT'))
        if os.getenv('EMAIL_FROM'):
            config['email_from'] = os.getenv('EMAIL_FROM')
        if os.getenv('EMAIL_PASSWORD'):
            config['email_password'] = os.getenv('EMAIL_PASSWORD')
        if os.getenv('EMAIL_TO'):
            config['email_to'] = os.getenv('EMAIL_TO')
        if os.getenv('WEBHOOK_ENABLED'):
            config['webhook_enabled'] = os.getenv('WEBHOOK_ENABLED').lower() == 'true'
        if os.getenv('WEBHOOK_URL'):
            config['webhook_url'] = os.getenv('WEBHOOK_URL')
        if os.getenv('WEBHOOK_TYPE'):
            config['webhook_type'] = os.getenv('WEBHOOK_TYPE').lower()
        if os.getenv('WEBHOOK_KEYWORD'):
            config['webhook_keyword'] = os.getenv('WEBHOOK_KEYWORD')
        
        return config
    
    def send_notifications(self):
        """发送所有待发送的通知"""
        notifications = monitor_db.get_pending_notifications()
        
        if not notifications:
            print("没有待发送的通知")
            return
        
        print(f"\n{'='*50}")
        print(f"开始发送通知，共 {len(notifications)} 条")
        print(f"{'='*50}")
        
        for notification in notifications:
            try:
                print(f"\n处理通知: {notification['symbol']} - {notification['type']}")
                if not self._is_notification_target_active(notification):
                    print(f"⏭️ 监控项已删除或停用，忽略通知: {notification['message']}")
                    continue
                if self.send_notification(notification):
                    monitor_db.mark_notification_sent(notification['id'])
                    print(f"✅ 通知已成功发送并标记: {notification['message']}")
                else:
                    print(f"❌ 通知发送失败: {notification['message']}")
            except Exception as e:
                print(f"❌ 发送通知时出错: {e}")
                import traceback
                traceback.print_exc()
        
        print(f"{'='*50}\n")
    
    def send_notification(self, notification: Dict) -> bool:
        """发送单个通知"""
        success = False
        
        # 尝试webhook通知
        if self.config['webhook_enabled']:
            webhook_success = self._send_webhook_notification(notification)
            if webhook_success:
                success = True
        
        # 尝试邮件通知
        if self.config['email_enabled']:
            email_success = self._send_email_notification(notification)
            if email_success:
                success = True
        
        # 未配置外部通知通道时，保留数据库事件供 UI 轮询展示。
        if not success:
            success = True
        
        return success

    def _is_notification_target_active(self, notification: Dict) -> bool:
        """仅向仍然有效的监控项发送通知，避免删除/停用后的遗留消息继续外发。"""
        raw_item_id = notification.get("stock_id") or notification.get("monitoring_item_id")
        item_id = None
        try:
            if raw_item_id is not None:
                item_id = int(raw_item_id)
        except (TypeError, ValueError):
            item_id = None
        if item_id is None or item_id <= 0:
            return True

        item = monitor_db.repository.get_item(item_id)
        if item and bool(item.get("enabled", True)) and bool(item.get("notification_enabled", True)):
            return True

        try:
            notification_id = notification.get("id")
            if notification_id is not None:
                monitor_db.ignore_notification(int(notification_id))
        except Exception:
            pass
        return False
    
    def _send_email_notification(self, notification: Dict) -> bool:
        """发送邮件通知"""
        try:
            # 检查邮件配置是否完整
            if not all([self.config['smtp_server'], self.config['email_from'], 
                       self.config['email_password'], self.config['email_to']]):
                print("⚠️ 邮件配置不完整，跳过邮件发送")
                print(f"  - SMTP服务器: {self.config['smtp_server'] or '未配置'}")
                print(f"  - 发件人: {self.config['email_from'] or '未配置'}")
                print(f"  - 收件人: {self.config['email_to'] or '未配置'}")
                print(f"  - 密码: {'已配置' if self.config['email_password'] else '未配置'}")
                return True
            
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = self.config['email_from']
            msg['To'] = self.config['email_to']
            msg['Subject'] = f"股票监测提醒 - {notification['symbol']}"
            
            # 邮件正文
            body = f"""
            <h2>股票监测提醒</h2>
            <p><strong>股票代码:</strong> {notification['symbol']}</p>
            <p><strong>股票名称:</strong> {notification['name']}</p>
            <p><strong>提醒类型:</strong> {notification['type']}</p>
            <p><strong>提醒内容:</strong> {notification['message']}</p>
            <p><strong>触发时间:</strong> {notification['triggered_at']}</p>
            <hr>
            <p><em>此邮件由AI股票分析系统自动发送</em></p>
            """
            
            msg.attach(MIMEText(body, 'html'))
            
            print(f"📧 正在发送邮件...")
            print(f"  - 收件人: {self.config['email_to']}")
            print(f"  - 主题: 股票监测提醒 - {notification['symbol']}")
            
            # 根据端口选择连接方式
            if self.config['smtp_port'] == 465:
                print(f"  - 使用 SMTP_SSL 连接 {self.config['smtp_server']}:{self.config['smtp_port']}")
                server = smtplib.SMTP_SSL(self.config['smtp_server'], self.config['smtp_port'], timeout=15)
            else:
                print(f"  - 使用 SMTP+TLS 连接 {self.config['smtp_server']}:{self.config['smtp_port']}")
                server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'], timeout=15)
                server.starttls()
            
            print(f"  - 正在登录...")
            server.login(self.config['email_from'], self.config['email_password'])
            print(f"  - 正在发送...")
            server.send_message(msg)
            server.quit()
            print(f"✅ 邮件发送成功: {notification['symbol']}")
            return True
            
        except Exception as e:
            print(f"邮件发送失败: {e}")
            return False
    
    def test_email_config(self) -> bool:
        """测试邮件配置"""
        if not self.config['email_enabled']:
            return False
        
        try:
            if self.config['smtp_port'] == 465:
                server = smtplib.SMTP_SSL(self.config['smtp_server'], self.config['smtp_port'], timeout=10)
            else:
                server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'], timeout=10)
                server.starttls()
            
            server.login(self.config['email_from'], self.config['email_password'])
            server.quit()
            return True
        except Exception as e:
            print(f"邮件配置测试失败: {e}")
            return False
    
    def send_test_email(self) -> tuple[bool, str]:
        """发送测试邮件"""
        try:
            # 检查邮件配置是否完整
            if not all([self.config['smtp_server'], self.config['email_from'], 
                       self.config['email_password'], self.config['email_to']]):
                return False, "邮件配置不完整，请检查.env文件中的邮件设置"
            
            # 创建测试邮件
            msg = MIMEMultipart()
            msg['From'] = self.config['email_from']
            msg['To'] = self.config['email_to']
            msg['Subject'] = "AI股票分析系统 - 邮件测试"
            
            # 邮件正文
            body = f"""
            <html>
            <body>
                <h2>邮件测试成功！</h2>
                <p>这是一封来自AI股票分析系统的测试邮件。</p>
                <p>如果您收到这封邮件，说明邮件通知功能已正常工作。</p>
                <hr>
                <p><strong>邮件配置信息：</strong></p>
                <ul>
                    <li>SMTP服务器: {self.config['smtp_server']}</li>
                    <li>SMTP端口: {self.config['smtp_port']}</li>
                    <li>发送邮箱: {self.config['email_from']}</li>
                    <li>接收邮箱: {self.config['email_to']}</li>
                </ul>
                <hr>
                <p><em>此邮件由AI股票分析系统自动发送</em></p>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(body, 'html'))
            
            # 根据端口选择连接方式
            if self.config['smtp_port'] == 465:
                server = smtplib.SMTP_SSL(self.config['smtp_server'], self.config['smtp_port'], timeout=15)
            else:
                server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'], timeout=15)
                server.starttls()
            
            server.login(self.config['email_from'], self.config['email_password'])
            server.send_message(msg)
            server.quit()
            return True, "测试邮件发送成功！请检查收件箱（包括垃圾邮件箱）。"
            
        except smtplib.SMTPAuthenticationError:
            return False, "邮箱认证失败，请检查邮箱和授权码是否正确"
        except smtplib.SMTPException as e:
            return False, f"SMTP错误: {str(e)}"
        except Exception as e:
            return False, f"发送失败: {str(e)}"
    
    def get_email_config_status(self) -> Dict:
        """获取邮件配置状态"""
        return {
            'enabled': self.config['email_enabled'],
            'smtp_server': self.config['smtp_server'] or '未配置',
            'smtp_port': self.config['smtp_port'],
            'email_from': self.config['email_from'] or '未配置',
            'email_to': self.config['email_to'] or '未配置',
            'configured': all([
                self.config['smtp_server'],
                self.config['email_from'],
                self.config['email_password'],
                self.config['email_to']
            ])
        }
    
    def _send_webhook_notification(self, notification: Dict) -> bool:
        """发送Webhook通知"""
        try:
            self._clear_webhook_error()
            # 检查webhook配置是否完整
            if not self.config['webhook_url']:
                self._set_webhook_error("Webhook URL未配置，请先保存通知配置")
                print("⚠️ Webhook URL未配置")
                return False
            
            webhook_type = self.config['webhook_type']
            validation_error = self._validate_webhook_url(webhook_type)
            if validation_error:
                self._set_webhook_error(validation_error)
                print(f"⚠️ {validation_error}")
                return False
            
            if webhook_type == 'dingtalk':
                return self._send_dingtalk_webhook(notification)
            elif webhook_type == 'feishu':
                return self._send_feishu_webhook(notification)
            else:
                self._set_webhook_error(f"不支持的webhook类型: {webhook_type}")
                print(f"⚠️ 不支持的webhook类型: {webhook_type}")
                return False
        
        except Exception as e:
            self._set_webhook_error(f"Webhook发送失败: {e}")
            print(f"Webhook发送失败: {e}")
            return False

    def _set_webhook_error(self, message: str) -> str:
        self.last_webhook_error = str(message or "").strip()
        return self.last_webhook_error

    def _clear_webhook_error(self):
        self.last_webhook_error = ""

    def _validate_webhook_url(self, webhook_type: str) -> Optional[str]:
        url = str(self.config.get('webhook_url') or "").strip()
        if not url:
            return "Webhook URL未配置，请先保存通知配置"

        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return "Webhook URL格式无效，请填写完整的 http(s) 地址"

        lowered = url.lower()
        if webhook_type == 'feishu' and ('dingtalk.com' in lowered or '/robot/send' in lowered):
            return "当前 URL 更像钉钉机器人地址，请确认 WEBHOOK_TYPE=feishu 且使用飞书机器人 Webhook"
        if webhook_type == 'dingtalk' and (
            'feishu.cn' in lowered or 'larksuite.com' in lowered or '/bot/v2/hook/' in lowered
        ):
            return "当前 URL 更像飞书机器人地址，请确认 WEBHOOK_TYPE=dingtalk 且使用钉钉机器人 Webhook"
        return None

    def _extract_response_message(self, response, *, default: str) -> str:
        try:
            payload = response.json()
        except ValueError:
            payload = None

        if isinstance(payload, dict):
            for key in ('msg', 'errmsg', 'message', 'error'):
                value = str(payload.get(key) or "").strip()
                if value:
                    return value

        raw_text = str(getattr(response, 'text', '') or '').strip()
        if raw_text:
            return raw_text[:160]
        return default
    
    def _send_dingtalk_webhook(self, notification: Dict) -> bool:
        """发送钉钉Webhook通知"""
        try:
            import requests

            def _fmt_num(value):
                if value is None:
                    return None
                try:
                    return f"{float(value):.2f}"
                except Exception:
                    return None
            
            # 构建钉钉消息格式（包含自定义关键词）
            keyword = self.config.get('webhook_keyword', '')
            title_prefix = f"{keyword} - " if keyword else ""
            content_prefix = f"### {keyword} - " if keyword else "### "

            turnover_rate = notification.get('turnover_rate')
            turnover_rate_text = _fmt_num(turnover_rate)
            
            message_lines = [
                f"{content_prefix}股票监测提醒",
                "",
                f"**股票代码**: {notification['symbol']}",
                "",
                f"**股票名称**: {notification['name']}",
                "",
                "**📊 实时行情**:",
            ]
            current_price = notification.get("current_price")
            if current_price is not None:
                message_lines.append(f"- 当前价格: {current_price}元")
            change_pct = notification.get("change_pct")
            if change_pct is not None:
                message_lines.append(f"- 涨跌幅: {change_pct}%")
            change_amount = notification.get("change_amount")
            if change_amount is not None:
                message_lines.append(f"- 涨跌额: {change_amount}元")
            volume = notification.get("volume")
            if volume is not None:
                message_lines.append(f"- 成交量: {volume}手")
            if turnover_rate_text is not None:
                message_lines.append(f"- 换手率: {turnover_rate_text}%")
            message_lines.extend([
                f"**🎯 AI决策**: {notification['type']}",
                "",
                f"**📝 分析内容**: {notification['message']}",
                "",
                "**💰 持仓信息**:",
            ])
            position_status = notification.get("position_status")
            if position_status is not None:
                message_lines.append(f"- 持仓状态: {position_status}")
            position_cost = notification.get("position_cost")
            if position_cost is not None:
                message_lines.append(f"- 持仓成本: {position_cost}元")
            profit_loss_pct = notification.get("profit_loss_pct")
            if profit_loss_pct is not None:
                message_lines.append(f"- 浮动盈亏: {profit_loss_pct}%")
            message_lines.extend([
                "",
                f"**⏰ 触发时间**: {notification['triggered_at']}",
                "",
            ])
            trading_session = notification.get("trading_session")
            if trading_session is not None:
                message_lines.append(f"**🕐 交易时段**: {trading_session}")
            message_lines.extend([
                "",
                "---",
                "",
                "_此消息由AI股票分析系统自动发送_",
            ])
            message_text = "\n".join(message_lines)
            
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": f"{title_prefix}{notification['symbol']} {notification['name']}",
                    "text": message_text
                }
            }
            
            print(f"[钉钉] 正在发送Webhook...")
            print(f"  - URL: {self.config['webhook_url'][:50]}...")
            
            response = requests.post(
                self.config['webhook_url'],
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    self._clear_webhook_error()
                    print(f"[成功] 钉钉Webhook发送成功")
                    return True
                else:
                    errmsg = result.get('errmsg')
                    error_message = f"钉钉Webhook返回错误: {errmsg}" if errmsg else "钉钉Webhook返回错误"
                    self._set_webhook_error(error_message)
                    print(f"[失败] {error_message}")
                    return False
            else:
                error_message = self._extract_response_message(
                    response,
                    default=f"HTTP {response.status_code}",
                )
                self._set_webhook_error(f"钉钉Webhook请求失败: HTTP {response.status_code} - {error_message}")
                print(f"[失败] 钉钉Webhook请求失败: HTTP {response.status_code}")
                return False
        
        except requests.exceptions.Timeout:
            self._set_webhook_error("钉钉Webhook请求超时，请检查网络连接、代理设置或目标地址可达性")
            print("钉钉Webhook发送异常: 请求超时")
            return False
        except requests.exceptions.RequestException as e:
            self._set_webhook_error(f"钉钉Webhook请求异常: {e}")
            print(f"钉钉Webhook发送异常: {e}")
            return False
        except Exception as e:
            self._set_webhook_error(f"钉钉Webhook发送异常: {e}")
            print(f"钉钉Webhook发送异常: {e}")
            return False
    
    def _send_feishu_webhook(self, notification: Dict) -> bool:
        """发送飞书Webhook通知"""
        try:
            import requests
            keyword = str(self.config.get('webhook_keyword') or '').strip()
            title_prefix = f"{keyword} - " if keyword else ""
            keyword_block = []
            if keyword:
                keyword_block.append(
                    {
                        "tag": "div",
                        "text": {
                            "content": f"**关键词**\n{keyword}",
                            "tag": "lark_md"
                        }
                    }
                )
            
            # 构建飞书消息格式
            data = {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {
                            "content": f"📊 {title_prefix}股票监测提醒 - {notification['symbol']}",
                            "tag": "plain_text"
                        },
                        "template": "blue"
                    },
                    "elements": keyword_block + [
                        {
                            "tag": "div",
                            "fields": [
                                {
                                    "is_short": True,
                                    "text": {
                                        "content": f"**股票代码**\n{notification['symbol']}",
                                        "tag": "lark_md"
                                    }
                                },
                                {
                                    "is_short": True,
                                    "text": {
                                        "content": f"**股票名称**\n{notification['name']}",
                                        "tag": "lark_md"
                                    }
                                }
                            ]
                        },
                        {
                            "tag": "div",
                            "fields": [
                                {
                                    "is_short": True,
                                    "text": {
                                        "content": f"**提醒类型**\n{notification['type']}",
                                        "tag": "lark_md"
                                    }
                                },
                                {
                                    "is_short": True,
                                    "text": {
                                        "content": f"**触发时间**\n{notification['triggered_at']}",
                                        "tag": "lark_md"
                                    }
                                }
                            ]
                        },
                        {
                            "tag": "div",
                            "text": {
                                "content": f"**提醒内容**\n{notification['message']}",
                                "tag": "lark_md"
                            }
                        },
                        {
                            "tag": "hr"
                        },
                        {
                            "tag": "note",
                            "elements": [
                                {
                                    "tag": "plain_text",
                                    "content": "此消息由AI股票分析系统自动发送"
                                }
                            ]
                        }
                    ]
                }
            }
            
            print(f"[飞书] 正在发送Webhook...")
            print(f"  - URL: {self.config['webhook_url'][:50]}...")
            
            response = requests.post(
                self.config['webhook_url'],
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    self._clear_webhook_error()
                    print(f"[成功] 飞书Webhook发送成功")
                    return True
                else:
                    msg = result.get('msg')
                    error_message = f"飞书Webhook返回错误: {msg}" if msg else "飞书Webhook返回错误"
                    self._set_webhook_error(error_message)
                    print(f"[失败] {error_message}")
                    return False
            else:
                error_message = self._extract_response_message(
                    response,
                    default=f"HTTP {response.status_code}",
                )
                self._set_webhook_error(f"飞书Webhook请求失败: HTTP {response.status_code} - {error_message}")
                print(f"[失败] 飞书Webhook请求失败: HTTP {response.status_code}")
                return False
        
        except requests.exceptions.Timeout:
            self._set_webhook_error("飞书Webhook请求超时，请检查网络连接、代理设置或目标地址可达性")
            print("飞书Webhook发送异常: 请求超时")
            return False
        except requests.exceptions.RequestException as e:
            self._set_webhook_error(f"飞书Webhook请求异常: {e}")
            print(f"飞书Webhook发送异常: {e}")
            return False
        except Exception as e:
            self._set_webhook_error(f"飞书Webhook发送异常: {e}")
            print(f"飞书Webhook发送异常: {e}")
            return False
    
    def send_test_webhook(self) -> tuple[bool, str]:
        """发送测试Webhook"""
        try:
            self._clear_webhook_error()
            # 检查webhook配置是否完整
            if not self.config['webhook_url']:
                return False, "Webhook URL未配置，请检查环境变量设置"
            
            # 创建测试通知（包含关键词"aiagents通知"以通过钉钉安全设置）
            test_notification = {
                'symbol': '测试',
                'name': 'Webhook配置测试',
                'type': '系统测试',
                'message': '如果您收到此消息，说明Webhook配置正确！',
                'triggered_at': '刚刚'
            }
            
            webhook_type = self.config['webhook_type']
            validation_error = self._validate_webhook_url(webhook_type)
            if validation_error:
                return False, validation_error
            
            if webhook_type == 'dingtalk':
                success = self._send_dingtalk_webhook(test_notification)
                if success:
                    return True, "钉钉Webhook测试成功！请检查钉钉群消息。"
                else:
                    return False, self.last_webhook_error or "钉钉Webhook发送失败，请检查URL和网络连接"
            
            elif webhook_type == 'feishu':
                success = self._send_feishu_webhook(test_notification)
                if success:
                    return True, "飞书Webhook测试成功！请检查飞书群消息。"
                else:
                    return False, self.last_webhook_error or "飞书Webhook发送失败，请检查URL和网络连接"
            
            else:
                return False, f"不支持的webhook类型: {webhook_type}"
        
        except Exception as e:
            return False, f"发送失败: {str(e)}"
    
    def get_webhook_config_status(self) -> Dict:
        """获取Webhook配置状态"""
        return {
            'enabled': self.config['webhook_enabled'],
            'webhook_type': self.config['webhook_type'],
            'webhook_url': self.config['webhook_url'][:50] + '...' if self.config['webhook_url'] else '未配置',
            'configured': bool(self.config['webhook_url']),
            'last_error': self.last_webhook_error or '无'
        }
    
    def send_portfolio_analysis_notification(self, analysis_results: dict, sync_result: dict = None) -> bool:
        """
        发送持仓分析完成通知
        
        Args:
            analysis_results: 批量分析结果
            sync_result: 监测同步结果（可选）
            
        Returns:
            是否发送成功
        """
        try:
            def _fmt_num(value):
                if value is None:
                    return None
                try:
                    return f"{float(value):.2f}"
                except Exception:
                    return None

            # 构建通知内容
            total = analysis_results.get("total")
            succeeded = len([r for r in analysis_results.get("results", []) if r.get("result", {}).get("success")])
            failed = analysis_results.get("failed")
            if failed is None and total is not None:
                failed = total - succeeded
            elapsed_time = analysis_results.get("elapsed_time")
            results = analysis_results.get("results", [])
            
            # 邮件主题
            subject = f"持仓定时分析完成 - 共{total}只股票" if total is not None else "持仓定时分析完成"
            
            # 构建邮件正文（HTML格式）
            html_body = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; }}
                    .summary {{ background-color: #f0f8ff; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                    .stock {{ border: 1px solid #ddd; padding: 10px; margin-bottom: 10px; border-radius: 5px; }}
                    .success {{ color: green; }}
                    .failed {{ color: red; }}
                    .rating-buy {{ color: #28a745; font-weight: bold; }}
                    .rating-hold {{ color: #ffc107; font-weight: bold; }}
                    .rating-sell {{ color: #dc3545; font-weight: bold; }}
                </style>
            </head>
            <body>
                <h2>持仓定时分析完成</h2>
                <div class="summary">
                    <h3>分析概况</h3>
                    <p>总数: {total if total is not None else ''} 只</p>
                    <p class="success">成功: {succeeded} 只</p>
                    <p class="failed">失败: {failed if failed is not None else ''} 只</p>
                    <p>耗时: {(_fmt_num(elapsed_time) or '')} 秒</p>
            """
            
            # 添加监测同步结果
            if sync_result:
                added = sync_result.get("added")
                updated = sync_result.get("updated")
                failed_sync = sync_result.get("failed")
                html_body += f"""
                    <h3>监测同步结果</h3>
                    <p>新增监测: {added if added is not None else ''} 只</p>
                    <p>更新监测: {updated if updated is not None else ''} 只</p>
                    <p>同步失败: {failed_sync if failed_sync is not None else ''} 只</p>
                """
            
            html_body += """
                </div>
                <h3>分析结果详情</h3>
            """
            
            # 添加每只股票的详细结果
            for item in results[:10]:  # 只显示前10只
                code = item.get("code", "")
                result = item.get("result", {})
                
                if result.get("success"):
                    final_decision = result.get("final_decision", {})
                    stock_info = result.get("stock_info", {})
                    
                    # 使用正确的字段名
                    rating = final_decision.get("rating")
                    confidence = final_decision.get("confidence_level")
                    entry_range = final_decision.get("entry_range")
                    take_profit = final_decision.get("take_profit")
                    stop_loss = final_decision.get("stop_loss")
                    
                    # 评级颜色
                    rating_class = "rating-hold"
                    if "强烈买入" in rating or "买入" in rating:
                        rating_class = "rating-buy"
                    elif "卖出" in rating:
                        rating_class = "rating-sell"
                    
                    html_body += f"""
                    <div class="stock">
                        <h4>{code} {stock_info.get('name', '')} - <span class="{rating_class}">{rating or ''}</span>{f' (信心度: {confidence})' if confidence is not None else ''}</h4>
                        {f'<p>进场区间: {entry_range}</p>' if entry_range is not None else ''}
                        {f'<p>止盈位: {take_profit} | 止损位: {stop_loss}</p>' if take_profit is not None or stop_loss is not None else ''}
                    </div>
                    """
                else:
                    error = result.get("error")
                    html_body += f"""
                    <div class="stock">
                        <h4 class="failed">{code} - 分析失败</h4>
                        {f'<p>错误: {error}</p>' if error is not None else ''}
                    </div>
                    """
            
            if len(results) > 10:
                html_body += f"<p>...还有 {len(results) - 10} 只股票未显示</p>"
            
            html_body += """
            </body>
            </html>
            """
            
            # 构建纯文本版本
            text_body = f"""
持仓定时分析完成

分析概况:
- 总数: {total if total is not None else ''} 只
- 成功: {succeeded} 只
- 失败: {failed if failed is not None else ''} 只
- 耗时: {(_fmt_num(elapsed_time) or '')} 秒
"""
            
            if sync_result:
                added = sync_result.get("added")
                updated = sync_result.get("updated")
                failed_sync = sync_result.get("failed")
                text_body += f"""
监测同步结果:
- 新增监测: {added if added is not None else ''} 只
- 更新监测: {updated if updated is not None else ''} 只
- 同步失败: {failed_sync if failed_sync is not None else ''} 只
"""
            
            text_body += "\n分析结果详情:\n"
            for item in results[:10]:
                code = item.get("code", "")
                result = item.get("result", {})
                
                if result.get("success"):
                    final_decision = result.get("final_decision", {})
                    stock_info = result.get("stock_info", {})
                    # 使用正确的字段名
                    rating = final_decision.get("rating")
                    text_body += f"- {code} {stock_info.get('name', '')}: {rating if rating is not None else ''}\n"
                else:
                    error = result.get("error")
                    text_body += f"- {code}: 分析失败 ({error if error is not None else ''})\n"
            
            success = False
            
            # 发送邮件
            if self.config['email_enabled']:
                email_success = self._send_custom_email(subject, html_body, text_body)
                if email_success:
                    success = True
                    print("[OK] 邮件通知发送成功")
            
            # 发送Webhook
            if self.config['webhook_enabled']:
                webhook_success = self._send_portfolio_webhook(analysis_results, sync_result)
                if webhook_success:
                    success = True
                    print("[OK] Webhook通知发送成功")
            
            return success
            
        except Exception as e:
            print(f"[ERROR] 发送持仓分析通知失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def _send_custom_email(self, subject: str, html_body: str, text_body: str) -> bool:
        """发送自定义邮件"""
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = self.config['email_from']
            msg['To'] = self.config['email_to']
            msg['Subject'] = subject
            
            part1 = MIMEText(text_body, 'plain', 'utf-8')
            part2 = MIMEText(html_body, 'html', 'utf-8')
            
            msg.attach(part1)
            msg.attach(part2)
            
            with smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port']) as server:
                server.starttls()
                server.login(self.config['email_from'], self.config['email_password'])
                server.send_message(msg)
            
            return True
            
        except Exception as e:
            print(f"[ERROR] 邮件发送失败: {str(e)}")
            return False
    
    def _send_portfolio_webhook(self, analysis_results: dict, sync_result: dict = None) -> bool:
        """发送持仓分析Webhook通知"""
        try:
            import requests
            
            total = analysis_results.get("total")
            succeeded = len([r for r in analysis_results.get("results", []) if r.get("result", {}).get("success")])
            failed = analysis_results.get("failed")
            if failed is None and total is not None:
                failed = total - succeeded
            elapsed_time = analysis_results.get("elapsed_time")
            
            # 构建Markdown消息
            content = f"### 持仓定时分析完成\\n\\n"
            content += f"**分析概况**\\n"
            content += f"- 总数: {total if total is not None else ''} 只\\n"
            content += f"- 成功: {succeeded} 只\\n"
            content += f"- 失败: {failed if failed is not None else ''} 只\\n"
            content += f"- 耗时: {elapsed_time if elapsed_time is not None else ''} 秒\\n\\n"
            
            if sync_result:
                content += f"**监测同步**\\n"
                added = sync_result.get("added")
                updated = sync_result.get("updated")
                content += f"- 新增: {added if added is not None else ''} 只\\n"
                content += f"- 更新: {updated if updated is not None else ''} 只\\n\\n"
            
            # 根据webhook类型构建请求
            if self.config['webhook_type'] == 'dingtalk':
                data = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": f"{self.config['webhook_keyword']}" if self.config['webhook_keyword'] else "持仓定时分析完成",
                        "text": f"{self.config['webhook_keyword']}\\n\\n{content}" if self.config['webhook_keyword'] else content
                    }
                }
            else:  # feishu
                data = {
                    "msg_type": "text",
                    "content": {
                        "text": content
                    }
                }
            
            response = requests.post(self.config['webhook_url'], json=data, timeout=10)
            return response.status_code == 200
            
        except Exception as e:
            print(f"[ERROR] Webhook发送失败: {str(e)}")
            return False

# 全局通知服务实例
notification_service = NotificationService()
