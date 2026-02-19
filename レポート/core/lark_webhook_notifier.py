"""
Lark Webhook通知機能
"""
import os
import requests
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class LarkWebhookNotifier:
    """Lark Webhook通知クラス"""
    
    def __init__(self):
        """初期化"""
        self.webhook_url = os.getenv('LARK_daily_webhook')
        if not self.webhook_url:
            logger.warning("⚠️ LARK_daily_webhookが設定されていません")
    
    def send_message(self, message: str) -> bool:
        """メッセージをLark webhookに送信"""
        try:
            if not self.webhook_url:
                logger.error("❌ Webhook URLが設定されていません")
                return False
            
            # Lark webhookのメッセージ形式
            payload = {
                "msg_type": "text",
                "content": {
                    "text": message
                }
            }
            
            logger.info("📤 Lark webhookにメッセージを送信中...")
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.info("✅ Lark webhook送信が成功しました")
                    return True
                else:
                    logger.error(f"❌ Lark webhook送信エラー: {result.get('msg', 'Unknown error')}")
                    return False
            else:
                logger.error(f"❌ HTTP エラー: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            logger.error("❌ Webhook送信がタイムアウトしました")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Webhook送信エラー: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ 予期しないエラー: {e}")
            return False
    
    def test_webhook(self) -> bool:
        """Webhook接続テスト"""
        try:
            test_message = "🧪 週次レポートシステムのWebhook接続テスト"
            return self.send_message(test_message)
        except Exception as e:
            logger.error(f"❌ Webhookテストエラー: {e}")
            return False
