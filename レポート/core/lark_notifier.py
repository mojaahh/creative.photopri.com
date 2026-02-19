#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lark通知システム
週次レポートをLarkに送信する機能
"""

import os
import sys
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lark_notifier.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LarkNotifier:
    """Lark通知クラス"""
    
    def __init__(self):
        """初期化"""
        # ルートディレクトリの.envファイルを読み込み
        load_dotenv('../.env')
        
        self.app_id = os.getenv('LARK_APP_ID')
        self.app_secret = os.getenv('LARK_APP_SECRET')
        
        if not self.app_id or not self.app_secret:
            raise ValueError("LARK_APP_ID または LARK_APP_SECRET が設定されていません")
        
        self.access_token = None
        self.base_url = "https://open.larksuite.com/open-apis"
        
        # アクセストークンを取得
        self._get_access_token()
    
    def _get_access_token(self):
        """Larkアクセストークンを取得"""
        try:
            url = f"{self.base_url}/auth/v3/tenant_access_token/internal"
            headers = {
                "Content-Type": "application/json; charset=utf-8"
            }
            data = {
                "app_id": self.app_id,
                "app_secret": self.app_secret
            }
            
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            
            result = response.json()
            if result.get('code') == 0:
                self.access_token = result.get('tenant_access_token')
                logger.info("Larkアクセストークンの取得に成功しました")
            else:
                raise Exception(f"アクセストークン取得エラー: {result.get('msg', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Larkアクセストークン取得エラー: {e}")
            raise
    
    def _get_headers(self):
        """APIリクエスト用のヘッダーを取得"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
    
    def get_chat_list(self) -> List[Dict]:
        """チャット一覧を取得"""
        try:
            url = f"{self.base_url}/im/v1/chats"
            headers = self._get_headers()
            params = {
                "page_size": 100
            }
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            result = response.json()
            if result.get('code') == 0:
                return result.get('data', {}).get('items', [])
            else:
                logger.error(f"チャット一覧取得エラー: {result.get('msg', 'Unknown error')}")
                return []
                
        except Exception as e:
            logger.error(f"チャット一覧取得エラー: {e}")
            return []
    
    def find_chat_by_name(self, chat_name: str) -> Optional[str]:
        """チャット名からチャットIDを検索"""
        try:
            chats = self.get_chat_list()
            for chat in chats:
                if chat.get('name') == chat_name:
                    return chat.get('chat_id')
            return None
        except Exception as e:
            logger.error(f"チャット検索エラー: {e}")
            return None
    
    def send_message(self, chat_id: str, message: str, message_type: str = "text") -> bool:
        """メッセージを送信"""
        try:
            url = f"{self.base_url}/im/v1/messages"
            headers = self._get_headers()
            params = {
                "receive_id_type": "chat_id"
            }
            
            if message_type == "text":
                content = {
                    "text": message
                }
            else:
                content = {
                    "text": message
                }
            
            data = {
                "receive_id": chat_id,
                "msg_type": message_type,
                "content": json.dumps(content)
            }
            
            response = requests.post(url, headers=headers, params=params, json=data)
            response.raise_for_status()
            
            result = response.json()
            if result.get('code') == 0:
                logger.info(f"メッセージ送信成功: {chat_id}")
                return True
            else:
                logger.error(f"メッセージ送信エラー: {result.get('msg', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"メッセージ送信エラー: {e}")
            return False
    
    def send_weekly_report(self, chat_id: str, report_data: Dict) -> bool:
        """週次レポートを送信"""
        try:
            # レポートメッセージを構築
            message = self._build_weekly_report_message(report_data)
            
            # メッセージを送信
            return self.send_message(chat_id, message)
            
        except Exception as e:
            logger.error(f"週次レポート送信エラー: {e}")
            return False
    
    def _build_weekly_report_message(self, report_data: Dict) -> str:
        """週次レポートメッセージを構築"""
        try:
            # 日付情報を取得
            today = datetime.now()
            current_month = today.month
            current_year = today.year
            
            # 週末の日付範囲を計算（直前の土曜日0時〜月曜9時30分）
            days_since_monday = today.weekday()  # 月曜日=0
            last_saturday = today - timedelta(days=days_since_monday + 2)  # 土曜日
            weekend_start = last_saturday.replace(hour=0, minute=0, second=0, microsecond=0)
            weekend_end = today.replace(hour=9, minute=30, second=0, microsecond=0)
            
            message = f"""📈{current_year}年{current_month}月{today.day}日〜{weekend_end.strftime('%m月%d日')}ウィークリーサマリー

【{current_month}月の目標売上】
全体：{report_data.get('monthly_target_total', 0):,}円

{self._format_service_targets(report_data.get('monthly_targets', {}))}

【本日時点での{current_month}月売上】
{self._format_monthly_sales(report_data.get('monthly_sales', {}), report_data.get('monthly_targets', {}))}

【週末({weekend_start.strftime('%m月%d日')}0時〜{weekend_end.strftime('%m月%d日')}9時30分)の注文】
{self._format_weekend_orders(report_data.get('weekend_orders', {}))}"""
            
            return message
            
        except Exception as e:
            logger.error(f"レポートメッセージ構築エラー: {e}")
            return f"レポート生成中にエラーが発生しました: {str(e)}"
    
    def _format_service_targets(self, targets: Dict) -> str:
        """サービス別目標売上をフォーマット"""
        if not targets:
            return ""
        
        lines = []
        for service, amount in targets.items():
            lines.append(f"{service}：{amount:,}円")
        
        return "\n".join(lines)
    
    def _format_monthly_sales(self, sales: Dict, targets: Dict) -> str:
        """月間売上をフォーマット"""
        if not sales:
            return "データがありません"
        
        lines = []
        
        # 全体
        total_sales = sales.get('total', {})
        total_amount = total_sales.get('amount', 0)
        total_orders = total_sales.get('orders', 0)
        total_target = targets.get('total', 1)
        total_percentage = (total_amount / total_target * 100) if total_target > 0 else 0
        
        lines.append(f"全体：{total_amount:,}円 - {total_percentage:.1f}%({total_orders}件)")
        
        # サービス別
        services = ['#P', '#E', '#A', '#Q']
        for service in services:
            service_sales = sales.get(service, {})
            service_amount = service_sales.get('amount', 0)
            service_orders = service_sales.get('orders', 0)
            service_target = targets.get(service, 1)
            service_percentage = (service_amount / service_target * 100) if service_target > 0 else 0
            
            lines.append(f"{service}：{service_amount:,}円 - {service_percentage:.1f}%({service_orders}件)")
        
        return "\n".join(lines)
    
    def _format_weekend_orders(self, orders: Dict) -> str:
        """週末注文をフォーマット"""
        if not orders:
            return "データがありません"
        
        lines = []
        
        # 全体
        total_orders = orders.get('total', {})
        total_amount = total_orders.get('amount', 0)
        total_count = total_orders.get('orders', 0)
        lines.append(f"全体：{total_amount:,}円({total_count}件)")
        
        # サービス別
        services = ['#P', '#E', '#A', '#Q']
        for service in services:
            service_orders = orders.get(service, {})
            service_amount = service_orders.get('amount', 0)
            service_count = service_orders.get('orders', 0)
            lines.append(f"{service}：{service_amount:,}円({service_count}件)")
        
        return "\n".join(lines)
    
    def test_connection(self) -> bool:
        """接続テスト"""
        try:
            chats = self.get_chat_list()
            logger.info(f"接続テスト成功: {len(chats)}件のチャットを取得")
            return True
        except Exception as e:
            logger.error(f"接続テスト失敗: {e}")
            return False

def main():
    """テスト用メイン関数"""
    try:
        notifier = LarkNotifier()
        
        # 接続テスト
        if notifier.test_connection():
            print("✅ Lark接続テスト成功")
        else:
            print("❌ Lark接続テスト失敗")
            return
        
        # チャット一覧を表示
        chats = notifier.get_chat_list()
        print(f"\n📋 利用可能なチャット一覧 ({len(chats)}件):")
        for chat in chats[:10]:  # 最初の10件のみ表示
            print(f"  - {chat.get('name', 'Unknown')} (ID: {chat.get('chat_id', 'Unknown')})")
        
        if len(chats) > 10:
            print(f"  ... 他{len(chats) - 10}件")
        
        # テストメッセージ送信（最初のチャットに送信）
        if chats:
            test_chat_id = chats[0].get('chat_id')
            test_message = "🧪 Lark通知システムのテストメッセージです"
            
            if notifier.send_message(test_chat_id, test_message):
                print(f"✅ テストメッセージ送信成功: {chats[0].get('name', 'Unknown')}")
            else:
                print("❌ テストメッセージ送信失敗")
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        logger.error(f"メイン実行エラー: {e}")

if __name__ == "__main__":
    main()
