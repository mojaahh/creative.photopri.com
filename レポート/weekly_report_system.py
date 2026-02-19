#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
週次レポートシステム
毎週月曜日9:30に注文情報回収とスプレッドシート反映を実行し、Larkに通知
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from core.weekly_scheduler import WeeklyScheduler
from core.lark_webhook_notifier import LarkWebhookNotifier
from core.summary_generator import SummaryGenerator

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weekly_report_system.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WeeklyReportSystem:
    """週次レポートシステム"""
    
    def __init__(self):
        """初期化"""
        self.scheduler = None
        self.lark_notifier = None
        self.summary_generator = None
    
    def setup_components(self):
        """コンポーネントを初期化"""
        try:
            logger.info("コンポーネントを初期化中...")
            
            # スケジューラーを初期化
            self.scheduler = WeeklyScheduler()
            
            # Lark Webhook通知システムを初期化
            self.lark_notifier = LarkWebhookNotifier()
            
            # サマリー生成システムを初期化
            self.summary_generator = SummaryGenerator()
            
            logger.info("✅ 全コンポーネントの初期化が完了しました")
            return True
            
        except Exception as e:
            logger.error(f"コンポーネント初期化エラー: {e}")
            return False
    
    def run_scheduler(self):
        """スケジューラーを実行"""
        try:
            if not self.setup_components():
                logger.error("コンポーネントの初期化に失敗しました")
                return False
            
            logger.info("🕐 週次スケジューラーを開始します")
            logger.info("📅 毎週月曜日 9:00 にレポート処理を実行します")
            logger.info("📤 Lark Webhook通知を使用します")
            
            print("\n" + "=" * 60)
            print("🚀 週次レポートシステム")
            print("=" * 60)
            print("📅 スケジュール: 毎週月曜日 9:00")
            print("📊 処理内容:")
            print("  1. 注文データの取得・更新（直近3ヶ月）")
            print("  2. 顧客データの更新")
            print("  3. ユーザー分析の更新")
            print("  4. サービス別分析の更新")
            print("  5. 離脱者アラートの更新")
            print("  6. 週次サマリーの生成")
            print("  7. Lark通知の送信")
            print("📤 通知先: Lark Webhook")
            print("⏹️  停止するには Ctrl+C を押してください")
            print("=" * 60)
            
            # スケジューラーを開始
            self.scheduler.schedule_weekly_report()
            
        except KeyboardInterrupt:
            logger.info("スケジューラーを停止します")
            print("\n👋 スケジューラーを停止しました")
        except Exception as e:
            logger.error(f"スケジューラー実行エラー: {e}")
            print(f"❌ エラー: {e}")
            return False
    
    def run_immediately(self, notify: bool = True):
        """即座に実行"""
        try:
            if not self.setup_components():
                logger.error("コンポーネントの初期化に失敗しました")
                return False
            
            logger.info("🚀 即座に週次レポート処理を実行します")
            
            print("\n" + "=" * 60)
            print("🚀 週次レポート処理（即座実行）")
            print("=" * 60)
            
            # 即座に実行
            success = self.scheduler.run_weekly_report(notify=notify)
            
            if success:
                print("✅ 週次レポート処理が完了しました")
                logger.info("週次レポート処理が正常に完了しました")
            else:
                print("❌ 週次レポート処理に失敗しました")
                logger.error("週次レポート処理に失敗しました")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"即座実行エラー: {e}")
            print(f"❌ エラー: {e}")
            return False
    
    def test_system(self):
        """システムテストを実行"""
        try:
            if not self.setup_components():
                logger.error("コンポーネントの初期化に失敗しました")
                return False
            
            logger.info("🧪 システムテストを実行します")
            
            print("\n" + "=" * 60)
            print("🧪 週次レポートシステムテスト")
            print("=" * 60)
            
            # コンポーネントテストを実行
            success = self.scheduler.test_components()
            
            if success:
                print("✅ 全コンポーネントテストが成功しました")
                logger.info("システムテストが正常に完了しました")
            else:
                print("❌ コンポーネントテストに失敗しました")
                logger.error("システムテストに失敗しました")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"システムテストエラー: {e}")
            print(f"❌ エラー: {e}")
            return False
    
    def show_chat_list(self):
        """利用可能なチャット一覧を表示"""
        try:
            if not self.setup_components():
                logger.error("コンポーネントの初期化に失敗しました")
                return False
            
            logger.info("📋 利用可能なチャット一覧を取得中...")
            
            print("\n" + "=" * 60)
            print("📋 利用可能なLarkチャット一覧")
            print("=" * 60)
            
            chats = self.lark_notifier.get_chat_list()
            
            if chats:
                print(f"📊 合計 {len(chats)} 件のチャットが見つかりました:")
                print()
                
                for i, chat in enumerate(chats[:20], 1):  # 最初の20件のみ表示
                    chat_name = chat.get('name', 'Unknown')
                    chat_id = chat.get('chat_id', 'Unknown')
                    print(f"{i:2d}. {chat_name}")
                    print(f"    ID: {chat_id}")
                    print()
                
                if len(chats) > 20:
                    print(f"    ... 他{len(chats) - 20}件")
                
                print("\n💡 使用方法:")
                print("   --chat-id <チャットID> で通知先を指定してください")
                print("   例: python weekly_report_system.py --mode schedule --chat-id oc_xxxxxxxxxx")
                
            else:
                print("❌ チャットが見つかりませんでした")
                print("   Larkアプリの権限設定を確認してください")
            
            return True
            
        except Exception as e:
            logger.error(f"チャット一覧取得エラー: {e}")
            print(f"❌ エラー: {e}")
            return False
    
    def test_webhook(self):
        """Webhook接続テスト"""
        try:
            if not self.setup_components():
                logger.error("コンポーネントの初期化に失敗しました")
                return False
            
            logger.info("🧪 Lark Webhook接続テストを実行します")
            
            print("\n" + "=" * 60)
            print("🧪 Lark Webhook接続テスト")
            print("=" * 60)
            
            # Webhookテストを実行
            success = self.lark_notifier.test_webhook()
            
            if success:
                print("✅ Lark Webhook接続テストが成功しました")
                logger.info("Lark Webhook接続テストが成功しました")
            else:
                print("❌ Lark Webhook接続テストが失敗しました")
                logger.error("Lark Webhook接続テストが失敗しました")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Webhookテストエラー: {e}")
            print(f"❌ エラー: {e}")
            return False
    
    def show_execution_history(self):
        """実行履歴を表示"""
        try:
            if not self.setup_components():
                logger.error("コンポーネントの初期化に失敗しました")
                return False
            
            # 実行履歴を読み込み
            self.scheduler.load_execution_history()
            
            print("\n" + "=" * 60)
            print("📊 実行履歴")
            print("=" * 60)
            
            if self.scheduler.execution_history:
                print(f"📈 合計 {len(self.scheduler.execution_history)} 回の実行記録:")
                print()
                
                for i, record in enumerate(self.scheduler.execution_history[-10:], 1):  # 最新10件
                    start_time = record.get('start_time', 'Unknown')
                    status = record.get('status', 'Unknown')
                    execution_time = record.get('execution_time_seconds', 0)
                    
                    print(f"{i:2d}. {start_time}")
                    print(f"    ステータス: {status}")
                    print(f"    実行時間: {execution_time:.1f}秒")
                    
                    if status == 'error':
                        error_msg = record.get('error_message', 'Unknown error')
                        print(f"    エラー: {error_msg}")
                    print()
                
                if len(self.scheduler.execution_history) > 10:
                    print(f"    ... 他{len(self.scheduler.execution_history) - 10}件")
            else:
                print("📝 実行履歴がありません")
                print("   システムを実行すると履歴が記録されます")
            
            return True
            
        except Exception as e:
            logger.error(f"実行履歴表示エラー: {e}")
            print(f"❌ エラー: {e}")
            return False

def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(
        description='週次レポートシステム',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # スケジューラーを開始（通知なし）
  python weekly_report_system.py --mode schedule
  
  # スケジューラーを開始（Lark通知あり）
  python weekly_report_system.py --mode schedule --chat-id oc_xxxxxxxxxx
  
  # 即座に実行
  python weekly_report_system.py --mode run --chat-id oc_xxxxxxxxxx
  
  # システムテスト
  python weekly_report_system.py --mode test
  
  # チャット一覧を表示
  python weekly_report_system.py --mode list-chats
  
  # 実行履歴を表示
  python weekly_report_system.py --mode history
        """
    )
    
    parser.add_argument('--mode', 
                       choices=['schedule', 'run', 'test', 'test-webhook', 'history'], 
                       default='schedule',
                       help='実行モード')
    
    parser.add_argument('--no-notify', 
                       action='store_true',
                       help='Lark通知を送信しない（コンソール表示のみ）')
    
    args = parser.parse_args()
    
    try:
        system = WeeklyReportSystem()
        
        if args.mode == 'schedule':
            success = system.run_scheduler()
            
        elif args.mode == 'run':
            success = system.run_immediately(notify=not args.no_notify)
            
        elif args.mode == 'test':
            success = system.test_system()
            
        elif args.mode == 'test-webhook':
            success = system.test_webhook()
            
        elif args.mode == 'history':
            success = system.show_execution_history()
        
        if not success:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n👋 システムを停止します")
    except Exception as e:
        print(f"❌ システムエラー: {e}")
        logger.error(f"メイン実行エラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
