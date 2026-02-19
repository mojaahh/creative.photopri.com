#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
注文データエクスポートスケジューラー
定期的に注文データをエクスポートしてスプレッドシートに反映します
"""

import os
import sys
import time
import logging
import schedule
from datetime import datetime, timedelta
from legacy.run_order_export_pipeline import OrderExportPipeline

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('レポート/scheduler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OrderExportScheduler:
    """注文データエクスポートスケジューラー"""
    
    def __init__(self):
        """初期化"""
        self.pipeline = None
        self.is_running = False
    
    def setup_pipeline(self):
        """パイプラインを初期化"""
        try:
            self.pipeline = OrderExportPipeline()
            if self.pipeline.setup_components():
                logger.info("✅ パイプラインの初期化が完了しました")
                return True
            else:
                logger.error("❌ パイプラインの初期化に失敗しました")
                return False
        except Exception as e:
            logger.error(f"パイプライン初期化エラー: {e}")
            return False
    
    def run_monthly_export(self):
        """月次エクスポートを実行"""
        try:
            logger.info("🕐 月次エクスポートを開始します")
            
            if not self.pipeline:
                if not self.setup_pipeline():
                    logger.error("パイプラインの初期化に失敗しました")
                    return
            
            # 前月のデータをエクスポート
            success = self.pipeline.run_pipeline(months_ago=1)
            
            if success:
                logger.info("✅ 月次エクスポートが完了しました")
            else:
                logger.error("❌ 月次エクスポートに失敗しました")
                
        except Exception as e:
            logger.error(f"月次エクスポートエラー: {e}")
    
    def run_weekly_export(self):
        """週次エクスポートを実行"""
        try:
            logger.info("🕐 週次エクスポートを開始します")
            
            if not self.pipeline:
                if not self.setup_pipeline():
                    logger.error("パイプラインの初期化に失敗しました")
                    return
            
            # 先週のデータをエクスポート（約1週間前）
            success = self.pipeline.run_pipeline(months_ago=0, sheet_name=f"週次注文データ_{datetime.now().strftime('%Y%m%d')}")
            
            if success:
                logger.info("✅ 週次エクスポートが完了しました")
            else:
                logger.error("❌ 週次エクスポートに失敗しました")
                
        except Exception as e:
            logger.error(f"週次エクスポートエラー: {e}")
    
    def run_daily_export(self):
        """日次エクスポートを実行"""
        try:
            logger.info("🕐 日次エクスポートを開始します")
            
            if not self.pipeline:
                if not self.setup_pipeline():
                    logger.error("パイプラインの初期化に失敗しました")
                    return
            
            # 昨日のデータをエクスポート
            yesterday = datetime.now() - timedelta(days=1)
            sheet_name = f"日次注文データ_{yesterday.strftime('%Y%m%d')}"
            
            success = self.pipeline.run_pipeline(months_ago=0, sheet_name=sheet_name)
            
            if success:
                logger.info("✅ 日次エクスポートが完了しました")
            else:
                logger.error("❌ 日次エクスポートに失敗しました")
                
        except Exception as e:
            logger.error(f"日次エクスポートエラー: {e}")
    
    def setup_schedule(self):
        """スケジュールを設定"""
        try:
            # 月次実行（毎月1日の午前9時）
            schedule.every().month.at("09:00").do(self.run_monthly_export)
            logger.info("📅 月次エクスポートスケジュールを設定: 毎月1日 09:00")
            
            # 週次実行（毎週月曜日の午前8時）
            schedule.every().monday.at("08:00").do(self.run_weekly_export)
            logger.info("📅 週次エクスポートスケジュールを設定: 毎週月曜日 08:00")
            
            # 日次実行（毎日午前7時）
            schedule.every().day.at("07:00").do(self.run_daily_export)
            logger.info("📅 日次エクスポートスケジュールを設定: 毎日 07:00")
            
            # 即座に1回実行（テスト用）
            schedule.every(1).minutes.do(self.run_monthly_export)
            logger.info("📅 テスト実行スケジュールを設定: 1分後に実行")
            
        except Exception as e:
            logger.error(f"スケジュール設定エラー: {e}")
    
    def start_scheduler(self):
        """スケジューラーを開始"""
        try:
            logger.info("🚀 注文データエクスポートスケジューラーを開始します")
            
            # スケジュールを設定
            self.setup_schedule()
            
            self.is_running = True
            
            while self.is_running:
                try:
                    # 待機中のジョブを実行
                    schedule.run_pending()
                    
                    # 1分待機
                    time.sleep(60)
                    
                except KeyboardInterrupt:
                    logger.info("⚠️ ユーザーによってスケジューラーが停止されました")
                    break
                except Exception as e:
                    logger.error(f"スケジューラー実行エラー: {e}")
                    time.sleep(60)  # エラーが発生した場合は1分待機
            
            logger.info("🛑 スケジューラーを停止しました")
            
        except Exception as e:
            logger.error(f"スケジューラー開始エラー: {e}")
    
    def stop_scheduler(self):
        """スケジューラーを停止"""
        self.is_running = False
        logger.info("スケジューラーの停止を要求しました")

def main():
    """メイン実行関数"""
    try:
        print("🚀 注文データエクスポートスケジューラーを開始します")
        print("📅 スケジュール:")
        print("   - 毎月1日 09:00: 月次エクスポート")
        print("   - 毎週月曜日 08:00: 週次エクスポート")
        print("   - 毎日 07:00: 日次エクスポート")
        print("   - 1分後: テスト実行")
        print("\n⏹️  停止するには Ctrl+C を押してください")
        
        # スケジューラーを開始
        scheduler = OrderExportScheduler()
        scheduler.start_scheduler()
        
    except KeyboardInterrupt:
        print("\n⚠️ ユーザーによって処理が中断されました")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 予期しないエラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

