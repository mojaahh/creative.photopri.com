#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
統合データ管理スクリプト
注文データと顧客データの統合管理
"""

import os
import sys
import logging
from datetime import datetime
from order_data_manager import OrderDataManager
from customer_data_manager import CustomerDataManager
from user_analysis_manager import UserAnalysisManager
from service_analysis_manager import ServiceAnalysisManager
from churn_alert_manager import ChurnAlertManager

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('integrated_data_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IntegratedDataManager:
    """統合データ管理クラス"""
    
    def __init__(self):
        """初期化"""
        self.order_manager = OrderDataManager()
        self.customer_manager = CustomerDataManager()
        self.user_analysis_manager = UserAnalysisManager()
        self.service_analysis_manager = ServiceAnalysisManager()
        self.churn_alert_manager = ChurnAlertManager()
    
    
    def weekly_update(self) -> bool:
        """週次更新処理（直近3ヶ月の注文データを上書き・追加）"""
        try:
            logger.info("週次更新処理を開始")
            overall_success = True
            
            # 直近3ヶ月の注文データをエクスポート・連番チェック・上書きアップロード
            print("📊 直近3ヶ月の注文データを取得・連番チェック・上書き更新します...")
            try:
                filename = self.order_manager.export_orders_with_sequential_check(mode="recent_3months")
                
                if filename:
                    csv_filepath = os.path.join('exports', filename)
                    success = self.order_manager.upload_orders_with_duplicate_handling(csv_filepath)
                    
                    if success:
                        print("✅ 直近3ヶ月の注文データの上書き・追加更新が完了しました")
                    else:
                        print("❌ 直近3ヶ月の注文データの上書き・追加更新に失敗しました")
                        overall_success = False
                else:
                    print("⚠️ 直近3ヶ月の注文データがありませんでした")
                    overall_success = False
            except Exception as e:
                print(f"❌ 注文データ更新でエラーが発生しました: {e}")
                overall_success = False
            
            # 顧客データの更新
            print("📊 顧客データを更新します...")
            try:
                customer_success = self.customer_manager.update_customer_metrics()
                
                if customer_success:
                    print("✅ 顧客データの更新が完了しました")
                else:
                    print("❌ 顧客データの更新に失敗しました")
                    overall_success = False
            except Exception as e:
                print(f"❌ 顧客データ更新でエラーが発生しました: {e}")
                overall_success = False
            
            # ユーザー分析の更新
            print("📊 上位100名ユーザー分析を更新します...")
            try:
                analysis_success = self.user_analysis_manager.create_analysis_sheet()
                
                if analysis_success:
                    print("✅ ユーザー分析の更新が完了しました")
                else:
                    print("❌ ユーザー分析の更新に失敗しました")
                    overall_success = False
            except Exception as e:
                print(f"❌ ユーザー分析更新でエラーが発生しました: {e}")
                overall_success = False
            
            # サービス別分析の更新
            print("📊 サービス別分析を更新します...")
            try:
                service_analysis_success = self.service_analysis_manager.create_all_service_analysis()
                
                if service_analysis_success:
                    print("✅ サービス別分析の更新が完了しました")
                else:
                    print("❌ サービス別分析の更新に失敗しました")
                    overall_success = False
            except Exception as e:
                print(f"❌ サービス別分析更新でエラーが発生しました: {e}")
                overall_success = False
            
            # 離脱者アラートの更新
            print("📊 離脱者アラートリストを更新します...")
            try:
                churn_alert_success = self.churn_alert_manager.create_churn_alert_list()
                
                if churn_alert_success:
                    print("✅ 離脱者アラートリストの更新が完了しました")
                else:
                    print("❌ 離脱者アラートリストの更新に失敗しました")
                    overall_success = False
            except Exception as e:
                print(f"❌ 離脱者アラート更新でエラーが発生しました: {e}")
                overall_success = False
            
            logger.info("週次更新処理が完了しました")
            return overall_success
            
        except Exception as e:
            logger.error(f"週次更新処理エラー: {e}")
            return False
    
    def full_import(self) -> bool:
        """全期間データの初回インポート（完全差し替え）"""
        try:
            logger.info("全期間データの初回インポートを開始")
            
            # 全期間の注文データをエクスポート・連番チェック・完全差し替え
            print("📊 全期間の注文データを取得・連番チェック・完全差し替えします...")
            filename = self.order_manager.export_orders_with_sequential_check(mode="all_time")
            
            if filename:
                csv_filepath = os.path.join('exports', filename)
                success = self.order_manager.upload_orders_with_replace(csv_filepath)
                
                if success:
                    print("✅ 全期間の注文データの完全差し替えが完了しました")
                else:
                    print("❌ 全期間の注文データの完全差し替えに失敗しました")
                    return False
            else:
                print("⚠️ 全期間の注文データがありませんでした")
            
            # 顧客データの全件更新
            print("📊 顧客データを全件更新します...")
            customer_success = self.customer_manager.update_customer_metrics()
            
            if customer_success:
                print("✅ 顧客データの全件更新が完了しました")
            else:
                print("❌ 顧客データの全件更新に失敗しました")
                return False
            
            # ユーザー分析の作成
            print("📊 上位100名ユーザー分析を作成します...")
            analysis_success = self.user_analysis_manager.create_analysis_sheet()
            
            if analysis_success:
                print("✅ ユーザー分析の作成が完了しました")
            else:
                print("❌ ユーザー分析の作成に失敗しました")
                return False
            
            # サービス別分析の作成
            print("📊 サービス別分析を作成します...")
            service_analysis_success = self.service_analysis_manager.create_all_service_analysis()
            
            if service_analysis_success:
                print("✅ サービス別分析の作成が完了しました")
            else:
                print("❌ サービス別分析の作成に失敗しました")
                return False
            
            # 離脱者アラートの作成
            print("📊 離脱者アラートリストを作成します...")
            churn_alert_success = self.churn_alert_manager.create_churn_alert_list()
            
            if churn_alert_success:
                print("✅ 離脱者アラートリストの作成が完了しました")
            else:
                print("❌ 離脱者アラートリストの作成に失敗しました")
                return False
            
            logger.info("全期間データの初回インポートが完了しました")
            return True
            
        except Exception as e:
            logger.error(f"全期間データインポートエラー: {e}")
            return False

def main():
    """メイン実行関数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='統合データ管理スクリプト')
    parser.add_argument('action', choices=['weekly', 'full_import', 'user_analysis', 'service_analysis', 'churn_alert'], 
                       help='実行するアクション')
    
    args = parser.parse_args()
    
    try:
        manager = IntegratedDataManager()
        
        if args.action == 'weekly':
            print("🔄 週次更新処理を実行します...")
            success = manager.weekly_update()
            
        elif args.action == 'full_import':
            print("🔄 全期間データの初回インポートを実行します...")
            success = manager.full_import()
            
        elif args.action == 'user_analysis':
            print("🔄 上位100名ユーザー分析を実行します...")
            success = manager.user_analysis_manager.create_analysis_sheet()
            
        elif args.action == 'service_analysis':
            print("🔄 サービス別分析を実行します...")
            success = manager.service_analysis_manager.create_all_service_analysis()
            
        elif args.action == 'churn_alert':
            print("🔄 離脱者アラートリストを作成します...")
            success = manager.churn_alert_manager.create_churn_alert_list()
        
        if success:
            print("✅ 処理が正常に完了しました")
        else:
            print("❌ 処理中にエラーが発生しました")
            sys.exit(1)
                
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
