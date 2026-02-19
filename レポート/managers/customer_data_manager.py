#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
顧客データ管理スクリプト
- 顧客データの全件更新
- 購入回数などの最新情報を反映
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from core.customer_db_generator import CustomerDBGenerator
from core.spreadsheet_uploader import SpreadsheetUploader

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('customer_data_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CustomerDataManager:
    """顧客データ管理クラス"""
    
    def __init__(self):
        """初期化"""
        self.customer_generator = CustomerDBGenerator()
        self.spreadsheet_uploader = SpreadsheetUploader()
    
    def export_all_customers(self) -> str:
        """全顧客データをエクスポート"""
        try:
            logger.info("全顧客データをエクスポート開始")
            
            # 全期間の顧客データを取得
            all_customers = self.customer_generator.fetch_all_customers()
            
            if not all_customers:
                logger.warning("顧客データがありません")
                return ""
            
            # CSVファイルにエクスポート
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"customers_all_{timestamp}.csv"
            
            filepath = self.customer_generator.export_customers(filename)
            
            if filepath:
                logger.info(f"全顧客データエクスポート完了: {filename}")
                return filename
            else:
                logger.error("全顧客データのエクスポートに失敗しました")
                return ""
                
        except Exception as e:
            logger.error(f"全顧客データエクスポートエラー: {e}")
            raise
    
    def upload_customers_with_full_update(self, csv_filepath: str, sheet_name: str = "CustomerDB") -> bool:
        """顧客データを全件更新モードでアップロード"""
        try:
            logger.info(f"顧客データを全件更新モードでアップロード開始: {csv_filepath}")
            
            # CSVファイルを読み込み
            data = self.spreadsheet_uploader.read_csv_file(csv_filepath)
            
            if not data:
                logger.warning("アップロードするデータがありません")
                return False
            
            # シートが存在しない場合は作成
            try:
                self.spreadsheet_uploader.service.spreadsheets().get(
                    spreadsheetId=self.spreadsheet_uploader.spreadsheet_id,
                    ranges=f"{sheet_name}!A1"
                ).execute()
                logger.info(f"シート '{sheet_name}' は既に存在します")
            except:
                # シートが存在しない場合は作成
                if not self.spreadsheet_uploader.create_new_sheet(sheet_name):
                    return False
                logger.info(f"シート '{sheet_name}' を作成しました")
            
            # シートの内容をクリア（全件更新）
            self.spreadsheet_uploader.clear_sheet_content(sheet_name)
            
            # データをアップロード
            if not self.spreadsheet_uploader.upload_data_to_sheet(sheet_name, data):
                return False
            
            # シートのフォーマットを設定
            self.spreadsheet_uploader.format_sheet(sheet_name, len(data[0]))
            
            logger.info("顧客データの全件更新が完了しました")
            return True
            
        except Exception as e:
            logger.error(f"顧客データ全件更新エラー: {e}")
            return False
    
    def update_customer_metrics(self) -> bool:
        """顧客メトリクスを更新（購入回数、総購入額など）"""
        try:
            logger.info("顧客メトリクスの更新を開始")
            
            # 全顧客データをエクスポート
            filename = self.export_all_customers()
            
            if not filename:
                logger.warning("更新対象の顧客データがありません")
                return False
            
            # スプレッドシートに全件更新
            csv_filepath = os.path.join('exports', filename)
            success = self.upload_customers_with_full_update(csv_filepath)
            
            if success:
                logger.info("顧客メトリクスの更新が完了しました")
            else:
                logger.error("顧客メトリクスの更新に失敗しました")
            
            return success
            
        except Exception as e:
            logger.error(f"顧客メトリクス更新エラー: {e}")
            return False

def main():
    """メイン実行関数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='顧客データ管理スクリプト')
    parser.add_argument('action', choices=['export', 'update', 'full_update'], 
                       help='実行するアクション')
    parser.add_argument('--upload', action='store_true', 
                       help='エクスポート後にスプレッドシートにアップロード')
    
    args = parser.parse_args()
    
    try:
        manager = CustomerDataManager()
        
        if args.action == 'export':
            print("📊 全顧客データをエクスポートします...")
            filename = manager.export_all_customers()
            
            if filename:
                print(f"✅ エクスポート完了: {filename}")
                print(f"📁 ファイル保存場所: exports/{filename}")
                
                if args.upload:
                    print("📤 スプレッドシートにアップロードします...")
                    csv_filepath = os.path.join('exports', filename)
                    success = manager.upload_customers_with_full_update(csv_filepath)
                    
                    if success:
                        print("✅ スプレッドシートへのアップロードが完了しました")
                        print(f"🔗 スプレッドシートURL: {manager.spreadsheet_uploader.get_spreadsheet_url()}")
                    else:
                        print("❌ スプレッドシートへのアップロードに失敗しました")
            else:
                print("⚠️ エクスポート対象の顧客データがありませんでした")
        
        elif args.action == 'update':
            print("📊 顧客メトリクスを更新します...")
            success = manager.update_customer_metrics()
            
            if success:
                print("✅ 顧客メトリクスの更新が完了しました")
            else:
                print("❌ 顧客メトリクスの更新に失敗しました")
        
        elif args.action == 'full_update':
            print("📊 顧客データの全件更新を実行します...")
            success = manager.update_customer_metrics()
            
            if success:
                print("✅ 顧客データの全件更新が完了しました")
            else:
                print("❌ 顧客データの全件更新に失敗しました")
                
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")

if __name__ == "__main__":
    main()
