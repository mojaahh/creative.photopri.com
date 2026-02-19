#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
注文データ管理スクリプト
- 全期間データの取得
- 直近3ヶ月データの上書き更新
- 直近2ヶ月データの新規追加
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from core.order_export import ShopifyOrderExporter
from core.spreadsheet_uploader import SpreadsheetUploader

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('order_data_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OrderDataManager:
    """注文データ管理クラス"""
    
    def __init__(self):
        """初期化"""
        self.order_exporter = ShopifyOrderExporter()
        self.spreadsheet_uploader = SpreadsheetUploader()
    
    def export_all_time_orders(self) -> str:
        """全期間の注文データをエクスポート"""
        try:
            logger.info("全期間の注文データをエクスポート開始")
            filename, mode = self.order_exporter.export_orders(mode="all_time")
            
            if filename:
                logger.info(f"全期間データエクスポート完了: {filename}")
                return filename
            else:
                logger.warning("全期間データのエクスポート対象がありません")
                return ""
                
        except Exception as e:
            logger.error(f"全期間データエクスポートエラー: {e}")
            raise
    
    def export_recent_3months_orders(self) -> str:
        """直近3ヶ月の注文データをエクスポート（上書き用）"""
        try:
            logger.info("直近3ヶ月の注文データをエクスポート開始（上書き用）")
            filename, mode = self.order_exporter.export_orders(mode="recent_3months")
            
            if filename:
                logger.info(f"直近3ヶ月データエクスポート完了: {filename}")
                return filename
            else:
                logger.warning("直近3ヶ月データのエクスポート対象がありません")
                return ""
                
        except Exception as e:
            logger.error(f"直近3ヶ月データエクスポートエラー: {e}")
            raise
    
    def export_recent_2months_orders(self) -> str:
        """直近2ヶ月の注文データをエクスポート（新規追加用）"""
        try:
            logger.info("直近2ヶ月の注文データをエクスポート開始（新規追加用）")
            filename, mode = self.order_exporter.export_orders(mode="recent_2months")
            
            if filename:
                logger.info(f"直近2ヶ月データエクスポート完了: {filename}")
                return filename
            else:
                logger.warning("直近2ヶ月データのエクスポート対象がありません")
                return ""
                
        except Exception as e:
            logger.error(f"直近2ヶ月データエクスポートエラー: {e}")
            raise
    
    def upload_orders_with_overwrite(self, csv_filepath: str, sheet_name: str = "db") -> bool:
        """注文データを上書きモードでアップロード"""
        try:
            logger.info(f"注文データを上書きモードでアップロード開始: {csv_filepath}")
            
            # CSVファイルを読み込み
            data = self.spreadsheet_uploader.read_csv_file(csv_filepath)
            
            if not data:
                logger.warning("アップロードするデータがありません")
                return False
            
            # 上書きモードでアップロード
            success = self.spreadsheet_uploader.upload_with_duplicate_handling(
                sheet_name, data, overwrite_mode=True
            )
            
            if success:
                logger.info("注文データの上書きアップロードが完了しました")
            else:
                logger.error("注文データの上書きアップロードに失敗しました")
            
            return success
            
        except Exception as e:
            logger.error(f"注文データ上書きアップロードエラー: {e}")
            return False
    
    def upload_orders_with_append(self, csv_filepath: str, sheet_name: str = "db") -> bool:
        """注文データを追加モードでアップロード"""
        try:
            logger.info(f"注文データを追加モードでアップロード開始: {csv_filepath}")
            
            # CSVファイルを読み込み
            data = self.spreadsheet_uploader.read_csv_file(csv_filepath)
            
            if not data:
                logger.warning("アップロードするデータがありません")
                return False
            
            # 追加モードでアップロード
            success = self.spreadsheet_uploader.upload_with_duplicate_handling(
                sheet_name, data, overwrite_mode=False
            )
            
            if success:
                logger.info("注文データの追加アップロードが完了しました")
            else:
                logger.error("注文データの追加アップロードに失敗しました")
            
            return success
            
        except Exception as e:
            logger.error(f"注文データ追加アップロードエラー: {e}")
            return False
    
    def export_orders_with_sequential_check(self, mode: str) -> str:
        """注文データをエクスポートして連番チェックを実行"""
        try:
            logger.info(f"注文データをエクスポート・連番チェック開始: {mode}")
            
            # 注文データをエクスポート
            filename, _ = self.order_exporter.export_orders(mode=mode)
            
            if not filename:
                logger.warning(f"{mode}データのエクスポート対象がありません")
                return ""
            
            # 連番チェックを実行
            csv_filepath = os.path.join('exports', filename)
            self.check_sequential_orders(csv_filepath)
            
            logger.info(f"注文データエクスポート・連番チェック完了: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"注文データエクスポート・連番チェックエラー: {e}")
            raise
    
    def check_sequential_orders(self, csv_filepath: str):
        """CSVファイルの注文番号の連番チェック"""
        try:
            import pandas as pd
            
            # CSVファイルを読み込み
            df = pd.read_csv(csv_filepath)
            
            if 'Name' not in df.columns:
                logger.warning("注文番号列（Name）が見つかりません")
                return
            
            # ストア別に連番チェック
            for store in df['Store'].unique():
                store_df = df[df['Store'] == store]
                order_names = store_df['Name'].dropna().tolist()
                
                if not order_names:
                    continue
                
                # 注文番号をソート
                order_names.sort()
                
                logger.info(f"{store}: {len(order_names)}件の注文を取得")
                logger.info(f"{store}: 最初の注文番号: {order_names[0]}")
                logger.info(f"{store}: 最後の注文番号: {order_names[-1]}")
                
                # 連番チェック
                missing_orders = []
                for i in range(len(order_names) - 1):
                    current = order_names[i]
                    next_order = order_names[i + 1]
                    
                    # 注文番号の形式をチェック（#で始まる場合）
                    if current.startswith('#') and next_order.startswith('#'):
                        try:
                            current_num = int(current[1:])
                            next_num = int(next_order[1:])
                            
                            if next_num - current_num > 1:
                                missing_orders.append(f"{current} → {next_order} (間隔: {next_num - current_num})")
                        except ValueError:
                            # 数値でない場合はスキップ
                            continue
                
                if missing_orders:
                    logger.warning(f"{store}: 連番でない注文番号を発見:")
                    for missing in missing_orders[:10]:  # 最初の10件のみ表示
                        logger.warning(f"  {missing}")
                    if len(missing_orders) > 10:
                        logger.warning(f"  ... 他{len(missing_orders) - 10}件")
                else:
                    logger.info(f"{store}: 連番チェック完了 - 問題なし")
                    
        except Exception as e:
            logger.error(f"連番チェックエラー: {e}")
    
    def upload_orders_with_duplicate_handling(self, csv_filepath: str, sheet_name: str = "db") -> bool:
        """注文データを重複処理モードでアップロード（上書き・追加）"""
        try:
            logger.info(f"注文データを重複処理モードでアップロード開始: {csv_filepath}")
            
            # CSVファイルを読み込み
            data = self.spreadsheet_uploader.read_csv_file(csv_filepath)
            
            if not data:
                logger.warning("アップロードするデータがありません")
                return False
            
            # 重複処理モードでアップロード（上書き・追加）
            success = self.spreadsheet_uploader.upload_with_duplicate_handling(
                sheet_name, data, overwrite_mode=True
            )
            
            if success:
                logger.info("注文データの重複処理アップロードが完了しました")
            else:
                logger.error("注文データの重複処理アップロードに失敗しました")
            
            return success
            
        except Exception as e:
            logger.error(f"注文データ重複処理アップロードエラー: {e}")
            return False
    
    def upload_orders_with_replace(self, csv_filepath: str, sheet_name: str = "db") -> bool:
        """注文データを完全差し替えモードでアップロード"""
        try:
            logger.info(f"注文データを完全差し替えモードでアップロード開始: {csv_filepath}")
            
            # 完全差し替えモードでアップロード
            success = self.spreadsheet_uploader.upload_csv_to_spreadsheet(
                csv_filepath, sheet_name, overwrite_mode=True
            )
            
            if success:
                logger.info("注文データの完全差し替えアップロードが完了しました")
            else:
                logger.error("注文データの完全差し替えアップロードに失敗しました")
            
            return success
            
        except Exception as e:
            logger.error(f"注文データ完全差し替えアップロードエラー: {e}")
            return False

def main():
    """メイン実行関数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='注文データ管理スクリプト')
    parser.add_argument('action', choices=['all_time', 'recent_3months', 'recent_2months'], 
                       help='実行するアクション')
    parser.add_argument('--upload', action='store_true', 
                       help='エクスポート後にスプレッドシートにアップロード')
    parser.add_argument('--overwrite', action='store_true', 
                       help='アップロード時に上書きモードを使用（recent_3monthsのみ）')
    
    args = parser.parse_args()
    
    try:
        manager = OrderDataManager()
        
        if args.action == 'all_time':
            print("📊 全期間の注文データをエクスポートします...")
            filename = manager.export_all_time_orders()
            
        elif args.action == 'recent_3months':
            print("📊 直近3ヶ月の注文データをエクスポートします（上書き用）...")
            filename = manager.export_recent_3months_orders()
            
        elif args.action == 'recent_2months':
            print("📊 直近2ヶ月の注文データをエクスポートします（新規追加用）...")
            filename = manager.export_recent_2months_orders()
        
        if not filename:
            print("⚠️ エクスポート対象のデータがありませんでした")
            return
        
        print(f"✅ エクスポート完了: {filename}")
        print(f"📁 ファイル保存場所: exports/{filename}")
        
        # アップロードオプションが指定された場合
        if args.upload:
            csv_filepath = os.path.join('exports', filename)
            
            if args.action == 'recent_3months' and args.overwrite:
                print("📤 上書きモードでスプレッドシートにアップロードします...")
                success = manager.upload_orders_with_overwrite(csv_filepath)
            else:
                print("📤 追加モードでスプレッドシートにアップロードします...")
                success = manager.upload_orders_with_append(csv_filepath)
            
            if success:
                print("✅ スプレッドシートへのアップロードが完了しました")
                print(f"🔗 スプレッドシートURL: {manager.spreadsheet_uploader.get_spreadsheet_url()}")
            else:
                print("❌ スプレッドシートへのアップロードに失敗しました")
                
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")

if __name__ == "__main__":
    main()
