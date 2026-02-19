#!/usr/bin/env python3
"""
TETTEストアのみの注文データをエクスポートするスクリプト
"""

import os
import sys
import logging
from pathlib import Path

# プロジェクトルートのパスを追加
sys.path.append(str(Path(__file__).parent.parent))

from order_export import ShopifyOrderExporter
from spreadsheet_uploader import SpreadsheetUploader

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('export_tette_only.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """TETTEストアのみのデータをエクスポート"""
    try:
        logger.info("🚀 TETTEストアの注文データエクスポートを開始します")
        
        # 1. コンポーネントを初期化
        logger.info("📊 コンポーネントを初期化中...")
        exporter = ShopifyOrderExporter()
        
        # 2. TETTEストアのみのデータをエクスポート
        logger.info("📊 TETTEストアの注文データをエクスポート中...")
        
        # TETTEストアのみを対象とするため、ストア設定を一時的に変更
        original_stores = exporter.active_stores.copy()
        exporter.active_stores = {k: v for k, v in original_stores.items() if k == 'tette'}
        
        if not exporter.active_stores:
            logger.error("❌ TETTEストアの設定が見つかりません")
            return False
        
        logger.info(f"📊 対象ストア: {list(exporter.active_stores.keys())}")
        
        # 全期間分のデータをエクスポート
        filename, is_initial = exporter.export_orders(force_initial=True)
        
        # ストア設定を元に戻す
        exporter.active_stores = original_stores
        
        if not filename:
            logger.error("❌ TETTEストアの注文データエクスポートに失敗しました")
            return False
        
        logger.info(f"✅ TETTEストアの注文データエクスポートが完了しました: {filename}")
        
        # 3. スプレッドシートにアップロード（既存データに追加）
        logger.info("📤 スプレッドシートにアップロード中...")
        uploader = SpreadsheetUploader()
        
        # CSVファイルを読み込んでアップロード
        csv_filepath = f"exports/{filename}"
        if os.path.exists(csv_filepath):
            import csv
            with open(csv_filepath, 'r', encoding='utf-8-sig') as csvfile:
                reader = csv.reader(csvfile)
                data = list(reader)
            
            if len(data) > 1:  # ヘッダー + データ行がある場合
                # ヘッダーを除くデータのみをアップロード
                data_without_header = data[1:]
                
                if uploader.append_data_to_sheet("db", data_without_header):
                    logger.info(f"✅ TETTEストアのデータをスプレッドシートに追加しました ({len(data_without_header)}行)")
                else:
                    logger.error("❌ スプレッドシートへのアップロードに失敗しました")
                    return False
            else:
                logger.warning("⚠️ TETTEストアのデータがありません")
        else:
            logger.error(f"❌ CSVファイルが見つかりません: {csv_filepath}")
            return False
        
        logger.info("🎉 TETTEストアのデータ取得とアップロードが完了しました！")
        return True
        
    except Exception as e:
        logger.error(f"❌ エラーが発生しました: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 TETTEストアの処理が正常に完了しました！")
    else:
        print("\n❌ TETTEストアの処理に失敗しました")
        sys.exit(1)
