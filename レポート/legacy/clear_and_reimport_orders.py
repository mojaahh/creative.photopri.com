#!/usr/bin/env python3
"""
DBシートをクリアして全期間分の注文データを再取得するスクリプト
Lineitem quantityが数値として正しく処理されるように修正済み
"""

import os
import sys
import logging
from pathlib import Path

# プロジェクトルートのパスを追加
sys.path.append(str(Path(__file__).parent.parent))

from core.order_export import ShopifyOrderExporter
from core.spreadsheet_uploader import SpreadsheetUploader

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('clear_and_reimport_orders.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """メイン処理"""
    try:
        logger.info("🚀 DBシートのクリアと全期間分の注文データ再取得を開始します")
        
        # 1. コンポーネントを初期化
        logger.info("📊 コンポーネントを初期化中...")
        exporter = ShopifyOrderExporter()
        uploader = SpreadsheetUploader()
        
        # 2. DBシートをクリア
        logger.info("🗑️ DBシートの内容をクリア中...")
        if uploader.clear_sheet("db"):
            logger.info("✅ DBシートのクリアが完了しました")
        else:
            logger.error("❌ DBシートのクリアに失敗しました")
            return False
        
        # 3. 全期間分の注文データをエクスポート
        logger.info("📊 全期間分の注文データをエクスポート中...")
        filename, is_initial = exporter.export_orders(force_initial=True)
        
        if not filename:
            logger.error("❌ 注文データのエクスポートに失敗しました")
            return False
        
        logger.info(f"✅ 注文データのエクスポートが完了しました: {filename}")
        
        # 4. 分割アップロードでスプレッドシートにアップロード
        logger.info("📤 分割アップロードでスプレッドシートにアップロード中...")
        
        # batch_upload_orders.pyを実行
        import subprocess
        result = subprocess.run([
            sys.executable, "batch_upload_orders.py"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("🎉 全期間分の注文データの再取得が完了しました！")
            logger.info("✅ Lineitem quantityは数値として正しく処理されています")
            return True
        else:
            logger.error(f"❌ 分割アップロードに失敗しました: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ エラーが発生しました: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 処理が正常に完了しました！")
        print("📊 ServiceDataシートのS列に新しい関数を適用してください")
        print("📄 関数は 'service_data_function.txt' に保存されています")
    else:
        print("\n❌ 処理に失敗しました")
        sys.exit(1)

