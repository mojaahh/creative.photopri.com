#!/usr/bin/env python3
"""
TETTEストアの注文データをデバッグするスクリプト
"""

import os
import sys
import logging
from pathlib import Path

# プロジェクトルートのパスを追加
sys.path.append(str(Path(__file__).parent))

from core.order_export import ShopifyOrderExporter

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug_tette.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def debug_tette_orders():
    """TETTEストアの注文データをデバッグ"""
    try:
        logger.info("🔍 TETTEストアの注文データをデバッグ中...")
        
        # エクスポーターを初期化
        exporter = ShopifyOrderExporter()
        
        # TETTEストアのみを対象とする
        original_stores = exporter.active_stores.copy()
        exporter.active_stores = {k: v for k, v in original_stores.items() if k == 'tette'}
        
        if not exporter.active_stores:
            logger.error("❌ TETTEストアの設定が見つかりません")
            return False
        
        logger.info(f"📊 対象ストア: {list(exporter.active_stores.keys())}")
        
        # 日付範囲を設定
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        logger.info(f"📅 対象期間: {start_date_str} 〜 {end_date_str}")
        
        # TETTEストアから注文データを取得
        tette_orders = exporter.fetch_orders_from_store('tette', start_date_str, end_date_str)
        logger.info(f"📊 TETTEストアから取得した注文数: {len(tette_orders)}")
        
        if tette_orders:
            # 最初の5件の注文を表示
            logger.info("📋 最初の5件の注文データ:")
            for i, order in enumerate(tette_orders[:5]):
                logger.info(f"  注文 {i+1}: {order.get('name', 'N/A')} - {order.get('createdAt', 'N/A')}")
            
            # 注文番号のパターンを確認
            order_names = [order.get('name', '') for order in tette_orders if order.get('name')]
            logger.info(f"📊 注文番号の例: {order_names[:10]}")
            
            # データ変換をテスト
            logger.info("🔄 データ変換をテスト中...")
            csv_data = exporter.convert_to_csv_data(tette_orders)
            logger.info(f"📊 変換後のCSVデータ行数: {len(csv_data)}")
            
            if csv_data:
                logger.info("📋 変換後の最初の5行:")
                for i, row in enumerate(csv_data[:5]):
                    logger.info(f"  行 {i+1}: {row[:5]}...")  # 最初の5列のみ表示
        else:
            logger.warning("⚠️ TETTEストアから注文データが取得できませんでした")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ エラーが発生しました: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = debug_tette_orders()
    if success:
        print("\n🎉 TETTEストアのデバッグが完了しました！")
    else:
        print("\n❌ TETTEストアのデバッグに失敗しました")
        sys.exit(1)
