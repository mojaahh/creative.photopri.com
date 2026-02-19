#!/usr/bin/env python3
"""
並列処理75件テスト用スクリプト
1期間のみ並列処理で75件ずつ取得し、連番チェックを行う
"""

import sys
import os
from pathlib import Path

# パスを追加
sys.path.append(str(Path(__file__).parent))

from core.order_export import ShopifyOrderExporter
import logging

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_parallel_75.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_sequential_orders(orders, store_name):
    """注文番号の連番チェック"""
    if not orders:
        logger.info(f"{store_name}: 注文データがありません")
        return
    
    # 注文番号を抽出してソート
    order_names = []
    for order in orders:
        if order.get('name'):
            order_names.append(order['name'])
    
    if not order_names:
        logger.info(f"{store_name}: 有効な注文番号がありません")
        return
    
    order_names.sort()
    logger.info(f"{store_name}: {len(order_names)}件の注文を取得")
    logger.info(f"{store_name}: 最初の注文番号: {order_names[0]}")
    logger.info(f"{store_name}: 最後の注文番号: {order_names[-1]}")
    
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
        logger.warning(f"{store_name}: 連番でない注文番号を発見:")
        for missing in missing_orders[:10]:  # 最初の10件のみ表示
            logger.warning(f"  {missing}")
        if len(missing_orders) > 10:
            logger.warning(f"  ... 他{len(missing_orders) - 10}件")
    else:
        logger.info(f"{store_name}: 連番チェック完了 - 問題なし")

def main():
    """メイン処理"""
    try:
        # エクスポーター初期化
        exporter = ShopifyOrderExporter()
        
        # テスト用期間（最近の3ヶ月）
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        logger.info(f"テスト期間: {start_date_str} 〜 {end_date_str}")
        logger.info("75件ずつ並列処理で取得開始...")
        
        # 1期間のみ並列処理で取得
        orders = exporter.test_single_period_parallel(start_date_str, end_date_str)
        
        if not orders:
            logger.warning("取得した注文データがありません")
            return
        
        logger.info(f"合計: {len(orders)}件の注文を取得")
        
        # ストア別に分類
        store_orders = {}
        for order in orders:
            store_key = order.get('_store_key', 'unknown')
            if store_key not in store_orders:
                store_orders[store_key] = []
            store_orders[store_key].append(order)
        
        # 各ストアの連番チェック
        for store_key, store_order_list in store_orders.items():
            store_name = store_order_list[0].get('_store_name', store_key)
            check_sequential_orders(store_order_list, store_name)
        
        # CSVデータに変換して保存
        csv_data = exporter.convert_to_csv_data(orders)
        if csv_data:
            output_file = f"test_parallel_75_{start_date_str}_to_{end_date_str}.csv"
            with open(output_file, 'w', encoding='utf-8') as f:
                if isinstance(csv_data, list):
                    f.write('\n'.join(csv_data))
                else:
                    f.write(csv_data)
            logger.info(f"CSVファイルを保存: {output_file}")
        
        logger.info("テスト完了")
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        raise

if __name__ == "__main__":
    main()
