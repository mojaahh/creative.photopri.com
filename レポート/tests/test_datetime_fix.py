#!/usr/bin/env python3
"""
日時データの修正テストスクリプト
"""

import sys
from pathlib import Path
import logging
from datetime import datetime

# プロジェクトルートのパスを追加
sys.path.append(str(Path(__file__).parent.parent))

from order_export import ShopifyOrderExporter

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    exporter = ShopifyOrderExporter()
    
    # テスト用の日時データ
    test_datetimes = [
        '2024-08-06T13:08:54Z',
        '2024-08-07T14:30:00Z',
        '2024-08-08T15:45:30Z',
        '',  # 空文字列
        None  # None
    ]
    
    print('日時フォーマットテスト:')
    for i, dt_str in enumerate(test_datetimes):
        formatted = exporter.format_datetime(dt_str)
        print(f'{i+1}: "{dt_str}" -> "{formatted}"')
        
    # 実際のCSVファイルの日時データを確認
    print('\n実際のCSVファイルの日時データ確認:')
    try:
        with open('exports/orders_initial_2019-01-01_to_2025-09-03_20250903_140227.csv', 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[:5]):
                parts = line.strip().split(',')
                if len(parts) > 17:  # Created at列（18列目）
                    created_at = parts[17]
                    print(f'行{i+1}: "{created_at}"')
    except Exception as e:
        logger.error(f'CSVファイル読み込みエラー: {e}')

if __name__ == "__main__":
    main()

