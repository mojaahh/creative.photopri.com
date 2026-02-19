#!/usr/bin/env python3
"""
修正された日時形式のテストスクリプト
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
    
    print('修正された日時フォーマットテスト:')
    for i, dt_str in enumerate(test_datetimes):
        formatted = exporter.format_datetime(dt_str)
        print(f'{i+1}: "{dt_str}" -> "{formatted}"')
        
    print('\n期待される結果:')
    print('1: "2024-08-06T13:08:54Z" -> "2024-08-06 13:08:54"')
    print('2: "2024-08-07T14:30:00Z" -> "2024-08-07 14:30:00"')
    print('3: "2024-08-08T15:45:30Z" -> "2024-08-08 15:45:30"')
    print('4: "" -> ""')
    print('5: "None" -> ""')

if __name__ == "__main__":
    main()

