#!/usr/bin/env python3
"""
最新のデータを確認するスクリプト
"""

import sys
from pathlib import Path
import logging

# プロジェクトルートのパスを追加
sys.path.append(str(Path(__file__).parent.parent))

from spreadsheet_uploader import SpreadsheetUploader

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
    uploader = SpreadsheetUploader()
    try:
        # 全行数を確認
        all_result = uploader.service.spreadsheets().values().get(
            spreadsheetId=uploader.spreadsheet_id,
            range='db!A:A'
        ).execute()
        
        all_orders = all_result.get('values', [])
        print(f'全行数: {len(all_orders)}')
        
        # 最後の100行を確認
        print('最後の100行:')
        for i, order in enumerate(all_orders[-100:]):
            row_num = len(all_orders) - 100 + i + 1
            order_text = order[0] if order else "空"
            print(f'{row_num}: {order_text}')
            
        # TETTEの注文を探す
        tette_orders = []
        for i, order in enumerate(all_orders):
            if order and order[0] and ('TETTE' in order[0] or '#T' in order[0]):
                tette_orders.append((i+1, order[0]))
        
        print(f'\nTETTEの注文数: {len(tette_orders)}')
        if tette_orders:
            print('TETTEの注文:')
            for row_num, order in tette_orders[:10]:
                print(f'行{row_num}: {order}')
                
    except Exception as e:
        logger.error(f'エラー: {e}')

if __name__ == "__main__":
    main()

