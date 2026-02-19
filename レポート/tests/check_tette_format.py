#!/usr/bin/env python3
"""
TETTEの注文番号とサービスの形式を確認するスクリプト
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
        # TETTEの注文番号の実際の形式を確認
        result = uploader.service.spreadsheets().values().get(
            spreadsheetId=uploader.spreadsheet_id,
            range='db!A2:A10000'
        ).execute()
        
        orders = result.get('values', [])
        tette_orders = [order[0] for order in orders if order and '#T' in order[0]]
        
        print('TETTEの注文番号例:')
        for i, order in enumerate(tette_orders[:10]):
            print(f'{i+1}: {order}')
            
        # サービス列（P列）のTETTEデータも確認
        service_result = uploader.service.spreadsheets().values().get(
            spreadsheetId=uploader.spreadsheet_id,
            range='db!P2:P10000'
        ).execute()
        
        services = service_result.get('values', [])
        tette_services = [service[0] for service in services if service and 'TETTE' in str(service[0])]
        
        print(f'\nTETTEのサービス数: {len(tette_services)}')
        if tette_services:
            print('TETTEのサービス例:')
            for i, service in enumerate(tette_services[:5]):
                print(f'{i+1}: {service}')
        
        # 全ストアの注文番号パターンを確認
        print('\n全ストアの注文番号パターン:')
        all_orders = [order[0] for order in orders if order and order[0]]
        patterns = {}
        for order in all_orders[:100]:  # 最初の100件を確認
            if '#' in order:
                prefix = order.split('#')[1][0] if len(order.split('#')[1]) > 0 else ''
                if prefix not in patterns:
                    patterns[prefix] = []
                patterns[prefix].append(order)
        
        for prefix, order_list in patterns.items():
            print(f'#{prefix}: {len(order_list)}件 (例: {order_list[0]})')
                
    except Exception as e:
        logger.error(f'エラー: {e}')

if __name__ == "__main__":
    main()

