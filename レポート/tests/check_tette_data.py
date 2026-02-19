#!/usr/bin/env python3
"""
TETTEのデータが正しく登録されているか確認するスクリプト
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
        # DBシートのデータを確認
        result = uploader.service.spreadsheets().values().get(
            spreadsheetId=uploader.spreadsheet_id,
            range='db!A2:A10000'  # 最初の10000行の注文番号を確認
        ).execute()
        
        orders = result.get('values', [])
        tette_orders = [order[0] for order in orders if order and '#T' in order[0]]
        
        print(f'TETTEの注文数: {len(tette_orders)}')
        if tette_orders:
            print(f'TETTEの注文例: {tette_orders[:5]}')
        
        # サービス列（P列）のデータも確認
        service_result = uploader.service.spreadsheets().values().get(
            spreadsheetId=uploader.spreadsheet_id,
            range='db!P2:P10000'
        ).execute()
        
        services = service_result.get('values', [])
        tette_services = [service[0] for service in services if service and 'TETTE' in str(service[0])]
        
        print(f'TETTEのサービス数: {len(tette_services)}')
        if tette_services:
            print(f'TETTEのサービス例: {tette_services[:5]}')
            
        # 全サービスの種類を確認
        all_services = [service[0] for service in services if service and service[0]]
        unique_services = list(set(all_services))
        print(f'全サービスの種類: {unique_services}')
            
    except Exception as e:
        print(f'エラー: {e}')

if __name__ == "__main__":
    main()

