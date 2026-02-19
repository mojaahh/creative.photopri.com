#!/usr/bin/env python3
"""
P列のデータを確認するスクリプト
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
        # P列のデータを確認
        result = uploader.service.spreadsheets().values().get(
            spreadsheetId=uploader.spreadsheet_id,
            range='db!P2:P20'
        ).execute()
        
        values = result.get('values', [])
        print('P列のデータ（最初の20行）:')
        for i, value in enumerate(values):
            cell_value = value[0] if value else "空"
            print(f'行{i+2}: "{cell_value}"')
            # 先頭の文字を確認
            if cell_value and len(cell_value) > 0:
                print(f'  先頭文字: "{cell_value[0]}" (ASCII: {ord(cell_value[0])})')
                
    except Exception as e:
        logger.error(f'エラー: {e}')

if __name__ == "__main__":
    main()

