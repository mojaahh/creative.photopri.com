#!/usr/bin/env python3
"""
P列のデータをテストするスクリプト
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
        # 小さな範囲でP列のデータを確認
        result = uploader.service.spreadsheets().values().get(
            spreadsheetId=uploader.spreadsheet_id,
            range='db!P2:P5'
        ).execute()
        
        values = result.get('values', [])
        print('P列のデータ（行2-5）:')
        for i, value in enumerate(values):
            cell_value = value[0] if value else "空"
            print(f'行{i+2}: "{cell_value}"')
            if cell_value and len(cell_value) > 0:
                print(f'  先頭文字: "{cell_value[0]}" (ASCII: {ord(cell_value[0])})')
                print(f'  文字数: {len(cell_value)}')
                
    except Exception as e:
        logger.error(f'エラー: {e}')

if __name__ == "__main__":
    main()

