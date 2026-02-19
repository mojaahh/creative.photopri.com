#!/usr/bin/env python3
"""
利用可能なシートを確認するスクリプト
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
        # スプレッドシートの情報を取得
        spreadsheet = uploader.service.spreadsheets().get(
            spreadsheetId=uploader.spreadsheet_id
        ).execute()
        
        sheets = spreadsheet.get('sheets', [])
        print('利用可能なシート:')
        for i, sheet in enumerate(sheets):
            sheet_name = sheet['properties']['title']
            print(f'{i+1}: {sheet_name}')
            
    except Exception as e:
        logger.error(f'エラー: {e}')

if __name__ == "__main__":
    main()

