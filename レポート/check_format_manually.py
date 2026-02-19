#!/usr/bin/env python3
"""
手動で書式設定を確認するスクリプト
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from core.spreadsheet_uploader import SpreadsheetUploader
import logging

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_manual_formatting():
    """手動で書式設定を確認"""
    try:
        # スプレッドシートアップローダーを初期化
        uploader = SpreadsheetUploader()
        
        # 特定のセルの値を取得して確認
        range_name = "db!I38944:L38946"  # I列からL列の38944-38946行
        result = uploader.service.spreadsheets().values().get(
            spreadsheetId=uploader.spreadsheet_id,
            range=range_name
        ).execute()
        
        print(f"📊 セルの値: {result.get('values', [])}")
        
        # セルの書式を取得
        range_name_format = "db!I38944:L38946"
        result_format = uploader.service.spreadsheets().get(
            spreadsheetId=uploader.spreadsheet_id,
            ranges=[range_name_format],
            includeGridData=True
        ).execute()
        
        print(f"📊 セルの書式情報:")
        if 'sheets' in result_format and len(result_format['sheets']) > 0:
            sheet = result_format['sheets'][0]
            if 'data' in sheet and len(sheet['data']) > 0:
                for row_idx, row in enumerate(sheet['data']):
                    if 'values' in row:
                        for col_idx, cell in enumerate(row['values']):
                            if 'userEnteredFormat' in cell:
                                format_info = cell['userEnteredFormat']
                                if 'numberFormat' in format_info:
                                    print(f"  行{38944+row_idx}, 列{chr(73+col_idx)}: {format_info['numberFormat']}")
                                else:
                                    print(f"  行{38944+row_idx}, 列{chr(73+col_idx)}: 書式なし")
                            else:
                                print(f"  行{38944+row_idx}, 列{chr(73+col_idx)}: 書式情報なし")
        
        print("✅ 書式確認完了")
        
    except Exception as e:
        logger.error(f"書式確認エラー: {e}")
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    check_manual_formatting()
