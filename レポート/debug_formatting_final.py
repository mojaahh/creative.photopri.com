#!/usr/bin/env python3
"""
書式設定の最終デバッグスクリプト
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from core.spreadsheet_uploader import SpreadsheetUploader
import logging

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_formatting_final():
    """書式設定の最終デバッグ"""
    try:
        # スプレッドシートアップローダーを初期化
        uploader = SpreadsheetUploader()
        
        # 最新の新規追加行を確認
        range_name = "db!I38947:L38951"  # 最新の5行
        result = uploader.service.spreadsheets().values().get(
            spreadsheetId=uploader.spreadsheet_id,
            range=range_name
        ).execute()
        
        print(f"📊 最新の5行の値: {result.get('values', [])}")
        
        # セルの書式情報を詳細に取得
        range_name_format = "db!I38947:L38951"
        result_format = uploader.service.spreadsheets().get(
            spreadsheetId=uploader.spreadsheet_id,
            ranges=[range_name_format],
            includeGridData=True
        ).execute()
        
        print(f"📊 詳細な書式情報:")
        if 'sheets' in result_format and len(result_format['sheets']) > 0:
            sheet = result_format['sheets'][0]
            if 'data' in sheet and len(sheet['data']) > 0:
                for row_idx, row in enumerate(sheet['data']):
                    if 'values' in row:
                        for col_idx, cell in enumerate(row['values']):
                            col_letter = chr(73 + col_idx)  # I, J, K, L
                            row_num = 38947 + row_idx
                            
                            if 'userEnteredFormat' in cell:
                                format_info = cell['userEnteredFormat']
                                if 'numberFormat' in format_info:
                                    print(f"  {col_letter}{row_num}: {format_info['numberFormat']}")
                                else:
                                    print(f"  {col_letter}{row_num}: 書式なし")
                            else:
                                print(f"  {col_letter}{row_num}: 書式情報なし")
        
        print("✅ 書式デバッグ完了")
        
    except Exception as e:
        logger.error(f"書式デバッグエラー: {e}")
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    debug_formatting_final()

