#!/usr/bin/env python3
"""
代替の書式設定テストスクリプト
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from core.spreadsheet_uploader import SpreadsheetUploader
import logging

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_alternative_formatting():
    """代替の書式設定テスト"""
    try:
        # スプレッドシートアップローダーを初期化
        uploader = SpreadsheetUploader()
        
        # シートIDを取得
        sheet_id = uploader._get_sheet_id("db")
        if sheet_id is None:
            print("❌ シートIDが取得できません")
            return
        
        print(f"📊 シートID: {sheet_id}")
        
        # 代替の書式設定方法を試行
        requests = []
        
        # I列（9列目）を通貨形式に設定
        requests.append({
            'updateCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 38943,  # 38944行目（0ベース）
                    'endRowIndex': 38947,    # 38947行目（0ベース）
                    'startColumnIndex': 8,   # I列（0ベース）
                    'endColumnIndex': 9
                },
                'rows': [
                    {
                        'values': [
                            {
                                'userEnteredFormat': {
                                    'numberFormat': {
                                        'type': 'CURRENCY',
                                        'pattern': '¥#,##0'
                                    }
                                }
                            }
                        ]
                    }
                ],
                'fields': 'userEnteredFormat.numberFormat'
            }
        })
        
        print(f"🧪 代替書式設定テスト開始: リクエスト数={len(requests)}")
        print(f"リクエスト内容: {requests}")
        
        # API呼び出し
        body = {'requests': requests}
        result = uploader.service.spreadsheets().batchUpdate(
            spreadsheetId=uploader.spreadsheet_id,
            body=body
        ).execute()
        
        print(f"APIレスポンス: {result}")
        print("✅ 代替書式設定テスト完了")
        
    except Exception as e:
        logger.error(f"代替書式設定テストエラー: {e}")
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    test_alternative_formatting()
