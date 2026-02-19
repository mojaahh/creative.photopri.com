#!/usr/bin/env python3
"""
手動で書式設定を強制適用するスクリプト
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from core.spreadsheet_uploader import SpreadsheetUploader
import logging

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def force_formatting():
    """手動で書式設定を強制適用"""
    try:
        # スプレッドシートアップローダーを初期化
        uploader = SpreadsheetUploader()
        
        # シートIDを取得
        sheet_id = uploader._get_sheet_id("db")
        if sheet_id is None:
            print("❌ シートIDが取得できません")
            return
        
        print(f"📊 シートID: {sheet_id}")
        
        # 強制書式設定を実行
        requests = []
        
        # 通貨書式を適用する列（I, J, K, L, S, AX, BH, BJ）
        currency_columns = [8, 9, 10, 11, 18, 49, 59, 61]  # 0ベースのインデックス
        
        for col_index in currency_columns:
            col_letter = chr(65 + col_index) if col_index < 26 else f"{chr(65 + col_index // 26 - 1)}{chr(65 + col_index % 26)}"
            print(f"🎨 {col_letter}列({col_index+1})を通貨形式に設定中...")
            
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 38946,  # 38947行目（0ベース）
                        'endRowIndex': 38952,    # 38952行目（0ベース）
                        'startColumnIndex': col_index,
                        'endColumnIndex': col_index + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'CURRENCY',
                                'pattern': '¥#,##0'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })
        
        # Q列（Lineitem quantity）を数値形式に設定
        print(f"🎨 Q列(17)を数値形式に設定中...")
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 38946,  # 38947行目（0ベース）
                    'endRowIndex': 38952,    # 38952行目（0ベース）
                    'startColumnIndex': 16,  # Q列（0ベース）
                    'endColumnIndex': 17
                },
                'cell': {
                    'userEnteredFormat': {
                        'numberFormat': {
                            'type': 'NUMBER',
                            'pattern': '0'
                        }
                    }
                },
                'fields': 'userEnteredFormat.numberFormat'
            }
        })
        
        print(f"🧪 強制書式設定開始: リクエスト数={len(requests)}")
        
        # API呼び出し
        body = {'requests': requests}
        result = uploader.service.spreadsheets().batchUpdate(
            spreadsheetId=uploader.spreadsheet_id,
            body=body
        ).execute()
        
        print(f"APIレスポンス: {result}")
        print("✅ 強制書式設定完了")
        
    except Exception as e:
        logger.error(f"強制書式設定エラー: {e}")
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    force_formatting()

