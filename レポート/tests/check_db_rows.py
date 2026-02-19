#!/usr/bin/env python3
"""DBシートの行数を確認するスクリプト"""

from spreadsheet_uploader import SpreadsheetUploader

def main():
    try:
        uploader = SpreadsheetUploader()
        result = uploader.service.spreadsheets().values().get(
            spreadsheetId=uploader.spreadsheet_id,
            range='db!A:A'
        ).execute()
        
        row_count = len(result.get('values', []))
        print(f"現在のDBシートの行数: {row_count}")
        
        if row_count > 1:
            print("データが存在します。全期間分を再取得する場合は、一度クリアしてから実行してください。")
        else:
            print("データが空です。全期間分のデータを取得できます。")
            
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    main()

