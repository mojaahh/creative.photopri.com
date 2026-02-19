#!/usr/bin/env python3
"""
日時データのアップロードテストスクリプト（dbシート使用）
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
        # テスト用のデータを作成（dbシートの形式に合わせる）
        test_data = [
            ['Name', 'Created at', 'Service'],
            ['#T1001', '2024/08/06 13:08:54', 'TETTE'],
            ['#P1001', '2024/08/07 14:30:00', 'Photopri'],
            ['#E1001', '2024/08/08 15:45:30', 'E1 Print']
        ]
        
        # dbシートの最後に追加
        if uploader.append_data_to_sheet('db', test_data[1:]):  # ヘッダーを除く
            logger.info("✅ テストデータの追加が完了しました")
            
            # 追加したデータを確認（最後の数行）
            result = uploader.service.spreadsheets().values().get(
                spreadsheetId=uploader.spreadsheet_id,
                range='db!A25040:A25050'  # 最後の10行程度を確認
            ).execute()
            
            values = result.get('values', [])
            print('追加されたデータ（最後の10行）:')
            for i, row in enumerate(values):
                print(f'行{25040+i}: {row[0] if row else "空"}')
                
        else:
            logger.error("❌ テストデータの追加に失敗しました")
            
    except Exception as e:
        logger.error(f'エラー: {e}')

if __name__ == "__main__":
    main()

