#!/usr/bin/env python3
"""
日時データのアップロードテストスクリプト（既存シート使用）
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
        # テスト用のデータを作成
        test_data = [
            ['Name', 'Created at', 'Service'],
            ['#T1001', '2024/08/06 13:08:54', 'TETTE'],
            ['#P1001', '2024/08/07 14:30:00', 'Photopri'],
            ['#E1001', '2024/08/08 15:45:30', 'E1 Print']
        ]
        
        # 既存のtestシートにアップロード
        if uploader.upload_data_to_sheet('test', test_data):
            logger.info("✅ テストデータのアップロードが完了しました")
            
            # アップロードしたデータを確認
            result = uploader.service.spreadsheets().values().get(
                spreadsheetId=uploader.spreadsheet_id,
                range='test!A1:C4'
            ).execute()
            
            values = result.get('values', [])
            print('アップロードされたデータ:')
            for i, row in enumerate(values):
                print(f'行{i+1}: {row}')
                
        else:
            logger.error("❌ テストデータのアップロードに失敗しました")
            
    except Exception as e:
        logger.error(f'エラー: {e}')

if __name__ == "__main__":
    main()

