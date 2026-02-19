#!/usr/bin/env python3
"""
日時データのアップロードテストスクリプト（ServiceDataシート使用）
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
        # テスト用のデータを作成（ServiceDataシートの形式に合わせる）
        test_data = [
            ['年月', 'サービス', '売上', '注文件数', '注文点数'],
            ['2024/08/01', '#T', '1000', '1', '1'],
            ['2024/08/01', '#P', '2000', '2', '2'],
            ['2024/08/01', '#E', '3000', '3', '3']
        ]
        
        # ServiceDataシートにアップロード
        if uploader.upload_data_to_sheet('ServiceData', test_data):
            logger.info("✅ テストデータのアップロードが完了しました")
            
            # アップロードしたデータを確認
            result = uploader.service.spreadsheets().values().get(
                spreadsheetId=uploader.spreadsheet_id,
                range='ServiceData!A1:E5'
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

