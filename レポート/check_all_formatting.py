#!/usr/bin/env python3
"""
全列の書式設定を確認するスクリプト
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from core.spreadsheet_uploader import SpreadsheetUploader
import logging

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_all_formatting():
    """全列の書式設定を確認"""
    try:
        # スプレッドシートアップローダーを初期化
        uploader = SpreadsheetUploader()
        
        # 指定された列の値を確認
        columns_to_check = ['I', 'J', 'K', 'L', 'Q', 'S', 'AX', 'BH', 'BJ']
        
        print("📊 最新の5行（38947-38951）の各列の値を確認:")
        for col in columns_to_check:
            range_name = f"db!{col}38947:{col}38951"
            result = uploader.service.spreadsheets().values().get(
                spreadsheetId=uploader.spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            print(f"  {col}列: {values}")
        
        print("✅ 全列確認完了")
        
    except Exception as e:
        logger.error(f"全列確認エラー: {e}")
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    check_all_formatting()

