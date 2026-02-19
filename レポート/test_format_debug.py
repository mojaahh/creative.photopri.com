#!/usr/bin/env python3
"""
書式設定のデバッグ用テストスクリプト
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from core.spreadsheet_uploader import SpreadsheetUploader
import logging

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_formatting():
    """書式設定のテスト"""
    try:
        # スプレッドシートアップローダーを初期化
        uploader = SpreadsheetUploader()
        
        # テスト用の書式設定を実行
        sheet_name = "db"
        col_count = 80  # 十分な列数
        start_row = 38944  # 実際の新規追加行
        end_row = 38947    # 実際の新規追加行
        
        print(f"🧪 書式設定テスト開始: シート='{sheet_name}', 行範囲={start_row}-{end_row}")
        
        # 書式設定を実行
        uploader._apply_formatting_to_range_fixed(sheet_name, col_count, start_row, end_row)
        
        print("✅ 書式設定テスト完了")
        
    except Exception as e:
        logger.error(f"書式設定テストエラー: {e}")
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    test_formatting()
