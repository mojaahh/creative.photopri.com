#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
顧客データベースをGoogleスプレッドシートにアップロードするスクリプト
"""

import os
import sys
from core.spreadsheet_uploader import SpreadsheetUploader

def main():
    """メイン実行関数"""
    try:
        # 最新の顧客データCSVファイルを検索
        exports_dir = "exports"
        if not os.path.exists(exports_dir):
            print("❌ エクスポートディレクトリが存在しません。先に顧客データをエクスポートしてください。")
            return
        
        # 顧客データのCSVファイルを検索（customers_export_で始まるファイル）
        csv_files = [f for f in os.listdir(exports_dir) 
                    if f.startswith('customers_export_') and f.endswith('.csv')]
        
        if not csv_files:
            print("❌ 顧客データのCSVファイルが見つかりません。")
            print("先に 'python customer_db_generator.py' を実行して顧客データをエクスポートしてください。")
            return
        
        # 最新の顧客データCSVファイルを選択
        latest_csv = max(csv_files, key=lambda x: os.path.getctime(os.path.join(exports_dir, x)))
        csv_filepath = os.path.join(exports_dir, latest_csv)
        
        print(f"📁 アップロード対象ファイル: {latest_csv}")
        print(f"📊 シート名: CustomerDB")
        
        # スプレッドシートアップローダーを初期化
        uploader = SpreadsheetUploader()
        
        # 顧客データベースをスプレッドシートにアップロード
        if uploader.upload_customer_db(csv_filepath, "CustomerDB"):
            print(f"✅ 顧客データベースのアップロードが完了しました")
            print(f"📊 シート名: CustomerDB")
            print(f"🔗 スプレッドシートURL: {uploader.get_spreadsheet_url()}")
            print("\n📋 次のステップ:")
            print("1. ServiceDataシートで新規顧客数の集計関数を更新")
            print("2. 各サービスの顧客分析を開始")
        else:
            print("❌ 顧客データベースのアップロードに失敗しました")
            
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    main()
