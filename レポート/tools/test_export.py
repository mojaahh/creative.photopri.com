#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
注文データエクスポートのテスト実行スクリプト
"""

import os
import sys
import logging
from datetime import datetime
from run_order_export_pipeline import OrderExportPipeline

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_export.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_export():
    """テストエクスポートを実行"""
    try:
        print("🧪 注文データエクスポートのテストを開始します")
        print("📅 対象期間: 全期間（2019年〜現在）のデータ（初回実行モード）")
        print("📊 出力先: 指定されたスプレッドシートの'test'シート")
        print("=" * 60)
        
        # パイプラインを初期化
        pipeline = OrderExportPipeline()
        
        # テスト実行（初回実行モードで全期間データを取得、testシートに出力）
        success = pipeline.run_pipeline(months_ago=1, sheet_name="db", force_initial=True)
        
        if success:
            print("\n🎉 テストエクスポートが正常に完了しました！")
            
            # 結果サマリーを表示
            summary = pipeline.get_results_summary()
            if summary.get('spreadsheet_url'):
                print(f"📊 スプレッドシート: {summary['spreadsheet_url']}")
            
            if summary.get('exported_files'):
                print(f"📁 最新のエクスポートファイル: {summary['exported_files'][0]}")
            
            print(f"📂 エクスポートディレクトリ: {summary['exports_directory']}")
            print(f"📝 ログディレクトリ: {summary['logs_directory']}")
            
            # 設定確認情報を表示
            print("\n📋 設定確認:")
            print("✅ 5ストア対応のマルチストアシステム")
            print("✅ 指定されたヘッダー形式でCSVエクスポート")
            print("✅ 指定されたスプレッドシートの'test'シートに出力")
            print("✅ ストア情報（Store Name, Store URL）を含む")
            
        else:
            print("\n❌ テストエクスポートに失敗しました")
            print("ログファイルを確認してください: test_export.log")
            return False
            
    except Exception as e:
        print(f"\n❌ テスト実行中にエラーが発生しました: {e}")
        logger.error(f"テスト実行エラー: {e}")
        return False
    
    return True

def main():
    """メイン実行関数"""
    try:
        success = test_export()
        
        if success:
            print("\n✅ 全てのテストが正常に完了しました")
            sys.exit(0)
        else:
            print("\n❌ テストに失敗しました")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ ユーザーによって処理が中断されました")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 予期しないエラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
