#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
注文データエクスポートパイプライン統合スクリプト
注文データの取得からスプレッドシートへの反映まで一連の処理を実行します

新機能: 注文データの更新と同時に顧客データベースも自動更新
- 注文回数、累計金額などの顧客情報が最新状態に保たれます
- 新規顧客数の正確な集計が可能になります
"""

import os
import sys
import logging
from datetime import datetime
from core.order_export import ShopifyOrderExporter
from core.spreadsheet_uploader import SpreadsheetUploader
from core.customer_db_generator import CustomerDBGenerator

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OrderExportPipeline:
    """注文データエクスポートパイプライン"""
    
    def __init__(self):
        """初期化"""
        self.exporter = None
        self.uploader = None
        self.customer_generator = None
    
    def setup_components(self):
        """コンポーネントを初期化"""
        try:
            logger.info("コンポーネントの初期化を開始")
            
            # 注文データエクスポーターを初期化
            self.exporter = ShopifyOrderExporter()
            logger.info("✅ 注文データエクスポーターの初期化完了")
            
            # スプレッドシートアップローダーを初期化
            self.uploader = SpreadsheetUploader()
            logger.info("✅ スプレッドシートアップローダーの初期化完了")
            
            # 顧客データベースジェネレーターを初期化
            self.customer_generator = CustomerDBGenerator()
            logger.info("✅ 顧客データベースジェネレーターの初期化完了")
            
            return True
            
        except Exception as e:
            logger.error(f"コンポーネント初期化エラー: {e}")
            return False
    
    def export_orders(self, months_ago: int = 1, force_initial: bool = False) -> tuple:
        """注文データをエクスポート"""
        try:
            logger.info(f"注文データのエクスポートを開始")
            
            filename, is_initial = self.exporter.export_orders(months_ago, force_initial)
            
            if filename:
                mode = "初回実行" if is_initial else "更新実行"
                logger.info(f"✅ 注文データのエクスポート完了 ({mode}): {filename}")
                return filename, is_initial
            else:
                logger.warning("⚠️ エクスポート対象のデータがありませんでした")
                return "", is_initial
                
        except Exception as e:
            logger.error(f"注文データエクスポートエラー: {e}")
            raise
    
    def upload_to_spreadsheet(self, csv_filename: str, is_initial: bool = False, sheet_name: str = None) -> bool:
        """CSVファイルをスプレッドシートにアップロード（重複処理対応）"""
        try:
            if not csv_filename:
                logger.warning("アップロードするCSVファイルが指定されていません")
                return False
            
            csv_filepath = f"exports/{csv_filename}"
            
            if not os.path.exists(csv_filepath):
                logger.error(f"CSVファイルが見つかりません: {csv_filepath}")
                return False
            
            if not sheet_name:
                sheet_name = "db"
            
            logger.info(f"スプレッドシートへのアップロードを開始: {csv_filename}")
            
            # CSVファイルを読み込み
            csv_data = self.uploader.read_csv_file(csv_filepath)
            
            if not csv_data:
                logger.warning("CSVファイルにデータがありません")
                return False
            
            if is_initial:
                # 初回実行：通常のアップロード（既存データをクリア）
                logger.info("初回実行モード: 既存データをクリアして全データをアップロード")
                
                # シートを作成または既存データをクリア
                if not self.uploader.create_new_sheet(sheet_name):
                    return False
                
                self.uploader.clear_sheet_content(sheet_name)
                success = self.uploader.upload_data_to_sheet(sheet_name, csv_data)
                
                if success:
                    self.uploader.format_sheet(sheet_name, len(csv_data[0]))
                
            else:
                # 通常実行：重複処理を含むアップロード
                logger.info("更新実行モード: 重複チェックと増分更新を実行")
                success = self.uploader.upload_with_duplicate_handling(sheet_name, csv_data)
            
            if success:
                mode = "初回アップロード" if is_initial else "増分更新"
                logger.info(f"✅ スプレッドシートへの{mode}完了: {sheet_name}")
                return True
            else:
                logger.error("❌ スプレッドシートへのアップロードに失敗しました")
                return False
                
        except Exception as e:
            logger.error(f"スプレッドシートアップロードエラー: {e}")
            return False
    
    def run_pipeline(self, months_ago: int = 1, sheet_name: str = None, force_initial: bool = False) -> bool:
        """パイプライン全体を実行"""
        try:
            logger.info("=" * 60)
            logger.info("注文データエクスポートパイプライン開始")
            logger.info("=" * 60)
            
            # ステップ1: コンポーネント初期化
            if not self.setup_components():
                logger.error("コンポーネントの初期化に失敗しました")
                return False
            
            # ステップ2: 注文データエクスポート
            csv_filename, is_initial = self.export_orders(months_ago, force_initial)
            if not csv_filename:
                logger.warning("注文データのエクスポートが完了しませんでした")
                return False
            
            # ステップ3: スプレッドシートへのアップロード（重複処理対応）
            if not self.upload_to_spreadsheet(csv_filename, is_initial, sheet_name):
                logger.error("スプレッドシートへのアップロードに失敗しました")
                return False
            
            # ステップ4: 顧客データベースの更新
            if not self.update_customer_database():
                logger.warning("⚠️ 顧客データベースの更新に失敗しましたが、注文データは正常に処理されました")
            
            logger.info("=" * 60)
            mode = "初回実行" if is_initial else "更新実行"
            logger.info(f"✅ パイプライン実行完了 ({mode})")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"パイプライン実行エラー: {e}")
            return False
    
    def update_customer_database(self) -> bool:
        """顧客データベースを更新"""
        try:
            logger.info("顧客データベースの更新を開始")
            
            # 顧客データをエクスポート
            customer_filename = self.customer_generator.export_customers()
            
            if not customer_filename:
                logger.error("顧客データのエクスポートに失敗しました")
                return False
            
            # スプレッドシートにアップロード
            if self.uploader.upload_customer_db(customer_filename, "CustomerDB"):
                logger.info("✅ 顧客データベースの更新が完了しました")
                return True
            else:
                logger.error("顧客データベースのスプレッドシートアップロードに失敗しました")
                return False
                
        except Exception as e:
            logger.error(f"顧客データベース更新エラー: {e}")
            return False
    
    def get_results_summary(self) -> dict:
        """実行結果のサマリーを取得"""
        try:
            summary = {
                'timestamp': datetime.now().isoformat(),
                'spreadsheet_url': self.uploader.get_spreadsheet_url() if self.uploader else None,
                'exports_directory': 'exports',
                'logs_directory': 'logs'
            }
            
            # エクスポートファイル一覧
            exports_dir = "exports"
            if os.path.exists(exports_dir):
                csv_files = [f for f in os.listdir(exports_dir) if f.endswith('.csv')]
                summary['exported_files'] = sorted(csv_files, reverse=True)
                summary['total_export_files'] = len(csv_files)
            
            return summary
            
        except Exception as e:
            logger.error(f"結果サマリー取得エラー: {e}")
            return {}

def main():
    """メイン実行関数"""
    try:
        print("🚀 注文データエクスポートパイプラインを開始します")
        
        # コマンドライン引数の処理
        months_ago = 1  # デフォルトは前月
        sheet_name = None
        
        if len(sys.argv) > 1:
            try:
                months_ago = int(sys.argv[1])
            except ValueError:
                print("⚠️ 月数は数値で指定してください。デフォルト値（1ヶ月前）を使用します。")
        
        if len(sys.argv) > 2:
            sheet_name = sys.argv[2]
        
        # 初回実行フラグの処理
        force_initial = False
        if len(sys.argv) > 3 and sys.argv[3].lower() == 'initial':
            force_initial = True
            print("🔄 初回実行モードが指定されました")
        
        # パイプラインを実行
        pipeline = OrderExportPipeline()
        success = pipeline.run_pipeline(months_ago, sheet_name, force_initial)
        
        if success:
            print("\n🎉 パイプラインの実行が完了しました！")
            
            # 結果サマリーを表示
            summary = pipeline.get_results_summary()
            if summary.get('spreadsheet_url'):
                print(f"📊 スプレッドシート: {summary['spreadsheet_url']}")
            
            if summary.get('exported_files'):
                print(f"📁 最新のエクスポートファイル: {summary['exported_files'][0]}")
            
            print(f"📂 エクスポートディレクトリ: {summary['exports_directory']}")
            print(f"📝 ログディレクトリ: {summary['logs_directory']}")
            print("\n📋 更新されたデータ:")
            print("✅ 注文データ（dbシート）")
            print("✅ 顧客データベース（CustomerDBシート）")
            print("\n💡 新規顧客数の集計が可能になりました")
            
        else:
            print("\n❌ パイプラインの実行に失敗しました")
            print("ログファイルを確認してください: レポート/pipeline.log")
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
