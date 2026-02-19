#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
注文データを分割してアップロードするスクリプト
大量データでもエラーが発生しないよう、小さなチャンクに分けてアップロードします
"""

import os
import sys
import csv
import logging
from datetime import datetime
from core.order_export import ShopifyOrderExporter
from core.spreadsheet_uploader import SpreadsheetUploader

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_upload.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchOrderUploader:
    """分割アップロード用の注文データアップローダー"""
    
    def __init__(self):
        """初期化"""
        self.exporter = None
        self.uploader = None
        self.batch_size = 500  # 1回あたりのアップロード行数
    
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
            
            return True
            
        except Exception as e:
            logger.error(f"コンポーネント初期化エラー: {e}")
            return False
    
    def split_csv_file(self, csv_filepath: str) -> list:
        """CSVファイルを小さなチャンクに分割"""
        try:
            logger.info(f"CSVファイルの分割を開始: {csv_filepath}")
            
            # CSVファイルを読み込み
            with open(csv_filepath, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                all_data = list(reader)
            
            if not all_data:
                logger.warning("CSVファイルにデータがありません")
                return []
            
            header = all_data[0]
            data_rows = all_data[1:]
            total_rows = len(data_rows)
            
            logger.info(f"総行数: {total_rows}行（ヘッダー除く）")
            
            # チャンクに分割
            chunks = []
            for i in range(0, total_rows, self.batch_size):
                chunk = [header] + data_rows[i:i + self.batch_size]
                
                chunk_filename = f"chunk_{i//self.batch_size + 1:03d}_{len(chunk)-1}rows.csv"
                chunk_filepath = os.path.join('exports', chunk_filename)
                
                # チャンクファイルを保存
                with open(chunk_filepath, 'w', newline='', encoding='utf-8') as chunk_file:
                    writer = csv.writer(chunk_file)
                    writer.writerows(chunk)
                
                chunks.append({
                    'filename': chunk_filename,
                    'filepath': chunk_filepath,
                    'data': chunk,
                    'row_count': len(chunk) - 1  # ヘッダー除く
                })
                
                logger.info(f"チャンク {i//self.batch_size + 1}: {len(chunk)-1}行")
            
            logger.info(f"✅ CSVファイルを {len(chunks)} 個のチャンクに分割完了")
            return chunks
            
        except Exception as e:
            logger.error(f"CSVファイル分割エラー: {e}")
            return []
    
    def upload_chunk_to_sheet(self, chunk: dict, sheet_name: str = "db", is_first_chunk: bool = False) -> bool:
        """単一チャンクをスプレッドシートにアップロード"""
        try:
            chunk_filename = chunk['filename']
            chunk_data = chunk['data']
            row_count = chunk['row_count']
            
            logger.info(f"チャンク '{chunk_filename}' のアップロード開始 ({row_count}行)")
            
            if is_first_chunk:
                # 最初のチャンクは通常のアップロード（ヘッダー付き）
                if self.uploader.upload_data_to_sheet(sheet_name, chunk_data):
                    logger.info(f"✅ チャンク '{chunk_filename}' のアップロード完了（ヘッダー付き）")
                    return True
                else:
                    logger.error(f"❌ チャンク '{chunk_filename}' のアップロード失敗")
                    return False
            else:
                # 2番目以降のチャンクはヘッダーなしで追加
                data_without_header = chunk_data[1:]  # ヘッダーを除く
                if self.uploader.append_data_to_sheet(sheet_name, data_without_header):
                    logger.info(f"✅ チャンク '{chunk_filename}' のアップロード完了（データ追加）")
                    return True
                else:
                    logger.error(f"❌ チャンク '{chunk_filename}' のアップロード失敗")
                    return False
                
        except Exception as e:
            logger.error(f"チャンクアップロードエラー: {e}")
            return False
    
    def batch_upload_orders(self, sheet_name: str = "db", force_initial: bool = True) -> bool:
        """注文データを分割してアップロード"""
        try:
            logger.info("分割アップロード処理を開始")
            
            # 全期間の注文データをエクスポート
            logger.info("全期間の注文データをエクスポート中...")
            filename, is_initial = self.exporter.export_orders(force_initial=force_initial)
            
            if not filename:
                logger.error("注文データのエクスポートに失敗しました")
                return False
            
            csv_filepath = f"exports/{filename}"
            logger.info(f"エクスポート完了: {filename}")
            
            # CSVファイルをチャンクに分割
            chunks = self.split_csv_file(csv_filepath)
            
            if not chunks:
                logger.error("CSVファイルの分割に失敗しました")
                return False
            
            # スプレッドシートの準備
            if force_initial:
                # 初回実行時はシートをクリア
                logger.info(f"シート '{sheet_name}' の内容をクリア中...")
                self.uploader.clear_sheet_content(sheet_name)
                logger.info("✅ シートのクリア完了")
            
            # 各チャンクを順次アップロード
            successful_chunks = 0
            total_chunks = len(chunks)
            
            for i, chunk in enumerate(chunks, 1):
                logger.info(f"チャンク {i}/{total_chunks} の処理中...")
                
                # 最初のチャンクかどうかを判定
                is_first_chunk = (i == 1)
                
                if self.upload_chunk_to_sheet(chunk, sheet_name, is_first_chunk):
                    successful_chunks += 1
                    logger.info(f"✅ チャンク {i}/{total_chunks} 完了")
                else:
                    logger.error(f"❌ チャンク {i}/{total_chunks} 失敗")
                    # エラーが発生しても続行
                
                # チャンク間の待機時間（API制限対策）
                if i < total_chunks:
                    logger.info("次のチャンクまで待機中...")
                    import time
                    time.sleep(2)
            
            # 結果サマリー
            logger.info("=" * 60)
            logger.info(f"分割アップロード完了: {successful_chunks}/{total_chunks} チャンク成功")
            logger.info(f"対象シート: {sheet_name}")
            logger.info("=" * 60)
            
            return successful_chunks == total_chunks
            
        except Exception as e:
            logger.error(f"分割アップロードエラー: {e}")
            return False
    
    def cleanup_chunk_files(self):
        """分割されたチャンクファイルを削除"""
        try:
            exports_dir = "exports"
            if not os.path.exists(exports_dir):
                return
            
            # chunk_で始まるファイルを削除
            chunk_files = [f for f in os.listdir(exports_dir) if f.startswith('chunk_')]
            
            for chunk_file in chunk_files:
                filepath = os.path.join(exports_dir, chunk_file)
                try:
                    os.remove(filepath)
                    logger.info(f"チャンクファイルを削除: {chunk_file}")
                except Exception as e:
                    logger.warning(f"チャンクファイルの削除に失敗: {chunk_file} - {e}")
            
            logger.info(f"✅ {len(chunk_files)} 個のチャンクファイルを削除完了")
            
        except Exception as e:
            logger.error(f"チャンクファイル削除エラー: {e}")

def main():
    """メイン実行関数"""
    try:
        print("🚀 注文データの分割アップロードを開始します")
        
        # コマンドライン引数の処理
        sheet_name = "db"  # デフォルトはdbシート
        force_initial = True  # デフォルトは全期間データ
        
        if len(sys.argv) > 1:
            sheet_name = sys.argv[1]
        
        if len(sys.argv) > 2 and sys.argv[2].lower() == 'update':
            force_initial = False
            print("🔄 更新モードで実行します")
        else:
            print("🔄 全期間データモードで実行します")
        
        # 分割アップローダーを初期化
        batch_uploader = BatchOrderUploader()
        
        if not batch_uploader.setup_components():
            print("❌ コンポーネントの初期化に失敗しました")
            sys.exit(1)
        
        # 分割アップロードを実行
        success = batch_uploader.batch_upload_orders(sheet_name, force_initial)
        
        if success:
            print("\n🎉 分割アップロードが完了しました！")
            print(f"📊 対象シート: {sheet_name}")
            print(f"🔗 スプレッドシートURL: {batch_uploader.uploader.get_spreadsheet_url()}")
            
            # チャンクファイルをクリーンアップ
            print("\n🧹 一時ファイルをクリーンアップ中...")
            batch_uploader.cleanup_chunk_files()
            
        else:
            print("\n❌ 分割アップロードに失敗しました")
            print("ログファイルを確認してください: レポート/batch_upload.log")
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
