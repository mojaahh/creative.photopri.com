#!/usr/bin/env python3
"""
CustomerDB用の分割アップロードスクリプト
大きな顧客データセットを安全にGoogle Sheetsにアップロードします
"""

import os
import sys
import csv
import time
import logging
from typing import List, Dict
from pathlib import Path

# プロジェクトルートのパスを追加
sys.path.append(str(Path(__file__).parent.parent))

from core.customer_db_generator import CustomerDBGenerator
from core.spreadsheet_uploader import SpreadsheetUploader

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_upload_customer_db.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CustomerDBBatchUploader:
    def __init__(self, batch_size: int = 500):
        """初期化"""
        self.batch_size = batch_size
        self.generator = None
        self.uploader = None
        
    def setup_components(self):
        """コンポーネントを初期化"""
        try:
            self.generator = CustomerDBGenerator()
            self.uploader = SpreadsheetUploader()
            logger.info("✅ コンポーネントの初期化が完了しました")
            return True
        except Exception as e:
            logger.error(f"❌ コンポーネントの初期化に失敗: {e}")
            return False
    
    def split_csv_file(self, csv_filepath: str) -> List[Dict]:
        """CSVファイルを分割"""
        try:
            chunks = []
            chunk_data = []
            chunk_number = 1
            
            with open(csv_filepath, 'r', encoding='utf-8-sig') as csvfile:
                reader = csv.reader(csvfile)
                header = next(reader)  # ヘッダー行を取得
                
                for i, row in enumerate(reader):
                    chunk_data.append(row)
                    
                    # バッチサイズに達したら、または最後の行の場合
                    if len(chunk_data) >= self.batch_size or i == len(list(csv.reader(open(csv_filepath, 'r', encoding='utf-8-sig')))) - 2:
                        # ヘッダー付きでチャンクを作成
                        chunk_with_header = [header] + chunk_data
                        
                        chunk_filename = f"customer_chunk_{chunk_number:03d}.csv"
                        chunk_filepath = f"exports/{chunk_filename}"
                        
                        # チャンクファイルに保存
                        with open(chunk_filepath, 'w', newline='', encoding='utf-8-sig') as chunk_file:
                            writer = csv.writer(chunk_file)
                            writer.writerows(chunk_with_header)
                        
                        chunks.append({
                            'filename': chunk_filename,
                            'filepath': chunk_filepath,
                            'data': chunk_with_header,
                            'row_count': len(chunk_with_header)
                        })
                        
                        logger.info(f"チャンク {chunk_number}: {len(chunk_data)}行のデータを保存")
                        
                        chunk_data = []
                        chunk_number += 1
            
            logger.info(f"📊 CSVファイルを {len(chunks)} 個のチャンクに分割しました")
            return chunks
            
        except Exception as e:
            logger.error(f"❌ CSVファイル分割エラー: {e}")
            return []
    
    def cleanup_chunk_files(self, chunks: List[Dict]):
        """一時的なチャンクファイルを削除"""
        try:
            for chunk in chunks:
                if os.path.exists(chunk['filepath']):
                    os.remove(chunk['filepath'])
                    logger.info(f"一時ファイル削除: {chunk['filename']}")
            logger.info("✅ 一時ファイルのクリーンアップが完了しました")
        except Exception as e:
            logger.error(f"❌ ファイルクリーンアップエラー: {e}")
    
    def upload_chunk_to_sheet(self, chunk: Dict, sheet_name: str = "CustomerDB", is_first_chunk: bool = False) -> bool:
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
            logger.error(f"❌ チャンクアップロードエラー: {e}")
            return False
    
    def batch_upload_customer_db(self, sheet_name: str = "CustomerDB") -> bool:
        """顧客データベースを分割アップロード"""
        try:
            logger.info("🚀 CustomerDBの分割アップロードを開始します")
            
            # 1. 顧客データをエクスポート
            logger.info("📊 顧客データのエクスポートを開始...")
            if not self.generator.export_customers():
                logger.error("❌ 顧客データのエクスポートに失敗しました")
                return False
            
            # 最新のCSVファイルを取得
            exports_dir = Path("exports")
            customer_files = list(exports_dir.glob("customers_export_*.csv"))
            if not customer_files:
                logger.error("❌ エクスポートされた顧客データファイルが見つかりません")
                return False
            
            latest_file = max(customer_files, key=lambda x: x.stat().st_mtime)
            logger.info(f"📁 アップロード対象ファイル: {latest_file.name}")
            
            # 2. CSVファイルを分割
            logger.info("✂️ CSVファイルの分割を開始...")
            chunks = self.split_csv_file(str(latest_file))
            if not chunks:
                logger.error("❌ CSVファイルの分割に失敗しました")
                return False
            
            total_chunks = len(chunks)
            logger.info(f"📊 分割完了: {total_chunks}個のチャンク")
            
            # 3. シートをクリアして最初のチャンクをアップロード
            logger.info(f"🗑️ シート '{sheet_name}' の内容をクリア中...")
            if not self.uploader.clear_sheet(sheet_name):
                logger.error(f"❌ シート '{sheet_name}' のクリアに失敗しました")
                return False
            
            # 4. チャンクを順次アップロード
            successful_chunks = 0
            failed_chunks = 0
            
            for i, chunk in enumerate(chunks, 1):
                try:
                    logger.info(f"📤 チャンク {i}/{total_chunks} の処理中...")
                    
                    # 最初のチャンクかどうかを判定
                    is_first_chunk = (i == 1)
                    
                    if self.upload_chunk_to_sheet(chunk, sheet_name, is_first_chunk):
                        successful_chunks += 1
                        logger.info(f"✅ チャンク {i}/{total_chunks} 完了")
                    else:
                        failed_chunks += 1
                        logger.error(f"❌ チャンク {i}/{total_chunks} 失敗")
                    
                    # レート制限対策
                    if i < total_chunks:
                        time.sleep(2)
                        
                except Exception as e:
                    failed_chunks += 1
                    logger.error(f"❌ チャンク {i}/{total_chunks} でエラー: {e}")
                    continue
            
            # 5. 一時ファイルをクリーンアップ
            logger.info("🧹 一時ファイルのクリーンアップ中...")
            self.cleanup_chunk_files(chunks)
            
            # 6. 結果を表示
            logger.info(f"🎉 分割アップロードが完了しました！")
            logger.info(f"成功: {successful_chunks}/{total_chunks} チャンク")
            if failed_chunks > 0:
                logger.warning(f"失敗: {failed_chunks}/{total_chunks} チャンク")
            
            return failed_chunks == 0
            
        except Exception as e:
            logger.error(f"❌ バッチアップロードエラー: {e}")
            return False

def main():
    """メイン関数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="CustomerDBの分割アップロード")
    parser.add_argument("--batch-size", type=int, default=500, help="チャンクサイズ（デフォルト: 500）")
    parser.add_argument("--sheet", default="CustomerDB", help="シート名（デフォルト: CustomerDB）")
    
    args = parser.parse_args()
    
    # エクスポートディレクトリを作成
    os.makedirs("exports", exist_ok=True)
    
    # バッチアップローダーを初期化
    uploader = CustomerDBBatchUploader(batch_size=args.batch_size)
    
    # コンポーネントをセットアップ
    if not uploader.setup_components():
        logger.error("❌ コンポーネントのセットアップに失敗しました")
        sys.exit(1)
    
    # 分割アップロードを実行
    if uploader.batch_upload_customer_db(args.sheet):
        logger.info("🎉 CustomerDBの分割アップロードが正常に完了しました！")
        sys.exit(0)
    else:
        logger.error("❌ CustomerDBの分割アップロードに失敗しました")
        sys.exit(1)

if __name__ == "__main__":
    main()
