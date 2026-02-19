#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSVファイルをGoogleスプレッドシートにアップロードするスクリプト
注文データをスプレッドシートに反映します
"""

import os
import sys
import csv
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spreadsheet_uploader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SpreadsheetUploader:
    """Googleスプレッドシートアップローダー"""
    
    # Google Sheets APIのスコープ
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    # レート制限対策のための待機時間（秒）
    RATE_LIMIT_DELAY = 10.0
    
    def __init__(self):
        """初期化"""
        # ルートディレクトリの.envファイルを読み込み
        load_dotenv('../.env')
        self.credentials = None
        self.service = None
        # 指定されたスプレッドシートIDを使用
        self.spreadsheet_id = "1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g"
        
        # 認証情報の設定
        self.setup_credentials()
        
        # 出力ディレクトリ作成
        os.makedirs('logs', exist_ok=True)
    
    def _rate_limit_delay(self):
        """レート制限対策のための待機"""
        time.sleep(self.RATE_LIMIT_DELAY)
    
    def _retry_on_error(self, func, max_retries=3):
        """エラー時のリトライ処理（レート制限、タイムアウト、その他のエラー）"""
        for attempt in range(max_retries):
            try:
                return func()
            except HttpError as e:
                if e.resp.status == 429:  # Rate limit exceeded
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 5  # 指数バックオフ
                        logger.warning(f"レート制限エラー。{wait_time}秒待機してリトライします... (試行 {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"レート制限エラー: 最大リトライ回数に達しました")
                        raise
                else:
                    raise
            except Exception as e:
                error_msg = str(e).lower()
                if 'timeout' in error_msg or 'timed out' in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 10  # タイムアウトの場合は長めの待機
                        logger.warning(f"タイムアウトエラー。{wait_time}秒待機してリトライします... (試行 {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"タイムアウトエラー: 最大リトライ回数に達しました")
                        raise
                else:
                    # その他のエラーは即座に再発生
                    raise
    
    def setup_credentials(self):
        """Google API認証情報を設定（Service Account使用）"""
        credentials_path = '../credentials.json'
        
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(
                f"Service Account認証ファイルが見つかりません: {credentials_path}"
            )
        
        try:
            # Service Account認証を使用
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path, scopes=self.SCOPES
            )
            logger.info("Service Account認証が完了しました")
            
            # Google Sheets APIサービスを構築（タイムアウト設定付き）
            self.service = build('sheets', 'v4', credentials=self.credentials, cache_discovery=False)
            logger.info("Google Sheets APIサービスを構築しました")
            
        except Exception as e:
            logger.error(f"認証エラー: {e}")
            raise
    
    def read_csv_file(self, csv_filepath: str) -> List[List]:
        """CSVファイルを読み込み"""
        try:
            with open(csv_filepath, 'r', encoding='utf-8-sig') as csvfile:
                reader = csv.reader(csvfile)
                return list(reader)
        
        except Exception as e:
            logger.error(f"CSVファイル読み込みエラー: {e}")
            raise
    
    def create_new_sheet(self, sheet_name: str) -> bool:
        """新しいシートを作成"""
        try:
            self._rate_limit_delay()
            request_body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_name,
                            'gridProperties': {
                                'rowCount': 1000,
                                'columnCount': 26
                            }
                        }
                    }
                }]
            }
            
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=request_body
            ).execute()
            
            logger.info(f"新しいシート '{sheet_name}' を作成しました")
            return True
        
        except HttpError as e:
            if e.resp.status == 400 and 'already exists' in str(e):
                logger.info(f"シート '{sheet_name}' は既に存在します")
                return True
            else:
                logger.error(f"シート作成エラー: {e}")
                return False
    
    def clear_sheet_content(self, sheet_name: str):
        """シートの内容をクリア"""
        try:
            self._rate_limit_delay()
            range_name = f"{sheet_name}!A:Z"
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()
            
            logger.info(f"シート '{sheet_name}' の内容をクリアしました")
        
        except HttpError as e:
            logger.error(f"シートクリアエラー: {e}")
    
    def upload_data_to_sheet(self, sheet_name: str, data: List[List]) -> bool:
        """データをシートにアップロード"""
        try:
            if not data:
                logger.warning("アップロードするデータがありません")
                return False
            
            # 必要な行数を事前に確保
            self._ensure_sheet_capacity(sheet_name, len(data))
            
            # データ範囲を指定
            # 列数が多い場合のカラム文字を正しく計算
            col_count = len(data[0])
            if col_count <= 26:
                end_col = chr(ord('A') + col_count - 1)
            else:
                # 26列を超える場合（AA, AB, AC...）
                first_char = chr(ord('A') + (col_count - 1) // 26 - 1)
                second_char = chr(ord('A') + (col_count - 1) % 26)
                end_col = first_char + second_char
            
            range_name = f"{sheet_name}!A1:{end_col}{len(data)}"
            
            # データをアップロード
            body = {
                'values': data
            }
            
            self._rate_limit_delay()
            result = self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            # 書式設定を適用
            self._apply_number_formatting(sheet_name, col_count, len(data))

            logger.info(f"シート '{sheet_name}' に {result.get('updatedCells')} セルのデータをアップロードしました")
            return True
        
        except HttpError as e:
            logger.error(f"データアップロードエラー: {e}")
            return False
    
    def append_data_to_sheet(self, sheet_name: str, data: List[List]) -> bool:
        """データをシートに追加（既存データの後に）"""
        try:
            if not data:
                return True
            
            # シートの現在の行数を取得
            current_range = f"{sheet_name}!A:A"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=current_range
            ).execute()
            
            current_rows = len(result.get('values', []))
            start_row = current_rows + 1
            
            # 必要な行数を事前に拡張
            required_rows = start_row + len(data) - 1
            self._ensure_sheet_capacity(sheet_name, required_rows)
            
            # データを追加する範囲を指定
            range_name = f"{sheet_name}!A{start_row}"
            
            body = {
                'values': data
            }
            
            result = self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.info(f"シート '{sheet_name}' に {len(data)} 行のデータを追加しました")
            return True
            
        except Exception as e:
            logger.error(f"データ追加エラー: {e}")
            return False
    
    def _get_column_letter(self, column_number: int) -> str:
        """列番号を列文字に変換（例: 1 -> A, 27 -> AA）"""
        result = ""
        while column_number > 0:
            column_number -= 1
            result = chr(65 + (column_number % 26)) + result
            column_number //= 26
        return result

    def _apply_number_formatting(self, sheet_name: str, col_count: int, row_count: int):
        """数値と通貨の書式設定を適用"""
        try:
            logger.info(f"書式設定開始: シート='{sheet_name}', 列数={col_count}, 行数={row_count}")
            
            # シートIDを取得
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            sheet_id = None
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
            
            if sheet_id is None:
                logger.error(f"シート '{sheet_name}' が見つかりません")
                return
            
            requests = []
            
            # Total列（12列目）を通貨形式に設定
            if col_count >= 12:
                logger.info(f"Total列（12列目）を通貨形式に設定: 行1-{row_count}")
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,  # ヘッダー行を除く
                            'endRowIndex': row_count,
                            'startColumnIndex': 11,  # 12列目（0ベース）
                            'endColumnIndex': 12
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'CURRENCY',
                                    'pattern': '"¥"#,##0'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })
            
            # 通貨形式の列を設定（I, J, K, S, AX, BH, BJ列）
            currency_columns = [8, 9, 10, 18, 49, 59, 61]  # I(9), J(10), K(11), S(19), AX(50), BH(60), BJ(62)列目
            for col in currency_columns:
                if col_count > col:
                    logger.info(f"列{col+1}を通貨形式に設定")
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': 1,  # ヘッダー行を除く
                                'endRowIndex': row_count,
                                'startColumnIndex': col,  # 0ベース
                                'endColumnIndex': col + 1
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'numberFormat': {
                                        'type': 'CURRENCY',
                                        'pattern': '"¥"#,##0'
                                    }
                                }
                            },
                            'fields': 'userEnteredFormat.numberFormat'
                        }
                    })
            
            # Lineitem quantity列（17列目）を数値形式に設定
            if col_count >= 17:
                logger.info(f"Lineitem quantity列（17列目）を数値形式に設定: 行1-{row_count}")
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,  # ヘッダー行を除く
                            'endRowIndex': row_count,
                            'startColumnIndex': 16,  # 17列目（0ベース）
                            'endColumnIndex': 17
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'NUMBER',
                                    'pattern': '0'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })
            
            # 書式設定を適用
            if requests:
                logger.info(f"書式設定リクエスト数: {len(requests)}件")
                
                # デバッグ用：リクエスト内容を出力
                for i, request in enumerate(requests):
                    logger.info(f"リクエスト {i+1}: {request}")
                
                body = {'requests': requests}
                result = self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                ).execute()
                
                logger.info(f"APIレスポンス: {result}")
                logger.info(f"シート '{sheet_name}' に書式設定を適用しました: {len(requests)}件のリクエスト")
                print(f"✅ 書式設定完了: {len(requests)}件のリクエストを実行")
            else:
                logger.warning("書式設定リクエストがありません")
                print("⚠️ 書式設定リクエストがありません")
                
        except Exception as e:
            logger.error(f"書式設定エラー: {e}")
            print(f"❌ 書式設定エラー: {e}")
    
    def _apply_formatting_to_specific_rows(self, sheet_name: str, col_count: int, row_numbers: List[int]):
        """特定の行番号に書式設定を適用"""
        try:
            logger.info(f"特定行書式設定開始: シート='{sheet_name}', 列数={col_count}, 対象行={len(row_numbers)}件")
            
            # シートIDを取得
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            sheet_id = None
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
            
            if sheet_id is None:
                logger.error(f"シート '{sheet_name}' が見つかりません")
                return
            
            requests = []
            
            # 各行に対して書式設定を適用
            for row_num in row_numbers:
                # Total列（12列目）を通貨形式に設定
                if col_count >= 12:
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': row_num - 1,  # 0ベース
                                'endRowIndex': row_num,
                                'startColumnIndex': 11,  # 12列目（0ベース）
                                'endColumnIndex': 12
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'numberFormat': {
                                        'type': 'CURRENCY',
                                        'pattern': '"¥"#,##0'
                                    }
                                }
                            },
                            'fields': 'userEnteredFormat.numberFormat'
                        }
                    })
                
                # 通貨形式の列を設定（I, J, K, S, AX, BH, BJ列）
                currency_columns = [8, 9, 10, 18, 49, 59, 61]  # I(9), J(10), K(11), S(19), AX(50), BH(60), BJ(62)列目
                for col in currency_columns:
                    if col_count > col:
                        requests.append({
                            'repeatCell': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': row_num - 1,  # 0ベース
                                    'endRowIndex': row_num,
                                    'startColumnIndex': col,  # 0ベース
                                    'endColumnIndex': col + 1
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'numberFormat': {
                                            'type': 'CURRENCY',
                                            'pattern': '"¥"#,##0'
                                        }
                                    }
                                },
                                'fields': 'userEnteredFormat.numberFormat'
                            }
                        })
                
                # Lineitem quantity列（17列目）を数値形式に設定
                if col_count >= 17:
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': row_num - 1,  # 0ベース
                                'endRowIndex': row_num,
                                'startColumnIndex': 16,  # 17列目（0ベース）
                                'endColumnIndex': 17
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'numberFormat': {
                                        'type': 'NUMBER',
                                        'pattern': '0'
                                    }
                                }
                            },
                            'fields': 'userEnteredFormat.numberFormat'
                        }
                    })
            
            # 書式設定を適用
            if requests:
                logger.info(f"特定行書式設定リクエスト数: {len(requests)}件")
                body = {'requests': requests}
                result = self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                ).execute()
                
                logger.info(f"特定行書式設定完了: {len(row_numbers)}行、{len(requests)}件のリクエスト")
            else:
                logger.warning("特定行書式設定リクエストがありません")
                
        except Exception as e:
            logger.error(f"特定行書式設定エラー: {e}")
    
    def _apply_formatting_to_range(self, sheet_name: str, col_count: int, start_row: int, end_row: int):
        """指定範囲に書式設定を適用"""
        try:
            logger.info(f"範囲書式設定開始: シート='{sheet_name}', 列数={col_count}, 行範囲={start_row}-{end_row}")
            
            # シートIDを取得
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            sheet_id = None
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
            
            if sheet_id is None:
                logger.error(f"シート '{sheet_name}' が見つかりません")
                return
            
            requests = []
            
            # Total列（12列目）を通貨形式に設定
            if col_count >= 12:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': start_row - 1,  # 0ベース
                            'endRowIndex': end_row,
                            'startColumnIndex': 11,  # 12列目（0ベース）
                            'endColumnIndex': 12
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'CURRENCY',
                                    'pattern': '"¥"#,##0'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })
            
            # Subtotal, Shipping, Taxes列も通貨形式に設定
            currency_columns = [8, 9, 10]  # Subtotal, Shipping, Taxes（9, 10, 11列目）
            for col in currency_columns:
                if col_count > col:
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': start_row - 1,  # 0ベース
                                'endRowIndex': end_row,
                                'startColumnIndex': col,  # 0ベース
                                'endColumnIndex': col + 1
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'numberFormat': {
                                        'type': 'CURRENCY',
                                        'pattern': '"¥"#,##0'
                                    }
                                }
                            },
                            'fields': 'userEnteredFormat.numberFormat'
                        }
                    })
            
            # Lineitem quantity列（17列目）を数値形式に設定
            if col_count >= 17:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': start_row - 1,  # 0ベース
                            'endRowIndex': end_row,
                            'startColumnIndex': 16,  # 17列目（0ベース）
                            'endColumnIndex': 17
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'NUMBER',
                                    'pattern': '0'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })
            
            # 書式設定を適用
            if requests:
                logger.info(f"範囲書式設定リクエスト数: {len(requests)}件")
                body = {'requests': requests}
                result = self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                ).execute()
                
                logger.info(f"範囲書式設定完了: 行{start_row}-{end_row}、{len(requests)}件のリクエスト")
            else:
                logger.warning("範囲書式設定リクエストがありません")
                
        except Exception as e:
            logger.error(f"範囲書式設定エラー: {e}")
    
    def _apply_formatting_to_range_fixed(self, sheet_name: str, col_count: int, start_row: int, end_row: int):
        """指定範囲に正しい書式設定を適用（Google Sheets API仕様準拠）"""
        try:
            logger.info(f"修正版書式設定開始: シート='{sheet_name}', 行範囲={start_row}-{end_row}")
            
            # シートIDを取得
            sheet_id = self._get_sheet_id(sheet_name)
            if sheet_id is None:
                logger.error(f"シート '{sheet_name}' が見つかりません")
                return
            
            requests = []
            
            # 通貨書式を適用する列（I, J, K, L, S, AX, BH, BJ）
            currency_columns = [8, 9, 10, 11, 18, 49, 59, 61]  # 0ベースのインデックス
            
            for col_index in currency_columns:
                if col_count > col_index:
                    col_letter = chr(65 + col_index) if col_index < 26 else f"{chr(65 + col_index // 26 - 1)}{chr(65 + col_index % 26)}"
                    logger.info(f"列{col_letter}({col_index+1})を通貨形式に設定")
                    
                    requests.append({
                        'repeatCell': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': start_row - 1,
                                'endRowIndex': end_row,
                                'startColumnIndex': col_index,
                                'endColumnIndex': col_index + 1
                            },
                            'cell': {
                                'userEnteredFormat': {
                                    'numberFormat': {
                                        'type': 'CURRENCY',
                                        'pattern': '¥#,##0'
                                    }
                                }
                            },
                            'fields': 'userEnteredFormat.numberFormat'
                        }
                    })
            
            # Q列（Lineitem quantity）を数値形式に設定
            if col_count >= 17:
                logger.info(f"Q列(17)を数値形式に設定")
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': start_row - 1,
                            'endRowIndex': end_row,
                            'startColumnIndex': 16,  # Q列（0ベース）
                            'endColumnIndex': 17
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'NUMBER',
                                    'pattern': '0'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })
            
            # 書式設定を適用（小さなバッチで実行）
            if requests:
                logger.info(f"修正版書式設定リクエスト数: {len(requests)}件")
                
                # 小さなバッチに分けて実行（タイムアウト対策）
                batch_size = 5
                for i in range(0, len(requests), batch_size):
                    batch_requests = requests[i:i+batch_size]
                    
                    try:
                        body = {'requests': batch_requests}
                        logger.info(f"書式設定API呼び出し: バッチ {i//batch_size + 1}, リクエスト数: {len(batch_requests)}")
                        logger.info(f"リクエスト内容: {body}")
                        
                        result = self.service.spreadsheets().batchUpdate(
                            spreadsheetId=self.spreadsheet_id,
                            body=body
                        ).execute()
                        
                        logger.info(f"APIレスポンス: {result}")
                        logger.info(f"書式設定バッチ {i//batch_size + 1} 完了: {len(batch_requests)}件")
                        
                        # レート制限対策
                        import time
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"書式設定バッチ {i//batch_size + 1} エラー: {e}")
                        continue
                
                logger.info(f"修正版書式設定完了: 行{start_row}-{end_row}")
            else:
                logger.warning("修正版書式設定リクエストがありません")
                
        except Exception as e:
            logger.error(f"修正版書式設定エラー: {e}")
    
    def _ensure_sheet_capacity(self, sheet_name: str, required_rows: int):
        """シートの行数容量を確保"""
        try:
            # シートの現在のプロパティを取得
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            sheet_id = None
            current_rows = 0
            
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    current_rows = sheet['properties']['gridProperties']['rowCount']
                    break
            
            if sheet_id is None:
                logger.error(f"シート '{sheet_name}' が見つかりません")
                return False
            
            # 必要な行数が現在の行数を超える場合、拡張
            if required_rows > current_rows:
                additional_rows = required_rows - current_rows
                
                request = {
                    'appendDimension': {
                        'sheetId': sheet_id,
                        'dimension': 'ROWS',
                        'length': additional_rows
                    }
                }
                
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={'requests': [request]}
                ).execute()
                
                logger.info(f"シート '{sheet_name}' の行数を {current_rows} から {required_rows} に拡張しました")
            
            return True
            
        except Exception as e:
            logger.error(f"シート容量確保エラー: {e}")
            return False
    
    def clear_sheet(self, sheet_name: str) -> bool:
        """シートの内容をクリア"""
        try:
            # シートの現在の行数を取得
            current_range = f"{sheet_name}!A:A"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=current_range
            ).execute()
            
            current_rows = len(result.get('values', []))
            
            if current_rows > 0:
                # データが存在する場合、クリア（列数は適切な範囲に制限）
                # CustomerDBの列数は約20列程度なので、Z列までで十分
                range_name = f"{sheet_name}!A1:Z{current_rows}"
                self.service.spreadsheets().values().clear(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name
                ).execute()
                
                logger.info(f"シート '{sheet_name}' の内容をクリアしました")
            
            return True
            
        except Exception as e:
            logger.error(f"シートクリアエラー: {e}")
            return False
    
    def format_sheet(self, sheet_name: str, column_count: int):
        """シートのフォーマットを設定"""
        try:
            # ヘッダー行のフォーマット
            header_format = {
                'requests': [{
                    'repeatCell': {
                        'range': {
                            'sheetId': self._get_sheet_id(sheet_name),
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': column_count
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {
                                    'red': 0.2,
                                    'green': 0.6,
                                    'blue': 0.9
                                },
                                'textFormat': {
                                    'bold': True,
                                    'foregroundColor': {
                                        'red': 1,
                                        'green': 1,
                                        'blue': 1
                                    }
                                },
                                'horizontalAlignment': 'CENTER'
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
                    }
                }]
            }
            
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=header_format
            ).execute()
            
            # 列幅の自動調整
            auto_resize = {
                'requests': [{
                    'autoResizeDimensions': {
                        'dimensions': {
                            'sheetId': self._get_sheet_id(sheet_name),
                            'dimension': 'COLUMNS',
                            'startIndex': 0,
                            'endIndex': column_count
                        }
                    }
                }]
            }
            
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=auto_resize
            ).execute()
            
            logger.info(f"シート '{sheet_name}' のフォーマットを設定しました")
        
        except HttpError as e:
            logger.error(f"シートフォーマットエラー: {e}")
    
    def get_existing_data(self, sheet_name: str, recent_months: int = None) -> List[List]:
        """既存のスプレッドシートデータを取得（タイムアウト対策付き）
        
        Args:
            sheet_name: シート名
            recent_months: 直近Nヶ月のデータのみ取得する場合の月数（Noneの場合は全データ）
        """
        try:
            if recent_months:
                # 直近Nヶ月のデータのみを取得
                return self._get_recent_data(sheet_name, recent_months)
            
            # まず行数を確認
            def _get_row_count():
                self._rate_limit_delay()
                range_name = f"{sheet_name}!A:A"
                return self.service.spreadsheets().values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name
                ).execute()
            
            row_count_result = self._retry_on_error(_get_row_count)
            total_rows = len(row_count_result.get('values', []))
            
            if total_rows == 0:
                logger.info(f"シート '{sheet_name}' にデータがありません")
                return []
            
            logger.info(f"シート '{sheet_name}' の総行数: {total_rows}")
            
            # 大きなデータセットの場合は分割して取得
            if total_rows > 10000:
                logger.info(f"大きなデータセットのため分割取得を実行します")
                return self._get_data_in_batches(sheet_name, total_rows)
            else:
                # 小さなデータセットは一括取得
                def _get_data():
                    self._rate_limit_delay()
                    range_name = f"{sheet_name}!A:ZZ"
                    return self.service.spreadsheets().values().get(
                        spreadsheetId=self.spreadsheet_id,
                        range=range_name
                    ).execute()
                
                result = self._retry_on_error(_get_data)
                values = result.get('values', [])
                logger.info(f"既存データを取得: {len(values)}行")
                return values
        
        except HttpError as e:
            if e.resp.status == 400:
                logger.info(f"シート '{sheet_name}' が存在しないか、データがありません")
                return []
            else:
                logger.error(f"既存データ取得エラー: {e}")
                return []
        except Exception as e:
            logger.error(f"既存データ取得予期しないエラー: {e}")
            return []
    
    def _get_recent_data(self, sheet_name: str, recent_months: int) -> List[List]:
        """直近Nヶ月のデータのみを取得（効率的な重複チェック用）"""
        try:
            from datetime import datetime, timedelta
            
            # 直近Nヶ月の開始日を計算
            end_date = datetime.now()
            start_date = end_date - timedelta(days=recent_months * 30)  # 概算で30日/月
            
            logger.info(f"直近{recent_months}ヶ月のデータを取得: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")
            
            # まずヘッダー行を取得
            def _get_headers():
                self._rate_limit_delay()
                range_name = f"{sheet_name}!1:1"
                return self.service.spreadsheets().values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name
                ).execute()
            
            headers_result = self._retry_on_error(_get_headers)
            headers = headers_result.get('values', [[]])[0] if headers_result.get('values') else []
            
            if not headers:
                logger.warning("ヘッダー行が取得できませんでした")
                return []
            
            # Created at列のインデックスを探す
            created_at_index = None
            for i, header in enumerate(headers):
                clean_header = header.strip().lstrip('\ufeff')
                if clean_header == 'Created at':
                    created_at_index = i
                    break
            
            if created_at_index is None:
                logger.warning("Created at列が見つかりません。全データを取得します")
                return self.get_existing_data(sheet_name)
            
            # 日付範囲でフィルタリングされたデータを取得
            # 注：Google Sheetsでは直接的な日付フィルタリングが難しいため、
            # 実際の実装では全データを取得してからフィルタリングするか、
            # または行数ベースの推定を使用する
            
            # 効率的な方法：全行数を確認してから直近の部分のみを取得
            def _get_total_rows():
                self._rate_limit_delay()
                range_name = f"{sheet_name}!A:A"
                return self.service.spreadsheets().values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name
                ).execute()
            
            total_rows_result = self._retry_on_error(_get_total_rows)
            total_rows = len(total_rows_result.get('values', []))
            
            if total_rows <= 1:
                logger.info("データが存在しません")
                return [headers]
            
            # 直近3ヶ月分の推定行数（月1000行と仮定）
            estimated_recent_rows = recent_months * 1000
            start_row = max(2, total_rows - estimated_recent_rows + 1)  # ヘッダー行を除く
            
            logger.info(f"全行数: {total_rows}, 直近{recent_months}ヶ月分の推定開始行: {start_row}")
            
            def _get_recent_rows():
                self._rate_limit_delay()
                # 直近の推定行数のみを取得
                range_name = f"{sheet_name}!A{start_row}:ZZ{total_rows}"
                return self.service.spreadsheets().values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name
                ).execute()
            
            result = self._retry_on_error(_get_recent_rows)
            recent_data = result.get('values', [])
            
            # ヘッダー行を追加
            if recent_data:
                all_data = [headers] + recent_data
            else:
                all_data = [headers]
            
            logger.info(f"直近{recent_months}ヶ月の推定データを取得: {len(all_data)}行（ヘッダー含む）")
            return all_data
            
        except Exception as e:
            logger.error(f"直近データ取得エラー: {e}")
            # エラーの場合は全データを取得
            logger.info("エラーのため全データを取得します")
            return self.get_existing_data(sheet_name)
    
    def _analyze_data_date_range(self, data: List[List]) -> tuple:
        """アップロードするデータの期間を分析
        
        Returns:
            tuple: (start_date, end_date) または (None, None) エラー時
        """
        try:
            if not data or len(data) < 2:
                return None, None
            
            headers = data[0]
            
            # Created at列のインデックスを探す
            created_at_index = None
            for i, header in enumerate(headers):
                clean_header = header.strip().lstrip('\ufeff')
                if clean_header == 'Created at':
                    created_at_index = i
                    break
            
            if created_at_index is None:
                logger.warning("Created at列が見つかりません")
                return None, None
            
            # データ行から日付を抽出
            dates = []
            for row in data[1:]:  # ヘッダー行を除く
                if len(row) > created_at_index and row[created_at_index]:
                    try:
                        # 日付文字列を解析
                        date_str = row[created_at_index].strip()
                        if date_str:
                            # 複数の日付形式に対応
                            from datetime import datetime
                            
                            # 一般的な日付形式を試す
                            date_formats = [
                                '%Y-%m-%d %H:%M:%S',  # 2023-01-01 12:00:00
                                '%Y-%m-%d',           # 2023-01-01
                                '%Y/%m/%d %H:%M:%S',  # 2023/01/01 12:00:00
                                '%Y/%m/%d',           # 2023/01/01
                                '%m/%d/%Y %H:%M:%S',  # 01/01/2023 12:00:00
                                '%m/%d/%Y',           # 01/01/2023
                                '%d/%m/%Y %H:%M:%S',  # 01/01/2023 12:00:00
                                '%d/%m/%Y',           # 01/01/2023
                            ]
                            
                            parsed_date = None
                            for fmt in date_formats:
                                try:
                                    parsed_date = datetime.strptime(date_str, fmt)
                                    break
                                except ValueError:
                                    continue
                            
                            if parsed_date:
                                dates.append(parsed_date)
                    except Exception as e:
                        logger.debug(f"日付解析エラー: {date_str} - {e}")
                        continue
            
            if not dates:
                logger.warning("有効な日付が見つかりません")
                return None, None
            
            # 最小日付と最大日付を取得
            start_date = min(dates)
            end_date = max(dates)
            
            # バッファ期間を追加（前後1日）
            from datetime import timedelta
            start_date = start_date - timedelta(days=1)
            end_date = end_date + timedelta(days=1)
            
            logger.info(f"データ期間分析結果: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')} ({len(dates)}件のデータ)")
            return start_date, end_date
            
        except Exception as e:
            logger.error(f"データ期間分析エラー: {e}")
            return None, None
    
    def _get_data_by_date_range(self, sheet_name: str, start_date, end_date) -> List[List]:
        """指定期間のデータのみを取得（期間ベースの最適化）"""
        try:
            from datetime import datetime, timedelta
            
            logger.info(f"期間指定データ取得: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")
            
            # まずヘッダー行を取得
            def _get_headers():
                self._rate_limit_delay()
                range_name = f"{sheet_name}!1:1"
                return self.service.spreadsheets().values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name
                ).execute()
            
            headers_result = self._retry_on_error(_get_headers)
            headers = headers_result.get('values', [[]])[0] if headers_result.get('values') else []
            
            if not headers:
                logger.warning("ヘッダー行が取得できませんでした")
                return []
            
            # Created at列のインデックスを探す
            created_at_index = None
            for i, header in enumerate(headers):
                clean_header = header.strip().lstrip('\ufeff')
                if clean_header == 'Created at':
                    created_at_index = i
                    break
            
            if created_at_index is None:
                logger.warning("Created at列が見つかりません。全データを取得します")
                return self.get_existing_data(sheet_name)
            
            # 期間の日数を計算して推定行数を算出
            days_diff = (end_date - start_date).days
            estimated_rows = days_diff * 50  # 1日50行と仮定（保守的）
            
            # 全行数を確認
            def _get_total_rows():
                self._rate_limit_delay()
                range_name = f"{sheet_name}!A:A"
                return self.service.spreadsheets().values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name
                ).execute()
            
            total_rows_result = self._retry_on_error(_get_total_rows)
            total_rows = len(total_rows_result.get('values', []))
            
            if total_rows <= 1:
                logger.info("データが存在しません")
                return [headers]
            
            # 期間に基づく推定開始行
            # 注文データは時系列で並んでいるため、期間に該当する可能性のある範囲を広く取得
            # より保守的に、期間の3倍の範囲を取得
            estimated_start_row = max(2, total_rows - estimated_rows * 5)  # 5倍のバッファ
            
            logger.info(f"期間ベース推定: 全行数={total_rows}, 推定開始行={estimated_start_row}, 期間日数={days_diff}日")
            
            # 推定範囲のデータを取得
            def _get_range_data():
                self._rate_limit_delay()
                range_name = f"{sheet_name}!A{estimated_start_row}:ZZ{total_rows}"
                return self.service.spreadsheets().values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name
                ).execute()
            
            result = self._retry_on_error(_get_range_data)
            range_data = result.get('values', [])
            
            logger.info(f"推定範囲から取得したデータ: {len(range_data)}行")
            
            # 取得したデータを期間でフィルタリング
            filtered_data = []
            date_formats = [
                '%Y-%m-%d %H:%M:%S',  # 2023-01-01 12:00:00
                '%Y-%m-%d',           # 2023-01-01
                '%Y/%m/%d %H:%M:%S',  # 2023/01/01 12:00:00
                '%Y/%m/%d',           # 2023/01/01
                '%m/%d/%Y %H:%M:%S',  # 01/01/2023 12:00:00
                '%m/%d/%Y',           # 01/01/2023
                '%d/%m/%Y %H:%M:%S',  # 01/01/2023 12:00:00
                '%d/%m/%Y',           # 01/01/2023
            ]
            
            logger.info(f"期間フィルタリング開始: 対象期間={start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")
            
            for i, row in enumerate(range_data):
                if len(row) > created_at_index and row[created_at_index]:
                    try:
                        date_str = row[created_at_index].strip()
                        if date_str:
                            parsed_date = None
                            for fmt in date_formats:
                                try:
                                    parsed_date = datetime.strptime(date_str, fmt)
                                    break
                                except ValueError:
                                    continue
                            
                            if parsed_date:
                                if start_date <= parsed_date <= end_date:
                                    filtered_data.append(row)
                                # デバッグ用：最初の10行の日付をログ出力
                                if i < 10:
                                    logger.info(f"行{i+1}: {date_str} -> {parsed_date.strftime('%Y-%m-%d')} (範囲内: {start_date <= parsed_date <= end_date})")
                            else:
                                # 日付解析に失敗した場合のデバッグ
                                if i < 10:
                                    logger.warning(f"行{i+1}: 日付解析失敗 - {date_str}")
                    except Exception as e:
                        logger.warning(f"行{i+1}: 日付フィルタリングエラー: {e}")
                        continue
                else:
                    # 日付列が存在しない場合のデバッグ
                    if i < 10:
                        logger.warning(f"行{i+1}: 日付列が存在しない - 行長: {len(row)}, 日付列インデックス: {created_at_index}")
            
            # ヘッダー行を追加
            all_data = [headers] + filtered_data
            
            logger.info(f"期間フィルタリング結果: {len(filtered_data)}行（ヘッダー除く）を取得")
            return all_data
            
        except Exception as e:
            logger.error(f"期間指定データ取得エラー: {e}")
            # エラーの場合は全データを取得
            logger.info("エラーのため全データを取得します")
            return self.get_existing_data(sheet_name)
    
    def _get_data_in_batches(self, sheet_name: str, total_rows: int, batch_size: int = 10000) -> List[List]:
        """大きなデータセットをバッチで分割取得"""
        try:
            all_data = []
            batch_count = (total_rows + batch_size - 1) // batch_size
            
            logger.info(f"バッチ取得開始: {batch_count}バッチ、バッチサイズ: {batch_size}")
            
            for batch_num in range(batch_count):
                start_row = batch_num * batch_size + 1  # 1ベース
                end_row = min((batch_num + 1) * batch_size, total_rows)
                
                def _get_batch():
                    self._rate_limit_delay()
                    range_name = f"{sheet_name}!A{start_row}:ZZ{end_row}"
                    return self.service.spreadsheets().values().get(
                        spreadsheetId=self.spreadsheet_id,
                        range=range_name
                    ).execute()
                
                try:
                    result = self._retry_on_error(_get_batch)
                    batch_data = result.get('values', [])
                    all_data.extend(batch_data)
                    
                    logger.info(f"バッチ {batch_num + 1}/{batch_count} 完了: 行{start_row}-{end_row} ({len(batch_data)}行)")
                    
                    # バッチ間の待機
                    if batch_num < batch_count - 1:
                        time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"バッチ {batch_num + 1} 取得エラー: {e}")
                    # エラーが発生しても次のバッチを続行
                    continue
            
            logger.info(f"バッチ取得完了: 合計 {len(all_data)}行")
            return all_data
            
        except Exception as e:
            logger.error(f"バッチ取得エラー: {e}")
            return []
    
    def find_duplicate_rows(self, existing_data: List[List], new_data: List[List]) -> dict:
        """重複行を検出し、更新対象を特定"""
        if not existing_data or len(existing_data) < 2:
            return {'duplicates': {}, 'next_empty_row': 2}
        
        # ヘッダー行を除く
        existing_rows = existing_data[1:] if len(existing_data) > 1 else []
        new_rows = new_data[1:] if len(new_data) > 1 else []
        
        duplicates = {}
        
        # 注文番号（Name列）での重複チェック
        id_column_index = None
        if existing_data and len(existing_data) > 0:
            headers = existing_data[0]
            try:
                # BOM文字を考慮してName列を検索
                id_column_index = None
                for i, header in enumerate(headers):
                    # BOM文字を除去して比較
                    clean_header = header.strip().lstrip('\ufeff')
                    if clean_header == 'Name':
                        id_column_index = i
                        break
                
                if id_column_index is not None:
                    logger.info("注文番号（Name列）で重複チェックを行います")
                else:
                    raise ValueError("Name列が見つかりません")
                    
            except ValueError:
                logger.warning("Name列が見つかりません。Id列で重複チェックを行います")
                try:
                    id_column_index = headers.index('Id')
                except ValueError:
                    logger.error("重複チェック用の列が見つかりません")
                    return {'duplicates': {}, 'next_empty_row': len(existing_data) + 1}
        
        if id_column_index is not None:
            existing_ids = {}
            for i, row in enumerate(existing_rows):
                if len(row) > id_column_index and row[id_column_index]:
                    existing_ids[row[id_column_index]] = i + 2  # ヘッダー行を考慮して+2
            
            # 重複チェックと新規データの分離
            new_rows_filtered = []
            for new_row in new_rows:
                if len(new_row) > id_column_index and new_row[id_column_index]:
                    order_id = new_row[id_column_index]
                    if order_id in existing_ids:
                        # 重複の場合は上書き対象として記録
                        duplicates[order_id] = {
                            'existing_row': existing_ids[order_id],
                            'new_data': new_row
                        }
                    else:
                        # 新規データとして追加
                        new_rows_filtered.append(new_row)
                else:
                    # 注文番号がない場合は新規データとして追加
                    new_rows_filtered.append(new_row)
            
            # 新規データを更新
            new_rows = new_rows_filtered
        
        next_empty_row = len(existing_data) + 1
        
        logger.info(f"重複検出結果: {len(duplicates)}件の重複、次の空行: {next_empty_row}")
        
        return {
            'duplicates': duplicates,
            'next_empty_row': next_empty_row,
            'filtered_new_rows': new_rows  # フィルタリングされた新規データ
        }
    
    def update_duplicate_rows(self, sheet_name: str, duplicates: dict) -> bool:
        """重複行を更新"""
        try:
            if not duplicates:
                return True
            
            print(f"🔄 {len(duplicates)}件の重複データを更新中...")
            
            # シートIDを取得
            sheet_id = self._get_sheet_id(sheet_name)
            if sheet_id is None:
                print(f"❌ シートIDの取得に失敗しました: {sheet_name}")
                logger.error(f"シートIDの取得に失敗しました: {sheet_name}")
                return False
            
            # バッチ更新のリクエストを準備
            requests = []
            processed_count = 0
            
            for order_id, duplicate_info in duplicates.items():
                row_number = duplicate_info['existing_row']
                new_data = duplicate_info['new_data']
                
                # 行全体を更新
                requests.append({
                    'updateCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_number - 1,
                            'endRowIndex': row_number,
                            'startColumnIndex': 0,
                            'endColumnIndex': len(new_data)
                        },
                        'rows': [{
                            'values': [{'userEnteredValue': {'stringValue': str(cell)}} for cell in new_data]
                        }],
                        'fields': 'userEnteredValue'
                    }
                })
                
                processed_count += 1
                if processed_count % 100 == 0 or processed_count == len(duplicates):
                    print(f"📊 重複更新進捗: {processed_count}/{len(duplicates)} 件処理完了 ({processed_count/len(duplicates)*100:.1f}%)")
            
            # バッチ更新を実行（100件ずつに分割）
            if requests:
                print("📤 重複データの更新を実行中...")
                batch_size = 200
                total_batches = (len(requests) + batch_size - 1) // batch_size
                
                for i in range(0, len(requests), batch_size):
                    batch_requests = requests[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    print(f"📤 バッチ {batch_num}/{total_batches} を実行中... ({len(batch_requests)}件)")
                    
                    try:
                        self._rate_limit_delay()
                        self.service.spreadsheets().batchUpdate(
                            spreadsheetId=self.spreadsheet_id,
                            body={'requests': batch_requests}
                        ).execute()
                        
                        print(f"✅ バッチ {batch_num}/{total_batches} 完了")
                        
                    except Exception as e:
                        print(f"❌ バッチ {batch_num} エラー: {e}")
                        logger.error(f"バッチ {batch_num} エラー: {e}")
                        # エラーが発生しても次のバッチを続行
                        continue
                
                print(f"✅ {len(duplicates)}件の重複データ更新完了")
                logger.info(f"{len(duplicates)}件の重複行を更新しました")
            
            return True
            
        except HttpError as e:
            print(f"❌ 重複行更新エラー: {e}")
            logger.error(f"重複行更新エラー: {e}")
            return False
        except Exception as e:
            print(f"❌ 予期しないエラー: {e}")
            logger.error(f"重複行更新予期しないエラー: {e}")
            return False
    
    def _ensure_sheet_has_rows(self, sheet_name: str, required_rows: int) -> bool:
        """シートに必要な行数があることを確認し、必要に応じて拡張"""
        try:
            # シートのプロパティを取得
            sheet_id = self._get_sheet_id(sheet_name)
            if sheet_id is None:
                return False
            
            # 現在のシート情報を取得
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id,
                fields='sheets.properties'
            ).execute()
            
            current_rows = None
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['sheetId'] == sheet_id:
                    current_rows = sheet['properties'].get('gridProperties', {}).get('rowCount', 0)
                    break
            
            if current_rows is None:
                logger.error(f"シート '{sheet_name}' の情報を取得できませんでした")
                return False
            
            if current_rows >= required_rows:
                return True
            
            # シートを拡張
            print(f"📏 シート '{sheet_name}' を {required_rows} 行に拡張中... (現在: {current_rows} 行)")
            
            requests = [{
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': sheet_id,
                        'gridProperties': {
                            'rowCount': required_rows
                        }
                    },
                    'fields': 'gridProperties.rowCount'
                }
            }]
            
            self._rate_limit_delay()
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={'requests': requests}
            ).execute()
            
            print(f"✅ シート '{sheet_name}' を {required_rows} 行に拡張完了")
            logger.info(f"シート '{sheet_name}' を {required_rows} 行に拡張しました")
            return True
            
        except Exception as e:
            logger.error(f"シート拡張エラー: {e}")
            return False

    def append_new_rows(self, sheet_name: str, new_rows: List[List], start_row: int) -> bool:
        """新規行を追加"""
        try:
            if not new_rows:
                return True
            
            # シートを拡張する必要があるかチェック
            required_rows = start_row + len(new_rows) - 1
            if not self._ensure_sheet_has_rows(sheet_name, required_rows):
                logger.error(f"シートの行数拡張に失敗しました: {required_rows}行必要")
                return False
            
            # 開始行から新規データを追加
            # 列数が多い場合のカラム文字を正しく計算
            col_count = len(new_rows[0])
            if col_count <= 26:
                end_col = chr(ord('A') + col_count - 1)
            else:
                # 26列を超える場合（AA, AB, AC...）
                first_char = chr(ord('A') + (col_count - 1) // 26 - 1)
                second_char = chr(ord('A') + (col_count - 1) % 26)
                end_col = first_char + second_char
            
            range_name = f"{sheet_name}!A{start_row}:{end_col}{start_row + len(new_rows) - 1}"
            
            body = {
                'values': new_rows
            }
            
            result = self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.info(f"{len(new_rows)}件の新規行を追加しました（行{start_row}から）")
            return True
            
        except HttpError as e:
            logger.error(f"新規行追加エラー: {e}")
            return False
    
    def upload_with_duplicate_handling(self, sheet_name: str, data: List[List], overwrite_mode: bool = False) -> bool:
        """重複処理を含むデータアップロード
        
        Args:
            sheet_name: シート名
            data: アップロードするデータ
            overwrite_mode: Trueの場合は重複データを上書き、Falseの場合は追加のみ
        """
        try:
            mode_text = "上書きモード" if overwrite_mode else "追加モード"
            print(f"🔄 重複処理を含むアップロードを開始: シート '{sheet_name}' ({mode_text})")
            logger.info(f"重複処理を含むアップロードを開始: シート '{sheet_name}' ({mode_text})")
            
            # 既存データを取得（シート名に基づいて処理方式を決定）
            if overwrite_mode:
                # 注文データ（dbシート）の場合も全データを取得（タイムアウト対策済み）
                if sheet_name.lower() == 'db':
                    print("📊 注文データの全データを取得中（タイムアウト対策済み）...")
                    existing_data = self.get_existing_data(sheet_name)
                else:
                    # 顧客データ（CustomerDB等）の場合は全データを取得
                    print("📊 顧客データのため全データを取得中...")
                    existing_data = self.get_existing_data(sheet_name)
            else:
                print("📊 既存データを取得中...")
                existing_data = self.get_existing_data(sheet_name)
            
            # 重複チェック
            print("🔍 重複データを検出中...")
            duplicate_info = self.find_duplicate_rows(existing_data, data)
            duplicates = duplicate_info['duplicates']
            next_empty_row = duplicate_info['next_empty_row']
            print(f"📈 重複検出結果: {len(duplicates)}件の重複、次の空行: {next_empty_row}")
            
            # シートが存在しない場合は作成
            if not existing_data:
                if not self.create_new_sheet(sheet_name):
                    return False
                # ヘッダー行を追加
                if not self.upload_data_to_sheet(sheet_name, [data[0]]):
                    return False
                next_empty_row = 2
            
            # 重複行を処理（上書きモードの場合のみ更新）
            if duplicates and overwrite_mode:
                print(f"🔄 {len(duplicates)}件の重複データを更新中...")
                if not self.update_duplicate_rows(sheet_name, duplicates):
                    return False
                print(f"✅ {len(duplicates)}件の重複データ更新完了")
                logger.info(f"上書きモード: {len(duplicates)}件の重複データを更新しました")
            elif duplicates and not overwrite_mode:
                print(f"⏭️ {len(duplicates)}件の重複データはスキップします")
                logger.info(f"追加モード: {len(duplicates)}件の重複データはスキップしました")
            
            # フィルタリングされた新規データを使用
            if 'filtered_new_rows' in duplicate_info:
                new_rows = duplicate_info['filtered_new_rows']
                print(f"📝 新規データ: {len(new_rows)}件（重複除外済み）")
            else:
                # フォールバック: 従来の方法で新規行を抽出
                new_rows = []
                duplicate_order_ids = set(duplicates.keys())
                
                # Id列のインデックスを取得
                id_column_index = None
                if data and len(data) > 0:
                    headers = data[0]
                    try:
                        # BOM文字を考慮してName列を検索
                        id_column_index = None
                        for i, header in enumerate(headers):
                            # BOM文字を除去して比較
                            clean_header = header.strip().lstrip('\ufeff')
                            if clean_header == 'Name':
                                id_column_index = i
                                break
                        
                        if id_column_index is None:
                            # Name列が見つからない場合はId列を試す
                            id_column_index = headers.index('Id')
                    except ValueError:
                        logger.warning("重複チェック用の列が見つかりません")
                
                # 新規行を抽出
                print("📝 新規データを抽出中...")
                total_rows = len(data) - 1  # ヘッダー行を除く
                processed_rows = 0
                
                for i, row in enumerate(data[1:], 1):  # ヘッダー行を除く
                    if id_column_index is not None and len(row) > id_column_index:
                        order_id = row[id_column_index]
                        # 上書きモードの場合は重複データも含める、追加モードの場合は重複を除外
                        if overwrite_mode or order_id not in duplicate_order_ids:
                            new_rows.append(row)
                    else:
                        new_rows.append(row)
                    
                    processed_rows += 1
                    if processed_rows % 100 == 0 or processed_rows == total_rows:
                        print(f"📊 進捗: {processed_rows}/{total_rows} 行処理完了 ({processed_rows/total_rows*100:.1f}%)")
            
            # 新規行を追加
            if new_rows:
                print(f"➕ {len(new_rows)}件の新規データを追加中...")
                if not self.append_new_rows(sheet_name, new_rows, next_empty_row):
                    return False
                print(f"✅ {len(new_rows)}件の新規データ追加完了")
            
            # 書式設定を適用（処理対象範囲のみ）
            print("🎨 書式設定を適用中...")
            
            # 新規データ（追加された行）に書式設定を適用
            if new_rows:
                print(f"🎨 {len(new_rows)}件の新規行に書式設定を適用中...")
                # 新規行の範囲を計算
                start_row = next_empty_row
                end_row = next_empty_row + len(new_rows)
                self._apply_formatting_to_range_fixed(sheet_name, len(data[0]), start_row, end_row)
                print(f"✅ 新規行の書式設定完了")
            
            print("✅ 書式設定完了")
            
            if overwrite_mode:
                print(f"🎉 アップロード完了: {len(duplicates)}件更新, {len(new_rows)}件新規追加")
                logger.info(f"アップロード完了: {len(duplicates)}件更新, {len(new_rows)}件新規追加")
            else:
                print(f"🎉 アップロード完了: {len(new_rows)}件新規追加（{len(duplicates)}件重複スキップ）")
                logger.info(f"アップロード完了: {len(new_rows)}件新規追加（{len(duplicates)}件重複スキップ）")
            return True
            
        except Exception as e:
            logger.error(f"重複処理アップロードエラー: {e}")
            return False
    
    def _get_sheet_id(self, sheet_name: str) -> Optional[int]:
        """シートIDを取得"""
        try:
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    return sheet['properties']['sheetId']
            
            return None
        
        except HttpError as e:
            logger.error(f"シートID取得エラー: {e}")
            return None
    
    def upload_csv_to_spreadsheet(self, csv_filepath: str, sheet_name: str = None, overwrite_mode: bool = False) -> bool:
        """CSVファイルをスプレッドシートにアップロード"""
        try:
            # シート名を決定
            if not sheet_name:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                sheet_name = f"注文データ_{timestamp}"
            
            logger.info(f"CSVファイル '{csv_filepath}' をスプレッドシートにアップロード開始")
            
            # CSVファイルを読み込み
            data = self.read_csv_file(csv_filepath)
            
            if not data:
                logger.warning("CSVファイルにデータがありません")
                return False
            
            # 新しいシートを作成（既に存在する場合は作成されない）
            if not self.create_new_sheet(sheet_name):
                return False
            
            if overwrite_mode:
                # 上書きモード：重複データを更新し、新規データを追加
                logger.info("上書きモード: 重複データを更新し、新規データを追加します")
                if not self.upload_with_duplicate_handling(sheet_name, data, overwrite_mode=True):
                    return False
            else:
                # 通常モード：シートの内容をクリアしてからアップロード
                logger.info("通常モード: シートの内容をクリアしてからアップロードします")
                self.clear_sheet_content(sheet_name)
                if not self.upload_data_to_sheet(sheet_name, data):
                    return False
            
            # シートのフォーマットを設定
            self.format_sheet(sheet_name, len(data[0]))
            
            logger.info(f"CSVファイルのアップロードが完了しました: シート '{sheet_name}'")
            return True
        
        except Exception as e:
            logger.error(f"CSVファイルアップロードエラー: {e}")
            return False
    
    def upload_customer_db(self, csv_file_path: str, sheet_name: str = "CustomerDB") -> bool:
        """顧客データベースをスプレッドシートにアップロード"""
        try:
            logger.info(f"顧客データベース '{csv_file_path}' をスプレッドシートにアップロード開始")
            
            # CSVファイルを読み込み
            data = self.read_csv_file(csv_file_path)
            
            if not data:
                logger.warning("CSVファイルにデータがありません")
                return False
            
            # シートが存在しない場合は作成
            try:
                self.service.spreadsheets().get(
                    spreadsheetId=self.spreadsheet_id,
                    ranges=f"{sheet_name}!A1"
                ).execute()
                logger.info(f"シート '{sheet_name}' は既に存在します")
            except:
                # シートが存在しない場合は作成
                if not self.create_new_sheet(sheet_name):
                    return False
                logger.info(f"シート '{sheet_name}' を作成しました")
            
            # シートの内容をクリア
            self.clear_sheet_content(sheet_name)
            
            # データをアップロード
            if not self.upload_data_to_sheet(sheet_name, data):
                return False
            
            # シートのフォーマットを設定
            self.format_sheet(sheet_name, len(data[0]))
            
            logger.info(f"顧客データベースのアップロードが完了しました: シート '{sheet_name}'")
            return True
            
        except Exception as e:
            logger.error(f"顧客データベースアップロードエラー: {e}")
            return False
    
    def get_spreadsheet_url(self) -> str:
        """スプレッドシートのURLを取得"""
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"
    
    def read_csv_file(self, csv_filepath: str) -> List[List[str]]:
        """CSVファイルを読み込んでリスト形式で返す"""
        try:
            import csv
            
            data = []
            with open(csv_filepath, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    data.append(row)
            
            return data
            
        except Exception as e:
            print(f"CSVファイル読み込みエラー: {e}")
            logger.error(f"CSVファイル読み込みエラー: {e}")
            return []

def main():
    """メイン実行関数"""
    try:
        # コマンドライン引数を解析
        if len(sys.argv) < 2:
            print("使用方法: python spreadsheet_uploader.py <CSVファイルパス> [シート名] [--overwrite]")
            print("例: python spreadsheet_uploader.py exports/orders_20250101.csv '注文データ' --overwrite")
            return
        
        csv_filepath = sys.argv[1]
        sheet_name = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith('--') else "db"
        overwrite_mode = '--overwrite' in sys.argv
        
        # ファイルの存在確認
        if not os.path.exists(csv_filepath):
            print(f"❌ CSVファイルが見つかりません: {csv_filepath}")
            return
        
        print(f"📁 アップロード対象ファイル: {os.path.basename(csv_filepath)}")
        if overwrite_mode:
            print("🔄 上書きモード: 重複データを更新し、新規データを追加します")
        else:
            print("🆕 通常モード: シートの内容をクリアしてからアップロードします")
        
        # スプレッドシートアップローダーを初期化
        uploader = SpreadsheetUploader()
        
        # CSVファイルをスプレッドシートにアップロード
        if uploader.upload_csv_to_spreadsheet(csv_filepath, sheet_name, overwrite_mode):
            print(f"✅ CSVファイルのアップロードが完了しました")
            print(f"📊 シート名: {sheet_name}")
            print(f"🔗 スプレッドシートURL: {uploader.get_spreadsheet_url()}")
        else:
            print("❌ CSVファイルのアップロードに失敗しました")
            
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")

if __name__ == "__main__":
    main()
