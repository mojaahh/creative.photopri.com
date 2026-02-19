#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
週次サマリー生成システム
スプレッドシートからデータを取得してサマリーを生成
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('summary_generator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SummaryGenerator:
    """週次サマリー生成クラス"""
    
    def __init__(self):
        """初期化"""
        # ルートディレクトリの.envファイルを読み込み
        load_dotenv('../.env')
        
        # スプレッドシートID
        self.spreadsheet_id = "1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g"
        
        # Google Sheets API設定
        self.setup_google_sheets()
    
    def setup_google_sheets(self):
        """Google Sheets APIの設定"""
        try:
            credentials_path = '../credentials.json'
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"認証ファイルが見つかりません: {credentials_path}")
            
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path, 
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            self.sheets_service = build('sheets', 'v4', credentials=self.credentials, cache_discovery=False)
            logger.info("Google Sheets API認証が完了しました")
            
        except Exception as e:
            logger.error(f"Google Sheets API認証エラー: {e}")
            raise
    
    def get_spreadsheet_data(self, sheet_name: str, range_name: str = None) -> Optional[pd.DataFrame]:
        """スプレッドシートからデータを取得"""
        try:
            if not range_name:
                range_name = f"{sheet_name}!A:Z"
            
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                logger.warning(f"スプレッドシート '{sheet_name}' にデータが見つかりません")
                return None
            
            # ヘッダー行を取得
            headers = values[0]
            data = values[1:]
            
            # 各行の長さをヘッダーに合わせる
            max_cols = len(headers)
            normalized_data = []
            for row in data:
                normalized_row = row[:max_cols] + [''] * (max_cols - len(row))
                normalized_data.append(normalized_row)
            
            # DataFrameに変換
            df = pd.DataFrame(normalized_data, columns=headers)
            logger.info(f"スプレッドシートから {len(df)} 件のデータを取得しました: {sheet_name}")
            return df
            
        except HttpError as e:
            logger.error(f"スプレッドシート取得エラー: {e}")
            return None
        except Exception as e:
            logger.error(f"データ取得エラー: {e}")
            return None
    
    def get_monthly_targets(self, current_month: int) -> Dict:
        """月間目標売上を取得"""
        try:
            # PlanInputシートからデータを取得
            df = self.get_spreadsheet_data("PlanInput")
            if df is None:
                logger.warning("PlanInputシートのデータが取得できませんでした")
                return {}
            
            # 列名を確認
            logger.info(f"PlanInputシートの列: {list(df.columns)}")
            
            # 月間目標を計算
            targets = {}
            
            # 現在の月のデータをフィルタリング（年月列から）
            current_year = datetime.now().year
            monthly_data = df[df.iloc[:, 0].astype(str).str.contains(f"{current_year}/{current_month:02d}", na=False)]
            
            # サービス別の目標を計算（サービス列が#A,#P,#E,#Qの場合の売上列の合計）
            services = ['#A', '#P', '#E', '#Q']
            total_target = 0
            
            for service in services:
                service_data = monthly_data[monthly_data.iloc[:, 1].astype(str).str.contains(service, na=False)]
                if not service_data.empty and len(service_data.columns) > 6:
                    # 売上列（6列目）の値を取得し、数値に変換
                    try:
                        service_target = 0
                        for _, row in service_data.iterrows():
                            target_value = str(row.iloc[6]).replace(',', '').replace('¥', '').strip()
                            if target_value and target_value != 'nan':
                                service_target += float(target_value)
                        
                        targets[service] = service_target
                        total_target += service_target
                        logger.info(f"{service}の目標売上: {service_target:,.0f}円")
                    except Exception as e:
                        logger.warning(f"{service}の目標売上取得エラー: {e}")
                        targets[service] = 0
            
            targets['total'] = total_target
            logger.info(f"月間目標売上合計: {total_target:,.0f}円")
            
            return targets
            
        except Exception as e:
            logger.error(f"月間目標取得エラー: {e}")
            return {}
    
    def get_monthly_sales(self, current_month: int) -> Dict:
        """月間売上を取得"""
        try:
            # ServiceDataシートからデータを取得
            df = self.get_spreadsheet_data("ServiceData")
            if df is None:
                logger.warning("ServiceDataシートのデータが取得できませんでした")
                return {}
            
            # 列名を確認
            logger.info(f"ServiceDataシートの列: {list(df.columns)}")
            
            # 月間売上を計算
            sales = {}
            
            # 現在の月のデータをフィルタリング（年月列から）
            current_year = datetime.now().year
            monthly_data = df[df.iloc[:, 0].astype(str).str.contains(f"{current_year}/{current_month:02d}", na=False)]
            
            # サービス別の売上を計算（サービス列が#A,#P,#E,#Qの場合の売上列の合計）
            services = ['#A', '#P', '#E', '#Q']
            total_sales = 0
            total_orders = 0
            
            for service in services:
                service_data = monthly_data[monthly_data.iloc[:, 1].astype(str).str.contains(service, na=False)]
                if not service_data.empty and len(service_data.columns) > 2:
                    # 売上列（2列目）の値を取得し、数値に変換
                    try:
                        service_amount = 0
                        service_orders = 0
                        for _, row in service_data.iterrows():
                            amount_value = str(row.iloc[2]).replace(',', '').replace('¥', '').strip()
                            orders_value = str(row.iloc[3]).replace(',', '').strip()
                            
                            if amount_value and amount_value != 'nan':
                                service_amount += float(amount_value)
                            if orders_value and orders_value != 'nan':
                                service_orders += int(float(orders_value))
                        
                        sales[service] = {
                            'amount': service_amount,
                            'orders': service_orders
                        }
                        total_sales += service_amount
                        total_orders += service_orders
                        logger.info(f"{service}の売上: {service_amount:,.0f}円 ({service_orders}件)")
                    except Exception as e:
                        logger.warning(f"{service}の売上取得エラー: {e}")
                        sales[service] = {'amount': 0, 'orders': 0}
            
            sales['total'] = {
                'amount': total_sales,
                'orders': total_orders
            }
            logger.info(f"月間売上合計: {total_sales:,.0f}円 ({total_orders}件)")
            
            return sales
            
        except Exception as e:
            logger.error(f"月間売上取得エラー: {e}")
            return {}
    
    def get_weekend_orders(self) -> Dict:
        """週末注文を取得"""
        try:
            # 正しい週末期間を計算（前週金曜日12:00〜今週月曜日9:00）
            today = datetime.now()
            # 今週の月曜日を取得
            this_week_monday = today - timedelta(days=today.weekday())
            # 前週の金曜日を取得
            last_week_friday = this_week_monday - timedelta(days=3)
            
            weekend_start = last_week_friday.replace(hour=12, minute=0, second=0, microsecond=0)
            weekend_end = today.replace(hour=9, minute=0, second=0, microsecond=0)
            
            logger.info(f"週末注文取得期間: {weekend_start} 〜 {weekend_end}")
            
            # dbシートからデータを取得（実際の注文データ）
            df = self.get_spreadsheet_data("db")
            if df is None:
                logger.warning("dbシートのデータが取得できませんでした")
                return {}
            
            logger.info(f"dbシートの列: {list(df.columns)}")
            
            # 日付列を確認（通常は作成日時や注文日時）
            date_columns = []
            for i, col in enumerate(df.columns):
                if any(keyword in str(col).lower() for keyword in ['date', 'created', 'order', '日時', '日付', 'created_at', 'updated_at']):
                    date_columns.append(i)
                    logger.info(f"日付列候補: {i} - {col}")
            
            if not date_columns:
                logger.warning("日付列が見つかりませんでした")
                return {}
            
            # 週末期間のデータをフィルタリング
            weekend_orders = []
            for _, row in df.iterrows():
                for col_idx in date_columns:
                    try:
                        date_str = str(row.iloc[col_idx])
                        if date_str and date_str != 'nan':
                            # 日付をパース
                            order_date = pd.to_datetime(date_str, errors='coerce')
                            if pd.notna(order_date) and weekend_start <= order_date <= weekend_end:
                                weekend_orders.append(row)
                                break
                    except:
                        continue
            
            if not weekend_orders:
                logger.info("週末期間の注文が見つかりませんでした")
                return {}
            
            weekend_df = pd.DataFrame(weekend_orders)
            logger.info(f"週末注文数: {len(weekend_df)}件")
            
            # サービス別の売上を計算
            orders = {}
            services = ['#A', '#P', '#E', '#Q']
            total_amount = 0
            total_count = 0  # #A, #P, #E, #Qのみの注文数を集計
            
            # 売上列を確認（Total列）
            amount_columns = [11]  # Total列
            logger.info(f"売上列: {amount_columns[0]} - {df.columns[11]}")
            
            # サービス列を確認（Name列にサービス情報が含まれている）
            service_columns = [0]  # Name列
            logger.info(f"サービス列: {service_columns[0]} - {df.columns[0]}")
            
            for service in services:
                service_amount = 0
                service_count = 0
                
                for _, row in weekend_df.iterrows():
                    # サービス列でフィルタリング
                    for service_col in service_columns:
                        if service in str(row.iloc[service_col]):
                            service_count += 1
                            # 売上列から金額を取得
                            for amount_col in amount_columns:
                                try:
                                    amount_str = str(row.iloc[amount_col]).replace(',', '').replace('¥', '').strip()
                                    if amount_str and amount_str != 'nan':
                                        service_amount += float(amount_str)
                                        break
                                except:
                                    continue
                            break
                
                orders[service] = {
                    'amount': service_amount,
                    'orders': service_count
                }
                total_amount += service_amount
                total_count += service_count  # 各サービスの注文数を合計に追加
                logger.info(f"{service}の週末売上: {service_amount:,.0f}円 ({service_count}件)")
            
            orders['total'] = {
                'amount': total_amount,
                'orders': total_count
            }
            logger.info(f"週末売上合計: {total_amount:,.0f}円 ({total_count}件)")
            
            return orders
            
        except Exception as e:
            logger.error(f"週末注文取得エラー: {e}")
            return {}
    
    def generate_weekly_summary(self) -> Dict:
        """週次サマリーを生成"""
        try:
            today = datetime.now()
            current_month = today.month
            
            logger.info(f"週次サマリー生成開始: {current_month}月")
            
            # 各データを取得
            monthly_targets = self.get_monthly_targets(current_month)
            monthly_sales = self.get_monthly_sales(current_month)
            weekend_orders = self.get_weekend_orders()
            
            # サマリーデータを構築
            summary = {
                'monthly_targets': monthly_targets,
                'monthly_sales': monthly_sales,
                'weekend_orders': weekend_orders,
                'generated_at': today.isoformat(),
                'month': current_month,
                'year': today.year
            }
            
            logger.info("週次サマリー生成完了")
            return summary
            
        except Exception as e:
            logger.error(f"週次サマリー生成エラー: {e}")
            return {}
    
    def save_summary_to_file(self, summary: Dict, filename: str = None) -> str:
        """サマリーをファイルに保存"""
        try:
            if not filename:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"weekly_summary_{timestamp}.json"
            
            filepath = os.path.join('data', filename)
            os.makedirs('data', exist_ok=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                import json
                json.dump(summary, f, ensure_ascii=False, indent=2)
            
            logger.info(f"サマリーをファイルに保存しました: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"サマリーファイル保存エラー: {e}")
            return ""
    
    def format_weekly_summary(self, summary_data: Dict) -> str:
        """週次サマリーをフォーマットして文字列として返す"""
        try:
            if not summary_data:
                return "❌ サマリーデータがありません"
            
            # 基本情報
            year = summary_data.get('year', datetime.now().year)
            month = summary_data.get('month', datetime.now().month)
            generated_at = summary_data.get('generated_at', datetime.now().isoformat())
            
            # 正しい週次期間を計算（前週月曜日〜日曜日）
            today = datetime.now()
            # 今週の月曜日を取得
            this_week_monday = today - timedelta(days=today.weekday())
            # 前週の月曜日を取得
            last_week_monday = this_week_monday - timedelta(days=7)
            # 前週の日曜日を取得
            last_week_sunday = last_week_monday + timedelta(days=6)
            # 前週の金曜日を取得（週末注文期間用）
            last_week_friday = this_week_monday - timedelta(days=3)
            
            # サマリー文字列を構築
            summary_text = f"📈 {last_week_monday.year}年{last_week_monday.month}月{last_week_monday.day}日〜{last_week_sunday.month}月{last_week_sunday.day}日ウィークリーサマリー\n\n"
            
            # 月間目標売上
            targets = summary_data.get('monthly_targets', {})
            if targets:
                summary_text += "【{month}月の目標売上】\n".format(month=month)
                total_target = targets.get('total', 0)
                summary_text += f"全体：{total_target:,.0f}円\n"
                
                for service in ['#P', '#E', '#A', '#Q']:
                    if service in targets:
                        summary_text += f"{service}：{targets[service]:,.0f}円\n"
                summary_text += "\n"
            
            # 月間売上実績
            sales = summary_data.get('monthly_sales', {})
            if sales:
                summary_text += f"【本日時点での{month}月売上＆注文件数】\n"
                
                total_sales = sales.get('total', {})
                total_amount = total_sales.get('amount', 0)
                total_orders = total_sales.get('orders', 0)
                total_target = targets.get('total', 0)
                achievement_rate = (total_amount / total_target * 100) if total_target > 0 else 0
                
                summary_text += f"全体：{total_amount:,.0f}円 - {achievement_rate:.1f}%({total_orders}件)\n"
                
                for service in ['#P', '#E', '#A', '#Q']:
                    if service in sales:
                        service_data = sales[service]
                        service_amount = service_data.get('amount', 0)
                        service_orders = service_data.get('orders', 0)
                        service_target = targets.get(service, 0)
                        service_rate = (service_amount / service_target * 100) if service_target > 0 else 0
                        
                        summary_text += f"{service}：{service_amount:,.0f}円 - {service_rate:.1f}%({service_orders}件)\n"
                summary_text += "\n"
            
            # 週末注文
            weekend = summary_data.get('weekend_orders', {})
            if weekend:
                # 週末期間の説明を動的に生成
                weekend_start_str = f"{last_week_friday.month}月{last_week_friday.day}日"
                weekend_end_str = f"{today.month}月{today.day}日"
                summary_text += f"【週末({weekend_start_str}12時〜{weekend_end_str}9時)の注文】\n"
                
                weekend_total = weekend.get('total', {})
                weekend_amount = weekend_total.get('amount', 0)
                weekend_orders = weekend_total.get('orders', 0)
                
                summary_text += f"全体：{weekend_amount:,.0f}円({weekend_orders}件)\n"
                
                for service in ['#P', '#E', '#A', '#Q']:
                    if service in weekend:
                        service_data = weekend[service]
                        service_amount = service_data.get('amount', 0)
                        service_orders = service_data.get('orders', 0)
                        
                        summary_text += f"{service}：{service_amount:,.0f}円({service_orders}件)\n"
            
            return summary_text
            
        except Exception as e:
            logger.error(f"サマリーフォーマットエラー: {e}")
            return f"❌ サマリーフォーマットエラー: {e}"

def main():
    """テスト用メイン関数"""
    try:
        generator = SummaryGenerator()
        
        # 週次サマリーを生成
        summary = generator.generate_weekly_summary()
        
        if summary:
            print("✅ 週次サマリー生成成功")
            print(f"📊 生成日時: {summary.get('generated_at', 'Unknown')}")
            print(f"📅 対象月: {summary.get('year', 'Unknown')}年{summary.get('month', 'Unknown')}月")
            
            # 月間目標
            targets = summary.get('monthly_targets', {})
            if targets:
                print(f"\n🎯 月間目標売上:")
                print(f"  全体: {targets.get('total', 0):,.0f}円")
                for service in ['#P', '#E', '#A', '#Q']:
                    if service in targets:
                        print(f"  {service}: {targets[service]:,.0f}円")
            
            # 月間売上
            sales = summary.get('monthly_sales', {})
            if sales:
                print(f"\n💰 月間売上:")
                total_sales = sales.get('total', {})
                print(f"  全体: {total_sales.get('amount', 0):,.0f}円 ({total_sales.get('orders', 0)}件)")
                for service in ['#P', '#E', '#A', '#Q']:
                    if service in sales:
                        service_data = sales[service]
                        print(f"  {service}: {service_data.get('amount', 0):,.0f}円 ({service_data.get('orders', 0)}件)")
            
            # 週末注文
            weekend = summary.get('weekend_orders', {})
            if weekend:
                print(f"\n📅 週末注文:")
                total_weekend = weekend.get('total', {})
                print(f"  全体: {total_weekend.get('amount', 0):,.0f}円 ({total_weekend.get('orders', 0)}件)")
                for service in ['#P', '#E', '#A', '#Q']:
                    if service in weekend:
                        service_data = weekend[service]
                        print(f"  {service}: {service_data.get('amount', 0):,.0f}円 ({service_data.get('orders', 0)}件)")
            
            # ファイルに保存
            filepath = generator.save_summary_to_file(summary)
            if filepath:
                print(f"\n💾 サマリーをファイルに保存: {filepath}")
        else:
            print("❌ 週次サマリー生成失敗")
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        logger.error(f"メイン実行エラー: {e}")

if __name__ == "__main__":
    main()
