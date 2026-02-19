#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
祝日対応用カスタムレポートスクリプト
期間: 2026/01/09 12:00 〜 2026/01/13 09:00
"""

import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from core.weekly_scheduler import WeeklyScheduler
from core.summary_generator import SummaryGenerator

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CustomSummaryGenerator(SummaryGenerator):
    """祝日対応用カスタムサマリー生成クラス"""
    
    def get_weekend_orders(self) -> dict:
        """週末注文を取得（祝日対応期間）"""
        try:
            # 期間設定: 2026/01/09 12:00 〜 2026/01/13 09:00
            weekend_start = datetime(2026, 1, 9, 12, 0, 0)
            weekend_end = datetime(2026, 1, 13, 9, 0, 0)
            
            logger.info(f"祝日期間注文取得: {weekend_start} 〜 {weekend_end}")
            
            # dbシートからデータを取得
            df = self.get_spreadsheet_data("db")
            if df is None:
                return {}
            
            # 日付列特定
            date_columns = []
            for i, col in enumerate(df.columns):
                if any(keyword in str(col).lower() for keyword in ['date', 'created', 'order', '日時', '日付']):
                    date_columns.append(i)
            
            if not date_columns:
                return {}
            
            # 期間フィルタリング
            weekend_orders = []
            for _, row in df.iterrows():
                for col_idx in date_columns:
                    try:
                        date_str = str(row.iloc[col_idx])
                        if date_str and date_str != 'nan':
                            order_date = pd.to_datetime(date_str, errors='coerce')
                            if pd.notna(order_date) and weekend_start <= order_date <= weekend_end:
                                weekend_orders.append(row)
                                break
                    except:
                        continue
            
            weekend_df = pd.DataFrame(weekend_orders)
            logger.info(f"期間内注文数: {len(weekend_df)}件")
            
            # 集計ロジック（親クラスと同様）
            orders = {}
            services = ['#A', '#P', '#E', '#Q']
            total_amount = 0
            total_count = 0
            
            amount_columns = [11]  # Total列
            service_columns = [0]  # Name列
            
            for service in services:
                service_amount = 0
                service_count = 0
                
                for _, row in weekend_df.iterrows():
                    for service_col in service_columns:
                        if service in str(row.iloc[service_col]):
                            service_count += 1
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
                total_count += service_count
            
            orders['total'] = {
                'amount': total_amount,
                'orders': total_count
            }
            
            return orders
            
        except Exception as e:
            logger.error(f"期間注文取得エラー: {e}")
            return {}

    def format_weekly_summary(self, summary_data: dict) -> str:
        """サマリーフォーマット（祝日対応版）"""
        try:
            if not summary_data:
                return "❌ サマリーデータがありません"
            
            # 基本情報
            month = summary_data.get('month', datetime.now().month)
            
            # カスタム期間の表示
            report_start = datetime(2026, 1, 5) # 前週月曜
            report_end = datetime(2026, 1, 12)  # 前週日曜（祝日月曜の前日だが、ウィークリーとしては日曜までが一般的？ユーザー要望は週末集計の変更なので、ウィークリー全体は通常通りか、変則か？
            # ユーザー要望: "2025/01/9のお昼〜13の9時までで実行" refers to the "weekend" check portion usually.
            # Usually weekly report covers Mon-Sun.
            # weekend orders cover Fri 12:00 - Mon 09:00.
            # Here we replace weekend orders with Thu 12:00? No Jan 9 is Friday. 
            # Jan 9 (Fri) 12:00 - Jan 13 (Tue) 09:00.
            # So it's just an extended weekend check.
            
            last_week_monday = datetime(2026, 1, 6) # Wait, Jan 5 is Mon.
            # 2026 Jan 5 (Mon) - Jan 11 (Sun) is the standard week?
            # Or does the user want the report to cover up to Jan 13?
            # "今回祝日があったので2025/01/9のお昼〜13の9時までで実行してもらいたい"
            # likely refers to the "weekend sales" part of the report. The monthly stats usually use "current month data".
            
            summary_text = f"📈 2026年変則ウィークリーサマリー\n\n"
            
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
            
            # 週末注文（変則期間）
            weekend = summary_data.get('weekend_orders', {})
            if weekend:
                summary_text += f"【週末・祝日変則期間(1/9 12時〜1/13 9時)の注文】\n"
                
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
            return f"❌ フォーマットエラー: {e}"

def main():
    parser = argparse.ArgumentParser(description='祝日対応レポート実行')
    parser.add_argument('--no-notify', action='store_true', help='通知をスキップ')
    parser.add_argument('--skip-data-update', action='store_true', help='データ更新をスキップ')
    args = parser.parse_args()
    
    logger.info("🚀 祝日対応レポート処理を開始します")
    
    scheduler = WeeklyScheduler()
    
    # 1. 注文データの更新
    if not args.skip_data_update:
        logger.info("📊 注文データの更新を開始")
        if scheduler._update_order_data():
            logger.info("✅ 注文データの更新完了")
        else:
            logger.warning("⚠️ 注文データの更新に失敗（継続します）")
    
    # 2. サマリー生成
    generator = CustomSummaryGenerator()
    summary_data = generator.generate_weekly_summary()
    summary_text = generator.format_weekly_summary(summary_data)
    
    # 3. 通知または表示
    if args.no_notify:
        print("\n" + "=" * 60)
        print("📊 生成されたサマリー (通知なし)")
        print("=" * 60)
        print(summary_text)
        print("=" * 60)
    else:
        logger.info("📤 Larkへ通知を送信中...")
        if scheduler.lark_webhook_notifier.send_message(summary_text):
            logger.info("✅ 送信完了")
        else:
            logger.error("❌ 送信失敗")

if __name__ == "__main__":
    main()
