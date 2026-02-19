#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
離脱者アラート管理スクリプト
中額顧客以上の離脱者を検出し、アラートリストを作成
"""

import os
import sys
import csv
import logging
from datetime import datetime, timedelta
from core.spreadsheet_uploader import SpreadsheetUploader

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('churn_alert_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ChurnAlertManager:
    """離脱者アラート管理クラス"""
    
    def __init__(self):
        """初期化"""
        self.spreadsheet_uploader = SpreadsheetUploader()
        self.spreadsheet_id = "1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g"
        
        # 離脱判定の閾値設定（動的判定に変更）
        self.churn_thresholds = {
            'high_value': {
                'min_amount': 100000,  # 10万円以上
                'min_orders': 5,
                'risk_score_threshold': 60  # リスクスコア60以上で離脱候補
            },
            'medium_value': {
                'min_amount': 50000,   # 5万円以上
                'min_orders': 3,
                'risk_score_threshold': 50  # リスクスコア50以上で離脱候補
            },
            'frequent_buyer': {
                'min_amount': 30000,   # 3万円以上
                'min_orders': 10,
                'risk_score_threshold': 40  # リスクスコア40以上で離脱候補
            }
        }
    
    def load_customer_data(self):
        """顧客データを読み込む"""
        try:
            # 最新の顧客データCSVファイルを検索
            exports_dir = 'exports'
            if not os.path.exists(exports_dir):
                logger.error("exportsディレクトリが見つかりません")
                return []
            
            customer_files = [f for f in os.listdir(exports_dir) if f.startswith('customers_all_') and f.endswith('.csv')]
            if not customer_files:
                logger.error("顧客データファイルが見つかりません")
                return []
            
            # 最新のファイルを選択
            latest_file = max(customer_files, key=lambda x: os.path.getctime(os.path.join(exports_dir, x)))
            filepath = os.path.join(exports_dir, latest_file)
            
            logger.info(f"顧客データを読み込み中: {latest_file}")
            
            customers = []
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    customers.append(row)
            
            logger.info(f"顧客データ読み込み完了: {len(customers)}件")
            return customers
            
        except Exception as e:
            logger.error(f"顧客データ読み込みエラー: {e}")
            return []
    
    def calculate_days_since_last_order(self, last_order_date_str):
        """最終注文日からの経過日数を計算"""
        if not last_order_date_str:
            return 9999
        
        try:
            if '/' in last_order_date_str:
                last_order_dt = datetime.strptime(last_order_date_str.split(' ')[0], '%Y/%m/%d')
            else:
                last_order_dt = datetime.fromisoformat(last_order_date_str.replace('Z', '+00:00'))
            
            days_since = (datetime.now() - last_order_dt).days
            return max(0, days_since)
        except:
            return 9999
    
    def classify_customer_value(self, total_amount, total_orders):
        """顧客価値を分類"""
        if total_amount >= 500000:
            return "VIP顧客"
        elif total_amount >= 200000:
            return "高額顧客"
        elif total_amount >= 100000:
            return "中額顧客"
        elif total_amount >= 50000:
            return "中低額顧客"
        elif total_amount >= 10000:
            return "低額顧客"
        else:
            return "新規顧客"
    
    def calculate_churn_risk_score(self, days_since_last_order, avg_order_interval, total_orders, total_amount):
        """離脱リスクスコアを計算（0-100、高いほどリスク大）"""
        risk_score = 0
        
        # 平均注文間隔を考慮した動的な判定
        if avg_order_interval > 0:
            # 平均注文間隔の1.5倍を超えた場合にリスク開始
            threshold_days = avg_order_interval * 1.5
            
            if days_since_last_order > threshold_days * 2:  # 平均間隔の3倍
                risk_score += 50
            elif days_since_last_order > threshold_days * 1.5:  # 平均間隔の2.25倍
                risk_score += 30
            elif days_since_last_order > threshold_days:  # 平均間隔の1.5倍
                risk_score += 15
        else:
            # 平均注文間隔が不明な場合の固定閾値
            if days_since_last_order > 365:
                risk_score += 50
            elif days_since_last_order > 180:
                risk_score += 30
            elif days_since_last_order > 90:
                risk_score += 15
            elif days_since_last_order > 60:
                risk_score += 5
        
        # 注文数の少なさによるリスク（長期間の顧客は軽減）
        if total_orders < 3:
            risk_score += 20
        elif total_orders < 5:
            risk_score += 10
        elif total_orders >= 10:  # 長期間の顧客はリスク軽減
            risk_score = max(0, risk_score - 10)
        
        # 購入金額による調整（高額顧客ほど離脱リスクを重視）
        if total_amount >= 200000:
            risk_score += 10
        elif total_amount >= 100000:
            risk_score += 5
        
        # 長期間の顧客（2年以上）はリスク軽減
        if total_orders >= 5 and avg_order_interval > 0:
            customer_lifespan = avg_order_interval * total_orders
            if customer_lifespan > 730:  # 2年以上
                risk_score = max(0, risk_score - 15)
        
        return min(100, risk_score)
    
    def is_churn_candidate(self, customer):
        """離脱候補かどうかを判定（動的リスクスコアベース）"""
        try:
            total_amount = float(customer.get('Total Service Amount', 0) or 0)
            total_orders = int(customer.get('Total Service Orders', 0) or 0)
            last_order_date = customer.get('Last Order Date', '')
            avg_order_interval = float(customer.get('Average Order Interval (Days)', 0) or 0)
            
            # 最終注文からの経過日数
            days_since_last_order = self.calculate_days_since_last_order(last_order_date)
            
            # リスクスコアを計算
            risk_score = self.calculate_churn_risk_score(
                days_since_last_order, avg_order_interval, total_orders, total_amount
            )
            
            # 各カテゴリの離脱判定（リスクスコアベース）
            for category, thresholds in self.churn_thresholds.items():
                if (total_amount >= thresholds['min_amount'] and 
                    total_orders >= thresholds['min_orders'] and
                    risk_score >= thresholds['risk_score_threshold']):
                    return True, category, days_since_last_order, risk_score
            
            return False, None, days_since_last_order, risk_score
            
        except Exception as e:
            logger.warning(f"離脱判定エラー: {e}")
            return False, None, 9999, 0
    
    def create_churn_alert_list(self):
        """離脱者アラートリストを作成"""
        try:
            logger.info("離脱者アラートリスト作成を開始")
            
            # 顧客データを読み込み
            customers = self.load_customer_data()
            if not customers:
                logger.error("顧客データが取得できませんでした")
                return False
            
            # 離脱候補を抽出
            churn_candidates = []
            
            for customer in customers:
                is_churn, category, days_since, risk_score = self.is_churn_candidate(customer)
                
                if is_churn:
                    # 基本データの取得
                    total_amount = float(customer.get('Total Service Amount', 0) or 0)
                    total_orders = int(customer.get('Total Service Orders', 0) or 0)
                    avg_order_value = float(customer.get('Average Order Value', 0) or 0)
                    avg_order_interval = float(customer.get('Average Order Interval (Days)', 0) or 0)
                    first_order_date = customer.get('First Order Date', '')
                    last_order_date = customer.get('Last Order Date', '')
                    
                    # 顧客価値分類
                    customer_value = self.classify_customer_value(total_amount, total_orders)
                    
                    # 離脱リスクスコア（既に計算済み）
                    churn_risk_score = risk_score
                    
                    # 優先度計算（金額×リスクスコア）
                    priority_score = total_amount * (churn_risk_score / 100)
                    
                    # アラートレベル
                    if churn_risk_score >= 80:
                        alert_level = "緊急"
                    elif churn_risk_score >= 60:
                        alert_level = "高"
                    elif churn_risk_score >= 40:
                        alert_level = "中"
                    else:
                        alert_level = "低"
                    
                    # 推奨アクション
                    if category == 'high_value':
                        recommended_action = "VIP顧客向け特別キャンペーン、個別フォローアップ"
                    elif category == 'medium_value':
                        recommended_action = "リターンキャンペーン、メール配信"
                    elif category == 'frequent_buyer':
                        recommended_action = "頻繁購入者向け特典、購入促進メール"
                    else:
                        recommended_action = "一般的なリターンキャンペーン"
                    
                    churn_candidate = {
                        # 基本情報
                        '顧客ID': customer.get('Customer ID', ''),
                        '名': customer.get('First Name', ''),
                        '姓': customer.get('Last Name', ''),
                        'メールアドレス': customer.get('Email', ''),
                        '電話番号': customer.get('Phone', ''),
                        'ストア名': customer.get('Store Name', ''),
                        
                        # 購入履歴
                        '総購入金額': total_amount,
                        '総注文数': total_orders,
                        '平均注文単価': avg_order_value,
                        '平均注文間隔（日）': avg_order_interval,
                        '初回注文日': first_order_date,
                        '最終注文日': last_order_date,
                        '最終注文からの経過日数': days_since,
                        
                        # 分析指標
                        '顧客価値カテゴリ': customer_value,
                        '離脱カテゴリ': category,
                        '離脱リスクスコア': churn_risk_score,
                        '優先度スコア': priority_score,
                        'アラートレベル': alert_level,
                        '推奨アクション': recommended_action,
                        
                        # 追加情報
                        '地域': customer.get('Default Address Province Code', ''),
                        'タグ': customer.get('Tags', ''),
                        '備考': customer.get('Note', ''),
                        '作成日': customer.get('Created At', ''),
                        '新規顧客フラグ': customer.get('Is New Customer', ''),
                        
                        # アラート情報
                        'アラート作成日': datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
                        '遅延日数': max(0, days_since - (avg_order_interval * 1.5) if avg_order_interval > 0 else 180),
                        '次回注文予想日': self.calculate_expected_next_order(last_order_date, avg_order_interval)
                    }
                    
                    churn_candidates.append(churn_candidate)
            
            # 優先度スコアでソート（高い順）
            churn_candidates.sort(key=lambda x: x['優先度スコア'], reverse=True)
            
            logger.info(f"離脱候補を検出: {len(churn_candidates)}名")
            
            # CSVファイルにエクスポート
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"churn_alert_list_{timestamp}.csv"
            filepath = os.path.join('exports', filename)
            
            if churn_candidates:
                fieldnames = list(churn_candidates[0].keys())
                with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(churn_candidates)
                
                logger.info(f"離脱者アラートリストをエクスポート: {filename}")
                
                # スプレッドシートにアップロード
                success = self.upload_to_churn_sheet(filepath)
                
                if success:
                    logger.info("離脱者アラートリストの作成が完了しました")
                    return True
                else:
                    logger.error("離脱者アラートリストのアップロードに失敗しました")
                    return False
            else:
                logger.info("離脱候補が見つかりませんでした")
                return True
                
        except Exception as e:
            logger.error(f"離脱者アラートリスト作成エラー: {e}")
            return False
    
    def calculate_expected_next_order(self, last_order_date, avg_order_interval):
        """次の注文予想日を計算"""
        if not last_order_date or avg_order_interval <= 0:
            return "不明"
        
        try:
            if '/' in last_order_date:
                last_dt = datetime.strptime(last_order_date.split(' ')[0], '%Y/%m/%d')
            else:
                last_dt = datetime.fromisoformat(last_order_date.replace('Z', '+00:00'))
            
            expected_date = last_dt + timedelta(days=avg_order_interval)
            return expected_date.strftime('%Y/%m/%d')
        except:
            return "不明"
    
    def upload_to_churn_sheet(self, csv_filepath):
        """離脱者アラートシートにアップロード"""
        try:
            sheet_name = "離脱者アラート"
            
            # シートの内容をクリア
            self.spreadsheet_uploader.clear_sheet_content(sheet_name)
            logger.info(f"シート '{sheet_name}' の内容をクリアしました")
            
            # CSVファイルをアップロード
            success = self.spreadsheet_uploader.upload_csv_to_spreadsheet(
                csv_filepath, 
                sheet_name
            )
            
            if success:
                logger.info(f"シート '{sheet_name}' にデータをアップロードしました")
                
                # 書式設定を適用
                self.apply_churn_formatting(sheet_name)
                
                return True
            else:
                logger.error(f"シート '{sheet_name}' へのアップロードに失敗しました")
                return False
                
        except Exception as e:
            logger.error(f"離脱者アラートシートアップロードエラー: {e}")
            return False
    
    def apply_churn_formatting(self, sheet_name):
        """離脱者アラートシートの書式設定を適用"""
        try:
            # 書式設定を適用
            self.spreadsheet_uploader.format_sheet(sheet_name, 30)  # 十分な列数を確保
            
            logger.info(f"シート '{sheet_name}' の書式設定を適用しました")
            
        except Exception as e:
            logger.error(f"書式設定エラー: {e}")
    
    def get_churn_summary(self):
        """離脱者サマリーを取得"""
        try:
            customers = self.load_customer_data()
            if not customers:
                return {}
            
            total_customers = len(customers)
            churn_candidates = []
            
            for customer in customers:
                is_churn, category, days_since, risk_score = self.is_churn_candidate(customer)
                if is_churn:
                    churn_candidates.append((customer, category, days_since, risk_score))
            
            # カテゴリ別集計
            category_counts = {}
            alert_levels = {'緊急': 0, '高': 0, '中': 0, '低': 0}
            
            for customer, category, days_since, risk_score in churn_candidates:
                category_counts[category] = category_counts.get(category, 0) + 1
                
                churn_risk_score = risk_score
                
                if churn_risk_score >= 80:
                    alert_levels['緊急'] += 1
                elif churn_risk_score >= 60:
                    alert_levels['高'] += 1
                elif churn_risk_score >= 40:
                    alert_levels['中'] += 1
                else:
                    alert_levels['低'] += 1
            
            summary = {
                'total_customers': total_customers,
                'churn_candidates': len(churn_candidates),
                'churn_rate': round(len(churn_candidates) / total_customers * 100, 2) if total_customers > 0 else 0,
                'category_counts': category_counts,
                'alert_levels': alert_levels
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"離脱者サマリー取得エラー: {e}")
            return {}

def main():
    """メイン実行関数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='離脱者アラート管理スクリプト')
    parser.add_argument('action', choices=['create_alert', 'summary'], 
                       help='実行するアクション')
    
    args = parser.parse_args()
    
    try:
        manager = ChurnAlertManager()
        
        if args.action == 'create_alert':
            print("📊 離脱者アラートリストを作成します...")
            success = manager.create_churn_alert_list()
            
            if success:
                print("✅ 離脱者アラートリストの作成が完了しました")
                print("🔗 スプレッドシートURL: https://docs.google.com/spreadsheets/d/1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g")
            else:
                print("❌ 離脱者アラートリストの作成に失敗しました")
                sys.exit(1)
                
        elif args.action == 'summary':
            print("📊 離脱者サマリーを取得します...")
            summary = manager.get_churn_summary()
            
            if summary:
                print(f"📈 離脱者サマリー:")
                print(f"   総顧客数: {summary['total_customers']:,}名")
                print(f"   離脱候補: {summary['churn_candidates']:,}名")
                print(f"   離脱率: {summary['churn_rate']:.2f}%")
                print(f"   カテゴリ別: {summary['category_counts']}")
                print(f"   アラートレベル別: {summary['alert_levels']}")
            else:
                print("❌ 離脱者サマリーの取得に失敗しました")
                sys.exit(1)
                
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
