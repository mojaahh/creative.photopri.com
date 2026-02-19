#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ユーザー分析管理スクリプト
上位100名のユーザー分析用データを生成・管理
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
        logging.FileHandler('user_analysis_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class UserAnalysisManager:
    """ユーザー分析管理クラス"""
    
    def __init__(self):
        """初期化"""
        self.spreadsheet_uploader = SpreadsheetUploader()
        self.spreadsheet_id = "1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g"
    
    def get_top_100_customers(self):
        """過去3年間に最終注文がある上位100名の顧客データを取得"""
        try:
            # 最新の顧客データCSVファイルを検索
            exports_dir = 'exports'
            if not os.path.exists(exports_dir):
                logger.error("exportsディレクトリが見つかりません")
                return []
            
            # 最新の顧客データファイルを検索
            customer_files = [f for f in os.listdir(exports_dir) if f.startswith('customers_all_') and f.endswith('.csv')]
            if not customer_files:
                logger.error("顧客データファイルが見つかりません")
                return []
            
            # 最新のファイルを選択
            latest_file = max(customer_files, key=lambda x: os.path.getctime(os.path.join(exports_dir, x)))
            filepath = os.path.join(exports_dir, latest_file)
            
            logger.info(f"顧客データを読み込み中: {latest_file}")
            
            # 過去3年間の条件
            three_years_ago = datetime.now() - timedelta(days=3*365)
            
            # CSVファイルを読み込み
            customers = []
            recent_customers = []
            
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 数値データを適切に変換
                    try:
                        total_amount = float(row.get('Total Service Amount', 0) or 0)
                        total_orders = int(row.get('Total Service Orders', 0) or 0)
                        
                        # 過去3年間の最終注文者かチェック
                        last_order_date_str = row.get('Last Order Date', '')
                        is_recent_customer = False
                        
                        if last_order_date_str:
                            try:
                                if '/' in last_order_date_str:
                                    last_order_dt = datetime.strptime(last_order_date_str.split(' ')[0], '%Y/%m/%d')
                                else:
                                    last_order_dt = datetime.fromisoformat(last_order_date_str.replace('Z', '+00:00'))
                                
                                if last_order_dt >= three_years_ago:
                                    is_recent_customer = True
                            except ValueError:
                                # 日付解析エラーの場合は除外
                                continue
                        
                        if is_recent_customer:
                            recent_customers.append({
                                'row': row,
                                'total_amount': total_amount,
                                'total_orders': total_orders
                            })
                        
                        customers.append({
                            'row': row,
                            'total_amount': total_amount,
                            'total_orders': total_orders
                        })
                    except (ValueError, TypeError) as e:
                        logger.warning(f"データ変換エラー: {e}")
                        continue
            
            logger.info(f"過去3年間の最終注文者: {len(recent_customers)}名（全顧客: {len(customers)}名）")
            
            # 過去3年間の顧客を総購入金額でソートして上位100名を取得
            recent_customers.sort(key=lambda x: x['total_amount'], reverse=True)
            top_100 = recent_customers[:100]
            
            logger.info(f"上位100名の顧客データを取得: {len(top_100)}件")
            return [customer['row'] for customer in top_100]
            
        except Exception as e:
            logger.error(f"上位100名顧客データ取得エラー: {e}")
            return []
    
    def calculate_analysis_metrics(self, customers):
        """分析用の指標を計算"""
        try:
            enhanced_customers = []
            
            for customer in customers:
                enhanced_customer = customer.copy()
                
                # 基本データの取得
                total_amount = float(customer.get('Total Service Amount', 0) or 0)
                total_orders = int(customer.get('Total Service Orders', 0) or 0)
                avg_order_value = float(customer.get('Average Order Value', 0) or 0)
                avg_order_interval = float(customer.get('Average Order Interval (Days)', 0) or 0)
                
                # 日付データの処理
                first_order_date = customer.get('First Order Date', '')
                last_order_date = customer.get('Last Order Date', '')
                created_at = customer.get('Created At', '')
                
                # 1. RFM分析
                recency = self.calculate_recency(last_order_date)
                frequency = total_orders
                monetary = total_amount
                
                # 2. 顧客継続期間（日数）
                customer_lifespan = self.calculate_customer_lifespan(first_order_date, last_order_date)
                
                # 3. 離脱リスクスコア（0-100、高いほど離脱リスクが高い）
                churn_risk = self.calculate_churn_risk(recency, avg_order_interval, total_orders)
                
                # 4. 顧客価値スコア（総購入金額 × 注文頻度 × 継続期間の重み付きスコア）
                customer_value_score = self.calculate_customer_value_score(total_amount, total_orders, customer_lifespan)
                
                # 5. エンゲージメントスコア（メール・SMS同意状況）
                engagement_score = self.calculate_engagement_score(customer)
                
                # 6. 地域分析
                region = self.extract_region(customer)
                
                # 7. 顧客タイプ分類
                customer_type = self.classify_customer_type(total_amount, total_orders, avg_order_interval, churn_risk)
                
                # 8. 成長軌跡（月別購入金額の推定）
                growth_trajectory = self.calculate_growth_trajectory(total_amount, customer_lifespan, total_orders)
                
                # 9. 新規顧客からの成長度
                new_customer_growth = self.calculate_new_customer_growth(customer, total_amount, total_orders)
                
                # 10. 予測LTV（顧客生涯価値）
                predicted_ltv = self.calculate_predicted_ltv(total_amount, avg_order_interval, customer_lifespan, churn_risk)
                
                # 分析指標を追加
                enhanced_customer.update({
                    # RFM分析
                    'Recency (Days)': recency,
                    'Frequency': frequency,
                    'Monetary': monetary,
                    
                    # 顧客分析
                    'Customer Lifespan (Days)': customer_lifespan,
                    'Churn Risk Score': churn_risk,
                    'Customer Value Score': customer_value_score,
                    'Engagement Score': engagement_score,
                    
                    # 地理・分類
                    'Region': region,
                    'Customer Type': customer_type,
                    
                    # 成長・予測
                    'Growth Trajectory': growth_trajectory,
                    'New Customer Growth': new_customer_growth,
                    'Predicted LTV': predicted_ltv,
                    
                    # 追加の便利な指標
                    'Orders per Month': round(total_orders / max(customer_lifespan / 30, 1), 2) if customer_lifespan > 0 else 0,
                    'Days Since Last Order': recency,
                    'Is High Value': 'Yes' if total_amount >= 100000 else 'No',  # 10万円以上
                    'Is Frequent Buyer': 'Yes' if total_orders >= 10 else 'No',  # 10回以上
                    'Is At Risk': 'Yes' if churn_risk >= 70 else 'No'  # 離脱リスク70%以上
                })
                
                enhanced_customers.append(enhanced_customer)
            
            return enhanced_customers
            
        except Exception as e:
            logger.error(f"分析指標計算エラー: {e}")
            return customers
    
    def calculate_recency(self, last_order_date):
        """最新購入日からの経過日数を計算"""
        if not last_order_date:
            return 9999  # 注文履歴がない場合は最大値
        
        try:
            # 日付形式を統一して処理
            if '/' in last_order_date:
                last_dt = datetime.strptime(last_order_date.split(' ')[0], '%Y/%m/%d')
            else:
                last_dt = datetime.fromisoformat(last_order_date.replace('Z', '+00:00'))
            
            days_since = (datetime.now() - last_dt).days
            return max(0, days_since)
        except:
            return 9999
    
    def calculate_customer_lifespan(self, first_order_date, last_order_date):
        """顧客継続期間を計算"""
        if not first_order_date or not last_order_date:
            return 0
        
        try:
            # 日付形式を統一して処理
            if '/' in first_order_date:
                first_dt = datetime.strptime(first_order_date.split(' ')[0], '%Y/%m/%d')
            else:
                first_dt = datetime.fromisoformat(first_order_date.replace('Z', '+00:00'))
            
            if '/' in last_order_date:
                last_dt = datetime.strptime(last_order_date.split(' ')[0], '%Y/%m/%d')
            else:
                last_dt = datetime.fromisoformat(last_order_date.replace('Z', '+00:00'))
            
            return (last_dt - first_dt).days
        except:
            return 0
    
    def calculate_churn_risk(self, recency, avg_order_interval, total_orders):
        """離脱リスクスコアを計算（0-100）"""
        if total_orders == 0:
            return 100
        
        # 基本リスクスコア
        risk_score = 0
        
        # 最新購入からの経過日数によるリスク
        if recency > 365:  # 1年以上
            risk_score += 50
        elif recency > 180:  # 6ヶ月以上
            risk_score += 30
        elif recency > 90:  # 3ヶ月以上
            risk_score += 15
        
        # 平均注文間隔との比較
        if avg_order_interval > 0 and recency > avg_order_interval * 2:
            risk_score += 25
        
        # 注文数が少ない場合のリスク
        if total_orders < 3:
            risk_score += 20
        
        return min(100, risk_score)
    
    def calculate_customer_value_score(self, total_amount, total_orders, customer_lifespan):
        """顧客価値スコアを計算"""
        if customer_lifespan == 0:
            return 0
        
        # 重み付きスコア: 総購入金額 × 注文頻度 × 継続期間の正規化
        amount_score = min(total_amount / 100000, 10)  # 10万円を10点とする
        frequency_score = min(total_orders / 10, 10)   # 10回を10点とする
        lifespan_score = min(customer_lifespan / 365, 10)  # 1年を10点とする
        
        return round(amount_score * frequency_score * lifespan_score, 2)
    
    def calculate_engagement_score(self, customer):
        """エンゲージメントスコアを計算（0-100）"""
        score = 0
        
        # メールマーケティング同意
        if customer.get('Accepts Email Marketing', '').lower() == 'true':
            score += 30
        
        # SMSマーケティング同意
        if customer.get('Accepts SMS Marketing', '').lower() == 'true':
            score += 20
        
        # メール検証済み
        if customer.get('Verified Email', '').lower() == 'true':
            score += 25
        
        # タグの有無
        if customer.get('Tags', '').strip():
            score += 15
        
        # 会社名の登録
        if customer.get('Default Address Company', '').strip():
            score += 10
        
        return min(100, score)
    
    def extract_region(self, customer):
        """地域を抽出"""
        province = customer.get('Default Address Province Code', '')
        city = customer.get('Default Address City', '')
        
        if province:
            return f"{province} {city}".strip()
        elif city:
            return city
        else:
            return "不明"
    
    def classify_customer_type(self, total_amount, total_orders, avg_order_interval, churn_risk):
        """顧客タイプを分類"""
        if total_amount >= 500000:  # 50万円以上
            if churn_risk < 30:
                return "VIP顧客"
            else:
                return "高額顧客（離脱リスク高）"
        elif total_amount >= 100000:  # 10万円以上
            if avg_order_interval < 30:
                return "頻繁購入者"
            else:
                return "中額顧客"
        elif total_orders >= 10:
            return "リピート顧客"
        elif total_orders >= 3:
            return "成長中顧客"
        else:
            return "新規顧客"
    
    def calculate_growth_trajectory(self, total_amount, customer_lifespan, total_orders):
        """成長軌跡を計算"""
        if customer_lifespan == 0 or total_orders == 0:
            return "データ不足"
        
        months = customer_lifespan / 30
        monthly_avg = total_amount / months
        
        if monthly_avg >= 50000:
            return "急成長"
        elif monthly_avg >= 20000:
            return "順調成長"
        elif monthly_avg >= 5000:
            return "緩やか成長"
        else:
            return "低成長"
    
    def calculate_new_customer_growth(self, customer, total_amount, total_orders):
        """新規顧客からの成長度を計算"""
        is_new = customer.get('Is New Customer', '').lower() == 'true'
        
        if not is_new:
            return "既存顧客"
        
        if total_amount >= 50000:
            return "優秀な新規顧客"
        elif total_amount >= 10000:
            return "有望な新規顧客"
        else:
            return "新規顧客"
    
    def calculate_predicted_ltv(self, total_amount, avg_order_interval, customer_lifespan, churn_risk):
        """予測LTVを計算"""
        if customer_lifespan == 0 or avg_order_interval == 0:
            return total_amount
        
        # 離脱リスクを考慮した予測期間
        risk_factor = (100 - churn_risk) / 100
        predicted_months = max(6, customer_lifespan / 30 * risk_factor)
        
        # 月間平均購入金額
        monthly_avg = total_amount / (customer_lifespan / 30) if customer_lifespan > 0 else 0
        
        predicted_ltv = monthly_avg * predicted_months
        return round(predicted_ltv, 0)
    
    def create_analysis_sheet(self):
        """ユーザー分析シートを作成"""
        try:
            logger.info("上位100名ユーザー分析を開始")
            
            # 上位100名の顧客データを取得
            top_100_customers = self.get_top_100_customers()
            if not top_100_customers:
                logger.error("上位100名の顧客データが取得できませんでした")
                return False
            
            # 分析指標を計算
            enhanced_customers = self.calculate_analysis_metrics(top_100_customers)
            
            # CSVファイルにエクスポート
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"user_analysis_top100_{timestamp}.csv"
            filepath = os.path.join('exports', filename)
            
            # CSVファイルを作成
            with open(filepath, 'w', encoding='utf-8', newline='') as f:
                if enhanced_customers:
                    fieldnames = list(enhanced_customers[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(enhanced_customers)
            
            logger.info(f"ユーザー分析データをエクスポート: {filename}")
            
            # スプレッドシートにアップロード
            success = self.upload_to_analysis_sheet(filepath)
            
            if success:
                logger.info("ユーザー分析シートの作成が完了しました")
                return True
            else:
                logger.error("ユーザー分析シートのアップロードに失敗しました")
                return False
                
        except Exception as e:
            logger.error(f"ユーザー分析シート作成エラー: {e}")
            return False
    
    def upload_to_analysis_sheet(self, csv_filepath):
        """分析シートにアップロード"""
        try:
            # シート名
            sheet_name = "ユーザー分析"
            
            # シートの内容をクリア（存在しない場合は作成される）
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
                self.apply_analysis_formatting(sheet_name)
                
                return True
            else:
                logger.error(f"シート '{sheet_name}' へのアップロードに失敗しました")
                return False
                
        except Exception as e:
            logger.error(f"分析シートアップロードエラー: {e}")
            return False
    
    def apply_analysis_formatting(self, sheet_name):
        """分析シートの書式設定を適用"""
        try:
            # 通貨形式の列を設定
            currency_columns = [
                'Total Service Amount', 'Average Order Value', 'Monetary', 
                'Customer Value Score', 'Predicted LTV'
            ]
            
            # 数値形式の列を設定
            number_columns = [
                'Total Service Orders', 'Recency (Days)', 'Frequency',
                'Customer Lifespan (Days)', 'Churn Risk Score', 'Engagement Score',
                'Orders per Month', 'Days Since Last Order'
            ]
            
            # 書式設定を適用
            self.spreadsheet_uploader.format_sheet(sheet_name, len(currency_columns) + len(number_columns))
            
            logger.info(f"シート '{sheet_name}' の書式設定を適用しました")
            
        except Exception as e:
            logger.error(f"書式設定エラー: {e}")

def main():
    """メイン実行関数"""
    try:
        manager = UserAnalysisManager()
        
        print("📊 上位100名ユーザー分析を開始します...")
        success = manager.create_analysis_sheet()
        
        if success:
            print("✅ ユーザー分析シートの作成が完了しました")
            print("🔗 スプレッドシートURL: https://docs.google.com/spreadsheets/d/1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g")
        else:
            print("❌ ユーザー分析シートの作成に失敗しました")
            sys.exit(1)
                
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
