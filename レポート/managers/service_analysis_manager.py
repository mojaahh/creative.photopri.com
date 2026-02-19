#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
サービス別分析管理スクリプト
各ストア（サービス）ごとの上位顧客分析
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
        logging.FileHandler('service_analysis_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ServiceAnalysisManager:
    """サービス別分析管理クラス"""
    
    def __init__(self):
        """初期化"""
        self.spreadsheet_uploader = SpreadsheetUploader()
        self.spreadsheet_id = "1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g"
        
        # サービス別の設定
        self.services = {
            'Photopri': {
                'store_key': 'PHOTOPRI_SHOP',
                'order_prefix': '#P',
                'sheet_name': 'Photopri分析'
            },
            'Artgraph': {
                'store_key': 'ARTGRAPH_SHOP', 
                'order_prefix': '#A',
                'sheet_name': 'Artgraph分析'
            },
            'E1 Print': {
                'store_key': 'E1_SHOP',
                'order_prefix': '#E', 
                'sheet_name': 'E1 Print分析'
            },
            'Qoo': {
                'store_key': 'QOO_SHOP',
                'order_prefix': '#Q',
                'sheet_name': 'Qoo分析'
            },
            'TETTE': {
                'store_key': 'TETTE_SHOP',
                'order_prefix': '#T',
                'sheet_name': 'TETTE分析'
            }
        }
    
    def get_service_customers(self, service_name, top_n=50):
        """指定サービスの上位顧客データを取得"""
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
            
            # CSVファイルを読み込み
            service_customers = []
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 指定サービスの顧客のみをフィルタ
                    store_key = row.get('Store Key', '')
                    if store_key == self.services[service_name]['store_key']:
                        try:
                            total_amount = float(row.get('Total Service Amount', 0) or 0)
                            total_orders = int(row.get('Total Service Orders', 0) or 0)
                            
                            service_customers.append({
                                'row': row,
                                'total_amount': total_amount,
                                'total_orders': total_orders
                            })
                        except (ValueError, TypeError) as e:
                            logger.warning(f"データ変換エラー: {e}")
                            continue
            
            # 総購入金額でソートして上位N名を取得
            service_customers.sort(key=lambda x: x['total_amount'], reverse=True)
            top_customers = service_customers[:top_n]
            
            logger.info(f"{service_name}の上位{len(top_customers)}名の顧客データを取得")
            return [customer['row'] for customer in top_customers]
            
        except Exception as e:
            logger.error(f"{service_name}顧客データ取得エラー: {e}")
            return []
    
    def calculate_service_metrics(self, customers, service_name):
        """サービス別の分析指標を計算"""
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
                
                # 1. サービス固有のRFM分析
                recency = self.calculate_recency(last_order_date)
                frequency = total_orders
                monetary = total_amount
                
                # 2. サービス内での相対的な位置
                service_rank = self.calculate_service_rank(customers, total_amount)
                
                # 3. サービス利用期間
                service_lifespan = self.calculate_service_lifespan(first_order_date, last_order_date)
                
                # 4. サービス内での成長度
                service_growth = self.calculate_service_growth(total_amount, service_lifespan, total_orders)
                
                # 5. サービス依存度（このサービスの購入金額が全体に占める割合）
                service_dependency = self.calculate_service_dependency(customer)
                
                # 6. サービス内での離脱リスク
                service_churn_risk = self.calculate_service_churn_risk(recency, avg_order_interval, total_orders)
                
                # 7. サービス価値スコア
                service_value_score = self.calculate_service_value_score(total_amount, total_orders, service_lifespan, service_name)
                
                # 8. 月間購入頻度（サービス別）
                monthly_frequency = self.calculate_monthly_frequency(total_orders, service_lifespan)
                
                # 9. 平均購入間隔（サービス別）
                avg_purchase_interval = avg_order_interval
                
                # 10. サービス内での顧客タイプ
                service_customer_type = self.classify_service_customer_type(total_amount, total_orders, avg_order_interval, service_churn_risk, service_name)
                
                # 11. 予測サービスLTV
                predicted_service_ltv = self.calculate_predicted_service_ltv(total_amount, avg_order_interval, service_lifespan, service_churn_risk)
                
                # 12. サービス満足度推定
                service_satisfaction = self.calculate_service_satisfaction(total_orders, avg_order_interval, service_lifespan)
                
                # 分析指標を追加
                enhanced_customer.update({
                    # サービス固有のRFM分析
                    f'{service_name} 最新購入日からの経過日数': recency,
                    f'{service_name} 総注文数': frequency,
                    f'{service_name} 総購入金額': monetary,
                    
                    # サービス内分析
                    f'{service_name} ランキング': service_rank,
                    f'{service_name} 利用期間（日）': service_lifespan,
                    f'{service_name} 成長度': service_growth,
                    f'{service_name} 依存度（%）': service_dependency,
                    f'{service_name} 離脱リスク': service_churn_risk,
                    f'{service_name} 価値スコア': service_value_score,
                    
                    # 購入パターン
                    f'{service_name} 月間購入頻度': monthly_frequency,
                    f'{service_name} 平均購入間隔': avg_purchase_interval,
                    
                    # 分類・予測
                    f'{service_name} 顧客タイプ': service_customer_type,
                    f'{service_name} 予測LTV': predicted_service_ltv,
                    f'{service_name} 満足度スコア': service_satisfaction,
                    
                    # 追加指標
                    f'{service_name} トップ顧客': 'Yes' if service_rank <= 10 else 'No',
                    f'{service_name} ロイヤル顧客': 'Yes' if total_orders >= 5 and service_churn_risk < 50 else 'No',
                    f'{service_name} 離脱リスク有り': 'Yes' if service_churn_risk >= 70 else 'No',
                    f'{service_name} 成長ポテンシャル': 'High' if service_growth == '急成長' else 'Medium' if service_growth == '順調成長' else 'Low'
                })
                
                enhanced_customers.append(enhanced_customer)
            
            return enhanced_customers
            
        except Exception as e:
            logger.error(f"{service_name}分析指標計算エラー: {e}")
            return customers
    
    def calculate_recency(self, last_order_date):
        """最新購入日からの経過日数を計算"""
        if not last_order_date:
            return 9999
        
        try:
            if '/' in last_order_date:
                last_dt = datetime.strptime(last_order_date.split(' ')[0], '%Y/%m/%d')
            else:
                last_dt = datetime.fromisoformat(last_order_date.replace('Z', '+00:00'))
            
            days_since = (datetime.now() - last_dt).days
            return max(0, days_since)
        except:
            return 9999
    
    def calculate_service_rank(self, all_customers, total_amount):
        """サービス内でのランキングを計算"""
        amounts = []
        for customer in all_customers:
            try:
                amount = float(customer.get('Total Service Amount', 0) or 0)
                amounts.append(amount)
            except:
                continue
        
        amounts.sort(reverse=True)
        try:
            rank = amounts.index(total_amount) + 1
            return rank
        except:
            return len(amounts) + 1
    
    def calculate_service_lifespan(self, first_order_date, last_order_date):
        """サービス利用期間を計算"""
        if not first_order_date or not last_order_date:
            return 0
        
        try:
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
    
    def calculate_service_growth(self, total_amount, service_lifespan, total_orders):
        """サービス内での成長度を計算"""
        if service_lifespan == 0 or total_orders == 0:
            return "データ不足"
        
        months = service_lifespan / 30
        monthly_avg = total_amount / months
        
        if monthly_avg >= 50000:
            return "急成長"
        elif monthly_avg >= 20000:
            return "順調成長"
        elif monthly_avg >= 5000:
            return "緩やか成長"
        else:
            return "低成長"
    
    def calculate_service_dependency(self, customer):
        """サービス依存度を計算"""
        total_amount = float(customer.get('Total Service Amount', 0) or 0)
        # 全サービスの総額は現在のデータでは取得できないため、簡易計算
        return 100.0  # 現在は100%として表示
    
    def calculate_service_churn_risk(self, recency, avg_order_interval, total_orders):
        """サービス内での離脱リスクを計算"""
        if total_orders == 0:
            return 100
        
        risk_score = 0
        
        if recency > 365:
            risk_score += 50
        elif recency > 180:
            risk_score += 30
        elif recency > 90:
            risk_score += 15
        
        if avg_order_interval > 0 and recency > avg_order_interval * 2:
            risk_score += 25
        
        if total_orders < 3:
            risk_score += 20
        
        return min(100, risk_score)
    
    def calculate_service_value_score(self, total_amount, total_orders, service_lifespan, service_name):
        """サービス価値スコアを計算"""
        if service_lifespan == 0:
            return 0
        
        # サービス別の重み付け
        service_weights = {
            'Photopri': 1.0,
            'Artgraph': 1.0,
            'E1 Print': 1.0,
            'Qoo': 1.0,
            'TETTE': 1.0
        }
        
        weight = service_weights.get(service_name, 1.0)
        amount_score = min(total_amount / 100000, 10) * weight
        frequency_score = min(total_orders / 10, 10)
        lifespan_score = min(service_lifespan / 365, 10)
        
        return round(amount_score * frequency_score * lifespan_score, 2)
    
    def calculate_monthly_frequency(self, total_orders, service_lifespan):
        """月間購入頻度を計算"""
        if service_lifespan == 0:
            return 0
        
        months = service_lifespan / 30
        return round(total_orders / months, 2) if months > 0 else 0
    
    def classify_service_customer_type(self, total_amount, total_orders, avg_order_interval, service_churn_risk, service_name):
        """サービス内での顧客タイプを分類"""
        if total_amount >= 500000:
            if service_churn_risk < 30:
                return f"{service_name} VIP顧客"
            else:
                return f"{service_name} 高額顧客（離脱リスク高）"
        elif total_amount >= 100000:
            if avg_order_interval < 30:
                return f"{service_name} 頻繁購入者"
            else:
                return f"{service_name} 中額顧客"
        elif total_orders >= 10:
            return f"{service_name} リピート顧客"
        elif total_orders >= 3:
            return f"{service_name} 成長中顧客"
        else:
            return f"{service_name} 新規顧客"
    
    def calculate_predicted_service_ltv(self, total_amount, avg_order_interval, service_lifespan, service_churn_risk):
        """予測サービスLTVを計算"""
        if service_lifespan == 0 or avg_order_interval == 0:
            return total_amount
        
        risk_factor = (100 - service_churn_risk) / 100
        predicted_months = max(6, service_lifespan / 30 * risk_factor)
        
        monthly_avg = total_amount / (service_lifespan / 30) if service_lifespan > 0 else 0
        predicted_ltv = monthly_avg * predicted_months
        
        return round(predicted_ltv, 0)
    
    def calculate_service_satisfaction(self, total_orders, avg_order_interval, service_lifespan):
        """サービス満足度を推定"""
        if total_orders == 0 or service_lifespan == 0:
            return 0
        
        # 注文頻度が高いほど満足度が高いと仮定
        frequency_score = min(total_orders / 10, 10)
        
        # 継続期間が長いほど満足度が高いと仮定
        lifespan_score = min(service_lifespan / 365, 10)
        
        # 平均注文間隔が短いほど満足度が高いと仮定
        interval_score = max(0, 10 - (avg_order_interval / 30))
        
        satisfaction = (frequency_score + lifespan_score + interval_score) / 3
        return round(satisfaction, 1)
    
    def create_service_analysis_sheet(self, service_name, top_n=50):
        """サービス別分析シートを作成"""
        try:
            logger.info(f"{service_name}の上位{top_n}名分析を開始")
            
            # サービス別の上位顧客データを取得
            service_customers = self.get_service_customers(service_name, top_n)
            if not service_customers:
                logger.error(f"{service_name}の顧客データが取得できませんでした")
                return False
            
            # 分析指標を計算
            enhanced_customers = self.calculate_service_metrics(service_customers, service_name)
            
            # CSVファイルにエクスポート
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"service_analysis_{service_name.lower().replace(' ', '_')}_{timestamp}.csv"
            filepath = os.path.join('exports', filename)
            
            # CSVファイルを作成
            with open(filepath, 'w', encoding='utf-8', newline='') as f:
                if enhanced_customers:
                    fieldnames = list(enhanced_customers[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(enhanced_customers)
            
            logger.info(f"{service_name}分析データをエクスポート: {filename}")
            
            # スプレッドシートにアップロード
            success = self.upload_to_service_sheet(filepath, service_name)
            
            if success:
                logger.info(f"{service_name}分析シートの作成が完了しました")
                return True
            else:
                logger.error(f"{service_name}分析シートのアップロードに失敗しました")
                return False
                
        except Exception as e:
            logger.error(f"{service_name}分析シート作成エラー: {e}")
            return False
    
    def upload_to_service_sheet(self, csv_filepath, service_name):
        """サービス分析シートにアップロード"""
        try:
            sheet_name = self.services[service_name]['sheet_name']
            
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
                self.apply_service_formatting(sheet_name)
                
                return True
            else:
                logger.error(f"シート '{sheet_name}' へのアップロードに失敗しました")
                return False
                
        except Exception as e:
            logger.error(f"サービス分析シートアップロードエラー: {e}")
            return False
    
    def apply_service_formatting(self, sheet_name):
        """サービス分析シートの書式設定を適用"""
        try:
            # 書式設定を適用
            self.spreadsheet_uploader.format_sheet(sheet_name, 50)  # 十分な列数を確保
            
            logger.info(f"シート '{sheet_name}' の書式設定を適用しました")
            
        except Exception as e:
            logger.error(f"書式設定エラー: {e}")
    
    def create_all_service_analysis(self):
        """全サービスの分析シートを作成"""
        try:
            results = {}
            
            for service_name in self.services.keys():
                print(f"📊 {service_name}の分析を開始します...")
                success = self.create_service_analysis_sheet(service_name, 50)
                results[service_name] = success
                
                if success:
                    print(f"✅ {service_name}の分析が完了しました")
                else:
                    print(f"❌ {service_name}の分析に失敗しました")
            
            # 結果サマリー
            successful = sum(1 for success in results.values() if success)
            total = len(results)
            
            print(f"\n📈 サービス別分析完了: {successful}/{total} サービス")
            
            return successful == total
            
        except Exception as e:
            logger.error(f"全サービス分析エラー: {e}")
            return False

def main():
    """メイン実行関数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='サービス別分析スクリプト')
    parser.add_argument('service', nargs='?', choices=['Photopri', 'Artgraph', 'E1 Print', 'Qoo', 'TETTE', 'all'],
                       help='分析するサービス（allで全サービス）')
    parser.add_argument('--top', type=int, default=50, help='上位何名まで分析するか（デフォルト: 50）')
    
    args = parser.parse_args()
    
    try:
        manager = ServiceAnalysisManager()
        
        if args.service == 'all':
            print("📊 全サービスの分析を開始します...")
            success = manager.create_all_service_analysis()
        else:
            print(f"📊 {args.service}の分析を開始します...")
            success = manager.create_service_analysis_sheet(args.service, args.top)
        
        if success:
            print("✅ サービス別分析が完了しました")
            print("🔗 スプレッドシートURL: https://docs.google.com/spreadsheets/d/1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g")
        else:
            print("❌ サービス別分析に失敗しました")
            sys.exit(1)
                
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
