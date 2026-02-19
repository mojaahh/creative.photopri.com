import os
import json
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
import time
import concurrent.futures
from threading import Lock
import logging

class CustomerDBGenerator:
    def __init__(self):
        # ログ設定
        self.setup_logging()
        
        # プロジェクトルートの.envを読み込み
        load_dotenv('../.env')
        
        # 注文データを読み込む
        self.orders_data = self.load_orders_data()
        
        # ストア設定
        self.stores = {
            'ARTGRAPH_SHOP': {
                'url': os.getenv('ARTGRAPH_SHOP'),
                'token': os.getenv('ARTGRAPH_TOKEN')
            },
            'E1_SHOP': {
                'url': os.getenv('E1_SHOP'),
                'token': os.getenv('E1_TOKEN')
            },
            'PHOTOPRI_SHOP': {
                'url': os.getenv('PHOTOPRI_SHOP'),
                'token': os.getenv('PHOTOPRI_TOKEN')
            },
            'QOO_SHOP': {
                'url': os.getenv('QOO_SHOP'),
                'token': os.getenv('QOO_TOKEN')
            },
            'COPYCENTERGALLERY_SHOP': {
                'url': os.getenv('COPYCENTERGALLERY_SHOP'),
                'token': os.getenv('COPYCENTERGALLERY_TOKEN')
            },
            'TETTE_SHOP': {
                'url': os.getenv('TETTE_SHOP'),
                'token': os.getenv('TETTE_TOKEN')
            }
        }
        
        # アクティブなストアのみフィルタ
        self.active_stores = {k: v for k, v in self.stores.items() 
                             if v['url'] and v['token']}
        
        if not self.active_stores:
            raise ValueError("有効なストア設定が見つかりません。環境変数を確認してください。")
        
        # パフォーマンス設定
        self.max_workers = min(4, len(self.active_stores))  # 最大4並列
        self.batch_size = 250  # Shopify APIの制限に合わせて250に設定
        self.rate_limit_delay = 0.2  # レート制限を短縮
        self.progress_lock = Lock()
        self.processed_customers = 0
    
    def setup_logging(self):
        """ログ設定を初期化"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/customer_db_generator.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_orders_data(self):
        """注文データを読み込む"""
        try:
            # 最新の注文データCSVファイルを検索
            exports_dir = '../../レポート/exports'
            if not os.path.exists(exports_dir):
                return {}
            
            # 注文データファイルを検索（全期間のデータを優先）
            order_files = [f for f in os.listdir(exports_dir) if f.startswith('orders_') and f.endswith('.csv')]
            if not order_files:
                print("注文データファイルが見つかりません")
                return {}
            
            # 全期間のデータファイルを優先的に選択
            all_time_files = [f for f in order_files if 'all_time' in f]
            if all_time_files:
                latest_file = max(all_time_files, key=lambda x: os.path.getctime(os.path.join(exports_dir, x)))
                print(f"全期間の注文データを使用: {latest_file}")
            else:
                # 全期間のデータがない場合は最新のファイルを選択
                latest_file = max(order_files, key=lambda x: os.path.getctime(os.path.join(exports_dir, x)))
                print(f"期間限定の注文データを使用: {latest_file}")
            filepath = os.path.join(exports_dir, latest_file)
            
            print(f"注文データを読み込み中: {latest_file}")
            
            # CSVファイルを読み込み（BOMを除去）
            orders_by_email = {}
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    email = row.get('Email', '').strip()
                    if not email:
                        continue
                    
                    # 注文日時を取得
                    order_date = row.get('Created at', '').strip()
                    if not order_date:
                        continue
                    
                    # ストア名を取得（Name列の注文番号から推定）
                    order_name = row.get('Name', '').strip()
                    store = 'Unknown'
                    
                    if order_name.startswith('#A'):
                        store = 'Artgraph'
                    elif order_name.startswith('#P'):
                        store = 'Photopri'
                    elif order_name.startswith('#E'):
                        store = 'E1 Print'
                    elif order_name.startswith('#Q'):
                        store = 'Qoo'
                    elif order_name.startswith('#T'):
                        store = 'TETTE'
                    else:
                        # 注文番号から判定できない場合は、Emailのドメインから推定
                        if 'photopri' in email.lower():
                            store = 'Photopri'
                        elif 'artgraph' in email.lower():
                            store = 'Artgraph'
                        else:
                            store = 'Unknown'
                    
                    if email not in orders_by_email:
                        orders_by_email[email] = []
                    
                    orders_by_email[email].append({
                        'date': order_date,
                        'store': store
                    })
            
            print(f"注文データ読み込み完了: {len(orders_by_email)}件のユニークなメールアドレス")
            return orders_by_email
            
        except Exception as e:
            print(f"注文データ読み込みエラー: {e}")
            return {}
    
    def create_customers_query(self, cursor=None, date_filter=None):
        """顧客データを取得するGraphQLクエリ（API 2025-07対応、最適化版）"""
        after_clause = f', after: "{cursor}"' if cursor else ""
        query_filter = f', query: "{date_filter}"' if date_filter else ""
        
        return f"""
        {{
          customers(first: {self.batch_size}{after_clause}{query_filter}) {{
            edges {{
              node {{
                id
                firstName
                lastName
                email
                phone
                createdAt
                updatedAt
                numberOfOrders
                amountSpent {{
                  amount
                  currencyCode
                }}
                defaultAddress {{
                  address1
                  address2
                  city
                  provinceCode
                  zip
                  country
                  company
                  phone
                }}
                lastOrder {{
                  id
                  name
                  createdAt
                  displayFinancialStatus
                  displayFulfillmentStatus
                  totalPriceSet {{
                    shopMoney {{
                      amount
                      currencyCode
                    }}
                  }}
                }}
                tags
                verifiedEmail
                taxExempt
                note
                emailMarketingConsent {{
                  marketingState
                }}
                smsMarketingConsent {{
                  marketingState
                }}
                metafields(first: 10) {{
                  edges {{
                    node {{
                      key
                      value
                    }}
                  }}
                }}
              }}
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
        }}
        """
    
    def execute_graphql_query(self, query, store_key, retry_count=3):
        """GraphQLクエリを実行（リトライ機能付き）"""
        if store_key not in self.active_stores:
            self.logger.error(f"ストアキー {store_key} が見つかりません")
            return None
            
        store = self.active_stores[store_key]
        if not store or not store.get('url') or not store.get('token'):
            self.logger.error(f"ストア {store_key} の設定が不完全です")
            return None
            
        url = f"https://{store['url']}/admin/api/2025-07/graphql.json"
        headers = {
            'X-Shopify-Access-Token': store['token'],
            'Content-Type': 'application/json'
        }
        
        for attempt in range(retry_count):
            try:
                response = requests.post(url, json={'query': query}, headers=headers, timeout=30)
                response.raise_for_status()
                result = response.json()
                
                # レスポンスの詳細をログに出力（デバッグ用）
                if 'errors' in result:
                    self.logger.warning(f"GraphQLエラー ({store_key}): {result['errors']}")
                
                return result
            except requests.exceptions.RequestException as e:
                if attempt < retry_count - 1:
                    wait_time = (attempt + 1) * 2  # 指数バックオフ
                    self.logger.warning(f"GraphQLエラー ({store_key}), リトライ {attempt + 1}/{retry_count}: {e}")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"GraphQLエラー ({store_key}): {e}")
                    return None
            except Exception as e:
                self.logger.error(f"予期しないエラー ({store_key}): {e}")
                return None
    
    def fetch_customers_from_store(self, store_key, date_filter=None):
        """単一ストアから顧客データを取得（最適化版）"""
        try:
            self.logger.info(f"{store_key}から顧客データを取得開始...")
            store_customers = []
            cursor = None
            page_count = 0
            
            while True:
                query = self.create_customers_query(cursor, date_filter)
                result = self.execute_graphql_query(query, store_key)
                
                if not result or 'data' not in result:
                    self.logger.error(f"{store_key}のデータ取得に失敗")
                    if result and 'errors' in result:
                        # アクセス権限エラーの場合はスキップ
                        errors = result['errors']
                        access_denied = any('ACCESS_DENIED' in str(error) or 'not approved to access' in str(error) for error in errors)
                        if access_denied:
                            self.logger.warning(f"{store_key}: 顧客データアクセス権限がありません（Basicプラン制限）")
                            return []  # 空のリストを返してスキップ
                        else:
                            self.logger.error(f"GraphQLエラー: {errors}")
                    break
            
                customers_data = result['data']['customers']
                page_count += 1
                
                # バッチ処理で顧客データを処理
                batch_customers = []
                for edge in customers_data['edges']:
                    if edge and edge.get('node'):
                        customer = edge['node']
                        if customer and customer.get('email'):
                            # ストア情報を追加
                            customer['_store_key'] = store_key
                            customer['_store_name'] = store_key.replace('_SHOP', '')
                            customer['_store_url'] = self.active_stores[store_key]['url']
                            
                            # 追加の分析データを計算
                            customer = self.calculate_customer_metrics(customer)
                            batch_customers.append(customer)
                
                store_customers.extend(batch_customers)
                
                # 進捗表示
                with self.progress_lock:
                    self.processed_customers += len(batch_customers)
                    if page_count % 5 == 0:  # 5ページごとに進捗表示
                        self.logger.info(f"{store_key}: {len(store_customers)}件取得済み (ページ {page_count})")
                
                # ページネーション
                page_info = customers_data['pageInfo']
                if not page_info.get('hasNextPage'):
                    break
                
                cursor = page_info['endCursor']
                time.sleep(self.rate_limit_delay)  # 最適化されたレート制限
            
            self.logger.info(f"{store_key}: {len(store_customers)}件の顧客データを取得完了")
            return store_customers
            
        except Exception as e:
            self.logger.error(f"{store_key}の処理中にエラー: {e}")
            return []
    
    def calculate_customer_metrics(self, customer):
        """顧客の追加メトリクスを計算（全サービス統合）"""
        try:
            # 基本的なメトリクス
            customer['_total_orders'] = customer.get('numberOfOrders', 0)
            customer['_total_amount'] = float(customer.get('amountSpent', {}).get('amount', 0))
            customer['_first_order_date'] = customer.get('createdAt', '')
            customer['_last_order_date'] = ''
            customer['_average_order_interval'] = 0
            customer['_average_order_value'] = 0.0
            
            # 最終注文日を設定
            last_order = customer.get('lastOrder', {})
            if last_order and last_order.get('createdAt'):
                customer['_last_order_date'] = last_order.get('createdAt')
            
            # 新規顧客判定（作成日が過去2ヶ月以内）
            created_date = customer.get('createdAt')
            if created_date:
                try:
                    created_dt = datetime.fromisoformat(created_date.replace('Z', '+00:00'))
                    two_months_ago = datetime.now() - timedelta(days=60)
                    customer['_is_new_customer'] = created_dt >= two_months_ago
                except:
                    customer['_is_new_customer'] = False
            else:
                customer['_is_new_customer'] = False
            
            # 実際の注文データから詳細なメトリクスを計算
            customer_email = customer.get('email', '').strip()
            if customer_email and customer_email in self.orders_data:
                self.calculate_detailed_metrics(customer, customer_email)
            
        except Exception as e:
            print(f"顧客メトリクス計算中にエラー: {e}")
            customer['_total_orders'] = 0
            customer['_total_amount'] = 0.0
            customer['_first_order_date'] = ''
            customer['_last_order_date'] = ''
            customer['_average_order_interval'] = 0
            customer['_average_order_value'] = 0.0
            customer['_is_new_customer'] = False
        
        return customer
    
    def calculate_detailed_metrics(self, customer, email):
        """実際の注文データから詳細なメトリクスを計算"""
        try:
            if email not in self.orders_data:
                return
            
            orders = self.orders_data[email]
            if not orders:
                return
            
            # 全注文を日付順にソート
            sorted_orders = sorted(orders, key=lambda x: x['date'])
            
            # 基本統計を計算
            total_orders = len(sorted_orders)
            total_amount = 0.0
            
            # 各注文の金額を取得（注文データから）
            for order in sorted_orders:
                # 注文データから金額を取得する必要があります
                # 現在の注文データには金額情報がないため、既存の顧客データを使用
                pass
            
            # 既存の顧客データから総額を使用
            total_amount = customer.get('_total_amount', 0.0)
            
            # 初回と最終注文日を更新
            if sorted_orders:
                customer['_first_order_date'] = sorted_orders[0]['date']
                customer['_last_order_date'] = sorted_orders[-1]['date']
            
            # 注文間隔の平均を計算
            if total_orders >= 2:
                intervals = []
                for i in range(1, total_orders):
                    prev_date = sorted_orders[i-1]['date']
                    curr_date = sorted_orders[i]['date']
                    interval = self.calculate_order_span(prev_date, curr_date)
                    if interval > 0:
                        intervals.append(interval)
                
                if intervals:
                    customer['_average_order_interval'] = round(sum(intervals) / len(intervals), 1)
            
            # 平均注文単価を計算
            if total_orders > 0 and total_amount > 0:
                customer['_average_order_value'] = round(total_amount / total_orders, 2)
            
        except Exception as e:
            print(f"詳細メトリクス計算エラー: {e}")
    
    def analyze_customer_orders(self, customer, customer_id):
        """顧客の注文履歴を分析してサービス別メトリクスを計算"""
        try:
            # ストアキーからサービスを判定
            store_key = customer.get('_store_key')
            service = self.determine_order_service(None, store_key)
            
            if not service or service not in customer['_service_orders']:
                return
            
            # 現在のストアでの注文情報を設定
            service_data = customer['_service_orders'][service]
            service_data['count'] = customer.get('numberOfOrders', 0)
            service_data['total_amount'] = float(customer.get('amountSpent', {}).get('amount', '0'))
            
            # 最終注文日を設定
            last_order = customer.get('lastOrder', {})
            if last_order and last_order.get('createdAt'):
                service_data['last_order_date'] = last_order.get('createdAt')
            
            # 初回注文日は顧客作成日を近似値として使用
            created_date = customer.get('createdAt')
            if created_date:
                service_data['first_order_date'] = created_date
            
            # 実際の注文データから注文スパンを計算
            customer_email = customer.get('email', '').strip()
            service_data['order_span_days'] = self.calculate_order_span_from_orders(
                customer_email, service, service_data['first_order_date'], service_data['last_order_date']
            )
            
        except Exception as e:
            print(f"顧客注文分析中にエラー: {e}")
    
    def determine_order_service(self, order, store_key):
        """注文からサービスを判定"""
        try:
            # ストアキーに基づいてサービスを判定
            if 'PHOTOPRI' in store_key:
                return '#P'
            elif 'E1' in store_key:
                return '#E'
            elif 'ARTGRAPH' in store_key:
                return '#A'
            elif 'QOO' in store_key:
                return '#Q'
            elif 'TETTE' in store_key:
                return '#T'
            else:
                return None
        except:
            return None
    
    def calculate_order_span(self, first_order_date, last_order_date):
        """注文スパン（日数）を計算"""
        try:
            if not first_order_date or not last_order_date:
                return 0
            
            # ISO日時文字列をdatetimeオブジェクトに変換
            first_dt = datetime.fromisoformat(first_order_date.replace('Z', '+00:00'))
            last_dt = datetime.fromisoformat(last_order_date.replace('Z', '+00:00'))
            
            # 日数の差を計算
            span_days = (last_dt - first_dt).days
            return max(0, span_days)  # 負の値は0に設定
            
        except Exception as e:
            print(f"注文スパン計算エラー: {e}")
            return 0
    
    def calculate_order_span_from_orders(self, email, service, fallback_first, fallback_last):
        """実際の注文データから注文スパン（平均注文間隔）を計算"""
        try:
            if not email or email not in self.orders_data:
                # 注文データがない場合はフォールバック値を使用
                return self.calculate_order_span(fallback_first, fallback_last)
            
            # サービスに対応するストア名を取得
            store_mapping = {
                '#P': 'Photopri',
                '#E': 'E1 Print', 
                '#A': 'Artgraph',
                '#Q': 'Qoo',
                '#T': 'TETTE'
            }
            
            target_store = store_mapping.get(service)
            if not target_store:
                return 0
            
            # 該当するストアの注文のみをフィルタ
            service_orders = []
            for order in self.orders_data[email]:
                if order['store'] == target_store:
                    service_orders.append(order['date'])
            
            if len(service_orders) < 2:
                # 注文が1件以下の場合は0
                return 0
            
            # 日付をソート
            service_orders.sort()
            
            # 各注文間の間隔を計算
            intervals = []
            for i in range(1, len(service_orders)):
                prev_date = service_orders[i-1]
                curr_date = service_orders[i]
                interval = self.calculate_order_span(prev_date, curr_date)
                if interval > 0:  # 0日間隔は除外
                    intervals.append(interval)
            
            if not intervals:
                return 0
            
            # 平均間隔を計算
            average_interval = sum(intervals) / len(intervals)
            return round(average_interval, 1)  # 小数点以下1桁で四捨五入
            
        except Exception as e:
            print(f"注文データからのスパン計算エラー: {e}")
            return 0
    
    def fetch_all_customers(self, date_filter=None):
        """全ストアから顧客データを取得（並列処理版）"""
        self.logger.info(f"全ストアから顧客データを並列取得開始 (最大{self.max_workers}並列)")
        all_customers = []
        
        # 並列処理でストアごとのデータ取得を実行
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 各ストアのタスクを開始
            future_to_store = {
                executor.submit(self.fetch_customers_from_store, store_key, date_filter): store_key
                for store_key in self.active_stores
            }
            
            # 完了したタスクから結果を取得
            for future in concurrent.futures.as_completed(future_to_store):
                store_key = future_to_store[future]
                try:
                    store_customers = future.result()
                    if store_customers:  # 空でない場合のみ追加
                        all_customers.extend(store_customers)
                        self.logger.info(f"{store_key}: {len(store_customers)}件のデータを統合完了")
                    else:
                        self.logger.warning(f"{store_key}: データが取得できませんでした")
                except Exception as e:
                    self.logger.error(f"{store_key}の処理中にエラー: {e}")
                    continue
        
        self.logger.info(f"全ストアからのデータ取得完了: 総計{len(all_customers)}件")
        return all_customers
    
    def convert_to_csv_data(self, customers):
        """顧客データをCSV形式に変換（最適化版）"""
        self.logger.info(f"CSV変換開始: {len(customers)}件の顧客データ")
        
        # 拡張ヘッダー行
        headers = [
            'Customer ID', 'First Name', 'Last Name', 'Email', 'Accepts Email Marketing',
            'Default Address Company', 'Default Address Address1', 'Default Address Address2',
            'Default Address City', 'Default Address Province Code', 'Default Address Country Code',
            'Default Address Zip', 'Default Address Phone', 'Phone', 'Accepts SMS Marketing',
            'Total Spent', 'Total Orders', 'Note', 'Tax Exempt', 'Tags',
            'Checkout Blocks Rule Trigger', '生年月日',
            # 追加フィールド
            'Created At', 'Updated At', 'Verified Email', 'Currency',
            'Last Order ID', 'Last Order Name', 'Last Order Date', 'Last Order Status',
            'Last Order Fulfillment Status', 'Last Order Amount',
            # サービス統合情報
            'Total Service Orders', 'Total Service Amount', 'First Order Date', 'Last Order Date', 'Average Order Interval (Days)', 'Average Order Value',
            # 分析フィールド
            'Is New Customer', 'Store Key', 'Store Name', 'Store URL'
        ]
        
        # ジェネレーターを使用してメモリ効率を向上
        def generate_csv_rows():
            yield headers
            
            processed_count = 0
            for customer in customers:
                if not customer or not isinstance(customer, dict):
                    continue
                    
                try:
                    row = self.extract_customer_row(customer)
                    if row:
                        yield row
                        processed_count += 1
                        
                        # 進捗表示（1000件ごと）
                        if processed_count % 1000 == 0:
                            self.logger.info(f"CSV変換進捗: {processed_count}/{len(customers)}件完了")
                            
                except Exception as e:
                    self.logger.error(f"顧客データの変換中にエラー: {e}")
                    continue
            
            self.logger.info(f"CSV変換完了: {processed_count}件のデータを変換")
        
        return generate_csv_rows()
    
    def extract_customer_row(self, customer):
        """顧客データから1行分のCSVデータを抽出（最適化版）"""
        try:
            # 基本情報
            customer_id = customer.get('id', '')
            first_name = customer.get('firstName', '')
            last_name = customer.get('lastName', '')
            email = customer.get('email', '')
            email_marketing = customer.get('emailMarketingConsent', {})
            accepts_marketing = email_marketing.get('marketingState', '') == 'SUBSCRIBED' if email_marketing else False
            
            # 住所情報
            default_address = customer.get('defaultAddress', {}) or {}
            address_company = default_address.get('company', '')
            address1 = default_address.get('address1', '')
            address2 = default_address.get('address2', '')
            city = default_address.get('city', '')
            province_code = default_address.get('provinceCode', '')
            country_code = default_address.get('country', '')
            zip_code = default_address.get('zip', '')
            address_phone = default_address.get('phone', '')
            
            # その他の基本情報
            phone = customer.get('phone', '')
            sms_marketing = customer.get('smsMarketingConsent', {})
            accepts_sms_marketing = sms_marketing.get('marketingState', '') == 'SUBSCRIBED' if sms_marketing else False
            total_spent = customer.get('amountSpent', {}).get('amount', '0')
            total_orders = customer.get('numberOfOrders', 0)
            note = customer.get('note', '')
            tax_exempt = customer.get('taxExempt', False)
            tags = ', '.join(customer.get('tags', [])) if customer.get('tags') else ''
            
            # メタフィールド（最適化）
            checkout_blocks_trigger = ''
            birth_date = ''
            
            metafields = customer.get('metafields', {}).get('edges', [])
            for meta_edge in metafields:
                if meta_edge and meta_edge.get('node'):
                    meta = meta_edge['node']
                    key = meta.get('key')
                    if key == 'trigger':
                        checkout_blocks_trigger = meta.get('value', '')
                    elif key == 'birth_date':
                        birth_date = meta.get('value', '')
            
            # 日時情報
            created_at = self.format_datetime(customer.get('createdAt', ''))
            updated_at = self.format_datetime(customer.get('updatedAt', ''))
            verified_email = customer.get('verifiedEmail', False)
            currency = customer.get('amountSpent', {}).get('currencyCode', '')
            
            # 最終注文情報
            last_order = customer.get('lastOrder', {}) or {}
            last_order_id = last_order.get('id', '')
            last_order_name = last_order.get('name', '')
            last_order_date = self.format_datetime(last_order.get('createdAt', ''))
            last_order_status = last_order.get('displayFinancialStatus', '')
            last_order_fulfillment = last_order.get('displayFulfillmentStatus', '')
            last_order_amount = last_order.get('totalPriceSet', {}).get('shopMoney', {}).get('amount', '0')
            
            # サービス統合情報
            total_service_orders = customer.get('_total_orders', 0)
            total_service_amount = customer.get('_total_amount', 0)
            first_order_date = self.format_datetime(customer.get('_first_order_date', ''))
            last_order_date = self.format_datetime(customer.get('_last_order_date', ''))
            average_order_interval = customer.get('_average_order_interval', 0)
            average_order_value = customer.get('_average_order_value', 0)
            
            # 分析フィールド
            is_new_customer = customer.get('_is_new_customer', False)
            store_key = customer.get('_store_key', '')
            store_name = customer.get('_store_name', '')
            store_url = customer.get('_store_url', '')
            
            return [
                customer_id, first_name, last_name, email, accepts_marketing,
                address_company, address1, address2, city, province_code, country_code,
                zip_code, address_phone, phone, accepts_sms_marketing,
                total_spent, total_orders, note, tax_exempt, tags,
                checkout_blocks_trigger, birth_date,
                # 追加フィールド
                created_at, updated_at, verified_email, currency,
                last_order_id, last_order_name, last_order_date, last_order_status,
                last_order_fulfillment, last_order_amount,
                # サービス統合情報
                total_service_orders, total_service_amount, first_order_date, last_order_date, average_order_interval, average_order_value,
                # 分析フィールド
                is_new_customer, store_key, store_name, store_url
            ]
            
        except Exception as e:
            self.logger.error(f"顧客行データ抽出エラー: {e}")
            return None
    
    def format_datetime(self, iso_string):
        """ISO日時文字列をYYYY/MM/DD HH:MM:SS形式に変換"""
        if not iso_string:
            return ''
        
        try:
            dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
            return dt.strftime('%Y/%m/%d %H:%M:%S')
        except:
            return iso_string
    
    def export_customers(self, filename=None, date_filter=None):
        """顧客データをエクスポート（最適化版）"""
        start_time = time.time()
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'customers_export_{timestamp}.csv'
        
        # ディレクトリ作成
        os.makedirs('exports', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        filepath = os.path.join('exports', filename)
        
        try:
            # 顧客データを取得
            self.logger.info("全ストアから顧客データを取得開始...")
            customers = self.fetch_all_customers(date_filter)
            
            if not customers:
                self.logger.error("顧客データが取得できませんでした")
                return None
            
            # CSVに変換（ジェネレーター使用）
            csv_generator = self.convert_to_csv_data(customers)
            
            # CSVファイルに保存（ストリーミング書き込み）
            self.logger.info(f"CSVファイルへの書き込み開始: {filepath}")
            row_count = 0
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                for row in csv_generator:
                    writer.writerow(row)
                    row_count += 1
                    
                    # 進捗表示（10000行ごと）
                    if row_count % 10000 == 0:
                        self.logger.info(f"CSV書き込み進捗: {row_count}行完了")
            
            # 処理時間を計算
            processing_time = time.time() - start_time
            
            self.logger.info(f"顧客データをエクスポート完了: {filepath}")
            self.logger.info(f"総顧客数: {row_count - 1}件")  # ヘッダーを除く
            self.logger.info(f"処理時間: {processing_time:.2f}秒")
            self.logger.info(f"平均処理速度: {(row_count - 1) / processing_time:.2f}件/秒")
            
            return filepath
            
        except Exception as e:
            self.logger.error(f"エクスポート中にエラー: {e}")
            return None

def main():
    """メイン実行関数（最適化版）"""
    start_time = time.time()
    
    try:
        # 初期化
        print("顧客データベース生成器を初期化中...")
        generator = CustomerDBGenerator()
        
        # 設定情報を表示
        print(f"\n=== 実行設定 ===")
        print(f"アクティブストア数: {len(generator.active_stores)}")
        print(f"並列処理数: {generator.max_workers}")
        print(f"バッチサイズ: {generator.batch_size}")
        print(f"レート制限: {generator.rate_limit_delay}秒")
        print(f"注文データ件数: {len(generator.orders_data)}")
        
        # 全期間のデータを取得
        print(f"\n=== データ取得開始 ===")
        filename = generator.export_customers()
        
        # 結果表示
        total_time = time.time() - start_time
        
        if filename:
            print(f"\n=== 処理完了 ===")
            print(f"出力ファイル: {filename}")
            print(f"総処理時間: {total_time:.2f}秒")
            print(f"平均処理速度: {generator.processed_customers / total_time:.2f}件/秒")
            print(f"\nこのファイルをGoogle SheetsのCustomerDBシートにアップロードしてください")
            print(f"\n新規顧客数の集計に必要な情報が含まれています:")
            print(f"- 各サービスの注文回数と金額")
            print(f"- 初回注文日（新規顧客判定用）")
            print(f"- ストア別の顧客情報")
        else:
            print(f"\n=== 処理失敗 ===")
            print(f"顧客データベースの作成に失敗しました")
            print(f"ログファイルを確認してください: logs/customer_db_generator.log")
            
    except KeyboardInterrupt:
        print(f"\n処理が中断されました")
        print(f"部分的な結果が保存されている可能性があります")
    except Exception as e:
        print(f"\n=== エラー発生 ===")
        print(f"エラー: {e}")
        print(f"詳細なログは logs/customer_db_generator.log を確認してください")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
