#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shopify注文データエクスポートスクリプト
前月の注文データをGraphQL APIで取得し、CSVファイルにエクスポートします
"""

import os
import json
import csv
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('order_export.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ShopifyOrderExporter:
    """Shopify注文データエクスポーター（マルチストア対応）"""
    
    def __init__(self):
        """初期化"""
        # ルートディレクトリの.envファイルを読み込み
        load_dotenv('../.env')
        self.api_version = os.getenv('API_VERSION', '2025-07')
        
        # 5ストアの設定を読み込み
        self.stores = {
                'artgraph': {
                    'shop_url': os.getenv('ARTGRAPH_SHOP'),
                    'access_token': os.getenv('ARTGRAPH_TOKEN'),
                    'name': 'Artgraph'
                },
                'e1print': {
                    'shop_url': os.getenv('E1_SHOP'),
                    'access_token': os.getenv('E1_TOKEN'),
                    'name': 'E1 Print'
                },
                'photopri': {
                    'shop_url': os.getenv('PHOTOPRI_SHOP'),
                    'access_token': os.getenv('PHOTOPRI_TOKEN'),
                    'name': 'Photopri'
                },
                'qoo': {
                    'shop_url': os.getenv('QOO_SHOP'),
                    'access_token': os.getenv('QOO_TOKEN'),
                    'name': 'Qoo'
                },
                'copycentergallery': {
                    'shop_url': os.getenv('COPYCENTERGALLERY_SHOP'),
                    'access_token': os.getenv('COPYCENTERGALLERY_TOKEN'),
                    'name': 'Copy Center Gallery'
                },
                'tette': {
                    'shop_url': os.getenv('TETTE_SHOP'),
                    'access_token': os.getenv('TETTE_TOKEN'),
                    'name': 'TETTE'
                }
        }
        
        # 有効なストアのみをフィルター
        self.active_stores = {}
        for store_key, store_config in self.stores.items():
                if store_config['shop_url'] and store_config['access_token']:
                    self.active_stores[store_key] = store_config
                    logger.info(f"✅ ストア設定確認: {store_config['name']} ({store_config['shop_url']})")
                else:
                    logger.warning(f"⚠️ ストア設定不完全: {store_key} - 環境変数を確認してください")
        
        if not self.active_stores:
                raise ValueError("有効なストア設定が見つかりません。環境変数を確認してください。")
        
        logger.info(f"有効なストア数: {len(self.active_stores)}")
        
        # 出力ディレクトリ作成
        os.makedirs('exports', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
    
    def get_date_range(self, mode: str = "recent_2months") -> tuple:
        """日付範囲を取得
        
        Args:
            mode: "all_time" (全期間), "recent_2months" (直近2ヶ月)
        """
        today = datetime.now()
        
        if mode == "all_time":
            # 全期間のデータを取得（2019年から現在まで）
            start_date = datetime(2019, 1, 1)
            end_date = today
            logger.info("全期間モード: 2019年から現在までのデータを取得します")
        elif mode == "recent_2months":
            # 直近2ヶ月分のデータを取得（新規追加対象期間）
            start_date = today - timedelta(days=60)  # 直近2ヶ月
            end_date = today
            logger.info("直近2ヶ月モード: 過去2ヶ月分のデータを取得します（新規追加）")
        else:
            raise ValueError(f"無効なモード: {mode}. 'all_time', 'recent_2months' のいずれかを指定してください。")
        
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    
    def get_date_ranges_chunked(self, mode: str, chunk_months: int = 6) -> List[tuple]:
        """日付範囲を分割して取得（全期間の場合のみ）"""
        if mode != "all_time":
            return [self.get_date_range(mode)]
        
        end_date = datetime.now()
        start_date = datetime(2019, 1, 1)
        
        date_ranges = []
        current_start = start_date
        
        while current_start < end_date:
            current_end = min(
                current_start + timedelta(days=chunk_months * 30),
                end_date
            )
            date_ranges.append((
                current_start.strftime('%Y-%m-%d'),
                current_end.strftime('%Y-%m-%d')
            ))
            current_start = current_end + timedelta(days=1)
        
        return date_ranges
    
    def is_initial_run(self) -> bool:
        """初回実行かどうかを判定"""
        # 実行履歴ファイルの存在をチェック
        history_file = 'execution_history.json'
        return not os.path.exists(history_file)
    
    def save_execution_history(self, execution_info: dict):
        """実行履歴を保存"""
        history_file = 'execution_history.json'
        
        history = []
        if os.path.exists(history_file):
                try:
                    with open(history_file, 'r', encoding='utf-8') as f:
                        history = json.load(f)
                except Exception as e:
                    logger.warning(f"実行履歴ファイルの読み込みエラー: {e}")
                    history = []
        
        history.append(execution_info)
        
        # 最新の10件のみ保持
        history = history[-10:]
        
        try:
                with open(history_file, 'w', encoding='utf-8') as f:
                    json.dump(history, f, ensure_ascii=False, indent=2)
                logger.info(f"実行履歴を保存しました: {history_file}")
        except Exception as e:
                logger.error(f"実行履歴保存エラー: {e}")
    
    def create_orders_query(self, start_date: str, end_date: str, cursor: str = None, batch_size: int = 75) -> str:
        """注文データ取得用GraphQLクエリを作成"""
        after_clause = f', after: "{cursor}"' if cursor else ""
        
        query = f"""
        query {{
          orders(first: {batch_size}, query: "created_at:>={start_date} AND created_at:<={end_date}"{after_clause}) {{
                edges {{
                  node {{
                    id
                    name
                    email
                    customer {{
                      id
                      firstName
                      lastName
                      email
                      phone
                    }}
                    displayFinancialStatus
                    displayFulfillmentStatus
                    createdAt
                    processedAt

                    fulfillments {{
                      createdAt
                    }}
                    currencyCode
                    subtotalPriceSet {{
                      shopMoney {{
                        amount
                        currencyCode
                      }}
                    }}
                    totalShippingPriceSet {{
                      shopMoney {{
                        amount
                        currencyCode
                      }}
                    }}
                    totalTaxSet {{
                      shopMoney {{
                        amount
                        currencyCode
                      }}
                    }}
                    totalPriceSet {{
                      shopMoney {{
                        amount
                        currencyCode
                      }}
                    }}
                    discountCodes
                    shippingLine {{
                      title
                    }}
                    lineItems(first: 50) {{
                      edges {{
                        node {{
                          name
                          quantity
                          originalUnitPriceSet {{
                            shopMoney {{
                              amount
                            }}
                          }}
                          variant {{
                            compareAtPrice
                          }}
                          sku
                          requiresShipping
                          taxable
                          fulfillmentStatus
                          discountAllocations {{
                            allocatedAmountSet {{
                              shopMoney {{
                                amount
                              }}
                            }}
                          }}

                        }}
                      }}
                    }}
                    billingAddress {{
                      company
                      city
                      province
                      provinceCode
                      country
                      countryCode
                    }}
                    shippingAddress {{
                      company
                      city
                      province
                      provinceCode
                      country
                      countryCode
                    }}
                    note
                    customAttributes {{
                      key
                      value
                    }}
                    cancelledAt
                    cancelReason
                    paymentGatewayNames
                    refunds {{
                      totalRefundedSet {{
                        shopMoney {{
                          amount
                          currencyCode
                        }}
                      }}
                    }}
                    tags
                    riskLevel
                    sourceIdentifier
                    taxLines {{
                      title
                      priceSet {{
                        shopMoney {{
                          amount
                        }}
                      }}
                    }}

                    transactions {{
                      gateway
                      kind
                      status
                      amountSet {{
                        shopMoney {{
                          amount
                          currencyCode
                        }}
                      }}
                      processedAt
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
        return query
    
    def execute_graphql_query(self, query: str, store_key: str) -> Dict:
        """GraphQLクエリを実行"""
        try:
                store_config = self.active_stores[store_key]
                base_url = f"https://{store_config['shop_url']}/admin/api/{self.api_version}/graphql.json"
                headers = {
                    'X-Shopify-Access-Token': store_config['access_token'],
                    'Content-Type': 'application/json',
                }
                
                response = requests.post(
                    base_url,
                    headers=headers,
                    json={'query': query},
                    timeout=30
                )
                response.raise_for_status()
                
                result = response.json()
                if 'errors' in result:
                    logger.error(f"GraphQLエラー ({store_config['name']}): {result['errors']}")
                    return None
                
                return result['data']
        
        except requests.exceptions.RequestException as e:
                logger.error(f"APIリクエストエラー ({store_config['name']}): {e}")
                return None
        except json.JSONDecodeError as e:
                logger.error(f"JSONデコードエラー ({store_config['name']}): {e}")
                return None
    
    def fetch_orders_from_store(self, store_key: str, start_date: str, end_date: str) -> List[Dict]:
        """指定ストアから注文データを取得"""
        store_config = self.active_stores[store_key]
        store_orders = []
        seen_order_names = set()  # 重複チェック用
        cursor = None
        page_count = 0
        batch_size = 75  # 初期バッチサイズ
        consecutive_small_batches = 0  # 連続して小さいバッチが返された回数
        
        logger.info(f"注文データ取得開始 ({store_config['name']}): {start_date} 〜 {end_date}")
        
        while True:
            retry_count = 0
            success = False
            max_retries = 3  # 最大リトライ回数
            
            while retry_count < max_retries and not success:
                try:
                    page_count += 1
                    logger.info(f"ページ {page_count} を取得中 ({store_config['name']})... (バッチサイズ: {batch_size})")
                    
                    query = self.create_orders_query(start_date, end_date, cursor, batch_size)
                    result = self.execute_graphql_query(query, store_key)
                    
                    if not result or 'orders' not in result:
                        logger.error(f"注文データの取得に失敗しました ({store_config['name']})")
                        return store_orders  # 早期リターン
                    
                    orders = result['orders']
                    edges = orders.get('edges', [])
                    
                    if not edges:
                        logger.info(f"注文データがありません ({store_config['name']})")
                        return store_orders  # 早期リターン
                    
                    # 注文データを追加（ストア情報も含める）
                    page_duplicates = 0
                    for edge in edges:
                        if edge and 'node' in edge and edge['node']:
                            order = edge['node']
                            # 注文オブジェクトの妥当性チェック
                            if isinstance(order, dict) and order.get('name'):
                                order_name = order.get('name')
                                
                                # 重複チェック
                                if order_name in seen_order_names:
                                    page_duplicates += 1
                                    logger.warning(f"重複注文をスキップ: {order_name}")
                                    continue
                                
                                seen_order_names.add(order_name)
                                order['_store_key'] = store_key
                                order['_store_name'] = store_config['name']
                                order['_store_url'] = store_config['shop_url']
                                store_orders.append(order)
                            else:
                                logger.warning(f"無効な注文オブジェクトをスキップ: {order}")
                        else:
                            logger.warning(f"無効なエッジオブジェクトをスキップ: {edge}")
                    
                    if page_duplicates > 0:
                        logger.warning(f"ページ {page_count}: {page_duplicates}件の重複注文をスキップ ({store_config['name']})")
                    
                    actual_count = len(edges) - page_duplicates
                    logger.info(f"ページ {page_count}: {actual_count}件の注文を取得 ({store_config['name']})")
                    
                    # 動的バッチサイズ調整
                    if actual_count < batch_size * 0.5:  # 要求した半分以下しか返されない場合
                        consecutive_small_batches += 1
                        if consecutive_small_batches >= 3:  # 3回連続で小さいバッチの場合
                            batch_size = max(25, batch_size - 10)  # 最小25件まで減らす
                            consecutive_small_batches = 0
                            logger.info(f"バッチサイズを {batch_size} に調整 ({store_config['name']})")
                    else:
                        consecutive_small_batches = 0  # リセット
                    
                    # 次のページがあるかチェック
                    page_info = orders.get('pageInfo', {})
                    if not page_info.get('hasNextPage'):
                        logger.info(f"最終ページに到達 ({store_config['name']})")
                        return store_orders  # 早期リターン
                    
                    cursor = page_info.get('endCursor')
                    if not cursor:
                        logger.info(f"カーソルが取得できません ({store_config['name']})")
                        return store_orders  # 早期リターン
                    
                    logger.info(f"次のページのカーソル: {cursor[:20]}... ({store_config['name']})")
                    success = True
                    
                except Exception as e:
                    retry_count += 1
                    error_msg = str(e)
                    
                    if 'Throttled' in error_msg or 'RATE_LIMIT' in error_msg:
                        # レート制限エラーの場合、待機時間を段階的に増加（75件固定を維持）
                        wait_time = min(20 * retry_count, 120)  # 最大120秒
                        logger.warning(f"レート制限エラー ({store_config['name']}): {wait_time}秒待機後リトライ ({retry_count}/{max_retries}) - 75件固定維持")
                        import time
                        time.sleep(wait_time)
                    else:
                        logger.error(f"ページ {page_count} 取得エラー ({store_config['name']}): {e}")
                        return store_orders  # エラー時も早期リターン
            
            if not success:
                logger.error(f"ページ {page_count} の取得に失敗しました ({store_config['name']})")
                return store_orders  # 早期リターン
            
            # レート制限対策（2秒待機に増加）
            import time
            time.sleep(2)
        
        logger.info(f"ストア合計 ({store_config['name']}): {len(store_orders)}件の注文データを取得")
        return store_orders
    
    def fetch_orders_from_store_no_duplicate_check(self, store_key: str, start_date: str, end_date: str) -> List[Dict]:
        """指定ストアから注文データを取得（重複チェックなし）"""
        store_config = self.active_stores[store_key]
        store_orders = []
        cursor = None
        page_count = 0
        batch_size = 75  # 初期バッチサイズ
        consecutive_small_batches = 0  # 連続して小さいバッチが返された回数
        max_retries = 3  # 最大リトライ回数
        
        logger.info(f"注文データ取得開始（重複チェックなし） ({store_config['name']}): {start_date} 〜 {end_date}")
        
        while True:
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    page_count += 1
                    logger.info(f"ページ {page_count} を取得中 ({store_config['name']})... (バッチサイズ: {batch_size})")
                    
                    query = self.create_orders_query(start_date, end_date, cursor, batch_size)
                    result = self.execute_graphql_query(query, store_key)
                    
                    if not result or 'orders' not in result:
                        logger.error(f"注文データの取得に失敗しました ({store_config['name']})")
                        return store_orders  # 早期リターン
                    
                    orders = result['orders']
                    edges = orders.get('edges', [])
                    
                    if not edges:
                        logger.info(f"注文データがありません ({store_config['name']})")
                        return store_orders  # 早期リターン
                    
                    # 注文データを追加（ストア情報も含める、重複チェックなし）
                    for edge in edges:
                        if edge and 'node' in edge and edge['node']:
                            order = edge['node']
                            # 注文オブジェクトの妥当性チェック
                            if isinstance(order, dict) and order.get('name'):
                                order['_store_key'] = store_key
                                order['_store_name'] = store_config['name']
                                order['_store_url'] = store_config['shop_url']
                                store_orders.append(order)
                            else:
                                logger.warning(f"無効な注文オブジェクトをスキップ: {order}")
                        else:
                            logger.warning(f"無効なエッジオブジェクトをスキップ: {edge}")
                    
                    actual_count = len(edges)
                    logger.info(f"ページ {page_count}: {actual_count}件の注文を取得 ({store_config['name']})")
                    
                    # 動的バッチサイズ調整
                    if actual_count < batch_size * 0.5:  # 要求した半分以下しか返されない場合
                        consecutive_small_batches += 1
                        if consecutive_small_batches >= 3:  # 3回連続で小さいバッチの場合
                            batch_size = max(25, batch_size - 10)  # 最小25件まで減らす
                            consecutive_small_batches = 0
                            logger.info(f"バッチサイズを {batch_size} に調整 ({store_config['name']})")
                    else:
                        consecutive_small_batches = 0  # リセット
                    
                    # 次のページがあるかチェック
                    page_info = orders.get('pageInfo', {})
                    if not page_info.get('hasNextPage'):
                        logger.info(f"最終ページに到達 ({store_config['name']})")
                        return store_orders  # 早期リターン
                    
                    cursor = page_info.get('endCursor')
                    if not cursor:
                        logger.info(f"カーソルが取得できません ({store_config['name']})")
                        return store_orders  # 早期リターン
                    
                    logger.info(f"次のページのカーソル: {cursor[:20]}... ({store_config['name']})")
                    success = True
                    
                except Exception as e:
                    retry_count += 1
                    error_msg = str(e)
                    
                    if 'Throttled' in error_msg or 'RATE_LIMIT' in error_msg:
                        # レート制限エラーの場合、待機時間を段階的に増加（75件固定を維持）
                        wait_time = min(20 * retry_count, 120)  # 最大120秒
                        logger.warning(f"レート制限エラー ({store_config['name']}): {wait_time}秒待機後リトライ ({retry_count}/{max_retries}) - 75件固定維持")
                        import time
                        time.sleep(wait_time)
                    else:
                        logger.error(f"ページ {page_count} 取得エラー ({store_config['name']}): {e}")
                        return store_orders  # エラー時も早期リターン
            
            if not success:
                logger.error(f"ページ {page_count} の取得に失敗しました ({store_config['name']})")
                return store_orders  # 早期リターン
            
            # レート制限対策（2秒待機に増加）
            import time
            time.sleep(2)
        
        logger.info(f"ストア合計 ({store_config['name']}): {len(store_orders)}件の注文データを取得")
        return store_orders

    def fetch_all_orders(self, start_date: str, end_date: str) -> List[Dict]:
        """全ストアから注文データを取得"""
        all_orders = []
        
        logger.info(f"全ストア注文データ取得開始: {start_date} 〜 {end_date}")
        
        for store_key in self.active_stores.keys():
                try:
                    store_orders = self.fetch_orders_from_store(store_key, start_date, end_date)
                    all_orders.extend(store_orders)
                    
                    # ストア間の待機時間（レート制限回避のため増加）
                    import time
                    time.sleep(3)
                    
                except Exception as e:
                    logger.error(f"ストア {self.active_stores[store_key]['name']} でエラー発生: {e}")
                    continue
        
        logger.info(f"全ストア合計: {len(all_orders)}件の注文データを取得しました")
        return all_orders
    
    def fetch_all_orders_no_duplicate_check(self, start_date: str, end_date: str) -> List[Dict]:
        """全ストアから注文データを取得（重複チェックなし）"""
        all_orders = []
        
        logger.info(f"全ストア注文データ取得開始（重複チェックなし）: {start_date} 〜 {end_date}")
        
        for store_key in self.active_stores.keys():
            try:
                store_orders = self.fetch_orders_from_store_no_duplicate_check(store_key, start_date, end_date)
                all_orders.extend(store_orders)
                
                # ストア間の待機時間
                import time
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"ストア {self.active_stores[store_key]['name']} でエラー発生: {e}")
                continue
        
        logger.info(f"全ストア合計: {len(all_orders)}件の注文データを取得しました")
        return all_orders
    
    def fetch_all_orders_parallel(self, start_date: str, end_date: str) -> List[Dict]:
        """全ストアから注文データを並列取得（重複チェックなし）"""
        import concurrent.futures
        import threading
        
        all_orders = []
        lock = threading.Lock()
        
        logger.info(f"全ストア注文データ並列取得開始: {start_date} 〜 {end_date}")
        
        def fetch_store_orders(store_key):
            try:
                store_orders = self.fetch_orders_from_store(store_key, start_date, end_date)  # 重複チェック付きに変更
                with lock:
                    all_orders.extend(store_orders)
                logger.info(f"ストア {self.active_stores[store_key]['name']} 完了: {len(store_orders)}件の注文を取得")
                return store_orders
            except Exception as e:
                logger.error(f"ストア {self.active_stores[store_key]['name']} でエラー発生: {e}")
                return []
        
        # 並列実行（最大2ストア同時に削減してレート制限を回避）
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(fetch_store_orders, store_key) for store_key in self.active_stores.keys()]
            
            # 完了を待つ
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"並列処理エラー: {e}")
        
        logger.info(f"全ストア合計: {len(all_orders)}件の注文データを取得しました")
        return all_orders
    
    def fetch_all_orders_chunked(self, mode: str) -> List[Dict]:
        """全期間データを分割して取得（レート制限回避）"""
        all_orders = []
        global_seen_order_names = set()  # 全期間通しての重複チェック用
        date_ranges = self.get_date_ranges_chunked(mode, chunk_months=6)
        
        logger.info(f"分割取得モード: {len(date_ranges)}期間に分割して取得します")
        
        for i, (start_date, end_date) in enumerate(date_ranges, 1):
            logger.info(f"期間 {i}/{len(date_ranges)}: {start_date} 〜 {end_date}")
            
            try:
                # 全期間の場合は並列で重複チェック付きで取得
                period_orders = self.fetch_all_orders_parallel(start_date, end_date)
                
                # グローバル重複チェック
                unique_orders = []
                period_duplicates = 0
                for order in period_orders:
                    order_name = order.get('name')
                    if order_name and order_name not in global_seen_order_names:
                        global_seen_order_names.add(order_name)
                        unique_orders.append(order)
                    else:
                        period_duplicates += 1
                
                all_orders.extend(unique_orders)
                logger.info(f"期間 {i} 完了: {len(unique_orders)}件の注文を取得 (重複除外: {period_duplicates}件)")
                
                # 期間完了後に連番チェックを実行
                if unique_orders:
                    self._check_sequential_orders_for_period(unique_orders, i)
                
                # 期間間の待機（レート制限回避）
                if i < len(date_ranges):
                    logger.info("次の期間まで60秒待機...")
                    import time
                    time.sleep(60)
                    
            except Exception as e:
                logger.error(f"期間 {i} の取得に失敗: {e}")
                continue
        
        logger.info(f"全期間合計: {len(all_orders)}件の注文データを取得しました")
        return all_orders
    
    def test_single_period_parallel(self, start_date: str, end_date: str) -> List[Dict]:
        """1期間のみ並列処理でテスト取得"""
        logger.info(f"テスト用1期間並列取得: {start_date} 〜 {end_date}")
        
        # 並列で重複チェック付きで取得
        period_orders = self.fetch_all_orders_parallel(start_date, end_date)
        
        logger.info(f"テスト期間完了: {len(period_orders)}件の注文を取得")
        return period_orders
    
    def _check_sequential_orders_for_period(self, orders: List[Dict], period_num: int):
        """期間ごとの連番チェック"""
        try:
            import re
            
            # ストア別に注文を整理
            store_orders = {}
            for order in orders:
                store_name = order.get('_store_name', 'Unknown')
                if store_name not in store_orders:
                    store_orders[store_name] = []
                store_orders[store_name].append(order)
            
            logger.info(f"期間 {period_num} 連番チェック開始")
            
            for store_name, store_order_list in store_orders.items():
                if not store_order_list:
                    continue
                
                # 注文番号を抽出してソート
                order_names = []
                for order in store_order_list:
                    name = order.get('name', '')
                    if name:
                        order_names.append(name)
                
                if not order_names:
                    continue
                
                order_names.sort()
                
                logger.info(f"期間 {period_num} - {store_name}: {len(order_names)}件の注文を取得")
                logger.info(f"期間 {period_num} - {store_name}: 最初の注文番号: {order_names[0]}")
                logger.info(f"期間 {period_num} - {store_name}: 最後の注文番号: {order_names[-1]}")
                
                # 連番チェック
                missing_orders = []
                for i in range(len(order_names) - 1):
                    current = order_names[i]
                    next_order = order_names[i + 1]
                    
                    # 注文番号の形式をチェック（#で始まる場合）
                    if current.startswith('#') and next_order.startswith('#'):
                        try:
                            current_num = int(current[1:])
                            next_num = int(next_order[1:])
                            
                            if next_num - current_num > 1:
                                missing_orders.append(f"{current} → {next_order} (間隔: {next_num - current_num})")
                        except ValueError:
                            # 数値でない場合はスキップ
                            continue
                
                if missing_orders:
                    logger.warning(f"期間 {period_num} - {store_name}: 連番でない注文番号を発見:")
                    for missing in missing_orders[:5]:  # 最初の5件のみ表示
                        logger.warning(f"  {missing}")
                    if len(missing_orders) > 5:
                        logger.warning(f"  ... 他{len(missing_orders) - 5}件")
                else:
                    logger.info(f"期間 {period_num} - {store_name}: 連番チェック完了 - 問題なし")
                    
        except Exception as e:
            logger.error(f"期間 {period_num} 連番チェックエラー: {e}")
    
    def format_datetime(self, datetime_str: str) -> str:
        """日時文字列をスプレッドシート用フォーマットに変換（UTCから日本時間に変換）"""
        if not datetime_str:
            return ""
        
        try:
            from datetime import timezone, timedelta
            
            # ISO形式の日時文字列をパース
            if datetime_str.endswith('Z'):
                # UTC時間としてパース
                dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(datetime_str)
            
            # UTCから日本時間（JST）に変換
            if dt.tzinfo is not None:
                # UTCから日本時間（+9時間）に変換
                jst = dt.astimezone(timezone(timedelta(hours=9)))
                dt = jst.replace(tzinfo=None)
            
            # スプレッドシートが認識しやすい形式に変換（YYYY-MM-DD HH:MM:SS）
            formatted = dt.strftime('%Y-%m-%d %H:%M:%S')
            return formatted
        except Exception as e:
            logger.warning(f"日時フォーマット変換エラー: {datetime_str} - {e}")
            return datetime_str
    
    def convert_to_csv_data(self, orders: List[Dict]) -> List[List]:
        """注文データをCSV形式に変換"""
        csv_data = []
        
                # 指定されたヘッダー行（注文番号をA列に配置、Email復活）
        headers = [
            'Name', 'Email', 'Financial Status', 'Paid at', 'Fulfillment Status', 'Fulfilled at',
                'Accepts Marketing', 'Currency', 'Subtotal', 'Shipping', 'Taxes', 'Total',
                'Discount Code', 'Discount Amount', 'Shipping Method', 'Created at',
                'Lineitem quantity', 'Lineitem name', 'Lineitem price', 'Lineitem compare at price',
                'Lineitem sku', 'Lineitem requires shipping', 'Lineitem taxable', 'Lineitem fulfillment status',
                'Billing Name', 'Billing Street', 'Billing Address1', 'Billing Address2', 'Billing Company',
                'Billing City', 'Billing Zip', 'Billing Province', 'Billing Country', 'Billing Phone',
                'Shipping Name', 'Shipping Street', 'Shipping Address1', 'Shipping Address2', 'Shipping Company',
                'Shipping City', 'Shipping Zip', 'Shipping Province', 'Shipping Country', 'Shipping Phone',
                'Notes', 'Note Attributes', 'Cancelled at', 'Payment Method', 'Payment Reference',
                'Refunded Amount', 'Vendor', 'Outstanding Balance', 'Employee', 'Location', 'Device ID',
                'Id', 'Tags', 'Risk Level', 'Source', 'Lineitem discount', 'Tax 1 Name', 'Tax 1 Value',
                'Tax 2 Name', 'Tax 2 Value', 'Tax 3 Name', 'Tax 3 Value', 'Tax 4 Name', 'Tax 4 Value',
                'Tax 5 Name', 'Tax 5 Value', 'Phone', 'Receipt Number', 'Duties',
                'Billing Province Name', 'Shipping Province Name', 'Payment ID', 'Payment Terms Name',
                'Next Payment Due At', 'Payment References'
        ]
        csv_data.append(headers)
        
        # 注文データ行
        for i, order in enumerate(orders):
                try:
                    # 注文オブジェクトの妥当性チェック
                    if not order or not isinstance(order, dict):
                        logger.warning(f"注文 {i}: 無効なオブジェクトが検出されました。スキップします。")
                        continue
                    
                    # 必須フィールドの存在チェック
                    if not order.get('name'):
                        logger.warning(f"注文 {i}: nameフィールドが存在しません。スキップします。")
                        continue
                    
                    # ストア情報
                    store_name = order.get('_store_name', '')
                    store_url = order.get('_store_url', '')
                    
                    # 基本情報
                    order_name = order.get('name', '')
                    # メールアドレス取得（注文レベル優先、次に顧客レベル）
                    email = order.get('email', '')
                    if not email and order.get('customer'):
                        customer = order.get('customer')
                        if customer and isinstance(customer, dict):
                            email = customer.get('email', '')
                    financial_status = order.get('displayFinancialStatus', '')
                    paid_at = self.format_datetime(order.get('processedAt', ''))
                    fulfillment_status = order.get('displayFulfillmentStatus', '')
                    fulfilled_at = ''
                    fulfillments = order.get('fulfillments')
                    if fulfillments and isinstance(fulfillments, list) and len(fulfillments) > 0:
                        fulfillment = fulfillments[0]
                        if fulfillment and isinstance(fulfillment, dict):
                            fulfilled_at = self.format_datetime(fulfillment.get('createdAt', ''))
                    customer_info = order.get('customer')
                    if not customer_info or not isinstance(customer_info, dict):
                        customer_info = {}
                    accepts_marketing = ''  # acceptsMarketingフィールドが利用不可
                    currency = order.get('currencyCode', '')
                    
                    # 金額情報（数値フィールドは0をデフォルト値として設定）
                    subtotal = '0'
                    if order.get('subtotalPriceSet', {}).get('shopMoney'):
                        amount = order['subtotalPriceSet']['shopMoney'].get('amount', '0')
                        subtotal = str(amount) if amount and str(amount).replace('.', '').replace('-', '').isdigit() else '0'
                    
                    shipping = '0'
                    if order.get('totalShippingPriceSet', {}).get('shopMoney'):
                        amount = order['totalShippingPriceSet']['shopMoney'].get('amount', '0')
                        shipping = str(amount) if amount and str(amount).replace('.', '').replace('-', '').isdigit() else '0'
                    
                    taxes = '0'
                    if order.get('totalTaxSet', {}).get('shopMoney'):
                        amount = order['totalTaxSet']['shopMoney'].get('amount', '0')
                        taxes = str(amount) if amount and str(amount).replace('.', '').replace('-', '').isdigit() else '0'
                    
                    total = '0'
                    if order.get('totalPriceSet', {}).get('shopMoney'):
                        amount = order['totalPriceSet']['shopMoney'].get('amount', '0')
                        total = str(amount) if amount and str(amount).replace('.', '').replace('-', '').isdigit() else '0'
                    
                    # 割引情報
                    discount_code = ', '.join(order.get('discountCodes', []))
                    discount_amount = ''  # 割引金額は別途計算が必要
                    
                    shipping_method = ''
                    shipping_line = order.get('shippingLine')
                    if shipping_line and isinstance(shipping_line, dict):
                        shipping_method = shipping_line.get('title', '')
                    created_at = self.format_datetime(order.get('createdAt', ''))
                    
                    # 請求・配送住所
                    billing = order.get('billingAddress')
                    if not billing or not isinstance(billing, dict):
                        billing = {}
                    billing_name = billing.get('name', '')
                    billing_street = billing.get('address1', '')
                    billing_address1 = billing.get('address1', '')
                    billing_address2 = billing.get('address2', '')
                    billing_company = billing.get('company', '')
                    billing_city = billing.get('city', '')
                    billing_zip = billing.get('zip', '')
                    billing_province = billing.get('provinceCode', '')
                    billing_country = billing.get('countryCode', '')
                    billing_phone = billing.get('phone', '')
                
                    shipping_addr = order.get('shippingAddress')
                    if not shipping_addr or not isinstance(shipping_addr, dict):
                        shipping_addr = {}
                    shipping_name = shipping_addr.get('name', '')
                    shipping_street = shipping_addr.get('address1', '')
                    shipping_address1 = shipping_addr.get('address1', '')
                    shipping_address2 = shipping_addr.get('address2', '')
                    shipping_company = shipping_addr.get('company', '')
                    shipping_city = shipping_addr.get('city', '')
                    shipping_zip = shipping_addr.get('zip', '')
                    shipping_province = shipping_addr.get('provinceCode', '')
                    shipping_country = shipping_addr.get('countryCode', '')
                    shipping_phone = shipping_addr.get('phone', '')
                    
                    # その他の情報
                    notes = order.get('note', '')
                    note_attributes = ''
                    if order.get('customAttributes'):
                        attrs = []
                        for attr in order['customAttributes']:
                            if attr.get('key') and attr.get('value'):
                                attrs.append(f"{attr['key']}: {attr['value']}")
                        note_attributes = '; '.join(attrs)
                    
                    cancelled_at = self.format_datetime(order.get('cancelledAt', ''))
                    payment_method = ', '.join(order.get('paymentGatewayNames', []))
                    payment_reference = ''  # transactionsから取得
                    
                    refunded_amount = ''
                    if order.get('refunds'):
                        total_refunded = 0
                        for refund in order['refunds']:
                            if refund.get('totalRefundedSet', {}).get('shopMoney'):
                                total_refunded += float(refund['totalRefundedSet']['shopMoney'].get('amount', 0))
                        refunded_amount = str(total_refunded)
                    
                    vendor = ''  # 商品レベルの情報
                    outstanding_balance = ''  # 利用不可
                    employee = ''  # 利用不可
                    location = ''  # 利用不可
                    device_id = ''  # clientDetailsフィールドが利用不可
                    
                    order_id = order.get('id', '').split('/')[-1] if order.get('id') else ''
                    tags = ', '.join(order.get('tags', [])) if order.get('tags') else ''
                    risk_level = order.get('riskLevel', '')
                    source = order.get('sourceIdentifier', '')
                    
                    # 税情報
                    tax_lines = order.get('taxLines', [])
                    tax_names = [''] * 5
                    tax_values = [''] * 5
                    for i, tax in enumerate(tax_lines[:5]):
                        tax_names[i] = tax.get('title', '')
                        if tax.get('priceSet', {}).get('shopMoney'):
                            tax_values[i] = tax['priceSet']['shopMoney'].get('amount', '')
                    
                    phone = customer_info.get('phone', '')
                    receipt_number = order.get('receiptNumber', '')
                    duties = 'Yes' if order.get('dutiesIncluded') else 'No'
                    
                    # 住所名
                    billing_province_name = billing.get('province', '')
                    shipping_province_name = shipping_addr.get('province', '')
                    
                    # 支払い条件・参照情報
                    payment_id = ''
                    payment_terms_name = ''
                    next_payment_due_at = ''
                    payment_references = ''
                    
                    # transactionsから支払い情報を取得
                    if order.get('transactions'):
                        gateways = []
                        for transaction in order['transactions']:
                            if transaction.get('gateway'):
                                gateways.append(transaction['gateway'])
                        if gateways:
                            payment_method = ', '.join(set(gateways))  # 重複を削除
                    
                    # 商品行アイテム
                    line_items = order.get('lineItems', {}).get('edges', [])
                    
                    if line_items:
                        # 各商品行アイテムに対して行を作成
                        for item_edge in line_items:
                            item = item_edge['node']
                            # 数量は数値として処理（0をデフォルト値）
                            quantity = item.get('quantity', 0)
                            try:
                                qty = int(quantity) if quantity else 0
                                lineitem_quantity = str(qty) if qty > 0 else '0'
                            except (ValueError, TypeError):
                                lineitem_quantity = '0'
                            lineitem_name = item.get('name', '')
                            # 価格は数値として処理（0をデフォルト値）
                            lineitem_price = '0'
                            if item.get('originalUnitPriceSet', {}).get('shopMoney'):
                                amount = item['originalUnitPriceSet']['shopMoney'].get('amount', '0')
                                lineitem_price = str(amount) if amount and str(amount).replace('.', '').replace('-', '').isdigit() else '0'
                            lineitem_compare_at_price = '0'
                            variant = item.get('variant')
                            if variant and isinstance(variant, dict) and variant.get('compareAtPrice'):
                                amount = variant['compareAtPrice']
                                lineitem_compare_at_price = str(amount) if amount and str(amount).replace('.', '').replace('-', '').isdigit() else '0'
                            lineitem_sku = item.get('sku', '')
                            lineitem_requires_shipping = 'Yes' if item.get('requiresShipping') else 'No'
                            lineitem_taxable = 'Yes' if item.get('taxable') else 'No'
                            lineitem_fulfillment_status = item.get('fulfillmentStatus', '')
                            
                            # 商品割引
                            lineitem_discount = ''
                            if item.get('discountAllocations'):
                                discounts = []
                                for discount in item['discountAllocations']:
                                    if discount.get('allocatedAmountSet', {}).get('shopMoney'):
                                        discounts.append(discount['allocatedAmountSet']['shopMoney'].get('amount', ''))
                                lineitem_discount = ', '.join(discounts)
                            
                            row = [
                                order_name, email, financial_status, paid_at, fulfillment_status, fulfilled_at,
                                accepts_marketing, currency, subtotal, shipping, taxes, total,
                                discount_code, discount_amount, shipping_method, created_at,
                                lineitem_quantity, lineitem_name, lineitem_price, lineitem_compare_at_price,
                                lineitem_sku, lineitem_requires_shipping, lineitem_taxable, lineitem_fulfillment_status,
                                billing_name, billing_street, billing_address1, billing_address2, billing_company,
                            billing_city, billing_zip, billing_province, billing_country, billing_phone,
                            shipping_name, shipping_street, shipping_address1, shipping_address2, shipping_company,
                            shipping_city, shipping_zip, shipping_province, shipping_country, shipping_phone,
                            notes, note_attributes, cancelled_at, payment_method, payment_reference,
                            refunded_amount, vendor, outstanding_balance, employee, location, device_id,
                            order_id, tags, risk_level, source, lineitem_discount,
                            tax_names[0], tax_values[0], tax_names[1], tax_values[1],
                            tax_names[2], tax_values[2], tax_names[3], tax_values[3],
                            tax_names[4], tax_values[4], phone, receipt_number, duties,
                            billing_province_name, shipping_province_name, payment_id, payment_terms_name,
                            next_payment_due_at, payment_references
                        ]
                        csv_data.append(row)
                    else:
                        # 商品行アイテムがない場合
                        row = [
                            order_name, email, financial_status, paid_at, fulfillment_status, fulfilled_at,
                            accepts_marketing, currency, subtotal, shipping, taxes, total,
                            discount_code, discount_amount, shipping_method, created_at,
                            '0', '', '0', '0', '', 'No', 'No', '',
                            billing_name, billing_street, billing_address1, billing_address2, billing_company,
                            billing_city, billing_zip, billing_province, billing_country, billing_phone,
                            shipping_name, shipping_street, shipping_address1, shipping_address2, shipping_company,
                            shipping_city, shipping_zip, shipping_province, shipping_country, shipping_phone,
                            notes, note_attributes, cancelled_at, payment_method, payment_reference,
                            refunded_amount, vendor, outstanding_balance, employee, location, device_id,
                            order_id, tags, risk_level, source, '',
                            tax_names[0], tax_values[0], tax_names[1], tax_values[1],
                            tax_names[2], tax_values[2], tax_names[3], tax_values[3],
                            tax_names[4], tax_values[4], phone, receipt_number, duties,
                            billing_province_name, shipping_province_name, payment_id, payment_terms_name,
                            next_payment_due_at, payment_references
                    ]
                        csv_data.append(row)
                    
                except Exception as e:
                    logger.error(f"注文 {i} の変換処理でエラー: {e}")
                    logger.error(f"注文データ: {order.get('name', 'Unknown')} - {order.get('_store_name', 'Unknown Store')}")
                    import traceback
                    logger.error(f"詳細エラー: {traceback.format_exc()}")
                    continue
        
        return csv_data
    
    def export_to_csv(self, csv_data: List[List], filename: str):
        """CSVファイルにエクスポート"""
        filepath = f"exports/{filename}"
        
        try:
                with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerows(csv_data)
                
                logger.info(f"CSVファイルにエクスポート完了: {filepath}")
                logger.info(f"合計 {len(csv_data) - 1}行のデータを出力")
                
        except Exception as e:
                logger.error(f"CSVファイル出力エラー: {e}")
                raise
    
    def export_orders(self, mode: str = "recent_2months") -> tuple:
        """注文データをエクスポート
        
        Args:
            mode: "all_time" (全期間), "recent_2months" (直近2ヶ月)
        """
        try:
                # 日付範囲を取得
                start_date, end_date = self.get_date_range(mode)
                logger.info(f"対象期間: {start_date} 〜 {end_date}")
                
                # 注文データを取得（全期間の場合は分割取得）
                if mode == "all_time":
                    orders = self.fetch_all_orders_chunked(mode)
                else:
                    orders = self.fetch_all_orders(start_date, end_date)
                
                if not orders:
                    logger.warning("エクスポート対象の注文データがありません")
                    return "", mode
                
                # CSVデータに変換
                csv_data = self.convert_to_csv_data(orders)
                
                # ファイル名を生成
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"orders_{mode}_{start_date}_to_{end_date}_{timestamp}.csv"
                
                # CSVファイルにエクスポート
                self.export_to_csv(csv_data, filename)
                
                # 実行履歴を保存
                execution_info = {
                    'timestamp': datetime.now().isoformat(),
                    'mode': mode,
                    'start_date': start_date,
                    'end_date': end_date,
                    'total_orders': len(orders),
                    'filename': filename,
                    'active_stores': list(self.active_stores.keys())
                }
                self.save_execution_history(execution_info)
                
                return filename, mode
                
        except Exception as e:
                logger.error(f"注文データエクスポートエラー: {e}")
                raise

def main():
    """メイン実行関数"""
    try:
        exporter = ShopifyOrderExporter()
        
        # 直近2ヶ月の注文データをエクスポート（新規追加処理用）
        filename, mode = exporter.export_orders(mode="recent_2months")
        
        if filename:
                print(f"✅ 注文データのエクスポートが完了しました: {filename}")
                print(f"📁 ファイル保存場所: exports/{filename}")
        else:
                print("⚠️ エクスポート対象のデータがありませんでした")
                
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        logger.error(f"メイン実行エラー: {e}")

if __name__ == "__main__":
    main()

