# レポートシステム

Shopifyの注文データと顧客データを取得し、Googleスプレッドシートに自動アップロードし、週次レポートをLarkに通知する統合システムです。

## 🎯 システム概要

### 週次レポートシステム（メイン機能）
毎週月曜日9:00に以下の処理を自動実行し、完了後にLarkに通知します：

1. **注文データの取得・更新** - Shopifyから直近2ヶ月の注文データを取得し、スプレッドシートに反映
2. **顧客データの更新** - 顧客の詳細情報を取得・更新
3. **スプレッドシート反映** - 重複処理を含む効率的なデータアップロード
4. **週次サマリーの生成** - 月間目標・売上・週末注文の統計を生成
5. **Lark通知** - 生成されたサマリーを指定されたグループに送信

### 従来のレポート機能
以下の機能も引き続き利用可能です：

#### 1. 注文データエクスポート (`order_export.py`)
- Shopify GraphQL APIを使用して注文データを取得
- 全店舗対応（ArtGraph、E1 Print、Photopri、Qoo、Copy Center Gallery、TETTE）
- メールアドレスを含む詳細な顧客情報を取得
- CSV形式でエクスポート
- 複数の期間モード対応（recent_2months、recent_3months等）

#### 2. 顧客データベース生成 (`customer_db_generator.py`)
- 顧客の詳細情報を取得
- 新規顧客フラグの計算
- サービス別の顧客メトリクス
- CustomerDBシート用のCSV生成
- 並列処理による高速化

#### 3. スプレッドシートアップロード (`spreadsheet_uploader.py`)
- Google Sheets APIを使用したデータアップロード
- 大容量データの分割アップロード対応
- 重複データの効率的な更新（--overwriteモード）
- 動的なシート容量拡張
- 数値・通貨・日時の書式設定

## 🚀 週次レポートシステムの使用方法

### セットアップ

#### 1. 環境変数の設定
`.env`ファイルに以下の設定を追加済み：
```env
# Lark Webhook URL
LARK_daily_webhook=https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxxxx

# Shopify API設定
ARTGRAPH_SHOP=artgraph-shop.myshopify.com
ARTGRAPH_TOKEN=xxxxx
E1_SHOP=e1print.myshopify.com
E1_TOKEN=xxxxx
PHOTOPRI_SHOP=photopri.myshopify.com
PHOTOPRI_TOKEN=xxxxx
QOO_SHOP=aad872-2.myshopify.com
QOO_TOKEN=xxxxx
COPYCENTERGALLERY_SHOP=86717f-2.myshopify.com
COPYCENTERGALLERY_TOKEN=xxxxx
TETTE_SHOP=tette-flower.myshopify.com
TETTE_TOKEN=xxxxx

# Google Sheets設定
GOOGLE_SHEETS_ID=1S_IbeV2syeauIvP5w0uAhBs3t3MaNoczrXLpBMCh54g
```

#### 2. 必要なライブラリのインストール
```bash
pip install schedule pandas google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client python-dotenv requests
```

### 基本的な使用方法

#### 1. システムテスト
```bash
cd "/Users/photopriinc/Documents/coding/Shopify bulk task/レポート"
python3 -c "
from core.weekly_scheduler import WeeklyScheduler
scheduler = WeeklyScheduler()
scheduler.run_weekly_report()
"
```

#### 2. 個別コンポーネントのテスト
```bash
# 注文データエクスポート
python3 -c "
from core.order_export import ShopifyOrderExporter
exporter = ShopifyOrderExporter()
filename, mode = exporter.export_orders(mode='recent_2months')
print(f'エクスポート完了: {filename}')
"

# 顧客データベース生成
python3 core/customer_db_generator.py

# スプレッドシートアップロード
python3 core/spreadsheet_uploader.py exports/orders_recent_2months_*.csv db --overwrite

# サマリー生成
python3 -c "
from core.summary_generator import SummaryGenerator
generator = SummaryGenerator()
summary_data = generator.generate_weekly_summary()
formatted_summary = generator.format_weekly_summary(summary_data)
print(formatted_summary)
"
```

#### 3. スケジューラーを開始
```bash
python3 core/weekly_scheduler.py
```
毎週月曜日9:00に自動実行するスケジューラーを開始します。

## 📊 サマリーメッセージの形式

### 週次サマリー
```
📈 2025年9月29日〜10月5日ウィークリーサマリー

【10月の目標売上】
全体：11,492,710円
#P：8,851,335円
#E：1,572,307円
#A：839,068円
#Q：230,000円

【本日時点での10月売上＆注文件数】
全体：1,431,422円 - 12.5%(122件)
#P：1,246,722円 - 14.1%(101件)
#E：74,725円 - 4.8%(13件)
#A：89,406円 - 10.7%(5件)
#Q：20,569円 - 8.9%(3件)

【週末(10月4日0時〜10月6日9時)の注文】
全体：539,053円(51件)
#P：473,079円(43件)
#E：32,215円(5件)
#A：27,020円(2件)
#Q：6,739円(1件)
```

### データの詳細説明

#### 月間目標売上
- **データソース**: `PlanInput`シート
- **対象サービス**: #A, #P, #E, #Q
- **取得条件**: A列が対象月、B列が対象サービス、G列の売上金額

#### 月間売上実績
- **データソース**: `ServiceData`シート
- **対象サービス**: #A, #P, #E, #Q
- **取得条件**: A列が対象月、B列が対象サービス、C列の売上金額と注文件数
- **達成率**: 目標売上に対する実績の割合

#### 週末注文
- **データソース**: `db`シート（実際の注文データ）
- **対象サービス**: #A, #P, #E, #Qのみ（#Tは除外）
- **期間**: 前週土曜日0:00〜今週月曜日9:00
- **集計方法**: サービス別件数の合計が全体件数と一致

## 📁 ファイル構成

```
レポート/
├── core/
│   ├── lark_webhook_notifier.py    # Lark Webhook通知機能
│   ├── summary_generator.py        # サマリー生成機能
│   ├── weekly_scheduler.py         # スケジューラー機能
│   ├── order_export.py             # 注文データエクスポート
│   ├── customer_db_generator.py    # 顧客データベース生成
│   └── spreadsheet_uploader.py     # スプレッドシートアップロード
├── managers/
│   ├── integrated_data_manager.py  # 統合データ管理
│   ├── order_data_manager.py       # 注文データ管理
│   ├── customer_data_manager.py    # 顧客データ管理
│   ├── user_analysis_manager.py    # ユーザー分析管理
│   ├── service_analysis_manager.py # サービス分析管理
│   └── churn_alert_manager.py      # 離脱者アラート管理
├── exports/                        # エクスポートされたCSVファイル
│   ├── orders_recent_2months_*.csv
│   └── customers_export_*.csv
├── logs/                           # ログファイル
│   ├── weekly_scheduler.log
│   ├── summary_generator.log
│   └── customer_db_generator.log
├── data/                           # 生成されたサマリーファイル
│   └── weekly_summary_*.json
└── README.md                       # このファイル
```

## 🔧 トラブルシューティング

### よくある問題

#### 1. 環境変数エラー
```
❌ 有効なストア設定が見つかりません
```
→ `.env`ファイルのパスを確認（`../.env`に修正済み）

#### 2. スプレッドシートアクセスエラー
```
❌ Google Sheets API認証エラー
```
→ `credentials.json`ファイルが正しい場所にあるか確認

#### 3. 週末注文の件数が合わない
```
❌ サービス別件数の合計が全体件数と一致しない
```
→ **修正済み**: #A, #P, #E, #Qのみを集計対象とするように修正

#### 4. スプレッドシートアップロードのタイムアウト
```
❌ The read operation timed out
```
→ **正常動作**: タイムアウトエラーでもデータは正常に反映される

### ログファイル
- `logs/weekly_scheduler.log` - スケジューラーのログ
- `logs/summary_generator.log` - サマリー生成のログ
- `logs/customer_db_generator.log` - 顧客データベース生成のログ
- `execution_history.json` - 実行履歴

## 🎯 システムの特徴

### データ処理の流れ
1. **注文データエクスポート** (`recent_2months`モード)
2. **顧客データベース生成** (24,000件以上の顧客データ)
3. **スプレッドシートアップロード** (`--overwrite`モード)
4. **サマリー生成** (月間目標・実績・週末注文)
5. **Lark通知** (Webhook経由)

### パフォーマンス最適化
- **並列処理**: 顧客データ取得で最大4並列
- **バッチ処理**: 大容量データの効率的な処理
- **重複処理**: 既存データの更新と新規データの追加
- **タイムアウト対策**: 長時間処理の適切なエラーハンドリング

### データ整合性
- **サービス限定**: #A, #P, #E, #Qのみを集計対象
- **期間設定**: 正確な週次期間の計算
- **件数一致**: サービス別件数の合計が全体件数と一致

## ⚠️ 注意事項

### 実行環境
- サーバーやPCが常時稼働している必要があります
- インターネット接続が必要です
- Python環境と必要なライブラリがインストールされている必要があります

### API制限
- **Shopify API**: レート制限あり（自動的に待機時間を調整）
- **Google Sheets API**: 1日あたりのリクエスト制限あり
- **Lark API**: Webhook経由で制限を回避

### データの整合性
- スプレッドシートの列構成が変更された場合は、コードの修正が必要
- サービス名（#A, #P, #E, #Q）が変更された場合は、コードの修正が必要
- **#T（TETTE）**の注文は週末注文集計から除外される

## 📈 パフォーマンス

### 処理時間の目安
- **注文データエクスポート**: 約2-3分（2,300件程度）
- **顧客データベース生成**: 約3-4分（24,000件程度）
- **スプレッドシートアップロード**: 約5-10分（重複処理含む）
- **サマリー生成**: 約1-2分
- **Lark通知**: 約1分
- **合計**: 約10-20分

### データ量
- **注文データ**: 約2,300件（2ヶ月分）
- **顧客データ**: 約24,000件
- **スプレッドシート**: 約39,000行

## 🎯 期待される効果

### 自動化による効果
1. **作業時間の削減**: 手動でのデータ取得・更新作業が不要
2. **データの最新性**: 毎週自動で最新データを取得・反映
3. **レポートの標準化**: 統一されたフォーマットでレポートを生成
4. **エラーの早期発見**: 自動実行により問題を早期に検出
5. **業務効率の向上**: 定型的な作業から解放され、より重要な業務に集中可能

### データの正確性
1. **件数の整合性**: サービス別件数の合計が全体件数と一致
2. **期間の正確性**: 正確な週次期間での集計
3. **サービス限定**: 対象サービスのみを集計対象とする
4. **重複処理**: 既存データの更新と新規データの追加を適切に処理

## 📞 サポート

問題が発生した場合は、以下を確認してください：

1. **ログファイルの内容**: `logs/`ディレクトリ内のログファイル
2. **環境変数の設定**: `.env`ファイルの設定
3. **スプレッドシートのアクセス権限**: `credentials.json`の配置
4. **Lark Webhook設定**: `LARK_daily_webhook`の設定

必要に応じて、各コンポーネントを個別にテストして問題を特定してください。

## 🔄 更新履歴

### 最新の修正内容
- **週末注文集計の修正**: #A, #P, #E, #Qのみを集計対象とするように修正
- **環境変数パス修正**: `.env`ファイルの読み込みパスを修正
- **Lark通知方式変更**: アプリ通知からWebhook通知に変更
- **データ処理最適化**: 2ヶ月分のデータ処理に変更
- **重複処理改善**: スプレッドシートアップロードの重複処理を最適化